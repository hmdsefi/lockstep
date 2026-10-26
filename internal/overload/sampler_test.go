// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload_test

import (
	"context"
	"log/slog"
	"math"
	"reflect"
	"runtime"
	"runtime/metrics"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/overload"
)

const tick = 100 * time.Millisecond

// metricFeed is a read hook: each call serves the next live-heap value; the limit is set by the
// test. A value of badLive makes the live-heap metric read as KindBad.
type metricFeed struct {
	live     []uint64
	limit    uint64
	bad      bool // the live-heap metric reads as KindBad
	badLimit bool // the memory-limit metric reads as KindBad
	calls    int
}

func (f *metricFeed) read(samples []metrics.Sample) {
	i := min(f.calls, len(f.live)-1)
	f.calls++
	for k := range samples {
		switch samples[k].Name {
		case "/gc/heap/live:bytes":
			samples[k].Value = metrics.Value{} // like the runtime, a bad value overwrites the previous one
			if !f.bad {
				samples[k].Value = overload.Uint64Value(f.live[i])
			}
		case "/gc/gomemlimit:bytes":
			samples[k].Value = metrics.Value{}
			if !f.badLimit {
				samples[k].Value = overload.Uint64Value(f.limit)
			}
		}
	}
}

// rig is a Sampler on an enginetest clock with a bound Gate.
type rig struct {
	eng     *enginetest.Engine
	gate    *overload.Gate
	changes []engine.AdmissionChange
	sampler *overload.Sampler
}

func newRig(t *testing.T, feed *metricFeed, high, low uint64, explicit bool, log *slog.Logger) *rig {
	t.Helper()
	t.Cleanup(overload.SetReadMetrics(feed.read))
	r := &rig{eng: enginetest.New("n", 1), gate: overload.NewGate()}
	r.gate.Bind(func(c engine.AdmissionChange) { r.changes = append(r.changes, c) }, nil)
	r.sampler = overload.NewSampler(r.gate, r.eng.Engine().Clock, high, low, explicit, log, nil)
	return r
}

// memoryOn reports whether the Gate refuses because of the memory source.
func (r *rig) memoryOn() bool { return r.gate.AdmitSend() != nil }

// record is one captured log record: level, message and every attribute in order, the handler's
// own attributes first.
type record struct {
	level slog.Level
	msg   string
	attrs []slog.Attr
}

type capture struct {
	mu   *sync.Mutex
	recs *[]record
	pre  []slog.Attr
}

func newCapture() (*capture, func() []record) {
	c := &capture{mu: new(sync.Mutex), recs: new([]record)}
	return c, func() []record {
		c.mu.Lock()
		defer c.mu.Unlock()
		return slices.Clone(*c.recs)
	}
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }

func (c *capture) Handle(_ context.Context, r slog.Record) error {
	rec := record{level: r.Level, msg: r.Message, attrs: slices.Clone(c.pre)}
	r.Attrs(func(a slog.Attr) bool { rec.attrs = append(rec.attrs, a); return true })
	c.mu.Lock()
	*c.recs = append(*c.recs, rec)
	c.mu.Unlock()
	return nil
}

func (c *capture) WithAttrs(as []slog.Attr) slog.Handler {
	return &capture{mu: c.mu, recs: c.recs, pre: append(slices.Clone(c.pre), as...)}
}

func (c *capture) WithGroup(string) slog.Handler { return c }

func wantStates(t *testing.T, got []bool, want ...bool) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("memory source states = %v; want %v", got, want)
	}
}

// AT-OVL-26: the source follows the limit's thresholds, one label per tick, no goroutine.
func TestATOVL26SamplerFollowsLimit(t *testing.T) {
	feed := &metricFeed{live: []uint64{899, 900, 850, 800, 799}, limit: 1000}
	r := newRig(t, feed, 0, 0, false, nil)
	goroutines := runtime.NumGoroutine()

	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	got := []bool{r.memoryOn()} // the first sample, taken by Start
	for range 4 {
		r.eng.Advance(tick)
		got = append(got, r.memoryOn())
	}
	wantStates(t, got, false, true, true, false, false)

	if feed.calls != 5 {
		t.Errorf("metrics read %d times; want 5 (Start plus four ticks)", feed.calls)
	}
	labels := r.eng.Labels()
	want := slices.Repeat([]string{"lockstep/overload-sample"}, 5) // armed by Start and re-armed by each tick
	if !reflect.DeepEqual(labels, want) {
		t.Errorf("Labels() = %v; want %v", labels, want)
	}
	if n := runtime.NumGoroutine(); n != goroutines {
		t.Errorf("goroutines = %d; want %d: the sampler starts none", n, goroutines)
	}

	wantChanges := []engine.AdmissionChange{
		{Source: engine.SourceMemory, On: true, Live: 900, High: 900, Low: 800, Shedding: true},
		{Source: engine.SourceMemory, On: false, Live: 800, High: 900, Low: 800, Shedding: false},
	}
	if !reflect.DeepEqual(r.changes, wantChanges) {
		t.Errorf("changes = %+v; want %+v", r.changes, wantChanges)
	}
}

// OVL-310: a limit change takes effect at the next sample; no limit turns the source off.
func TestSamplerLimitChange(t *testing.T) {
	feed := &metricFeed{live: []uint64{950}, limit: 1000}
	r := newRig(t, feed, 0, 0, false, nil)
	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	if !r.memoryOn() {
		t.Fatal("source is off at 950 with limit 1000; want on")
	}

	feed.limit = 2000 // high 1800, low 1600: 950 is below low
	r.eng.Advance(tick)
	if r.memoryOn() {
		t.Fatal("source is on at 950 after the limit rose to 2000; want off")
	}

	feed.live, feed.calls = []uint64{950}, 0
	feed.limit = 1000
	r.eng.Advance(tick)
	if !r.memoryOn() {
		t.Fatal("source is off at 950 after the limit fell to 1000; want on")
	}

	feed.limit = math.MaxInt64 // no limit: no thresholds
	r.eng.Advance(tick)
	if r.memoryOn() {
		t.Fatal("source is on with no memory limit; want off")
	}
}

// AT-OVL-29: explicit thresholds apply with no limit.
func TestATOVL29ExplicitThresholds(t *testing.T) {
	feed := &metricFeed{live: []uint64{2000, 1500, 1000}, limit: math.MaxInt64}
	r := newRig(t, feed, 2000, 1000, true, nil)
	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	got := []bool{r.memoryOn()}
	for range 2 {
		r.eng.Advance(tick)
		got = append(got, r.memoryOn())
	}
	wantStates(t, got, true, true, false)
	want := []engine.AdmissionChange{
		{Source: engine.SourceMemory, On: true, Live: 2000, High: 2000, Low: 1000, Shedding: true},
		{Source: engine.SourceMemory, On: false, Live: 1000, High: 2000, Low: 1000, Shedding: false},
	}
	if !reflect.DeepEqual(r.changes, want) {
		t.Errorf("changes = %+v; want %+v", r.changes, want)
	}
}

// AT-OVL-31: a metric that reads KindBad keeps the source off and logs nothing.
func TestATOVL31KindBad(t *testing.T) {
	capt, records := newCapture()
	feed := &metricFeed{live: []uint64{999}, limit: 1000, bad: true}
	r := newRig(t, feed, 0, 0, false, slog.New(capt))
	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	for range 4 {
		if r.memoryOn() {
			t.Fatal("source is on while the live heap reads KindBad")
		}
		r.eng.Advance(tick)
	}
	if r.memoryOn() {
		t.Fatal("source is on while the live heap reads KindBad")
	}
	if feed.calls != 5 {
		t.Errorf("metrics read %d times; want 5", feed.calls)
	}
	if recs := records(); len(recs) != 0 {
		t.Errorf("logged %d records %+v; want none", len(recs), recs)
	}
	if len(r.changes) != 0 {
		t.Errorf("changes = %+v; want none", r.changes)
	}
}

// wantOffChange asserts that the memory source went on and then off, that the off change and the
// "admission resumed" record carry live and the thresholds last in force (900 and 800), and that
// the source is off.
func wantOffChange(t *testing.T, r *rig, records func() []record, live uint64) {
	t.Helper()
	if r.memoryOn() {
		t.Fatal("source stays on; want off")
	}
	want := []engine.AdmissionChange{
		{Source: engine.SourceMemory, On: true, Live: 950, High: 900, Low: 800, Shedding: true},
		{Source: engine.SourceMemory, On: false, Live: live, High: 900, Low: 800, Shedding: false},
	}
	if !reflect.DeepEqual(r.changes, want) {
		t.Errorf("changes = %+v; want %+v", r.changes, want)
	}
	recs := records()
	if len(recs) != 2 {
		t.Fatalf("logged %d records %+v; want 2", len(recs), recs)
	}
	wantAttrs := []slog.Attr{slog.Uint64("heap_live_bytes", live), slog.Uint64("low_mark_bytes", 800)}
	got := recs[1]
	if got.level != slog.LevelInfo || got.msg != "admission resumed" || !slices.EqualFunc(got.attrs, wantAttrs, slog.Attr.Equal) {
		t.Errorf("off record = %v %q %v; want Info \"admission resumed\" %v", got.level, got.msg, got.attrs, wantAttrs)
	}
}

// OVL-330: a source that is on goes off when the live heap becomes unreadable. The off change
// carries 0 for the live heap and the thresholds last in force.
func TestSamplerKindBadAfterOn(t *testing.T) {
	capt, records := newCapture()
	feed := &metricFeed{live: []uint64{950}, limit: 1000}
	r := newRig(t, feed, 0, 0, false, slog.New(capt))
	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	if !r.memoryOn() {
		t.Fatal("source is off at 950 with limit 1000; want on")
	}
	feed.bad = true
	r.eng.Advance(tick)
	wantOffChange(t, r, records, 0)
}

// OVL-330: when only the limit becomes unreadable the live heap is still reported.
func TestSamplerLimitKindBadAfterOn(t *testing.T) {
	capt, records := newCapture()
	feed := &metricFeed{live: []uint64{950}, limit: 1000}
	r := newRig(t, feed, 0, 0, false, slog.New(capt))
	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	if !r.memoryOn() {
		t.Fatal("source is off at 950 with limit 1000; want on")
	}
	feed.badLimit = true
	r.eng.Advance(tick)
	wantOffChange(t, r, records, 950)
}

// OVL-310: a source that is on goes off when the limit vanishes. The off change carries the live
// heap and the thresholds last in force.
func TestSamplerLimitVanishesAfterOn(t *testing.T) {
	capt, records := newCapture()
	feed := &metricFeed{live: []uint64{950}, limit: 1000}
	r := newRig(t, feed, 0, 0, false, slog.New(capt))
	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	if !r.memoryOn() {
		t.Fatal("source is off at 950 with limit 1000; want on")
	}
	feed.limit = math.MaxInt64
	r.eng.Advance(tick)
	wantOffChange(t, r, records, 950)
}

// OVL-300: with WithAdmission(0, 0) the sampler never reads the metrics, yet the tick re-arms.
func TestSamplerAdmissionOff(t *testing.T) {
	feed := &metricFeed{live: []uint64{1 << 40}, limit: 1}
	r := newRig(t, feed, 0, 0, true, nil)
	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	for range 3 {
		r.eng.Advance(tick)
	}
	if feed.calls != 0 {
		t.Errorf("metrics read %d times; want never", feed.calls)
	}
	if r.memoryOn() {
		t.Error("source is on with admission off")
	}
	if got := len(r.eng.Labels()); got != 4 {
		t.Errorf("timer armed %d times; want 4 (Start plus three re-arms)", got)
	}
}

// OVL-340: the two admission events, attributes after the logger's own.
func TestSamplerLogsChanges(t *testing.T) {
	capt, records := newCapture()
	log := slog.New(capt).With("node", "n1")
	feed := &metricFeed{live: []uint64{2100, 2100, 900}, limit: math.MaxInt64}
	r := newRig(t, feed, 2000, 1000, true, log)
	r.sampler.Start()
	t.Cleanup(r.sampler.Stop)
	r.eng.Advance(tick) // 2100 again: no change
	r.eng.Advance(tick)

	want := []record{
		{slog.LevelWarn, "admission refusing", []slog.Attr{
			slog.String("node", "n1"), slog.Uint64("heap_live_bytes", 2100), slog.Uint64("high_mark_bytes", 2000)}},
		{slog.LevelInfo, "admission resumed", []slog.Attr{
			slog.String("node", "n1"), slog.Uint64("heap_live_bytes", 900), slog.Uint64("low_mark_bytes", 1000)}},
	}
	got := records()
	if len(got) != len(want) {
		t.Fatalf("logged %d records %+v; want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i].level != want[i].level || got[i].msg != want[i].msg {
			t.Errorf("record %d = %v %q; want %v %q", i, got[i].level, got[i].msg, want[i].level, want[i].msg)
		}
		if len(got[i].attrs) != len(want[i].attrs) {
			t.Errorf("record %d attributes = %v; want %v", i, got[i].attrs, want[i].attrs)
			continue
		}
		for k, a := range want[i].attrs {
			if !got[i].attrs[k].Equal(a) {
				t.Errorf("record %d attribute %d = %v; want %v", i, k, got[i].attrs[k], a)
			}
		}
	}
}

// cancelSpy counts the Cancel calls of the clock it wraps.
type cancelSpy struct {
	engine.Clock
	mu      sync.Mutex
	cancels int
}

func (c *cancelSpy) AfterFunc(d time.Duration, label string, fn func()) engine.Cancel {
	cancel := c.Clock.AfterFunc(d, label, fn)
	return func() bool {
		c.mu.Lock()
		c.cancels++
		c.mu.Unlock()
		return cancel()
	}
}

// OVL-700: after Stop no tick runs and the pending timer was cancelled.
func TestSamplerStop(t *testing.T) {
	feed := &metricFeed{live: []uint64{10}, limit: 1000}
	t.Cleanup(overload.SetReadMetrics(feed.read))
	eng := enginetest.New("n", 1)
	spy := &cancelSpy{Clock: eng.Engine().Clock}
	g := overload.NewGate()
	g.Bind(func(engine.AdmissionChange) {}, nil)
	s := overload.NewSampler(g, spy, 0, 0, false, nil, nil)

	s.Start()
	eng.Advance(tick)
	if feed.calls != 2 {
		t.Fatalf("metrics read %d times before Stop; want 2", feed.calls)
	}
	s.Stop()
	if spy.cancels != 1 {
		t.Errorf("Stop cancelled %d timers; want 1", spy.cancels)
	}
	eng.Advance(10 * tick)
	if feed.calls != 2 {
		t.Errorf("metrics read %d times after Stop; want still 2", feed.calls)
	}
	if got := len(eng.Labels()); got != 2 {
		t.Errorf("timer armed %d times; want 2: no re-arm after Stop", got)
	}
	s.Stop() // idempotent
	s.Start()
	if feed.calls != 2 {
		t.Errorf("Start after Stop read the metrics; calls = %d", feed.calls)
	}
}

// The thresholds-in-force state that the admission_enabled gauge of OBS §5.5 reads (OVL-310): on
// while a sample finds thresholds, off before the first sample, without a memory limit, while a
// metric reads KindBad (OVL-330) and with admission off.
func TestSamplerEnabled(t *testing.T) {
	t.Run("limit", func(t *testing.T) {
		feed := &metricFeed{live: []uint64{10}, limit: math.MaxInt64}
		r := newRig(t, feed, 0, 0, false, nil)
		if r.sampler.Enabled() {
			t.Fatal("Enabled before Start; want false")
		}
		r.sampler.Start()
		t.Cleanup(r.sampler.Stop)
		if r.sampler.Enabled() {
			t.Fatal("Enabled with no memory limit; want false")
		}
		feed.limit = 1000
		r.eng.Advance(tick)
		if !r.sampler.Enabled() {
			t.Fatal("not Enabled with a memory limit of 1000; want true")
		}
		feed.bad = true
		r.eng.Advance(tick)
		if r.sampler.Enabled() {
			t.Fatal("Enabled while the live heap reads KindBad; want false")
		}
		feed.bad = false
		r.eng.Advance(tick)
		if !r.sampler.Enabled() {
			t.Fatal("not Enabled once the live heap reads again; want true")
		}
		feed.limit = math.MaxInt64
		r.eng.Advance(tick)
		if r.sampler.Enabled() {
			t.Fatal("Enabled after the memory limit was removed; want false")
		}
	})
	t.Run("explicit", func(t *testing.T) {
		feed := &metricFeed{live: []uint64{10}, limit: math.MaxInt64}
		r := newRig(t, feed, 2000, 1000, true, nil)
		r.sampler.Start()
		t.Cleanup(r.sampler.Stop)
		if !r.sampler.Enabled() {
			t.Fatal("not Enabled with WithAdmission(2000, 1000) and no limit; want true")
		}
	})
	t.Run("off", func(t *testing.T) {
		feed := &metricFeed{live: []uint64{10}, limit: 1000}
		r := newRig(t, feed, 0, 0, true, nil)
		r.sampler.Start()
		t.Cleanup(r.sampler.Stop)
		r.eng.Advance(tick)
		if r.sampler.Enabled() {
			t.Fatal("Enabled with WithAdmission(0, 0); want false")
		}
	})
}
