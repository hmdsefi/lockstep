// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"math"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/overload"
	"github.com/hmdsefi/lockstep/internal/prodengine"
)

// noMemoryLimit is what /gc/gomemlimit:bytes reads when no Go memory limit is set.
const noMemoryLimit = math.MaxInt64

// sampleLabel is the label of the sampler's timer (OVL-300).
const sampleLabel = "lockstep/overload-sample"

// memFeed is a read hook for the sampler (OVL §10). It serves the live heap and the memory limit
// the test stores, or, when real is set, the runtime's own values, and counts its calls. The
// sampler calls it on New's goroutine and on workers, so its fields are atomic. A test can make
// the next call block until it releases it.
type memFeed struct {
	live, limit atomic.Uint64
	real        bool
	calls       atomic.Int64

	block   atomic.Bool   // the next call blocks
	entered chan struct{} // receives once a blocked call has started
	release chan struct{} // closed by unblock to let the blocked call return
	once    sync.Once
}

// unblock disarms the block and lets a blocked call return. It may be called more than once.
func (f *memFeed) unblock() {
	f.block.Store(false)
	f.once.Do(func() { close(f.release) })
}

// newMemFeed installs a feed serving live and limit as the sampler's read hook until the test
// ends. Register it before the system is built, so the hook is restored after the system shuts
// down.
func newMemFeed(t *testing.T, live, limit uint64) *memFeed {
	t.Helper()
	f := &memFeed{entered: make(chan struct{}, 1), release: make(chan struct{})}
	f.live.Store(live)
	f.limit.Store(limit)
	t.Cleanup(overload.SetReadMetrics(f.read))
	return f
}

func (f *memFeed) read(samples []metrics.Sample) {
	f.calls.Add(1)
	if f.block.CompareAndSwap(true, false) {
		f.entered <- struct{}{}
		<-f.release
	}
	if f.real {
		metrics.Read(samples)
		return
	}
	for i := range samples {
		switch samples[i].Name {
		case "/gc/heap/live:bytes":
			samples[i].Value = overload.Uint64Value(f.live.Load())
		case "/gc/gomemlimit:bytes":
			samples[i].Value = overload.Uint64Value(f.limit.Load())
		}
	}
}

// waitCalls waits up to 10 s until the feed has been read at least n times.
func (f *memFeed) waitCalls(t *testing.T, n int64) {
	t.Helper()
	waitUntil(t, "metrics reads", func() bool { return f.calls.Load() >= n })
}

// verifyNoneAtEnd checks with goleak, once every other cleanup of the test has run, that no
// goroutine is left over (OVL-700).
func verifyNoneAtEnd(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { goleak.VerifyNone(t) })
}

// heapLive returns the live heap in bytes after a garbage collection, read with runtime/metrics.
// The collection drops the garbage of earlier tests, which would inflate the reading.
func heapLive(t *testing.T) uint64 {
	t.Helper()
	runtime.GC()
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindUint64 {
		t.Fatalf("metric %s has kind %v", s[0].Name, s[0].Value.Kind())
	}
	return s[0].Value.Uint64()
}

// wantMemoryRefusal fails the test unless err is OVL-100's refusal of a top-level spawn because
// the memory source is on.
func wantMemoryRefusal(t *testing.T, err error) {
	t.Helper()
	const want = "lockstep: spawn in /user: overloaded (memory)"
	if err == nil || !errors.Is(err, ErrOverloaded) || err.Error() != want {
		t.Fatalf("Spawn = %v, want ErrOverloaded with text %q", err, want)
	}
}

// overloadConfig returns the configuration New would build from opts for node local.
func overloadConfig(t *testing.T, opts ...Option) *systemConfig {
	t.Helper()
	cfg, err := newConfig(opts, "local")
	if err != nil {
		t.Fatalf("newConfig: %v", err)
	}
	return cfg
}

// sampleClock wraps an engine clock and keeps the sampler's timers that are pending: armed, and
// neither started nor cancelled through their own Cancel. A timer that the engine's Stop drops
// stays pending here, so a count of 0 after Shutdown means the sampler cancelled its own tick.
type sampleClock struct {
	engine.Clock
	mu      sync.Mutex
	pending map[*struct{}]bool
}

func (c *sampleClock) AfterFunc(d time.Duration, label string, fn func()) engine.Cancel {
	if label != sampleLabel {
		return c.Clock.AfterFunc(d, label, fn)
	}
	id := new(struct{})
	c.mu.Lock()
	c.pending[id] = true
	c.mu.Unlock()
	cancel := c.Clock.AfterFunc(d, label, func() {
		c.done(id)
		fn()
	})
	return func() bool {
		ok := cancel()
		if ok {
			c.done(id)
		}
		return ok
	}
}

func (c *sampleClock) done(id *struct{}) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

// pendingTicks returns the number of pending sampler timers.
func (c *sampleClock) pendingTicks() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// newSampledSystem returns a system built through the ACT-015 constructor on the bundle of a new
// production engine with workers workers whose Admission is a Gate, with edit applied to the
// bundle, and starts its overload machinery as New does, with opts. The system shuts down when
// the test ends.
func newSampledSystem(t *testing.T, workers int, edit func(*engine.Engine), opts ...Option) (*System, func()) {
	t.Helper()
	gate := overload.NewGate()
	sys, pe := newProdSystemWith(t, prodengine.Config{Node: "local", Workers: workers, Admission: gate}, edit, opts...)
	return sys, func() { sys.startOverload(pe, gate, overloadConfig(t, opts...)) }
}

// slogRecord is one captured log record: level, message and every attribute, the logger's own
// attributes first.
type slogRecord struct {
	level slog.Level
	msg   string
	attrs []slog.Attr
}

// slogCapture is a slog.Handler that keeps every record. It is safe for concurrent use.
type slogCapture struct {
	mu   *sync.Mutex
	recs *[]slogRecord
	pre  []slog.Attr
}

func (*slogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *slogCapture) Handle(_ context.Context, r slog.Record) error {
	rec := slogRecord{level: r.Level, msg: r.Message, attrs: slices.Clone(c.pre)}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs = append(rec.attrs, a)
		return true
	})
	c.mu.Lock()
	*c.recs = append(*c.recs, rec)
	c.mu.Unlock()
	return nil
}

func (c *slogCapture) WithAttrs(as []slog.Attr) slog.Handler {
	return &slogCapture{mu: c.mu, recs: c.recs, pre: append(slices.Clone(c.pre), as...)}
}

func (c *slogCapture) WithGroup(string) slog.Handler { return c }

// captureDefaultLogger makes slog's default logger one that captures every record, until the test
// ends, and returns a function that lists the records so far. A system takes the default logger
// when it is built, so call it before New and before the system's cleanup is registered.
// slog.SetDefault with a handler of its own also sends the log package's output to that handler
// and clears its flags, and setting the old logger back does not undo that, so the cleanup also
// restores the log package's writer and flags.
func captureDefaultLogger(t *testing.T) func() []slogRecord {
	t.Helper()
	c := &slogCapture{mu: new(sync.Mutex), recs: new([]slogRecord)}
	old, oldWriter, oldFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() {
		slog.SetDefault(old)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})
	return func() []slogRecord {
		c.mu.Lock()
		defer c.mu.Unlock()
		return slices.Clone(*c.recs)
	}
}

// countMetrics is a Metrics that keeps the total of each counter series, by name and by its labels
// after node, and ignores the gauges.
type countMetrics struct {
	mu     sync.Mutex
	series map[string]*countSeries
}

// countSeries is one counter series of a countMetrics.
type countSeries struct{ n atomic.Uint64 }

func (c *countSeries) Add(n uint64) { c.n.Add(n) }

// seriesKey returns the key of the series name with the labels kv, name and value pairs after node.
func seriesKey(name string, kv ...string) string {
	return name + "|" + strings.Join(kv, ",")
}

func (m *countMetrics) Counter(name, _ string, labels ...MetricLabel) Counter {
	var kv []string
	for _, l := range labels[1:] { // node comes first
		kv = append(kv, l.Name, l.Value)
	}
	c := &countSeries{}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.series == nil {
		m.series = make(map[string]*countSeries)
	}
	m.series[seriesKey(name, kv...)] = c
	return c
}

func (*countMetrics) GaugeFunc(string, string, func() int64, ...MetricLabel) {}

// count returns the total of the counter series name with the labels kv after node, or 0 if there
// is no such series.
func (m *countMetrics) count(name string, kv ...string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c := m.series[seriesKey(name, kv...)]; c != nil {
		return c.n.Load()
	}
	return 0
}

// AT-OVL-27: OVL-300. New takes the first sample before it returns, so a spawn right after New
// is refused when the live heap is above the high mark of the memory limit.
func TestATOVL27FirstSampleBeforeNew(t *testing.T) {
	verifyNoneAtEnd(t)
	newMemFeed(t, 950, 1000) // high = 1000 - 1000/10 = 900
	sys := newLocalSystem(t)
	_, err := Spawn(sys, "a", idle[int]())
	wantMemoryRefusal(t, err)
}

// AT-OVL-28: OVL-310. Without WithAdmission and without a Go memory limit the memory source is
// off; once a limit of half the live heap is set, a later sample turns it on and spawns are
// refused. The test restores the limit.
func TestATOVL28RealMemoryLimit(t *testing.T) {
	verifyNoneAtEnd(t)
	old := debug.SetMemoryLimit(noMemoryLimit)
	sys := newLocalSystem(t)
	t.Cleanup(func() { debug.SetMemoryLimit(old) }) // before the shutdown, which a low limit slows
	if _, err := Spawn(sys, "first", idle[int]()); err != nil {
		t.Fatalf("Spawn with no memory limit = %v, want nil", err)
	}

	debug.SetMemoryLimit(int64(max(heapLive(t)/2, 1))) //nolint:gosec // half a heap size is far below math.MaxInt64
	waitUntil(t, "memory source on after the limit fell below the live heap", sys.shedding)
	_, err := Spawn(sys, "second", idle[int]())
	wantMemoryRefusal(t, err)
	debug.SetMemoryLimit(old)
}

// AT-OVL-30: OVL-300. WithAdmission(0, 0) turns memory admission off: with a Go memory limit of
// half the live heap a spawn is admitted, and the sampler never reads the metrics, neither in
// New nor on its ticks.
func TestATOVL30AdmissionOff(t *testing.T) {
	verifyNoneAtEnd(t)
	feed := newMemFeed(t, 0, 0)
	feed.real = true
	old := debug.SetMemoryLimit(int64(max(heapLive(t)/2, 1))) //nolint:gosec // half a heap size is far below math.MaxInt64
	restore := func() { debug.SetMemoryLimit(old) }
	t.Cleanup(restore) // whatever happens next
	sys := newLocalSystem(t, WithAdmission(0, 0))
	t.Cleanup(restore) // before the shutdown, which a low limit slows
	if _, err := Spawn(sys, "a", idle[int]()); err != nil {
		t.Fatalf("Spawn = %v, want nil", err)
	}
	time.Sleep(250 * time.Millisecond) // two ticks of 100 ms
	if _, err := Spawn(sys, "b", idle[int]()); err != nil {
		t.Fatalf("Spawn after two ticks = %v, want nil", err)
	}
	if n := feed.calls.Load(); n != 0 {
		t.Errorf("metrics read %d times, want never", n)
	}
}

// AT-OVL-32: OVL-340. A memory change is logged through the system's logger, slog's default when
// New runs, with the node: one Warn "admission refusing" when the source turns on and one Info
// "admission resumed" when it turns off, with OBS-060's attributes and no others. The system's
// other events, such as the shutdown's, are left out. Each change adds one to
// lockstep_admission_transitions_total with the label to of its new state (OBS-120).
func TestATOVL32AdmissionLogs(t *testing.T) {
	verifyNoneAtEnd(t)
	writer, flags := log.Writer(), log.Flags()
	t.Cleanup(func() { // runs after captureDefaultLogger's cleanup
		if log.Writer() != writer || log.Flags() != flags {
			t.Errorf("the log package writes to %T with flags %d after the test, want %T with flags %d",
				log.Writer(), log.Flags(), writer, flags)
		}
	})
	records := captureDefaultLogger(t)
	feed := newMemFeed(t, 2100, noMemoryLimit)
	counts := &countMetrics{}
	sys := newLocalSystem(t, WithNodeName("n1"), WithAdmission(2000, 1000), WithMetrics(counts))
	if !sys.shedding() {
		t.Fatal("the system admits after New with a live heap of 2100 over the high mark 2000")
	}
	feed.live.Store(900)
	waitUntil(t, "memory source off at 900", func() bool { return !sys.shedding() })
	waitUntil(t, "the transition to admitting counted", func() bool {
		return counts.count("lockstep_admission_transitions_total", "to", "admitting") > 0
	})
	for to, want := range map[string]uint64{"refusing": 1, "admitting": 1} {
		if got := counts.count("lockstep_admission_transitions_total", "to", to); got != want {
			t.Errorf("lockstep_admission_transitions_total{to=%q} = %d, want %d", to, got, want)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}

	want := []slogRecord{
		{slog.LevelWarn, "admission refusing", []slog.Attr{
			slog.String("node", "n1"), slog.Uint64("heap_live_bytes", 2100), slog.Uint64("high_mark_bytes", 2000)}},
		{slog.LevelInfo, "admission resumed", []slog.Attr{
			slog.String("node", "n1"), slog.Uint64("heap_live_bytes", 900), slog.Uint64("low_mark_bytes", 1000)}},
	}
	var got []slogRecord
	for _, r := range records() {
		if r.msg == "admission refusing" || r.msg == "admission resumed" {
			got = append(got, r)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("logged %d admission records %+v, want %d", len(got), got, len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.level != w.level || g.msg != w.msg {
			t.Errorf("record %d = %v %q, want %v %q", i, g.level, g.msg, w.level, w.msg)
		}
		if !slices.EqualFunc(g.attrs, w.attrs, slog.Attr.Equal) {
			t.Errorf("record %d attributes = %v, want %v", i, g.attrs, w.attrs)
		}
	}
}

// goroutineWarnings returns the records of OBS-060's "Goroutines above bound" event.
func goroutineWarnings(recs []slogRecord) []slogRecord {
	return slices.DeleteFunc(slices.Clone(recs), func(r slogRecord) bool { return r.msg != "goroutine count above bound" })
}

// OVL-600, OVL-610, OVL-620: New starts a goroutine monitor with the bound of WithGoroutineWarning
// that logs through the system's logger, slog's default when New runs, with the node. A count
// above the bound that stays there is warned about once, on the tenth tick.
func TestGoroutineWarningInNew(t *testing.T) {
	verifyNoneAtEnd(t)
	records := captureDefaultLogger(t)
	counts := &atomic.Int64{}
	counts.Store(800)
	t.Cleanup(overload.SetNumGoroutine(func() int { return int(counts.Load()) }))
	t.Cleanup(overload.SetGoroutineProfile(func(w io.Writer) error {
		_, err := io.WriteString(w, "goroutine profile: total 800\n800 @ 0x1\n#\t0x1\tmain.busy+0x1\t/src/main.go:1\n")
		return err
	}))
	sys := newLocalSystem(t, WithNodeName("n1"), WithGoroutineWarning(500))
	waitUntil(t, "the goroutine warning", func() bool { return len(goroutineWarnings(records())) > 0 })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	got := goroutineWarnings(records())
	want := slogRecord{slog.LevelWarn, "goroutine count above bound", []slog.Attr{
		slog.String("node", "n1"), slog.Int("goroutines", 800), slog.Int("bound", 500),
		slog.String("summary", "800 main.busy")}}
	if len(got) != 1 || got[0].level != want.level || got[0].msg != want.msg ||
		!slices.EqualFunc(got[0].attrs, want.attrs, slog.Attr.Equal) {
		t.Errorf("warnings = %+v, want exactly %+v", got, want)
	}
}

// memoryShed returns the lockstep.overload event=shed record of a message from from to to that a
// BlockSender mailbox makes wait while the memory source is on.
func memoryShed(from, to Address) string {
	return "lockstep.overload event=shed from=" + from.String() + " to=" + to.String() +
		" reason=memory policy=block-sender dropped=none"
}

// AT-OVL-46: OVL-120, OVL-150, MBX-075 on the production engine with the memory source fed by the
// sampler. An outside sender to a BlockSender mailbox with room waits while the live heap is over
// the high mark (event=shed dropped=none); a tick that samples a live heap under the low mark
// turns the source off (event=off), SendContext returns nil and the message is delivered. A wait
// whose context ends first returns context.DeadlineExceeded unwrapped.
func TestATOVL46BlockSenderWaitsForAdmission(t *testing.T) {
	t.Run("released", func(t *testing.T) {
		verifyNoneAtEnd(t)
		feed := newMemFeed(t, 2500, noMemoryLimit)
		rec := enginetest.NewRecorder()
		sys, start := newSampledSystem(t, 2, func(e *engine.Engine) { e.Tracer = rec }, WithAdmission(2000, 1000))
		tp, got := spawnCollector(t, sys, "t", 1, BlockSender)
		to := tp.Address()
		start()
		if !sys.shedding() {
			t.Fatal("the system admits after the first sample of 2500 over the high mark 2000")
		}

		before := len(rec.Records())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ch := sendContextAsync(ctx, sys, tp, 1)
		waitingSenders(t, cellOf(t, sys, tp), 1)
		waitRecorded(t, rec, before, memoryShed(sys.user, to))
		select {
		case r := <-ch:
			t.Fatalf("SendContext returned %v while the memory source is on, want it to wait", r.err)
		default:
		}
		feed.live.Store(900)
		if err := sendReturned(t, ch); err != nil {
			t.Errorf("SendContext = %v, want nil", err)
		}
		if m := recv(t, got); m != 1 {
			t.Errorf("the target received %d, want 1", m)
		}
		sameLines(t, "records", offerRecords(rec.Records(), before, to), withoutLen([]string{
			memoryShed(sys.user, to),
			"lockstep.overload event=off reason=memory live=900 high=2000 low=1000 shedding=false",
			"lockstep.deliver from=" + sys.user.String() + " to=" + to.String() + " seq=0 len=1 msg=1",
		}))
	})

	t.Run("deadline", func(t *testing.T) {
		verifyNoneAtEnd(t)
		newMemFeed(t, 2500, noMemoryLimit)
		rec := enginetest.NewRecorder()
		sys, start := newSampledSystem(t, 2, func(e *engine.Engine) { e.Tracer = rec }, WithAdmission(2000, 1000))
		tp := spawnTarget(t, sys, "t", 1, BlockSender)
		to := tp.Address()
		start()

		before := len(rec.Records())
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := SendContext(ctx, sys, tp, 1); err != context.DeadlineExceeded {
			t.Errorf("SendContext = %v, want context.DeadlineExceeded unwrapped", err)
		}
		sameLines(t, "records", offerRecords(rec.Records(), before, to), []string{memoryShed(sys.user, to)})
	})
}

// OVL-700: the sampler's tick is cancelled when Shutdown returns, however the shutdown ends, and
// no goroutine is left: through New; with a tick running on a worker of a multi-worker engine
// while Shutdown stops the engine; and when an actor shuts the system down, so the shutdown
// completes in engine context and the engine stops on a goroutine of its own.
func TestOVL700SamplerStopsAtShutdown(t *testing.T) {
	shutdown := func(t *testing.T, sys *System) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := sys.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown = %v", err)
		}
	}
	// noMoreTicks fails the test if the feed is read again within three ticks.
	noMoreTicks := func(t *testing.T, feed *memFeed) {
		t.Helper()
		n := feed.calls.Load()
		time.Sleep(300 * time.Millisecond)
		if m := feed.calls.Load(); m != n {
			t.Errorf("metrics read %d times after Shutdown returned, want none", m-n)
		}
	}

	t.Run("New", func(t *testing.T) {
		feed := newMemFeed(t, 10, noMemoryLimit)
		sys := newLocalSystem(t, WithWorkers(4))
		feed.waitCalls(t, 3) // the first sample and two ticks
		shutdown(t, sys)
		goleak.VerifyNone(t)
		noMoreTicks(t, feed)
	})

	t.Run("tick in flight", func(t *testing.T) {
		feed := newMemFeed(t, 10, noMemoryLimit)
		var clk *sampleClock
		sys, start := newSampledSystem(t, 4, func(e *engine.Engine) {
			clk = &sampleClock{Clock: e.Clock, pending: map[*struct{}]bool{}}
			e.Clock = clk
		})
		t.Cleanup(feed.unblock) // before the system's cleanup: a failed wait leaves no tick blocked
		start()
		feed.block.Store(true)
		select {
		case <-feed.entered:
		case <-time.After(10 * time.Second):
			t.Fatal("no tick within 10 s")
		}
		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			done <- sys.Shutdown(ctx)
		}()
		// Shutdown cannot return while the tick holds its worker: the engine's Stop waits for it.
		waitUntil(t, "actors stopped", sys.shut.over.Load)
		// Give Shutdown time to reach the engine's Stop. Shutdown cannot return before the tick
		// ends whatever the timing, so a short sleep can only weaken the check below, never fail it.
		time.Sleep(20 * time.Millisecond)
		select {
		case err := <-done:
			t.Fatalf("Shutdown returned %v while a tick was running", err)
		default:
		}
		feed.unblock()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Shutdown = %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Shutdown did not return within 10 s of the tick's end")
		}
		if n := clk.pendingTicks(); n != 0 {
			t.Errorf("%d sampler ticks pending after Shutdown, want 0", n)
		}
		goleak.VerifyNone(t)
	})

	t.Run("engine context", func(t *testing.T) {
		feed := newMemFeed(t, 10, noMemoryLimit)
		var clk *sampleClock
		sys, start := newSampledSystem(t, 2, func(e *engine.Engine) {
			clk = &sampleClock{Clock: e.Clock, pending: map[*struct{}]bool{}}
			e.Clock = clk
		})
		returned := make(chan error, 1)
		closer, err := Spawn(sys, "closer", actorOf[int](&fnActor[int]{receive: func(*Context[int], int) {
			returned <- sys.Shutdown(context.Background())
		}}))
		if err != nil {
			t.Fatal(err)
		}
		start()
		feed.waitCalls(t, 3)
		send(t, sys, closer, 1)
		if err := recv(t, returned); err != nil {
			t.Fatalf("Shutdown during a turn = %v, want nil", err)
		}
		waitUntil(t, "engine stopped", sys.shut.engStopped.Load)
		if n := clk.pendingTicks(); n != 0 {
			t.Errorf("%d sampler ticks pending after the engine stopped, want 0", n)
		}
		goleak.VerifyNone(t)
		noMoreTicks(t, feed)
	})
}
