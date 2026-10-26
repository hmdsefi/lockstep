// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload_test

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/overload"
)

// defaultBound is lockstep.DefaultGoroutineWarning.
const defaultBound = 10_000

// goroutineFeed is a NumGoroutine hook that serves the next count on each call and counts its calls.
type goroutineFeed struct {
	counts []int
	calls  atomic.Int64
}

func (f *goroutineFeed) read() int {
	i := int(f.calls.Add(1)) - 1
	return f.counts[min(i, len(f.counts)-1)]
}

// installGoroutines installs a feed serving counts as the monitor's goroutine count until the test ends.
func installGoroutines(t *testing.T, counts ...int) *goroutineFeed {
	t.Helper()
	f := &goroutineFeed{counts: counts}
	t.Cleanup(overload.SetNumGoroutine(f.read))
	return f
}

// installProfile installs a goroutine profile hook that writes text, until the test ends.
func installProfile(t *testing.T, text string) {
	t.Helper()
	t.Cleanup(overload.SetGoroutineProfile(func(w io.Writer) error {
		_, err := io.WriteString(w, text)
		return err
	}))
}

// wantWarning fails the test unless rec is OBS-060's "Goroutines above bound" event with the node
// attribute first, then goroutines, bound and summary, in that order and with those types.
func wantWarning(t *testing.T, rec record, goroutines, bound int, summary string) {
	t.Helper()
	want := []slog.Attr{
		slog.String("node", "n1"), slog.Int("goroutines", goroutines), slog.Int("bound", bound),
		slog.String("summary", summary),
	}
	if rec.level != slog.LevelWarn || rec.msg != "goroutine count above bound" || !reflect.DeepEqual(rec.attrs, want) {
		t.Errorf("record = %v %q %v; want WARN %q %v", rec.level, rec.msg, rec.attrs, "goroutine count above bound", want)
	}
}

// AT-OVL-42: a sample at or above the bound warns once and disarms; a sample below bound - bound/5
// re-arms.
func TestATOVL42MonitorWarnsAndRearms(t *testing.T) {
	counts := []int{9999, 10000, 10500, 7999, 8000, 7999, 10000}
	installGoroutines(t, counts...)
	installProfile(t, "goroutine profile: total 1\n")
	c, records := newCapture()
	m := overload.NewMonitor(defaultBound, slog.New(c).With("node", "n1"))

	var warned []int // the samples after which a warning had been logged
	for i := range counts {
		m.Sample()
		if n := len(records()); n > len(warned) {
			warned = append(warned, i+1)
		}
	}
	if want := []int{2, 7}; !reflect.DeepEqual(warned, want) {
		t.Fatalf("warnings at samples %v; want %v", warned, want)
	}
	recs := records()
	if len(recs) != 2 {
		t.Fatalf("%d records; want 2", len(recs))
	}
	wantWarning(t, recs[0], 10000, 10000, "")
	wantWarning(t, recs[1], 10000, 10000, "")
}

// OVL-610: the warning carries the sample that crossed the bound, not the bound.
func TestMonitorWarningCarriesTheSample(t *testing.T) {
	installGoroutines(t, 10500)
	installProfile(t, "")
	c, records := newCapture()
	overload.NewMonitor(10000, slog.New(c).With("node", "n1")).Sample()
	recs := records()
	if len(recs) != 1 {
		t.Fatalf("%d records; want 1", len(recs))
	}
	wantWarning(t, recs[0], 10500, 10000, "")
}

// profileOf12 is a debug-1 goroutine profile of twelve functions: one summed over two records,
// two tied at 5, one upper-case name that sorts before the lower-case ones, labels lines, frames
// that must not be credited (second frames) and a header.
const profileOf12 = `goroutine profile: total 38
4 @ 0x1 0x2
#	0x1	main.alpha+0x1d	/src/main.go:10
#	0x2	main.run+0x2	/src/main.go:20

3 @ 0x3 0x4
# labels: {"role":"worker"}
#	0x3	main.alpha+0x40	/src/main.go:11
#	0x4	main.run+0x2	/src/main.go:20

9 @ 0x5 0x6
#	0x5	main.bravo+0x10	/src/main.go:30
#	0x6	main.alpha+0x7	/src/main.go:12

5 @ 0x7
#	0x7	main.(*server).charlie.func1+0x9	/src/main.go:40

5 @ 0x8
#	0x8	main.delta+0x1	/src/main.go:50

3 @ 0x9
#	0x9	main.echo+0x1	/src/main.go:60

3 @ 0xa
#	0xa	main.foxtrot[...]+0x1	/src/main.go:70

2 @ 0xb
#	0xb	main.golf+0x1	/src/main.go:80

2 @ 0xc
#	0xc	main.hotel+0x1	/src/main.go:90

1 @ 0xd
#	0xd	main.india+0x1	/src/main.go:100

1 @ 0xe
#	0xe	main.juliet+0x1	/src/main.go:110

1 @ 0xf
#	0xf	main.kilo+0x1	/src/main.go:120

1 @ 0x10
#	0x10	main.Zulu+0x1	/src/main.go:130
`

// AT-OVL-43: the summary holds the ten largest functions, each record's count credited to its
// first frame's function, ordered by count, then by name in byte order.
func TestATOVL43ProfileSummary(t *testing.T) {
	installGoroutines(t, 40)
	installProfile(t, profileOf12)
	c, records := newCapture()
	overload.NewMonitor(10, slog.New(c).With("node", "n1")).Sample()
	recs := records()
	if len(recs) != 1 {
		t.Fatalf("%d records; want 1", len(recs))
	}
	const want = "9 main.bravo; 7 main.alpha; 5 main.(*server).charlie.func1; 5 main.delta; " +
		"3 main.echo; 3 main.foxtrot[...]; 2 main.golf; 2 main.hotel; 1 main.Zulu; 1 main.india"
	wantWarning(t, recs[0], 40, 10, want)
}

// parkedGoroutine is the first frame of the goroutines TestMonitorSummaryOfTheRealProfile parks.
func parkedGoroutine(release <-chan struct{}) { <-release }

// OVL-620: the summary parses the text pprof writes at debug 1.
func TestMonitorSummaryOfTheRealProfile(t *testing.T) {
	installGoroutines(t, 1)
	release := make(chan struct{})
	const parked = 7
	for range parked {
		go parkedGoroutine(release)
	}
	t.Cleanup(func() { close(release) })

	// The goroutines show up in the profile once they have parked: sample until they do.
	want := fmt.Sprintf("%d github.com/hmdsefi/lockstep/internal/overload_test.parkedGoroutine", parked)
	var summary string
	for deadline := time.Now().Add(10 * time.Second); !strings.Contains(summary, want); {
		if time.Now().After(deadline) {
			t.Fatalf("summary = %q; want it to contain %q", summary, want)
		}
		time.Sleep(time.Millisecond)
		c, records := newCapture()
		overload.NewMonitor(1, slog.New(c)).Sample()
		recs := records()
		if len(recs) != 1 {
			t.Fatalf("%d records; want 1", len(recs))
		}
		for _, a := range recs[0].attrs {
			if a.Key == "summary" {
				summary = a.Value.String()
			}
		}
	}
	if n := len(strings.Split(summary, "; ")); n > 10 {
		t.Errorf("summary has %d items; want at most 10", n)
	}
}

// A profile that cannot be written does not lose the warning.
func TestMonitorProfileError(t *testing.T) {
	installGoroutines(t, 5)
	t.Cleanup(overload.SetGoroutineProfile(func(io.Writer) error { return errors.New("no profile") }))
	c, records := newCapture()
	overload.NewMonitor(5, slog.New(c).With("node", "n1")).Sample()
	recs := records()
	if len(recs) != 1 {
		t.Fatalf("%d records; want 1", len(recs))
	}
	wantWarning(t, recs[0], 5, 5, "")
}

// OVL-610: a bound of 0 turns the warning off: the monitor never samples the goroutine count.
func TestMonitorOff(t *testing.T) {
	feed := installGoroutines(t, 1_000_000)
	t.Cleanup(overload.SetGoroutineProfile(func(io.Writer) error {
		t.Error("the goroutine profile was read with the warning off")
		return nil
	}))
	c, records := newCapture()
	m := overload.NewMonitor(0, slog.New(c))
	for range 3 {
		m.Sample()
	}
	if n := feed.calls.Load(); n != 0 {
		t.Errorf("runtime.NumGoroutine was sampled %d times; want never", n)
	}
	if n := len(records()); n != 0 {
		t.Errorf("%d records; want none", n)
	}
}

// OVL-600: the sampler calls the monitor on every 10th tick, whether or not admission is on; Start
// is not a tick.
func TestSamplerCallsMonitorEveryTenthTick(t *testing.T) {
	feed := installGoroutines(t, 1)
	eng := enginetest.New("n", 1)
	// WithAdmission(0, 0): the sampler's own sample returns early, the monitor still runs.
	s := overload.NewSampler(overload.NewGate(), eng.Engine().Clock, 0, 0, true, nil, overload.NewMonitor(defaultBound, nil))
	s.Start()
	t.Cleanup(s.Stop)
	if n := feed.calls.Load(); n != 0 {
		t.Fatalf("the monitor sampled %d times in Start; want 0: Start is not a tick", n)
	}
	for tickNo := 1; tickNo <= 20; tickNo++ {
		eng.Advance(tick)
		want := int64(tickNo / 10)
		if n := feed.calls.Load(); n != want {
			t.Fatalf("after %d ticks the monitor sampled %d times; want %d", tickNo, n, want)
		}
	}
}
