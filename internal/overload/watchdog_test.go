// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload_test

import (
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep/internal/overload"
)

// fakeTurns is a watchdog's running function over a fixed set of turns. It counts its calls
// (polls). Turns in stuck are reported as they are; each worker in fresh is reported with a turn
// that starts at the poll, so it is never stuck.
type fakeTurns struct {
	mu    sync.Mutex
	stuck []overload.Turn
	fresh []int
	polls int
}

func (f *fakeTurns) running(dst []overload.Turn) []overload.Turn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls++
	dst = append(dst, f.stuck...)
	for _, w := range f.fresh {
		dst = append(dst, overload.Turn{Worker: w, GoID: 1, Start: time.Now()})
	}
	return dst
}

func (f *fakeTurns) pollCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.polls
}

// setStart gives the stuck turn of worker a new start: the worker runs a new turn.
func (f *fakeTurns) setStart(worker int, start time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.stuck {
		if f.stuck[i].Worker == worker {
			f.stuck[i].Start = start
		}
	}
}

// stuckReport is one onStuck call.
type stuckReport struct {
	turn    overload.Turn
	elapsed time.Duration
	stack   string
}

// reportLog collects onStuck calls. It is safe for concurrent use.
type reportLog struct {
	mu   sync.Mutex
	list []stuckReport
}

func (r *reportLog) onStuck(t overload.Turn, elapsed time.Duration, stack string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, stuckReport{t, elapsed, stack})
}

func (r *reportLog) reports() []stuckReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]stuckReport(nil), r.list...)
}

// waitFor waits up to 10 s until cond holds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 10 s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// returnsWithin fails the test unless fn returns within 10 s.
func returnsWithin(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return within 10 s", what)
	}
}

// parkedWorker stands in for a worker stuck in a turn: it sends its goroutine ID and blocks until
// release is closed.
func parkedWorker(id chan<- uint64, release <-chan struct{}) {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	f := strings.Fields(string(buf)) // "goroutine <id> [running]:"
	n, err := strconv.ParseUint(f[1], 10, 64)
	if err != nil {
		panic(err)
	}
	id <- n
	<-release
}

// startParkedWorker starts a parkedWorker that the test releases when it ends and returns its
// goroutine ID.
func startParkedWorker(t *testing.T) uint64 {
	t.Helper()
	id, release, exited := make(chan uint64, 1), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(exited)
		parkedWorker(id, release)
	}()
	t.Cleanup(func() {
		close(release)
		<-exited
	})
	return <-id
}

// OVL-500, OVL-510: a turn running past the timeout is reported once, however many polls see
// it, with its running time and the stack of its worker's goroutine; a turn younger than the
// timeout is not reported; a new turn on the same worker (a new Start) is reported again.
func TestWatchdogReportsOncePerTurn(t *testing.T) {
	goid := startParkedWorker(t)
	start := time.Now().Add(-time.Hour)
	f := &fakeTurns{stuck: []overload.Turn{{Worker: 1, GoID: goid, Start: start}}, fresh: []int{0}}
	var r reportLog
	w := overload.NewWatchdog(50*time.Millisecond, f.running, r.onStuck) // polls every 10 ms
	w.Start()
	t.Cleanup(w.Stop)

	waitFor(t, "10 polls", func() bool { return f.pollCount() >= 10 })
	got := r.reports()
	if len(got) != 1 {
		t.Fatalf("%d reports after 10 polls of one stuck turn, want 1", len(got))
	}
	rep := got[0]
	if rep.turn.Worker != 1 || rep.turn.GoID != goid || !rep.turn.Start.Equal(start) {
		t.Errorf("reported turn %+v, want worker 1, goroutine %d, start %v", rep.turn, goid, start)
	}
	if rep.elapsed < time.Hour {
		t.Errorf("elapsed = %v, want at least 1h", rep.elapsed)
	}
	if head := "goroutine " + strconv.FormatUint(goid, 10) + " ["; !strings.HasPrefix(rep.stack, head) {
		t.Errorf("stack does not start with %q:\n%s", head, rep.stack)
	}
	if !strings.Contains(rep.stack, "overload_test.parkedWorker") {
		t.Errorf("stack does not hold the worker's function:\n%s", rep.stack)
	}

	again := start.Add(time.Minute)
	f.setStart(1, again)
	waitFor(t, "the report of the new turn", func() bool { return len(r.reports()) == 2 })
	polls := f.pollCount()
	waitFor(t, "10 more polls", func() bool { return f.pollCount() >= polls+10 })
	got = r.reports()
	if len(got) != 2 {
		t.Fatalf("%d reports for two stuck turns, want 2", len(got))
	}
	if !got[1].turn.Start.Equal(again) {
		t.Errorf("second report's start = %v, want %v", got[1].turn.Start, again)
	}
}

// OVL-700: Stop returns after the polling goroutine has exited, including while onStuck runs;
// Stop without Start, a second Stop and a Start after Stop start nothing and do not block.
func TestWatchdogStopJoins(t *testing.T) {
	t.Run("never started", func(t *testing.T) {
		w := overload.NewWatchdog(time.Second, (&fakeTurns{}).running, (&reportLog{}).onStuck)
		returnsWithin(t, "Stop without Start", w.Stop)
		w.Start()
		goleak.VerifyNone(t)
	})

	t.Run("polling", func(t *testing.T) {
		f := &fakeTurns{}
		w := overload.NewWatchdog(time.Millisecond, f.running, (&reportLog{}).onStuck)
		w.Start()
		waitFor(t, "2 polls", func() bool { return f.pollCount() >= 2 })
		returnsWithin(t, "Stop", w.Stop)
		goleak.VerifyNone(t)
		returnsWithin(t, "a second Stop", w.Stop)
		w.Start()
		goleak.VerifyNone(t)
	})

	t.Run("reporting", func(t *testing.T) {
		f := &fakeTurns{stuck: []overload.Turn{{Worker: 0, GoID: 1, Start: time.Now().Add(-time.Hour)}}}
		entered, release := make(chan struct{}), make(chan struct{})
		w := overload.NewWatchdog(time.Millisecond, f.running, func(overload.Turn, time.Duration, string) {
			close(entered)
			<-release
		})
		w.Start()
		<-entered
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			w.Stop()
		}()
		select {
		case <-stopped:
			t.Fatal("Stop returned while onStuck was running")
		case <-time.After(50 * time.Millisecond):
		}
		close(release)
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Fatal("Stop did not return within 10 s of onStuck's return")
		}
		goleak.VerifyNone(t)
	})
}

// dumpOf returns a stack function that serves dump like runtime.Stack: as much of it as fits in
// buf. It counts its calls.
func dumpOf(dump string, calls *int) func([]byte, bool) int {
	return func(buf []byte, all bool) int {
		if !all {
			panic("GoroutineStack asked for one goroutine")
		}
		*calls++
		return copy(buf, dump)
	}
}

// OVL-510: GoroutineStack cuts the section of one goroutine out of the all-goroutines dump, from
// its header to the end of its last line; a goroutine missing from the dump has an empty section.
func TestGoroutineStackSection(t *testing.T) {
	const (
		g1   = "goroutine 1 [running]:\nmain.main()\n\t/m.go:1 +0x1"
		g17  = "goroutine 17 [chan receive]:\nfoo.bar()\n\t/f.go:2 +0x2\ncreated by foo.baz in goroutine 1\n\t/f.go:3 +0x3"
		g170 = "goroutine 170 [select]:\nqux.wait()\n\t/q.go:4 +0x4"
	)
	dump := g1 + "\n\n" + g17 + "\n\n" + g170 + "\n"
	var calls int
	t.Cleanup(overload.SetStack(dumpOf(dump, &calls)))
	for _, c := range []struct {
		goid uint64
		want string
	}{{1, g1}, {17, g17}, {170, g170}, {7, ""}} {
		if got := overload.GoroutineStack(c.goid); got != c.want {
			t.Errorf("GoroutineStack(%d) = %q, want %q", c.goid, got, c.want)
		}
	}
}

// OVL-510: a dump larger than the first buffer is read again into a larger one, and the next
// dump starts at that size; a section longer than 64 KiB is cut at the last line end within its
// first 64 KiB and followed by "...(truncated)"; a section of exactly 64 KiB is kept whole.
func TestGoroutineStackLimit(t *testing.T) {
	const limit = 64 << 10
	// section returns the section of goroutine id: its header and frame lines of 100 bytes each,
	// padded with a last line so that it is exactly n bytes long.
	section := func(id string, n int) string {
		var b strings.Builder
		b.WriteString("goroutine " + id + " [select]:\n")
		frame := "\tpkg.frame()" + strings.Repeat(" ", 100-len("\tpkg.frame()\n")) + "\n"
		for b.Len()+len(frame) < n-1 {
			b.WriteString(frame)
		}
		b.WriteString(strings.Repeat("x", n-b.Len()))
		return b.String()
	}
	whole := section("5", limit)
	long := section("9", 3*limit)
	dump := section("1", 2*limit) + "\n\n" + whole + "\n\n" + long + "\n"
	var calls int
	t.Cleanup(overload.SetStack(dumpOf(dump, &calls)))

	if got := overload.GoroutineStack(5); got != whole {
		t.Errorf("a section of exactly 64 KiB was changed: %d bytes, ends %q", len(got), got[max(len(got)-40, 0):])
	}
	if calls < 2 {
		t.Errorf("the dump of %d bytes was read %d times, want a second read into a larger buffer", len(dump), calls)
	}

	calls = 0
	got := overload.GoroutineStack(9)
	if calls != 1 {
		t.Errorf("the same dump was read %d times, want once into a buffer of the size that held it", calls)
	}
	kept, ok := strings.CutSuffix(got, "...(truncated)")
	if !ok {
		t.Fatalf("a section of %d bytes does not end with ...(truncated): ends %q", len(long), got[max(len(got)-40, 0):])
	}
	if len(kept) > limit || !strings.HasSuffix(kept, "\n") || !strings.HasPrefix(long, kept) {
		t.Fatalf("kept %d bytes, want a prefix of the section of at most %d bytes ending at a line end", len(kept), limit)
	}
	if next := strings.IndexByte(long[len(kept):], '\n'); len(kept)+next+1 <= limit {
		t.Errorf("kept %d bytes, but the line ending at byte %d also fits in 64 KiB", len(kept), len(kept)+next+1)
	}
}

// OVL-510: the buffer GoroutineStack starts from follows the size of the last dump, so it shrinks
// after a spike instead of staying at the largest size ever needed.
func TestGoroutineStackHintShrinks(t *testing.T) {
	const limit = 64 << 10
	big := "goroutine 1 [running]:\n" + strings.Repeat("x", 5*limit)
	small := "goroutine 1 [running]:\nmain.main()"
	dump := big
	var first int // the buffer size of the first read of each GoroutineStack call
	var reads int
	t.Cleanup(overload.SetStack(func(buf []byte, all bool) int {
		if reads == 0 {
			first = len(buf)
		}
		reads++
		return copy(buf, dump)
	}))

	overload.GoroutineStack(1) // grows to fit the spike
	reads = 0
	overload.GoroutineStack(1)
	if first < len(big) {
		t.Fatalf("a repeated dump of %d bytes started from a buffer of %d bytes; want one that holds it", len(big), first)
	}

	dump = small
	reads = 0
	overload.GoroutineStack(1) // the dump has shrunk
	reads = 0
	overload.GoroutineStack(1)
	if first != limit {
		t.Errorf("after the dump shrank to %d bytes the buffer is %d bytes; want %d", len(small), first, limit)
	}
}

// OVL-510: without a test hook GoroutineStack reads the runtime's dump.
func TestGoroutineStackRuntime(t *testing.T) {
	goid := startParkedWorker(t)
	got := overload.GoroutineStack(goid)
	if !strings.HasPrefix(got, "goroutine "+strconv.FormatUint(goid, 10)+" [") ||
		!strings.Contains(got, "overload_test.parkedWorker") {
		t.Errorf("GoroutineStack(%d) = %q, want the parked goroutine's section", goid, got)
	}
}
