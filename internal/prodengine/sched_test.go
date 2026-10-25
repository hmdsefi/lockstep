// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// TestATSCH05ExactlyOnceNeverConcurrent covers SCH-050, SCH-080, SCH-085 and SCH-090
// (AT-SCH-05): every message is processed exactly once and no actor's turns overlap.
func TestATSCH05ExactlyOnceNeverConcurrent(t *testing.T) {
	const (
		actorCount = 1000
		senders    = 8
		perSender  = 10_000
	)
	e := newEngine(t, Config{Node: "n", Workers: 4, Throughput: 8})
	as := fakes(t, actorCount)
	var wg sync.WaitGroup
	for i := range senders {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(i), 0)) //nolint:gosec // G115, G404: a small index seeds a test stream
			for range perSender {
				a := as[r.IntN(actorCount)]
				a.add()
				e.s.Ready(a)
			}
		})
	}
	wg.Wait()

	waitFor(t, 10*time.Second, "all messages to be processed", func() bool {
		var sum int64
		for _, a := range as {
			sum += a.processed.Load()
		}
		return sum == senders*perSender
	})
	waitFor(t, time.Second, "every Slot to be idle", func() bool {
		for _, a := range as {
			if a.slot.State() != engine.SlotIdle {
				return false
			}
		}
		return true
	})
	for _, a := range as {
		if p, n := a.processed.Load(), a.added.Load(); p != n {
			t.Errorf("fake %d processed %d messages, want %d", a.id, p, n)
		}
	}
}

// TestATSCH06ReadyDuringTurn covers SCH-080 and SCH-090 (AT-SCH-06): rounds race a Ready
// against the end of a turn (the running-ready path of ENG-040), and every round completes.
func TestATSCH06ReadyDuringTurn(t *testing.T) {
	const rounds = 100_000
	e := newEngine(t, Config{Node: "n", Workers: 2})
	a := newFake(t, 0)
	for i := range rounds {
		a.add()
		e.s.Ready(a)
		deadline := time.Now().Add(10 * time.Second)
		for a.processed.Load() != a.added.Load() {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: processed %d of %d messages after 10s (Slot %v)",
					i, a.processed.Load(), a.added.Load(), a.slot.State())
			}
			runtime.Gosched()
		}
	}
}

// TestATSCH12ParkedWorkers covers SCH-090 (AT-SCH-12): idle workers park, and park again after
// work.
func TestATSCH12ParkedWorkers(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 4})
	waitFor(t, time.Second, "4 parked workers", func() bool { return e.parked() == 4 })
	a := newFake(t, 0)
	for range 100 {
		a.add()
		e.s.Ready(a)
	}
	waitFor(t, time.Second, "100 processed messages", func() bool { return a.processed.Load() == 100 })
	waitFor(t, time.Second, "4 parked workers after the work", func() bool { return e.parked() == 4 })
}

// TestInTurn covers InTurn of SCH-115: true on a worker inside a turn, false on other goroutines
// and on another engine's worker.
func TestInTurn(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2})
	other := newEngine(t, Config{Node: "o", Workers: 1})
	var inside, otherInside atomic.Int32 // 0 not run, 1 false, 2 true
	record := func(dst *atomic.Int32, s *scheduler) {
		if s.InTurn() {
			dst.Store(2)
		} else {
			dst.Store(1)
		}
	}
	a := newFake(t, 0)
	a.onTurn = func(*fakeActor, int) {
		record(&inside, &e.s)
		record(&otherInside, &other.s)
	}
	a.add()
	e.s.Ready(a)
	waitFor(t, time.Second, "the turn", func() bool { return a.processed.Load() == 1 })
	if got := inside.Load(); got != 2 {
		t.Errorf("InTurn() inside RunTurn: state %d, want 2 (true)", got)
	}
	if got := otherInside.Load(); got != 1 {
		t.Errorf("another engine's InTurn() inside RunTurn: state %d, want 1 (false)", got)
	}
	if e.s.InTurn() {
		t.Error("InTurn() on the test goroutine = true, want false")
	}
}

// TestYieldRunsAgain covers the yield path of SCH-080: a turn that uses up its budget is queued
// again until the actor has no work left.
func TestYieldRunsAgain(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 1, Throughput: 1})
	a := newFake(t, 0)
	for range 100 {
		a.add()
	}
	e.s.Ready(a)
	waitFor(t, time.Second, "100 processed messages", func() bool { return a.processed.Load() == 100 })
	waitFor(t, time.Second, "an idle Slot", func() bool { return a.slot.State() == engine.SlotIdle })
	if n := a.turns.Load(); n != 100 {
		t.Errorf("%d turns with budget 1 for 100 messages, want 100", n)
	}
	if h := a.slot.Hint.Load(); h != 1 {
		t.Errorf("Slot.Hint = %d after turns on worker 0, want 1", h)
	}
}

// TestStopWhileWorking covers the first form of Stop (SCH-200 steps 1, 3 and 4): Stop returns
// while workers run, choose and park, and afterwards Ready does nothing.
func TestStopWhileWorking(t *testing.T) {
	for i := range 200 {
		e := New(Config{Node: "n", Workers: 4})
		as := fakes(t, 8)
		for j := range i % 16 {
			a := as[j%len(as)]
			a.add()
			e.s.Ready(a)
		}
		e.Stop()
		e.Stop() // idempotent
		if p := e.parked(); p != 0 {
			t.Fatalf("parked() = %d after Stop, want 0", p)
		}
	}
	waitFor(t, time.Second, "workers to exit", func() bool { return workerGoroutines() == 0 })

	e := New(Config{Node: "n", Workers: 2})
	e.Stop()
	a := newFake(t, 0)
	a.add()
	e.s.Ready(a)
	if s := a.slot.State(); s != engine.SlotIdle {
		t.Errorf("Ready after Stop left the Slot %v, want idle", s)
	}
}

// TestStopInTurnPanics covers SCH-200's engine-context check: Stop inside a turn panics, and the
// engine keeps working.
func TestStopInTurnPanics(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2})
	var text atomic.Value
	a := newFake(t, 0)
	a.onTurn = func(*fakeActor, int) { text.Store(panicText(e.Stop)) }
	a.add()
	e.s.Ready(a)
	waitFor(t, time.Second, "the turn", func() bool { return a.processed.Load() == 1 })
	const want = "lockstep: prodengine: Stop called from engine context"
	if got, _ := text.Load().(string); got != want {
		t.Errorf("Stop inside RunTurn panicked with %q, want %q", got, want)
	}
	a.add()
	e.s.Ready(a)
	waitFor(t, time.Second, "a second turn", func() bool { return a.processed.Load() == 2 })
}

// TestParseGoroutineID covers the parse of SCH-270's first stack line, "goroutine <id> [".
func TestParseGoroutineID(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want uint64
	}{
		{"goroutine 1 [running]:\n", 1},
		{"goroutine 18446744073709551615 [running]:", 18446744073709551615},
		{"goroutine 42 [", 42},
		{"goroutine 42", 0},
		{"goroutine x [running]:", 0},
		{"goroutine  [running]:", 0},
		{"goroutine ", 0},
		{"thread 7 [running]:", 0},
		{"", 0},
	} {
		if got := parseGoroutineID([]byte(tc.in)); got != tc.want {
			t.Errorf("parseGoroutineID(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
	if goroutineID() == 0 {
		t.Error("goroutineID() = 0 on the test goroutine")
	}
}

// TestInTurnNoAlloc checks that InTurn does not allocate once its stack buffer is pooled.
func TestInTurnNoAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops buffers at random under the race detector")
	}
	e := newEngine(t, Config{Node: "n", Workers: 2})
	if n := testing.AllocsPerRun(1000, func() { _ = e.s.InTurn() }); n != 0 {
		t.Errorf("InTurn: %v allocations per call, want 0", n)
	}
}

// TestCallbackInEngineContext covers SCH-115 for callback entries: a worker runs each pushed
// callback once, with InTurn true inside it.
func TestCallbackInEngineContext(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2})
	const n = 100
	var runs, inside atomic.Int32
	for range n {
		e.s.pushCallback(func() {
			if e.s.InTurn() {
				inside.Add(1)
			}
			runs.Add(1) // last, so the test reads inside after every callback updated it
		})
	}
	waitFor(t, time.Second, "100 callbacks", func() bool { return runs.Load() == n })
	if got := inside.Load(); got != n {
		t.Errorf("InTurn() true in %d of %d callbacks, want all", got, n)
	}
	time.Sleep(10 * time.Millisecond)
	if got := runs.Load(); got != n {
		t.Errorf("%d callback runs, want %d", got, n)
	}
}
