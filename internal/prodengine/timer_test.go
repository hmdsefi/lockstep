// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestATSCH14Timers covers SCH-115, SCH-120, SCH-130 and SCH-140 (AT-SCH-14).
func TestATSCH14Timers(t *testing.T) {
	t.Run("RunsOnceInEngineContext", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		var runs atomic.Int32
		var inTurn atomic.Bool
		ran := make(chan struct{}, 2)
		e.clock().AfterFunc(10*time.Millisecond, "lockstep/timer", func() {
			runs.Add(1)
			inTurn.Store(e.s.InTurn())
			ran <- struct{}{}
		})
		select {
		case <-ran:
		case <-time.After(time.Second):
			t.Fatal("the 10 ms timer did not run within 1 s")
		}
		if !inTurn.Load() {
			t.Error("InTurn() inside the timer function = false, want true")
		}
		if e.s.InTurn() {
			t.Error("InTurn() on the test goroutine = true, want false")
		}
		if n := e.pendingTimers(); n != 0 {
			t.Errorf("pendingTimers() after the timer ran = %d, want 0", n)
		}
		time.Sleep(50 * time.Millisecond)
		if n := runs.Load(); n != 1 {
			t.Errorf("the timer ran %d times, want once", n)
		}
	})

	t.Run("NegativeDelayRunsAfterAfterFuncReturned", func(t *testing.T) {
		// One worker: the callback cannot start before the turn that armed it ends, so the
		// flag set after the call is visible to it.
		e := newEngine(t, Config{Node: "n", Workers: 1})
		var returned atomic.Bool
		type result struct{ afterReturn, inTurn bool }
		got := make(chan result, 1)
		a := newFake(t, 0)
		a.onTurn = func(*fakeActor, int) {
			e.clock().AfterFunc(-1, "lockstep/timer", func() {
				got <- result{returned.Load(), e.s.InTurn()}
			})
			returned.Store(true)
		}
		a.add()
		e.s.Ready(a)
		select {
		case r := <-got:
			if !r.afterReturn {
				t.Error("the timer function ran before AfterFunc returned")
			}
			if !r.inTurn {
				t.Error("InTurn() inside the timer function = false, want true")
			}
		case <-time.After(time.Second):
			t.Fatal("the timer with delay -1 did not run within 1 s")
		}
	})

	t.Run("CancelIsExactlyOnce", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		var runs atomic.Int32
		c := e.clock().AfterFunc(time.Hour, "lockstep/timer", func() { runs.Add(1) })
		if n := e.pendingTimers(); n != 1 {
			t.Errorf("pendingTimers() with a 1 h timer armed = %d, want 1", n)
		}
		if !c() {
			t.Error("first Cancel() = false, want true")
		}
		if n := e.pendingTimers(); n != 0 {
			t.Errorf("pendingTimers() after Cancel = %d, want 0", n)
		}
		if c() {
			t.Error("second Cancel() = true, want false")
		}
		if n := runs.Load(); n != 0 {
			t.Errorf("the cancelled timer ran %d times", n)
		}
	})

	t.Run("CancelAfterRunIsFalse", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		ran := make(chan struct{})
		c := e.clock().AfterFunc(0, "lockstep/timer", func() { close(ran) })
		select {
		case <-ran:
		case <-time.After(time.Second):
			t.Fatal("the timer did not run within 1 s")
		}
		if c() {
			t.Error("Cancel() after the timer ran = true, want false")
		}
	})

	t.Run("StopBeforeFiring", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		var runs atomic.Int32
		e.clock().AfterFunc(50*time.Millisecond, "lockstep/timer", func() { runs.Add(1) })
		e.Stop()
		time.Sleep(100 * time.Millisecond)
		if n := runs.Load(); n != 0 {
			t.Errorf("a timer armed before Stop ran %d times within 100 ms of Stop, want 0", n)
		}
	})

	t.Run("AfterFuncAfterStop", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		e.Stop()
		var runs atomic.Int32
		c := e.clock().AfterFunc(0, "lockstep/timer", func() { runs.Add(1) })
		if c() {
			t.Error("Cancel() of a timer armed after Stop = true, want false")
		}
		if n := e.pendingTimers(); n != 0 {
			t.Errorf("pendingTimers() after AfterFunc on a stopped engine = %d, want 0", n)
		}
		time.Sleep(100 * time.Millisecond)
		if n := runs.Load(); n != 0 {
			t.Errorf("a timer armed after Stop ran %d times, want 0", n)
		}
	})

	t.Run("NowIsWallTime", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 1})
		before := time.Now()
		got := e.clock().Now()
		after := time.Now()
		if got.Before(before) || got.After(after) {
			t.Errorf("Now() = %v, want a time in [%v, %v]", got, before, after)
		}
	})
}

// TestCancelBeforeCallbackStarts checks that a timer cancelled after it fired but before its
// callback started runs nothing (SCH-130).
func TestCancelBeforeCallbackStarts(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 1})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
	// Cleanups run last-in first-out: the worker is released before Stop waits for it, even
	// when an assertion below fails.
	t.Cleanup(releaseWorker)
	entered := make(chan struct{})
	a := newFake(t, 0)
	a.onTurn = func(*fakeActor, int) {
		close(entered)
		<-release // the only worker is busy, so the fired timer's callback stays queued
	}
	a.add()
	e.s.Ready(a)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("the turn did not start within 1 s")
	}

	var ran atomic.Bool
	c := e.clock().AfterFunc(0, "lockstep/timer", func() { ran.Store(true) })
	waitFor(t, time.Second, "the timer to fire", func() bool { return e.s.global.len() == 1 })
	if !c() {
		t.Error("Cancel() of a fired timer whose callback has not started = false, want true")
	}
	if n := e.pendingTimers(); n != 0 {
		t.Errorf("pendingTimers() after Cancel = %d, want 0", n)
	}
	marker := make(chan struct{})
	e.s.pushCallback(func() { close(marker) }) // queued behind the timer's callback
	releaseWorker()
	select {
	case <-marker:
	case <-time.After(time.Second):
		t.Fatal("the marker callback did not run within 1 s")
	}
	if ran.Load() {
		t.Error("the callback of a cancelled timer ran its function")
	}
}

// TestCancelRacesFiring checks that each timer either is cancelled (Cancel true, function never
// runs) or runs once (Cancel false), never both and never neither (SCH-130).
func TestCancelRacesFiring(t *testing.T) {
	const n = 2000
	e := newEngine(t, Config{Node: "n", Workers: 4})
	runs := make([]atomic.Int32, n)
	cancels := make([]atomic.Bool, n)
	var settled atomic.Int32 // timers that ran, plus timers whose Cancel returned true
	var wg sync.WaitGroup
	for i := range n {
		c := e.clock().AfterFunc(time.Duration(i%3)*time.Microsecond, "lockstep/timer", func() {
			runs[i].Add(1)
			settled.Add(1)
		})
		wg.Go(func() {
			if c() {
				cancels[i].Store(true)
				settled.Add(1)
			}
		})
	}
	wg.Wait()
	waitFor(t, 5*time.Second, "every timer to run or be cancelled", func() bool { return settled.Load() >= n })
	waitFor(t, time.Second, "pending timers to drain", func() bool { return e.pendingTimers() == 0 })
	time.Sleep(20 * time.Millisecond) // a timer that wrongly ran after Cancel would show below
	for i := range n {
		want := int32(1)
		if cancels[i].Load() {
			want = 0
		}
		if got := runs[i].Load(); got != want {
			t.Fatalf("timer %d: cancelled=%v but ran %d times, want %d", i, cancels[i].Load(), got, want)
		}
	}
}

// TestCallbackOneAtATime covers SCH-115: each timer's function runs once. Every function
// increments its own element, so -race reports any entry that ran twice concurrently.
func TestCallbackOneAtATime(t *testing.T) {
	const n = 1000
	e := newEngine(t, Config{Node: "n", Workers: 4})
	hits := make([]int, n)
	var finished atomic.Int32 // incremented after hits[i]++, so reading it orders the reads below
	for i := range n {
		e.clock().AfterFunc(0, "lockstep/timer", func() {
			hits[i]++
			finished.Add(1)
		})
	}
	waitFor(t, 5*time.Second, "all timers to run", func() bool { return finished.Load() == n })
	waitFor(t, time.Second, "pending timers to drain", func() bool { return e.pendingTimers() == 0 })
	for i, h := range hits {
		if h != 1 {
			t.Errorf("timer %d ran %d times, want 1", i, h)
		}
	}
}
