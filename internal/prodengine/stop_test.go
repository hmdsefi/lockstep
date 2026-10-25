// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// TestATSCH19Stop covers SCH-200 and SCH-220 (AT-SCH-19).
func TestATSCH19Stop(t *testing.T) {
	// Registered first, so it runs after the cleanups of the subtests and of this test, which
	// stop every engine built here.
	t.Cleanup(func() { goleak.VerifyNone(t) })

	t.Run("Effects", func(t *testing.T) {
		waitFor(t, time.Second, "earlier effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
		e := newEngine(t, Config{Node: "n", Workers: 2, EffectWorkers: 1, EffectQueue: 1})
		fx := e.effects()
		ctxs := make(chan context.Context, 1)
		fx.Start(engine.EffectRequest{Name: "running", Run: func(ctx context.Context) (any, error) {
			ctxs <- ctx
			<-ctx.Done()
			return nil, ctx.Err()
		}}, noDone(t, "the running effect"))
		ctx := recv(t, ctxs, "the running effect to start")
		var queuedRan atomic.Bool
		fx.Start(engine.EffectRequest{Name: "queued", Run: func(context.Context) (any, error) {
			queuedRan.Store(true)
			return nil, nil
		}}, noDone(t, "the queued effect"))

		e.Stop()
		select {
		case <-ctx.Done():
		default:
			t.Error("the running effect's context is not done after Stop returned")
		}
		waitFor(t, time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
		time.Sleep(50 * time.Millisecond) // a done that ran would fail the test through noDone
		if queuedRan.Load() {
			t.Error("Run of the queued effect was called after Stop")
		}

		a := newFake(t, 0)
		a.add()
		e.s.Ready(a)
		time.Sleep(100 * time.Millisecond)
		if n := a.turns.Load(); n != 0 {
			t.Errorf("Ready after Stop ran %d turns, want 0", n)
		}

		late := fx.Start(engine.EffectRequest{Name: "late", Run: func(context.Context) (any, error) {
			t.Error("Run of an effect started after Stop was called")
			return nil, nil
		}}, noDone(t, "the late effect"))
		if late() {
			t.Error("Cancel of an effect started after Stop = true, want false")
		}

		returned := make(chan struct{})
		go func() {
			e.Stop()
			close(returned)
		}()
		recv(t, returned, "a second Stop to return")
	})

	t.Run("TurnInProgress", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		entered := make(chan struct{})
		var finished atomic.Bool
		a := newFake(t, 0)
		a.onTurn = func(*fakeActor, int) {
			close(entered)
			time.Sleep(20 * time.Millisecond)
			finished.Store(true)
		}
		a.add()
		e.s.Ready(a)
		recv(t, entered, "the 20 ms turn to start")
		e.Stop()
		if !finished.Load() {
			t.Error("Stop returned before the turn in progress returned")
		}
	})

	t.Run("InsideRunTurn", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		texts := make(chan string, 1)
		a := newFake(t, 0)
		// The panic is recovered inside RunTurn: one that escapes RunTurn crashes the process
		// (SCH-085).
		a.onTurn = func(*fakeActor, int) { texts <- panicText(e.Stop) }
		a.add()
		e.s.Ready(a)
		const want = "lockstep: prodengine: Stop called from engine context"
		if got := recv(t, texts, "the turn"); got != want {
			t.Errorf("Stop inside RunTurn panicked with %q, want %q", got, want)
		}
	})
}

// TestATSCH20StopIgnoredContext covers SCH-200 for an effect whose Run ignores its context
// (AT-SCH-20): Stop does not wait for it, and its done never runs.
func TestATSCH20StopIgnoredContext(t *testing.T) {
	waitFor(t, time.Second, "earlier effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
	e := newEngine(t, Config{Node: "n", Workers: 2, EffectWorkers: 1})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	entered := make(chan struct{})
	e.effects().Start(engine.EffectRequest{Name: "stubborn", Run: func(context.Context) (any, error) {
		close(entered)
		<-release
		return 1, nil
	}}, noDone(t, "the effect that ignores its context"))
	recv(t, entered, "the effect to start")

	e.Stop()
	if g := effectGoroutines(); g != 1 {
		t.Errorf("effectGoroutines() after Stop = %d, want 1 (Run still blocked)", g)
	}
	releaseOnce.Do(func() { close(release) })
	waitFor(t, time.Second, "the effect goroutine to exit", func() bool { return effectGoroutines() == 0 })
	time.Sleep(50 * time.Millisecond) // a done that ran would fail the test through noDone
}

// TestStopStopsTimers covers SCH-200 step 2 and SCH-140: Stop stops every pending timer, the
// pending count drops to 0, and the timers' Cancel then reports false.
func TestStopStopsTimers(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2})
	var cancels []engine.Cancel
	for range 3 {
		cancels = append(cancels, e.clock().AfterFunc(time.Hour, "lockstep/timer", func() {
			t.Error("a timer stopped by Stop ran")
		}))
	}
	if n := e.pendingTimers(); n != 3 {
		t.Fatalf("pendingTimers() = %d, want 3", n)
	}
	e.Stop()
	if n := e.pendingTimers(); n != 0 {
		t.Errorf("pendingTimers() after Stop = %d, want 0", n)
	}
	for i, c := range cancels {
		if c() {
			t.Errorf("Cancel of timer %d after Stop = true, want false", i)
		}
	}
}

// TestStopDiscardsQueued covers SCH-200 steps 2 and 3: turns and callbacks queued when Stop
// starts never run, the global queue is empty afterwards, and an entry pushed after Stop is
// dropped.
func TestStopDiscardsQueued(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 1})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseWorker) // runs before newEngine's Stop cleanup
	entered := make(chan struct{})
	blocker := newFake(t, 0)
	blocker.onTurn = func(*fakeActor, int) {
		close(entered)
		<-release // the only worker is busy, so everything below stays queued
	}
	blocker.add()
	e.s.Ready(blocker)
	recv(t, entered, "the blocking turn to start")

	queued := fakes(t, 5)
	for _, a := range queued {
		a.add()
		e.s.Ready(a)
	}
	var callbackRan atomic.Bool
	e.s.pushCallback(func() { callbackRan.Store(true) })
	if n := e.globalLen(); n != 6 {
		t.Fatalf("globalLen() = %d, want 6 (5 turns and a callback)", n)
	}

	stopped := make(chan struct{})
	go func() {
		e.Stop()
		close(stopped)
	}()
	waitFor(t, time.Second, "Stop to mark the engine closing", e.s.closing.Load)
	releaseWorker()
	recv(t, stopped, "Stop to return")

	for _, a := range queued {
		if n := a.turns.Load(); n != 0 {
			t.Errorf("fake %d queued before Stop ran %d turns, want 0", a.id, n)
		}
	}
	if callbackRan.Load() {
		t.Error("a callback queued before Stop ran")
	}
	if n := e.globalLen(); n != 0 {
		t.Errorf("globalLen() after Stop = %d, want 0", n)
	}
	e.s.pushCallback(func() { t.Error("a callback pushed after Stop ran") })
	if n := e.globalLen(); n != 0 {
		t.Errorf("globalLen() after a push after Stop = %d, want 0", n)
	}
}

// TestStopRacesEngineCalls covers ENG-140 and SCH-200 under contention: Stop runs while other
// goroutines call Ready, AfterFunc and Start, and no turn, timer function or done runs after Stop
// returned.
func TestStopRacesEngineCalls(t *testing.T) {
	rounds := 100
	if testing.Short() {
		rounds = 20
	}
	for range rounds {
		e := newEngine(t, Config{Node: "n", Workers: 4, EffectWorkers: 2, EffectQueue: 2})
		var stopped, quit atomic.Bool
		after := func(what string) {
			if stopped.Load() {
				t.Errorf("%s ran after Stop returned", what)
			}
		}
		as := fakes(t, 8)
		for _, a := range as {
			a.onTurn = func(*fakeActor, int) { after("a turn") }
		}
		var callers sync.WaitGroup
		for i := range 3 {
			callers.Go(func() {
				for n := 0; !quit.Load(); n++ {
					switch (n + i) % 3 {
					case 0:
						a := as[n%len(as)]
						a.add()
						e.s.Ready(a)
					case 1:
						e.clock().AfterFunc(time.Duration(n%3)*time.Microsecond, "lockstep/timer", func() { after("a timer function") })
					case 2:
						e.effects().Start(engine.EffectRequest{Name: "quick", Run: func(context.Context) (any, error) {
							return nil, nil
						}}, func(any, error) { after("an effect's done") })
					}
				}
			})
		}
		time.Sleep(time.Millisecond)
		e.Stop()
		stopped.Store(true)
		time.Sleep(time.Millisecond) // callers keep calling the stopped engine
		quit.Store(true)
		callers.Wait()
		if n := e.pendingTimers(); n != 0 {
			t.Fatalf("pendingTimers() after Stop = %d, want 0", n)
		}
	}
	waitFor(t, time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
	waitFor(t, time.Second, "workers to exit", func() bool { return workerGoroutines() == 0 })
}

// TestStartAfterClosingMark covers SCH-200 step 1 for Start: once Stop has marked the engine
// closing, even before it marks the effect pool, Start runs nothing and its Cancel reports false.
func TestStartAfterClosingMark(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 1, EffectWorkers: 1, EffectQueue: 1})
	e.s.closing.Store(true) // the first statement of Stop, before the pool's own mark
	var ran atomic.Int32
	run := func(context.Context) (any, error) {
		ran.Add(1)
		return nil, nil
	}
	// Without the check, the first Start would run, the second queue and the third be refused.
	for i := range 3 {
		c := e.effects().Start(engine.EffectRequest{Name: "late", Run: run}, noDone(t, "an effect started after the closing mark"))
		if c() {
			t.Errorf("Cancel of effect %d started after the closing mark = true, want false", i)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if n := ran.Load(); n != 0 {
		t.Errorf("Run of an effect started after the closing mark was called %d times, want 0", n)
	}
	if n := e.globalLen(); n != 0 {
		t.Errorf("globalLen() = %d, want 0 (no refusal callback queued)", n)
	}
}
