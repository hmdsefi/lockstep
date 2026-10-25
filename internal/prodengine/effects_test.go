// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// effectGoroutines returns the number of effect goroutines of any engine that are alive.
func effectGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return bytes.Count(buf[:n], []byte("prodengine.(*Engine).effectLoop("))
		}
		buf = make([]byte, 2*len(buf))
	}
}

// doneResult is one call of an effect's done function.
type doneResult struct {
	id     int
	v      any
	err    error
	inTurn bool
}

// inTurn runs f inside one turn of a fake actor on e and waits for the turn to end. With
// Workers: 1 no callback of e can run until f returned.
func inTurn(t *testing.T, e *Engine, f func()) {
	t.Helper()
	ended := make(chan struct{})
	a := newFake(t, 0)
	a.onTurn = func(*fakeActor, int) {
		f()
		close(ended)
	}
	a.add()
	e.s.Ready(a)
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the turn did not end within 5 s")
	}
}

// noDone fails the test if an effect's done runs.
func noDone(t *testing.T, name string) func(any, error) {
	return func(v any, err error) { t.Errorf("done of %s ran with (%v, %v), want it never to run", name, v, err) }
}

// TestATSCH16EffectPoolBounds covers SCH-150, SCH-160 and SCH-190 (AT-SCH-16).
func TestATSCH16EffectPoolBounds(t *testing.T) {
	// One worker: no callback can run before the turn that issues the six Starts ends, so
	// "done runs after Start returned" holds without a race.
	e := newEngine(t, Config{Node: "n", Workers: 1, EffectWorkers: 2, EffectQueue: 3})
	fx := e.effects()

	const n = 6
	var gate [n + 1]chan struct{} // gate[i] releases the Run of effect i
	for i := 1; i <= n; i++ {
		gate[i] = make(chan struct{})
	}
	entered := make(chan int, n)
	dones := make(chan doneResult, n)
	var startsReturned atomic.Bool
	var afterReturn [n + 1]atomic.Bool // done of effect i ran after Start returned

	inTurn(t, e, func() {
		for i := 1; i <= n; i++ {
			fx.Start(engine.EffectRequest{
				Name: fmt.Sprintf("e%d", i),
				Run: func(ctx context.Context) (any, error) {
					entered <- i
					<-gate[i]
					return fmt.Sprintf("r%d", i), nil
				},
			}, func(v any, err error) {
				afterReturn[i].Store(startsReturned.Load())
				dones <- doneResult{i, v, err, e.s.InTurn()}
			})
		}
		startsReturned.Store(true)
	})

	// Effects 1 and 2 run, in either order; 3 to 5 wait; 6 is refused.
	first := map[int]bool{}
	for range 2 {
		first[recv(t, entered, "an effect to enter Run")] = true
	}
	if !first[1] || !first[2] {
		t.Fatalf("effects that entered Run first = %v, want 1 and 2", first)
	}
	refused := recv(t, dones, "the refused effect's done")
	if refused.id != 6 || refused.v != nil || !errors.Is(refused.err, engine.ErrOverloaded) {
		t.Errorf("first done = %+v, want effect 6 with (nil, ErrOverloaded)", refused)
	}
	if !refused.inTurn {
		t.Error("InTurn() inside done of effect 6 = false, want true")
	}
	if !afterReturn[6].Load() {
		t.Error("done of effect 6 ran before Start returned")
	}
	if g := effectGoroutines(); g != 2 {
		t.Errorf("effectGoroutines() with 2 running and 3 queued = %d, want 2", g)
	}
	select {
	case id := <-entered:
		t.Fatalf("effect %d entered Run while both effect goroutines were busy", id)
	case <-time.After(50 * time.Millisecond):
	}

	// Release one at a time: the freed goroutine takes the head of the queue (FIFO).
	for _, step := range [][2]int{{1, 3}, {2, 4}} {
		close(gate[step[0]])
		if got := recv(t, entered, "the next queued effect"); got != step[1] {
			t.Fatalf("after releasing effect %d, effect %d started, want effect %d", step[0], got, step[1])
		}
	}
	close(gate[3])
	if got := recv(t, entered, "effect 5"); got != 5 {
		t.Fatalf("after releasing effect 3, effect %d started, want effect 5", got)
	}
	close(gate[4])
	close(gate[5])

	got := map[int]doneResult{}
	for range 5 {
		r := recv(t, dones, "a done")
		if _, dup := got[r.id]; dup {
			t.Fatalf("done of effect %d ran twice", r.id)
		}
		got[r.id] = r
	}
	for i := 1; i <= 5; i++ {
		r, ok := got[i]
		if !ok {
			t.Errorf("done of effect %d never ran", i)
			continue
		}
		if want := fmt.Sprintf("r%d", i); r.v != want || r.err != nil {
			t.Errorf("done of effect %d = (%v, %v), want (%q, nil)", i, r.v, r.err, want)
		}
		if !r.inTurn {
			t.Errorf("InTurn() inside done of effect %d = false, want true", i)
		}
		if !afterReturn[i].Load() {
			t.Errorf("done of effect %d ran before Start returned", i)
		}
	}
	waitFor(t, time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
	select {
	case r := <-dones:
		t.Errorf("extra done after all effects finished: %+v", r)
	case id := <-entered:
		t.Errorf("effect %d entered Run after the others finished", id)
	case <-time.After(20 * time.Millisecond):
	}
}

// recv receives from c or fails the test after 5 s.
func recv[T any](t *testing.T, c <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// TestATSCH17EffectCancel covers SCH-170, SCH-180 and SCH-190 (AT-SCH-17).
func TestATSCH17EffectCancel(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2, EffectWorkers: 1, EffectQueue: 2})
	fx := e.effects()

	entered := make(chan struct{})
	returned := make(chan error, 1)
	var ran2 atomic.Bool
	cancel1 := fx.Start(engine.EffectRequest{
		Name: "first",
		Run: func(ctx context.Context) (any, error) {
			close(entered)
			<-ctx.Done()
			err := ctx.Err()
			returned <- err
			return nil, err
		},
	}, noDone(t, "effect 1"))
	recv(t, entered, "effect 1 to enter Run")
	cancel2 := fx.Start(engine.EffectRequest{
		Name: "second",
		Run: func(context.Context) (any, error) {
			ran2.Store(true)
			return nil, nil
		},
	}, noDone(t, "effect 2"))

	if !cancel2() {
		t.Error("Cancel of the queued effect = false, want true")
	}
	if !cancel1() {
		t.Error("Cancel of the running effect = false, want true")
	}
	if err := recv(t, returned, "effect 1's Run to return"); !errors.Is(err, context.Canceled) {
		t.Errorf("effect 1's context error = %v, want context.Canceled", err)
	}
	time.Sleep(50 * time.Millisecond)
	if ran2.Load() {
		t.Error("Run of the cancelled queued effect ran")
	}
	if cancel1() || cancel2() {
		t.Error("a second Cancel returned true, want false")
	}
	waitFor(t, time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })

	// An effect whose done already ran cannot be cancelled.
	done := make(chan struct{})
	cancel3 := fx.Start(engine.EffectRequest{
		Name: "third",
		Run:  func(context.Context) (any, error) { return 3, nil },
	}, func(v any, err error) {
		if v != 3 || err != nil {
			t.Errorf("done of effect 3 got (%v, %v), want (3, nil)", v, err)
		}
		close(done)
	})
	recv(t, done, "done of effect 3")
	if cancel3() {
		t.Error("Cancel after done ran = true, want false")
	}
}

// TestEffectCancelBeforeCallbackStarts covers the window between Run returning and done
// starting: Cancel wins there (SCH-180), for an effect that ran and for a refused one.
func TestEffectCancelBeforeCallbackStarts(t *testing.T) {
	// One worker: done cannot start while the turn that cancels is still running.
	e := newEngine(t, Config{Node: "n", Workers: 1, EffectWorkers: 1, EffectQueue: -1})
	fx := e.effects()
	ran := make(chan struct{})
	release := make(chan struct{})
	var cancelRan, cancelRefused engine.Cancel
	inTurn(t, e, func() {
		cancelRan = fx.Start(engine.EffectRequest{
			Name: "ran",
			Run: func(context.Context) (any, error) {
				close(ran)
				<-release
				return nil, nil
			},
		}, noDone(t, "the effect that ran"))
		cancelRefused = fx.Start(engine.EffectRequest{
			Name: "refused",
			Run:  func(context.Context) (any, error) { return nil, nil },
		}, noDone(t, "the refused effect"))
		// This runs on a worker: report with Error, never Fatal.
		select {
		case <-ran:
		case <-time.After(5 * time.Second):
			t.Error("Run did not start within 5 s")
		}
		close(release)
		for deadline := time.Now().Add(5 * time.Second); effectGoroutines() != 0; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Error("the effect goroutine did not exit within 5 s")
				break
			}
		}
		if !cancelRan() {
			t.Error("Cancel after Run returned and before done started = false, want true")
		}
		if !cancelRefused() {
			t.Error("Cancel of the refused effect before its done started = false, want true")
		}
	})
	time.Sleep(50 * time.Millisecond)
}

// TestEffectCancelFreesQueueSlot checks that a cancelled queued effect leaves the queue.
func TestEffectCancelFreesQueueSlot(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2, EffectWorkers: 1, EffectQueue: 1})
	fx := e.effects()
	release := make(chan struct{})
	entered := make(chan struct{})
	fx.Start(engine.EffectRequest{Name: "busy", Run: func(context.Context) (any, error) {
		close(entered)
		<-release
		return nil, nil
	}}, func(any, error) {})
	recv(t, entered, "the first effect")
	cancel := fx.Start(engine.EffectRequest{Name: "queued", Run: func(context.Context) (any, error) { return nil, nil }}, noDone(t, "the cancelled effect"))
	if !cancel() {
		t.Fatal("Cancel of the queued effect = false, want true")
	}
	got := make(chan error, 1)
	fx.Start(engine.EffectRequest{Name: "replacement", Run: func(context.Context) (any, error) { return "ok", nil }},
		func(v any, err error) { got <- err })
	close(release)
	if err := recv(t, got, "done of the replacement"); err != nil {
		t.Errorf("done of the replacement got error %v: its queue slot was not freed", err)
	}
}

// TestATSCH18EffectPanic covers SCH-170 (AT-SCH-18).
func TestATSCH18EffectPanic(t *testing.T) {
	// One effect goroutine: the second effect waits in the queue and must run on the goroutine
	// that recovered the panic.
	e := newEngine(t, Config{Node: "n", Workers: 2, EffectWorkers: 1, EffectQueue: 1})
	fx := e.effects()
	release := make(chan struct{})
	boom := make(chan doneResult, 1)
	next := make(chan doneResult, 1)
	fx.Start(engine.EffectRequest{Name: "boom", Run: func(context.Context) (any, error) {
		<-release
		panic("x")
	}}, func(v any, err error) { boom <- doneResult{v: v, err: err} })
	fx.Start(engine.EffectRequest{Name: "next", Run: func(context.Context) (any, error) { return 7, nil }},
		func(v any, err error) { next <- doneResult{v: v, err: err} })
	close(release)

	r := recv(t, boom, "done of the panicking effect")
	var pe *EffectPanicError
	if !errors.As(r.err, &pe) {
		t.Fatalf("done got error %v (%T), want *EffectPanicError", r.err, r.err)
	}
	if r.v != nil {
		t.Errorf("done got value %v, want nil", r.v)
	}
	if pe.Name != "boom" || pe.Value != "x" {
		t.Errorf("EffectPanicError = {Name: %q, Value: %v}, want {Name: \"boom\", Value: \"x\"}", pe.Name, pe.Value)
	}
	if len(pe.Stack) == 0 || !strings.Contains(string(pe.Stack), "TestATSCH18EffectPanic") {
		t.Errorf("EffectPanicError.Stack = %q, want a stack of the effect goroutine", pe.Stack)
	}
	if want := `lockstep: effect "boom" panicked: x`; pe.Error() != want {
		t.Errorf("Error() = %q, want %q", pe.Error(), want)
	}
	if r := recv(t, next, "done of the effect after the panic"); r.v != 7 || r.err != nil {
		t.Errorf("effect after the panic: done got (%v, %v), want (7, nil)", r.v, r.err)
	}
}

// TestEffectPoolNeverExceedsWorkers runs many short effects and checks the bound and that every
// done runs once.
func TestEffectPoolNeverExceedsWorkers(t *testing.T) {
	const workers, total = 3, 60
	e := newEngine(t, Config{Node: "n", Workers: 2, EffectWorkers: workers, EffectQueue: total})
	fx := e.effects()
	var cur, peak atomic.Int32
	dones := make(chan int, total)
	for i := range total {
		fx.Start(engine.EffectRequest{Name: "e", Run: func(context.Context) (any, error) {
			c := cur.Add(1)
			for p := peak.Load(); c > p && !peak.CompareAndSwap(p, c); p = peak.Load() {
			}
			if g := effectGoroutines(); g > workers {
				t.Errorf("effectGoroutines() = %d, want at most %d", g, workers)
			}
			time.Sleep(time.Millisecond)
			cur.Add(-1)
			return i, nil
		}}, func(v any, err error) {
			id, _ := v.(int)
			dones <- id
		})
	}
	seen := map[int]bool{}
	for range total {
		id := recv(t, dones, "a done")
		if seen[id] {
			t.Fatalf("done of effect %d ran twice", id)
		}
		seen[id] = true
	}
	if p := peak.Load(); p > workers {
		t.Errorf("peak concurrent effects = %d, want at most %d", p, workers)
	}
	waitFor(t, time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
}

// TestEffectsAfterStop covers SCH-150 step 1 and the pool's part of SCH-200: Stop cancels the
// contexts of running effects, drops the queued ones, and later Starts report false.
func TestEffectsAfterStop(t *testing.T) {
	e := New(Config{Node: "n", Workers: 2, EffectWorkers: 1, EffectQueue: 1})
	t.Cleanup(e.Stop)
	fx := e.effects()
	entered := make(chan struct{})
	ctxDone := make(chan struct{})
	fx.Start(engine.EffectRequest{Name: "running", Run: func(ctx context.Context) (any, error) {
		close(entered)
		<-ctx.Done()
		close(ctxDone)
		return nil, ctx.Err()
	}}, noDone(t, "the running effect"))
	recv(t, entered, "the running effect")
	var ranQueued atomic.Bool
	fx.Start(engine.EffectRequest{Name: "queued", Run: func(context.Context) (any, error) {
		ranQueued.Store(true)
		return nil, nil
	}}, noDone(t, "the queued effect"))
	e.Stop()
	recv(t, ctxDone, "the running effect's context to be cancelled")
	waitFor(t, time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
	if ranQueued.Load() {
		t.Error("the queued effect ran after Stop")
	}
	if fx.Start(engine.EffectRequest{Name: "late", Run: func(context.Context) (any, error) {
		t.Error("Run of an effect started after Stop ran")
		return nil, nil
	}}, noDone(t, "the late effect"))() {
		t.Error("Cancel of an effect started after Stop = true, want false")
	}
	time.Sleep(50 * time.Millisecond)
}

// TestEffectCancelRacesQueueHead races Cancel of the queued effect against the running effect
// returning, which makes the goroutine take the queue head. Whatever the interleaving: a true
// Cancel means done never runs and a Run that did start sees its context cancelled; a false
// Cancel means done runs exactly once.
func TestEffectCancelRacesQueueHead(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2, EffectWorkers: 1, EffectQueue: 1})
	fx := e.effects()
	for i := range 300 {
		release := make(chan struct{})
		entered := make(chan struct{})
		fx.Start(engine.EffectRequest{Name: "head", Run: func(context.Context) (any, error) {
			close(entered)
			<-release
			return nil, nil
		}}, func(any, error) {})
		recv(t, entered, "the running effect")

		var runs, dones atomic.Int32
		ctxs := make(chan context.Context, 1)
		cancel := fx.Start(engine.EffectRequest{Name: "queued", Run: func(ctx context.Context) (any, error) {
			runs.Add(1)
			ctxs <- ctx
			return nil, nil
		}}, func(any, error) { dones.Add(1) })

		won := make(chan bool, 1)
		go func() { won <- cancel() }()
		close(release)
		if <-won {
			if runs.Load() == 1 {
				// Run started before Cancel took the job: its context must end.
				ctx := recv(t, ctxs, "the context of the started effect")
				select {
				case <-ctx.Done():
				case <-time.After(5 * time.Second):
					t.Fatalf("iteration %d: Cancel returned true after Run started, but its context is not cancelled", i)
				}
			}
			waitFor(t, 5*time.Second, "the effect goroutine to exit", func() bool { return effectGoroutines() == 0 })
			time.Sleep(time.Millisecond)
			if dones.Load() != 0 {
				t.Fatalf("iteration %d: done ran after Cancel returned true", i)
			}
		} else {
			waitFor(t, 5*time.Second, "done", func() bool { return dones.Load() > 0 })
			if n := dones.Load(); n != 1 {
				t.Fatalf("iteration %d: done ran %d times, want once", i, n)
			}
		}
		waitFor(t, 5*time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
	}
}

// TestEffectStopSuppressesDoneOfCancelledContext checks that a Run that returns because Stop
// cancelled its context never has its done called (SCH-190, SCH-200).
func TestEffectStopSuppressesDoneOfCancelledContext(t *testing.T) {
	for range 100 {
		e := New(Config{Node: "n", Workers: 2, EffectWorkers: 1, EffectQueue: 1})
		entered := make(chan struct{})
		e.effects().Start(engine.EffectRequest{Name: "running", Run: func(ctx context.Context) (any, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}}, noDone(t, "the effect cancelled by Stop"))
		recv(t, entered, "the running effect")
		e.Stop()
		waitFor(t, 5*time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
	}
	time.Sleep(20 * time.Millisecond)
}

// TestEffectGoexitKeepsPoolSize checks that a Run that ends its goroutine with runtime.Goexit
// neither leaks a pool slot nor strands the queue.
func TestEffectGoexitKeepsPoolSize(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2, EffectWorkers: 1, EffectQueue: 1})
	fx := e.effects()
	release := make(chan struct{})
	entered := make(chan struct{})
	fx.Start(engine.EffectRequest{Name: "exits", Run: func(context.Context) (any, error) {
		close(entered)
		<-release
		runtime.Goexit()
		return nil, nil
	}}, noDone(t, "the effect that called Goexit"))
	recv(t, entered, "the first effect")
	got := make(chan any, 2)
	fx.Start(engine.EffectRequest{Name: "queued", Run: func(context.Context) (any, error) { return "queued", nil }},
		func(v any, _ error) { got <- v })
	close(release)
	if v := recv(t, got, "done of the queued effect"); v != "queued" {
		t.Errorf("done of the queued effect got %v, want %q", v, "queued")
	}
	waitFor(t, time.Second, "effect goroutines to exit", func() bool { return effectGoroutines() == 0 })
	fx.Start(engine.EffectRequest{Name: "later", Run: func(context.Context) (any, error) { return "later", nil }},
		func(v any, _ error) { got <- v })
	if v := recv(t, got, "done of a later effect"); v != "later" {
		t.Errorf("done of a later effect got %v, want %q (a pool slot leaked)", v, "later")
	}
}
