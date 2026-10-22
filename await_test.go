// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// AT-MSG-21: MSG-410. A local reply attempt resolves the ask before ReplyTo.Send returns, so the
// asker's Await returns while the replier's turn is still running.
func TestATMSG21LocalResolutionIsSynchronous(t *testing.T) {
	sys := newLocalSystem(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock) // before the engine stops (cleanups run last-registered first)
	replier, err := Spawn(sys, "replier", actorOf[ReplyTo[int]](&fnActor[ReplyTo[int]]{
		receive: func(ctx *Context[ReplyTo[int]], rt ReplyTo[int]) {
			rt.Send(ctx, 5)
			<-release
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	r := Ask(sys, replier, func(rt ReplyTo[int]) ReplyTo[int] { return rt })
	if v, err := r.Await(within(t, time.Second)); v != 5 || err != nil {
		t.Errorf("Await while the replier is blocked = %d, %v, want 5, nil", v, err)
	}
	unblock()
}

// AT-MSG-22: MSG-440, MSG-450, MSG-470, MSG-480. A cancelled Await leaves the ask pending; every
// Await after the resolution, concurrent or with a done ctx, returns the same result; a nil Reply
// panics.
func TestATMSG22AwaitSemantics(t *testing.T) {
	sys := newLocalSystem(t)
	silent, stored := spawnSilent(t, sys)
	r := askSilent(sys, silent, WithTimeout(time.Hour))
	recv(t, stored)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Millisecond, cancel)
	if v, err := r.Await(ctx); v != 0 || err != context.Canceled { //nolint:errorlint // Await returns ctx.Err() unwrapped
		t.Errorf("Await with a ctx cancelled after 10 ms = %d, %v, want 0, context.Canceled", v, err)
	}
	if r.resolved.Load() {
		t.Fatal("a cancelled Await resolved the ask")
	}

	type result struct {
		v   int
		err error
	}
	results := make(chan result, 10)
	for range 10 {
		go func() {
			v, err := r.Await(within(t, 5*time.Second))
			results <- result{v, err}
		}()
	}
	send(t, sys, silent, any(answer{7}))
	for range 10 {
		if got := recv(t, results); got.v != 7 || got.err != nil {
			t.Errorf("concurrent Await = %d, %v, want 7, nil", got.v, got.err)
		}
	}
	if v, err := r.Await(cancelled()); v != 7 || err != nil {
		t.Errorf("Await with a done ctx after the resolution = %d, %v, want 7, nil", v, err)
	}
	if v := mustPanic(t, func() { _, _ = (*Reply[int])(nil).Await(context.Background()) }); v != "lockstep: nil Reply" {
		t.Errorf("Await on a nil Reply panicked with %v", v)
	}
}

// awaitResult is what an actor's Await inside its turn returned, and the reply it awaited.
type awaitResult struct {
	v     int
	err   error
	reply *Reply[int]
}

// awaitInReceive spawns "a", an int actor whose Receive asks silent with a 50 ms timeout, awaits
// the reply inside the turn and sends the result to the returned channel.
func awaitInReceive(t *testing.T, sys *System, silent PID[any]) (PID[int], <-chan awaitResult) {
	t.Helper()
	out := make(chan awaitResult, 1)
	a, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(ctx *Context[int], _ int) {
			r := askSilent(ctx, silent, WithTimeout(50*time.Millisecond))
			v, err := r.Await(context.Background())
			out <- awaitResult{v, err, r}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return a, out
}

// AT-MSG-23 (local and manual clauses): MSG-460. Await on a pending reply inside a turn returns
// ErrAwaitInActor without waiting and calls Failer.Fail; the ask still resolves later.
func TestATMSG23AwaitInsideActor(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		sys := newLocalSystem(t)
		silent, _ := spawnSilent(t, sys)
		a, out := awaitInReceive(t, sys, silent)
		send(t, sys, a, 1)
		// The result arrives within 1 s, so the turn did not block.
		got := recv(t, out)
		if got.v != 0 || got.err != ErrAwaitInActor { //nolint:errorlint // not wrapped (MSG §7)
			t.Errorf("Await inside Receive = %d, %v, want 0, ErrAwaitInActor", got.v, got.err)
		}
		// Handed to the test goroutine, the reply resolves at its timeout (m18: Pipe is 1.2-09's).
		if _, err := got.reply.Await(within(t, time.Second)); !errors.Is(err, ErrTimeout) {
			t.Errorf("Await of the same reply from the test = %v, want ErrTimeout", err)
		}
	})
	t.Run("manual", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		silent, _ := spawnSilent(t, sys)
		a, out := awaitInReceive(t, sys, silent)
		eng.RunUntilIdle()
		send(t, sys, a, 1)
		eng.RunUntilIdle()
		got := <-out
		if got.v != 0 || got.err != ErrAwaitInActor { //nolint:errorlint // not wrapped (MSG §7)
			t.Errorf("Await inside Receive = %d, %v, want 0, ErrAwaitInActor", got.v, got.err)
		}
		if f := eng.Failures(); len(f) != 1 || f[0] != ErrAwaitInActor { //nolint:errorlint // the sentinel itself
			t.Errorf("Failures() = %v, want [ErrAwaitInActor]", f)
		}
		eng.Advance(50 * time.Millisecond)
		if _, err := got.reply.Await(context.Background()); !errors.Is(err, ErrTimeout) {
			t.Errorf("Await of the same reply after the timeout = %v, want ErrTimeout", err)
		}
	})
}

// AT-ENG-25 (InTurn and Await clauses): ENG-130, ENG-135, MSG-460, MSG-470. InTurn is true inside
// Receive and false between steps; the manual Waiter answers ErrWouldBlock for an unanswered ask
// and nil once the answer has been delivered.
func TestATENG25InTurnAwait(t *testing.T) {
	sys, eng := newManualSystem(t)
	silent, _ := spawnSilent(t, sys)
	var inTurn []bool
	var inside awaitResult
	a, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(ctx *Context[int], _ int) {
			inTurn = append(inTurn, sys.eng.Scheduler.InTurn())
			r := askSilent(ctx, silent, WithTimeout(time.Hour))
			inside.v, inside.err = r.Await(context.Background())
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, a, 1)
	for eng.Step() {
		if sys.eng.Scheduler.InTurn() {
			t.Error("InTurn() is true between Step calls")
		}
	}
	if len(inTurn) != 1 || !inTurn[0] {
		t.Errorf("InTurn() inside Receive: %v, want [true]", inTurn)
	}
	if inside.err != ErrAwaitInActor { //nolint:errorlint // not wrapped (MSG §7)
		t.Errorf("Await inside Receive = %v, want ErrAwaitInActor", inside.err)
	}
	if f := eng.Failures(); len(f) != 1 {
		t.Errorf("Failures() = %v, want one error", f)
	}

	r := askSilent(sys, silent, WithTimeout(time.Hour))
	if _, err := r.Await(context.Background()); !errors.Is(err, enginetest.ErrWouldBlock) {
		t.Errorf("Await of an unanswered ask on the manual engine = %v, want ErrWouldBlock", err)
	}
	send(t, sys, silent, any(answer{9}))
	if v, err := r.Await(context.Background()); v != 9 || err != nil {
		t.Errorf("Await once the answer is queued = %d, %v, want 9, nil", v, err)
	}
	if f := eng.Failures(); len(f) != 1 {
		t.Errorf("Failures() after the test's Await calls = %v, want still one", f)
	}
}
