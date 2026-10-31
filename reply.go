// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// ReplyTo is the address an answer goes to. Ask creates it and passes it to build; the message
// carries it to whoever answers. It is a comparable value without pointers, so messages that hold
// it stay safe. The zero ReplyTo answers nothing.
type ReplyTo[R any] struct {
	addr Address // the ask address; its only field (MSG-400)
}

// Send answers the ask with v. The first reply attempt resolves the ask; later ones are dropped
// and traced. Send panics if from is nil or is a Context outside its turn.
func (rt ReplyTo[R]) Send(from Sender, v R) {
	sys, addr := replier(from)
	sys.replyAttempt(rt.addr, addr, v, nil)
}

// Fail answers the ask with err. It panics if err is nil, and like Send otherwise.
func (rt ReplyTo[R]) Fail(from Sender, err error) {
	sys, addr := replier(from)
	if err == nil {
		panic("lockstep: ReplyTo.Fail with nil error")
	}
	sys.replyAttempt(rt.addr, addr, nil, err)
}

// replier returns the system and the address of a reply attempt's sender, after MSG-410's first
// two checks: from nil and a Context outside its turn (both panics).
func replier(from Sender) (*System, Address) {
	sys, addr, _ := mustSender(from).sendOrigin()
	return sys, addr
}

// Reply is the typed future of one ask.
type Reply[R any] struct {
	sys  *System       // the asker system
	ask  Address       // the ask address; zero for a reply resolved before it was sent (MSG-300 step 3)
	done chan struct{} // closed at resolution (MSG-470)

	// entry is the ask's entry in the asker's table (asktable.go), allocated with the reply. A
	// reply resolved before it was sent leaves it unused.
	entry pendingAsk

	// val and err are written once, by the resolution (under the asker table's lock, or before
	// Ask returns an already-resolved reply), before resolved is set and done is closed; readers
	// load resolved, or receive from done, before they read them.
	resolved atomic.Bool
	val      R
	err      error

	// The Pipe continuations (MSG-600 to MSG-630). They run, in registration order, when the
	// resolution calls fire after it has released the ask table's lock, or at once for a Pipe
	// after that. pmu is held while they run, so a Pipe that races with fire cannot overtake
	// the continuations before it; they are runtime-only (they enqueue and schedule) and never
	// call user code. Lock order: pmu, then the cell's mu and the engine; nothing that holds
	// those takes pmu.
	pmu   sync.Mutex
	conts []func()
	fired bool
}

// newReply returns a pending reply on the asker system sys.
func newReply[R any](sys *System) *Reply[R] {
	return &Reply[R]{sys: sys, done: make(chan struct{})}
}

// resolvedReply returns a reply on sys already resolved with err (MSG-300 step 3).
func resolvedReply[R any](sys *System, err error) *Reply[R] {
	r := newReply[R](sys)
	r.resolve(nil, err)
	r.fire()
	return r
}

// resolve sets r's result, v asserted to R (a nil v, or one with a non-nil err, is the zero R),
// and closes done. It reports whether this call resolved r; a later call changes nothing
// (MSG-430). It is the resolver of r's table entry, called by settle under the table's lock,
// which has already turned a v that is not an R into an ErrWrongType error (wrongType).
func (r *Reply[R]) resolve(v any, err error) bool {
	if r.resolved.Load() {
		return false
	}
	if err == nil {
		r.val, _ = v.(R)
	}
	r.err = err
	r.resolved.Store(true)
	close(r.done)
	return true
}

// fire runs the continuations that Pipe registered, in the order of the Pipe calls, and makes a
// later Pipe run its continuation at once. The resolver of the reply's table entry calls it, after
// resolve and after the ask table's lock is released (MSG-610): a continuation takes the actor's
// mailbox lock and calls the engine's Ready, and the table's lock must not be held across that.
func (r *Reply[R]) fire() {
	r.pmu.Lock()
	defer r.pmu.Unlock()
	r.fired = true
	for _, f := range r.conts {
		f()
	}
	r.conts = nil
}

// then registers the continuation f (MSG-600): it runs when the reply's resolution fires, or now
// if that has happened. f runs while pmu is held and must not call back into r.
func (r *Reply[R]) then(f func()) {
	r.pmu.Lock()
	defer r.pmu.Unlock()
	if r.fired {
		f()
		return
	}
	r.conts = append(r.conts, f)
}

// isResolved reports, without waiting, whether the ask has resolved (hooks.AskResolved, SIM-200).
func (r *Reply[R]) isResolved() bool {
	if r == nil {
		panic("lockstep: nil Reply")
	}
	return r.resolved.Load()
}

// wrongType returns MSG-420's error for an answer v whose dynamic type is not R (a ReplyTo
// converted between instantiations, or a remote reply from other code), and nil for an R or a nil
// v. Type names are reflect's String, as in ACT's ErrWrongType texts.
func (r *Reply[R]) wrongType(ask Address, v any) error {
	if _, ok := v.(R); ok || v == nil {
		return nil
	}
	return fmt.Errorf("%w: reply to %s: got %s, want %s",
		ErrWrongType, ask, reflect.TypeOf(v).String(), reflect.TypeFor[R]().String())
}

// Await waits for the ask to resolve and returns its value or error. If ctx ends first it returns
// the zero R and ctx.Err(), and the ask stays pending. Called during a turn it returns
// ErrAwaitInActor at once. In simulation Await never waits and never drives the run: on a pending
// reply it fails the seed and returns an error wrapping ErrAwaitInActor, from a turn or from the
// test body (test bodies use sim.Ask). Await may be called any number of times from any
// goroutine; every call that sees the resolution returns the same result.
func (r *Reply[R]) Await(ctx context.Context) (R, error) {
	if r == nil {
		panic("lockstep: nil Reply")
	}
	if r.resolved.Load() { // MSG-450: even with ctx done, even during a turn
		return r.val, r.err
	}
	var zero R
	eng := &r.sys.eng
	if eng.Scheduler.InTurn() { // MSG-460
		eng.Failer.Fail(ErrAwaitInActor)
		return zero, ErrAwaitInActor
	}
	// MSG-470: ctx.Err() from the local engine, the simulation engine's error wrapping
	// ErrAwaitInActor, enginetest.ErrWouldBlock: each returned unchanged.
	if err := eng.Waiter.Wait(ctx, r.done); err != nil {
		return zero, err
	}
	return r.val, r.err
}
