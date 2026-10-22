// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
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
	done chan struct{} // closed at resolution (MSG-470)

	// val and err are written once, by the resolution (under the asker table's lock, or before
	// Ask returns an already-resolved reply), before resolved is set and done is closed; readers
	// load resolved, or receive from done, before they read them.
	resolved atomic.Bool
	val      R
	err      error
}

// newReply returns a pending reply on the asker system sys.
func newReply[R any](sys *System) *Reply[R] {
	return &Reply[R]{sys: sys, done: make(chan struct{})}
}

// resolvedReply returns a reply on sys already resolved with err (MSG-300 step 3).
func resolvedReply[R any](sys *System, err error) *Reply[R] {
	r := newReply[R](sys)
	r.resolve(nil, err)
	return r
}

// resolve sets r's result, v asserted to R (a nil v, or one with a non-nil err, is the zero R),
// and closes done. It reports whether this call resolved r; a later call changes nothing
// (MSG-430). It is the untyped resolver of r's table entry, called by settle under the table's
// lock.
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
