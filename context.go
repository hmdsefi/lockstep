// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Context is an incarnation's handle to the runtime. The runtime passes the same *Context to
// every turn of one incarnation and a new one to the next incarnation. Self and Parent may be
// called at any time; every other method panics when called outside a turn of its incarnation.
// Every method panics on a nil *Context.
type Context[M any] struct {
	cell   *cell[M]
	inc    uint32      // the incarnation number (SUP-020)
	inTurn atomic.Bool // set for the duration of each handler call of this incarnation (ACT-120)

	// failure is the first Fail argument of the handler call that runs, nil if none (SUP-140).
	// Turn-owned: callHandler clears it before each handler call and reads it after.
	failure error

	// watched is the incarnation's watched set (ACT-600). Turn-owned: only Watch, Unwatch and
	// signal delivery, all in a turn of this incarnation, touch it; so does the stop sequence,
	// which clears it in the turn that starts the stop (SUP-060 step 1).
	watched watchedSet
}

// Self returns the actor's PID. It is the same for every incarnation of the actor.
func (c *Context[M]) Self() PID[M] {
	c.mustNotNil()
	return PID[M]{addr: c.cell.addr}
}

// Parent returns the parent's address: the user guardian's for actors spawned from a System.
func (c *Context[M]) Parent() Address {
	c.mustNotNil()
	return c.cell.parent
}

// mustNotNil panics on a nil *Context (ACT §7). Self and Parent, which work outside a turn, call it
// first; every other method gets it from mustInTurn.
func (c *Context[M]) mustNotNil() {
	if c == nil {
		panic("lockstep: nil Context")
	}
}

// mustInTurn panics on a nil *Context (ACT §7) and unless a handler of c's incarnation is running
// (ACT-120). Every Context method but Self and Parent, and every function given c as Spawner,
// Sender or ctx, calls it first.
func (c *Context[M]) mustInTurn() {
	c.mustNotNil()
	if !c.inTurn.Load() {
		panic("lockstep: Context of " + c.cell.addr.String() + " used outside its turn")
	}
}

func (*Context[M]) spawner() {}
func (*Context[M]) sender()  {}

// nilSpawner reports whether c is a nil *Context (ACT-200).
func (c *Context[M]) nilSpawner() bool { return c == nil }

// spawnParent returns the system, the actor as the parent's address, and the actor's cell, the
// turn that spawns. It panics outside a turn (ACT-300 step 2).
func (c *Context[M]) spawnParent() (*System, Address, engine.ActorRef) {
	c.mustInTurn()
	return c.cell.sys, c.cell.addr, c.cell
}

// parentStopping reports whether the actor is Stopping, Restarting or Stopped (SUP-090). Only
// Stopped runs in a turn then.
func (c *Context[M]) parentStopping() bool { return c.cell.ending() }
