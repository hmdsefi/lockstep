// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist

import "github.com/hmdsefi/lockstep"

// Context is passed to Handle, Then callbacks and the optional hooks. It embeds the actor's
// lockstep context, so it is a lockstep.Sender and lockstep.Spawner and offers Self, Now, After,
// Rand and the other Context methods. Stop and Fail are overridden: see their documentation.
type Context[C, E, S any] struct {
	*lockstep.Context[C]

	w *wrapper[C, E, S]

	// The fields below belong to the Handle call that runs, if any: handling is set during it,
	// thens holds its Then callbacks in registration order, and failed records a Fail call.
	handling bool
	thens    []func(ctx *Context[C, E, S], state S)
	failed   bool

	// snapshot records a Snapshot call since the last snapshot started (PER-170).
	snapshot bool
}

// PersistenceID returns the persistence ID given to Spawn.
func (c *Context[C, E, S]) PersistenceID() string { return c.w.id }

// LastSeq returns the sequence number of the last committed and applied event.
func (c *Context[C, E, S]) LastSeq() uint64 { return c.w.lastSeq }

// Then registers fn to run after the events returned by the current Handle call are committed
// and applied, with the new state; at once after Handle returns if it returns no events. It
// panics outside Handle.
func (c *Context[C, E, S]) Then(fn func(ctx *Context[C, E, S], state S)) {
	if !c.handling {
		panic("persist: Then called outside Handle")
	}
	if fn == nil {
		panic("persist: Then called with a nil callback")
	}
	c.thens = append(c.thens, fn)
}

// Snapshot requests a snapshot of the state after the current command's events are applied.
func (c *Context[C, E, S]) Snapshot() {
	if c.w.cfg.Snapshots != nil {
		c.snapshot = true
	}
}

// Stop stops the actor with reason Normal, as lockstep's Context.Stop. Inside a Handle call that
// returns events, the stop waits until those events are committed and the Then callbacks ran.
func (c *Context[C, E, S]) Stop() {
	c.Context.Stop()
}

// Fail marks the actor failed, as lockstep's Context.Fail: the failure takes effect when the
// current handler returns, and Fail panics if err is nil. Events returned by the same Handle call
// are discarded.
func (c *Context[C, E, S]) Fail(err error) {
	c.Context.Fail(err)
	if c.handling {
		c.failed = true
	}
}
