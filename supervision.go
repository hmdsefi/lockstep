// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

// Fail marks the actor failed with err when the current handler returns. Only the first call per
// handler counts. It panics if err is nil.
func (c *Context[M]) Fail(err error) {
	c.mustInTurn()
	if err == nil {
		// Inside the handler this panic is recovered as a failure with this PanicError (SUP-140).
		panic("lockstep: Fail called with nil error")
	}
	if c.failure == nil {
		c.failure = err
	}
}

// Stop stops the actor with reason Normal when the current handler returns. No further user
// message is processed by this incarnation.
func (c *Context[M]) Stop() {
	c.mustInTurn()
	// Inside Stopped, the only handler of an ending incarnation, it does nothing (SUP-050,
	// SUP-100).
	if c.cell.ending() {
		return
	}
	// The request goes on the system queue, which the turn processes as soon as the handler
	// returns and before it pops another user message (SUP-010).
	c.cell.requestStop(Normal, c.cell)
}
