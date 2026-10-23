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
