// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

// The hold hook of MBX §10 is for tests: while a cell is held it runs no turn, so its mailbox
// fills without being consumed. Production code never holds a cell. The fields held and
// heldReady of cell are guarded by cell.mu, so a turn that is running when the hold starts
// processes no further message: it checks held under mu before every pop.

// holdTurns holds c: RunTurn processes nothing and returns false, and every Ready the runtime
// would call for c is remembered. The returned function releases the hold and calls Ready once if
// any was remembered; calling it again does nothing.
func (c *cell[M]) holdTurns() (release func()) {
	c.mu.Lock()
	c.held = true
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		schedule := c.held && c.heldReady
		c.held, c.heldReady = false, false
		c.mu.Unlock()
		if schedule {
			c.sys.ready(c, nil)
		}
	}
}

// holding reports whether c is held and, if it is, remembers that c has work to schedule once
// released. The caller holds c.mu.
func (c *cell[M]) holding() bool {
	if c.held {
		c.heldReady = true
	}
	return c.held
}
