// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// The in-place restart of SUP-190, child side. The supervisor's end{inc, err} ends the
// incarnation: the cell changes to Restarting, stops its children one at a time like the stop
// sequence (stop.go), then calls Stopped with Restarting(err), closes the owned resources, records
// lockstep.restart and pushes ended to the supervisor. The cell then waits, with no live
// incarnation, for the begin the supervisor pushes after the backoff; begin in Restarting starts
// the next incarnation (SUP-030). The PID, the registry entry and the mailbox with its sent
// entries survive (MBX-010); the Self entries of the ended incarnation are dropped when popped
// (MBX-170).

// endIncarnation processes end{inc, err} (SUP-190 steps 1 and 2). It acts only in Running or
// Suspended for the current incarnation number; any other end is discarded. A cell whose factory
// failed has no live incarnation: it calls no Stopped, closes nothing and has no child, but still
// records lockstep.restart and pushes ended for the number of the last incarnation that began, 0
// if none, which the failed notice carried.
func (c *cell[M]) endIncarnation(m end) {
	if c.state != supervise.Running && c.state != supervise.Suspended || m.inc != c.inc {
		return
	}
	c.setState(supervise.Restarting)
	c.endErr = m.err
	c.endIncarnationWork()
	c.stopNextChild() // finishEnd once every child has stopped
}

// finishEnd completes the restart's end of the incarnation once its children have stopped
// (SUP-190 steps 3 and 4): Stopped(ctx, Restarting(err)) if implemented, the owned resources
// closed, then the incarnation is gone (Pipe's continuations drop, MSG-610), lockstep.restart is
// recorded and ended is pushed to the supervisor. A Stopped failure pushes cleanupFailed before
// ended.
func (c *cell[M]) finishEnd() {
	err := c.endErr
	c.endErr = nil
	if c.ctx != nil {
		if c.stopper != nil {
			c.callStopped(Restarting(err))
		}
		c.closeOwned()
	}
	c.mu.Lock()
	c.liveInc = 0
	c.mu.Unlock()
	// The next incarnation gets a new Context, so a new owned list and watched set (SUP-120,
	// SUP-340), and its own optional interfaces (ACT-100). Until then nothing is live: a stop now
	// calls no Stopped (SUP-290).
	c.actor, c.ctx = nil, nil
	c.starter, c.stopper, c.signals = nil, nil, nil
	c.recordRestart(err)
	c.sys.notifySupervisor(c.parent, ended{child: c.addr, inc: c.inc}, c)
}

// recordRestart emits lockstep.restart (SUP §8) for the incarnation c.inc, ended for a restart
// caused by err.
func (c *cell[M]) recordRestart(err error) {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.restart",
		engine.Attr{Key: "actor", Value: c.addr.String()},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(c.inc), 10)},
		engine.Attr{Key: "err", Value: tr.Describe(err)},
	)
}
