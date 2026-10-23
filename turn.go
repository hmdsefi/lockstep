// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// RunTurn runs one turn of the actor (engine.ActorRef, ENG-310): system messages first, every one
// of them, including those that arrive during the turn, each between two handler calls; between
// them up to budget user messages while the cell is Running (SUP-010). A handler that fails ends
// the popping of user messages for the turn: the cell is Suspended. RunTurn reports whether the
// actor still has work (ENG-320). It never blocks, starts no goroutine and recovers every panic of
// actor code (ENG-330, SUP-140). The engine re-queues the cell when it returns true; the runtime
// never calls Ready for that (ENG-350).
func (c *cell[M]) RunTurn(budget int) (more bool) {
	if budget < 1 {
		panic("lockstep: RunTurn budget " + strconv.Itoa(budget) + " < 1")
	}
	c.mu.Lock()
	held := c.holding()
	c.mu.Unlock()
	if held {
		return false // held by a test (hold.go): nothing runs until the release schedules the cell
	}
	var system, u int
	var first any
	var firstShown bool // the first user message has a description for lockstep.turn
	for {
		if m, ok := c.popSystem(); ok {
			c.processSystem(m)
			system++
			continue
		}
		if u < budget && c.accepting() {
			if e, ok := c.popUser(); ok {
				msg, shown := c.processUser(e)
				if u == 0 {
					first, firstShown = msg, shown
				}
				u++
				continue
			}
		}
		break
	}
	if system+u > 0 {
		c.recordTurn(system, firstShown, u, first)
	}
	return c.hasWork()
}

// accepting reports whether the cell pops user messages: only in Running (SUP-010, ENG-310 step
// 2), so not before the incarnation has started, after a handler failed, or once a stop started.
func (c *cell[M]) accepting() bool { return c.state == supervise.Running }

// popSystem and popUser pop under c.mu, so a hold that starts during a turn stops it at the next
// pop (hold.go); a held cell pops nothing.
func (c *cell[M]) popSystem() (sysMsg, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.holding() {
		return nil, false
	}
	return c.sysq.Pop()
}

// popUser returns the oldest user entry. The queue's kind is the authority for e.kind.
func (c *cell[M]) popUser() (entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.holding() {
		return entry{}, false
	}
	e, k, ok := c.user.Pop()
	e.kind = entryKindOf(k)
	return e, ok
}

// hasWork is RunTurn's result (ENG-320). A held cell has none: its release schedules it.
func (c *cell[M]) hasWork() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.holding() {
		return false
	}
	return c.sysq.Len() > 0 || c.accepting() && c.user.Len() > 0
}

// recordTurn emits lockstep.turn for a turn that processed system system messages and u user
// messages, the first of which was first (ENG-340). shown is false when that message has no
// description: a discarded or failed piped reply.
func (c *cell[M]) recordTurn(system int, shown bool, u int, first any) {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	msg := ""
	if u > 0 && shown {
		msg = tr.Describe(first)
	}
	tr.Record("lockstep.turn",
		engine.Attr{Key: "actor", Value: c.addr.String()},
		engine.Attr{Key: "system", Value: strconv.Itoa(system)},
		engine.Attr{Key: "user", Value: strconv.Itoa(u)},
		engine.Attr{Key: "msg", Value: msg},
	)
}

// processSystem handles one system message, in every state but Stopped (SUP-010): a Stopped cell
// has closed its queues, so it pops none.
func (c *cell[M]) processSystem(m sysMsg) {
	switch m := m.(type) {
	case begin:
		c.begin()
	case signalMsg:
		c.receiveSignal(m)
	case stop:
		c.startStop(m.reason)
	case stopped:
		c.sup.childStopped(m.child)
		c.childStopped(m.child)
	case failed:
		// Decided in Running (SUP-170) and kept in Suspended until a resume (SUP-160). Starting
		// has no failed child, and Restarting, Stopping and Stopped discard it: the children are
		// being stopped.
		switch c.state {
		case supervise.Running:
			c.decideChild(m)
		case supervise.Suspended:
			c.keep(m)
		}
	case ended:
		// The restart of a child goes on in Running (SUP-190) and after a resume (SUP-160). In
		// Restarting and Stopping the child is being stopped.
		switch c.state {
		case supervise.Running:
			c.sup.childEnded(c.sys, m, c)
		case supervise.Suspended:
			c.keep(m)
		}
	case cleanupFailed:
		// A child's Stopped failed (SUP-110): it is never decided and never counted. Its
		// ChildFailed{Cleanup: true} is a signal like any other (SUP-320).
		c.receiveSignal(m.asSignal())
	case end:
		c.endIncarnation(m)
	case resume:
		c.resumeIncarnation(m)
	}
}

// begin processes begin in Starting (SUP-030), in one turn: it calls the factory as a guarded
// call; on a non-nil actor it numbers the new incarnation (SUP-020), decides its optional
// interfaces once (ACT-100), records lockstep.start, calls Started and changes to Running. A
// factory that panics or returns nil fails with source factory: no incarnation begins and inc is
// unchanged (SUP-040). In Restarting, once the incarnation has ended, begin changes to Starting
// first (SUP-190); in any other state it is discarded.
func (c *cell[M]) begin() {
	if c.state == supervise.Restarting && c.ctx == nil && c.waitChild.IsZero() {
		c.setState(supervise.Starting)
	}
	if c.state != supervise.Starting {
		return
	}
	var a Actor[M]
	failed, err := guard(func() { a = c.factory() })
	if !failed && a == nil {
		failed, err = true, errNilActor
	}
	if failed {
		c.handlerFailed(supervise.InFactory, err)
		return
	}
	c.inc++
	c.mu.Lock()
	c.liveInc = c.inc // Pipe's continuations enqueue for this incarnation from now on (MSG-610)
	c.seenInc = c.inc
	c.mu.Unlock()
	c.actor = a
	c.ctx = &Context[M]{cell: c, inc: c.inc}
	c.starter, _ = a.(Starter[M])
	c.stopper, _ = a.(Stopper[M])
	c.signals, _ = a.(SignalReceiver[M])
	c.recordStart()
	if c.starter != nil {
		// OVL §3: a rebuild context is the Started of an incarnation a restart created (2 or
		// later) and the first Started of an actor spawned from a rebuild context. Spawns from it
		// are reserve spawns, so a restarted subtree rebuilds within the cap's reserve.
		c.ctx.rebuild = c.inc >= 2 || c.rebuildSpawn
		c.callHandler(supervise.InStarted, func() { c.starter.Started(c.ctx) })
		c.ctx.rebuild = false
	}
	if c.state == supervise.Starting { // Started did not fail
		c.setState(supervise.Running)
	}
}

// processUser handles one user message: it hands a Sent entry to Receive and a Self entry to
// processSelf. It returns the message that the turn's lockstep.turn record describes, and false
// when there is none (ENG-340).
func (c *cell[M]) processUser(e entry) (msg any, shown bool) {
	if e.kind == entrySelf {
		return c.processSelf(e)
	}
	m, _ := e.msg.(M) // a nil e.msg of an interface type M is M's zero value
	// A failing Receive consumes its message: it is never redelivered (SUP-150), and it stays the
	// turn record's description.
	c.callHandler(supervise.InReceive, func() { c.actor.Receive(c.ctx, m) })
	return e.msg, true
}
