// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// RunTurn runs one turn of the actor (engine.ActorRef, ENG-310): system messages first, every one
// of them, including those that arrive during the turn; between them up to budget user messages
// while the incarnation accepts them. It reports whether the actor still has work (ENG-320). It
// never blocks, starts no goroutine and recovers every panic of actor code (ENG-330). The engine
// re-queues the cell when it returns true; the runtime never calls Ready for that (ENG-350).
func (c *cell[M]) RunTurn(budget int) (more bool) {
	if budget < 1 {
		panic("lockstep: RunTurn budget " + strconv.Itoa(budget) + " < 1")
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

// accepting reports whether the incarnation takes user messages: it has started and no handler
// failed (ENG-310 step 2).
func (c *cell[M]) accepting() bool { return c.state == cellRunning }

func (c *cell[M]) popSystem() (sysMsg, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sysq.pop()
}

func (c *cell[M]) popUser() (entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.user.pop()
}

// hasWork is RunTurn's result (ENG-320).
func (c *cell[M]) hasWork() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sysq.size() > 0 || c.accepting() && c.user.size() > 0
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

// processSystem handles one system message.
func (c *cell[M]) processSystem(m sysMsg) {
	switch m := m.(type) {
	case begin:
		c.begin()
	case signalMsg:
		c.deliverSignal(m.sig)
	}
}

// begin processes the begin message (ACT-340, SUP-030): it calls the factory, numbers the new
// incarnation, decides its optional interfaces once (ACT-100), calls Started and lets the
// incarnation take user messages.
func (c *cell[M]) begin() {
	if c.state != cellStarting {
		return
	}
	a, err := c.callFactory()
	if err == nil && a == nil {
		err = errNilActor
	}
	if err != nil {
		c.handlerFailed(err) // no incarnation begins and inc is unchanged (SUP-040)
		return
	}
	c.inc++
	c.actor = a
	c.ctx = &Context[M]{cell: c, inc: c.inc}
	c.starter, _ = a.(Starter[M])
	c.stopper, _ = a.(Stopper[M])
	c.signals, _ = a.(SignalReceiver[M])
	if c.starter != nil {
		c.callStarted()
	}
	if c.state == cellStarting {
		c.state = cellRunning
	}
}

// callFactory calls the factory, turning a panic into an error.
func (c *cell[M]) callFactory() (a Actor[M], err error) {
	defer func() {
		if p := recover(); p != nil {
			a, err = nil, &handlerPanic{value: p}
		}
	}()
	return c.factory(), nil
}

func (c *cell[M]) callStarted() {
	c.enter()
	defer c.leave()
	c.starter.Started(c.ctx)
}

// processUser handles one user message: it hands a Sent entry to Receive and a Self entry to
// processSelf. It returns the message that the turn's lockstep.turn record describes, and false
// when there is none (ENG-340).
func (c *cell[M]) processUser(e entry) (msg any, shown bool) {
	if e.kind == entrySelf {
		return c.processSelf(e)
	}
	m, _ := e.msg.(M)        // a nil e.msg of an interface type M is M's zero value
	msg, shown = e.msg, true // set before Receive: a panic of Receive still leaves the description
	c.enter()
	defer c.leave()
	c.actor.Receive(c.ctx, m)
	return msg, shown
}

// enter sets the in-turn flag before a handler call (ACT-120).
func (c *cell[M]) enter() { c.ctx.inTurn.Store(true) }

// leave runs deferred after a handler call: it clears the in-turn flag and hands a panic of the
// handler to handlerFailed (ENG-330).
func (c *cell[M]) leave() {
	c.ctx.inTurn.Store(false)
	if p := recover(); p != nil {
		c.handlerFailed(&handlerPanic{value: p})
	}
}
