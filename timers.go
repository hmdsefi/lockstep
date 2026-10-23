// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"strconv"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// timerLabel is the label of the engine timers of After and Every (ENG-070).
const timerLabel = "lockstep/timer"

// dropTimerCancelled is the lockstep.drop reason of a timer message whose timer was cancelled
// before the message was popped (TMR-070).
const dropTimerCancelled = "timer-cancelled"

// TimerID identifies a timer of one actor. IDs are unique within the actor for its whole life,
// across restarts. 0 is never a valid timer.
type TimerID uint64

// timer is one timer of a cell (TMR §11). The fire closure captures the *timer, so a timer that
// is no longer active sees active == false without a map read. id, every, d, msg, desc and inc
// never change; active, queued and cancel are guarded by the cell's tmu.
type timer[M any] struct {
	id     TimerID
	every  bool          // Every: re-arms with fixed delay d
	d      time.Duration // the requested delay for After (negative as requested), the interval for Every
	msg    M
	desc   string // Tracer.Describe(msg), computed once by setTimer outside every lock; "" when tracing is off
	inc    uint32 // the incarnation that set the timer
	active bool
	queued bool          // the timer's last message is in the mailbox and was not popped
	cancel engine.Cancel // of the armed AfterFunc
}

// Now returns the engine clock: wall time in production, the node's local clock in simulation,
// where faultline's clock drift and jumps apply.
func (c *Context[M]) Now() time.Time {
	c.mustInTurn()
	return c.cell.sys.eng.Clock.Now()
}

// After delivers msg to the actor itself once, d from now on the engine clock. A negative d is
// treated as 0. The timer is cancelled if the incarnation ends first. Called from Stopped it
// schedules nothing and returns 0.
func (c *Context[M]) After(d time.Duration, msg M) TimerID {
	c.mustInTurn()
	return c.setTimer(d, msg, false)
}

// Every delivers msg to the actor itself every d, starting d from now, until the timer is
// cancelled or the incarnation ends. While a previous message of the timer is still in the
// mailbox, a firing delivers nothing. It panics if d <= 0. Called from Stopped it schedules
// nothing and returns 0.
func (c *Context[M]) Every(d time.Duration, msg M) TimerID {
	c.mustInTurn()
	if d <= 0 {
		panic("lockstep: Every interval must be positive, got " + d.String())
	}
	return c.setTimer(d, msg, true)
}

// CancelTimer stops timer id. It reports whether the timer was active. Once it returns, the actor
// receives no message from that timer, even one already in the mailbox.
func (c *Context[M]) CancelTimer(id TimerID) bool {
	c.mustInTurn()
	cl := c.cell
	cl.tmu.Lock()
	defer cl.tmu.Unlock()
	// cl.timers holds only timers of the live incarnation (cancelTimers empties it when one ends),
	// but the incarnation is checked too (TMR-090).
	t := cl.timers[id]
	if t == nil || !t.active || t.inc != c.inc {
		return false
	}
	// A message the callback has already enqueued stays in the mailbox; its timer is gone from
	// cl.timers, so popTimer finds none and the pop records it as timer-cancelled (TMR-070). A
	// callback that runs after this critical section finds the timer inactive.
	t.active = false
	t.cancel()
	delete(cl.timers, id)
	cl.recordTimer(t.inc, t.id, "cancel")
	return true
}

// setTimer allocates a TimerID, registers an active timer of c's incarnation, arms its first
// firing on the engine clock and records lockstep.timer op set (TMR-020, TMR-030, TMR-040).
// The firing never runs inside the call (ENG-070). The armed AfterFunc is stored, and set is
// recorded, before the timer lock is released, so a callback on another goroutine finds the
// timer complete and records fire after set.
//
// Inside Stopped the incarnation is ending: setTimer schedules nothing, leaves the counter
// unchanged, records op refused and returns 0 (TMR-100).
func (c *Context[M]) setTimer(d time.Duration, msg M, every bool) TimerID {
	cl := c.cell
	tr := cl.sys.eng.Tracer
	if cl.ending() {
		if tr.Enabled() {
			cl.recordTimer(c.inc, 0, "refused",
				engine.Attr{Key: "d", Value: d.String()},
				engine.Attr{Key: "repeat", Value: strconv.FormatBool(every)},
			)
		}
		return 0
	}
	desc := ""
	if tr.Enabled() {
		desc = tr.Describe(msg)
	}
	cl.tmu.Lock()
	defer cl.tmu.Unlock()
	cl.nextTimer++
	t := &timer[M]{id: cl.nextTimer, every: every, d: d, msg: msg, desc: desc, inc: c.inc, active: true}
	if cl.timers == nil {
		cl.timers = make(map[TimerID]*timer[M])
	}
	cl.timers[t.id] = t
	t.cancel = cl.sys.eng.Clock.AfterFunc(max(d, 0), timerLabel, func() { cl.fireTimer(t) })
	if tr.Enabled() {
		cl.recordTimer(t.inc, t.id, "set",
			engine.Attr{Key: "d", Value: d.String()},
			engine.Attr{Key: "repeat", Value: strconv.FormatBool(every)},
			engine.Attr{Key: "msg", Value: desc},
		)
	}
	return t.id
}

// cancelTimers ends the timers of the incarnation that is ending (TMR-080, SUP-060 step 1,
// SUP-190 step 1): each becomes inactive, its armed AfterFunc is cancelled, and the cell forgets
// it. A message one of them already enqueued is dropped when popped (MBX-170) or at the stop's
// drain (MBX-160). A callback running concurrently either enqueued before this critical section
// or finds its timer inactive after it.
func (c *cell[M]) cancelTimers() {
	c.tmu.Lock()
	defer c.tmu.Unlock()
	for _, t := range c.timers { //lockstep:maporder each Cancel only removes a pending engine callback, so the order is unobservable (TMR §6)
		t.active = false
		t.cancel()
	}
	clear(c.timers)
}

// fireTimer is the callback of t's AfterFunc, which runs in engine context (TMR-050). A timer that
// is no longer active does nothing. An Every timer arms its next firing before anything is
// enqueued (TMR-030) and records skip instead of enqueueing while its message is queued
// (TMR-060). Otherwise the timer message, a Self entry of the timer's incarnation that the mailbox
// always admits (MBX-030), is enqueued and lockstep.timer op fire is recorded.
//
// Lock order: tmu, then offerSelf's mu and the queue's lock.
func (c *cell[M]) fireTimer(t *timer[M]) {
	c.tmu.Lock()
	defer c.tmu.Unlock()
	if !t.active {
		return
	}
	tr := c.sys.eng.Tracer
	if t.every {
		t.cancel = c.sys.eng.Clock.AfterFunc(t.d, timerLabel, func() { c.fireTimer(t) })
		if t.queued {
			if tr.Enabled() {
				c.recordTimer(t.inc, t.id, "skip")
			}
			return
		}
	}
	e := entry{msg: t.msg, kind: entrySelf, inc: t.inc, timer: t.id}
	if !c.offerSelf(e) {
		// The incarnation has ended or the cell has stopped: nothing will pop this message. Ending
		// an incarnation runs cancelTimers first, which makes its timers inactive, so this only
		// guards against a timer outliving its incarnation.
		t.active = false
		if t.every {
			t.cancel()
		}
		delete(c.timers, t.id)
		return
	}
	t.queued = true
	if tr.Enabled() {
		// The description setTimer computed outside every lock: no user code runs under tmu.
		c.recordTimer(t.inc, t.id, "fire", engine.Attr{Key: "msg", Value: t.desc})
	}
}

// recordTimer emits lockstep.timer (TMR §8) for timer id of incarnation inc with operation op,
// followed by the attributes more. A caller that holds c.tmu orders the records of one timer.
func (c *cell[M]) recordTimer(inc uint32, id TimerID, op string, more ...engine.Attr) {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	attrs := append([]engine.Attr{
		{Key: "actor", Value: c.addr.String()},
		{Key: "inc", Value: strconv.FormatUint(uint64(inc), 10)},
		{Key: "timer", Value: strconv.FormatUint(uint64(id), 10)},
		{Key: "op", Value: op},
	}, more...)
	tr.Record("lockstep.timer", attrs...)
}

// processTimer handles the timer message e that the turn popped (TMR-070). It clears the timer's
// queued mark and makes an After timer inactive. The message reaches Receive, in this turn, only
// if the timer was active and its incarnation is the current one; otherwise it is recorded as
// lockstep.drop with reason incarnation-ended (MBX-170) or timer-cancelled. It returns the message
// that the turn's lockstep.turn record describes, and false when there is none (ENG-340).
//
// The turn popped e under c.mu and released it before taking the timer lock here, so the lock
// order stays tmu before mu.
func (c *cell[M]) processTimer(e entry) (msg any, ok bool) {
	if e.inc != c.inc {
		c.sys.recordDropEnded(c.addr, e.inc, e.msg)
		return nil, false
	}
	if !c.popTimer(e.timer) {
		c.recordDropCancelled(e)
		return nil, false
	}
	m, _ := e.msg.(M) // a nil e.msg of an interface type M is M's zero value
	c.callHandler(supervise.InReceive, func() { c.actor.Receive(c.ctx, m) })
	return e.msg, true
}

// popTimer updates the timer id for the pop of its message and reports whether the message is
// delivered: the timer exists and is active.
func (c *cell[M]) popTimer(id TimerID) bool {
	c.tmu.Lock()
	defer c.tmu.Unlock()
	t := c.timers[id]
	if t == nil {
		return false
	}
	t.queued = false
	if !t.active {
		return false
	}
	if !t.every {
		t.active = false
		delete(c.timers, id)
	}
	return true
}

// recordDropCancelled emits lockstep.drop with reason timer-cancelled (TMR §8) for the timer
// message e, popped by the actor c although its timer is no longer active.
func (c *cell[M]) recordDropCancelled(e entry) {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.drop",
		engine.Attr{Key: "to", Value: c.addr.String()},
		engine.Attr{Key: "reason", Value: dropTimerCancelled},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(e.inc), 10)},
		engine.Attr{Key: "msg", Value: tr.Describe(e.msg)},
	)
}
