// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// The group operations of AllForOne (SUP-260). A restart or stop directive acts on every child
// that is Running or Suspended when the directive is decided, the group. The supervisor pushes
// its commands to the group's children one at a time, in reverse spawn order, and waits after each
// for the child's ended notice (a restart's end) or stopped notice (a stop, or a child that stops
// on its own). A restart then waits the delay once and pushes begin to the children still in
// Restarting, in spawn order. The supervisor's own stop or end abandons the operation (SUP-260).

// groupMember is a child of the group and the incarnation the directive found it in.
type groupMember struct {
	addr Address
	inc  uint32
}

// groupOp is a group operation that has commands left to push. It is turn-owned by the
// supervisor, like the rest of its state.
type groupOp struct {
	restart bool // end and begin the group, or stop it
	err     error
	delay   time.Duration // the delay a restart waits, once, before begin
	members []groupMember // in spawn order
	next    int           // the index of the next member to command; they are commanded downwards
	wait    groupMember   // the member the operation waits for, or the zero value
}

// waits reports whether the operation waits for the notice of child.
func (op *groupOp) waits(child Address) bool {
	return !op.wait.addr.IsZero() && op.wait.addr == child
}

// pendingBegin is the delay timer of a group restart. The timer's callback runs in engine context,
// outside any turn, so the two flags are atomic: the callback sets fired when it starts, and
// abandonGroup sets abandoned, which the callback checks before it pushes anything. That closes
// the window in which Cancel loses to a callback that has started (ENG-070).
type pendingBegin struct {
	cancel    engine.Cancel
	fired     atomic.Bool
	abandoned atomic.Bool
}

// startGroup starts the group operation for the directive just applied to the failed child of n:
// a restart with delay, or a stop with Failed(err). The group is the supervisor's children that
// are Running or Suspended now, read in spawn order from the registry (SUP-260). It pushes the
// first command; the notices of the children push the rest (stepGroup).
func (sv *supervisor) startGroup(sys *System, self Address, n failed, restart bool, delay time.Duration,
	from engine.ActorRef,
) {
	op := &groupOp{restart: restart, err: n.err, delay: delay}
	for _, k := range sys.reg.Children(self.path) {
		c, ok := sys.lookupCell(k)
		if !ok {
			continue
		}
		if st, inc := c.lifecycle(); st == supervise.Running || st == supervise.Suspended {
			op.members = append(op.members, groupMember{addr: Address{node: k.Node, path: k.Path, uid: k.UID}, inc: inc})
		}
	}
	op.next = len(op.members) - 1
	sv.op = op
	sv.stepGroup(sys, from)
}

// stepGroup pushes the command to the next member that is still registered, and waits for its
// notice. A member that completed its stop meanwhile has left the registry after its stop, and has
// sent its stopped notice, so it is skipped. With no member left the operation ends: a restart waits
// its delay and begins the group (beginGroup), a stop is done, and the failed notices held meanwhile
// are decided (nextHeld). from is the ActorRef whose turn pushes.
func (sv *supervisor) stepGroup(sys *System, from engine.ActorRef) {
	op := sv.op
	op.wait = groupMember{}
	for op.next >= 0 {
		m := op.members[op.next]
		op.next--
		c, ok := sys.lookupCell(registryAddr(m.addr))
		if !ok {
			continue
		}
		op.wait = m
		if op.restart {
			c.pushSystem(end{inc: m.inc, err: op.err}, from)
		} else {
			c.requestStop(Failed(op.err), from)
		}
		return
	}
	sv.op = nil
	if op.restart {
		sv.beginGroup(sys, op, from)
	}
}

// beginGroup pushes begin to the members of op that are still in Restarting after the operation's
// delay, in spawn order, without waiting: at once for a zero delay, otherwise from a timer on the
// engine clock with label lockstep/backoff (SUP-260, SUP-280, ENG-070). The callback runs in
// engine context, outside any turn: it touches no supervisor state and pushes with no origin
// (ENG-350). A guardian's group whose delay ends once the shutdown has started gets no begin: the
// shutdown's sweep stops its children (SUP-290, SUP-350).
func (sv *supervisor) beginGroup(sys *System, op *groupOp, from engine.ActorRef) {
	if op.delay <= 0 {
		for _, m := range op.members {
			sys.beginRestart(m.addr, m.inc, from)
		}
		return
	}
	members, guardian := op.members, sv.guardian
	// A timer that has fired needs no cancelling; dropping its handle keeps the list from growing
	// with every restart. One that is due but has not fired yet stays.
	sv.begins = pruneFired(sv.begins)
	b := new(pendingBegin)
	b.cancel = sys.eng.Clock.AfterFunc(op.delay, "lockstep/backoff", func() {
		b.fired.Store(true)
		if b.abandoned.Load() || guardian && sys.closing.Load() {
			return
		}
		for _, m := range members {
			sys.beginRestart(m.addr, m.inc, nil)
		}
	})
	sv.begins = append(sv.begins, b)
}

// pruneFired returns begins without the timers whose callback has started.
func pruneFired(begins []*pendingBegin) []*pendingBegin {
	kept := begins[:0]
	for _, b := range begins {
		if !b.fired.Load() {
			kept = append(kept, b)
		}
	}
	clear(begins[len(kept):])
	return kept
}

// nextHeld returns the next failed notice held during a group operation, in arrival order, once
// no operation is pushing commands. A notice of a child that the operation restarted or stopped is
// stale by then (SUP-170); one of a child outside the group is decided as usual.
func (sv *supervisor) nextHeld() (failed, bool) {
	if sv.op != nil || len(sv.held) == 0 {
		return failed{}, false
	}
	n := sv.held[0]
	sv.held[0] = failed{}
	sv.held = sv.held[1:]
	if len(sv.held) == 0 {
		sv.held = nil
	}
	return n, true
}

// abandonGroup ends the supervisor's part in group operations because its own incarnation ends,
// by stop or restart, or the system shuts down (SUP-260): it cancels the delay timers, drops the
// failed notices held and pushes no more end or begin. The stop that follows stops every child,
// those in Restarting included. The group's restart intensity counted this incarnation's
// children, which that stop ends too, so it starts over, as a OneForOne child's does when the
// child stops (SUP-250).
func (sv *supervisor) abandonGroup() {
	sv.op = nil
	sv.held = nil
	for _, b := range sv.begins {
		b.abandoned.Store(true)
		b.cancel()
	}
	sv.begins = nil
	sv.group = supervise.Intensity{}
}

// replayHeld decides the failed notices held during a group operation, now that it has pushed its
// last command, while the cell is Running: each is decided as if it had just arrived, so one that
// starts another group operation holds the rest again.
func (c *cell[M]) replayHeld() {
	for c.state == supervise.Running {
		n, ok := c.sup.nextHeld()
		if !ok {
			return
		}
		c.decideChild(n)
	}
}
