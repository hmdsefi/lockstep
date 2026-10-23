// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/registry"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// Stop asks the actor at addr to stop with reason Normal and returns without waiting. A parent
// may stop its child this way; any Sender may stop any local actor. A path address stops the actor
// registered at its path. Errors: ErrNotFound if addr is not alive or names a guardian,
// ErrNotLocal for another node's address in every phase (there is no remote stop). Stop panics if
// from is nil.
func Stop(from Sender, addr Address) error {
	// ACT-700's checks, in its order: from nil and a Context outside its turn panic.
	sys, _, ref := mustSender(from).sendOrigin()
	if addr.IsZero() || sys.isGuardian(addr) {
		return fmt.Errorf("%w: %s", ErrNotFound, addr)
	}
	if addr.node != sys.node {
		return fmt.Errorf("%w: %s", ErrNotLocal, addr)
	}
	// A path address acts on the actor registered at its path, whatever its type (ACT-440).
	c, _, _ := sys.resolve(addr, nil)
	if c == nil {
		return fmt.Errorf("%w: %s", ErrNotFound, addr)
	}
	// An actor whose stop has started keeps its first request and reason (ACT-710).
	c.requestStop(Normal, ref)
	return nil
}

// The stop sequence of SUP-060. It runs in the cell's own turns as a small resumable state
// machine: the stop request starts it (startStop), each child's stopped notice resumes it
// (childStopped), and the cell remembers in waitChild which child it waits for. requestStop keeps
// one request per cell (SUP-050).

// The lockstep.drop reasons of the stop sequence's drain (MBX §8, SUP §8).
const (
	dropIncarnationEnded = "incarnation-ended" // a Self entry of an incarnation that has ended (MBX-160)
	dropNotRunning       = "not-running"       // a signal that reached a cell outside Running (SUP-060, SUP-320)
)

// stopping reports whether the cell's stop has started (turn-owned state).
func (c *cell[M]) stopping() bool {
	return c.state == supervise.Stopping || c.state == supervise.Stopped
}

// ending reports whether the cell's incarnation is ending, for good or for a restart: the cell is
// Stopping, Restarting or Stopped, and only Stopped runs in its turns (turn-owned state). Spawn
// under it returns ErrStopped and ctx.Stop does nothing (SUP-050, SUP-090, SUP-100).
func (c *cell[M]) ending() bool {
	return c.stopping() || c.state == supervise.Restarting
}

// startStop processes the stop request (SUP-060 step 1): the cell changes to Stopping and pops no
// more user messages; if an incarnation is live, its watches leave every watcher list and its
// watched set (SUP-340, ACT-640). Then the children start to stop (step 2). A request that reaches
// the cell before its begin finds it Starting with no incarnation: it stops with inc 0 (SUP-010).
func (c *cell[M]) startStop(reason StopReason) {
	if c.stopping() {
		return // unreachable: requestStop queues one request (SUP-050)
	}
	c.setState(supervise.Stopping)
	c.stopReason = reason
	if c.ctx != nil {
		// Cancelling the incarnation's timers (TMR-080) and effects (EFF) belongs here, with its
		// watches.
		c.sys.reg.DropWatcher(registry.Watcher{Addr: registryAddr(c.addr), Inc: c.ctx.inc})
		c.ctx.watched = watchedSet{}
	}
	c.stopNextChild()
}

// stopNextChild asks the most recently spawned child that has not stopped to stop, and waits for
// its stopped notice; with no child left it completes the stop (SUP-060 step 2, SUP-070). A child
// that stops on its own meanwhile has left the registry's child list and is skipped. The children
// are the registry's spawn-ordered list, never a map.
func (c *cell[M]) stopNextChild() {
	for {
		kids := c.sys.reg.Children(c.addr.path)
		if len(kids) == 0 {
			c.finishStop()
			return
		}
		k := kids[len(kids)-1]
		child, ok := c.sys.lookupCell(k)
		if !ok {
			continue // it completed its stop after Children returned
		}
		c.waitChild = Address{node: k.Node, path: k.Path, uid: k.UID}
		child.requestStop(childReason(c.stopReason), c)
		return
	}
}

// lookupCell returns the cell registered as k, and false if k has completed its stop (its path is
// free or registered with another uid). Every registry entry is an actor cell: any other value is
// a bug in the runtime, so lookupCell panics on it rather than report it gone, which would make
// the callers' loops look it up forever.
func (s *System) lookupCell(k registry.Addr) (actorCell, bool) {
	uid, a, ok := s.reg.Lookup(k.Path)
	if !ok || uid != k.UID {
		return nil, false
	}
	c, isCell := a.(actorCell)
	if !isCell || c == nil {
		panic(fmt.Sprintf("lockstep: registry entry at %s is not an actor cell: %T", k.Path, a))
	}
	return c, true
}

// childReason is the reason of a child stopped by a parent stopping with r (SUP-080).
func childReason(r StopReason) StopReason {
	if r.Kind == StopShutdown {
		return Shutdown
	}
	return ParentStopping
}

// childStopped processes a child's stopped notice. The notice of the child the stop sequence
// waits for moves it to the next child; any other (a child that stopped on its own, while the
// cell runs or waits for another child) changes nothing, since that child has left the registry.
func (c *cell[M]) childStopped(child Address) {
	if c.state != supervise.Stopping || child != c.waitChild {
		return
	}
	c.waitChild = Address{}
	c.stopNextChild()
}

// finishStop completes the stop once every child has stopped (SUP-060 steps 3 to 7): Stopped,
// registry removal, the closed and drained queues, the tracked asks, the record, the watchers'
// Terminated and the parent's notice. It holds no lock across these steps (c.mu is a leaf;
// asks are settled with no cell or registry lock held).
func (c *cell[M]) finishStop() {
	sys := c.sys
	// Step 3: Stopped, then closing the owned resources, whether or not the actor has a Stopped
	// hook (SUP-100, SUP-120).
	if c.ctx != nil {
		if c.stopper != nil {
			c.callStopped()
		}
		c.closeOwned()
	}
	c.mu.Lock()
	c.liveInc = 0 // the incarnation has ended: Pipe's continuations drop (MSG-610)
	c.mu.Unlock()

	// Step 4: the name is free and the address not alive (ACT-550).
	watchers := sys.reg.Remove(c.addr.path)

	// Step 5. An ask whose request was among the dead letters fails after the drain, once
	// (MSG-340).
	c.drain()
	failure := fmt.Errorf("%w: %s", ErrTerminated, c.addr)
	for _, ask := range sys.asks.untrackAll(c) {
		sys.replyAttempt(ask, c.addr, nil, failure)
	}

	// Steps 6 and 7.
	c.setState(supervise.Stopped)
	sys.recordStop(c.addr, c.inc, c.stopReason)
	for _, w := range watchers {
		sys.pushTerminated(w, c.stopReason, c)
	}
	sys.notifyParent(c.parent, c.addr, c)
}

// drain closes both queues and records what they held (SUP-060 step 5, MBX-160), in this order:
// each Sent entry, in queue order, as a dead letter with reason stopped, from its sender to the
// stopped actor's address (MBX §8); each Self entry, piped replies included, as lockstep.drop
// reason incarnation-ended, a piped reply described by its value or error (MBX §8); each signal
// left in the system queue as lockstep.drop reason not-running. The other system messages are
// discarded without a record.
func (c *cell[M]) drain() {
	sent, self, signals := c.closeQueues()
	for _, e := range sent {
		c.sys.deadLetter(e.from, c.addr, reasonStopped, e.msg)
	}
	for _, e := range self {
		msg := e.msg
		if p, ok := msg.(pipedReply[M]); ok {
			msg = p.reply
		}
		c.sys.recordDropEnded(c.addr, e.inc, msg)
	}
	for _, sig := range signals {
		c.sys.recordDropNotRunning(c.addr, sig)
	}
}

// callStopped calls Stopped with the stop's reason as a guarded call during the turn. A failure
// in it (a panic or a Fail) is a cleanup failure (SUP-110): it is reported and the stop continues
// as if Stopped had returned.
func (c *cell[M]) callStopped() {
	if failed, err := c.callGuarded(func() { c.stopper.Stopped(c.ctx, c.stopReason) }); failed {
		c.failCleanup(err)
	}
}

// failCleanup reports a failure of Stopped (SUP-110): it records lockstep.fail with source
// stopped and pushes cleanupFailed to the supervisor. It is never decided and never counts toward
// restart intensity, and the cell keeps its state: the stop it is in goes on.
func (c *cell[M]) failCleanup(err error) {
	c.recordFail(supervise.InStopped, err)
	c.sys.notifyCleanupFailed(c.parent, cleanupFailed{child: c.addr, inc: c.inc, err: err}, c)
}

// notifyCleanupFailed pushes the cleanupFailed notice n to the supervisor parent, the cell whose
// turn pushes being from. A guardian has no cell, and the supervision of top-level and system
// actors by the guardians (SUP-300) does not exist yet: a guardian child's notice is dropped here.
func (s *System) notifyCleanupFailed(parent Address, n cleanupFailed, from engine.ActorRef) {
	if s.isGuardian(parent) {
		return
	}
	s.pushToParent(parent, n, from)
}

// pushTerminated pushes Terminated{key, reason} to the watcher of one entry of a stopped actor's
// watcher list (SUP-330), if that watcher is still registered, with the entry's sequence number.
// Its delivery skips a key the watcher no longer watches under that number (ACT-630).
func (s *System) pushTerminated(w registry.Watch, reason StopReason, from engine.ActorRef) {
	c, ok := s.lookupCell(w.Watcher.Addr)
	if !ok {
		return // the watcher completed its own stop
	}
	key := Address{node: w.Key.Node, path: w.Key.Path, uid: w.Key.UID}
	c.pushSystem(signalMsg{sig: Terminated{Addr: key, Reason: reason}, seq: w.Seq}, from)
}

// notifyParent pushes stopped{child} to the parent's system queue (SUP-060 step 7). A guardian
// parent has no cell and gets the notice through guardianChildStopped.
func (s *System) notifyParent(parent, child Address, from engine.ActorRef) {
	if s.isGuardian(parent) {
		s.guardianChildStopped(child, from)
		return
	}
	s.pushToParent(parent, stopped{child: child}, from)
}

// recordStop emits lockstep.stop (SUP §8) for a cell that reached Stopped: inc is its last
// incarnation (0 if none began), reason the kind of the final stop, err empty without an error.
func (s *System) recordStop(addr Address, inc uint32, reason StopReason) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	desc := ""
	if reason.Err != nil {
		desc = tr.Describe(reason.Err)
	}
	tr.Record("lockstep.stop",
		engine.Attr{Key: "actor", Value: addr.String()},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(inc), 10)},
		engine.Attr{Key: "reason", Value: reason.Kind.String()},
		engine.Attr{Key: "err", Value: desc},
	)
}

// recordDropEnded emits lockstep.drop with reason incarnation-ended (MBX §8) for a Self entry of
// incarnation inc, with message msg, discarded by the actor at to.
func (s *System) recordDropEnded(to Address, inc uint32, msg any) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.drop",
		engine.Attr{Key: "to", Value: to.String()},
		engine.Attr{Key: "reason", Value: dropIncarnationEnded},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(inc), 10)},
		engine.Attr{Key: "msg", Value: tr.Describe(msg)},
	)
}

// recordDropNotRunning emits lockstep.drop with reason not-running (SUP §8) for the signal sig
// discarded by the actor at to.
func (s *System) recordDropNotRunning(to Address, sig Signal) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.drop",
		engine.Attr{Key: "to", Value: to.String()},
		engine.Attr{Key: "reason", Value: dropNotRunning},
		engine.Attr{Key: "msg", Value: tr.Describe(sig)},
	)
}
