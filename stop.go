// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/registry"
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
	c, _ := sys.resolve(addr, nil)
	if c == nil {
		return fmt.Errorf("%w: %s", ErrNotFound, addr)
	}
	// An actor whose stop has started keeps its first request and reason (ACT-710).
	c.requestStop(Normal, ref)
	return nil
}

// stop is a stop request on a cell's system queue (SUP-050). A cell keeps at most one
// (requestStop).
type stop struct{ reason StopReason }

func (stop) isSysMsg() {}

// stopped is the notice a parent's system queue gets when its child completes its final stop
// (SUP-060 step 7). A stopping parent waits for it before it stops its next child.
type stopped struct{ child Address }

func (stopped) isSysMsg() {}

// The interim stop sequence. It runs in the cell's own turns, without failures, restarts, timers
// or owned resources: plan 1.3's SUP-060 replaces it behind the same entry points (requestStop and
// the stop and stopped messages).

// stopping reports whether the cell's stop has started (turn-owned state).
func (c *cell[M]) stopping() bool { return c.state == cellStopping || c.state == cellStopped }

// startStop processes the stop request (SUP-060 step 1): the cell takes no more user messages,
// the incarnation's watches leave every watcher list and its watched set (SUP-340, ACT-640), and
// the children start to stop.
func (c *cell[M]) startStop(reason StopReason) {
	if c.stopping() {
		return // unreachable: requestStop queues one request (SUP-050)
	}
	c.state = cellStopping
	c.stopReason = reason
	if c.ctx != nil {
		c.sys.reg.DropWatcher(registry.Watcher{Addr: registryAddr(c.addr), Inc: c.ctx.inc})
		c.ctx.watched = watchedSet{}
	}
	c.stopNextChild()
}

// stopNextChild asks the most recently spawned child that has not stopped to stop, and waits for
// its stopped notice; with no child left it completes the stop (SUP-060 step 2, SUP-070). A child
// that stops on its own meanwhile has left the registry's child list and is skipped.
func (c *cell[M]) stopNextChild() {
	for {
		kids := c.sys.reg.Children(c.addr.path)
		if len(kids) == 0 {
			c.finishStop()
			return
		}
		k := kids[len(kids)-1]
		uid, a, ok := c.sys.reg.Lookup(k.Path)
		child, _ := a.(actorCell)
		if !ok || child == nil || uid != k.UID {
			continue // it completed its stop after Children returned
		}
		c.waitChild = Address{node: k.Node, path: k.Path, uid: k.UID}
		child.requestStop(childReason(c.stopReason), c)
		return
	}
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
	if c.state != cellStopping || child != c.waitChild {
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
	if c.ctx != nil && c.stopper != nil {
		c.callStopped()
	}
	c.mu.Lock()
	c.liveInc = 0 // the incarnation has ended: Pipe's continuations drop (MSG-610)
	c.mu.Unlock()

	watchers := sys.reg.Remove(c.addr.path) // the name is free and the address not alive (ACT-550)
	for _, e := range c.closeQueues() {
		sys.deadLetter(e.from, c.addr, reasonStopped, e.msg) // Sent entries only, in queue order (MBX-160)
	}
	// An ask whose request was among the dead letters fails here, once (MSG-340).
	failure := fmt.Errorf("%w: %s", ErrTerminated, c.addr)
	for _, ask := range sys.asks.untrackAll(c) {
		sys.replyAttempt(ask, c.addr, nil, failure)
	}

	c.state = cellStopped
	sys.recordStop(c.addr, c.inc, c.stopReason)
	for _, w := range watchers {
		sys.pushTerminated(w, c.stopReason, c)
	}
	sys.notifyParent(c.parent, c.addr, c)
}

// callStopped calls Stopped with the stop's reason during the turn. A panic is recovered and
// ignored: reporting it as a cleanup failure (SUP-110) is plan 1.3's.
func (c *cell[M]) callStopped() {
	c.enter()
	defer func() {
		c.ctx.inTurn.Store(false)
		_ = recover() // SUP-110's lockstep.fail record and cleanupFailed notice are plan 1.3's
	}()
	c.stopper.Stopped(c.ctx, c.stopReason)
}

// pushTerminated pushes Terminated{key, reason} to the watcher of one entry of a stopped actor's
// watcher list (SUP-330), if that watcher is still registered. Its delivery skips a key the
// watcher no longer watches (ACT-630).
func (s *System) pushTerminated(w registry.Watch, reason StopReason, from engine.ActorRef) {
	wa := w.Watcher.Addr
	uid, a, ok := s.reg.Lookup(wa.Path)
	c, _ := a.(actorCell)
	if !ok || c == nil || uid != wa.UID {
		return // the watcher completed its own stop
	}
	key := Address{node: w.Key.Node, path: w.Key.Path, uid: w.Key.UID}
	c.pushSystem(signalMsg{sig: Terminated{Addr: key, Reason: reason}}, from)
}

// notifyParent pushes stopped{child} to the parent's system queue (SUP-060 step 7). A guardian
// parent has no cell and gets the notice through guardianChildStopped.
func (s *System) notifyParent(parent, child Address, from engine.ActorRef) {
	if s.isGuardian(parent) {
		s.guardianChildStopped(child, from)
		return
	}
	uid, a, ok := s.reg.Lookup(parent.path)
	c, _ := a.(actorCell)
	if !ok || c == nil || uid != parent.uid {
		return // no parent is registered under that uid: there is nobody to notify
	}
	c.pushSystem(stopped{child: child}, from)
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
