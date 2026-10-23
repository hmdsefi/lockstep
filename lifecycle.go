// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// The commands and notices of SUP §3. They travel on the system queue, so every lifecycle step runs
// in a turn of the cell that receives them, before its user messages (SUP-010). Commands go from a
// supervisor to its child, notices from a child to its supervisor.

// begin starts an incarnation (SUP-030): Spawn pushes it onto the new cell's system queue.
type begin struct{}

// end ends incarnation inc for a restart caused by err (SUP-190).
type end struct {
	inc uint32
	err error
}

// resume returns the Suspended incarnation inc to Running (SUP-210).
type resume struct{ inc uint32 }

// stop is a stop request (SUP-050). A cell keeps at most one (requestStop).
type stop struct{ reason StopReason }

// inject makes the cell fail with err as if its handler had called ctx.Fail(err), with source
// injected (SFT-040). The simulation pushes it with Sim.InjectFail.
type inject struct{ err error }

// String describes the command for the lockstep.drop record of one a cell discards, without the
// error, whose text is the caller's.
func (inject) String() string { return "inject" }

// failed reports that incarnation inc of child failed with err in source (SUP-150). For a factory
// failure inc is the number of the last incarnation that began, 0 if none.
type failed struct {
	child  Address
	inc    uint32
	err    error
	source supervise.Source
}

// String describes the notice for the lockstep.drop record of a stale failure (SUP §8): "failed",
// the child's address and the incarnation, without the error, whose text is the user's.
func (n failed) String() string {
	return "failed " + n.child.String() + " inc " + strconv.FormatUint(uint64(n.inc), 10)
}

// ended reports that child ended incarnation inc for a restart (SUP-190 step 4).
type ended struct {
	child Address
	inc   uint32
}

// stopped reports that child completed its final stop (SUP-060 step 7). A stopping parent waits for
// it before it stops its next child.
type stopped struct{ child Address }

// cleanupFailed reports that Stopped of incarnation inc of child failed with err (SUP-110).
type cleanupFailed struct {
	child Address
	inc   uint32
	err   error
}

// asSignal returns the signal message a supervisor handles for the notice: ChildFailed with
// Cleanup true and no directive, delivered, kept or dropped under SUP-320 (SUP-110).
func (n cleanupFailed) asSignal() signalMsg {
	return signalMsg{sig: ChildFailed{Child: n.child, Err: n.err, Cleanup: true}}
}

func (begin) isSysMsg()         {}
func (end) isSysMsg()           {}
func (inject) isSysMsg()        {}
func (resume) isSysMsg()        {}
func (stop) isSysMsg()          {}
func (failed) isSysMsg()        {}
func (ended) isSysMsg()         {}
func (stopped) isSysMsg()       {}
func (cleanupFailed) isSysMsg() {}

// errNilActor is the failure of a factory that returns nil (SUP-040).
var errNilActor = errors.New("lockstep: factory returned nil actor")

// setState moves the cell to s along an edge of SUP-010 and mirrors s for lifecycle. Any other
// change is a bug in the runtime, so it panics.
func (c *cell[M]) setState(s supervise.State) {
	if !supervise.CanTransition(c.state, s) {
		panic("lockstep: " + c.addr.String() + ": no transition from " + c.state.String() + " to " + s.String())
	}
	c.state = s
	c.mu.Lock()
	c.seenState = s
	c.mu.Unlock()
}

// lifecycle returns the cell's state and incarnation number as its turns last set them. Unlike
// the turn-owned state and inc, it may be called from any goroutine: a supervisor reads its
// child's with it, from its own turn, for the stale-failure check of SUP-170.
func (c *cell[M]) lifecycle() (supervise.State, uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seenState, c.seenInc
}

// callHandler calls fn, a handler of the live incarnation with source src, as a guarded call in
// the in-turn window (ACT-120, SUP-140), and hands a failure to handlerFailed.
func (c *cell[M]) callHandler(src supervise.Source, fn func()) {
	c.handled++
	if failed, err := c.callGuarded(fn); failed {
		c.handlerFailed(src, err)
	}
}

// callGuarded calls fn in the in-turn window of the live incarnation's Context and reports its
// failure (SUP-140): a panic, as a *PanicError, wins over a Fail call; otherwise the first Fail
// argument of this call is the failure.
func (c *cell[M]) callGuarded(fn func()) (failed bool, err error) {
	ctx := c.ctx
	ctx.failure = nil
	ctx.inTurn.Store(true)
	failed, err = guard(fn)
	ctx.inTurn.Store(false)
	if !failed && ctx.failure != nil {
		failed, err = true, ctx.failure
	}
	ctx.failure = nil
	return failed, err
}

// handlerFailed is the failure path of SUP-150 for a failure with source src (the factory,
// Started, Receive or Signal): it records lockstep.fail, suspends the cell, which then pops no user
// message (SUP-010, SUP-160), and pushes failed to the supervisor. For a factory failure inc is
// unchanged (SUP-040).
func (c *cell[M]) handlerFailed(src supervise.Source, err error) {
	c.recordFail(src, err)
	c.setState(supervise.Suspended)
	c.sys.notifySupervisor(c.parent, failed{child: c.addr, inc: c.inc, err: err, source: src}, c)
}

// notifySupervisor pushes the notice m (failed, ended, stopped or cleanupFailed) to the supervisor
// parent, the cell whose turn pushes being from. A guardian has no cell: the guardians' ActorRef
// handles its children's notices (SUP-300). A parent cell that has completed its final stop is not
// notified: there is nobody to notify.
func (s *System) notifySupervisor(parent Address, m sysMsg, from engine.ActorRef) {
	if s.isGuardian(parent) {
		s.guardians.push(m, from)
		return
	}
	if c, ok := s.lookupCell(registryAddr(parent)); ok {
		c.pushSystem(m, from)
	}
}

// recordStart emits lockstep.start (SUP §8) for the incarnation that has just begun.
func (c *cell[M]) recordStart() {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.start",
		engine.Attr{Key: "actor", Value: c.addr.String()},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(c.inc), 10)},
	)
}

// recordFail emits lockstep.fail (SUP §8) for a failure of the cell with source src and error
// err.
func (c *cell[M]) recordFail(src supervise.Source, err error) {
	c.sys.recordFail(c.addr, c.inc, src, err)
}

// recordFail emits lockstep.fail (SUP §8) for a failure of the actor at addr, in incarnation inc
// (0 for a guardian), with source src and error err.
func (s *System) recordFail(addr Address, inc uint32, src supervise.Source, err error) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.fail",
		engine.Attr{Key: "actor", Value: addr.String()},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(inc), 10)},
		engine.Attr{Key: "source", Value: src.String()},
		engine.Attr{Key: "err", Value: tr.Describe(err)},
	)
}

// keep keeps m, a signal or a child's failed or ended notice that reached the Suspended cell, in
// arrival order (SUP-160). They are handled after a resume (resumeIncarnation), or, when the
// incarnation ends, the signals are recorded as dropped and the notices discarded (dropKept).
func (c *cell[M]) keep(m sysMsg) { c.kept = append(c.kept, m) }

// dropKept empties the kept list when the Suspended cell's incarnation ends, by restart or stop
// (SUP-160): each kept signal is recorded as lockstep.drop reason not-running, in arrival order,
// except a Terminated that a delivery would skip (ACT-630: its address is not watched, or is
// watched under another Watch call's sequence number), which is discarded without a record; the
// kept notices are discarded, since the children they name are being stopped. The children whose
// failure the cell escalated are stopped the same way, so the escalated list is cleared too
// (SUP-220).
func (c *cell[M]) dropKept() {
	kept := c.kept
	c.kept = nil
	c.sup.escalated = nil
	for _, m := range kept {
		sm, ok := m.(signalMsg)
		if !ok {
			continue
		}
		if t, isTerm := sm.sig.(Terminated); isTerm && c.ctx != nil {
			if seq, in := c.ctx.watched.seq(t.Addr); !in || seq != sm.seq {
				continue
			}
		}
		c.sys.recordDropNotRunning(c.addr, sm.sig)
	}
}

// resumeIncarnation processes resume{inc} (SUP-210). Only a Suspended cell in incarnation inc
// resumes, and only with a live incarnation; any other resume is discarded. The cell changes to
// Running, resumes the children whose failure it escalated (SUP-220), then handles what it kept
// while Suspended in arrival order: it delivers the signals and decides or goes on with its
// children's notices (SUP-160). If one of them fails the cell again, the rest stay kept, ahead of
// what arrives later.
func (c *cell[M]) resumeIncarnation(m resume) {
	if c.state != supervise.Suspended || m.inc != c.inc || c.ctx == nil {
		return
	}
	c.setState(supervise.Running)
	c.resumeEscalated()
	c.replayHeld()
	for c.state == supervise.Running && len(c.kept) > 0 {
		k := c.kept[0]
		c.kept[0] = nil
		c.kept = c.kept[1:]
		c.processSystem(k)
	}
	if len(c.kept) == 0 {
		c.kept = nil
	}
}
