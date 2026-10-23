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
//
//nolint:unused // the fields are the restart's, read once supervisors decide failures (SUP-190)
type end struct {
	inc uint32
	err error
}

// resume returns the Suspended incarnation inc to Running (SUP-210).
//
//nolint:unused // inc is read once supervisors decide failures (SUP-210)
type resume struct{ inc uint32 }

// stop is a stop request (SUP-050). A cell keeps at most one (requestStop).
type stop struct{ reason StopReason }

// failed reports that incarnation inc of child failed with err in source (SUP-150). For a factory
// failure inc is the number of the last incarnation that began, 0 if none.
type failed struct {
	child  Address
	inc    uint32
	err    error
	source supervise.Source
}

// ended reports that child ended incarnation inc for a restart (SUP-190 step 4).
//
//nolint:unused // the fields are read once supervisors restart children (SUP-190)
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

func (begin) isSysMsg()         {}
func (end) isSysMsg()           {}
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
	c.sys.notifyFailed(c.parent, failed{child: c.addr, inc: c.inc, err: err, source: src}, c)
}

// notifyFailed pushes the failed notice n to the supervisor parent (SUP-150), the cell whose turn
// pushes being from. A guardian has no cell, and the supervision of top-level and system actors
// by the guardians (SUP-300) does not exist yet: a guardian child's notice is dropped here, and the
// child stays Suspended.
func (s *System) notifyFailed(parent Address, n failed, from engine.ActorRef) {
	if s.isGuardian(parent) {
		return
	}
	s.pushToParent(parent, n, from)
}

// pushToParent pushes the notice m onto the system queue of the cell registered as parent. A
// parent that has completed its final stop is not notified: there is nobody to notify.
func (s *System) pushToParent(parent Address, m sysMsg, from engine.ActorRef) {
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

// recordFail emits lockstep.fail (SUP §8) for a failure with source src and error err.
func (c *cell[M]) recordFail(src supervise.Source, err error) {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.fail",
		engine.Attr{Key: "actor", Value: c.addr.String()},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(c.inc), 10)},
		engine.Attr{Key: "source", Value: src.String()},
		engine.Attr{Key: "err", Value: tr.Describe(err)},
	)
}
