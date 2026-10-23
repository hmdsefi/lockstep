// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// supervisor is the supervision state of one supervisor: a cell for its children, or a guardian
// for the top-level or system actors (SUP-300). Cells and guardians run the same decision and
// restart code through it, each in its own turns, so it needs no lock: it is turn-owned by its
// cell, or by the guardians' engine.ActorRef.
//
// The decision path of a failed notice is SUP-170 to SUP-190: the stale check, the guarded
// decider, Normalize, restart intensity at the engine clock's Now, the lockstep.directive record,
// then the command: end for a restart, stop{Failed(err)} for a stop, resume for a resume
// (SUP-190 to SUP-210). An escalation pushes no command: the supervising cell fails with the
// child's error (SUP-220), and a guardian, which has no supervisor, applies it as a stop
// (SUP-300). A restart's end is followed by the child's ended notice, which starts the backoff,
// after which begin is pushed (SUP-190, SUP-280); the child's stopped notice cancels a pending
// begin and discards the child's state (SUP-250, SUP-290).
//
// Under AllForOne a restart or stop acts on the whole group (SUP-260): groupOp pushes one command
// at a time, each acknowledged by the child's ended or stopped notice (group.go).
type supervisor struct {
	strategy Strategy
	// guardian is set for the two guardians' supervisors (SUP-300).
	guardian bool
	// kids holds the state of every child that has been restarted and has not stopped since, by
	// address. It answers lookups only and is never ranged over, so its order never matters (DET).
	kids map[Address]*kidState
	// escalated holds the failed notices of the children whose failure the supervising cell
	// escalated, or left undecided because its decider failed, in that order. The children stay
	// Suspended; a resume of the cell resumes them, and its end or stop clears the list, since its
	// stop sequence stops them (SUP-180, SUP-220).
	escalated []failed

	// group is the restart intensity of an AllForOne supervisor's whole group (SUP-260). It is
	// reset when the supervisor's own incarnation ends, which stops the children it counts.
	group supervise.Intensity
	// op is the group operation that still has commands to push, or nil (SUP-260).
	op *groupOp
	// held holds the failed notices that reached the supervisor while op was pushing commands;
	// nextHeld hands them out once it has pushed the last.
	held []failed
	// begins holds the delay timers of the group restarts that may still be pending, for the end
	// of the supervisor's incarnation to cancel (SUP-260).
	begins []*pendingBegin
}

// kidState is a supervisor's state for one child under OneForOne (SUP-250).
type kidState struct {
	intensity supervise.Intensity // the child's restarts in the window (SUP-270)
	// restarting is set while the supervisor waits for the ended notice of incarnation inc, after
	// pushing end; delay is the backoff that notice starts (SUP-190, SUP-280).
	restarting bool
	inc        uint32
	delay      time.Duration
	// cancel cancels the pending begin of the last backoff timer, or is nil. Once the timer has
	// fired it returns false, which is harmless (ENG-070).
	cancel engine.Cancel
}

// kid returns the state of child, creating it on the child's first restart.
func (sv *supervisor) kid(child Address) *kidState {
	k := sv.kids[child]
	if k == nil {
		if sv.kids == nil {
			sv.kids = make(map[Address]*kidState)
		}
		k = &kidState{}
		sv.kids[child] = k
	}
	return k
}

// decision is what a supervisor did with a failed notice.
type decision struct {
	// d is the directive applied, after Normalize, intensity and SUP-300; 0 when none was: the
	// notice was stale, or the decider failed.
	d supervise.Directive
	// deciderErr is the failure of the decider (SUP-180): a *PanicError, or the invalid-directive
	// error. Only a cell's decision has it; a guardian applies it as an escalation.
	deciderErr error
}

// childFailed decides the failed notice n of a child, in a turn of the supervisor self, which is
// in Running (a cell) or a guardian (SUP-170, SUP-300); from is the ActorRef whose turn it is. A
// notice whose child has stopped, is not Suspended or is in another incarnation is stale: it is
// dropped with lockstep.drop reason stale-failure. Otherwise the strategy decides (decide), and
// apply records the final directive and pushes its command.
//
// A decider that panics or returns a value outside DirectiveRestart to DirectiveResume is
// returned in the decision's deciderErr: the supervising cell fails with it (SUP-180). A guardian
// cannot fail: it records the failure in lockstep.fail with source decider and applies it as an
// escalation, so as a stop (SUP-300).
func (sv *supervisor) childFailed(sys *System, self Address, n failed, from engine.ActorRef) decision {
	child, ok := sv.current(sys, n)
	if !ok {
		sys.recordDropStale(self, n)
		return decision{}
	}
	if sv.op != nil {
		// A group operation is pushing commands, and the notice may name a child it has yet to
		// reach. It is decided once the operation has pushed its last command, when it is stale,
		// or describes a child outside the group (SUP-260).
		sv.held = append(sv.held, n)
		return decision{}
	}
	d, err := sv.decide(n)
	if err != nil {
		if !sv.guardian {
			return decision{deciderErr: err}
		}
		sys.recordFail(self, 0, supervise.InDecider, err)
		d = supervise.Escalate
	}
	return decision{d: sv.apply(sys, self, child, n, d, from)}
}

// current returns the child of the failed notice n if n still describes it: the child is
// registered, Suspended, and in n's incarnation (SUP-170). It reads the child's state as the
// child's own turns last set it: lifecycle reads the mirror under the child's lock, never its
// turn-owned fields.
func (sv *supervisor) current(sys *System, n failed) (actorCell, bool) {
	child, ok := sys.lookupCell(registryAddr(n.child))
	if !ok {
		return nil, false
	}
	if st, inc := child.lifecycle(); st != supervise.Suspended || inc != n.inc {
		return nil, false
	}
	return child, true
}

// apply applies the directive d to the failed child of notice n and returns the directive
// applied. Under AllForOne a restart or stop acts on the group (startGroup). A restart is admitted
// by restart intensity first (SUP-270): one more restart in the
// window than MaxRestarts allows becomes an escalation, which a guardian applies as a stop
// (SUP-300). It records lockstep.directive, with the restarts in the window for every directive,
// then pushes the command (SUP-190 to SUP-210). An escalation at a cell pushes nothing: the caller
// fails the cell (SUP-220).
func (sv *supervisor) apply(sys *System, self Address, child actorCell, n failed, d supervise.Directive,
	from engine.ActorRef,
) supervise.Directive {
	l := sv.strategy.limits()
	now := sys.eng.Clock.Now()
	allForOne := sv.strategy.scope == uint8(supervise.AllForOne)
	var restarts int
	var delay time.Duration
	limit := false
	if d == supervise.Restart {
		// One Intensity for the group under AllForOne (SUP-260), one per child under OneForOne
		// (SUP-250).
		in := &sv.group
		if !allForOne {
			in = &sv.kid(n.child).intensity
		}
		count, admitted := in.Admit(now, l)
		restarts = count
		if admitted {
			delay = supervise.Backoff(count, l)
		} else {
			d, limit = supervise.Escalate, true
		}
	} else if allForOne {
		restarts = sv.group.Count(now, l)
	} else if k := sv.kids[n.child]; k != nil {
		restarts = k.intensity.Count(now, l)
	}
	escalated := false
	if sv.guardian && d == supervise.Escalate {
		d, escalated = supervise.Stop, true
	}
	sys.recordDirective(self, n, supervise.Scope(sv.strategy.scope), d, restarts, delay, limit, escalated)
	switch d {
	case supervise.Restart:
		if allForOne {
			sv.startGroup(sys, self, n, true, delay, from) // SUP-260
			break
		}
		k := sv.kids[n.child]
		k.restarting, k.inc, k.delay = true, n.inc, delay
		child.pushSystem(end{inc: n.inc, err: n.err}, from)
	case supervise.Stop:
		if allForOne {
			sv.startGroup(sys, self, n, false, 0, from) // SUP-260
			break
		}
		// The child stops for good with Failed(err) (SUP-200). A stop request already queued keeps
		// its reason (SUP-050).
		child.requestStop(Failed(n.err), from)
	case supervise.Resume:
		// SUP-210, SUP-260: the failed child only; not counted toward intensity.
		child.pushSystem(resume{inc: n.inc}, from)
	case supervise.Escalate:
		// SUP-220: the cell fails; the child stays Suspended.
	}
	return d
}

// decide calls the strategy's decider for n as a guarded call with source decider, nil meaning
// DirectiveRestart, and normalizes its result for n's source (SUP-180): DirectiveResume after a
// failure in the factory or Started becomes DirectiveRestart. err is the decider's failure: the
// *PanicError of a panic, or fmt.Errorf("lockstep: invalid directive %s", d) for a value outside
// DirectiveRestart to DirectiveResume.
func (sv *supervisor) decide(n failed) (supervise.Directive, error) {
	d := DirectiveRestart
	if decider := sv.strategy.Decider; decider != nil {
		if failed, err := guard(func() { d = decider(n.err) }); failed {
			return 0, err
		}
	}
	eff, ok := supervise.Normalize(supervise.Directive(d), n.source)
	if !ok {
		return 0, fmt.Errorf("lockstep: invalid directive %s", d)
	}
	return eff, nil
}

// decideChild handles the failed notice n of a child in Running (SUP-170 to SUP-230): the
// supervisor decides and applies the directive, then the cell acts on the decision (afterDecision).
func (c *cell[M]) decideChild(n failed) {
	c.afterDecision(n, c.sup.childFailed(c.sys, c.addr, n, c))
}

// afterDecision is the cell's part of a decision about the failed notice n. After a restart, a
// stop or a resume, whose command apply has pushed, the cell delivers ChildFailed with that
// directive to its own Signal in the same turn (SUP-230); a failure there is the cell's, with
// source signal. After an escalation the cell fails with the child's error and source escalate
// (SUP-220), and after a decider failure with that failure and source decider (SUP-180); either
// way the child stays Suspended and joins the escalated list. A stale notice changes nothing.
func (c *cell[M]) afterDecision(n failed, dec decision) {
	switch {
	case dec.deciderErr != nil:
		c.sup.escalated = append(c.sup.escalated, n)
		c.handlerFailed(supervise.InDecider, dec.deciderErr)
	case dec.d == supervise.Escalate:
		c.sup.escalated = append(c.sup.escalated, n)
		c.handlerFailed(supervise.InEscalate, n.err)
	case dec.d != 0:
		c.signal(ChildFailed{Child: n.child, Err: n.err, Directive: Directive(dec.d)})
	}
}

// resumeEscalated pushes resume{inc} to every child whose failure the cell escalated, or left
// undecided, in that order, now that the cell itself has resumed (SUP-180, SUP-220). A child that
// no longer is Suspended in the notice's incarnation has moved on and is skipped. A failure in the
// factory or Started left no initialized state to resume: as Normalize does for a decider's
// DirectiveResume (SUP-180), that child is restarted instead, through the decision path, intensity
// included. If that fails the cell again, the children left stay in the list.
func (c *cell[M]) resumeEscalated() {
	for c.state == supervise.Running && len(c.sup.escalated) > 0 {
		n := c.sup.escalated[0]
		c.sup.escalated = c.sup.escalated[1:]
		child, ok := c.sup.current(c.sys, n)
		if !ok {
			continue
		}
		if d, _ := supervise.Normalize(supervise.Resume, n.source); d == supervise.Resume {
			child.pushSystem(resume{inc: n.inc}, c)
			continue
		}
		c.afterDecision(n, decision{d: c.sup.apply(c.sys, c.addr, child, n, supervise.Restart, c)})
	}
	if len(c.sup.escalated) == 0 {
		c.sup.escalated = nil
	}
}

// childEnded handles the ended notice n of a child the supervisor restarts (SUP-190): it pushes
// begin to the child at once for a zero delay, and otherwise arms a backoff timer on the engine
// clock with label lockstep/backoff (ENG-070) whose callback pushes it. A notice the supervisor
// does not wait for, of another incarnation, changes nothing. from is the ActorRef whose turn it
// is.
func (sv *supervisor) childEnded(sys *System, n ended, from engine.ActorRef) {
	if op := sv.op; op != nil && op.restart && op.waits(n.child) && n.inc == op.wait.inc {
		sv.stepGroup(sys, from) // SUP-260: the group's next end
		return
	}
	k := sv.kids[n.child]
	if k == nil || !k.restarting || k.inc != n.inc {
		return
	}
	k.restarting = false
	if k.delay <= 0 {
		sys.beginRestart(n.child, n.inc, from)
		return
	}
	child, inc, guardian := n.child, n.inc, sv.guardian
	// The callback runs in engine context, outside any turn: it touches no supervisor state and
	// pushes begin with no origin (ENG-070, ENG-350). A guardian's child whose backoff ends once
	// the shutdown has started gets no begin: the shutdown's sweep stops it, Restarting, with no
	// Stopped call (SUP-290, SUP-350).
	k.cancel = sys.eng.Clock.AfterFunc(k.delay, "lockstep/backoff", func() {
		if guardian && sys.closing.Load() {
			return
		}
		sys.beginRestart(child, inc, nil)
	})
}

// childStopped handles a child's stopped notice for supervision: it cancels the child's pending
// begin, if any (SUP-290), and discards the child's state, its restart intensity included
// (SUP-250). It also goes on with a group operation that waits for the child (SUP-260). from is
// the ActorRef whose turn it is.
func (sv *supervisor) childStopped(sys *System, child Address, from engine.ActorRef) {
	if k := sv.kids[child]; k != nil {
		if k.cancel != nil {
			k.cancel()
		}
		delete(sv.kids, child)
	}
	if op := sv.op; op != nil && op.waits(child) {
		sv.stepGroup(sys, from)
	}
}

// beginRestart pushes begin to child after the backoff of the restart that ended incarnation inc
// (SUP-190). The child must still be Restarting after that incarnation: a stop that won the race
// with the timer (its Cancel can lose to the firing on a concurrent engine) has removed it, or
// moved it on, and then nothing is pushed (SUP-290). from is the ActorRef whose turn pushes, or
// nil in the timer's callback.
func (s *System) beginRestart(child Address, inc uint32, from engine.ActorRef) {
	c, ok := s.lookupCell(registryAddr(child))
	if !ok {
		return
	}
	if st, i := c.lifecycle(); st != supervise.Restarting || i != inc {
		return
	}
	c.pushSystem(begin{}, from)
}

// The lockstep.drop reason of a failed notice that no longer describes the child (SUP-170).
const dropStaleFailure = "stale-failure"

// recordDropStale emits lockstep.drop with reason stale-failure (SUP §8) for the failed notice n
// that the supervisor to discarded.
func (s *System) recordDropStale(to Address, n failed) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.drop",
		engine.Attr{Key: "to", Value: to.String()},
		engine.Attr{Key: "reason", Value: dropStaleFailure},
		engine.Attr{Key: "msg", Value: tr.Describe(n)},
	)
}

// recordDirective emits lockstep.directive (SUP §8) for the supervisor's decision about the failed
// notice n: the final directive d under scope, the restarts counted in the window, the backoff
// delay (0 unless d is a restart), limit, true when intensity turned a restart into an
// escalation, and escalated, true when a guardian applied an escalation as a stop (SUP-300).
func (s *System) recordDirective(sup Address, n failed, scope supervise.Scope, d supervise.Directive,
	restarts int, delay time.Duration, limit, escalated bool,
) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.directive",
		engine.Attr{Key: "supervisor", Value: sup.String()},
		engine.Attr{Key: "child", Value: n.child.String()},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(n.inc), 10)},
		engine.Attr{Key: "scope", Value: scopeName(scope)},
		engine.Attr{Key: "directive", Value: Directive(d).String()},
		engine.Attr{Key: "err", Value: tr.Describe(n.err)},
		engine.Attr{Key: "restarts", Value: strconv.Itoa(restarts)},
		engine.Attr{Key: "delay", Value: delay.String()},
		engine.Attr{Key: "limit", Value: strconv.FormatBool(limit)},
		engine.Attr{Key: "escalated", Value: strconv.FormatBool(escalated)},
	)
}

// scopeName returns the scope attribute of lockstep.directive: "one-for-one" or "all-for-one".
func scopeName(s supervise.Scope) string {
	if s == supervise.AllForOne {
		return "all-for-one"
	}
	return "one-for-one"
}
