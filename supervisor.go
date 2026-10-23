// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
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
// then the command. A restart pushes end to the child; the child's ended notice starts the
// backoff, after which begin is pushed (SUP-190, SUP-280); the child's stopped notice cancels a
// pending begin and discards the child's state (SUP-250, SUP-290).
type supervisor struct {
	strategy Strategy
	// kids holds the state of every child that has failed and not stopped since, by address. It
	// answers lookups only and is never ranged over, so its order never matters (DET).
	kids map[Address]*kidState
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

// kid returns the state of child, creating it on the child's first failure.
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

// childFailed decides the failed notice n of a child, in a turn of the supervisor self, which is
// in Running (a cell) or a guardian (SUP-170, SUP-300); from is the ActorRef whose turn it is. A
// notice whose child has stopped, is not Suspended or is in another incarnation is stale: it is
// dropped with lockstep.drop reason stale-failure. Otherwise the strategy decides (decide) and the
// final directive is recorded in lockstep.directive. A restart pushes end{inc, err} to the child
// and waits for its ended notice (SUP-190).
func (sv *supervisor) childFailed(sys *System, self Address, n failed, from engine.ActorRef) {
	child, ok := sys.lookupCell(registryAddr(n.child))
	if !ok {
		sys.recordDropStale(self, n)
		return
	}
	// The child's state as its own turns last set it: lifecycle reads the mirror under the
	// child's lock, never its turn-owned fields.
	if st, inc := child.lifecycle(); st != supervise.Suspended || inc != n.inc {
		sys.recordDropStale(self, n)
		return
	}
	d, ok := sv.decide(n)
	if !ok {
		// SUP-180: a decider that panics or returns a value outside DirectiveRestart to
		// DirectiveResume fails the supervisor itself with source decider; that failure is applied
		// with the directives of SUP-200 to SUP-230. The child stays Suspended meanwhile.
		return
	}
	var restarts int
	var delay time.Duration
	limit := false
	k := sv.kid(n.child)
	if d == supervise.Restart {
		// SUP-270: one more restart in the window than MaxRestarts allows becomes an escalation.
		l := sv.strategy.limits()
		count, admitted := k.intensity.Admit(sys.eng.Clock.Now(), l)
		restarts = count
		if admitted {
			delay = supervise.Backoff(count, l)
		} else {
			d, limit = supervise.Escalate, true
		}
	}
	sys.recordDirective(self, n, supervise.Scope(sv.strategy.scope), d, restarts, delay, limit)
	switch d {
	case supervise.Restart:
		// OneForOne: the failed child only (SUP-190, SUP-250). AllForOne's group restart is
		// SUP-260's; until it exists, an AllForOne strategy restarts the failed child alone.
		k.restarting, k.inc, k.delay = true, n.inc, delay
		child.pushSystem(end{inc: n.inc, err: n.err}, from)
	case supervise.Stop, supervise.Escalate, supervise.Resume:
		// SUP-200 to SUP-220 and SUP-300's escalation at a guardian are applied with the
		// supervision signals; until then the child stays Suspended.
	}
}

// decide calls the strategy's decider for n as a guarded call with source decider, nil meaning
// DirectiveRestart, and normalizes its result for n's source (SUP-180): DirectiveResume after a
// failure in the factory or Started becomes DirectiveRestart. ok is false when the decider
// panicked or returned a value outside DirectiveRestart to DirectiveResume.
func (sv *supervisor) decide(n failed) (supervise.Directive, bool) {
	d := DirectiveRestart
	if decider := sv.strategy.Decider; decider != nil {
		if failed, _ := guard(func() { d = decider(n.err) }); failed {
			return 0, false
		}
	}
	return supervise.Normalize(supervise.Directive(d), n.source)
}

// childEnded handles the ended notice n of a child the supervisor restarts (SUP-190): it pushes
// begin to the child at once for a zero delay, and otherwise arms a backoff timer on the engine
// clock with label lockstep/backoff (ENG-070) whose callback pushes it. A notice the supervisor
// does not wait for, of another incarnation, changes nothing. from is the ActorRef whose turn it
// is.
func (sv *supervisor) childEnded(sys *System, n ended, from engine.ActorRef) {
	k := sv.kids[n.child]
	if k == nil || !k.restarting || k.inc != n.inc {
		return
	}
	k.restarting = false
	if k.delay <= 0 {
		sys.beginRestart(n.child, n.inc, from)
		return
	}
	child, inc := n.child, n.inc
	// The callback runs in engine context, outside any turn: it touches no supervisor state and
	// pushes begin with no origin (ENG-070, ENG-350).
	k.cancel = sys.eng.Clock.AfterFunc(k.delay, "lockstep/backoff", func() { sys.beginRestart(child, inc, nil) })
}

// childStopped handles a child's stopped notice for supervision: it cancels the child's pending
// begin, if any (SUP-290), and discards the child's state, its restart intensity included
// (SUP-250).
func (sv *supervisor) childStopped(child Address) {
	k := sv.kids[child]
	if k == nil {
		return
	}
	if k.cancel != nil {
		k.cancel()
	}
	delete(sv.kids, child)
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
// delay (0 unless d is a restart) and limit, true when intensity turned a restart into an
// escalation. escalated, SUP-300's mark of an escalation applied as a stop at a guardian, is false:
// no supervisor applies an escalation yet.
func (s *System) recordDirective(sup Address, n failed, scope supervise.Scope, d supervise.Directive,
	restarts int, delay time.Duration, limit bool,
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
		engine.Attr{Key: "escalated", Value: "false"},
	)
}

// scopeName returns the scope attribute of lockstep.directive: "one-for-one" or "all-for-one".
func scopeName(s supervise.Scope) string {
	if s == supervise.AllForOne {
		return "all-for-one"
	}
	return "one-for-one"
}
