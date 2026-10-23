// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/artifact"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// waitKind says what a Wait message is.
type waitKind int

const (
	waitStart waitKind = iota // the body starts the actor
	waitAsk                   // a request to answer through ReplyTo
	waitDone                  // the actor's own ask resolved; Err is its error
	waitTick                  // a periodic timer tick
)

// Wait is the message type of the actors of the deadlock tests.
type Wait struct {
	Kind    waitKind
	ReplyTo lockstep.ReplyTo[int] // waitAsk only
	Err     error                 // waitDone only
}

// askWait is the build function of an ask that sends a Wait request.
func askWait(r lockstep.ReplyTo[int]) Wait { return Wait{Kind: waitAsk, ReplyTo: r} }

// waiter asks *peer when started, and when it gets a request it keeps the request and asks *peer
// unless it has asked already. It answers the requests it keeps only once its own ask has its
// answer, so two waiters that are each other's peer wait for each other; when its ask fails, it
// answers nothing, so the asks waiting for it fail too. onStart and onHold, when set,
// run in the turn that handles waitStart and in the turn that keeps a request.
type waiter struct {
	peer    *lockstep.PID[Wait]
	asked   bool
	held    []lockstep.ReplyTo[int]
	onStart func(ctx *lockstep.Context[Wait])
	onHold  func(ctx *lockstep.Context[Wait])
}

func (a *waiter) Receive(ctx *lockstep.Context[Wait], m Wait) {
	switch m.Kind {
	case waitStart:
		if a.onStart != nil {
			a.onStart(ctx)
		}
		a.ask(ctx)
	case waitAsk:
		a.held = append(a.held, m.ReplyTo)
		if a.onHold != nil {
			a.onHold(ctx)
		}
		a.ask(ctx)
	case waitDone:
		if m.Err != nil {
			return
		}
		for _, r := range a.held {
			r.Send(ctx, 1)
		}
		a.held = nil
	case waitTick:
	}
}

// ask asks *peer once and pipes the outcome back as waitDone.
func (a *waiter) ask(ctx *lockstep.Context[Wait]) {
	if a.asked {
		return
	}
	a.asked = true
	r := lockstep.Ask(ctx, *a.peer, askWait)
	lockstep.Pipe(ctx, r, func(_ int, err error) Wait { return Wait{Kind: waitDone, Err: err} })
}

// spawnWait spawns a at /user/<name> and stores its PID in *pid.
func spawnWait(sys *lockstep.System, name string, a lockstep.Actor[Wait], pid *lockstep.PID[Wait]) {
	p, err := lockstep.Spawn(sys, name, func() lockstep.Actor[Wait] { return a })
	if err != nil {
		panic(err)
	}
	*pid = p
}

// sink is an actor that never answers.
type sink struct{}

func (sink) Receive(*lockstep.Context[Wait], Wait) {}

// pairRun is AT-SIM-21's run: waiter A at /user/a on n1 and waiter B at /user/b on n2, each the
// other's peer. a and b configure the two waiters before they are spawned.
type pairRun struct {
	a, b       waiter
	pidA, pidB lockstep.PID[Wait]
}

// addPair adds n1 and n2 with the pair's waiters and runs until both nodes have booted.
func addPair(s *sim.Sim, p *pairRun) {
	p.a.peer, p.b.peer = &p.pidB, &p.pidA
	s.Node("n1", func(sys *lockstep.System) { spawnWait(sys, "a", &p.a, &p.pidA) })
	s.Node("n2", func(sys *lockstep.System) { spawnWait(sys, "b", &p.b, &p.pidB) })
	s.World().RunFor(10 * time.Millisecond)
}

// start sends waitStart to pid from n1's System.
func start(s *sim.Sim, pid lockstep.PID[Wait]) {
	if err := sim.Send(s, "n1", pid, Wait{Kind: waitStart}); err != nil {
		s.World().T().Fatalf("sim.Send: %v", err)
	}
}

// Addresses of the deadlock tests' actors. An actor's uid has the node incarnation, 1, in its high
// 32 bits and its spawn sequence in the low ones; the user and system guardians take 1 and 2
// (ACT-030, ACT-040), so the first actor that setup spawns has sequence 3.
const (
	addrA  = "lockstep://n1/user/a#100000003"
	addrB2 = "lockstep://n2/user/b#100000003" // B on n2 (AT-SIM-21)
	addrB1 = "lockstep://n1/user/b#100000004" // B on n1 (AT-SIM-23)
)

// pairDeadlock is AT-SIM-21's scenario: A starts at t=10ms, asks B, and B asks A. onHold, when
// set, is A's onHold.
func pairDeadlock(t *testing.T, onHold func(ctx *lockstep.Context[Wait])) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		p := &pairRun{}
		p.a.onHold = onHold
		addPair(s, p)
		start(s, p.pidA)
		s.World().RunFor(10 * time.Second)
	})
}

var _ = scenario("deadlock-pair", func(t *testing.T) { pairDeadlock(t, nil) })

// deadlockRecords returns the lockstep.deadlock records of a's trace.
func deadlockRecords(t *testing.T, a *artifact.Artifact) []kernel.Record {
	t.Helper()
	if a.Trace == nil {
		t.Fatal("the failing seed left no trace.jsonl")
	}
	return ofKind(a.Trace.Records, "lockstep.deadlock")
}

// checkDeadlockRecord checks that recs is one lockstep.deadlock record of node 0 with text
// "deadlock" and attributes cycle and cycles, in that order.
func checkDeadlockRecord(t *testing.T, recs []kernel.Record, cycle, cycles string) {
	t.Helper()
	if len(recs) != 1 {
		t.Fatalf("%d lockstep.deadlock records, want 1", len(recs))
	}
	r := recs[0]
	want := []kernel.Attr{{Key: "cycle", Value: cycle}, {Key: "cycles", Value: cycles}}
	if r.Node != 0 || r.Text != "deadlock" || len(r.Attrs) != 2 || r.Attrs[0] != want[0] || r.Attrs[1] != want[1] {
		t.Errorf("lockstep.deadlock record: node %d, text %q, attributes %v; want node 0, text %q, attributes %v",
			r.Node, r.Text, r.Attrs, "deadlock", want)
	}
}

// AT-SIM-21 (SIM-250, SIM-260, SIM-270, SIM-280): two actors on two nodes that ask each other fail
// the seed through lockstep/deadlock as soon as nothing else can happen, before either ask times
// out, with the cycle from the smaller address and a lockstep.deadlock record.
func TestATSIM21AskCycle(t *testing.T) {
	a, _ := failingSeedArtifact(t, "deadlock-pair")
	cycle := addrA + " → " + addrB2 + " → " + addrA
	f := a.Report.Failure
	want := failure{Kind: "invariant", Check: sim.CheckDeadlock, Signature: "invariant:" + sim.CheckDeadlock, Message: "lockstep: deadlock: " + cycle}
	if got := (failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}); got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
	// A asks at t=10ms, so either ask would time out at t=5.01s at the earliest.
	if at := time.Duration(f.AtNS); at <= 10*time.Millisecond || at >= 5*time.Second {
		t.Errorf("failure at %v, want after the asks and before their 5s timeouts", at)
	}
	checkDeadlockRecord(t, deadlockRecords(t, a), cycle, "1")
}

// timeoutRecords fails tt unless the trace of s has exactly want lockstep.timeout records and no
// lockstep.deadlock record.
func timeoutRecords(tt testing.TB, s *sim.Sim, want int) {
	tt.Helper()
	recs := s.World().Sim.Records()
	if got := len(ofKind(recs, "lockstep.timeout")); got != want {
		tt.Errorf("%d lockstep.timeout records, want %d", got, want)
	}
	if got := len(ofKind(recs, "lockstep.deadlock")); got != 0 {
		tt.Errorf("%d lockstep.deadlock records, want 0", got)
	}
}

// AT-SIM-22 (SIM-260): asks that do not count add no edge, so a wait that involves them is no
// deadlock: both asks time out and the seed passes.
func TestATSIM22NonCountingAsks(t *testing.T) {
	// The body asks B through n1's System, so the asker is no actor (ByActor false); B asks A, and
	// A never answers.
	t.Run("system-asker", func(t *testing.T) {
		sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
			var pidA, pidB lockstep.PID[Wait]
			b := &waiter{peer: &pidA}
			s.Node("n1", func(sys *lockstep.System) { spawnWait(sys, "a", sink{}, &pidA) })
			s.Node("n2", func(sys *lockstep.System) { spawnWait(sys, "b", b, &pidB) })
			s.World().RunFor(10 * time.Millisecond)
			r := lockstep.Ask(s.System("n1"), pidB, askWait)
			s.World().RunFor(10 * time.Second)
			if _, err := r.Await(context.Background()); err == nil {
				s.World().T().Error("the body's ask resolved without an error, want its timeout")
			}
			timeoutRecords(s.World().T(), s, 2)
		})
	})
	// A makes a NewReply ask, whose target is the zero address, and sends its ReplyTo to B; B keeps
	// it and asks A, and A never answers.
	t.Run("new-reply", func(t *testing.T) {
		sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
			var pidA, pidB lockstep.PID[Wait]
			b := &waiter{peer: &pidA}
			a := &actor[Wait]{receive: func(ctx *lockstep.Context[Wait], m Wait) {
				if m.Kind != waitStart {
					return
				}
				rt, _ := lockstep.NewReply[int](ctx)
				if err := lockstep.Send(ctx, pidB, Wait{Kind: waitAsk, ReplyTo: rt}); err != nil {
					panic(err)
				}
			}}
			s.Node("n1", func(sys *lockstep.System) { spawnWait(sys, "a", a, &pidA) })
			s.Node("n2", func(sys *lockstep.System) { spawnWait(sys, "b", b, &pidB) })
			s.World().RunFor(10 * time.Millisecond)
			start(s, pidA)
			s.World().RunFor(10 * time.Second)
			timeoutRecords(s.World().T(), s, 2)
		})
	})
}

// AT-SIM-23 (SIM-270, SIM-280): three cycles on one node, {C→D→C}, {A→B→A} and E asking itself,
// are reported by the first start address, with the number of the others.
var _ = scenario("deadlock-three", func(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		var pids [5]lockstep.PID[Wait]
		ws := [5]waiter{{peer: &pids[1]}, {peer: &pids[0]}, {peer: &pids[3]}, {peer: &pids[2]}, {peer: &pids[4]}}
		s.Node("n1", func(sys *lockstep.System) {
			for i, name := range []string{"a", "b", "c", "d", "e"} {
				spawnWait(sys, name, &ws[i], &pids[i])
			}
		})
		s.World().RunFor(10 * time.Millisecond)
		for _, i := range []int{2, 0, 4} {
			start(s, pids[i])
		}
		s.World().RunFor(10 * time.Second)
	})
})

func TestATSIM23SeveralCycles(t *testing.T) {
	a, _ := failingSeedArtifact(t, "deadlock-three")
	cycle := addrA + " → " + addrB1 + " → " + addrA
	f := a.Report.Failure
	want := failure{Kind: "invariant", Check: sim.CheckDeadlock, Signature: "invariant:" + sim.CheckDeadlock,
		Message: "lockstep: deadlock: " + cycle + " (and 2 more cycles)"}
	if got := (failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}); got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
	checkDeadlockRecord(t, deadlockRecords(t, a), cycle, "3")
}

// AT-SIM-24 (SIM-250): as AT-SIM-21, but A also runs a periodic timer, which is lockstep activity:
// the world never becomes quiescent, so there is no deadlock failure and both asks time out.
func TestATSIM24PeriodicTimerKeepsBusy(t *testing.T) {
	sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
		p := &pairRun{}
		p.a.onStart = func(ctx *lockstep.Context[Wait]) { ctx.Every(time.Second, Wait{Kind: waitTick}) }
		addPair(s, p)
		start(s, p.pidA)
		s.World().RunFor(10 * time.Second)
		timeoutRecords(s.World().T(), s, 2)
	})
}

// precedenceMessage is EFF-450's failure text for A of the precedence scenario.
const precedenceMessage = `lockstep: no effect handler: effect "prices.fetch" called by ` + addrA

// AT-SIM-27's scenario: as AT-SIM-21, but the turn in which A keeps B's request, which completes
// the deadlock, also calls Do for an effect without a handler (EFF-450) and cancels it, so its
// timeout timer does not keep the world busy.
var _ = scenario("deadlock-precedence", func(t *testing.T) {
	pairDeadlock(t, func(ctx *lockstep.Context[Wait]) {
		id := lockstep.Do(ctx, testFetch{"X"}, func(int64, error) Wait { return Wait{Kind: waitTick} })
		if !ctx.CancelEffect(id) {
			panic("the effect was not pending")
		}
	})
})

// AT-SIM-27 (SIM-300, SIM-310): when one event fails two named checks, the check registered first
// reports; a misuse failure stored by the body after the last event is reported by the final check.
func TestATSIM27CheckPrecedence(t *testing.T) {
	t.Run("invariant", func(t *testing.T) {
		a, _ := failingSeedArtifact(t, "deadlock-precedence")
		f := a.Report.Failure
		want := wantFailure("invariant", sim.CheckNoEffectHandler)
		want.Message = precedenceMessage
		if got := (failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}); got != want {
			t.Errorf("failure %+v, want %+v", got, want)
		}
		// The same run without the effect fails lockstep/deadlock at the same event: that event
		// completed the deadlock, and lockstep/no-effect-handler, registered first, reported.
		pair, _ := failingSeedArtifact(t, "deadlock-pair")
		if pf := pair.Report.Failure; pf.Check != sim.CheckDeadlock || pf.Event != f.Event {
			t.Errorf("without the effect: check %q at event %d, want %q at event %d", pf.Check, pf.Event, sim.CheckDeadlock, f.Event)
		}
		if recs := deadlockRecords(t, a); len(recs) != 0 {
			t.Errorf("%d lockstep.deadlock records, want 0: lockstep/deadlock must not run after an earlier check failed", len(recs))
		}
	})
	t.Run("final", func(t *testing.T) {
		if got, want := failingSeed(t, "final-misuse"), wantFailure("final", sim.CheckMisuse); got != want {
			t.Errorf("failure %+v, want %+v", got, want)
		}
	})
}
