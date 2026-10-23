// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// budgetChanges returns the lockstep.overload records of node n with event on or off and reason
// budget, in order, each as "<event> live=<live> high=<high> low=<low>".
func budgetChanges(recs []kernel.Record, n *kernel.Node) []string {
	var out []string
	for _, r := range ofKind(recs, "lockstep.overload") {
		ev := attr(r, "event")
		if r.Node != n.ID() || (ev != "on" && ev != "off") || attr(r, "reason") != "budget" {
			continue
		}
		out = append(out, fmt.Sprintf("%s live=%s high=%s low=%s", ev, attr(r, "live"), attr(r, "high"), attr(r, "low")))
	}
	return out
}

// expectNext reports an error on tt unless the record right after the first record that matches
// is lockstep.overload with event and reason budget: the budget changes in the event of that
// record (SFT-140).
func expectNext(tt testing.TB, recs []kernel.Record, what, event string, match func(kernel.Record) bool) {
	tt.Helper()
	i := slices.IndexFunc(recs, match)
	if i < 0 {
		tt.Errorf("no %s record", what)
		return
	}
	if i+1 >= len(recs) {
		tt.Errorf("no record after the %s record", what)
		return
	}
	next := recs[i+1]
	if next.Kind != "lockstep.overload" || attr(next, "event") != event || attr(next, "reason") != "budget" {
		tt.Errorf("record after the %s record: %s %v, want lockstep.overload event=%s reason=budget", what, next.Kind, next.Attrs, event)
	}
}

// overloadWatcher subscribes to Overloaded signals when it starts and keeps the ones it gets.
type overloadWatcher struct{ got *[]lockstep.Overloaded }

func (a *overloadWatcher) Started(ctx *lockstep.Context[Deposit]) { ctx.SubscribeOverload() }

func (a *overloadWatcher) Receive(*lockstep.Context[Deposit], Deposit) {}

func (a *overloadWatcher) Signal(_ *lockstep.Context[Deposit], sig lockstep.Signal) {
	if o, ok := sig.(lockstep.Overloaded); ok {
		*a.got = append(*a.got, o)
	}
}

// AT-SFT-17, SFT-140, SFT-150, SFT-160: with Budget{Actors: 10, ResumePercent: 50} the 10th live
// actor turns the budget source on, an 11th spawn is refused with ErrOverloaded, 6 live actors keep
// it on and 5 turn it off (5*100 <= 10*50). Each change is recorded right after the record that
// caused it, and a subscriber gets Active true, then false, with reason OverloadBudget.
func TestATSFT17ActorBudget(t *testing.T) {
	opts := fullTrace(3)
	opts.Budget = sim.Budget{Actors: 10, ResumePercent: 50}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []lockstep.Overloaded
		var pids []lockstep.PID[Deposit]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			_, err := lockstep.Spawn(sys, "watcher", func() lockstep.Actor[Deposit] { return &overloadWatcher{got: &got} })
			if err != nil {
				panic(err)
			}
			for i := 1; i <= 8; i++ {
				pids = append(pids, spawn(sys, fmt.Sprintf("a%d", i), &actor[Deposit]{}))
			}
		})
		w.RunFor(time.Second)
		sys := s.System("n1")
		if n := len(budgetChanges(w.Sim.Records(), n1)); n != 0 {
			tt.Fatalf("%d budget changes with 9 live actors, want 0", n)
		}

		pids = append(pids, spawn(sys, "a9", &actor[Deposit]{})) // the 10th live actor
		_, err := lockstep.Spawn(sys, "a10", func() lockstep.Actor[Deposit] { return &actor[Deposit]{} })
		if want := "lockstep: spawn in /user: overloaded (budget)"; err == nil || err.Error() != want || !errors.Is(err, lockstep.ErrOverloaded) {
			tt.Errorf("11th spawn = %v, want ErrOverloaded with text %q", err, want)
		}
		for _, p := range pids[:4] {
			if err := lockstep.Stop(sys, p.Address()); err != nil {
				tt.Fatalf("Stop(%s): %v", p, err)
			}
		}
		w.RunFor(time.Second)
		equalStrings(tt, "budget changes with 6 live actors", budgetChanges(w.Sim.Records(), n1), []string{"on live=0 high=0 low=0"})

		fifth := pids[4]
		if err := lockstep.Stop(sys, fifth.Address()); err != nil {
			tt.Fatalf("Stop(%s): %v", fifth, err)
		}
		w.RunFor(time.Second)

		recs := w.Sim.Records()
		equalStrings(tt, "budget changes", budgetChanges(recs, n1), []string{"on live=0 high=0 low=0", "off live=0 high=0 low=0"})
		expectNext(tt, recs, "lockstep.spawn of a9", "on", func(r kernel.Record) bool {
			return r.Kind == "lockstep.spawn" && attr(r, "name") == "a9" && attr(r, "result") == "ok"
		})
		expectNext(tt, recs, "lockstep.stop of "+fifth.String(), "off", func(r kernel.Record) bool {
			return r.Kind == "lockstep.stop" && attr(r, "actor") == fifth.String()
		})
		want := []lockstep.Overloaded{{Active: true, Reason: lockstep.OverloadBudget}, {Active: false, Reason: lockstep.OverloadBudget}}
		if !slices.Equal(got, want) {
			tt.Errorf("the subscriber got %+v, want %+v", got, want)
		}
		noStateOrSource(tt, recs)
	})
}

// Blob is a message whose size estimate is N bytes.
type Blob struct{ N int }

// Size returns the estimate of b.
func (b Blob) Size() int { return b.N }

// AT-SFT-18, SFT-140: with Budget{Bytes: 1000} the node systems keep memory estimates. An actor on
// a paused node gets Blobs of 600 and 400 bytes: the second reaches the budget and turns the
// budget source on with live=1000, high=1000 and low=800 (the default resume mark, 80%), and a
// third send is shed. After the node resumes, the actor's turn that handles the 600-byte Blob leaves
// 400 bytes, at or below the resume mark, and the source turns off with live=400.
func TestATSFT18ByteBudget(t *testing.T) {
	opts := fullTrace(3)
	opts.Budget = sim.Budget{Bytes: 1000}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var sink lockstep.PID[Blob]
		handled := 0
		n1 := s.Node("n1", func(sys *lockstep.System) {
			sink = spawn(sys, "sink", &actor[Blob]{receive: func(*lockstep.Context[Blob], Blob) { handled++ }})
		})
		w.RunFor(time.Second)
		if s.System("n1").Estimates(1) == nil {
			tt.Errorf("Estimates(1) = nil, want the sink's estimate: the node system has no WithMemoryEstimates")
		}

		n1.Pause()
		for _, n := range []int{600, 400} {
			if err := sim.Send(s, "n1", sink, Blob{N: n}); err != nil {
				tt.Fatalf("Send(Blob{%d}): %v", n, err)
			}
		}
		if err := sim.Send(s, "n1", sink, Blob{N: 1}); !errors.Is(err, lockstep.ErrOverloaded) {
			tt.Errorf("third Send = %v, want ErrOverloaded", err)
		}
		n1.Resume()
		w.RunFor(time.Second)
		if handled != 2 {
			tt.Errorf("the sink handled %d messages, want 2", handled)
		}

		recs := w.Sim.Records()
		// The 600-byte Blob's turn leaves 400 bytes, at or below the resume mark of 800: the source
		// turns off there (SFT-140).
		equalStrings(tt, "budget changes", budgetChanges(recs, n1), []string{"on live=1000 high=1000 low=800", "off live=400 high=1000 low=800"})
		if est := s.System("n1").Estimates(1); len(est) != 1 || est[0].MailboxBytes != 0 {
			tt.Errorf("Estimates(1) after both turns = %+v, want the sink with 0 mailbox bytes", est)
		}
		expectNext(tt, recs, "lockstep.deliver of the 400-byte Blob", "on", func(r kernel.Record) bool {
			return r.Kind == "lockstep.deliver" && attr(r, "to") == sink.String() && attr(r, "len") == "2"
		})
		expectNext(tt, recs, "lockstep.turn that handled the 600-byte Blob", "off", func(r kernel.Record) bool {
			return r.Kind == "lockstep.turn" && attr(r, "actor") == sink.String() && attr(r, "user") == "1"
		})
		shed := 0
		for _, r := range ofKind(recs, "lockstep.overload") {
			if attr(r, "event") == "shed" && attr(r, "to") == sink.String() && attr(r, "reason") == "budget" {
				shed++
			}
		}
		if shed != 1 {
			tt.Errorf("%d event=shed records for the sink with reason budget, want 1", shed)
		}
		noStateOrSource(tt, recs)
	})
}

// The scenarios of TestATSFT19InvalidBudget: each prints bodyRan from body.
var (
	_ = scenario("budget-actors", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Budget: sim.Budget{Actors: -1}}, printBody)
	})
	_ = scenario("budget-bytes", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Budget: sim.Budget{Bytes: -1}}, printBody)
	})
	_ = scenario("budget-resume", func(t *testing.T) {
		sim.SimulateSeeds(t, sim.Options{Budget: sim.Budget{ResumePercent: 100}}, printBody, 1)
	})
	_ = scenario("budget-resume-negative", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Budget: sim.Budget{Actors: 10, ResumePercent: -1}}, printBody)
	})
)

// AT-SFT-19, SFT-130, SIM-020: an invalid Options.Budget fails the test with
// "lockstep: sim: Options.Budget: <problem>" before any seed runs.
func TestATSFT19InvalidBudget(t *testing.T) {
	for _, tc := range []struct {
		scenario string
		want     string
	}{
		{"budget-actors", "lockstep: sim: Options.Budget: Actors -1 is negative"},
		{"budget-bytes", "lockstep: sim: Options.Budget: Bytes -1 is negative"},
		{"budget-resume", "lockstep: sim: Options.Budget: ResumePercent 100 is outside 0..99"},
		{"budget-resume-negative", "lockstep: sim: Options.Budget: ResumePercent -1 is outside 0..99"},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			out, code := runScenario(t, tc.scenario)
			if code == 0 {
				t.Fatalf("child exited 0, want a failure\n%s", out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("output lacks %q:\n%s", tc.want, out)
			}
			if strings.Contains(out, bodyRan) || strings.Contains(out, "seed=0x") {
				t.Errorf("a seed ran:\n%s", out)
			}
		})
	}
}
