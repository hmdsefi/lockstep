// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// pressureLabel is the label of the kernel events that start and end pressure episodes.
const pressureLabel = "lockstep/pressure"

// vt returns the virtual time d after the start of the run.
func vt(d time.Duration) kernel.Time { return kernel.Time(0).Add(d) }

// pressureChanges returns the lockstep.overload records of node n with event on or off and
// reason pressure, in order, each as "<event>@<time>".
func pressureChanges(recs []kernel.Record, n *kernel.Node) []string {
	var out []string
	for _, r := range ofKind(recs, "lockstep.overload") {
		ev := attr(r, "event")
		if r.Node != n.ID() || (ev != "on" && ev != "off") || attr(r, "reason") != "pressure" {
			continue
		}
		out = append(out, ev+"@"+r.At.String())
	}
	return out
}

// noStateOrSource reports an error on tt for every lockstep.overload record that carries a state or
// source attribute: SFT emits no lockstep.overload record of its own (SFT-160).
func noStateOrSource(tt testing.TB, recs []kernel.Record) {
	tt.Helper()
	for _, r := range ofKind(recs, "lockstep.overload") {
		for _, k := range attrKeys(r) {
			if k == "state" || k == "source" {
				tt.Errorf("lockstep.overload record %d %v has a %s attribute", r.Seq, attrKeys(r), k)
			}
		}
	}
}

// change returns the pressureChanges entry of a change to event at d.
func change(event string, d time.Duration) string { return event + "@" + vt(d).String() }

// equalStrings reports an error on tt unless got equals want.
func equalStrings(tt testing.TB, what string, got, want []string) {
	tt.Helper()
	if strings.Join(got, " ") != strings.Join(want, " ") {
		tt.Errorf("%s: %v, want %v", what, got, want)
	}
}

// AT-SFT-13, SFT-100, SFT-110, SFT-170: random pressure episodes on n1 follow the draws of a fresh
// lockstep/pressure/n1 stream: a gap at the rule's start, a length at each episode start and a gap
// at each episode end. No episode starts at or after the recovery start (45s), an episode still
// active there ends there, and every pressure event is a kernel event labeled lockstep/pressure.
func TestATSFT13PressureRule(t *testing.T) {
	opts := fullTrace(1)
	opts.Duration = 60 * time.Second
	rule := sim.PressureRule{Every: 5 * time.Second, MinFor: time.Second, MaxFor: 2 * time.Second}
	cut := 0 // seeds with an episode active at the recovery start
	seeds := make([]uint64, 20)
	for i := range seeds {
		seeds[i] = uint64(i + 1)
	}
	sim.SimulateSeeds(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		n1 := s.Node("n1", noSetup)
		w.Plan(&fault.Random{
			Rules: []fault.Rule{{Kind: fault.KindPause, Every: 10 * time.Second, MaxFor: time.Second}},
			Quiet: 15 * time.Second,
		})
		s.Pressure("n1", rule)
		w.RunFor(60 * time.Second)

		rec := w.RecoveryStart()
		if rec != vt(45*time.Second) {
			tt.Fatalf("RecoveryStart() = %v, want 45s", rec)
		}
		ref := kernel.New(kernel.Config{Seed: w.Seed()}).Rand("lockstep/pressure/n1")
		gap := func() time.Duration { return kernel.Uniform(ref, rule.Every/2, rule.Every+rule.Every/2) }
		var want []string
		events := 1 // the start event that finds the recovery start
		for start := vt(gap()); start < rec; {
			end := min(start.Add(kernel.Uniform(ref, rule.MinFor, rule.MaxFor)), rec)
			want = append(want, "on@"+start.String(), "off@"+end.String())
			if end == rec {
				cut++
			}
			events += 2
			start = end.Add(gap())
		}
		recs := w.Sim.Records()
		equalStrings(tt, "pressure changes on n1", pressureChanges(recs, n1), want)

		labeled, last := 0, ""
		for _, r := range recs {
			switch {
			case r.Kind == "kernel.event":
				last = r.Text
				if r.Text == pressureLabel {
					labeled++
				}
			case r.Kind == "lockstep.overload" && attr(r, "reason") == "pressure" && last != pressureLabel:
				tt.Errorf("lockstep.overload event=%s at %v ran in a kernel event labeled %q, want %q",
					attr(r, "event"), r.At, last, pressureLabel)
			}
		}
		if labeled != events {
			tt.Errorf("%d kernel events labeled %s, want %d", labeled, pressureLabel, events)
		}
		noStateOrSource(tt, recs)
	}, seeds...)
	if cut == 0 {
		t.Errorf("no seed has an episode active at the recovery start; pick other seeds")
	}
}

// pressureNode is the state of a node whose setup spawns a FailSend sink and a flaky actor that
// fails once at failAt, and counts the flaky actor's starts.
type pressureNode struct {
	sink   lockstep.PID[Deposit]
	starts int
}

// setup returns the node setup of p.
func (p *pressureNode) setup(failAt time.Duration) func(sys *lockstep.System) {
	return func(sys *lockstep.System) {
		p.sink = spawn(sys, "sink", &actor[Deposit]{})
		spawn(sys, "flaky", &actor[Tick]{
			started: func(ctx *lockstep.Context[Tick]) {
				p.starts++
				if p.starts == 1 {
					ctx.After(failAt, Tick{})
				}
			},
			receive: func(*lockstep.Context[Tick], Tick) { panic("flaky fails") },
		})
	}
}

// AT-SFT-14, SFT-125, SFT-150, SFT-160: during PressureFor's episode a normal spawn on n1 is refused
// with ErrOverloaded, a send to a FailSend actor is shed with both sentinels and an event=shed
// record, and a supervisor restart succeeds; after the episode a spawn succeeds. The runtime
// records one event=on at 1s and one event=off at 2s, and no lockstep.overload record has a state
// or source attribute.
func TestATSFT14PressureEffects(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		p := &pressureNode{}
		n1 := s.Node("n1", p.setup(1200*time.Millisecond))
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(500 * time.Millisecond)

		sys := s.System("n1")
		_, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[Deposit] { return &actor[Deposit]{} })
		if want := "lockstep: spawn in /user: overloaded (pressure)"; err == nil || err.Error() != want || !errors.Is(err, lockstep.ErrOverloaded) {
			tt.Errorf("Spawn during the episode = %v, want ErrOverloaded with text %q", err, want)
		}
		err = sim.Send(s, "n1", p.sink, Deposit{N: 1})
		if !errors.Is(err, lockstep.ErrOverloaded) || !errors.Is(err, lockstep.ErrMailboxFull) {
			tt.Errorf("Send to the FailSend sink during the episode = %v, want ErrOverloaded and ErrMailboxFull", err)
		}
		w.RunFor(400 * time.Millisecond)
		if p.starts != 2 {
			tt.Errorf("flaky started %d times by 1.9s, want 2 (one restart during the episode)", p.starts)
		}
		w.RunFor(200 * time.Millisecond)
		if _, err := lockstep.Spawn(sys, "b", func() lockstep.Actor[Deposit] { return &actor[Deposit]{} }); err != nil {
			tt.Errorf("Spawn after the episode: %v", err)
		}

		recs := w.Sim.Records()
		equalStrings(tt, "pressure changes on n1", pressureChanges(recs, n1), []string{change("on", time.Second), change("off", 2*time.Second)})
		shed := 0
		for _, r := range ofKind(recs, "lockstep.overload") {
			if attr(r, "event") == "shed" && attr(r, "to") == p.sink.String() && attr(r, "reason") == "pressure" {
				shed++
			}
		}
		if shed != 1 {
			tt.Errorf("%d event=shed records for the sink, want 1", shed)
		}
		if n := len(ofKind(recs, "lockstep.mailbox.full")); n != 0 {
			tt.Errorf("%d lockstep.mailbox.full records, want 0", n)
		}
		noStateOrSource(tt, recs)
	})
}

// AT-SFT-15, SFT-100, SFT-120: Pressure and PressureFor panic for an unknown node, an invalid rule
// and a duration that is not positive.
func TestATSFT15PressureMisuse(t *testing.T) {
	oneSeed(t, func(s *sim.Sim) {
		tt := s.World().T()
		s.Node("n1", noSetup)
		ok := sim.PressureRule{Every: time.Second, MinFor: time.Second, MaxFor: time.Second}
		expectPanic(tt, "lockstep: sim: PressureFor: duration 0s must be positive", func() { s.PressureFor("n1", 0) })
		expectPanic(tt, "lockstep: sim: PressureFor: duration -1s must be positive", func() { s.PressureFor("n1", -time.Second) })
		expectPanic(tt, `lockstep: sim: unknown node "zz"`, func() { s.Pressure("zz", ok) })
		expectPanic(tt, `lockstep: sim: unknown node "zz"`, func() { s.PressureFor("zz", time.Second) })
		for _, rule := range []sim.PressureRule{
			{Every: time.Second, MinFor: 2 * time.Second, MaxFor: time.Second},
			{Every: 0, MinFor: time.Second, MaxFor: time.Second},
			{Every: -time.Second, MinFor: time.Second, MaxFor: time.Second},
			{Every: time.Second, MinFor: 0, MaxFor: time.Second},
			{Every: fault.MaxRuleDuration + 1, MinFor: time.Second, MaxFor: time.Second},
			{Every: time.Second, MinFor: time.Second, MaxFor: fault.MaxRuleDuration + 1},
		} {
			func() {
				defer func() {
					msg, _ := recover().(string)
					if !strings.HasPrefix(msg, "lockstep: sim: Pressure: ") {
						tt.Errorf("Pressure(n1, %+v) panicked with %q, want a message starting %q", rule, msg, "lockstep: sim: Pressure: ")
					}
				}()
				s.Pressure("n1", rule)
			}()
		}
	})
}

// AT-SFT-16, SFT-125: pressure belongs to the kernel node. A system booted during an episode records
// event=on before setup runs, and setup's spawn is refused; the episode ends at its time on the new
// system. Overlapping episodes turn the source on and off once.
func TestATSFT16PressureAcrossCrash(t *testing.T) {
	t.Run("restart", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var errs []error
			n1 := s.Node("n1", func(sys *lockstep.System) {
				_, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[Deposit] { return &actor[Deposit]{} })
				errs = append(errs, err)
			})
			w.RunFor(time.Second)
			s.PressureFor("n1", 3*time.Second)
			w.RunFor(500 * time.Millisecond)
			n1.Crash()
			w.RunFor(500 * time.Millisecond)
			n1.Restart()
			w.RunFor(3 * time.Second)

			if len(errs) != 2 || errs[0] != nil || !errors.Is(errs[1], lockstep.ErrOverloaded) {
				tt.Fatalf("setup spawns: %v; want admitted at the first boot, ErrOverloaded at the restart", errs)
			}
			recs := w.Sim.Records()
			equalStrings(tt, "pressure changes on n1", pressureChanges(recs, n1), []string{change("on", time.Second), change("on", 2*time.Second), change("off", 4*time.Second)})
			var at2 []string
			for _, r := range ofKind(recs, "lockstep.overload") {
				if r.Node == n1.ID() && r.At == vt(2*time.Second) {
					at2 = append(at2, attr(r, "event"))
				}
			}
			equalStrings(tt, "lockstep.overload events on n1 at 2s", at2, []string{"on", "refuse"})
			for _, r := range recs {
				if r.Node == n1.ID() && r.At == vt(2*time.Second) && strings.HasPrefix(r.Kind, "lockstep.") {
					if r.Kind != "lockstep.overload" || attr(r, "event") != "on" {
						tt.Errorf("first lockstep record of n1 at 2s: %s %v, want lockstep.overload event=on", r.Kind, r.Attrs)
					}
					break
				}
			}
			noStateOrSource(tt, recs)
		})
	})
	t.Run("overlap", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			n1 := s.Node("n1", noSetup)
			w.RunFor(time.Second)
			s.PressureFor("n1", 2*time.Second)
			w.RunFor(time.Second)
			s.PressureFor("n1", 2*time.Second)
			w.RunFor(3 * time.Second)
			equalStrings(tt, "pressure changes on n1", pressureChanges(w.Sim.Records(), n1), []string{change("on", time.Second), change("off", 4*time.Second)})
		})
	})
}

// AT-OVL-12, OVL-100, OVL-210: under pressure from 1s to 2s a normal spawn at 1.5s is refused with
// OVL-100's text, a spawn from the Started of a restarted actor (a rebuild, a reserve spawn) is
// admitted, and a normal spawn at 2.5s is admitted.
func TestATOVL12RefusalUnderPressure(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		starts := 0
		var rebuildErr error
		s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "p", &actor[Tick]{
				started: func(ctx *lockstep.Context[Tick]) {
					starts++
					if starts == 1 {
						ctx.After(1500*time.Millisecond, Tick{})
						return
					}
					_, rebuildErr = lockstep.Spawn(ctx, "k", func() lockstep.Actor[Deposit] { return &actor[Deposit]{} })
				},
				receive: func(*lockstep.Context[Tick], Tick) { panic("p fails") },
			})
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(500 * time.Millisecond)

		_, err := lockstep.Spawn(s.System("n1"), "a", func() lockstep.Actor[Deposit] { return &actor[Deposit]{} })
		if want := "lockstep: spawn in /user: overloaded (pressure)"; err == nil || err.Error() != want || !errors.Is(err, lockstep.ErrOverloaded) {
			tt.Errorf("normal spawn at 1.5s = %v, want ErrOverloaded with text %q", err, want)
		}
		w.RunFor(100 * time.Millisecond)
		if starts != 2 || rebuildErr != nil {
			tt.Errorf("p started %d times, the rebuild's spawn returned %v; want 2 starts and the spawn admitted", starts, rebuildErr)
		}
		w.RunFor(900 * time.Millisecond)
		if _, err := lockstep.Spawn(s.System("n1"), "b", func() lockstep.Actor[Deposit] { return &actor[Deposit]{} }); err != nil {
			tt.Errorf("normal spawn at 2.5s: %v", err)
		}
	})
}

// AT-OVL-19, OVL-160: an episode that ends while its node is down leaves the node's next system
// admitting: setup's spawn at the restart is admitted and the new system records no event=on.
func TestATOVL19RebuiltSystemAdmits(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var errs []error
		n1 := s.Node("n1", func(sys *lockstep.System) {
			_, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[Deposit] { return &actor[Deposit]{} })
			errs = append(errs, err)
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(500 * time.Millisecond)
		n1.Crash()
		w.RunFor(1500 * time.Millisecond)
		n1.Restart()
		w.RunFor(time.Second)

		if len(errs) != 2 || errs[0] != nil || errs[1] != nil {
			tt.Fatalf("setup spawns: %v; want both admitted", errs)
		}
		equalStrings(tt, "pressure changes on n1", pressureChanges(w.Sim.Records(), n1), []string{change("on", time.Second)})
	})
}
