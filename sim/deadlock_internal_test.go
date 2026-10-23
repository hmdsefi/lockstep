// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/gograph"
	"github.com/hmdsefi/lockstep"
)

// SIM-250: of the events sim scheduled, only lockstep activity keeps the world from being
// quiescent. A pending background timer (ENG-070) does not; an activity timer does until it is
// cancelled or its node crashes; an ask timeout counts as a pending ask, not as activity.
func TestSIM250BackgroundTimersIgnored(t *testing.T) {
	Simulate(t, Options{Options: faultline.Options{Seeds: 1}}, func(s *Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("n1", func(*lockstep.System) {})
		w.RunFor(10 * time.Millisecond)
		e := s.lookup("n1").eng
		check := func(when string, quiescent bool, asks int) {
			tt.Helper()
			if got := s.quiescent(); got != quiescent {
				tt.Errorf("%s: quiescent() = %v, want %v", when, got, quiescent)
			}
			if s.events.asks != asks {
				tt.Errorf("%s: %d pending ask timeouts, want %d", when, s.events.asks, asks)
			}
		}
		check("booted", true, 0)

		e.AfterFunc(time.Hour, "lockstep/remote/tick", func() { tt.Error("the background timer ran") })
		e.AfterFunc(time.Hour, "lockstep/overload-sample", func() { tt.Error("the background timer ran") })
		check("background timers pending", true, 0)

		cancelAsk := e.AfterFunc(time.Hour, "lockstep/ask-timeout", func() {})
		check("ask timeout pending", true, 1)
		if !cancelAsk() {
			tt.Fatal("cancelling the pending ask timeout returned false")
		}
		check("ask timeout cancelled", true, 0)

		cancelTimer := e.AfterFunc(time.Hour, "lockstep/timer", func() {})
		check("activity timer pending", false, 0)
		if !cancelTimer() {
			tt.Fatal("cancelling the pending timer returned false")
		}
		if cancelTimer() {
			tt.Error("cancelling the timer twice returned true")
		}
		check("activity timer cancelled", true, 0)

		ran := false
		e.AfterFunc(time.Millisecond, "lockstep/backoff", func() { ran = true })
		check("backoff pending", false, 0)
		w.RunFor(10 * time.Millisecond)
		if !ran {
			tt.Fatal("the backoff timer did not run")
		}
		check("backoff ran", true, 0)

		// A label ENG-070 does not list is lockstep activity.
		e.AfterFunc(time.Hour, "lockstep/other", func() {})
		e.AfterFunc(time.Hour, "lockstep/ask-timeout", func() {})
		check("unlisted label pending", false, 1)
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n1"})
		check("crashed", true, 0)
	})
}

// SIM-250: a pending world event of class activity, such as a lockstep/delay release (SFT-090),
// keeps the world from being quiescent until it runs or is cancelled, and survives a crash; a
// pending lockstep/pressure world event does not count.
func TestSIM250WorldEvents(t *testing.T) {
	Simulate(t, Options{Options: faultline.Options{Seeds: 1}}, func(s *Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("n1", func(*lockstep.System) {})
		w.RunFor(10 * time.Millisecond)
		check := func(when string, want bool) {
			tt.Helper()
			if got := s.quiescent(); got != want {
				tt.Errorf("%s: quiescent() = %v, want %v", when, got, want)
			}
		}

		s.worldAtFront(w.Sim.Now().Add(time.Hour), "lockstep/pressure", func() {})
		s.worldAt(w.Sim.Now().Add(time.Hour), "lockstep/pressure", func() {})
		check("pressure events pending", true)

		cancelFront := s.worldAtFront(w.Sim.Now().Add(time.Hour), "lockstep/delay", func() { tt.Error("the cancelled release ran") })
		check("front release pending", false)
		if !cancelFront() {
			tt.Fatal("cancelling the pending front release returned false")
		}
		check("front release cancelled", true)

		cancel := s.worldAt(w.Sim.Now().Add(time.Hour), "lockstep/delay", func() { tt.Error("the cancelled release ran") })
		check("release pending", false)
		if !cancel() {
			tt.Fatal("cancelling the pending release returned false")
		}
		if cancel() {
			tt.Error("cancelling the release twice returned true")
		}
		check("release cancelled", true)

		ran := false
		s.worldAtFront(w.Sim.Now().Add(time.Millisecond), "lockstep/delay", func() { ran = true })
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n1"})
		check("release pending after a crash", false)
		w.RunFor(10 * time.Millisecond)
		if !ran {
			tt.Fatal("the release did not run")
		}
		check("release ran", true)
		if s.world.activity != 0 || s.world.byLabel["lockstep/delay"] != 0 || s.world.byLabel["lockstep/pressure"] != 2 {
			tt.Errorf("world counts %+v, want only the 2 pressure events pending", s.world)
		}
	})
}

// SIM-250: when a node with pending asks, turns and timers crashes, each of its counted events is
// subtracted exactly once, whether Discard cancels it or the crash drops it: every count returns
// to zero, and none goes below zero (eventCounts.add panics if one would).
func TestSIM250CrashSettlesEachEventOnce(t *testing.T) {
	Simulate(t, Options{Options: faultline.Options{Seeds: 3}}, func(s *Sim) {
		w, tt := s.World(), s.World().T()
		var silentLocal, silentRemote, askerPID lockstep.PID[int]
		silent := func(sys *lockstep.System, pid *lockstep.PID[int]) {
			p, err := lockstep.Spawn(sys, "silent", func() lockstep.Actor[int] { return silentActor{} })
			if err != nil {
				panic(err)
			}
			*pid = p
		}
		s.Node("n2", func(sys *lockstep.System) { silent(sys, &silentRemote) })
		s.Node("n1", func(sys *lockstep.System) {
			silent(sys, &silentLocal)
			p, err := lockstep.Spawn(sys, "asker", func() lockstep.Actor[int] { return asker{&silentLocal, &silentRemote} })
			if err != nil {
				panic(err)
			}
			askerPID = p
		})
		w.RunFor(10 * time.Millisecond)
		if err := Send(s, "n1", askerPID, 1); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)
		e := s.lookup("n1").eng
		if e.events.asks != 4 || e.events.activity == 0 {
			tt.Fatalf("n1's counts before the crash %+v, want 4 asks and some activity", e.events)
		}
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n1"})
		for _, label := range e.labels {
			if n := e.events.byLabel[label]; n != 0 {
				tt.Errorf("n1 still counts %d %s events after the crash", n, label)
			}
			if n := s.events.byLabel[label]; n != 0 {
				tt.Errorf("the Sim still counts %d %s events after the crash", n, label)
			}
		}
		if s.events.activity != 0 || s.events.asks != 0 || e.events.activity != 0 || e.events.asks != 0 {
			tt.Errorf("counts after the crash: Sim %+v, n1 %+v; want all zero", s.events, e.events)
		}
		if !s.quiescent() {
			tt.Error("the world is not quiescent after the crash")
		}
		w.RunFor(10 * time.Second) // nothing of n1's crashed incarnation runs or is subtracted again
		if s.events.activity != 0 || s.events.asks != 0 {
			tt.Errorf("counts after running on: %+v, want all zero", s.events)
		}
	})
}

// SIM-250: settle subtracts an event once, however often it is called, and nothing once the crash
// drop has subtracted the engine's pending events.
func TestSIM250SettleOnce(t *testing.T) {
	Simulate(t, Options{Options: faultline.Options{Seeds: 1}}, func(s *Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("n1", func(*lockstep.System) {})
		w.RunFor(10 * time.Millisecond)
		e := s.lookup("n1").eng
		a, b := &pendingEvent{label: "lockstep/timer"}, &pendingEvent{label: "lockstep/timer"}
		e.count("lockstep/timer", 2)
		e.settle(a)
		e.settle(a)
		if n := s.events.byLabel["lockstep/timer"]; n != 1 {
			tt.Fatalf("after settling one of two events twice: %d pending, want 1", n)
		}
		e.dropPending()
		e.settle(b)
		if n := s.events.byLabel["lockstep/timer"]; n != 0 || s.events.activity != 0 {
			tt.Errorf("after the drop and a late settle: %d pending, activity %d; want 0", n, s.events.activity)
		}
	})
}

// silentActor never answers.
type silentActor struct{}

func (silentActor) Receive(*lockstep.Context[int], int) {}

// asker, on 1, asks *local and *remote twice each and starts a periodic timer, which sends 2.
type asker struct{ local, remote *lockstep.PID[int] }

func (a asker) Receive(ctx *lockstep.Context[int], m int) {
	if m != 1 {
		return
	}
	for _, to := range []lockstep.PID[int]{*a.local, *a.remote, *a.local, *a.remote} {
		lockstep.Ask(ctx, to, func(lockstep.ReplyTo[int]) int { return 0 })
	}
	ctx.Every(100*time.Millisecond, 2)
}

// SIM-270: each strongly connected component with two or more vertices, or with a self edge, is a
// cycle; its path is the shortest one from its smallest label back to it, inside the component;
// cycles are ordered by start label.
func TestSIM270CyclePaths(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edges []string // "x>y" is an edge from x to y, added in order
		want  []string // each cycle's path, joined with ">"
	}{
		{"none", []string{"a>b", "b>c"}, nil},
		{"pair", []string{"b>a", "a>b"}, []string{"a>b>a"}},
		{"self", []string{"e>e"}, []string{"e>e"}},
		// The path follows the shortcut c>a, not the longer way through d.
		{"shortest", []string{"a>b", "b>c", "c>d", "d>a", "c>a"}, []string{"a>b>c>a"}},
		// Of two shortest paths, the one through the first neighbor in edge order.
		{"first-neighbor", []string{"a>c", "a>b", "b>a", "c>a"}, []string{"a>c>a"}},
		// x is reachable from the cycle but not on it, and y waits on it from outside.
		{"outside", []string{"y>b", "b>x", "b>a", "a>b"}, []string{"a>b>a"}},
		{"ordered", []string{"d>c", "c>d", "e>e", "b>a", "a>b"}, []string{"a>b>a", "c>d>c", "e>e"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := gograph.New[string](gograph.Directed())
			for _, e := range tc.edges {
				from, to, _ := strings.Cut(e, ">")
				if _, err := g.AddEdge(vertex(g, from), vertex(g, to)); err != nil {
					t.Fatal(err)
				}
			}
			var got []string
			for _, c := range findCycles(g) {
				got = append(got, strings.Join(c, ">"))
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("cycles %q, want %q", got, tc.want)
			}
		})
	}
}
