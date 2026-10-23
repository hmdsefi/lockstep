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
