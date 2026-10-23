// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package supervise

import "testing"

var allStates = []State{Starting, Running, Suspended, Restarting, Stopping, Stopped}

// TestATSUP01TransitionTable checks AT-SUP-01: CanTransition is true for exactly the 12 edges of
// SUP-010 among the 36 ordered pairs of states.
func TestATSUP01TransitionTable(t *testing.T) {
	edges := map[[2]State]bool{
		{Starting, Running}:     true,
		{Starting, Suspended}:   true,
		{Starting, Stopping}:    true,
		{Running, Suspended}:    true,
		{Running, Restarting}:   true,
		{Running, Stopping}:     true,
		{Suspended, Running}:    true,
		{Suspended, Restarting}: true,
		{Suspended, Stopping}:   true,
		{Restarting, Starting}:  true,
		{Restarting, Stopping}:  true,
		{Stopping, Stopped}:     true,
	}
	if len(edges) != 12 {
		t.Fatalf("test table has %d edges, want 12", len(edges))
	}
	pairs, trues := 0, 0
	for _, from := range allStates {
		for _, to := range allStates {
			pairs++
			got := CanTransition(from, to)
			if got {
				trues++
			}
			if want := edges[[2]State{from, to}]; got != want {
				t.Errorf("CanTransition(%v, %v) = %v, want %v", from, to, got, want)
			}
		}
	}
	if pairs != 36 || trues != 12 {
		t.Fatalf("pairs = %d, true = %d; want 36 and 12", pairs, trues)
	}
}

func TestCanTransitionRejectsUnknownStates(t *testing.T) {
	for _, bad := range []State{0, 7, 255} {
		for _, s := range allStates {
			if CanTransition(bad, s) || CanTransition(s, bad) {
				t.Errorf("CanTransition with unknown state %v and %v = true, want false", bad, s)
			}
		}
	}
}

func TestStateValues(t *testing.T) {
	for i, s := range allStates {
		if int(s) != i+1 {
			t.Errorf("%v = %d, want %d", s, s, i+1)
		}
	}
}

func TestStrings(t *testing.T) {
	for _, c := range []struct {
		s    State
		want string
	}{
		{Starting, "starting"}, {Running, "running"}, {Suspended, "suspended"},
		{Restarting, "restarting"}, {Stopping, "stopping"}, {Stopped, "stopped"},
		{0, "State(0)"}, {7, "State(7)"}, {255, "State(255)"},
	} {
		if got := c.s.String(); got != c.want {
			t.Errorf("State(%d).String() = %q, want %q", uint8(c.s), got, c.want)
		}
	}
	for _, c := range []struct {
		s    Source
		want string
	}{
		{InFactory, "factory"}, {InStarted, "started"}, {InReceive, "receive"}, {InSignal, "signal"},
		{InStopped, "stopped"}, {InDecider, "decider"}, {InEscalate, "escalate"},
		{0, "Source(0)"}, {8, "Source(8)"}, {255, "Source(255)"},
	} {
		if got := c.s.String(); got != c.want {
			t.Errorf("Source(%d).String() = %q, want %q", uint8(c.s), got, c.want)
		}
	}
}
