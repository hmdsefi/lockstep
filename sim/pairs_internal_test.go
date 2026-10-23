// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/lockstep"
)

// pairPing is the message of the actors of the pair counting tests.
type pairPing struct{}

// pairSink counts the pairPing messages it receives in *n.
type pairSink struct{ n *int }

func (a pairSink) Receive(*lockstep.Context[pairPing], pairPing) { *a.n++ }

// pairSender sends one pairPing to to for every pairPing it receives.
type pairSender struct{ to lockstep.PID[pairPing] }

func (a pairSender) Receive(ctx *lockstep.Context[pairPing], _ pairPing) {
	if err := lockstep.Send(ctx, a.to, pairPing{}); err != nil {
		panic(err)
	}
}

// SIM-250, SFT-090: a held message is pending lockstep/delay activity until it is offered: as a
// world event before the release time, and as a node event after it while the receiver is paused.
// Discarding the queue (the receiver crashed) cancels the event and settles the counts.
func TestSFT090HeldMessageIsActivity(t *testing.T) {
	Simulate(t, Options{Options: faultline.Options{Seeds: 3}}, func(s *Sim) {
		w, tt := s.World(), s.World().T()
		var got int
		toB := lockstep.PIDAt[pairPing]("n2", "/user/b")
		a := lockstep.PIDAt[pairPing]("n1", "/user/a")
		s.Node("n1", func(sys *lockstep.System) {
			if _, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[pairPing] { return pairSender{to: toB} }); err != nil {
				panic(err)
			}
		})
		n2 := s.Node("n2", func(sys *lockstep.System) {
			if _, err := lockstep.Spawn(sys, "b", func() lockstep.Actor[pairPing] { return pairSink{n: &got} }); err != nil {
				panic(err)
			}
		})
		s.DelayBetween(a.Address(), toB.Address(), 50*time.Millisecond, 50*time.Millisecond)
		w.RunFor(10 * time.Millisecond)
		check := func(when string, quiescent bool, worldDelays, nodeDelays int) {
			tt.Helper()
			if s.quiescent() != quiescent || s.world.byLabel[delayLabel] != worldDelays || s.events.byLabel[delayLabel] != nodeDelays {
				tt.Errorf("%s: quiescent %v, %d world and %d node lockstep/delay events; want %v, %d and %d",
					when, s.quiescent(), s.world.byLabel[delayLabel], s.events.byLabel[delayLabel], quiescent, worldDelays, nodeDelays)
			}
		}
		check("nothing held", true, 0, 0)

		send := func() {
			tt.Helper()
			if err := Send(s, "n1", a, pairPing{}); err != nil {
				tt.Fatalf("Send: %v", err)
			}
			w.RunFor(10 * time.Millisecond) // the message is sent, arrives and is held
		}
		send()
		check("held before the release", false, 1, 0)
		w.RunFor(100 * time.Millisecond)
		check("offered", true, 0, 0)
		if got != 1 {
			tt.Fatalf("B received %d messages, want 1", got)
		}

		send()
		n2.Pause()
		w.RunFor(100 * time.Millisecond)
		check("release posted to the paused node", false, 0, 1)
		n2.Resume()
		w.RunFor(10 * time.Millisecond)
		check("offered after the resume", true, 0, 0)
		if got != 2 {
			tt.Fatalf("B received %d messages, want 2", got)
		}

		send()
		n2.Crash()
		w.RunFor(100 * time.Millisecond) // the release finds the node down and discards the queue
		check("discarded after a crash", true, 0, 0)
		if got != 2 {
			tt.Errorf("B received %d messages, want 2", got)
		}
	})
}
