// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// ACT-602, ACT-605, SIM-160: the simulation's transport carries a cross-node watch by reference,
// and the watched node applies it as the remoting core would. The watcher on n1 gets Terminated
// with the stop reason when the actor on n2 stops, and at once with NoActor for an address on n2
// that is not alive.
func TestRemoteWatchAcrossSimNodes(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var target lockstep.PID[int]
		s.Node("n2", func(sys *lockstep.System) { target = spawn(sys, "b", &actor[int]{}) })
		w.RunFor(time.Millisecond)

		byPath := lockstep.PIDAt[int]("n2", "/user/b").Address()
		none := lockstep.PIDAt[int]("n2", "/user/none").Address()
		var live, dead []lockstep.Terminated
		s.Node("n1", func(sys *lockstep.System) {
			for _, wt := range []struct {
				name string
				w    *watcher
			}{
				{"live", &watcher{target: byPath, got: &live}},
				{"dead", &watcher{target: none, got: &dead}},
			} {
				if _, err := lockstep.Spawn(sys, wt.name, func() lockstep.Actor[Tick] { return wt.w }); err != nil {
					panic(err)
				}
			}
		})
		w.RunFor(time.Second)
		if want := []lockstep.Terminated{{Addr: none, Reason: lockstep.NoActor}}; !slices.Equal(dead, want) {
			tt.Errorf("the watcher of %v got %v, want %v", none, dead, want)
		}
		if len(live) != 0 {
			tt.Errorf("the watcher of %v got %v before the stop, want nothing", byPath, live)
		}

		if err := lockstep.Stop(s.System("n2"), target.Address()); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)
		if want := []lockstep.Terminated{{Addr: byPath, Reason: lockstep.Normal}}; !slices.Equal(live, want) {
			tt.Errorf("the watcher of %v got %v, want %v", byPath, live, want)
		}
	})
}
