// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/lockstep/sim"
)

var (
	// One event stores a leak failure and then a misuse failure, and misuse stores a second one.
	_ = scenario("store-order", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, func(s *sim.Sim) {
			s.World().Sim.After(time.Millisecond, "test/store", func() {
				s.Store(sim.CheckLeak, errors.New("leak failure"))
				s.Store(sim.CheckMisuse, errors.New("first misuse"))
				s.Store(sim.CheckMisuse, errors.New("second misuse"))
			})
			s.World().RunFor(10 * time.Millisecond)
		})
	})
	// Body stores a misuse failure after the last event.
	_ = scenario("store-final", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, func(s *sim.Sim) {
			s.World().RunFor(s.World().End().Sub(s.World().Sim.Now()))
			s.Store(sim.CheckMisuse, errors.New("late misuse"))
		})
	})
	// The two checks that no earlier slot outranks: no-effect-handler before leak, deadlock last.
	_ = scenario("store-later-slots", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, func(s *sim.Sim) {
			s.World().Sim.After(time.Millisecond, "test/store", func() {
				s.Store(sim.CheckDeadlock, errors.New("deadlock failure"))
				s.Store(sim.CheckLeak, errors.New("leak failure"))
				s.Store(sim.CheckNoEffectHandler, errors.New("no handler failure"))
			})
			s.World().RunFor(10 * time.Millisecond)
		})
	})
	_ = scenario("store-deadlock-final", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, func(s *sim.Sim) {
			s.World().RunFor(s.World().End().Sub(s.World().Sim.Now()))
			s.Store(sim.CheckDeadlock, errors.New("late deadlock"))
		})
	})
	_ = scenario("store-nothing", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, func(s *sim.Sim) {
			s.Store(sim.CheckLeak, nil)
			s.World().RunFor(10 * time.Millisecond)
		})
	})
)

// SIM-300, SIM-310: of the failures stored by one event, the first registered check reports, with
// kind invariant; a slot keeps the first failure stored in it.
func TestSIM300CheckOrder(t *testing.T) {
	got := failingSeed(t, "store-order")
	want := failure{
		Kind:      "invariant",
		Check:     "lockstep/misuse",
		Signature: "invariant:lockstep/misuse",
		Message:   "first misuse",
	}
	if got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}

	got = failingSeed(t, "store-later-slots")
	want = failure{
		Kind:      "invariant",
		Check:     "lockstep/no-effect-handler",
		Signature: "invariant:lockstep/no-effect-handler",
		Message:   "no handler failure",
	}
	if got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
}

// SIM-300, SIM-310: a failure stored by body after the last event is reported by the final check.
func TestSIM300FinalCheck(t *testing.T) {
	for _, tc := range []struct {
		scenario, check, message string
	}{
		{"store-final", "lockstep/misuse", "late misuse"},
		{"store-deadlock-final", "lockstep/deadlock", "late deadlock"},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			got := failingSeed(t, tc.scenario)
			want := failure{Kind: "final", Check: tc.check, Signature: "final:" + tc.check, Message: tc.message}
			if got != want {
				t.Errorf("failure %+v, want %+v", got, want)
			}
		})
	}
}

// SIM-300: storing a nil error stores nothing, and a run with nothing stored passes.
func TestSIM300NilStoresNothing(t *testing.T) {
	out, code := runScenario(t, "store-nothing")
	if code != 0 {
		t.Errorf("child exited %d, want 0\n%s", code, out)
	}
}

// SIM-300: an unknown check name is a programming error in sim itself.
func TestSIM300UnknownCheckPanics(t *testing.T) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, func(s *sim.Sim) {
		defer func() {
			if got, want := recover(), "lockstep: sim: unknown check lockstep/nope"; got != want {
				t.Errorf("panic %v, want %q", got, want)
			}
		}()
		s.Store("lockstep/nope", errors.New("x"))
	})
}
