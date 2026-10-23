// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/lockstep/sim"
)

// oneSeed runs body in one seed.
func oneSeed(t *testing.T, body func(s *sim.Sim)) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, body)
}

// runToEnd advances the run to its end, so that no event is pending.
func runToEnd(s *sim.Sim) {
	s.World().RunFor(s.World().End().Sub(s.World().Sim.Now()))
}

// storeInEvent returns a scenario whose one event stores a failure in each of checks, in order,
// each with the message "<check> failure".
func storeInEvent(checks ...string) func(t *testing.T) {
	return func(t *testing.T) {
		oneSeed(t, func(s *sim.Sim) {
			s.World().Sim.After(time.Millisecond, "test/store", func() {
				for _, check := range checks {
					s.Store(check, errors.New(check+" failure"))
				}
			})
			s.World().RunFor(10 * time.Millisecond)
		})
	}
}

// storeAfterEnd returns a scenario whose body stores a failure in each of checks, in order, after
// the last event, each with the message "<check> failure".
func storeAfterEnd(checks ...string) func(t *testing.T) {
	return func(t *testing.T) {
		oneSeed(t, func(s *sim.Sim) {
			runToEnd(s)
			for _, check := range checks {
				s.Store(check, errors.New(check+" failure"))
			}
		})
	}
}

var (
	// One check stored from an event (invariant) and from body after the last event (final).
	_ = scenario("event-misuse", storeInEvent(sim.CheckMisuse))
	_ = scenario("event-no-effect-handler", storeInEvent(sim.CheckNoEffectHandler))
	_ = scenario("event-leak", storeInEvent(sim.CheckLeak))
	_ = scenario("event-deadlock", storeInEvent(sim.CheckDeadlock))
	_ = scenario("final-misuse", storeAfterEnd(sim.CheckMisuse))
	_ = scenario("final-no-effect-handler", storeAfterEnd(sim.CheckNoEffectHandler))
	_ = scenario("final-leak", storeAfterEnd(sim.CheckLeak))
	_ = scenario("final-deadlock", storeAfterEnd(sim.CheckDeadlock))

	// Several checks failing at once: the first in SIM-300's order reports, whichever was stored first.
	_ = scenario("event-order", storeInEvent(sim.CheckLeak, sim.CheckMisuse))
	_ = scenario("event-later-slots", storeInEvent(sim.CheckDeadlock, sim.CheckLeak, sim.CheckNoEffectHandler))
	_ = scenario("final-order", storeAfterEnd(sim.CheckDeadlock, sim.CheckLeak))

	// A slot keeps the first failure stored in it, also against a nil.
	_ = scenario("event-second-store", func(t *testing.T) {
		oneSeed(t, func(s *sim.Sim) {
			s.World().Sim.After(time.Millisecond, "test/store", func() {
				s.Store(sim.CheckMisuse, errors.New("first misuse"))
				s.Store(sim.CheckMisuse, errors.New("second misuse"))
				s.Store(sim.CheckMisuse, nil)
			})
			s.World().RunFor(10 * time.Millisecond)
		})
	})
	// Storing nil, alone, fails nothing.
	_ = scenario("store-nil", func(t *testing.T) {
		oneSeed(t, func(s *sim.Sim) {
			s.Store(sim.CheckLeak, nil)
			s.World().RunFor(10 * time.Millisecond)
		})
	})
)

// wantFailure is the failure of a check reported with kind.
func wantFailure(kind, check string) failure {
	return failure{Kind: kind, Check: check, Signature: kind + ":" + check, Message: check + " failure"}
}

// namedChecks lists the checks in SIM-300's order.
var namedChecks = []struct{ name, check string }{
	{"misuse", sim.CheckMisuse},
	{"no-effect-handler", sim.CheckNoEffectHandler},
	{"leak", sim.CheckLeak},
	{"deadlock", sim.CheckDeadlock},
}

// withAlso adds the line faultline appends to a final failure when another final check failed too.
func withAlso(f failure, check string) failure {
	f.Message += "\nalso failed: " + strconv.Quote(check)
	return f
}

// SIM-300, SIM-310: each named check reports a failure stored by an event as an invariant failure.
func TestSIM300InvariantChecks(t *testing.T) {
	for _, tc := range namedChecks {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := failingSeed(t, "event-"+tc.name), wantFailure("invariant", tc.check); got != want {
				t.Errorf("failure %+v, want %+v", got, want)
			}
		})
	}
}

// SIM-300, SIM-310: each named check reports a failure stored by body after the last event as a
// final failure.
func TestSIM300FinalCheck(t *testing.T) {
	for _, tc := range namedChecks {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := failingSeed(t, "final-"+tc.name), wantFailure("final", tc.check); got != want {
				t.Errorf("failure %+v, want %+v", got, want)
			}
		})
	}
}

// SIM-300: of the failures stored together, the check registered first reports, whichever was
// stored first, as an invariant or as a final check.
func TestSIM300CheckOrder(t *testing.T) {
	for _, tc := range []struct {
		scenario string
		want     failure
	}{
		{"event-order", wantFailure("invariant", sim.CheckMisuse)},
		{"event-later-slots", wantFailure("invariant", sim.CheckNoEffectHandler)},
		{"final-order", withAlso(wantFailure("final", sim.CheckLeak), sim.CheckDeadlock)},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			if got := failingSeed(t, tc.scenario); got != tc.want {
				t.Errorf("failure %+v, want %+v", got, tc.want)
			}
		})
	}
}

// SIM-300: a slot keeps the first failure stored in it, and storing nil changes nothing.
func TestSIM300FirstFailureKept(t *testing.T) {
	got := failingSeed(t, "event-second-store")
	want := failure{
		Kind:      "invariant",
		Check:     "lockstep/misuse",
		Signature: "invariant:lockstep/misuse",
		Message:   "first misuse",
	}
	if got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
	if out, code := runScenario(t, "store-nil"); code != 0 {
		t.Errorf("storing nil failed the seed (exit %d)\n%s", code, out)
	}
}

// SIM-300: an unknown check name is a programming error in sim itself.
func TestSIM300UnknownCheckPanics(t *testing.T) {
	oneSeed(t, func(s *sim.Sim) {
		defer func() {
			if got, want := recover(), "lockstep: sim: unknown check lockstep/nope"; got != want {
				t.Errorf("panic %v, want %q", got, want)
			}
		}()
		s.Store("lockstep/nope", errors.New("x"))
	})
}
