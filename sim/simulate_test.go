// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-SIM-04, SIM-010, SIM-100: SimulateSeeds runs exactly the listed seeds as faultline.RunSeeds
// subtests, and World returns each run's World.
func TestATSIM04SimulateSeeds(t *testing.T) {
	var names []string
	var seeds []uint64
	runs := 1
	sim.SimulateSeeds(t, sim.Options{}, func(s *sim.Sim) {
		if s.World().Options().CheckDeterminism {
			runs = 2 // every seed runs twice in the same subtest
		}
		names = append(names, s.World().T().Name())
		seeds = append(seeds, s.World().Seed())
	}, 0x1, 0x2)

	var wantNames []string
	var wantSeeds []uint64
	for _, seed := range []uint64{1, 2} {
		for range runs {
			wantNames = append(wantNames, fmt.Sprintf("%s/seed=0x%016x", t.Name(), seed))
			wantSeeds = append(wantSeeds, seed)
		}
	}
	if !slices.Equal(names, wantNames) {
		t.Errorf("subtests %q, want %q", names, wantNames)
	}
	if !slices.Equal(seeds, wantSeeds) {
		t.Errorf("World().Seed() %v, want %v", seeds, wantSeeds)
	}
}

// SIM-010: Simulate runs body once for every run of every seed, each with its own Sim and World.
func TestSIM010SimulateRunsEverySeed(t *testing.T) {
	var sims []*sim.Sim
	var worlds []*faultline.World
	want := 3
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 3}}, func(s *sim.Sim) {
		if s.World().Options().CheckDeterminism {
			want = 6 // every seed runs twice
		}
		sims = append(sims, s)
		worlds = append(worlds, s.World())
	})
	if len(sims) != want {
		t.Fatalf("body ran %d times, want %d", len(sims), want)
	}
	for i := 1; i < len(sims); i++ {
		if sims[i] == sims[i-1] || worlds[i] == worlds[i-1] {
			t.Errorf("run %d reused the Sim or World of run %d", i, i-1)
		}
	}
}

// expectEnded calls fn and fails t unless it panics with SIM-040's message.
func expectEnded(t testing.TB, what string, fn func()) {
	t.Helper()
	const want = "lockstep: sim: Sim used after its run ended"
	defer func() {
		t.Helper()
		if got := recover(); got != want {
			t.Errorf("%s: panic %v, want %q", what, got, want)
		}
	}()
	fn()
}

// SIM-040: a Sim kept from one run panics when used in a later run, in a cleanup of the seed
// subtest or after SimulateSeeds returns, and works in kernel callbacks of its own run after body
// returned.
func TestSIM040UsedAfterRun(t *testing.T) {
	var saved *sim.Sim
	var later, cleanups int
	sim.SimulateSeeds(t, sim.Options{}, func(s *sim.Sim) {
		w := s.World()
		w.T().Cleanup(func() {
			cleanups++
			expectEnded(w.T(), "Sim in a cleanup that body registered", func() { s.World() })
		})
		w.Sim.After(time.Millisecond, "test/later", func() {
			if s.World() != w {
				w.T().Error("World() in a callback of the run returned another World")
			}
			later++
		})
		if saved == nil {
			saved = s
			return
		}
		expectEnded(w.T(), "Sim of an earlier run", func() { saved.World() })
	}, 0x1, 0x2)
	if later == 0 {
		t.Error("the callback scheduled by body never ran")
	}
	if cleanups == 0 {
		t.Error("the cleanup registered by body never ran")
	}
	expectEnded(t, "Sim after SimulateSeeds returned", func() { saved.World() })
}

// The scenarios of TestSIM020InvalidOptions: each prints bodyRan from body.
const bodyRan = "BODY RAN"

func printBody(*sim.Sim) { fmt.Println(bodyRan) }

var (
	_ = scenario("mode-goroutine", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Options: faultline.Options{Mode: faultline.ModeGoroutine}}, printBody)
	})
	_ = scenario("mode-goroutine-seeds", func(t *testing.T) {
		sim.SimulateSeeds(t, sim.Options{Options: faultline.Options{Mode: faultline.ModeGoroutine}}, printBody, 1)
	})
	_ = scenario("nil-option", func(t *testing.T) {
		sim.Simulate(t, sim.Options{System: []lockstep.Option{nil}}, printBody)
	})
	_ = scenario("nil-option-seeds", func(t *testing.T) {
		opts := []lockstep.Option{lockstep.WithNodeName("x"), lockstep.WithNodeName("y"), nil}
		sim.SimulateSeeds(t, sim.Options{System: opts}, printBody, 1)
	})
	_ = scenario("mode-before-option", func(t *testing.T) {
		opts := sim.Options{Options: faultline.Options{Mode: faultline.ModeGoroutine}, System: []lockstep.Option{nil}}
		sim.Simulate(t, opts, printBody)
	})
)

// SIM-020: invalid Options fail the test with SIM-020's message before any seed runs.
func TestSIM020InvalidOptions(t *testing.T) {
	const (
		mode   = "lockstep: sim: Options.Mode must be faultline.ModeEvent"
		option = "lockstep: sim: Options.System[%d] is nil"
	)
	for _, tc := range []struct {
		scenario string
		want     string
	}{
		{"mode-goroutine", mode},
		{"mode-goroutine-seeds", mode},
		{"nil-option", fmt.Sprintf(option, 0)},
		{"nil-option-seeds", fmt.Sprintf(option, 2)},
		{"mode-before-option", mode},
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

// SIM-020: lockstep.WithNodeName in Options.System is allowed.
func TestSIM020WithNodeNameAllowed(t *testing.T) {
	ran := false
	opts := sim.Options{Options: faultline.Options{Seeds: 1}, System: []lockstep.Option{lockstep.WithNodeName("x")}}
	sim.Simulate(t, opts, func(*sim.Sim) { ran = true })
	if !ran {
		t.Error("body never ran")
	}
}
