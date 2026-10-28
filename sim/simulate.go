// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/lockstep"
)

// Options configures Simulate and SimulateSeeds.
type Options struct {
	// Options configures faultline.Run. Mode must be faultline.ModeEvent, the zero value.
	faultline.Options

	// System holds options applied, in order, to the System of every lockstep node at every
	// boot. lockstep.WithNodeName has no effect: a node system's name is its kernel node's name.
	// lockstep.WithLogger has no effect either: a node system logs to its kernel node's log.
	System []lockstep.Option

	// Budget sets count-based admission budgets for every lockstep node. The zero value sets
	// none. See Budget.
	Budget Budget
}

// Simulate runs body once for every run of every seed, as faultline.Run does, with a fresh Sim
// built on that run's World. body adds lockstep nodes, registers handlers, faults and checks,
// and may drive the run with s.World().RunFor, Send and Ask.
//
// Simulate reads LOCKSTEP_CHECK_DETERMINISM once per call, before any seed runs:
// LOCKSTEP_CHECK_DETERMINISM=1 runs every seed twice and compares the traces.
func Simulate(t *testing.T, opts Options, body func(s *Sim)) {
	t.Helper()
	fo := setup(t, opts)
	r := &runner{body: body, system: slices.Clone(opts.System), budget: opts.Budget}
	defer r.end()
	faultline.Run(t, fo, r.run)
}

// SimulateSeeds is Simulate for a fixed list of seeds, as faultline.RunSeeds is for Run. Use it
// to keep a failing seed as a regression test.
func SimulateSeeds(t *testing.T, opts Options, body func(s *Sim), seeds ...uint64) {
	t.Helper()
	fo := setup(t, opts)
	r := &runner{body: body, system: slices.Clone(opts.System), budget: opts.Budget}
	defer r.end()
	faultline.RunSeeds(t, fo, r.run, seeds...)
}

// setup validates opts (SIM-020), then applies the environment (SIM-030), and returns the
// faultline options to run with. It fails t, running no seed, if either step fails.
func setup(t *testing.T, opts Options) faultline.Options {
	t.Helper()
	fo, err := validate(opts)
	if err == nil {
		fo, err = applyEnv(fo)
	}
	if err != nil {
		t.Fatalf("%v", err)
	}
	return fo
}

// validate checks opts as SIM-020 says and returns a copy of the faultline options.
func validate(opts Options) (faultline.Options, error) {
	if opts.Mode != faultline.ModeEvent {
		return faultline.Options{}, errors.New("lockstep: sim: Options.Mode must be faultline.ModeEvent")
	}
	for i, o := range opts.System {
		if o == nil {
			return faultline.Options{}, fmt.Errorf("lockstep: sim: Options.System[%d] is nil", i)
		}
	}
	if err := validateBudget(opts.Budget); err != nil {
		return faultline.Options{}, err
	}
	return opts.Options, nil
}

// runner builds a Sim for every run of one Simulate or SimulateSeeds call.
type runner struct {
	body   func(s *Sim)
	system []lockstep.Option // Options.System
	budget Budget            // Options.Budget
	cur    *Sim              // the Sim of the latest run
}

// run is the function passed to faultline: it ends the previous run's Sim (SIM-040), builds a
// new one on w, registers the named checks (SIM-300) and the crash hook (SIM-110), and calls
// body with it. The Sim also ends when the seed subtest's cleanups run: registered after body
// returns, this cleanup runs before the ones body registered.
func (r *runner) run(w *faultline.World) {
	r.end()
	s := &Sim{w: w, system: r.system, budget: r.budget}
	r.cur = s
	s.registerChecks()
	w.Sim.OnCrash(s.crashed)
	r.body(s)
	w.T().Cleanup(func() { s.ended = true })
}

// end ends the Sim of the latest run, if any.
func (r *runner) end() {
	if r.cur != nil {
		r.cur.ended = true
	}
}
