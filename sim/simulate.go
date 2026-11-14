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
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// Options configures Simulate and SimulateSeeds.
type Options struct {
	// Options configures faultline.Run. Mode must be faultline.ModeEvent, the zero value.
	faultline.Options

	// System holds options applied, in order, to the System of every lockstep node at every
	// boot. lockstep.WithNodeName has no effect: a node system's name is its kernel node's name.
	// lockstep.WithLogger and lockstep.WithMetrics have no effect either: a node system logs to
	// its kernel node's log and has metrics off.
	System []lockstep.Option

	// Budget sets count-based admission budgets for every lockstep node. The zero value sets
	// none. See Budget.
	Budget Budget

	// Remote holds the remoting protocol settings of every lockstep node. Messages between nodes
	// travel as encoded remoting frames, so each type sent, asked or replied with across nodes
	// must be registered in Registry and supported by Codec. The network fields (Listen,
	// Advertise, Peers, Resolve, TLS) are ignored, zero fields get remote's defaults, and the
	// zero value uses codec.Default() with JSON. Simulate validates Remote, then checks that Codec
	// supports every type of Registry, once per call, and fails the test before any seed if
	// either fails.
	Remote remote.Config
}

// Simulate runs body once for every run of every seed, as faultline.Run does, with a fresh Sim
// built on that run's World. body adds lockstep nodes, registers handlers, faults and checks,
// and may drive the run with s.World().RunFor, Send and Ask.
//
// Simulate reads LOCKSTEP_CHECK_DETERMINISM once per call, before any seed runs:
// LOCKSTEP_CHECK_DETERMINISM=1 runs every seed twice and compares the traces.
func Simulate(t *testing.T, opts Options, body func(s *Sim)) {
	t.Helper()
	fo, set := setup(t, opts)
	r := &runner{body: body, system: slices.Clone(opts.System), budget: opts.Budget, remote: set}
	defer r.end()
	faultline.Run(t, fo, r.run)
}

// SimulateSeeds is Simulate for a fixed list of seeds, as faultline.RunSeeds is for Run. Use it
// to keep a failing seed as a regression test.
func SimulateSeeds(t *testing.T, opts Options, body func(s *Sim), seeds ...uint64) {
	t.Helper()
	fo, set := setup(t, opts)
	r := &runner{body: body, system: slices.Clone(opts.System), budget: opts.Budget, remote: set}
	defer r.end()
	faultline.RunSeeds(t, fo, r.run, seeds...)
}

// setup validates opts (SIM-020), then applies the environment (SIM-030), and returns the
// faultline options to run with and the remoting settings of every node. It fails t, running no
// seed, if either step fails.
func setup(t *testing.T, opts Options) (faultline.Options, remoting.Settings) {
	t.Helper()
	fo, set, err := validate(opts)
	if err == nil {
		fo, err = applyEnv(fo)
	}
	if err != nil {
		t.Fatalf("%v", err)
	}
	return fo, set
}

// validate checks opts as SIM-020 says, then Options.Remote (remoteSettings), and returns a copy
// of the faultline options and the remoting settings.
func validate(opts Options) (faultline.Options, remoting.Settings, error) {
	if opts.Mode != faultline.ModeEvent {
		return faultline.Options{}, remoting.Settings{}, errors.New("lockstep: sim: Options.Mode must be faultline.ModeEvent")
	}
	for i, o := range opts.System {
		if o == nil {
			return faultline.Options{}, remoting.Settings{}, fmt.Errorf("lockstep: sim: Options.System[%d] is nil", i)
		}
	}
	if err := validateBudget(opts.Budget); err != nil {
		return faultline.Options{}, remoting.Settings{}, err
	}
	set, err := remoteSettings(opts.Remote)
	if err != nil {
		return faultline.Options{}, remoting.Settings{}, err
	}
	return opts.Options, set, nil
}

// remoteSettings returns the protocol settings of c, Options.Remote (REM-300): it zeroes the
// network fields, which simulation ignores, applies the defaults and checks the rest with
// remote.Config.Validate, then checks that the codec supports every registered type (REM-030).
// Its errors start with "lockstep: sim: Options.Remote: ".
func remoteSettings(c remote.Config) (remoting.Settings, error) {
	c.Listen, c.Advertise, c.Peers, c.Resolve, c.TLS = "", "", nil, nil, nil
	if err := c.Validate(); err != nil {
		return remoting.Settings{}, fmt.Errorf("lockstep: sim: Options.Remote: %w", err)
	}
	if err := c.Registry.Check(c.Codec); err != nil {
		return remoting.Settings{}, fmt.Errorf("lockstep: sim: Options.Remote: %w", err)
	}
	return remoting.Settings{
		Registry:      c.Registry,
		Codec:         c.Codec,
		MaxFrame:      c.MaxFrame,
		OutboundQueue: c.OutboundQueue,
		Heartbeat:     c.Heartbeat,
		IdleTimeout:   c.IdleTimeout,
		DialTimeout:   c.DialTimeout,
		RedialMin:     c.RedialMin,
		RedialMax:     c.RedialMax,
	}, nil
}

// runner builds a Sim for every run of one Simulate or SimulateSeeds call.
type runner struct {
	body   func(s *Sim)
	system []lockstep.Option // Options.System
	budget Budget            // Options.Budget
	remote remoting.Settings // Options.Remote, validated
	cur    *Sim              // the Sim of the latest run
}

// run is the function passed to faultline: it ends the previous run's Sim (SIM-040), builds a
// new one on w, registers the named checks (SIM-300) and the crash hook (SIM-110), and calls
// body with it. The Sim also ends when the seed subtest's cleanups run: registered after body
// returns, this cleanup runs before the ones body registered.
func (r *runner) run(w *faultline.World) {
	r.end()
	s := &Sim{w: w, system: r.system, budget: r.budget, remote: r.remote}
	r.cur = s
	s.registerChecks()
	w.Sim.OnCrash(s.crashed)
	r.body(s)
	w.T().Cleanup(s.end)
}

// end ends the Sim of the latest run, if any.
func (r *runner) end() {
	if r.cur != nil {
		r.cur.end()
	}
}

// end ends the run of s (SIM-040) and takes every system of the run out of the table of
// remoting.CoreOf, which would otherwise keep the run's world alive for the rest of the test
// binary: the cores of the live systems and the stopped markers that the crashes and shutdowns
// left (REM-040). Calls after the first do nothing more.
func (s *Sim) end() {
	s.ended = true
	for _, sys := range s.registered {
		remoting.Unregister(sys)
	}
	s.registered = nil
}
