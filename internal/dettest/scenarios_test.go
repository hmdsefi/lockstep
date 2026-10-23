// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/toys"
	"github.com/hmdsefi/lockstep/sim"
)

// toyObs collects what the bodies of one scenario record in every run of every seed, for the
// checks across seeds that a World.Final check of a single seed cannot make.
type toyObs struct {
	ask []*toys.AskProbe // the probe of every run of the ask scenario
}

// toyRow is one scenario of DET-260: the toy systems' scenario table. body adds the nodes, drives
// the run and registers the scenario's final check with s.World().Final(name, ...). after, when
// set, checks what body recorded in obs once all seeds have run; it runs only on the row's default
// seeds, so not when a variable of DET-270 step 1 picks other seeds. scenario turns a row into the
// golden suite's scenario, which runs the same body with its own observer.
type toyRow struct {
	name     string
	baseSeed uint64
	body     func(s *sim.Sim, obs *toyObs)
	after    func(t *testing.T, obs *toyObs)
}

// options returns the options every scenario runs with (DET-260): three seeds from the row's
// BaseSeed, a 2 s run and the defaults otherwise.
func (r toyRow) options() sim.Options {
	return sim.Options{Options: faultline.Options{Seeds: goldenSeeds, BaseSeed: r.baseSeed, Duration: 2 * time.Second}}
}

// run simulates the row's body on every seed, recording into obs.
func (r toyRow) run(t *testing.T, obs *toyObs) {
	t.Helper()
	sim.Simulate(t, r.options(), func(s *sim.Sim) { r.body(s, obs) })
}

// scenario returns the row as a golden scenario.
func (r toyRow) scenario() scenario {
	return scenario{name: r.name, run: func(t *testing.T) { r.run(t, &toyObs{}) }}
}

// toyRows are the scenarios of DET-260, in its order.
var toyRows = []toyRow{
	{name: "pingpong", baseSeed: 0x1001, body: pingpongBody},
	{name: "fanin", baseSeed: 0x1002, body: faninBody},
	{name: "ticker", baseSeed: 0x1003, body: tickerBody},
	{name: "ask", baseSeed: 0x1004, body: askBody, after: askAfter},
	{name: "beacon-crash", baseSeed: 0x1005, body: beaconCrashBody},
}

// boot runs the simulation until every node's setup has run (DET-260).
func boot(s *sim.Sim) { s.World().RunFor(time.Millisecond) }

// must returns v, and panics with err if there is one: setup closures run inside kernel
// callbacks, where a panic fails the seed.
func must[T any](v T, err error) T {
	check(err)
	return v
}

// check panics with err if there is one, as must does.
func check(err error) {
	if err != nil {
		panic(err)
	}
}

// send sends msg from node's system to to, and fails the seed if that does not work.
func send[M any](s *sim.Sim, node string, to lockstep.PID[M], msg M) {
	if err := sim.Send(s, node, to, msg); err != nil {
		s.World().T().Fatalf("send %T from %s: %v", msg, node, err)
	}
}

func pingpongBody(s *sim.Sim, _ *toyObs) {
	p := &toys.PingPongProbe{}
	var pinger lockstep.PID[toys.PingerMsg]
	var ponger lockstep.PID[toys.PongerMsg]
	s.Node("n1", func(sys *lockstep.System) { pinger = must(toys.SpawnPinger(sys, p)) })
	s.Node("n2", func(sys *lockstep.System) { ponger = must(toys.SpawnPonger(sys)) })
	boot(s)
	send(s, "n1", pinger, toys.PingerMsg(toys.StartPing{Peer: ponger, Rounds: 50}))
	s.World().Final("pingpong", func() error {
		if p.Rounds != 50 {
			return fmt.Errorf("pinger received pong %d, want 50", p.Rounds)
		}
		return nil
	})
}

func faninBody(s *sim.Sim, _ *toyObs) {
	p := &toys.FanInProbe{}
	s.Node("n1", func(sys *lockstep.System) { check(toys.SpawnFanIn(sys, 3, 10, p)) })
	s.World().Final("fanin", func() error {
		if p.Items != 30 || p.Reordered != 0 {
			return fmt.Errorf("collector saw %d items and %d reordered, want 30 and 0", p.Items, p.Reordered)
		}
		return nil
	})
}

func tickerBody(s *sim.Sim, _ *toyObs) {
	p := &toys.TickerProbe{}
	s.Node("n1", func(sys *lockstep.System) { check(toys.SpawnTicker(sys, 25, p)) })
	s.World().Final("ticker", func() error {
		if !p.Stopped || p.Starts < 2 {
			return fmt.Errorf("ticker stopped %t after %d starts, want stopped after at least 2", p.Stopped, p.Starts)
		}
		return nil
	})
}

func askBody(s *sim.Sim, obs *toyObs) {
	p := &toys.AskProbe{}
	obs.ask = append(obs.ask, p)
	var server lockstep.PID[toys.ServerMsg]
	var client lockstep.PID[toys.ClientMsg]
	s.Node("s", func(sys *lockstep.System) { server = must(toys.SpawnServer(sys)) })
	s.Client("c", func(sys *lockstep.System) { client = must(toys.SpawnClient(sys, p)) })
	boot(s)
	send(s, "c", client, toys.ClientMsg(toys.StartAsks{Server: server, Count: 20}))
	s.World().Final("ask", func() error {
		if p.Answered+p.TimedOut != 20 || p.Wrong != 0 {
			return fmt.Errorf("%d answered, %d timed out and %d wrong, want 20 answered or timed out and none wrong",
				p.Answered, p.TimedOut, p.Wrong)
		}
		return nil
	})
}

// askAfter is the check of AT-DET-17 across the seeds of ask: the server's latency is sometimes
// below the client's timeout and sometimes above it.
func askAfter(t *testing.T, obs *toyObs) {
	t.Helper()
	var answered, timedOut int
	for _, p := range obs.ask {
		answered += p.Answered
		timedOut += p.TimedOut
	}
	if len(obs.ask) == 0 || answered == 0 || timedOut == 0 {
		t.Errorf("over %d runs %d asks were answered and %d timed out, want some of each", len(obs.ask), answered, timedOut)
	}
}

func beaconCrashBody(s *sim.Sim, _ *toyObs) {
	p := &toys.BeaconProbe{}
	var beacon lockstep.PID[toys.BeaconMsg]
	var listener lockstep.PID[toys.ListenerMsg]
	s.Node("n1", func(sys *lockstep.System) { beacon = must(toys.SpawnBeacon(sys, p)) })
	s.Node("n2", func(sys *lockstep.System) { listener = must(toys.SpawnListener(sys, p)) })
	boot(s)
	send(s, "n1", beacon, toys.BeaconMsg(toys.StartBeacon{Peer: listener, Beats: 40}))
	s.World().Faults.Load(fault.Schedule{Version: 1, Events: []fault.Event{
		{At: kernel.Time(105 * time.Millisecond), Kind: fault.KindCrash, Node: "n2"},
		{At: kernel.Time(155 * time.Millisecond), Kind: fault.KindRestart, Node: "n2"},
	}})
	s.World().Final("beacon-crash", func() error {
		// Beats to the crashed and then restarted listener are dead letters.
		if p.Sent != 40 || p.Heard >= 40 {
			return fmt.Errorf("beacon sent %d and listener heard %d, want 40 sent and fewer heard", p.Sent, p.Heard)
		}
		return nil
	})
}

// TestToys is AT-DET-17: every scenario of DET-260 passes its final check on its three seeds,
// also with LOCKSTEP_CHECK_DETERMINISM=1, which runs each seed twice. The checks across seeds run
// only on the default seeds: a seed override (DET-270 step 1) may run seeds that never show what
// they look for.
func TestToys(t *testing.T) {
	overridden := skipReason(os.Getenv) != ""
	for _, row := range toyRows {
		t.Run(row.name, func(t *testing.T) {
			var obs toyObs
			row.run(t, &obs)
			if row.after != nil && !overridden && !t.Failed() {
				row.after(t, &obs)
			}
		})
	}
}

// TestToyRowNames checks DET-265 for the toy rows: a scenario name is unique, has no spaces or
// slashes, and is the name of the golden scenario the row yields.
func TestToyRowNames(t *testing.T) {
	seen := map[string]bool{}
	for _, row := range toyRows {
		sc := row.scenario()
		if sc.name != row.name || sc.run == nil {
			t.Errorf("row %q yields scenario %q, or one without a body", row.name, sc.name)
		}
		if row.name == "" || strings.ContainsAny(row.name, " /") {
			t.Errorf("scenario name %q is empty or has a space or slash", row.name)
		}
		if seen[row.name] {
			t.Errorf("scenario name %q is not unique", row.name)
		}
		seen[row.name] = true
	}
}
