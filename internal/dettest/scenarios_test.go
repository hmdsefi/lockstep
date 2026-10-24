// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"context"
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

// toyRows are the scenarios of DET-260, in its order, then the rows other specs add under DET-265.
var toyRows = []toyRow{
	{name: "pingpong", baseSeed: 0x1001, body: pingpongBody},
	{name: "fanin", baseSeed: 0x1002, body: faninBody},
	{name: "ticker", baseSeed: 0x1003, body: tickerBody},
	{name: "ask", baseSeed: 0x1004, body: askBody, after: askAfter},
	{name: "beacon-crash", baseSeed: 0x1005, body: beaconCrashBody},
	{name: "pingpong-faults", baseSeed: 0x1006, body: pingpongFaultsBody}, // AT-SIM-28
	{name: "act-lifecycle", baseSeed: 0x1007, body: actLifecycleBody},     // AT-ACT-33
	{name: "msg-asks", baseSeed: 0x1008, body: msgAsksBody},               // AT-MSG-33
}

// toyScenarios returns toyRows as golden scenarios, in table order.
func toyScenarios() []scenario {
	list := make([]scenario, 0, len(toyRows))
	for _, row := range toyRows {
		list = append(list, row.scenario())
	}
	return list
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

// pingpongFaultsBody is the ping-pong across three nodes of AT-SIM-28: two pingers play the one
// ponger while random crash and partition faults hit the nodes. Each pinger stops at 50 rounds, so
// neither probe passes 50; crashes and partitions usually cut the game short. The seed passing
// means no lockstep check failed under the faults.
func pingpongFaultsBody(s *sim.Sim, _ *toyObs) {
	p1, p3 := &toys.PingPongProbe{}, &toys.PingPongProbe{}
	var pinger1, pinger3 lockstep.PID[toys.PingerMsg]
	var ponger lockstep.PID[toys.PongerMsg]
	s.Node("n1", func(sys *lockstep.System) { pinger1 = must(toys.SpawnPinger(sys, p1)) })
	s.Node("n2", func(sys *lockstep.System) { ponger = must(toys.SpawnPonger(sys)) })
	s.Node("n3", func(sys *lockstep.System) { pinger3 = must(toys.SpawnPinger(sys, p3)) })
	s.World().Plan(&fault.Random{Rules: []fault.Rule{
		{Kind: fault.KindCrash, Every: 500 * time.Millisecond, MaxFor: 100 * time.Millisecond},
		{Kind: fault.KindPartition, Every: 400 * time.Millisecond, MaxFor: 150 * time.Millisecond},
	}})
	boot(s)
	send(s, "n1", pinger1, toys.PingerMsg(toys.StartPing{Peer: ponger, Rounds: 50}))
	send(s, "n3", pinger3, toys.PingerMsg(toys.StartPing{Peer: ponger, Rounds: 50}))
	s.World().Final("pingpong-faults", func() error {
		if p1.Rounds > 50 || p3.Rounds > 50 {
			return fmt.Errorf("pingers received pongs %d and %d, want at most 50", p1.Rounds, p3.Rounds)
		}
		return nil
	})
}

// actLifecycleBody is the scenario of AT-ACT-33: a group of 10 actors on each of two nodes, spawned
// with explicit and generated names, two watches per group, one stop and the shutdown of one node's
// system. n1's group stops its first worker, so one watch fires (the group of n2 is stopping with
// its system when its workers stop, so none of its watches does); n2's system shuts down with its
// 10 actors, and n1's other 9 stay alive.
func actLifecycleBody(s *sim.Sim, _ *toyObs) {
	p := &toys.GroupProbe{}
	var group lockstep.PID[toys.GroupMsg]
	s.Node("n1", func(sys *lockstep.System) { group = must(toys.SpawnGroup(sys, 10, p)) })
	s.Node("n2", func(sys *lockstep.System) { must(toys.SpawnGroup(sys, 10, p)) })
	boot(s)
	s.World().RunFor(100 * time.Millisecond)
	send(s, "n1", group, toys.GroupMsg(toys.StopFirst{}))
	s.World().RunFor(50 * time.Millisecond)
	check(s.System("n2").Shutdown(context.Background()))
	s.World().Final("act-lifecycle", func() error {
		if p.Started != 20 || p.Stopped != 11 || p.Terminated != 1 || p.Reports != 18 {
			return fmt.Errorf("%d actors started, %d stopped, %d terminated signals and %d reports, want 20, 11, 1 and 18",
				p.Started, p.Stopped, p.Terminated, p.Reports)
		}
		return nil
	})
}

// msgAsksBody is the scenario of AT-MSG-33: a requester on n1 asks 50 questions across n2 and n3
// with timeouts, one of them forwarded from n2 to n3, while random partitions cut the links.
func msgAsksBody(s *sim.Sim, _ *toyObs) {
	p := &toys.RequestsProbe{}
	var requester lockstep.PID[toys.RequesterMsg]
	var a, b lockstep.PID[toys.ResponderMsg]
	s.Node("n1", func(sys *lockstep.System) { requester = must(toys.SpawnRequester(sys, p)) })
	s.Node("n2", func(sys *lockstep.System) { a = must(toys.SpawnResponder(sys)) })
	s.Node("n3", func(sys *lockstep.System) { b = must(toys.SpawnResponder(sys)) })
	s.World().Plan(&fault.Random{Rules: []fault.Rule{
		{Kind: fault.KindPartition, Every: 150 * time.Millisecond, MaxFor: 60 * time.Millisecond},
	}})
	boot(s)
	send(s, "n1", requester, toys.RequesterMsg(toys.StartRound{A: a, B: b, Count: 50}))
	s.World().Final("msg-asks", func() error {
		if p.Answered+p.TimedOut+p.Other != 50 || p.Wrong != 0 {
			return fmt.Errorf("%d answered, %d timed out, %d failed otherwise and %d wrong, want 50 asks resolved and none wrong",
				p.Answered, p.TimedOut, p.Other, p.Wrong)
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
