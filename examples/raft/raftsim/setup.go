// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/check/history"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/raft/client"
	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
	"github.com/hmdsefi/lockstep/sim"
)

// drainMargin is how long before the end of the run Setup stops the workload clients, so that
// every server can apply the last acknowledged writes before the replicas-agree check (RFT-530).
const drainMargin = time.Second

// clientPath is the path of the client actor on every client node.
const clientPath = "/user/client"

// registrar registers invariants and final checks. *faultline.World is one; tests record the
// names through another.
type registrar interface {
	Invariant(name string, check func() error)
	Final(name string, check func() error)
}

// hooks are setup's test seams: reg receives the invariants and finals, and wrap, if not nil,
// returns the Observer the servers get in place of the oracle.
type hooks struct {
	reg  registrar
	wrap func(raft.Observer) raft.Observer
}

// Setup builds c inside s (RFT-390) and returns its oracle. Invalid c fails the test.
//
// Setup stops the workload clients one second before the end of the run, so the servers can
// catch up before the final checks; the operation a client has in flight then stays pending in
// the history. Calling Setup twice on one Sim panics.
func Setup(s *sim.Sim, c Cluster) *Oracle {
	return setup(s, c, hooks{reg: s.World()})
}

// Test runs sim.Simulate(t, opts, func(s) { Setup(s, c) }).
func Test(t *testing.T, opts sim.Options, c Cluster) {
	t.Helper()
	sim.Simulate(t, opts, func(s *sim.Sim) { Setup(s, c) })
}

// setup is Setup with its test seams.
func setup(s *sim.Sim, c Cluster, h hooks) *Oracle {
	w := s.World()
	if w.Sim.Lookup("n1") != nil {
		panic("raftsim: Setup called twice")
	}
	if err := c.validate(); err != nil {
		w.T().Fatalf("%v", err)
		return nil
	}

	// 1. The oracle and the stable store, which outlives every server incarnation.
	servers := c.serverNames()
	o := newOracle(servers, c.Clients, w.Sim.Now, func(r kernel.Record) { w.Sim.Emit(r) })
	var obs raft.Observer = o
	if h.wrap != nil {
		obs = h.wrap(o)
	}
	store := raft.NewMemStore()

	// 2. The servers, in order, so that server i is node i (see newOracle). The setup runs at
	// every boot.
	nodes := make([]*kernel.Node, len(servers))
	for i, name := range servers {
		cfg := c.raftConfig(name, servers, obs)
		nodes[i] = s.Node(name, func(sys *lockstep.System) {
			o.boot(name)
			if _, err := raft.Spawn(sys, cfg); err != nil {
				o.fail(CheckHarness, "%s: spawn raft: %v", name, err)
			}
		})
	}

	// 3. The effect handlers.
	handleEffects(s, o, store, c.PersistMin, c.PersistMax)

	// 4. The leader role and the fault preset. Progress counts from the recovery start, or from
	// time 0 without faults.
	w.Role(leaderRole, func() []kernel.NodeID {
		leaders := o.latestLeaders()
		var ids []kernel.NodeID
		for i, n := range nodes {
			if n.State() == kernel.NodeUp && slices.Contains(leaders, servers[i]) {
				ids = append(ids, n.ID())
			}
		}
		return ids
	})
	var recovery kernel.Time
	if p := preset(c.Faults); p != nil {
		w.Plan(p)
		recovery = w.RecoveryStart()
	}

	// 5. The workload clients c1..cK and the probe, which starts at the recovery start. Faults
	// never crash a client, so each spawns once; a restarted client would invoke under its name
	// again while its last operation is pending, which history.Recorder rejects with a panic.
	rec := historyRecorder{h: w.History}
	for k := 1; k <= c.Clients+1; k++ {
		cfg := c.clientConfig(k, servers, rec, time.Duration(recovery))
		s.Client(cfg.Name, func(sys *lockstep.System) {
			factory, err := client.New(cfg)
			if err == nil {
				_, err = lockstep.Spawn(sys, "client", factory)
			}
			if err != nil {
				o.fail(CheckHarness, "%s: spawn client: %v", cfg.Name, err)
			}
		})
	}
	// The drain: the workload clients stop drainMargin before the end (not in a shorter run).
	if at := w.End() - kernel.Time(drainMargin); at > w.Sim.Now() {
		w.Sim.At(at, "raftsim.drain", func() {
			for k := 1; k <= c.Clients; k++ {
				name := c.clientName(k)
				if err := s.StopActor(lockstep.PIDAt[client.Msg](name, clientPath).Address()); err != nil {
					o.fail(CheckHarness, "%s: stop client: %v", name, err)
				}
			}
		})
	}

	// 6. The invariants, then the final checks.
	for _, name := range []string{
		CheckHarness, CheckElectionSafety, CheckLogMatching, CheckLeaderCompleteness,
		CheckStateMachineSafety, CheckDurableState,
	} {
		h.reg.Invariant(name, func() error { return o.Err(name) })
	}
	h.reg.Final(CheckLinearizable, func() error { return o.linearizable(w.History.Ops()) })
	h.reg.Final(CheckProgress, func() error { return o.progress(w.History.Ops(), recovery, c.ProgressWithin) })
	h.reg.Final(CheckReplicasAgree, func() error {
		var up []string // in NodeID order
		for i, n := range nodes {
			if n.State() == kernel.NodeUp {
				up = append(up, servers[i])
			}
		}
		return o.replicasAgree(w.History.Ops(), up)
	})

	// 7. The planted bugs, for the trace.
	if c.Bugs != 0 {
		bugs := c.Bugs.String()
		w.Sim.Emit(kernel.Record{
			Kind:  "raft.bugs",
			Text:  "bugs enabled: " + bugs,
			Attrs: []kernel.Attr{{Key: "bugs", Value: bugs}},
		})
	}
	return o
}

// handleEffects installs the raft.persist and raft.load handlers over store (RFT-400). A persist
// changes the store when its handler runs, so a crash before its result arrives keeps it.
func handleEffects(s *sim.Sim, o *Oracle, store *raft.MemStore, persistMin, persistMax time.Duration) {
	s.HandleEffect(raft.Persist{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		p, ok := c.Effect().(raft.Persist)
		if !ok {
			return wrongEffect(o, c)
		}
		err := store.Save(p.Node, p.HardState, p.From, p.Entries)
		return sim.EffectResult{Value: struct{}{}, Err: err, Latency: kernel.Uniform(c.Rand(), persistMin, persistMax)}
	})
	s.HandleEffect(raft.Load{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		l, ok := c.Effect().(raft.Load)
		if !ok {
			return wrongEffect(o, c)
		}
		d, err := store.Load(l.Node) // a copy: the server never shares the store's log
		return sim.EffectResult{Value: d, Err: err, Latency: kernel.Uniform(c.Rand(), 0, persistMin)}
	})
}

// wrongEffect records an effect value of the wrong type as a harness violation (RFT-490) and
// fails the effect.
func wrongEffect(o *Oracle, c sim.EffectCall) sim.EffectResult {
	o.fail(CheckHarness, "%s: %s got %T", c.Node, c.Name, c.Effect())
	return sim.EffectResult{Err: fmt.Errorf("raftsim: %s got %T", c.Name, c.Effect())}
}

// newStateMachine returns a new KV store for each server incarnation.
func newStateMachine() raft.StateMachine { return kv.New() }

// historyRecorder records client operations into a faultline history.
type historyRecorder struct {
	h *history.Recorder
}

// Invoke implements client.Recorder.
func (r historyRecorder) Invoke(process, f string, input any) int64 {
	return r.h.Invoke(process, f, input)
}

// Complete implements client.Recorder: OK, Fail and Info map to history's statuses of the same
// names.
func (r historyRecorder) Complete(id int64, outcome client.Outcome, output any) {
	status := history.Info
	switch outcome {
	case client.OK:
		status = history.OK
	case client.Fail:
		status = history.Fail
	}
	r.h.Complete(id, status, output)
}
