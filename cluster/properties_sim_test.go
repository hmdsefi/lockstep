// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// The property tests of CLU §5.9. Each seed draws the size of the cluster, from minNodes to
// maxNodes, and every node seeds all of them (CLU-830's precondition holds for every partition).
// The properties are faultline invariants: they read the views after every event.

const (
	minNodes = 3
	maxNodes = 7
)

// transientCut is the transient bound of AT-CLU-24's "transient" cuts, whatever the size of the
// cluster.
const transientCut = 4 * time.Second

// transientConfig returns simConfig(seeds...), validated, with SuspicionTimeout set so that the
// transient bound of a cluster of n nodes is transientCut.
func transientConfig(n int, seeds ...string) Config {
	cfg := simConfig(seeds...)
	cfg.SuspicionTimeout = time.Duration(2*n-3)*DefaultProbeInterval + transientCut
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return cfg
}

// propSeeds is the default number of seeds of each property test; FAULTLINE_SEEDS overrides it.
const propSeeds = 20

// propCluster draws the size of the cluster of a seed from the stream "workload/cluster/size" and
// returns its node names, n1 to nN.
func propCluster(w interface{ Rand(string) *rand.Rand }) []string {
	n := minNodes + w.Rand("cluster/size").IntN(maxNodes-minNodes+1)
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("n%d", i+1)
	}
	return names
}

// AT-CLU-14, CLU-800: after the cluster has converged, one to (N-1)/2 of its N nodes crash, at
// times drawn uniformly from the D(N) that follows the warm-up, so that a crash can fall at any
// point of the probe rounds and while an earlier crash is being detected. The invariant: once
// D(N) has passed since a crash, no live process that was there at the crash has the crashed
// process up. At the end every live view has each crashed process Dead.
func TestATCLU14Detection(t *testing.T) {
	const warm = 3 * time.Second
	worst := boundsOf(simConfig(), maxNodes)
	sim.Simulate(t, simOpts(propSeeds, warm+2*worst.detect+2*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		names := propCluster(w)
		cfg := simConfig(names...)
		b := boundsOf(cfg, len(names))
		c := newSimCluster(s)
		for _, n := range names {
			c.node(n, cfg)
		}
		tr := newTruth(c, names)
		w.Invariant("cluster/detection", func() error {
			tr.observe()
			now := w.Sim.Now()
			for _, d := range tr.procs {
				if d.ended == 0 || now.Sub(d.ended) <= b.detect {
					continue
				}
				for _, p := range tr.live() {
					if p.booted > d.ended {
						continue
					}
					if m, ok := tr.view(p).Member(d.node); ok && m.ID == d.id && m.State.Up() {
						return fmt.Errorf("%s still has %s:%d %s %s after its crash at %s, more than D(N) = %s", p.node, d.node, d.id, m.State, now.Sub(d.ended), d.ended, b.detect)
					}
				}
			}
			return nil
		})

		w.RunFor(warm)
		tr.observe()
		if err := tr.converged(); err != nil {
			tt.Fatalf("not converged after the warm-up of %s: %v", warm, err)
		}
		r := w.Rand("cluster/crash")
		k := 1 + r.IntN((len(names)-1)/2)
		victims := r.Perm(len(names))[:k]
		at := make([]time.Duration, k)
		for i := range at {
			at[i] = warm + time.Duration(r.Int64N(int64(b.detect)))
		}
		slices.Sort(at)
		var crashed []proc
		for i, v := range victims {
			if d := at[i] - w.Sim.Now().Sub(kernel.Time(0)); d > 0 {
				w.RunFor(d)
			}
			w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: names[v]})
			tr.observe()
			crashed = append(crashed, tr.procs[slices.IndexFunc(tr.procs, func(p proc) bool { return p.node == names[v] })])
		}
		w.RunFor(b.detect + time.Second)

		for _, p := range tr.live() {
			v := tr.view(p)
			for _, d := range crashed {
				if m, ok := v.Member(d.node); !ok || m.ID != d.id || m.State != Dead {
					tt.Errorf("%s's view has %s %v, %t at the end, want %s:%d Dead", p.node, d.node, m, ok, d.node, d.id)
				}
			}
		}
	})
}

// AT-CLU-15, CLU-810: N nodes join, all through n1 or each through all, and run for 30s without
// any fault. The invariant: no view of a live process ever has a member that is not Alive. No
// probe fails and no node refutes, and at the end the views agree.
func TestATCLU15Accuracy(t *testing.T) {
	sim.Simulate(t, simOpts(propSeeds, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		names := propCluster(w)
		seeds := names
		if w.Rand("cluster/seeds").IntN(2) == 0 {
			seeds = names[:1]
		}
		c := newSimCluster(s)
		for _, n := range names {
			c.node(n, simConfig(seeds...))
		}
		tr := newTruth(c, names)
		w.Invariant("cluster/accuracy", func() error {
			tr.observe()
			for _, p := range tr.live() {
				v := tr.view(p)
				for _, m := range v.Members {
					if m.State != Alive {
						return fmt.Errorf("%s's view %d has %v without any fault", p.node, v.Version, m)
					}
				}
			}
			return nil
		})
		w.RunFor(w.End().Sub(w.Sim.Now()))

		tr.observe()
		if err := tr.converged(); err != nil {
			tt.Error(err)
		}
		for _, n := range names {
			for _, kind := range []string{"probe.fail", "refute"} {
				if recs := c.recs(n, kind); len(recs) != 0 {
					tt.Errorf("%s recorded %s without faults: %s", n, kind, describe(recs))
				}
			}
		}
	})
}

// AT-CLU-24, CLU-820, CLU-830. "faults": random partitions, isolations and crashes, each crashed
// node restarted with a new ID after a while, until the recovery window, where every fault ends.
// The invariant: from R(N) after the start of the recovery window, the view of every live process
// has exactly the live processes up, with their IDs. "transient": no crashes; from time to time a
// random node is isolated, or the nodes are split in two, for at least ProbeInterval and less than
// the transient bound SuspicionTimeout - (2N-3)*ProbeInterval (see bounds), each such cut healed
// for at least SuspicionTimeout + ProbeInterval before the next. SuspicionTimeout is set per seed so
// that the bound is transientCut. The invariants: no view ever has a member down, and from R(N)
// after the last heal the views agree.
func TestATCLU24Convergence(t *testing.T) {
	const faulty = 40 * time.Second // the fault phase of "faults"; "transient" cuts until then too
	worst := boundsOf(simConfig(), maxNodes)
	opts := simOpts(propSeeds, faulty+worst.converge+5*time.Second)

	t.Run("faults", func(t *testing.T) {
		sim.Simulate(t, opts, func(s *sim.Sim) {
			w := s.World()
			names := propCluster(w)
			cfg := simConfig(names...)
			b := boundsOf(cfg, len(names))
			c := newSimCluster(s)
			for _, n := range names {
				c.node(n, cfg)
			}
			w.Plan(&fault.Random{Quiet: w.End().Sub(kernel.Time(0).Add(faulty)), Rules: []fault.Rule{
				{Kind: fault.KindPartition, Every: 5 * time.Second, MinFor: 500 * time.Millisecond, MaxFor: 6 * time.Second},
				{Kind: fault.KindIsolate, Every: 9 * time.Second, MinFor: 500 * time.Millisecond, MaxFor: 6 * time.Second},
				{Kind: fault.KindCrash, Every: 6 * time.Second, MinFor: 200 * time.Millisecond, MaxFor: 5 * time.Second},
			}})
			tr := newTruth(c, names)
			settled := w.RecoveryStart().Add(b.converge)
			w.Invariant("cluster/convergence", func() error {
				tr.observe()
				if w.Sim.Now() <= settled {
					return nil
				}
				return tr.converged()
			})
		})
	})

	t.Run("transient", func(t *testing.T) {
		worst := boundsOf(transientConfig(maxNodes), maxNodes)
		sim.Simulate(t, simOpts(propSeeds, faulty+worst.converge+5*time.Second), func(s *sim.Sim) {
			w := s.World()
			names := propCluster(w)
			cfg := transientConfig(len(names), names...)
			b := boundsOf(cfg, len(names))
			if b.transient != transientCut {
				panic("transientConfig does not give the transient bound transientCut")
			}
			c := newSimCluster(s)
			for _, n := range names {
				c.node(n, cfg)
			}
			r := w.Rand("cluster/cuts")
			var events []fault.Event
			var lastHeal time.Duration
			for at := 3*time.Second + time.Duration(r.Int64N(int64(cfg.ProbeInterval))); at < faulty; {
				d := cfg.ProbeInterval + time.Duration(r.Int64N(int64(b.transient-cfg.ProbeInterval)))
				e := fault.Event{At: kernel.Time(0).Add(at), Kind: fault.KindIsolate, Node: names[r.IntN(len(names))]}
				if r.IntN(2) == 0 {
					perm := r.Perm(len(names))
					cut := 1 + r.IntN(len(names)-1)
					var g1, g2 []string
					for i, j := range perm {
						if i < cut {
							g1 = append(g1, names[j])
						} else {
							g2 = append(g2, names[j])
						}
					}
					slices.Sort(g1)
					slices.Sort(g2)
					e = fault.Event{At: e.At, Kind: fault.KindPartition, Groups: [][]string{g1, g2}}
				}
				lastHeal = at + d
				events = append(events, e, fault.Event{At: kernel.Time(0).Add(lastHeal), Kind: fault.KindHeal})
				at = lastHeal + cfg.SuspicionTimeout + cfg.ProbeInterval + time.Duration(r.Int64N(int64(3*time.Second)))
			}
			w.Plan(fault.Script(events...))
			tr := newTruth(c, names)
			settled := kernel.Time(0).Add(lastHeal + b.converge)
			w.Invariant("cluster/transient", func() error {
				tr.observe()
				for _, p := range tr.live() {
					v := tr.view(p)
					for _, m := range v.Members {
						if !m.State.Up() {
							return fmt.Errorf("%s's view %d has %v after cuts shorter than %s", p.node, v.Version, m, b.transient)
						}
					}
				}
				if w.Sim.Now() <= settled {
					return nil
				}
				return tr.converged()
			})
		})
	})
}

// AT-CLU-28, CLU-840: every boot of every node subscribes /user/sub at once; random partitions,
// isolations and crashes (each restarted with a new ID after a while) run until the recovery
// window, and meanwhile the membership actor of a random live node fails from time to time (it
// restarts and keeps its table and subscribers), a late subscriber joins on a random live node,
// and once a random node leaves. The invariant: per subscriber and node name, the signals
// alternate NodeUp, NodeDown, ..., starting with NodeUp, each NodeDown of the ID of the NodeUp
// before it. At the end, every subscriber of a live node that has not left has, by its signals,
// exactly the members up that its node's view has up, other than the node itself.
func TestATCLU28Alternation(t *testing.T) {
	const faulty = 40 * time.Second
	worst := boundsOf(simConfig(), maxNodes)
	sim.Simulate(t, simOpts(propSeeds, faulty+worst.converge+5*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		names := propCluster(w)
		cfg := simConfig(names...)
		c := newSimCluster(s)
		var logs []*subLog
		var owners []*lockstep.System // the System of each log's subscriber
		add := func(sys *lockstep.System, l *subLog) {
			logs = append(logs, l)
			owners = append(owners, sys)
		}
		for _, n := range names {
			c.n[n] = s.Node(n, func(sys *lockstep.System) {
				m, err := Join(sys, cfg)
				if err != nil {
					panic(err)
				}
				c.ms[n] = m
				add(sys, c.subscribe(sys, m, "sub"))
			})
		}
		w.Plan(&fault.Random{Quiet: w.End().Sub(kernel.Time(0).Add(faulty)), Rules: []fault.Rule{
			{Kind: fault.KindPartition, Every: 5 * time.Second, MinFor: 500 * time.Millisecond, MaxFor: 6 * time.Second},
			{Kind: fault.KindIsolate, Every: 9 * time.Second, MinFor: 500 * time.Millisecond, MaxFor: 6 * time.Second},
			{Kind: fault.KindCrash, Every: 6 * time.Second, MinFor: 200 * time.Millisecond, MaxFor: 5 * time.Second},
		}})
		alt := &alternation{logs: &logs}
		w.Invariant("cluster/alternation", alt.check)

		r := w.Rand("cluster/actions")
		leaveAt := time.Duration(r.Int64N(int64(faulty)))
		late := 0
		for w.Sim.Now() < kernel.Time(0).Add(faulty) {
			w.RunFor(250 * time.Millisecond)
			name := names[r.IntN(len(names))]
			sys := s.System(name)
			if sys == nil {
				continue
			}
			switch x := r.IntN(40); {
			case leaveAt >= 0 && w.Sim.Now() >= kernel.Time(0).Add(leaveAt):
				leaveAt = -1
				if err := c.ms[name].Leave(); err != nil && !errors.Is(err, ErrLeft) {
					tt.Fatal(err)
				}
			case x == 0:
				if err := s.InjectFail(lockstep.PIDAt[memberMsg](name, actorPath).Address(), errors.New("injected")); err != nil && !errors.Is(err, lockstep.ErrNotFound) {
					tt.Fatal(err)
				}
			case x == 1 && !c.ms[name].left.Load():
				late++
				add(sys, c.subscribe(sys, c.ms[name], fmt.Sprintf("late%d", late)))
			}
		}
		w.RunFor(w.End().Sub(w.Sim.Now()))

		if err := alt.check(); err != nil {
			tt.Fatal(err)
		}
		downs := 0
		for _, l := range logs {
			for _, e := range l.log {
				if _, ok := e.sig.(lockstep.NodeDown); ok {
					downs++
				}
			}
		}
		if downs == 0 {
			tt.Error("no subscriber got a NodeDown, want the faults to bring members down")
		}
		for i, l := range logs {
			name := owners[i].Node()
			m := c.ms[name]
			if s.System(name) != owners[i] || m.left.Load() {
				continue
			}
			var want []string
			for _, u := range upIDs(m.View()) {
				if u != name+":"+uitoa(m.View().Self.ID) {
					want = append(want, u)
				}
			}
			if got := alt.up[i]; !slices.Equal(got, want) {
				tt.Errorf("%s has up by its signals %v, want %v as in its node's view %v", l.name, got, want, m.View().Members)
			}
		}
	})
}
