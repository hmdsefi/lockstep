// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/check/history"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/lockstep/examples/raft/raft"
	"github.com/hmdsefi/lockstep/sim"
)

// recordingRegistrar records the names of the checks registered through it, in order, and runs
// none of them.
type recordingRegistrar struct {
	names []string
}

func (r *recordingRegistrar) Invariant(name string, _ func() error) {
	r.names = append(r.names, "invariant "+name)
}

func (r *recordingRegistrar) Final(name string, _ func() error) {
	r.names = append(r.names, "final "+name)
}

// recordingObserver passes every call to the wrapped Observer and records the Recovered and
// BecameLeader calls.
type recordingObserver struct {
	raft.Observer
	recovered []string     // servers, one entry per Recovered call
	leaders   []leaderCall // BecameLeader calls, in order
}

// leaderCall is one BecameLeader call.
type leaderCall struct {
	node       string
	term, last uint64
}

func (r *recordingObserver) Recovered(node string, hs raft.HardState, log raft.LogView) {
	r.recovered = append(r.recovered, node)
	r.Observer.Recovered(node, hs, log)
}

func (r *recordingObserver) BecameLeader(node string, term uint64, log raft.LogView) {
	r.leaders = append(r.leaders, leaderCall{node: node, term: term, last: log.LastIndex()})
	r.Observer.BecameLeader(node, term, log)
}

// recordingHooks returns hooks that register on s's World and wrap the oracle in rec.
func recordingHooks(s *sim.Sim, rec *recordingObserver) hooks {
	return hooks{reg: s.World(), wrap: func(o raft.Observer) raft.Observer {
		rec.Observer = o
		return rec
	}}
}

// fullTraceOptions returns sim options for one seed of d with a full trace.
func fullTraceOptions(d time.Duration) sim.Options {
	opts := Options(1)
	opts.Duration = d
	opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
	return opts
}

// runToEnd runs the simulation to its end and returns its records. It returns false when the
// run keeps no records: faultline traces in full only the first run of a seed, so a second run
// for a determinism check has none.
func runToEnd(s *sim.Sim) ([]kernel.Record, bool) {
	w := s.World()
	w.RunFor(time.Duration(w.End() - w.Sim.Now()))
	recs := w.Sim.Records()
	return recs, recs != nil
}

// TestATRFT42InvariantOrder checks RFT-500: Setup registers the six invariants, then the three
// finals, in order.
func TestATRFT42InvariantOrder(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 1, Duration: 100 * time.Millisecond, BaseSeed: 1}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		reg := &recordingRegistrar{}
		setup(s, DefaultCluster(), hooks{reg: reg})
		want := []string{
			"invariant harness", "invariant election safety", "invariant log matching",
			"invariant leader completeness", "invariant state machine safety", "invariant durable state",
			"final linearizable", "final progress", "final replicas agree",
		}
		if !slices.Equal(reg.names, want) {
			t.Errorf("registered %q,\nwant %q", reg.names, want)
		}
	})
}

// TestATRFT31DefaultClusterNoFaults checks RFT-390 and RFT-430: the default cluster without faults
// adds its nodes in order, stops its workload clients one second before the end, emits no
// raft.bugs record and passes every check. It logs the wall time per seed.
func TestATRFT31DefaultClusterNoFaults(t *testing.T) {
	c := DefaultCluster()
	c.Faults = FaultsNone
	opts := Options(1)
	opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
	start := time.Now()
	sim.Simulate(t, opts, func(s *sim.Sim) {
		Setup(s, c)
		var names []string
		for _, n := range s.World().Sim.Nodes() {
			names = append(names, n.Name())
		}
		if want := []string{"n1", "n2", "n3", "c1", "c2", "c3", "probe"}; !slices.Equal(names, want) {
			t.Errorf("nodes %q, want %q", names, want)
		}
		recs, ok := runToEnd(s)

		// The workload clients stop one second before the end; the probe stopped long before.
		drain := s.World().End() - kernel.Time(time.Second)
		for _, op := range s.World().History.Ops() {
			if op.Call > drain {
				t.Errorf("op %d of %s invoked at %s, after the clients stopped at %s", op.ID, op.Process, op.Call, drain)
			}
		}
		if !ok {
			return
		}
		for _, r := range recs {
			if r.Kind == "raft.bugs" {
				t.Errorf("raft.bugs record %+v without bugs", r)
			}
		}
	})
	t.Logf("%d seed of %v in %v", opts.Seeds, opts.Duration, time.Since(start).Round(time.Millisecond))
}

// TestATRFT05FirstLeader checks RFT-050, RFT-080 and RFT-180: without faults, one leader is
// elected within 1 s, every server loads its state once, and the client's operations invoked
// after the election succeed.
func TestATRFT05FirstLeader(t *testing.T) {
	c := DefaultCluster()
	c.Faults = FaultsNone
	c.Clients = 1
	sim.Simulate(t, fullTraceOptions(2*time.Second), func(s *sim.Sim) {
		rec := &recordingObserver{}
		setup(s, c, recordingHooks(s, rec))
		recs, ok := runToEnd(s)
		if !ok {
			return
		}

		var first kernel.Time // when the first leader was elected
		leaders := 0
		for _, r := range recs {
			if r.Kind != "raft.leader" {
				continue
			}
			if leaders == 0 {
				first = r.At
			}
			if r.At <= kernel.Time(time.Second) {
				leaders++
			}
		}
		if leaders != 1 {
			t.Errorf("%d raft.leader records by 1s, want 1", leaders)
		}
		if want := []string{"n1", "n2", "n3"}; !slices.Equal(slices.Sorted(slices.Values(rec.recovered)), want) {
			t.Errorf("Recovered calls %q, want one per server %q", rec.recovered, want)
		}

		// Before the first leader, every server answers NotLeader, so an operation invoked then
		// may run out of retries and fail; every operation invoked after it succeeds. The run's
		// end stops the client, so its last operation may still be pending.
		ops := history.ForProcess(s.World().History.Ops(), "c1")
		if n := len(ops); n > 0 && ops[n-1].Status == history.Pending {
			ops = ops[:n-1]
		}
		okAfter := 0
		for _, op := range ops {
			switch {
			case op.Call > first && op.Status != history.OK:
				t.Errorf("c1 op %d (%s at %s) ended %v after the first leader (at %s), want ok",
					op.ID, op.F, op.Call, op.Status, first)
			case op.Status != history.OK && op.Status != history.Fail:
				t.Errorf("c1 op %d (%s at %s) ended %v, want ok or fail", op.ID, op.F, op.Call, op.Status)
			case op.Call > first:
				okAfter++
			}
		}
		if okAfter == 0 {
			t.Errorf("c1 completed no operation after the first leader (at %s)", first)
		}
	})
}

// TestATRFT50Records checks §8: a cluster with bugs emits one global raft.bugs record, and every
// raft.leader record names the server's node and carries term and last.
func TestATRFT50Records(t *testing.T) {
	c := DefaultCluster()
	c.Faults = FaultsNone
	c.Bugs = raft.BugVoteIgnoresLog | raft.BugStaleRead
	sim.Simulate(t, fullTraceOptions(2*time.Second), func(s *sim.Sim) {
		rec := &recordingObserver{}
		setup(s, c, recordingHooks(s, rec))
		recs, ok := runToEnd(s)
		if !ok {
			return
		}

		var bugs, leaders []kernel.Record
		for _, r := range recs {
			switch r.Kind {
			case "raft.bugs":
				bugs = append(bugs, r)
			case "raft.leader":
				leaders = append(leaders, r)
			}
		}
		wantAttrs := []kernel.Attr{{Key: "bugs", Value: "vote-ignores-log,stale-read"}}
		if len(bugs) != 1 || bugs[0].Node != 0 || bugs[0].Text != "bugs enabled: vote-ignores-log,stale-read" ||
			!slices.Equal(bugs[0].Attrs, wantAttrs) {
			t.Errorf("raft.bugs records %+v, want one global record with text %q and attrs %v",
				bugs, "bugs enabled: vote-ignores-log,stale-read", wantAttrs)
		}

		if len(leaders) == 0 || len(leaders) != len(rec.leaders) {
			t.Fatalf("%d raft.leader records for %d BecameLeader calls, want as many and at least 1",
				len(leaders), len(rec.leaders))
		}
		for i, r := range leaders {
			call := rec.leaders[i]
			term, last := strconv.FormatUint(call.term, 10), strconv.FormatUint(call.last, 10)
			want := kernel.Record{
				Node:  s.World().Sim.Lookup(call.node).ID(),
				Kind:  "raft.leader",
				Text:  "leader of term " + term,
				Attrs: []kernel.Attr{{Key: "term", Value: term}, {Key: "last", Value: last}},
			}
			if r.Node != want.Node || r.Text != want.Text || !slices.Equal(r.Attrs, want.Attrs) {
				t.Errorf("raft.leader record %d = node %d, %q, %v; want node %d (%s), %q, %v",
					i, r.Node, r.Text, r.Attrs, want.Node, call.node, want.Text, want.Attrs)
			}
		}
	})
}

// TestWrongEffectIsHarness checks RFT-400 and RFT-490: an effect value of the wrong type is a
// harness violation and fails the effect.
func TestWrongEffectIsHarness(t *testing.T) {
	r := newRig()
	res := wrongEffect(r.o, sim.EffectCall{Name: "raft.persist", Node: "n2"})
	wantOnly(t, r.o, CheckHarness, "n2: raft.persist got <nil>")
	if res.Err == nil {
		t.Error("wrongEffect returned no error; want the effect to fail")
	}
}

// TestPersistErrorIsHarness checks RFT-400 and RFT-490: a persist the store rejects (here one whose
// From leaves a hole after the stored log) is a harness violation and fails the effect.
func TestPersistErrorIsHarness(t *testing.T) {
	r := newRig()
	store := raft.NewMemStore()
	ok := raft.Persist{Node: "n2", HardState: raft.HardState{Term: 1}, From: 1, Entries: []raft.Entry{{Term: 1}}}
	if err := save(r.o, store, ok); err != nil {
		t.Fatalf("save from 1: %v", err)
	}
	wantErr(t, r.o, CheckHarness, "")

	hole := raft.Persist{Node: "n2", HardState: raft.HardState{Term: 1}, From: 3, Entries: []raft.Entry{{Term: 1}}}
	if err := save(r.o, store, hole); err == nil {
		t.Error("save from 3 returned no error; want the effect to fail")
	}
	wantOnly(t, r.o, CheckHarness, "n2: raft.persist: raft: Save from 3 leaves a hole after log index 1")
}
