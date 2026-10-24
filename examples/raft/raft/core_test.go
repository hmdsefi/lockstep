// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"
)

// fakeEnv records the timer calls a core makes.
type fakeEnv struct {
	arms    []uint64 // the gen of every armElection call, in order
	cancels int
}

func (e *fakeEnv) armElection(gen uint64) { e.arms = append(e.arms, gen) }
func (e *fakeEnv) cancelElection()        { e.cancels++ }

// lastGen returns the gen of the latest armElection call.
func (e *fakeEnv) lastGen(t *testing.T) uint64 {
	t.Helper()
	if len(e.arms) == 0 {
		t.Fatal("the election timer was never armed")
	}
	return e.arms[len(e.arms)-1]
}

// recObserver records every Observer call as one line.
type recObserver struct{ calls []string }

func (o *recObserver) add(format string, args ...any) {
	o.calls = append(o.calls, fmt.Sprintf(format, args...))
}

func (o *recObserver) Recovered(node string, hs HardState, log LogView) {
	o.add("Recovered %s %+v last=%d", node, hs, log.LastIndex())
}

func (o *recObserver) PersistStarted(node string, hs HardState, last, chain uint64) {
	o.add("PersistStarted %s %+v last=%d chain=%d", node, hs, last, chain)
}

func (o *recObserver) Durable(node string, hs HardState, last, chain uint64) {
	o.add("Durable %s %+v last=%d chain=%d", node, hs, last, chain)
}

func (o *recObserver) LogWritten(node string, from uint64, log LogView) {
	o.add("LogWritten %s from=%d last=%d", node, from, log.LastIndex())
}

func (o *recObserver) BecameLeader(node string, term uint64, log LogView) {
	o.add("BecameLeader %s term=%d last=%d", node, term, log.LastIndex())
}

func (o *recObserver) Committed(node string, term, from, to uint64, _ LogView) {
	o.add("Committed %s term=%d from=%d to=%d", node, term, from, to)
}

func (o *recObserver) Applied(node string, index uint64, e Entry, stateHash uint64) {
	o.add("Applied %s index=%d %+v hash=%d", node, index, e, stateHash)
}

func (o *recObserver) Replied(node string, client uint32, seq uint64, r Result) {
	o.add("Replied %s client=%d seq=%d %+v", node, client, seq, r)
}

// take returns the calls recorded since the last take.
func (o *recObserver) take() []string {
	calls := o.calls
	o.calls = nil
	return calls
}

// nopMachine is a StateMachine that keeps no state.
type nopMachine struct{}

func (nopMachine) Apply(uint64, string) string { return "" }
func (nopMachine) Query(string) string         { return "" }
func (nopMachine) Hash() uint64                { return 0 }

// harness is a core driven by a test, with its fake env and recording observer.
type harness struct {
	c   *core
	env *fakeEnv
	obs *recObserver
}

var (
	peers3 = []string{"n1", "n2", "n3"}
	peers5 = []string{"n1", "n2", "n3", "n4", "n5"}
)

// newHarness builds a core for id with the given durable state. The core is not started.
func newHarness(id string, peers []string, d Durable, bugs Bug) *harness {
	env := &fakeEnv{}
	obs := &recObserver{}
	cfg := Config{
		ID:              id,
		Peers:           peers,
		ElectionMin:     DefaultElectionMin,
		ElectionMax:     DefaultElectionMax,
		Heartbeat:       DefaultHeartbeat,
		MaxBatch:        DefaultMaxBatch,
		MaxPending:      DefaultMaxPending,
		NewStateMachine: func() StateMachine { return nopMachine{} },
		Observer:        obs,
		Bugs:            bugs,
	}
	return &harness{c: newCore(cfg, d, cfg.NewStateMachine(), env), env: env, obs: obs}
}

// terms returns a log with one entry per term.
func terms(ts ...uint64) []Entry {
	log := make([]Entry, len(ts))
	for i, t := range ts {
		log[i] = Entry{Term: t}
	}
	return log
}

// sends returns the outbox's peers and messages, without tags, in queue order.
func (h *harness) sends() []queued {
	var out []queued
	for _, q := range h.c.outbox {
		out = append(out, queued{peer: q.peer, msg: q.msg})
	}
	return out
}

func TestATRFT13ElectionTimer(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404: a fixed seeded stream is the point
	minD := DefaultElectionMin
	maxD := minD + 2*time.Nanosecond
	var counts [3]int
	for range 1000 {
		d := electionDelay(r, minD, maxD)
		if d < minD || d > maxD {
			t.Fatalf("electionDelay = %v, want within [%v, %v]", d, minD, maxD)
		}
		counts[d-minD]++
	}
	if counts[0] == 0 || counts[2] == 0 {
		t.Errorf("draws per offset 0, 1, 2 ns = %v, want both ends to occur", counts)
	}

	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 3}, Log: terms(1, 3)}, 0)
	h.c.start()
	h.c.step(RequestVote{Term: 3, Candidate: "n2", LastIndex: 2, LastTerm: 3}) // grant: arms again
	if len(h.env.arms) != 2 {
		t.Fatalf("arms = %v, want 2 after start and a granted vote", h.env.arms)
	}
	stale := h.env.arms[0]
	beforeOutbox := slices.Clone(h.c.outbox)
	h.obs.take()

	h.c.onElectionTimeout(stale)
	if h.c.term != 3 || h.c.role != follower || h.c.vote != "n2" {
		t.Errorf("after a stale timeout: term %d, role %v, vote %q; want 3, follower, n2",
			h.c.term, h.c.role, h.c.vote)
	}
	if !reflect.DeepEqual(h.c.outbox, beforeOutbox) {
		t.Errorf("a stale timeout changed the outbox: %+v, want %+v", h.c.outbox, beforeOutbox)
	}
	if len(h.env.arms) != 2 {
		t.Errorf("a stale timeout armed the timer: arms %v", h.env.arms)
	}
	if calls := h.obs.take(); len(calls) != 0 {
		t.Errorf("a stale timeout called the observer: %q", calls)
	}

	h.c.onElectionTimeout(h.env.lastGen(t))
	if h.c.term != 4 || h.c.role != candidate {
		t.Errorf("after the latest timeout: term %d, role %v; want 4, candidate", h.c.term, h.c.role)
	}
}

func TestHigherTermStepsDown(t *testing.T) {
	msgs := []Msg{
		RequestVote{Term: 5, Candidate: "n2"},
		VoteReply{Term: 5, From: "n2", Granted: true},
		AppendEntries{Term: 5, Leader: "n2"},
		AppendReply{Term: 5, From: "n2"},
	}
	roles := []struct {
		role   role
		vote   string
		leader string
	}{
		{follower, "n3", "n3"},
		{candidate, "n1", ""},
		{leader, "n1", "n1"},
	}
	for _, m := range msgs {
		for _, rl := range roles {
			t.Run(fmt.Sprintf("%T/%v", m, rl.role), func(t *testing.T) {
				h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 3}, Log: terms(1, 3)}, 0)
				h.c.role, h.c.vote, h.c.leader = rl.role, rl.vote, rl.leader
				h.c.step(m)
				if h.c.term != 5 || h.c.vote != "" || h.c.role != follower {
					t.Errorf("term %d, vote %q, role %v; want 5, \"\", follower", h.c.term, h.c.vote, h.c.role)
				}
				_, isAppend := m.(AppendEntries)
				if h.c.leader != "" && (!isAppend || h.c.leader != "n2") {
					t.Errorf("leader %q, want the old leader %q forgotten", h.c.leader, rl.leader)
				}
			})
		}
	}

	t.Run("lower term", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 5, Vote: "n3"}, Log: terms(1, 3)}, 0)
		h.c.role = candidate
		h.c.step(RequestVote{Term: 4, Candidate: "n2", LastIndex: 9, LastTerm: 9})
		h.c.step(AppendEntries{Term: 4, Leader: "n3", Commit: 2})
		h.c.step(VoteReply{Term: 4, From: "n2", Granted: true})
		h.c.step(AppendReply{Term: 4, From: "n3", Success: true, Match: 2})
		if h.c.term != 5 || h.c.vote != "n3" || h.c.role != candidate {
			t.Errorf("term %d, vote %q, role %v; want 5, n3, candidate", h.c.term, h.c.vote, h.c.role)
		}
		got := h.sends()
		if len(got) != 2 {
			t.Fatalf("queued %+v, want two rejections", got)
		}
		if got[0].peer != 1 || got[0].msg != (VoteReply{Term: 5, From: "n1", Granted: false}) {
			t.Errorf("RequestVote answered with %+v, want VoteReply{5 n1 false} to n2", got[0])
		}
		ar, ok := got[1].msg.(AppendReply)
		if got[1].peer != 2 || !ok || ar.Term != 5 || ar.From != "n1" || ar.Success {
			t.Errorf("AppendEntries answered with %+v, want a failed AppendReply of term 5 to n3", got[1])
		}
	})

	t.Run("sender outside Peers", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 3, Vote: "n2"}, Log: terms(1, 3)}, 0)
		h.c.role = candidate
		for _, m := range []Msg{
			RequestVote{Term: 9, Candidate: "n9", LastIndex: 9, LastTerm: 9},
			VoteReply{Term: 9, From: "n9", Granted: true},
			AppendEntries{Term: 9, Leader: "n9"},
			AppendReply{Term: 9, From: "n9"},
		} {
			h.c.step(m)
		}
		if h.c.term != 3 || h.c.vote != "n2" || h.c.role != candidate || len(h.c.outbox) != 0 ||
			len(h.env.arms) != 0 || len(h.obs.calls) != 0 {
			t.Errorf("term %d, vote %q, role %v, outbox %+v, arms %v, observer %q; want nothing changed",
				h.c.term, h.c.vote, h.c.role, h.c.outbox, h.env.arms, h.obs.calls)
		}
	})
}

func TestPersistPipeline(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 4}, Log: terms(1, 4)}, 0)
	h.c.start()
	h.obs.take()
	chain2 := h.c.log.Chain(2)

	// A vote grant makes the state dirty; its VoteReply waits for the persist.
	h.c.step(RequestVote{Term: 4, Candidate: "n2", LastIndex: 2, LastTerm: 4})
	if !h.c.dirty {
		t.Fatal("a granted vote did not make the state dirty")
	}
	if len(h.c.outbox) != 1 || h.c.outbox[0].tag != 1 {
		t.Fatalf("outbox %+v, want one VoteReply tagged issued+1 = 1", h.c.outbox)
	}
	if got := h.c.takeSends(); len(got) != 0 {
		t.Fatalf("takeSends before the persist = %+v, want none", got)
	}
	p, ok := h.c.endTurn()
	want := Persist{Node: "n1", HardState: HardState{Term: 4, Vote: "n2"}}
	if !ok || !reflect.DeepEqual(p, want) {
		t.Fatalf("endTurn = %+v, %v; want %+v, true", p, ok, want)
	}
	wantStarted := fmt.Sprintf("PersistStarted n1 {Term:4 Vote:n2} last=2 chain=%d", chain2)
	if calls := h.obs.take(); !slices.Equal(calls, []string{wantStarted}) {
		t.Errorf("observer calls %q, want [%q]", calls, wantStarted)
	}
	if _, ok := h.c.endTurn(); ok {
		t.Error("endTurn issued a second persist while one is in flight")
	}

	// Changes while the persist is in flight wait for the next one; their messages for it too.
	h.c.onElectionTimeout(h.env.lastGen(t)) // term 5, vote n1, RequestVotes tagged 2
	if _, ok := h.c.endTurn(); ok {
		t.Error("endTurn issued a persist while one is in flight")
	}
	h.obs.take()
	h.c.persisted()
	wantDurable := fmt.Sprintf("Durable n1 {Term:4 Vote:n2} last=2 chain=%d", chain2)
	if calls := h.obs.take(); !slices.Equal(calls, []string{wantDurable}) {
		t.Errorf("observer calls %q, want [%q]", calls, wantDurable)
	}
	sent := h.c.takeSends()
	if len(sent) != 1 || sent[0].msg != (VoteReply{Term: 4, From: "n1", Granted: true}) || sent[0].peer != 1 {
		t.Fatalf("takeSends after the persist = %+v, want only the VoteReply to n2", sent)
	}
	p, ok = h.c.endTurn()
	want = Persist{Node: "n1", HardState: HardState{Term: 5, Vote: "n1"}}
	if !ok || !reflect.DeepEqual(p, want) {
		t.Fatalf("endTurn after persisted = %+v, %v; want %+v, true", p, ok, want)
	}

	// Winning appends the no-op: the next persist starts at its index with a fresh slice.
	h.c.step(VoteReply{Term: 5, From: "n2", Granted: true})
	if h.c.role != leader {
		t.Fatalf("role %v, want leader", h.c.role)
	}
	h.c.persisted()
	if got := h.c.takeSends(); len(got) != 2 {
		t.Errorf("takeSends after the second persist = %+v, want the two RequestVotes", got)
	}
	h.obs.take()
	p, ok = h.c.endTurn()
	if !ok || p.HardState != (HardState{Term: 5, Vote: "n1"}) || p.From != 3 ||
		!slices.Equal(p.Entries, []Entry{{Term: 5}}) {
		t.Fatalf("endTurn after a log write = %+v, %v; want From 3 and the no-op", p, ok)
	}
	if &p.Entries[0] == &h.c.log.entries[2] {
		t.Error("Persist.Entries shares memory with the log")
	}
	wantStarted = fmt.Sprintf("PersistStarted n1 {Term:5 Vote:n1} last=3 chain=%d", h.c.log.Chain(3))
	if calls := h.obs.take(); !slices.Equal(calls, []string{wantStarted}) {
		t.Errorf("observer calls %q, want [%q]", calls, wantStarted)
	}
	h.c.persisted()
	if h.c.durableLast != 3 {
		t.Errorf("durable last index %d, want 3", h.c.durableLast)
	}
	h.c.takeSends()

	// A message queued while the state is clean and nothing is in flight leaves at once.
	h.c.step(RequestVote{Term: 4, Candidate: "n3", LastIndex: 9, LastTerm: 9})
	sent = h.c.takeSends()
	if len(sent) != 1 || sent[0].msg != (VoteReply{Term: 5, From: "n1"}) || sent[0].peer != 2 {
		t.Errorf("takeSends of a clean rejection = %+v, want the VoteReply to n3 at once", sent)
	}
	if _, ok := h.c.endTurn(); ok {
		t.Error("endTurn issued a persist for a clean state")
	}
}

func TestBugVoteNotDurable(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 4}, Log: terms(1, 4)}, BugVoteNotDurable)
	h.c.step(RequestVote{Term: 4, Candidate: "n2", LastIndex: 2, LastTerm: 4})
	p, ok := h.c.endTurn()
	if !ok || p.HardState != (HardState{Term: 4}) {
		t.Fatalf("endTurn = %+v, %v; want HardState {4 \"\"}", p, ok)
	}
	h.c.persisted()
	chain := h.c.log.Chain(2)
	want := []string{
		fmt.Sprintf("PersistStarted n1 {Term:4 Vote:n2} last=2 chain=%d", chain),
		fmt.Sprintf("Durable n1 {Term:4 Vote:n2} last=2 chain=%d", chain),
	}
	if calls := h.obs.take(); !slices.Equal(calls, want) {
		t.Errorf("observer calls %q, want %q", calls, want)
	}
}
