// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"reflect"
	"slices"
	"testing"
)

// echoMachine is a StateMachine whose outputs name the command. It records the indexes it applied;
// its hash is their count.
type echoMachine struct{ applied []uint64 }

func (m *echoMachine) Apply(index uint64, cmd string) string {
	m.applied = append(m.applied, index)
	return "applied " + cmd
}

func (m *echoMachine) Query(cmd string) string { return "query " + cmd }
func (m *echoMachine) Hash() uint64            { return uint64(len(m.applied)) }

// lead makes the harness's core the leader of its current term, with next = last+1 and match 0
// for every peer, as RFT-180 sets them, but without appending a no-op.
func (h *harness) lead() {
	c := h.c
	c.role = leader
	c.leader = c.cfg.ID
	for p := range c.cfg.Peers {
		c.next[p] = c.log.LastIndex() + 1
		c.match[p] = 0
	}
}

// results returns the Results of takeReplies, in order.
func (h *harness) results() []Result {
	var out []Result
	for _, r := range h.c.takeReplies() {
		out = append(out, r.r)
	}
	return out
}

func TestATRFT03FreshEntrySlices(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 1}}, 0)
	h.lead()
	h.c.appendLog(terms(1, 1, 1)...)
	h.c.next[1], h.c.next[2] = 1, 1
	h.c.broadcastAppend()
	got := h.sends()
	if len(got) != 2 {
		t.Fatalf("queued %+v, want one AppendEntries per peer", got)
	}
	a, aok := got[0].msg.(AppendEntries)
	b, bok := got[1].msg.(AppendEntries)
	if !aok || !bok || !slices.Equal(a.Entries, terms(1, 1, 1)) || !slices.Equal(b.Entries, terms(1, 1, 1)) {
		t.Fatalf("queued %+v, want two AppendEntries carrying [t1 t1 t1]", got)
	}
	first := &h.c.log.entries[0]
	if &a.Entries[0] == &b.Entries[0] || &a.Entries[0] == first || &b.Entries[0] == first {
		t.Error("the two AppendEntries and the log share entry memory")
	}

	p, ok := h.c.endTurn()
	if !ok || p.From != 1 || !slices.Equal(p.Entries, terms(1, 1, 1)) {
		t.Fatalf("endTurn = %+v, %v; want From 1 and [t1 t1 t1]", p, ok)
	}
	if &p.Entries[0] == first || &p.Entries[0] == &a.Entries[0] || &p.Entries[0] == &b.Entries[0] {
		t.Error("Persist.Entries shares memory with the log or an AppendEntries")
	}
}

func TestLeaderSends(t *testing.T) {
	t.Run("tick and proposal", func(t *testing.T) {
		h := newHarness("n2", peers3, Durable{HardState: HardState{Term: 3}, Log: terms(1, 1, 2, 3, 3)}, 0)
		h.c.cfg.MaxBatch = 2
		h.lead()
		h.c.commit, h.c.applied = 2, 2
		h.c.next[0], h.c.next[2] = 2, 6

		h.c.onTick()
		want := []queued{
			{peer: 0, msg: AppendEntries{Term: 3, Leader: "n2", PrevIndex: 1, PrevTerm: 1, Entries: terms(1, 2), Commit: 2}},
			{peer: 2, msg: AppendEntries{Term: 3, Leader: "n2", PrevIndex: 5, PrevTerm: 3, Commit: 2}},
		}
		if got := h.sends(); !reflect.DeepEqual(got, want) {
			t.Fatalf("on tick queued %+v, want %+v", got, want)
		}

		h.c.step(Propose{Client: 1, Seq: 1, Cmd: "x"})
		cmd := Entry{Term: 3, Client: 1, Seq: 1, Cmd: "x"}
		want = append(want,
			queued{peer: 0, msg: AppendEntries{Term: 3, Leader: "n2", PrevIndex: 1, PrevTerm: 1, Entries: terms(1, 2), Commit: 2}},
			queued{peer: 2, msg: AppendEntries{Term: 3, Leader: "n2", PrevIndex: 5, PrevTerm: 3, Entries: []Entry{cmd}, Commit: 2}},
		)
		if got := h.sends(); !reflect.DeepEqual(got, want) {
			t.Fatalf("after a proposal queued %+v, want %+v", got, want)
		}
		if h.c.next[0] != 2 || h.c.next[2] != 6 {
			t.Errorf("next %v after sending, want n1 2 and n3 6 unchanged", h.c.next)
		}
	})

	t.Run("won election", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 1}, Log: terms(1)}, 0)
		h.c.start()
		h.c.onElectionTimeout(h.env.lastGen(t))
		h.c.outbox = h.c.outbox[:0] // drop the election's RequestVotes
		h.c.step(VoteReply{Term: 2, From: "n2", Granted: true})
		ae := AppendEntries{Term: 2, Leader: "n1", PrevIndex: 1, PrevTerm: 1, Entries: []Entry{{Term: 2}}}
		want := []queued{{peer: 1, msg: ae}, {peer: 2, msg: ae}}
		if got := h.sends(); !reflect.DeepEqual(got, want) {
			t.Errorf("winning queued %+v, want the no-op to every peer %+v", got, want)
		}
	})
}

func TestATRFT15AppendMatchingAndConflict(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 3}, Log: terms(1, 1, 2)}, 0)
	h.c.step(AppendEntries{Term: 3, Leader: "n2", PrevIndex: 1, PrevTerm: 1, Entries: terms(1)})
	if !slices.Equal(h.c.log.entries, terms(1, 1, 2)) {
		t.Errorf("after a matching AppendEntries the log is %+v, want [t1 t1 t2]", h.c.log.entries)
	}
	want := []queued{{peer: 1, msg: AppendReply{Term: 3, From: "n1", Success: true, Match: 2}}}
	if got := h.sends(); !reflect.DeepEqual(got, want) {
		t.Errorf("queued %+v, want %+v", got, want)
	}
	if calls := h.obs.take(); len(calls) != 0 {
		t.Errorf("a matching AppendEntries called the observer: %q", calls)
	}
	if h.c.dirty {
		t.Error("a matching AppendEntries made the state dirty")
	}
	if h.c.leader != "n2" || len(h.env.arms) != 1 {
		t.Errorf("leader %q, arms %v; want n2 and one arm", h.c.leader, h.env.arms)
	}

	h.c.step(AppendEntries{Term: 3, Leader: "n2", PrevIndex: 1, PrevTerm: 1, Entries: terms(3)})
	if !slices.Equal(h.c.log.entries, terms(1, 3)) {
		t.Errorf("after a conflicting AppendEntries the log is %+v, want [t1 t3]", h.c.log.entries)
	}
	if calls := h.obs.take(); !slices.Equal(calls, []string{"LogWritten n1 from=2 last=2"}) {
		t.Errorf("observer calls %q, want one LogWritten from 2", calls)
	}
	if h.c.log.Chain(2) != chainNext(chainNext(ChainSeed, Entry{Term: 1}), Entry{Term: 3}) {
		t.Error("the chain hash of index 2 does not cover the new entry")
	}
}

func TestATRFT16AppendGap(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1)}, 0)
	h.c.step(AppendEntries{Term: 2, Leader: "n3", PrevIndex: 3, PrevTerm: 1})
	want := []queued{{peer: 2, msg: AppendReply{Term: 2, From: "n1", LastIndex: 1}}}
	if got := h.sends(); !reflect.DeepEqual(got, want) {
		t.Errorf("queued %+v, want %+v", got, want)
	}

	h.c.step(AppendEntries{Term: 2, Leader: "n3", PrevIndex: 1, PrevTerm: 2, Entries: terms(2)})
	want = append(want, queued{peer: 2, msg: AppendReply{Term: 2, From: "n1", LastIndex: 1}})
	if got := h.sends(); !reflect.DeepEqual(got, want) {
		t.Errorf("after a PrevTerm mismatch queued %+v, want %+v", got, want)
	}
	if !slices.Equal(h.c.log.entries, terms(1)) {
		t.Errorf("log %+v, want [t1]", h.c.log.entries)
	}
}

func TestATRFT17CommitBoundedByMatch(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 1}}, 0)
	sm := &echoMachine{}
	h.c.sm = sm
	h.c.step(AppendEntries{Term: 1, Leader: "n2", Entries: terms(1, 1), Commit: 5})
	if h.c.commit != 2 || h.c.applied != 2 {
		t.Errorf("commit %d, applied %d; want 2, 2", h.c.commit, h.c.applied)
	}
	want := []string{
		"LogWritten n1 from=1 last=2",
		"Committed n1 term=1 from=0 to=2",
		"Applied n1 index=1 {Term:1 Client:0 Seq:0 Cmd:} hash=0",
		"Applied n1 index=2 {Term:1 Client:0 Seq:0 Cmd:} hash=0",
	}
	if calls := h.obs.take(); !slices.Equal(calls, want) {
		t.Errorf("observer calls %q, want %q", calls, want)
	}
	if len(sm.applied) != 0 {
		t.Errorf("the state machine applied %v, want the no-ops skipped", sm.applied)
	}

	// A stale AppendEntries never lowers the commit index.
	h.c.step(AppendEntries{Term: 1, Leader: "n2", Commit: 4})
	if h.c.commit != 2 {
		t.Errorf("commit %d after a stale AppendEntries, want 2", h.c.commit)
	}
}

func TestAppendReplyBackoff(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 1}, Log: terms(1, 1, 1, 1, 1)}, 0)
	h.lead()
	h.c.next[1] = 5
	steps := []struct {
		reply       AppendReply
		next, match uint64
	}{
		{AppendReply{Term: 1, From: "n2", LastIndex: 9}, 4, 0},
		{AppendReply{Term: 1, From: "n2", LastIndex: 1}, 2, 0},
		{AppendReply{Term: 1, From: "n2", LastIndex: 0}, 1, 0},
		{AppendReply{Term: 1, From: "n2", LastIndex: 0}, 1, 0},
		{AppendReply{Term: 1, From: "n2", Success: true, Match: 3}, 4, 3},
		{AppendReply{Term: 1, From: "n2", Success: true, Match: 2}, 4, 3},
	}
	for i, s := range steps {
		h.c.step(s.reply)
		if h.c.next[1] != s.next || h.c.match[1] != s.match {
			t.Errorf("step %d %+v: next %d, match %d; want %d, %d", i, s.reply, h.c.next[1], h.c.match[1], s.next, s.match)
		}
	}
	if h.c.next[2] != 6 || h.c.match[2] != 0 {
		t.Errorf("n3: next %d, match %d; want 6, 0 untouched", h.c.next[2], h.c.match[2])
	}
}

func TestATRFT18CommitNeedsCurrentTerm(t *testing.T) {
	h := newHarness("n1", peers5, Durable{HardState: HardState{Term: 3}, Log: terms(1, 2, 3, 3)}, 0)
	if h.c.durableLast != 4 {
		t.Fatalf("durable last index %d after loading 4 entries, want 4", h.c.durableLast)
	}
	h.lead()
	h.c.step(AppendReply{Term: 3, From: "n2", Success: true, Match: 4})
	h.c.step(AppendReply{Term: 3, From: "n3", Success: true, Match: 2})
	if h.c.commit != 0 {
		t.Fatalf("commit %d after Match 4 and Match 2, want 0 (index 2 has term 2)", h.c.commit)
	}
	h.c.step(AppendReply{Term: 3, From: "n4", Success: true, Match: 4})
	if h.c.commit != 4 {
		t.Fatalf("commit %d after a third Match 4, want 4", h.c.commit)
	}
	if calls := h.obs.take(); len(calls) == 0 || calls[0] != "Committed n1 term=3 from=0 to=4" {
		t.Errorf("observer calls %q, want one commit from 0 to 4 first", calls)
	}
}

func TestLeaderCommitUsesDurableLast(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1, 2)}, 0)
	h.lead()
	h.c.appendLog(Entry{Term: 2, Client: 1, Seq: 1, Cmd: "x"})
	h.c.step(AppendReply{Term: 2, From: "n2", Success: true, Match: 3})
	if h.c.commit != 2 {
		t.Fatalf("commit %d before the leader's own entry is durable, want 2", h.c.commit)
	}
	if _, ok := h.c.endTurn(); !ok {
		t.Fatal("endTurn issued no persist for the appended entry")
	}
	h.obs.take()
	h.c.persisted()
	if h.c.commit != 3 {
		t.Errorf("commit %d after the leader's persist, want 3", h.c.commit)
	}
	calls := h.obs.take()
	if len(calls) < 2 || calls[1] != "Committed n1 term=2 from=2 to=3" {
		t.Errorf("observer calls %q, want Durable then a commit from 2 to 3", calls)
	}
}

func TestTruncationLowersDurableLast(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1, 1, 1, 1)}, 0)
	h.c.step(AppendEntries{Term: 2, Leader: "n2", PrevIndex: 4, PrevTerm: 1, Entries: terms(2)})
	if p, ok := h.c.endTurn(); !ok || p.From != 5 {
		t.Fatalf("endTurn = %+v, %v; want a persist from 5", p, ok)
	}
	// While that persist (last 5) is in flight, a new leader's entry replaces index 2 onward.
	h.c.step(AppendEntries{Term: 2, Leader: "n2", PrevIndex: 1, PrevTerm: 1, Entries: terms(2)})
	if !slices.Equal(h.c.log.entries, terms(1, 2)) || h.c.durableLast != 1 {
		t.Fatalf("log %+v, durable last %d; want [t1 t2] and 1", h.c.log.entries, h.c.durableLast)
	}
	h.c.persisted()
	if h.c.durableLast != 1 {
		t.Errorf("durable last %d after the older persist completed, want 1", h.c.durableLast)
	}
	p, ok := h.c.endTurn()
	if !ok || p.From != 2 || !slices.Equal(p.Entries, terms(2)) {
		t.Errorf("endTurn = %+v, %v; want From 2 and [t2]", p, ok)
	}
}

func TestLeaderStepDownArmsElection(t *testing.T) {
	msgs := []struct {
		msg  Msg
		arms int // arms beyond the step-down's own: an accepted AppendEntries arms too (RFT-150)
	}{
		{RequestVote{Term: 5, Candidate: "n2"}, 0},
		{VoteReply{Term: 5, From: "n2"}, 0},
		{AppendEntries{Term: 5, Leader: "n2", PrevIndex: 2, PrevTerm: 3}, 1},
		{AppendReply{Term: 5, From: "n2"}, 0},
	}
	for _, m := range msgs {
		for _, r := range []role{follower, candidate, leader} {
			h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 3}, Log: terms(1, 3)}, 0)
			h.c.role = r
			h.c.step(m.msg)
			want := m.arms
			if r == leader {
				want++
			}
			if len(h.env.arms) != want {
				t.Errorf("%T to a %v: armed %d times, want %d", m.msg, r, len(h.env.arms), want)
			}
		}
	}
}

func TestFollowerMatchingAppendAllocs(t *testing.T) {
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1, 1, 2)}, 0)
	var m Msg = AppendEntries{Term: 2, Leader: "n2", PrevIndex: 1, PrevTerm: 1, Entries: terms(1, 2), Commit: 0}
	h.env.arms = make([]uint64, 0, 1024)
	allocs := testing.AllocsPerRun(100, func() {
		h.c.step(m)
		h.c.takeSends()
	})
	// The one allocation is the AppendReply boxed into a Msg, which sending needs anyway.
	if allocs > 1 {
		t.Errorf("a matching AppendEntries allocates %v times per call, want at most 1", allocs)
	}
}
