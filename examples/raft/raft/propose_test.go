// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"reflect"
	"slices"
	"testing"
)

func TestATRFT19ApplyRepliesToProposal(t *testing.T) {
	t.Run("matching entry", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1, 1, 1, 2)}, 0)
		sm := &echoMachine{}
		h.c.sm = sm
		h.lead()
		h.c.step(Propose{Client: 7, Seq: 3, Cmd: "put"})
		if got := h.results(); len(got) != 0 {
			t.Fatalf("replies %+v before the entry is applied, want none", got)
		}
		if _, ok := h.c.endTurn(); !ok {
			t.Fatal("endTurn issued no persist for the proposal")
		}
		h.c.persisted()
		h.obs.take()
		h.c.step(AppendReply{Term: 2, From: "n2", Success: true, Match: 5})

		want := Result{Status: OK, Output: "applied put", Index: 5}
		calls := h.obs.take()
		wantCalls := []string{
			"Committed n1 term=2 from=0 to=5",
			"Applied n1 index=1 {Term:1 Client:0 Seq:0 Cmd:} hash=0",
			"Applied n1 index=2 {Term:1 Client:0 Seq:0 Cmd:} hash=0",
			"Applied n1 index=3 {Term:1 Client:0 Seq:0 Cmd:} hash=0",
			"Applied n1 index=4 {Term:2 Client:0 Seq:0 Cmd:} hash=0",
			"Applied n1 index=5 {Term:2 Client:7 Seq:3 Cmd:put} hash=1",
			"Replied n1 client=7 seq=3 {Status:ok Output:applied put Index:5 Leader:}",
		}
		if !slices.Equal(calls, wantCalls) {
			t.Errorf("observer calls %q, want %q", calls, wantCalls)
		}
		if got := h.results(); !reflect.DeepEqual(got, []Result{want}) {
			t.Errorf("replies %+v, want [%+v]", got, want)
		}
		if !slices.Equal(sm.applied, []uint64{5}) || len(h.c.pending) != 0 {
			t.Errorf("applied %v, pending %+v; want [5] and none pending", sm.applied, h.c.pending)
		}
	})

	t.Run("another client's entry", func(t *testing.T) {
		log := append(terms(1, 1, 1, 2), Entry{Term: 2, Client: 9, Seq: 1, Cmd: "other"})
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: log}, 0)
		h.c.sm = &echoMachine{}
		h.lead()
		h.c.pending = []proposal{{index: 5, term: 2, client: 7, seq: 3}}
		h.c.step(AppendReply{Term: 2, From: "n2", Success: true, Match: 5})
		calls := h.obs.take()
		wantLast := []string{
			"Applied n1 index=5 {Term:2 Client:9 Seq:1 Cmd:other} hash=1",
			"Replied n1 client=7 seq=3 {Status:unknown Output: Index:0 Leader:}",
		}
		if len(calls) < 2 || !slices.Equal(calls[len(calls)-2:], wantLast) {
			t.Errorf("observer calls %q, want them to end with %q", calls, wantLast)
		}
		if got := h.results(); !reflect.DeepEqual(got, []Result{{Status: Unknown}}) {
			t.Errorf("replies %+v, want [Unknown]", got)
		}
	})
}

func TestATRFT20ProposeRejections(t *testing.T) {
	t.Run("follower", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1)}, 0)
		h.c.leader = "n3"
		h.c.step(Propose{Client: 1, Seq: 1, Cmd: "x"})
		if got := h.results(); !reflect.DeepEqual(got, []Result{{Status: NotLeader, Leader: "n3"}}) {
			t.Errorf("replies %+v, want [NotLeader n3]", got)
		}
		if h.c.log.LastIndex() != 1 || len(h.c.outbox) != 0 || h.c.dirty {
			t.Errorf("log %+v, outbox %+v, dirty %v; want nothing appended or sent",
				h.c.log.entries, h.c.outbox, h.c.dirty)
		}
	})

	t.Run("leader at MaxPending", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1, 2)}, 0)
		h.c.cfg.MaxPending = 1
		h.lead()
		h.c.step(Propose{Client: 1, Seq: 1, Cmd: "x"})
		if len(h.c.pending) != 1 || h.c.log.LastIndex() != 3 {
			t.Fatalf("pending %+v, last %d; want one pending at 3", h.c.pending, h.c.log.LastIndex())
		}
		outbox := len(h.c.outbox)
		h.obs.take()
		h.c.step(Propose{Client: 2, Seq: 1, Cmd: "y"})
		if got := h.results(); !reflect.DeepEqual(got, []Result{{Status: Busy}}) {
			t.Errorf("replies %+v, want [Busy]", got)
		}
		if h.c.log.LastIndex() != 3 || len(h.c.pending) != 1 || len(h.c.outbox) != outbox {
			t.Errorf("last %d, pending %d, outbox %d; want the Busy proposal neither appended nor sent",
				h.c.log.LastIndex(), len(h.c.pending), len(h.c.outbox))
		}
		want := []string{"Replied n1 client=2 seq=1 {Status:busy Output: Index:0 Leader:}"}
		if calls := h.obs.take(); !slices.Equal(calls, want) {
			t.Errorf("observer calls %q, want %q", calls, want)
		}
	})
}

func TestATRFT21StepDownFailsPending(t *testing.T) {
	log := append(terms(1, 1, 2), Entry{Term: 2, Client: 1, Seq: 4}, Entry{Term: 2, Client: 2, Seq: 5},
		Entry{Term: 2, Client: 3, Seq: 6})
	h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: log}, 0)
	h.lead()
	h.c.pending = []proposal{{index: 4, term: 2, client: 1, seq: 4}, {index: 6, term: 2, client: 3, seq: 6}}
	h.c.step(AppendEntries{Term: 3, Leader: "n2", PrevIndex: 6, PrevTerm: 2})

	if h.c.role != follower || h.c.term != 3 || h.c.leader != "n2" || len(h.c.pending) != 0 {
		t.Errorf("role %v, term %d, leader %q, pending %+v; want follower, 3, n2, none",
			h.c.role, h.c.term, h.c.leader, h.c.pending)
	}
	if got := h.results(); !reflect.DeepEqual(got, []Result{{Status: Unknown}, {Status: Unknown}}) {
		t.Errorf("replies %+v, want two Unknown", got)
	}
	want := []string{
		"Replied n1 client=1 seq=4 {Status:unknown Output: Index:0 Leader:}",
		"Replied n1 client=3 seq=6 {Status:unknown Output: Index:0 Leader:}",
	}
	if calls := h.obs.take(); !slices.Equal(calls, want) {
		t.Errorf("observer calls %q, want %q (index 4, then 6)", calls, want)
	}
}

func TestBugStaleRead(t *testing.T) {
	read := Propose{Client: 1, Seq: 1, Cmd: "get", Read: true}

	t.Run("with the switch", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1, 2)}, BugStaleRead)
		h.c.sm = &echoMachine{}
		h.lead()
		h.c.commit, h.c.applied = 1, 1
		h.c.step(read)
		want := Result{Status: OK, Output: "query get", Index: 1}
		if got := h.results(); !reflect.DeepEqual(got, []Result{want}) {
			t.Errorf("replies %+v, want [%+v]", got, want)
		}
		wantCalls := []string{"Replied n1 client=1 seq=1 {Status:ok Output:query get Index:1 Leader:}"}
		if calls := h.obs.take(); !slices.Equal(calls, wantCalls) {
			t.Errorf("observer calls %q, want %q", calls, wantCalls)
		}
		if h.c.log.LastIndex() != 2 || len(h.c.pending) != 0 || len(h.c.outbox) != 0 {
			t.Errorf("last %d, pending %d, outbox %d; want nothing appended or sent",
				h.c.log.LastIndex(), len(h.c.pending), len(h.c.outbox))
		}
	})

	t.Run("without the switch", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 2}, Log: terms(1, 2)}, 0)
		h.c.sm = &echoMachine{}
		h.lead()
		h.c.commit, h.c.applied = 1, 1
		h.c.step(read)
		if got := h.results(); len(got) != 0 {
			t.Errorf("replies %+v, want none before the read is applied", got)
		}
		if h.c.log.LastIndex() != 3 || h.c.log.Entry(3) != (Entry{Term: 2, Client: 1, Seq: 1, Cmd: "get"}) {
			t.Errorf("log %+v, want the read appended at 3", h.c.log.entries)
		}
		if len(h.c.pending) != 1 || len(h.c.outbox) != 2 {
			t.Errorf("pending %d, outbox %d; want 1 pending and an AppendEntries per peer",
				len(h.c.pending), len(h.c.outbox))
		}
	})
}
