// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"reflect"
	"slices"
	"testing"
)

func TestATRFT12VoteGranting(t *testing.T) {
	cases := []struct {
		name          string
		bugs          Bug
		first, second bool
		vote          string
	}{
		{"correct", 0, false, true, "n3"},
		{"BugVoteIgnoresLog", BugVoteIgnoresLog, true, false, "n2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 3}, Log: terms(1, 3)}, tc.bugs)
			h.c.step(RequestVote{Term: 4, Candidate: "n2", LastIndex: 1, LastTerm: 1})
			h.c.step(RequestVote{Term: 4, Candidate: "n3", LastIndex: 2, LastTerm: 3})
			if h.c.term != 4 || h.c.vote != tc.vote {
				t.Errorf("term %d, vote %q; want 4, %q", h.c.term, h.c.vote, tc.vote)
			}
			want := []queued{
				{peer: 1, msg: VoteReply{Term: 4, From: "n1", Granted: tc.first}},
				{peer: 2, msg: VoteReply{Term: 4, From: "n1", Granted: tc.second}},
			}
			if got := h.sends(); !reflect.DeepEqual(got, want) {
				t.Errorf("queued %+v, want %+v", got, want)
			}
			if len(h.env.arms) != 1 {
				t.Errorf("election timer armed %d times, want once", len(h.env.arms))
			}
		})
	}
}

func TestStartElection(t *testing.T) {
	h := newHarness("n3", peers5, Durable{HardState: HardState{Term: 2, Vote: "n1"}, Log: terms(1, 2)}, 0)
	h.c.start()
	h.c.onElectionTimeout(h.env.lastGen(t))
	if h.c.term != 3 || h.c.vote != "n3" || h.c.role != candidate {
		t.Fatalf("term %d, vote %q, role %v; want 3, n3, candidate", h.c.term, h.c.vote, h.c.role)
	}
	if len(h.env.arms) != 2 {
		t.Errorf("arms %v, want the start's and the election's", h.env.arms)
	}
	rv := RequestVote{Term: 3, Candidate: "n3", LastIndex: 2, LastTerm: 2}
	want := []queued{{peer: 0, msg: rv}, {peer: 1, msg: rv}, {peer: 3, msg: rv}, {peer: 4, msg: rv}}
	if got := h.sends(); !reflect.DeepEqual(got, want) {
		t.Fatalf("queued %+v, want %+v", got, want)
	}

	h.c.step(VoteReply{Term: 3, From: "n1", Granted: false})
	h.c.step(VoteReply{Term: 3, From: "n4", Granted: true})
	if h.c.role != candidate {
		t.Fatalf("role %v with 2 of 5 votes, want candidate", h.c.role)
	}
	h.c.onTick()
	want = append(want, queued{peer: 1, msg: rv}, queued{peer: 4, msg: rv})
	if got := h.sends(); !reflect.DeepEqual(got, want) {
		t.Errorf("after a tick queued %+v, want %+v", got, want)
	}
}

func TestWinElection(t *testing.T) {
	t.Run("three servers", func(t *testing.T) {
		h := newHarness("n1", peers3, Durable{HardState: HardState{Term: 1}, Log: terms(1)}, 0)
		h.c.start()
		h.c.onElectionTimeout(h.env.lastGen(t))
		h.obs.take()
		h.c.step(VoteReply{Term: 2, From: "n2", Granted: true})
		h.c.step(VoteReply{Term: 2, From: "n3", Granted: true}) // already leader: no second win
		if h.c.role != leader || h.c.leader != "n1" {
			t.Fatalf("role %v, leader %q; want leader, n1", h.c.role, h.c.leader)
		}
		if h.env.cancels != 1 {
			t.Errorf("election timer cancelled %d times, want once", h.env.cancels)
		}
		if !slices.Equal(h.c.next, []uint64{2, 2, 2}) || !slices.Equal(h.c.match, []uint64{0, 0, 0}) {
			t.Errorf("next %v, match %v; want [2 2 2], [0 0 0]", h.c.next, h.c.match)
		}
		want := []string{"BecameLeader n1 term=2 last=1", "LogWritten n1 from=2 last=2"}
		if calls := h.obs.take(); !slices.Equal(calls, want) {
			t.Errorf("observer calls %q, want %q", calls, want)
		}
		if !slices.Equal(h.c.log.entries, terms(1, 2)) {
			t.Errorf("log %+v, want [t1, no-op t2]", h.c.log.entries)
		}
	})

	t.Run("one server", func(t *testing.T) {
		h := newHarness("n1", []string{"n1"}, Durable{}, 0)
		h.c.start()
		h.obs.take()
		h.c.onElectionTimeout(h.env.lastGen(t))
		if h.c.role != leader || h.c.term != 1 || h.env.cancels != 1 {
			t.Fatalf("role %v, term %d, cancels %d; want leader, 1, 1", h.c.role, h.c.term, h.env.cancels)
		}
		want := []string{"BecameLeader n1 term=1 last=0", "LogWritten n1 from=1 last=1"}
		if calls := h.obs.take(); !slices.Equal(calls, want) {
			t.Errorf("observer calls %q, want %q", calls, want)
		}
		if len(h.c.outbox) != 0 {
			t.Errorf("outbox %+v, want empty", h.c.outbox)
		}
	})
}
