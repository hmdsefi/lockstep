// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"

	"github.com/hmdsefi/lockstep"
)

// TestStatusString checks the names of the Status values and of an undefined one.
func TestStatusString(t *testing.T) {
	tests := []struct {
		s    Status
		want string
	}{
		{OK, "ok"},
		{NotLeader, "not-leader"},
		{Busy, "busy"},
		{Unknown, "unknown"},
		{Status(9), "status(9)"},
		{Status(0), "status(0)"},
	}
	for _, tc := range tests {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("Status(%d).String() = %q, want %q", uint8(tc.s), got, tc.want)
		}
	}
	if OK != 1 || NotLeader != 2 || Busy != 3 || Unknown != 4 {
		t.Errorf("Status values = %d %d %d %d, want 1 2 3 4", OK, NotLeader, Busy, Unknown)
	}
}

// TestBugString checks the names, their order and the rendering of undefined bits.
func TestBugString(t *testing.T) {
	tests := []struct {
		b    Bug
		want string
	}{
		{0, ""},
		{BugVoteIgnoresLog, "vote-ignores-log"},
		{BugStaleRead, "stale-read"},
		{BugVoteNotDurable, "vote-not-durable"},
		{BugVoteIgnoresLog | BugStaleRead, "vote-ignores-log,stale-read"},
		{BugStaleRead | BugVoteNotDurable, "stale-read,vote-not-durable"},
		{BugVoteNotDurable | BugVoteIgnoresLog | BugStaleRead, "vote-ignores-log,stale-read,vote-not-durable"},
		{1 << 3, "0x8"},
		{BugStaleRead | 1<<3, "stale-read,0x8"},
		{BugVoteIgnoresLog | 1<<3 | 1<<4, "vote-ignores-log,0x18"},
	}
	for _, tc := range tests {
		if got := tc.b.String(); got != tc.want {
			t.Errorf("Bug(%#x).String() = %q, want %q", uint32(tc.b), got, tc.want)
		}
	}
	if BugVoteIgnoresLog != 1 || BugStaleRead != 2 || BugVoteNotDurable != 4 {
		t.Errorf("Bug values = %d %d %d, want 1 2 4", BugVoteIgnoresLog, BugStaleRead, BugVoteNotDurable)
	}
}

// TestChainSeed checks the FNV-1a 64 offset basis.
func TestChainSeed(t *testing.T) {
	if ChainSeed != 0xcbf29ce484222325 {
		t.Fatalf("ChainSeed = %#x, want the FNV-1a 64 offset basis 0xcbf29ce484222325", ChainSeed)
	}
}

// TestMsgImplementations checks that the five exported messages are Msg values.
func TestMsgImplementations(t *testing.T) {
	msgs := []Msg{RequestVote{}, VoteReply{}, AppendEntries{}, AppendReply{}, Propose{}}
	if len(msgs) != 5 {
		t.Fatalf("got %d messages, want 5", len(msgs))
	}
}

// TestNodeIsActor checks that *Node satisfies lockstep.Actor[Msg].
func TestNodeIsActor(t *testing.T) {
	var _ lockstep.Actor[Msg] = (*Node)(nil)
}
