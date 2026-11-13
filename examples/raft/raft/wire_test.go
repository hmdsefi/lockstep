// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"reflect"
	"testing"

	"github.com/hmdsefi/lockstep/codec"
)

// TestATRFT51Registrations checks the registration half of AT-RFT-51: the six cross-node types
// hold their wire names, and the default registry passes the JSON support check.
func TestATRFT51Registrations(t *testing.T) {
	reg := codec.Default()
	cases := []struct {
		name string
		val  any
	}{
		{"raft.RequestVote", RequestVote{}},
		{"raft.VoteReply", VoteReply{}},
		{"raft.AppendEntries", AppendEntries{}},
		{"raft.AppendReply", AppendReply{}},
		{"raft.Propose", Propose{}},
		{"raft.Result", Result{}},
	}
	for _, tc := range cases {
		got, err := reg.NameOf(tc.val)
		if err != nil || got != tc.name {
			t.Errorf("NameOf(%T) = %q, %v; want %q", tc.val, got, err, tc.name)
		}
		typ, err := reg.TypeOf(tc.name)
		if err != nil || typ != reflect.TypeOf(tc.val) {
			t.Errorf("TypeOf(%q) = %v, %v; want %T", tc.name, typ, err, tc.val)
		}
	}
	if err := reg.Check(codec.JSON{}); err != nil {
		t.Fatalf("Check(JSON) = %v; want nil", err)
	}
}
