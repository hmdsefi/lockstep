// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/codec"
)

// TestATCLU26WireTypes checks that the seven wire types are registered in the default registry
// under CLU-700's names, round-trip through codec.JSON, and are accepted by JSON.Supports.
func TestATCLU26WireTypes(t *testing.T) {
	u := func(n string, s MemberState) update {
		return update{Node: n, ID: 7, Inc: 1 << 40, State: s, Kinds: []string{"counter", "session.v2"}}
	}
	ups := []update{u("a", Alive), u("b", Suspect), u("c", Dead), u("d", Left)}
	cases := []struct {
		name string
		val  memberMsg
	}{
		{"lockstep.cluster.update", u("a", Alive)},
		{"lockstep.cluster.ping", ping{From: u("s", Alive), Seq: 1 << 63, Target: "t", TargetID: 9, Updates: ups}},
		{"lockstep.cluster.pingreq", pingReq{From: u("s", Alive), Seq: 2, Target: "t", TargetID: 9, Updates: ups}},
		{"lockstep.cluster.ack", ack{From: u("s", Alive), Seq: 3, Target: "t", Updates: ups}},
		{"lockstep.cluster.syncreq", syncReq{From: u("s", Alive), Members: ups}},
		{"lockstep.cluster.syncresp", syncResp{From: u("s", Alive), Members: ups}},
		{"lockstep.cluster.leave", leave{From: u("s", Left)}},
	}
	reg := codec.Default()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ, err := reg.TypeOf(tc.name)
			if err != nil || typ != reflect.TypeOf(tc.val) {
				t.Fatalf("TypeOf(%q) = %v, %v, want %v", tc.name, typ, err, reflect.TypeOf(tc.val))
			}
			if got, err := reg.NameOf(tc.val); err != nil || got != tc.name {
				t.Fatalf("NameOf = %q, %v, want %q", got, err, tc.name)
			}
			if err := (codec.JSON{}).Supports(typ); err != nil {
				t.Fatalf("Supports: %v", err)
			}
			name, data, err := reg.Encode(codec.JSON{}, tc.val)
			if err != nil || name != tc.name {
				t.Fatalf("Encode = %q, %v, want %q", name, err, tc.name)
			}
			got, err := reg.Decode(codec.JSON{}, name, data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.val) {
				t.Fatalf("round trip = %+v, want %+v", got, tc.val)
			}
		})
	}
	if err := reg.Check(codec.JSON{}); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

// TestInvalidUpdates checks validUpdate (CLU-710) over every state and the edges around them.
func TestInvalidUpdates(t *testing.T) {
	for _, tc := range []struct {
		name string
		u    update
		want string
	}{
		{"valid alive", update{Node: "a", ID: 1, State: Alive}, ""},
		{"valid suspect", update{Node: "a", State: Suspect}, ""},
		{"valid dead", update{Node: "a", State: Dead}, ""},
		{"valid left", update{Node: "a", State: Left}, ""},
		{"zero id and incarnation", update{Node: "a", State: Alive}, ""},
		{"empty node", update{State: Alive}, reasonEmptyNode},
		{"empty node and bad state", update{State: 9}, reasonEmptyNode},
		{"state zero", update{Node: "a"}, reasonBadState},
		{"state past Left", update{Node: "a", State: Left + 1}, reasonBadState},
		{"state max", update{Node: "a", State: 255}, reasonBadState},
		{"kinds", update{Node: "a", State: Alive, Kinds: []string{"a", "b.1", "c_2-x"}}, ""},
		{"kinds empty", update{Node: "a", State: Alive, Kinds: []string{}}, ""},
		{"kinds unsorted", update{Node: "a", State: Alive, Kinds: []string{"b", "a"}}, reasonBadKinds},
		{"kinds duplicate", update{Node: "a", State: Alive, Kinds: []string{"a", "a"}}, reasonBadKinds},
		{"kinds empty name", update{Node: "a", State: Alive, Kinds: []string{""}}, reasonBadKinds},
		{"kinds invalid name", update{Node: "a", State: Alive, Kinds: []string{"a", "B"}}, reasonBadKinds},
		{"kinds dot", update{Node: "a", State: Alive, Kinds: []string{".."}}, reasonBadKinds},
		{"kinds long name", update{Node: "a", State: Alive, Kinds: []string{strings.Repeat("a", 65)}}, reasonBadKinds},
		{"kinds after state", update{Node: "a", Kinds: []string{"b", "a"}}, reasonBadState},
		{"kinds after node", update{State: Alive, Kinds: []string{"b", "a"}}, reasonEmptyNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := validUpdate(tc.u); got != tc.want {
				t.Fatalf("validUpdate(%+v) = %q, want %q", tc.u, got, tc.want)
			}
		})
	}
	if reasonEmptyNode != "empty-node" || reasonBadState != "bad-state" || reasonBadKinds != "bad-kinds" {
		t.Fatal("the reasons are part of the invalid record")
	}
}

// TestUpdateMember checks the conversions between Member and update.
func TestUpdateMember(t *testing.T) {
	m := Member{Node: "a", ID: 3, Incarnation: 4, State: Suspect}
	if got := toUpdate(m); !sameUpdate(got, update{Node: "a", ID: 3, Inc: 4, State: Suspect}) {
		t.Fatalf("toUpdate = %+v", got)
	}
	if got := toUpdate(m).member(); !sameMember(got, m) {
		t.Fatalf("member = %+v", got)
	}
	// Kinds travel with the entry in both directions; an empty list is nil in a Member.
	k := Member{Node: "a", ID: 3, State: Alive, Kinds: []string{"a", "b"}}
	if got := toUpdate(k); !sameUpdate(got, update{Node: "a", ID: 3, State: Alive, Kinds: []string{"a", "b"}}) {
		t.Fatalf("toUpdate with kinds = %+v", got)
	}
	if got := toUpdate(k).member(); !sameMember(got, k) {
		t.Fatalf("member with kinds = %+v", got)
	}
	if got := (update{Node: "a", State: Alive, Kinds: []string{}}).member(); got.Kinds != nil {
		t.Fatalf("member of an empty Kinds has Kinds %#v, want nil", got.Kinds)
	}
	// Every wire type is a memberMsg (checked at compile time).
	_ = []memberMsg{update{}, ping{}, pingReq{}, ack{}, syncReq{}, syncResp{}, leave{}}
}

// sameUpdate reports whether two updates are equal, Kinds included.
func sameUpdate(a, b update) bool {
	return a.Node == b.Node && a.ID == b.ID && a.Inc == b.Inc && a.State == b.State && slices.Equal(a.Kinds, b.Kinds)
}
