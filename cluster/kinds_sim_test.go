// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// withKinds returns m with Kinds set.
func withKinds(m Member, kinds ...string) Member {
	m.Kinds = kinds
	return m
}

// VAC-700: a member's kinds reach every other node's view through the membership protocol, and a
// restart of the membership actor keeps them (CLU-620). n1 hosts b and a (declared in that order),
// n2 hosts c, n3 hosts nothing.
func TestKindsReachEveryView(t *testing.T) {
	sim.Simulate(t, simOpts(3, 8*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		cfg := func(kinds ...Kind) Config {
			x := simConfig("n1")
			x.Kinds = kinds
			return x
		}
		c.node("n1", cfg(validKind("b"), validKind("a")))
		c.node("n2", cfg(validKind("c")))
		c.node("n3", cfg())
		w.RunFor(5 * time.Second)

		want := []Member{
			withKinds(alive("n1", c.id("n1")), "a", "b"),
			withKinds(alive("n2", c.id("n2")), "c"),
			alive("n3", c.id("n3")),
		}
		for _, n := range []string{"n1", "n2", "n3"} {
			expectView(tt, n, c.view(n), want)
		}
		if got := c.view("n3").Self.Kinds; got != nil {
			tt.Errorf("n3's Self.Kinds = %#v, want nil", got)
		}

		// A restart of the membership actor rebuilds its table from the view: kinds included, own
		// and others'.
		before := c.view("n1").Version
		if err := s.InjectFail(lockstep.PIDAt[memberMsg]("n1", actorPath).Address(), errors.New("boom")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(2 * time.Second)
		if restarts := c.recs("n1", "start"); len(restarts) != 2 || attr(restarts[1], "restart") != "true" {
			tt.Fatalf("n1's start records: %s, want a restart", describe(restarts))
		}
		if v := c.view("n1"); v.Version < before {
			tt.Errorf("n1's view went back from %d to %d", before, v.Version)
		}
		for _, n := range []string{"n1", "n2", "n3"} {
			expectView(tt, n, c.view(n), want)
		}
	})
}

// VAC-700, CLU-710: an update whose kinds are not sorted, unique, valid kind names is dropped with
// reason=bad-kinds (alone when it is an element of Members, with its message when it is From); the
// kinds of an entry come from the update that adds it, and a later update with the same ID never
// changes them.
func TestBadKindsAndFixedKinds(t *testing.T) {
	sim.Simulate(t, simOpts(2, 3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		c.node("n1", Config{})
		w.RunFor(10 * time.Millisecond)
		to := lockstep.PIDAt[memberMsg]("n1", actorPath)
		send := func(msg memberMsg) {
			tt.Helper()
			if err := sim.Send(s, "n1", to, msg); err != nil {
				tt.Fatal(err)
			}
			w.RunFor(10 * time.Millisecond)
		}

		a := update{Node: "a", ID: 11, State: Alive, Kinds: []string{"x", "y"}}
		send(syncReq{From: a, Members: []update{
			{Node: "b", ID: 12, State: Alive, Kinds: []string{"y", "x"}},
			{Node: "c", ID: 13, State: Alive, Kinds: []string{"x", "x"}},
			{Node: "d", ID: 14, State: Alive, Kinds: []string{"X"}},
			{Node: "e", ID: 15, State: Alive, Kinds: []string{"k1"}},
		}})
		// An invalid From drops the whole message, its valid members included.
		send(syncResp{From: update{Node: "f", ID: 16, State: Alive, Kinds: []string{"z", "y"}},
			Members: []update{{Node: "g", ID: 17, State: Alive}}})

		var got []string
		for _, r := range c.recs("n1", "invalid") {
			got = append(got, attrs(r))
		}
		want := []string{
			"peer=a type=lockstep.cluster.syncreq reason=bad-kinds",
			"peer=a type=lockstep.cluster.syncreq reason=bad-kinds",
			"peer=a type=lockstep.cluster.syncreq reason=bad-kinds",
			"peer=f type=lockstep.cluster.syncresp reason=bad-kinds",
		}
		if !slices.Equal(got, want) {
			tt.Errorf("invalid records %q, want %q", got, want)
		}
		self := c.view("n1").Self
		wantMembers := []Member{withKinds(alive("a", 11), "x", "y"), withKinds(alive("e", 15), "k1"), self}
		if v := c.view("n1"); !slices.EqualFunc(v.Members, wantMembers, sameMember) {
			tt.Errorf("n1's members %v, want %v", v.Members, wantMembers)
		}

		// An update with the entry's ID changes its state, never its kinds.
		a.Inc, a.Kinds = 1, []string{"other"}
		send(ping{From: a})
		a.State, a.Kinds = Suspect, nil
		a.Inc = 2
		send(ping{From: update{Node: "e", ID: 15, State: Alive}, Updates: []update{a}})
		wantMembers = []Member{
			withKinds(Member{Node: "a", ID: 11, Incarnation: 2, State: Suspect}, "x", "y"),
			withKinds(alive("e", 15), "k1"),
			self,
		}
		if v := c.view("n1"); !slices.EqualFunc(v.Members, wantMembers, sameMember) {
			tt.Errorf("n1's members after the same-ID updates %v, want %v", v.Members, wantMembers)
		}

		// A new ID supersedes the entry and brings its own kinds.
		send(ping{From: update{Node: "a", ID: 21, State: Alive, Kinds: []string{"w"}}})
		if m, _ := c.view("n1").Member("a"); !sameMember(m, withKinds(alive("a", 21), "w")) {
			tt.Errorf("a after its restart: %v, want ID 21 with kinds [w]", m)
		}
	})
}
