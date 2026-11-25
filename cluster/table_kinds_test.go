// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"slices"
	"testing"
	"time"
)

// VAC-700: the table takes Kinds from the update that adds an entry, supersedes it or restores it
// under a new ID, and never changes them for an update with the entry's ID; a tombstone keeps them,
// so Belief and a restore under the same ID carry them; SetSelf keeps the own kinds and a table
// rebuilt from a view keeps every entry's.
func TestTableKinds(t *testing.T) {
	km := func(node string, id uint32, inc uint64, s MemberState, kinds ...string) Member {
		x := m(node, id, inc, s)
		x.Kinds = kinds
		return x
	}
	kindsOf := func(t *testing.T, tb *table, name string) []string {
		t.Helper()
		x, ok := tb.Get(name)
		if !ok {
			t.Fatalf("no entry %q", name)
		}
		return x.Kinds
	}
	wantKinds := func(t *testing.T, got []string, want ...string) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Fatalf("kinds = %v, want %v", got, want)
		}
	}
	apply := func(t *testing.T, tb *table, u Member, direct bool, now time.Time) {
		t.Helper()
		if _, res := tb.Apply(u, direct, "test", now); res != applyChanged {
			t.Fatalf("Apply(%+v) = %v, want applyChanged", u, res)
		}
	}

	t.Run("add", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		apply(t, tb, km("x", 1, 0, Alive, "a", "b"), true, t0)
		apply(t, tb, km("y", 2, 0, Suspect), false, t0)
		wantKinds(t, kindsOf(t, tb, "x"), "a", "b")
		if got := kindsOf(t, tb, "y"); got != nil {
			t.Fatalf("y kinds = %v, want nil", got)
		}
	})

	t.Run("same ID never changes them", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		apply(t, tb, km("x", 1, 0, Alive, "a", "b"), true, t0)
		apply(t, tb, km("x", 1, 0, Suspect, "z"), false, t0.Add(time.Second))
		wantKinds(t, kindsOf(t, tb, "x"), "a", "b")
		apply(t, tb, km("x", 1, 1, Alive), true, t0.Add(2*time.Second))
		wantKinds(t, kindsOf(t, tb, "x"), "a", "b")
		apply(t, tb, km("x", 1, 1, Dead, "q"), true, t0.Add(3*time.Second))
		wantKinds(t, kindsOf(t, tb, "x"), "a", "b")
		// An update that loses changes nothing, kinds included.
		before := snapshot(tb)
		if _, res := tb.Apply(km("x", 1, 0, Alive, "k"), true, "test", t0.Add(4*time.Second)); res != applyIgnored {
			t.Fatalf("a losing update was %v", res)
		}
		if !sameTable(&before, tb) {
			t.Fatal("a losing update changed the table")
		}
	})

	t.Run("supersede takes the new kinds", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		apply(t, tb, km("x", 1, 0, Alive, "a"), true, t0)
		apply(t, tb, km("x", 2, 0, Alive, "c"), true, t0.Add(time.Second))
		wantKinds(t, kindsOf(t, tb, "x"), "c")
		apply(t, tb, km("x", 3, 0, Alive), true, t0.Add(2*time.Second))
		if got := kindsOf(t, tb, "x"); got != nil {
			t.Fatalf("kinds after a supersede with none = %v, want nil", got)
		}
		// Hearsay supersedes a down entry, and takes its kinds too.
		apply(t, tb, km("x", 3, 0, Dead), true, t0.Add(3*time.Second))
		apply(t, tb, km("x", 4, 0, Alive, "d"), false, t0.Add(4*time.Second))
		wantKinds(t, kindsOf(t, tb, "x"), "d")
	})

	t.Run("tombstone keeps them", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		apply(t, tb, km("x", 1, 0, Alive, "a", "b"), true, t0)
		apply(t, tb, km("x", 1, 0, Dead), true, t0.Add(time.Second))
		if !tb.Reap("x", 1, 0, Dead, t0.Add(time.Second+retention)) {
			t.Fatal("Reap failed")
		}
		b, tomb, ok := tb.Belief("x")
		if !ok || !tomb {
			t.Fatalf("Belief = %+v, %v, %v, want the tombstone", b, tomb, ok)
		}
		wantKinds(t, b.Kinds, "a", "b")
		if u := forcedUpdate(tb, "x"); u == nil || !slices.Equal(u.Kinds, []string{"a", "b"}) {
			t.Fatalf("forced update of the tombstone = %+v, want its kinds", u)
		}
		// A restore under the same ID keeps the tombstone's kinds, whatever the update says.
		apply(t, tb, km("x", 1, 1, Alive, "z"), true, t0.Add(2*time.Hour))
		wantKinds(t, kindsOf(t, tb, "x"), "a", "b")

		// A new ID meeting a tombstone is an addition and takes its own kinds.
		apply(t, tb, km("x", 1, 1, Dead), true, t0.Add(3*time.Hour))
		if !tb.Reap("x", 1, 1, Dead, t0.Add(3*time.Hour+retention)) {
			t.Fatal("second Reap failed")
		}
		apply(t, tb, km("x", 2, 0, Alive, "n"), true, t0.Add(4*time.Hour))
		wantKinds(t, kindsOf(t, tb, "x"), "n")
	})

	t.Run("own entry", func(t *testing.T) {
		self := selfEntry
		self.Kinds = []string{"a", "b"}
		tb := newTable(self, retention, t0)
		if _, ok := tb.SetSelf(Suspect, 3, t0); !ok {
			t.Fatal("SetSelf changed nothing")
		}
		wantKinds(t, tb.Self().Kinds, "a", "b")
		// An update about self with another ID is ignored, whatever kinds it has.
		if _, res := tb.Apply(km("self", 7, 0, Alive, "z"), true, "test", t0); res != applyIgnored {
			t.Fatalf("update about self with another ID: %v", res)
		}
		wantKinds(t, tb.Self().Kinds, "a", "b")
	})

	t.Run("rebuild", func(t *testing.T) {
		v := &View{Version: 4, Self: km("self", 100, 2, Alive, "a"), Members: []Member{
			km("self", 100, 0, Alive), km("x", 1, 0, Alive, "b", "c"), km("y", 2, 0, Dead),
		}}
		tb := rebuildTable(v, retention, t0)
		wantKinds(t, tb.Self().Kinds, "a")
		wantKinds(t, kindsOf(t, tb, "x"), "b", "c")
		if got := kindsOf(t, tb, "y"); got != nil {
			t.Fatalf("y kinds = %v, want nil", got)
		}
	})
}
