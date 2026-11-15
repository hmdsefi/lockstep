// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

var (
	t0        = time.Unix(1_000_000, 0).UTC()
	retention = DefaultDeadRetention
	selfEntry = Member{Node: "self", ID: 100, Incarnation: 0, State: Alive}
	allStates = []MemberState{Alive, Suspect, Dead, Left}
)

func m(node string, id uint32, inc uint64, s MemberState) Member {
	return Member{Node: node, ID: id, Incarnation: inc, State: s}
}

// mustApply applies u and fails unless it changed the table.
func mustApply(t *testing.T, tb *table, u Member, now time.Time) change {
	t.Helper()
	c, res := tb.Apply(u, "test", now)
	if res != applyChanged {
		t.Fatalf("Apply(%+v) = %v, want applyChanged", u, res)
	}
	return c
}

// snapshot returns a deep copy of the table, retired IDs included.
func snapshot(tb *table) table {
	s := *tb
	s.recs = slices.Clone(tb.recs)
	for i := range s.recs {
		s.recs[i].retired = slices.Clone(s.recs[i].retired)
	}
	return s
}

// sameTable reports whether a and b hold the same records, retired IDs and tombstone counters.
func sameTable(a, b *table) bool {
	if len(a.recs) != len(b.recs) || a.tombs != b.tombs || a.tombSeq != b.tombSeq {
		return false
	}
	for i := range a.recs {
		x, y := &a.recs[i], &b.recs[i]
		if x.m != y.m || x.tomb != y.tomb || x.tombSeq != y.tombSeq || !x.changed.Equal(y.changed) ||
			!slices.Equal(x.retired, y.retired) {
			return false
		}
	}
	return true
}

// mustIgnore applies u and fails unless it was ignored and left the table as it was, retired IDs
// and tombstones included.
func mustIgnore(t *testing.T, tb *table, u Member, now time.Time) {
	t.Helper()
	before := snapshot(tb)
	if c, res := tb.Apply(u, "test", now); res != applyIgnored || c != (change{}) {
		t.Fatalf("Apply(%+v) = %+v, %v, want ignored", u, c, res)
	}
	if !sameTable(&before, tb) {
		t.Fatalf("Apply(%+v) changed the table: %+v, was %+v", u, *tb, before)
	}
}

// mustRetireOnly applies u, an up update about the own name with another ID, and fails unless it
// was ignored and changed nothing but retiring u.ID.
func mustRetireOnly(t *testing.T, tb *table, u Member, now time.Time) {
	t.Helper()
	before := snapshot(tb)
	if c, res := tb.Apply(u, "test", now); res != applyIgnored || c != (change{}) {
		t.Fatalf("Apply(%+v) = %+v, %v, want ignored", u, c, res)
	}
	want := snapshot(&before)
	want.own().retire(u.ID)
	if !sameTable(&want, tb) {
		t.Fatalf("Apply(%+v) left %+v, want %+v", u, *tb, want)
	}
}

// mustGet fails unless the table's entry of want.Node is want.
func mustGet(t *testing.T, tb *table, want Member) {
	t.Helper()
	if got, ok := tb.Get(want.Node); !ok || got != want {
		t.Fatalf("Get(%q) = %+v, %v, want %+v", want.Node, got, ok, want)
	}
}

// retiredIDs returns the IDs retired for name.
func retiredIDs(tb *table, name string) []uint32 {
	i, ok := tb.find(name)
	if !ok {
		return nil
	}
	return tb.recs[i].retired
}

// enter puts x into the table with state s at now: added Alive, then moved to s at the same
// incarnation (every state but Alive wins over Alive at an equal incarnation).
func enter(t *testing.T, tb *table, x Member, now time.Time) {
	t.Helper()
	mustApply(t, tb, m(x.Node, x.ID, x.Incarnation, Alive), now)
	if x.State != Alive {
		mustApply(t, tb, x, now)
	}
	mustGet(t, tb, x)
}

// TestMemberState checks the names and the up states of MemberState.
func TestMemberState(t *testing.T) {
	for _, c := range []struct {
		s    MemberState
		name string
		up   bool
	}{
		{0, "MemberState(0)", false},
		{Alive, "alive", true},
		{Suspect, "suspect", true},
		{Dead, "dead", false},
		{Left, "left", false},
		{5, "MemberState(5)", false},
		{255, "MemberState(255)", false},
	} {
		if got := c.s.String(); got != c.name {
			t.Errorf("MemberState(%d).String() = %q, want %q", uint8(c.s), got, c.name)
		}
		if got := c.s.Up(); got != c.up {
			t.Errorf("MemberState(%d).Up() = %v, want %v", uint8(c.s), got, c.up)
		}
	}
}

// TestViewMemberAndUp checks View.Member's lookup and View.Up's new, sorted slice of up members.
func TestViewMemberAndUp(t *testing.T) {
	v := &View{
		Version: 3,
		Self:    m("c", 3, 0, Alive),
		Members: []Member{m("a", 1, 2, Dead), m("b", 2, 0, Suspect), m("c", 3, 0, Alive), m("d", 4, 1, Left), m("e", 5, 7, Alive)},
	}
	for _, want := range v.Members {
		if got, ok := v.Member(want.Node); !ok || got != want {
			t.Errorf("Member(%q) = %+v, %v, want %+v", want.Node, got, ok, want)
		}
	}
	for _, name := range []string{"", "0", "bb", "f", "C"} {
		if got, ok := v.Member(name); ok || got != (Member{}) {
			t.Errorf("Member(%q) = %+v, %v, want none", name, got, ok)
		}
	}
	up := v.Up()
	want := []Member{m("b", 2, 0, Suspect), m("c", 3, 0, Alive), m("e", 5, 7, Alive)}
	if !slices.Equal(up, want) {
		t.Fatalf("Up() = %+v, want %+v", up, want)
	}
	up[0].State = Dead
	if v.Members[1].State != Suspect {
		t.Fatalf("Up() shares its slice with the view")
	}
	if got := (&View{}).Up(); len(got) != 0 {
		t.Fatalf("empty Up() = %+v", got)
	}
	if _, ok := (&View{}).Member("a"); ok {
		t.Fatalf("empty Member found an entry")
	}
}

// TestATCLU08Precedence checks CLU-110 for every pair of states and every relation of the
// incarnations, on an entry and on a tombstone: a winning update replaces state and incarnation, a
// losing one changes nothing, and nothing overrides Left.
func TestATCLU08Precedence(t *testing.T) {
	// The CLU-110 table, row by row: when an update in state u beats an entry in state e.
	const (
		never = iota
		higher
		atLeast
		always
	)
	rule := map[[2]MemberState]int{
		{Left, Alive}: never, {Left, Suspect}: never, {Left, Dead}: never, {Left, Left}: never,
		{Alive, Left}: always, {Suspect, Left}: always, {Dead, Left}: always,
		{Alive, Alive}: higher, {Alive, Suspect}: atLeast,
		{Alive, Dead}: atLeast, {Suspect, Dead}: atLeast,
		{Suspect, Alive}: higher, {Suspect, Suspect}: higher,
		{Dead, Alive}: higher, {Dead, Suspect}: higher, {Dead, Dead}: higher,
	}
	const entryInc = 5
	relations := []struct {
		name string
		inc  uint64
	}{{"lower", entryInc - 1}, {"equal", entryInc}, {"higher", entryInc + 1}, {"zero", 0}, {"max", ^uint64(0)}}
	expect := func(e, u MemberState, inc uint64) bool {
		switch rule[[2]MemberState{e, u}] {
		case always:
			return true
		case higher:
			return inc > entryInc
		case atLeast:
			return inc >= entryInc
		default:
			return false
		}
	}
	for _, e := range allStates {
		for _, u := range allStates {
			for _, rel := range relations {
				name := fmt.Sprintf("%s by %s %s", e, u, rel.name)
				want := expect(e, u, rel.inc)
				entry := m("x", 7, entryInc, e)
				upd := m("x", 7, rel.inc, u)
				t.Run(name, func(t *testing.T) {
					if got := wins(entry, upd); got != want {
						t.Fatalf("wins(%+v, %+v) = %v, want %v", entry, upd, got, want)
					}

					// On an entry.
					tb := newTable(selfEntry, retention, t0)
					enter(t, tb, entry, t0)
					now := t0.Add(time.Second)
					if !want {
						mustIgnore(t, tb, upd, now)
						mustGet(t, tb, entry)
						return
					}
					c := mustApply(t, tb, upd, now)
					wantC := change{Node: "x", ID: 7, Inc: rel.inc, From: e, To: u, Source: "test"}
					if c != wantC {
						t.Fatalf("change = %+v, want %+v", c, wantC)
					}
					if c.member() != upd {
						t.Fatalf("change.member() = %+v, want %+v", c.member(), upd)
					}
					mustGet(t, tb, upd)
					if i, _ := tb.find("x"); !tb.recs[i].changed.Equal(now) {
						t.Fatalf("changed = %v, want %v", tb.recs[i].changed, now)
					}
				})
				if e == Alive || e == Suspect {
					continue // only Dead and Left entries become tombstones
				}
				t.Run(name+" on a tombstone", func(t *testing.T) {
					tb := newTable(selfEntry, retention, t0)
					enter(t, tb, entry, t0)
					if !tb.Reap("x", 7, entryInc, e, t0.Add(retention)) {
						t.Fatalf("Reap failed")
					}
					now := t0.Add(2 * retention)
					if !want {
						mustIgnore(t, tb, upd, now)
						if b, tomb, ok := tb.Belief("x"); !ok || !tomb || b != entry {
							t.Fatalf("Belief = %+v, %v, %v, want the tombstone %+v", b, tomb, ok, entry)
						}
						return
					}
					c := mustApply(t, tb, upd, now)
					wantC := change{Node: "x", ID: 7, Inc: rel.inc, From: e, To: u, Source: "test"}
					if c != wantC {
						t.Fatalf("change = %+v, want %+v", c, wantC)
					}
					mustGet(t, tb, upd)
					if tb.tombs != 0 || tb.Len() != 2 {
						t.Fatalf("restore left tombs=%d len=%d", tb.tombs, tb.Len())
					}
					if i, _ := tb.find("x"); !tb.recs[i].changed.Equal(now) {
						t.Fatalf("restore left changed = %v, want %v", tb.recs[i].changed, now)
					}
				})
			}
		}
	}
}

// TestATCLU09Identity checks the identity rules of the member table: additions (CLU-120),
// supersede (CLU-130), tombstones and restore (CLU-140), the caps (CLU-150), reaping (CLU-160) and
// updates about the own name.
func TestATCLU09Identity(t *testing.T) {
	t.Run("new table", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		if tb.Self() != selfEntry || tb.Len() != 1 || !slices.Equal(tb.Members(), []Member{selfEntry}) {
			t.Fatalf("new table: self %+v, len %d, members %+v", tb.Self(), tb.Len(), tb.Members())
		}
		mustGet(t, tb, selfEntry)
	})

	t.Run("additions", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		for _, u := range []Member{m("c", 3, 0, Alive), m("a", 1, 4, Suspect), m("z", 9, 2, Alive), m("b", 2, 0, Alive)} {
			c := mustApply(t, tb, u, t0)
			if want := (change{Node: u.Node, ID: u.ID, Inc: u.Incarnation, To: u.State, Source: "test"}); c != want {
				t.Fatalf("addition change = %+v, want %+v", c, want)
			}
		}
		// Down states and invalid updates about unknown names add nothing.
		for _, u := range []Member{m("d", 4, 0, Dead), m("e", 5, 9, Left), m("", 1, 0, Alive), m("f", 6, 0, 0), m("g", 7, 0, 5)} {
			mustIgnore(t, tb, u, t0)
		}
		want := []Member{m("a", 1, 4, Suspect), m("b", 2, 0, Alive), m("c", 3, 0, Alive), selfEntry, m("z", 9, 2, Alive)}
		if got := tb.Members(); !slices.Equal(got, want) {
			t.Fatalf("Members() = %+v, want %+v", got, want)
		}
		if tb.Len() != 5 {
			t.Fatalf("Len() = %d, want 5", tb.Len())
		}
		if _, ok := tb.Get("d"); ok {
			t.Fatalf("Get(d) found an entry")
		}
		// Members returns a new slice.
		tb.Members()[0].State = Dead
		mustGet(t, tb, m("a", 1, 4, Suspect))
		// An invalid update about a known name changes nothing either.
		mustIgnore(t, tb, m("a", 1, 9, 0), t0)
		mustIgnore(t, tb, m("a", 1, 9, 7), t0)
	})

	t.Run("supersede", func(t *testing.T) {
		for _, old := range allStates {
			for _, nu := range []MemberState{Alive, Suspect} {
				t.Run(fmt.Sprintf("%s by %s", old, nu), func(t *testing.T) {
					tb := newTable(selfEntry, retention, t0)
					enter(t, tb, m("x", 1, 5, old), t0)
					// IDs are not ordered: a smaller ID with a smaller incarnation supersedes too.
					c := mustApply(t, tb, m("x", 0, 0, nu), t0.Add(time.Second))
					want := change{Node: "x", ID: 0, Inc: 0, From: old, To: nu, OldID: 1, Superseded: true, Source: "test"}
					if c != want {
						t.Fatalf("change = %+v, want %+v", c, want)
					}
					mustGet(t, tb, m("x", 0, 0, nu))
					if got := retiredIDs(tb, "x"); !slices.Equal(got, []uint32{1}) {
						t.Fatalf("retired = %v, want [1]", got)
					}
					// The retired ID is ignored in every state, at any incarnation.
					for _, s := range allStates {
						mustIgnore(t, tb, m("x", 1, 99, s), t0.Add(2*time.Second))
					}
				})
			}
		}
		t.Run("down update with another ID", func(t *testing.T) {
			tb := newTable(selfEntry, retention, t0)
			enter(t, tb, m("x", 1, 5, Alive), t0)
			mustIgnore(t, tb, m("x", 2, 9, Dead), t0)
			mustIgnore(t, tb, m("x", 2, 9, Left), t0)
			if got := retiredIDs(tb, "x"); len(got) != 0 {
				t.Fatalf("retired = %v, want none", got)
			}
		})
	})

	t.Run("retired IDs cap", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		enter(t, tb, m("x", 1, 0, Alive), t0)
		for id := uint32(2); id <= maxRetired+2; id++ {
			mustApply(t, tb, m("x", id, 0, Alive), t0)
		}
		// IDs 1 to 17 were retired; the oldest, 1, was evicted.
		want := make([]uint32, 0, maxRetired)
		for id := uint32(2); id <= maxRetired+1; id++ {
			want = append(want, id)
		}
		if got := retiredIDs(tb, "x"); !slices.Equal(got, want) {
			t.Fatalf("retired = %v, want %v", got, want)
		}
		mustIgnore(t, tb, m("x", 2, 0, Alive), t0)
		c := mustApply(t, tb, m("x", 1, 0, Alive), t0)
		if !c.Superseded || c.OldID != maxRetired+2 {
			t.Fatalf("change = %+v, want a supersede of ID %d", c, maxRetired+2)
		}
		// Now 18 was retired and 2 evicted.
		if got := retiredIDs(tb, "x"); got[0] != 3 || got[len(got)-1] != maxRetired+2 || len(got) != maxRetired {
			t.Fatalf("retired = %v", got)
		}
	})

	t.Run("own name", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		for _, s := range allStates {
			for _, inc := range []uint64{0, 1, 99} {
				u := m("self", selfEntry.ID, inc, s)
				if c, res := tb.Apply(u, "test", t0); res != applyAboutSelf || c != (change{}) {
					t.Fatalf("Apply(%+v) = %+v, %v, want applyAboutSelf", u, c, res)
				}
			}
		}
		if tb.Self() != selfEntry {
			t.Fatalf("Apply changed the own entry: %+v", tb.Self())
		}
		// Another ID of the own name is ignored, and retired only when up; retiring it again
		// changes nothing.
		mustIgnore(t, tb, m("self", 7, 9, Dead), t0)
		mustIgnore(t, tb, m("self", 7, 9, Left), t0)
		if got := retiredIDs(tb, "self"); len(got) != 0 {
			t.Fatalf("retired = %v, want none", got)
		}
		mustRetireOnly(t, tb, m("self", 7, 9, Suspect), t0)
		mustRetireOnly(t, tb, m("self", 8, 0, Alive), t0)
		mustIgnore(t, tb, m("self", 7, 10, Alive), t0) // already retired: nothing changes
		if got := retiredIDs(tb, "self"); !slices.Equal(got, []uint32{7, 8}) {
			t.Fatalf("retired = %v, want [7 8]", got)
		}
		if tb.Self() != selfEntry {
			t.Fatalf("Apply changed the own entry: %+v", tb.Self())
		}

		// The own entry changes only through SetSelf.
		now := t0.Add(time.Second)
		c, ok := tb.SetSelf(Alive, 3, now)
		want := change{Node: "self", ID: selfEntry.ID, Inc: 3, From: Alive, To: Alive, Source: "self"}
		if !ok || c != want {
			t.Fatalf("SetSelf = %+v, %v, want %+v", c, ok, want)
		}
		if c, ok := tb.SetSelf(Alive, 3, now); ok || c != (change{}) {
			t.Fatalf("SetSelf without a change = %+v, %v", c, ok)
		}
		c, ok = tb.SetSelf(Left, 3, now)
		want = change{Node: "self", ID: selfEntry.ID, Inc: 3, From: Alive, To: Left, Source: "self"}
		if !ok || c != want {
			t.Fatalf("SetSelf = %+v, %v, want %+v", c, ok, want)
		}
		mustGet(t, tb, m("self", selfEntry.ID, 3, Left))
		// The own entry is never reaped.
		if tb.Reap("self", selfEntry.ID, 3, Left, now.Add(10*retention)) {
			t.Fatalf("Reap reaped the own entry")
		}
		mustGet(t, tb, m("self", selfEntry.ID, 3, Left))
	})

	t.Run("reaping", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		enter(t, tb, m("x", 1, 2, Dead), t0)
		enter(t, tb, m("y", 2, 0, Left), t0)
		enter(t, tb, m("z", 3, 0, Suspect), t0)
		at := t0.Add(retention)
		for _, c := range []struct {
			name  string
			id    uint32
			inc   uint64
			state MemberState
			now   time.Time
		}{
			{"x", 1, 2, Dead, at.Add(-time.Nanosecond)}, // too early
			{"x", 9, 2, Dead, at},                       // another ID
			{"x", 1, 3, Dead, at},                       // another incarnation
			{"x", 1, 2, Left, at},                       // another state
			{"z", 3, 0, Suspect, at},                    // up
			{"z", 3, 0, Dead, at},                       // not its state
			{"w", 1, 0, Dead, at},                       // unknown
		} {
			if tb.Reap(c.name, c.id, c.inc, c.state, c.now) {
				t.Fatalf("Reap(%s, %d, %d, %s, %v) reaped", c.name, c.id, c.inc, c.state, c.now.Sub(t0))
			}
		}
		if tb.Len() != 4 || tb.tombs != 0 {
			t.Fatalf("len %d tombs %d", tb.Len(), tb.tombs)
		}
		if !tb.Reap("x", 1, 2, Dead, at) || !tb.Reap("y", 2, 0, Left, at.Add(time.Hour)) {
			t.Fatalf("Reap did not reap")
		}
		if tb.Reap("x", 1, 2, Dead, at) {
			t.Fatalf("Reap reaped a tombstone")
		}
		if _, ok := tb.Get("x"); ok {
			t.Fatalf("Get(x) found a reaped entry")
		}
		want := []Member{selfEntry, m("z", 3, 0, Suspect)}
		if got := tb.Members(); !slices.Equal(got, want) || tb.Len() != 2 || tb.tombs != 2 {
			t.Fatalf("Members() = %+v (len %d, tombs %d), want %+v", got, tb.Len(), tb.tombs, want)
		}

		// A change restarts the retention: the timer of the old change does nothing.
		tb = newTable(selfEntry, retention, t0)
		enter(t, tb, m("x", 1, 2, Dead), t0)
		t1 := t0.Add(retention / 2)
		mustApply(t, tb, m("x", 1, 3, Dead), t1)
		if tb.Reap("x", 1, 2, Dead, t0.Add(retention)) || tb.Reap("x", 1, 3, Dead, t0.Add(retention)) {
			t.Fatalf("Reap reaped a changed entry before its retention")
		}
		if !tb.Reap("x", 1, 3, Dead, t1.Add(retention)) {
			t.Fatalf("Reap did not reap after the new retention")
		}
	})

	t.Run("tombstones", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		enter(t, tb, m("x", 1, 2, Dead), t0)
		if !tb.Reap("x", 1, 2, Dead, t0.Add(retention)) {
			t.Fatalf("Reap failed")
		}
		now := t0.Add(2 * retention)
		// A down update with another ID is ignored; the tombstone stays.
		mustIgnore(t, tb, m("x", 2, 9, Dead), now)
		mustIgnore(t, tb, m("x", 2, 9, Left), now)
		// An up update with another ID adds an entry and retires the tombstone's ID.
		c := mustApply(t, tb, m("x", 2, 0, Suspect), now)
		if want := (change{Node: "x", ID: 2, To: Suspect, Source: "test"}); c != want {
			t.Fatalf("change = %+v, want %+v", c, want)
		}
		mustGet(t, tb, m("x", 2, 0, Suspect))
		if b, tomb, ok := tb.Belief("x"); !ok || tomb || b != m("x", 2, 0, Suspect) {
			t.Fatalf("Belief = %+v, %v, %v", b, tomb, ok)
		}
		if tb.tombs != 0 || !slices.Equal(retiredIDs(tb, "x"), []uint32{1}) {
			t.Fatalf("tombs %d retired %v", tb.tombs, retiredIDs(tb, "x"))
		}
		mustIgnore(t, tb, m("x", 1, 99, Alive), now)

		// A retired ID does not restore a tombstone either.
		mustApply(t, tb, m("x", 2, 0, Left), now)
		if !tb.Reap("x", 2, 0, Left, now.Add(retention)) {
			t.Fatalf("Reap failed")
		}
		mustIgnore(t, tb, m("x", 1, 99, Alive), now.Add(retention))
		if b, tomb, ok := tb.Belief("x"); !ok || !tomb || b != m("x", 2, 0, Left) {
			t.Fatalf("Belief = %+v, %v, %v", b, tomb, ok)
		}
	})

	t.Run("tombstone cap", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		name := func(i int) string { return fmt.Sprintf("n%05d", i) }
		// The first node gets a retired ID, which goes with its tombstone.
		enter(t, tb, m(name(0), 1, 0, Alive), t0)
		enter(t, tb, m(name(0), 2, 0, Dead), t0)
		for i := 1; i <= maxTombstones; i++ {
			enter(t, tb, m(name(i), 2, 0, Dead), t0)
		}
		// Reap in reverse name order, so the oldest tombstone is not the first record.
		for i := maxTombstones; i >= 0; i-- {
			at := t0.Add(retention + time.Duration(maxTombstones-i)*time.Millisecond)
			if !tb.Reap(name(i), 2, 0, Dead, at) {
				t.Fatalf("Reap(%s) failed", name(i))
			}
		}
		// The first reaped, name(maxTombstones), was evicted with everything known about it.
		if tb.tombs != maxTombstones || len(tb.recs) != maxTombstones+1 || tb.Len() != 1 {
			t.Fatalf("tombs %d recs %d len %d", tb.tombs, len(tb.recs), tb.Len())
		}
		evicted := name(maxTombstones)
		if _, _, ok := tb.Belief(evicted); ok {
			t.Fatalf("Belief(%s) found the evicted tombstone", evicted)
		}
		for i := range maxTombstones {
			if b, tomb, ok := tb.Belief(name(i)); !ok || !tomb || b != m(name(i), 2, 0, Dead) {
				t.Fatalf("Belief(%s) = %+v, %v, %v", name(i), b, tomb, ok)
			}
		}
		// The table forgot the evicted name: its old ID is added again.
		mustApply(t, tb, m(evicted, 2, 0, Alive), t0.Add(2*retention))
		// One more tombstone evicts the oldest remaining one, name(maxTombstones-1), and then the
		// name(0) tombstone stays with its retired ID.
		mustApply(t, tb, m(evicted, 2, 0, Dead), t0.Add(2*retention))
		if !tb.Reap(evicted, 2, 0, Dead, t0.Add(3*retention)) {
			t.Fatalf("Reap failed")
		}
		if _, _, ok := tb.Belief(name(maxTombstones - 1)); ok || tb.tombs != maxTombstones {
			t.Fatalf("the oldest tombstone was not evicted (tombs %d)", tb.tombs)
		}
		if !slices.Equal(retiredIDs(tb, name(0)), []uint32{1}) {
			t.Fatalf("retired(%s) = %v", name(0), retiredIDs(tb, name(0)))
		}
		mustIgnore(t, tb, m(name(0), 1, 9, Alive), t0.Add(3*retention))

		// Evicting name(0) drops its retired ID: ID 1 is accepted again.
		tb2 := newTable(selfEntry, retention, t0)
		enter(t, tb2, m("a", 1, 0, Alive), t0)
		enter(t, tb2, m("a", 2, 0, Dead), t0)
		if !tb2.Reap("a", 2, 0, Dead, t0.Add(retention)) {
			t.Fatalf("Reap failed")
		}
		for i := 1; i <= maxTombstones; i++ {
			enter(t, tb2, m(name(i), 2, 0, Dead), t0)
			if !tb2.Reap(name(i), 2, 0, Dead, t0.Add(retention)) {
				t.Fatalf("Reap(%s) failed", name(i))
			}
		}
		if _, _, ok := tb2.Belief("a"); ok {
			t.Fatalf("the oldest tombstone, a, was not evicted")
		}
		mustApply(t, tb2, m("a", 1, 0, Alive), t0.Add(2*retention))
	})
}

// TestATCLU10ForcedBelief checks the table's part of the forced update: Belief returns the entry,
// or else the tombstone, or nothing for an unknown name.
func TestATCLU10ForcedBelief(t *testing.T) {
	tb := newTable(selfEntry, retention, t0)
	if b, tomb, ok := tb.Belief("x"); ok || tomb || b != (Member{}) {
		t.Fatalf("Belief(unknown) = %+v, %v, %v", b, tomb, ok)
	}
	if b, tomb, ok := tb.Belief("self"); !ok || tomb || b != selfEntry {
		t.Fatalf("Belief(self) = %+v, %v, %v", b, tomb, ok)
	}
	for _, s := range allStates {
		x := m("x", 1, 4, s)
		tb := newTable(selfEntry, retention, t0)
		enter(t, tb, x, t0)
		if b, tomb, ok := tb.Belief("x"); !ok || tomb || b != x {
			t.Fatalf("Belief(%s entry) = %+v, %v, %v, want the entry", s, b, tomb, ok)
		}
		if s.Up() {
			continue
		}
		if !tb.Reap("x", 1, 4, s, t0.Add(retention)) {
			t.Fatalf("Reap failed")
		}
		if b, tomb, ok := tb.Belief("x"); !ok || !tomb || b != x {
			t.Fatalf("Belief(%s tombstone) = %+v, %v, %v, want the tombstone", s, b, tomb, ok)
		}
		if s == Left {
			continue // nothing restores a Left tombstone
		}
		// A restored tombstone is an entry again.
		mustApply(t, tb, m("x", 1, 5, Suspect), t0.Add(2*retention))
		if b, tomb, ok := tb.Belief("x"); !ok || tomb || b != m("x", 1, 5, Suspect) {
			t.Fatalf("Belief(restored) = %+v, %v, %v, want the entry", b, tomb, ok)
		}
	}
}
