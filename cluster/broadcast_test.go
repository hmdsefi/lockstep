// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"slices"
	"testing"
)

func upd(node string, inc uint64) update {
	return update{Node: node, ID: 1, Inc: inc, State: Alive}
}

// nodes returns the node names of us, in order.
func nodes(us []update) []string {
	out := make([]string, len(us))
	for i, u := range us {
		out[i] = u.Node
	}
	return out
}

func wantNodes(t *testing.T, got []update, want ...string) {
	t.Helper()
	if g := nodes(got); !slices.Equal(g, want) && (len(g) != 0 || len(want) != 0) {
		t.Fatalf("took %v, want %v", g, want)
	}
}

// TestATCLU10ForcedUpdateFirst checks the queue's part of the forced update (CLU-400): the table
// decides it from Belief, it comes first, it does not count against the maximum, and a queued
// update about the recipient is the forced one.
func TestATCLU10ForcedUpdateFirst(t *testing.T) {
	t.Run("belief decides", func(t *testing.T) {
		tb := newTable(selfEntry, retention, t0)
		if forcedUpdate(tb, "x") != nil {
			t.Fatal("forced update for an unknown node")
		}
		if forcedUpdate(tb, "self") != nil {
			t.Fatal("forced update for an alive node")
		}
		for _, tc := range []struct {
			state  MemberState
			forced bool
		}{{Alive, false}, {Suspect, true}, {Dead, true}, {Left, false}} {
			tb := newTable(selfEntry, retention, t0)
			x := m("x", 1, 4, tc.state)
			enter(t, tb, x, t0)
			got := forcedUpdate(tb, "x")
			if (got != nil) != tc.forced {
				t.Fatalf("%s entry: forced = %v, want %v", tc.state, got, tc.forced)
			}
			if got != nil && *got != toUpdate(x) {
				t.Fatalf("%s entry: forced = %+v, want %+v", tc.state, *got, toUpdate(x))
			}
		}
		// Every tombstone is forced, a Left one included.
		for _, s := range []MemberState{Dead, Left} {
			tb := newTable(selfEntry, retention, t0)
			x := m("x", 1, 4, s)
			enter(t, tb, x, t0)
			if !tb.Reap("x", 1, 4, s, t0.Add(retention)) {
				t.Fatal("Reap failed")
			}
			if got := forcedUpdate(tb, "x"); got == nil || *got != toUpdate(x) {
				t.Fatalf("%s tombstone: forced = %v, want %+v", s, got, toUpdate(x))
			}
		}
	})

	t.Run("first and not counted", func(t *testing.T) {
		b := newBroadcasts()
		b.enqueue(upd("a", 1))
		b.enqueue(upd("b", 1))
		f := update{Node: "z", ID: 9, Inc: 3, State: Dead}
		got := b.take(&f, 1, 100)
		wantNodes(t, got, "z", "b") // max 1 applies to the queued ones only
		if got[0] != f {
			t.Fatalf("forced = %+v, want %+v", got[0], f)
		}
		if b.len() != 2 {
			t.Fatalf("len = %d, the forced update must not be queued", b.len())
		}
		// With a limit of 1 the forced update does not count, and the taken one is removed.
		b = newBroadcasts()
		b.enqueue(upd("a", 1))
		wantNodes(t, b.take(&f, 5, 1), "z", "a")
		if b.len() != 0 {
			t.Fatalf("len = %d, want 0", b.len())
		}
	})

	t.Run("a queued update about the recipient is the forced one", func(t *testing.T) {
		b := newBroadcasts()
		b.enqueue(upd("z", 7)) // older and newer entries around it
		b.enqueue(upd("a", 1))
		b.enqueue(upd("b", 1))
		f := update{Node: "z", ID: 9, Inc: 3, State: Dead}
		got := b.take(&f, 8, 100)
		wantNodes(t, got, "z", "b", "a")
		if got[0].Inc != 7 {
			t.Fatalf("forced = %+v, want the queued update", got[0])
		}
		if i, _ := b.find("z"); b.items[i].count != 1 {
			t.Fatalf("count of the forced queued update = %d, want 1", b.items[i].count)
		}
		// It counts as a transmission and is removed at the limit, without being repeated.
		got = b.take(&f, 8, 2)
		wantNodes(t, got, "z", "b", "a")
		if _, ok := b.find("z"); ok {
			t.Fatal("the forced queued update stays after reaching the limit")
		}
		// Now nothing is queued about z, so the table's belief is sent.
		wantNodes(t, b.take(&f, 8, 100), "z")
	})

	t.Run("no forced update", func(t *testing.T) {
		b := newBroadcasts()
		b.enqueue(upd("a", 1))
		wantNodes(t, b.take(nil, 8, 100), "a")
	})
}

// TestATCLU12Piggyback checks the queue (CLU-400): one update per name, replacing resets the
// count, selection by count ascending and newest first, the maximum and the removal limit.
func TestATCLU12Piggyback(t *testing.T) {
	t.Run("one per name and replace resets the count", func(t *testing.T) {
		b := newBroadcasts()
		b.enqueue(upd("a", 1))
		b.enqueue(upd("b", 1))
		b.take(nil, 8, 100)
		b.take(nil, 8, 100)
		b.enqueue(upd("a", 2))
		if b.len() != 2 {
			t.Fatalf("len = %d, want 2", b.len())
		}
		i, _ := b.find("a")
		if b.items[i].count != 0 || b.items[i].u.Inc != 2 {
			t.Fatalf("replaced = %+v, want count 0 and the new update", b.items[i])
		}
		// a has count 0, b has count 2.
		wantNodes(t, b.take(nil, 1, 100), "a")
	})

	t.Run("count ascending, ties newest first", func(t *testing.T) {
		b := newBroadcasts()
		for _, n := range []string{"d", "a", "c", "b"} {
			b.enqueue(upd(n, 1))
		}
		wantNodes(t, b.take(nil, 8, 100), "b", "c", "a", "d") // newest enqueue first
		b.enqueue(upd("e", 1))                                // count 0 beats count 1
		wantNodes(t, b.take(nil, 2, 100), "e", "b")
		// e and b now have 1 and 2; counts are a:1 c:1 d:1 e:1 b:2. Ties by enqueue age.
		wantNodes(t, b.take(nil, 8, 100), "e", "c", "a", "d", "b")
		// Re-enqueueing moves a name to the newest position.
		b = newBroadcasts()
		b.enqueue(upd("a", 1))
		b.enqueue(upd("b", 1))
		b.enqueue(upd("a", 2))
		wantNodes(t, b.take(nil, 8, 100), "a", "b")
	})

	t.Run("maximum", func(t *testing.T) {
		b := newBroadcasts()
		for i := range 10 {
			b.enqueue(upd(fmt.Sprintf("n%02d", i), 1))
		}
		got := b.take(nil, 3, 100)
		wantNodes(t, got, "n09", "n08", "n07")
		for _, max := range []int{0, -1} {
			wantNodes(t, b.take(nil, max, 100))
		}
		// Only the three taken were counted.
		for i, q := range b.items {
			want := 0
			if i >= 7 {
				want = 1
			}
			if q.count != want {
				t.Fatalf("%s count = %d, want %d", q.u.Node, q.count, want)
			}
		}
	})

	t.Run("removal at the limit", func(t *testing.T) {
		for n := 1; n <= 9; n++ {
			limit := retransmitLimit(DefaultRetransmitMult, n)
			b := newBroadcasts()
			b.enqueue(upd("a", 1))
			for i := 1; i <= limit; i++ {
				if b.len() != 1 {
					t.Fatalf("n=%d: removed before transmission %d of %d", n, i, limit)
				}
				wantNodes(t, b.take(nil, 8, limit), "a")
			}
			if b.len() != 0 {
				t.Fatalf("n=%d: still queued after %d transmissions", n, limit)
			}
		}
	})

	t.Run("retransmit limit", func(t *testing.T) {
		for _, tc := range []struct{ mult, n, want int }{
			{4, 1, 4}, {4, 2, 8}, {4, 3, 8}, {4, 4, 12}, {4, 7, 12}, {4, 8, 16}, {3, 100, 21}, {1, 0, 0},
		} {
			if got := retransmitLimit(tc.mult, tc.n); got != tc.want {
				t.Errorf("retransmitLimit(%d, %d) = %d, want %d", tc.mult, tc.n, got, tc.want)
			}
		}
	})

	t.Run("limit not above zero removes only the taken", func(t *testing.T) {
		for _, limit := range []int{0, -1} {
			b := newBroadcasts()
			for _, n := range []string{"a", "b", "c", "d", "e"} {
				b.enqueue(upd(n, 1))
			}
			wantNodes(t, b.take(nil, 1, limit), "e")
			if b.len() != 4 {
				t.Fatalf("limit %d: len = %d, want 4", limit, b.len())
			}
			if _, ok := b.find("e"); ok {
				t.Fatalf("limit %d: the taken update stays", limit)
			}
		}
	})

	t.Run("empty", func(t *testing.T) {
		b := newBroadcasts()
		wantNodes(t, b.take(nil, 8, 4))
		if b.len() != 0 {
			t.Fatal("len != 0")
		}
	})
}
