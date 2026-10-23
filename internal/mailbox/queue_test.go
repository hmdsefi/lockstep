// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package mailbox

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// drain pops every entry of q, in order.
func drain(q *Queue[int]) []Entry[int] {
	var got []Entry[int]
	for {
		v, kind, ok := q.Pop()
		if !ok {
			return got
		}
		got = append(got, Entry[int]{V: v, Kind: kind})
	}
}

// offerAll offers each entry with forceFull false and returns the outcomes.
func offerAll(q *Queue[int], entries ...Entry[int]) []Outcome {
	outs := make([]Outcome, 0, len(entries))
	for _, e := range entries {
		out, _ := q.Offer(e.V, e.Kind, false)
		outs = append(outs, out)
	}
	return outs
}

func TestATMBX04SelfEntriesBypassCapacity(t *testing.T) {
	q := New[int](2, FailSend)
	got := offerAll(q,
		Entry[int]{1, Sent}, Entry[int]{2, Sent}, Entry[int]{3, Self}, Entry[int]{4, Self}, Entry[int]{5, Sent})
	want := []Outcome{Enqueued, Enqueued, Enqueued, Enqueued, Rejected}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("outcomes = %v, want %v", got, want)
	}
	if q.Len() != 4 || q.SentLen() != 2 {
		t.Fatalf("Len, SentLen = %d, %d, want 4, 2", q.Len(), q.SentLen())
	}
	wantEntries := []Entry[int]{{1, Sent}, {2, Sent}, {3, Self}, {4, Self}}
	if e := drain(q); !reflect.DeepEqual(e, wantEntries) {
		t.Fatalf("pops = %v, want %v", e, wantEntries)
	}
	if q.Len() != 0 || q.SentLen() != 0 {
		t.Fatalf("after drain Len, SentLen = %d, %d, want 0, 0", q.Len(), q.SentLen())
	}
}

func TestSelfEntryIgnoresForceFullAndPolicy(t *testing.T) {
	for _, p := range []Policy{FailSend, DropNewest, DropOldest, BlockSender} {
		t.Run(fmt.Sprint(p), func(t *testing.T) {
			q := New[int](1, p)
			if out, _ := q.Offer(1, Sent, false); out != Enqueued {
				t.Fatalf("Sent offer = %v, want enqueued", out)
			}
			out, dropped := q.Offer(2, Self, true)
			if out != Enqueued || dropped != 0 {
				t.Fatalf("Self offer = %v, %d, want enqueued, 0", out, dropped)
			}
			if q.SentLen() != 1 || q.Len() != 2 {
				t.Fatalf("SentLen, Len = %d, %d, want 1, 2", q.SentLen(), q.Len())
			}
		})
	}
}

func TestATMBX05Room(t *testing.T) {
	q := New[int](3, FailSend)
	if got := offerAll(q, Entry[int]{1, Sent}, Entry[int]{2, Sent}); got[0] != Enqueued || got[1] != Enqueued {
		t.Fatalf("outcomes = %v, want two enqueued", got)
	}
	if q.SentLen() != 2 {
		t.Fatalf("SentLen = %d, want 2", q.SentLen())
	}
	if out, _ := q.Offer(3, Sent, true); out != Rejected {
		t.Fatalf("forceFull offer = %v, want rejected", out)
	}
	if q.SentLen() != 2 {
		t.Fatalf("SentLen after forced refusal = %d, want 2", q.SentLen())
	}
}

func TestATMBX06FullPolicies(t *testing.T) {
	prime := func(p Policy) *Queue[int] {
		q := New[int](2, p)
		offerAll(q, Entry[int]{1, Sent}, Entry[int]{9, Self}, Entry[int]{2, Sent})
		return q
	}
	cases := []struct {
		policy      Policy
		out         Outcome
		dropped     int
		queue       []Entry[int]
		wantSentLen int
	}{
		{FailSend, Rejected, 0, []Entry[int]{{1, Sent}, {9, Self}, {2, Sent}}, 2},
		{BlockSender, Rejected, 0, []Entry[int]{{1, Sent}, {9, Self}, {2, Sent}}, 2},
		{DropNewest, DroppedNewest, 0, []Entry[int]{{1, Sent}, {9, Self}, {2, Sent}}, 2},
		{DropOldest, DroppedOldest, 1, []Entry[int]{{9, Self}, {2, Sent}, {3, Sent}}, 2},
	}
	for _, c := range cases {
		t.Run(fmt.Sprint(c.policy), func(t *testing.T) {
			q := prime(c.policy)
			out, dropped := q.Offer(3, Sent, false)
			if out != c.out || dropped != c.dropped {
				t.Fatalf("Offer = %v, %d, want %v, %d", out, dropped, c.out, c.dropped)
			}
			if q.SentLen() != c.wantSentLen {
				t.Fatalf("SentLen = %d, want %d", q.SentLen(), c.wantSentLen)
			}
			if q.Len() != len(c.queue) {
				t.Fatalf("Len = %d, want %d", q.Len(), len(c.queue))
			}
			if e := drain(q); !reflect.DeepEqual(e, c.queue) {
				t.Fatalf("queue = %v, want %v", e, c.queue)
			}
		})
	}

	t.Run("DropOldest with no Sent entry", func(t *testing.T) {
		q := New[int](2, DropOldest)
		q.Offer(7, Self, false)
		out, dropped := q.Offer(3, Sent, true)
		if out != DroppedNewest || dropped != 0 {
			t.Fatalf("Offer = %v, %d, want dropped-newest, 0", out, dropped)
		}
		if e, want := drain(q), []Entry[int]{{7, Self}}; !reflect.DeepEqual(e, want) {
			t.Fatalf("queue = %v, want %v", e, want)
		}
	})

	t.Run("forceFull with room", func(t *testing.T) {
		q := New[int](4, DropOldest)
		offerAll(q, Entry[int]{1, Sent}, Entry[int]{2, Self})
		out, dropped := q.Offer(3, Sent, true)
		if out != DroppedOldest || dropped != 1 {
			t.Fatalf("Offer = %v, %d, want dropped-oldest, 1", out, dropped)
		}
		if e, want := drain(q), []Entry[int]{{2, Self}, {3, Sent}}; !reflect.DeepEqual(e, want) {
			t.Fatalf("queue = %v, want %v", e, want)
		}
	})
}

func TestATMBX07Close(t *testing.T) {
	q := New[int](4, FailSend)
	offerAll(q, Entry[int]{1, Sent}, Entry[int]{2, Self}, Entry[int]{3, Sent})
	got := q.Close()
	want := []Entry[int]{{1, Sent}, {2, Self}, {3, Sent}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Close = %v, want %v", got, want)
	}
	if out, _ := q.Offer(4, Sent, false); out != Closed {
		t.Fatalf("Sent offer after close = %v, want closed", out)
	}
	if out, _ := q.Offer(5, Self, false); out != Closed {
		t.Fatalf("Self offer after close = %v, want closed", out)
	}
	if q.Len() != 0 || q.SentLen() != 0 {
		t.Fatalf("Len, SentLen = %d, %d, want 0, 0", q.Len(), q.SentLen())
	}
	if _, _, ok := q.Pop(); ok {
		t.Fatal("Pop on a closed queue returned an entry")
	}
	if again := q.Close(); again != nil {
		t.Fatalf("second Close = %v, want nil", again)
	}

	s := NewSystemQueue[string]()
	if !s.Push("a") || !s.Push("b") {
		t.Fatal("Push on an open system queue returned false")
	}
	if got, want := s.Close(), []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("system Close = %v, want %v", got, want)
	}
	if s.Push("c") {
		t.Fatal("Push after Close returned true")
	}
	if s.Len() != 0 {
		t.Fatalf("system Len after Close = %d, want 0", s.Len())
	}
	if again := s.Close(); again != nil {
		t.Fatalf("second system Close = %v, want nil", again)
	}
}

func TestCloseEmptyQueueReturnsNoEntries(t *testing.T) {
	q := New[int](1, FailSend)
	if got := q.Close(); len(got) != 0 {
		t.Fatalf("Close of an empty queue = %v, want no entries", got)
	}
	if out, _ := q.Offer(1, Sent, false); out != Closed {
		t.Fatalf("Offer after close = %v, want closed", out)
	}
}

func TestNewPanics(t *testing.T) {
	cases := []struct {
		name     string
		capacity int
		policy   Policy
		want     string
	}{
		{"zero capacity", 0, FailSend, "mailbox: invalid capacity 0"},
		{"negative capacity", -3, DropNewest, "mailbox: invalid capacity -3"},
		{"unknown policy", 1, Policy(9), "mailbox: unknown policy 9"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("New did not panic")
				}
				if got := fmt.Sprint(r); got != c.want {
					t.Fatalf("panic = %q, want %q", got, c.want)
				}
			}()
			New[int](c.capacity, c.policy)
		})
	}
}

func TestAccessors(t *testing.T) {
	q := New[int](8, DropOldest)
	if q.Capacity() != 8 || q.Policy() != DropOldest {
		t.Fatalf("Capacity, Policy = %d, %v, want 8, DropOldest", q.Capacity(), q.Policy())
	}
	if DefaultCapacity != 1024 {
		t.Fatalf("DefaultCapacity = %d, want 1024", DefaultCapacity)
	}
}

func TestOutcomeString(t *testing.T) {
	cases := []struct {
		o    Outcome
		want string
	}{
		{Enqueued, "enqueued"},
		{Rejected, "rejected"},
		{DroppedNewest, "dropped-newest"},
		{DroppedOldest, "dropped-oldest"},
		{Closed, "closed"},
		{Outcome(9), "Outcome(9)"},
		{Outcome(0), "Outcome(0)"},
	}
	for _, c := range cases {
		if got := c.o.String(); got != c.want {
			t.Errorf("Outcome(%d).String() = %q, want %q", uint8(c.o), got, c.want)
		}
	}
}

// TestRingGrowsAndWraps adds three entries and pops two per round, so the queue grows slowly past
// its first ring while the head wraps around it.
func TestRingGrowsAndWraps(t *testing.T) {
	q := New[int](1000, FailSend)
	next, want := 0, []int(nil)
	for round := 0; round < 50; round++ {
		for i := 0; i < 3; i++ {
			next++
			kind := Sent
			if i == 2 {
				kind = Self
			}
			if out, _ := q.Offer(next, kind, false); out != Enqueued {
				t.Fatalf("round %d offer %d = %v, want enqueued", round, next, out)
			}
			want = append(want, next)
		}
		for i := 0; i < 2; i++ {
			v, _, ok := q.Pop()
			if !ok || v != want[0] {
				t.Fatalf("round %d pop = %d, %v, want %d", round, v, ok, want[0])
			}
			want = want[1:]
		}
	}
	if q.Len() != len(want) {
		t.Fatalf("Len = %d, want %d", q.Len(), len(want))
	}
	for _, e := range drain(q) {
		if e.V != want[0] {
			t.Fatalf("drain got %d, want %d", e.V, want[0])
		}
		want = want[1:]
	}
	if len(want) != 0 {
		t.Fatalf("entries lost: %v", want)
	}
}

// model is the obvious slice implementation the ring is checked against.
type model struct {
	capacity int
	policy   Policy
	entries  []Entry[int]
}

func (m *model) sentLen() int {
	n := 0
	for _, e := range m.entries {
		if e.Kind == Sent {
			n++
		}
	}
	return n
}

func (m *model) offer(v int, kind Kind, forceFull bool) (Outcome, int) {
	if kind == Self {
		m.entries = append(m.entries, Entry[int]{v, kind})
		return Enqueued, 0
	}
	if !forceFull && m.sentLen() < m.capacity {
		m.entries = append(m.entries, Entry[int]{v, kind})
		return Enqueued, 0
	}
	switch m.policy {
	case DropNewest:
		return DroppedNewest, 0
	case DropOldest:
		for i, e := range m.entries {
			if e.Kind == Sent {
				m.entries = append(m.entries[:i:i], m.entries[i+1:]...)
				m.entries = append(m.entries, Entry[int]{v, kind})
				return DroppedOldest, e.V
			}
		}
		return DroppedNewest, 0
	default:
		return Rejected, 0
	}
}

// TestQueueMatchesModel drives a DropOldest queue, which marks and compacts slots, with a fixed
// pseudo-random mix of operations and compares every result with the slice model.
func TestQueueMatchesModel(t *testing.T) {
	for _, capacity := range []int{1, 2, 5, 16} {
		for _, p := range []Policy{FailSend, DropNewest, DropOldest} {
			t.Run(fmt.Sprintf("cap%d/%v", capacity, p), func(t *testing.T) {
				q := New[int](capacity, p)
				m := &model{capacity: capacity, policy: p}
				state := uint32(2654435761)
				for range capacity*4 + int(p) + 1 {
					state = state*1664525 + 1013904223
				}
				rnd := func(n int) int {
					state = state*1664525 + 1013904223
					return int(state>>16) % n
				}
				for i := 1; i <= 4000; i++ {
					switch op := rnd(10); {
					case op < 6:
						kind := Sent
						if rnd(3) == 0 {
							kind = Self
						}
						force := rnd(8) == 0
						gotOut, gotDropped := q.Offer(i, kind, force)
						wantOut, wantDropped := m.offer(i, kind, force)
						if gotOut != wantOut || gotDropped != wantDropped {
							t.Fatalf("step %d Offer = %v, %d, want %v, %d", i, gotOut, gotDropped, wantOut, wantDropped)
						}
					default:
						v, kind, ok := q.Pop()
						if len(m.entries) == 0 {
							if ok {
								t.Fatalf("step %d Pop returned %d from an empty model", i, v)
							}
							break
						}
						want := m.entries[0]
						m.entries = m.entries[1:]
						if !ok || v != want.V || kind != want.Kind {
							t.Fatalf("step %d Pop = %d, %v, %v, want %v", i, v, kind, ok, want)
						}
					}
					if q.Len() != len(m.entries) || q.SentLen() != m.sentLen() {
						t.Fatalf("step %d Len, SentLen = %d, %d, want %d, %d",
							i, q.Len(), q.SentLen(), len(m.entries), m.sentLen())
					}
				}
				got := q.Close()
				if len(got) != len(m.entries) {
					t.Fatalf("Close returned %d entries, want %d", len(got), len(m.entries))
				}
				for i := range got {
					if got[i] != m.entries[i] {
						t.Fatalf("Close entry %d = %v, want %v", i, got[i], m.entries[i])
					}
				}
			})
		}
	}
}

// TestDropOldestCompacts checks that repeated drops do not let the ring grow without bound.
func TestDropOldestCompacts(t *testing.T) {
	q := New[int](4, DropOldest)
	for i := 0; i < 4; i++ {
		q.Offer(i, Sent, false)
	}
	for i := 4; i < 10000; i++ {
		if out, dropped := q.Offer(i, Sent, false); out != DroppedOldest || dropped != i-4 {
			t.Fatalf("offer %d = %v, %d, want dropped-oldest, %d", i, out, dropped, i-4)
		}
	}
	if q.Len() != 4 || q.SentLen() != 4 {
		t.Fatalf("Len, SentLen = %d, %d, want 4, 4", q.Len(), q.SentLen())
	}
	if n := len(q.ring); n > 32 {
		t.Fatalf("ring length = %d after repeated drops, want a small ring", n)
	}
	if e := drain(q); len(e) != 4 || e[0].V != 9996 || e[3].V != 9999 {
		t.Fatalf("queue = %v, want 9996..9999", e)
	}
}

// TestATMBX10NoGoroutines is the grep of AT-MBX-10 as a test: the non-test files of the package
// start no goroutine and use no select statement, channel send or channel receive.
func TestATMBX10NoGoroutines(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		ast.Inspect(f, func(n ast.Node) bool {
			var what string
			switch x := n.(type) {
			case *ast.GoStmt:
				what = "go statement"
			case *ast.SelectStmt:
				what = "select statement"
			case *ast.SendStmt:
				what = "channel send"
			case *ast.UnaryExpr:
				if x.Op == token.ARROW {
					what = "channel receive"
				}
			}
			if what != "" {
				t.Errorf("%s: %s", fset.Position(n.Pos()), what)
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no non-test files found")
	}
}

// TestConcurrentProducers has several goroutines offer while the test pops, then checks that each
// producer's entries came out in the order it offered them. It is for -race.
func TestConcurrentProducers(t *testing.T) {
	const producers, each = 4, 500
	q := New[int](producers*each, FailSend)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if out, _ := q.Offer(p*each+i, Sent, false); out != Enqueued {
					t.Errorf("producer %d offer %d = %v", p, i, out)
					return
				}
			}
		}()
	}
	last := make([]int, producers)
	for p := range last {
		last[p] = p*each - 1
	}
	popped := 0
	check := func(v int) {
		p := v / each
		if v != last[p]+1 {
			t.Errorf("producer %d: got %d after %d", p, v, last[p])
		}
		last[p] = v
		popped++
	}
	for popped < producers*each && !t.Failed() {
		if v, _, ok := q.Pop(); ok {
			check(v)
		} else {
			runtime.Gosched()
		}
	}
	wg.Wait()
	if popped != producers*each && !t.Failed() {
		t.Fatalf("popped %d entries, want %d", popped, producers*each)
	}
}
