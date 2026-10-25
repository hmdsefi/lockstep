// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// fakeActor is an engine.ActorRef identified by id.
type fakeActor struct {
	id   int
	slot engine.Slot
}

func (f *fakeActor) Address() engine.Address {
	return engine.Address{Node: "n", Path: "/fake"}
}
func (f *fakeActor) Slot() *engine.Slot { return &f.slot }
func (f *fakeActor) RunTurn(int) bool   { return false }

func actors(n int) []*fakeActor {
	out := make([]*fakeActor, n)
	for i := range out {
		out[i] = &fakeActor{id: i}
	}
	return out
}

// idOf returns the id of a fakeActor ref, or -1 for nil.
func idOf(t testing.TB, r engine.ActorRef) int {
	t.Helper()
	f, ok := r.(*fakeActor)
	if !ok {
		t.Fatalf("ref is %T, want *fakeActor", r)
	}
	return f.id
}

// drainRing pops every entry of q and returns their ids in order.
func drainRing(t testing.TB, q *localRing) []int {
	t.Helper()
	var ids []int
	for {
		r, ok := q.pop()
		if !ok {
			return ids
		}
		ids = append(ids, idOf(t, r))
	}
}

// drainGlobal pops every entry of g and returns the actor ids in order (callbacks count as -1).
func drainGlobal(t testing.TB, g *globalQueue) []int {
	t.Helper()
	var ids []int
	for {
		e, ok := g.pop()
		if !ok {
			return ids
		}
		if e.fn != nil {
			ids = append(ids, -1)
			continue
		}
		ids = append(ids, idOf(t, e.actor))
	}
}

func seq(from, to int) []int {
	out := make([]int, 0, to-from)
	for i := from; i < to; i++ {
		out = append(out, i)
	}
	return out
}

func TestLocalRingFIFO(t *testing.T) {
	var q localRing
	var g globalQueue
	as := actors(LocalQueueSize)
	if _, ok := q.pop(); ok {
		t.Fatal("pop on an empty ring reported an entry")
	}
	for _, a := range as {
		if q.push(a, &g) {
			t.Fatalf("push of actor %d overflowed a ring with room", a.id)
		}
	}
	if got := q.len(); got != LocalQueueSize {
		t.Fatalf("len = %d, want %d", got, LocalQueueSize)
	}
	if got := drainRing(t, &q); !slices.Equal(got, seq(0, LocalQueueSize)) {
		t.Fatalf("pop order = %v, want push order", got)
	}
	if _, ok := q.pop(); ok {
		t.Fatal("pop on a drained ring reported an entry")
	}
	if g.len() != 0 {
		t.Fatalf("global len = %d, want 0", g.len())
	}
}

// TestLocalRingOverflow is AT-SCH-08's arithmetic at unit level (SCH-060).
func TestLocalRingOverflow(t *testing.T) {
	var q localRing
	var g globalQueue
	as := actors(LocalQueueSize + 1)
	for _, a := range as[:LocalQueueSize] {
		q.push(a, &g)
	}
	if !q.push(as[LocalQueueSize], &g) {
		t.Fatal("the 257th push did not report overflow")
	}
	if got := q.len(); got != LocalQueueSize/2 {
		t.Fatalf("local len = %d, want %d", got, LocalQueueSize/2)
	}
	if got := g.len(); got != LocalQueueSize/2+1 {
		t.Fatalf("global len = %d, want %d", got, LocalQueueSize/2+1)
	}
	want := append(seq(0, LocalQueueSize/2), LocalQueueSize)
	if got := drainGlobal(t, &g); !slices.Equal(got, want) {
		t.Fatalf("global order = %v, want the oldest 128 then the new entry", got)
	}
	if got := drainRing(t, &q); !slices.Equal(got, seq(LocalQueueSize/2, LocalQueueSize)) {
		t.Fatalf("local order = %v, want entries 128..255", got)
	}
}

func TestGrabHalf(t *testing.T) {
	for _, n := range []int{1, 2, 5, 256} {
		var victim, thief localRing
		var g globalQueue
		for _, a := range actors(n) {
			victim.push(a, &g)
		}
		take := (n + 1) / 2
		if got := victim.grabHalf(&thief); got != take {
			t.Fatalf("n=%d: grabHalf = %d, want %d", n, got, take)
		}
		if got := drainRing(t, &thief); !slices.Equal(got, seq(0, take)) {
			t.Errorf("n=%d: thief got %v, want the oldest %d in order", n, got, take)
		}
		if got := drainRing(t, &victim); !slices.Equal(got, seq(take, n)) {
			t.Errorf("n=%d: victim kept %v, want %v", n, got, seq(take, n))
		}
	}
	var victim, thief localRing
	if got := victim.grabHalf(&thief); got != 0 {
		t.Fatalf("grabHalf from an empty ring = %d, want 0", got)
	}
}

// TestGrabRacesOwner runs the owner against three thieves; every entry must be seen exactly once.
func TestGrabRacesOwner(t *testing.T) {
	const total = 100_000
	as := actors(total)
	seen := make([]atomic.Int32, total)
	mark := func(r engine.ActorRef) { seen[idOf(t, r)].Add(1) }

	var victim localRing
	var g globalQueue
	var done atomic.Bool
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine localRing
			for !done.Load() {
				if victim.grabHalf(&mine) == 0 {
					continue
				}
				for {
					r, ok := mine.pop()
					if !ok {
						break
					}
					mark(r)
				}
			}
		}()
	}
	for i, a := range as {
		victim.push(a, &g)
		if i%3 == 0 {
			if r, ok := victim.pop(); ok {
				mark(r)
			}
		}
	}
	for {
		r, ok := victim.pop()
		if !ok {
			break
		}
		mark(r)
	}
	done.Store(true)
	wg.Wait()
	for {
		e, ok := g.pop()
		if !ok {
			break
		}
		mark(e.actor)
	}
	for i := range seen {
		if n := seen[i].Load(); n != 1 {
			t.Fatalf("entry %d seen %d times, want 1", i, n)
		}
	}
}

func TestGlobalQueueOrder(t *testing.T) {
	var g globalQueue
	as := actors(10_000)
	if _, ok := g.pop(); ok {
		t.Fatal("pop on an empty queue reported an entry")
	}
	var want []int
	for i, a := range as {
		switch i % 3 {
		case 0:
			g.pushTurn(a)
			want = append(want, i)
		case 1:
			g.pushCallback(func() {})
			want = append(want, -1)
		default:
			g.pushBatch([]engine.ActorRef{a})
			want = append(want, i)
		}
		if g.len() != i+1 {
			t.Fatalf("len = %d after %d pushes", g.len(), i+1)
		}
	}
	if got := drainGlobal(t, &g); !slices.Equal(got, want) {
		t.Fatal("entries did not come out in push order across growth")
	}
	if g.len() != 0 {
		t.Fatalf("len = %d after drain, want 0", g.len())
	}
}

func TestGlobalQueueBatch(t *testing.T) {
	var g globalQueue
	as := actors(40)
	g.pushTurn(as[0])
	batch := make([]engine.ActorRef, 0, 39)
	for _, a := range as[1:] {
		batch = append(batch, a)
	}
	g.pushBatch(batch)
	g.pushCallback(func() {})
	want := append(seq(0, 40), -1)
	if got := drainGlobal(t, &g); !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestGlobalQueueWrap(t *testing.T) {
	// Interleave pushes and pops so the ring buffer's head wraps before it grows.
	var g globalQueue
	as := actors(2000)
	next, popped := 0, 0
	for round := range 200 {
		for range round%7 + 3 {
			g.pushTurn(as[next])
			next++
		}
		for range round%5 + 1 {
			e, ok := g.pop()
			if !ok {
				break
			}
			if id := idOf(t, e.actor); id != popped {
				t.Fatalf("popped %d, want %d", id, popped)
			}
			popped++
		}
		if g.len() != next-popped {
			t.Fatalf("len = %d, want %d", g.len(), next-popped)
		}
	}
}

func TestRunqNoAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	as := actors(2)
	cb := func() {}

	var g globalQueue
	for range 100 { // grow the buffer to its working size
		g.pushTurn(as[0])
	}
	for range 100 {
		g.pop()
	}
	if n := testing.AllocsPerRun(1000, func() {
		g.pushTurn(as[0])
		g.pushCallback(cb)
		g.pushBatch([]engine.ActorRef{as[0], as[1]})
		for range 4 {
			g.pop()
		}
	}); n != 0 {
		t.Errorf("global queue cycle allocates %v times, want 0", n)
	}

	var q, other localRing
	if n := testing.AllocsPerRun(1000, func() {
		q.push(as[0], &g)
		q.push(as[1], &g)
		q.grabHalf(&other)
		q.pop()
		other.pop()
	}); n != 0 {
		t.Errorf("local ring cycle allocates %v times, want 0", n)
	}

	for range LocalQueueSize {
		q.push(as[0], &g)
	}
	if n := testing.AllocsPerRun(100, func() {
		q.push(as[1], &g) // overflows: spills half to the global queue
		for range LocalQueueSize / 2 {
			g.pop()
		}
		g.pop()
		for range LocalQueueSize / 2 {
			q.push(as[0], &g)
		}
	}); n != 0 {
		t.Errorf("overflow path allocates %v times, want 0", n)
	}
}
