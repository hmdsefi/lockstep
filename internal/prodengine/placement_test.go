// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pingPong is the actor pair of AT-SCH-10 and AT-SCH-13: x and y each process one message per
// turn and, while turns are left, give the other one message and ReadyFrom it, so the pair keeps
// readying each other through the local queue of the worker that runs them.
type pingPong struct {
	x, y  *fakeActor
	turns atomic.Int64 // turns of x and y together, counted at turn start
	left  atomic.Int64 // turns left; the turn that takes the last one readies nobody
}

// newPingPong returns a stopped pair on e; start sets the turns left and readies x.
func newPingPong(t testing.TB, e *Engine) *pingPong {
	p := &pingPong{x: newFake(t, 0), y: newFake(t, 1)}
	turn := func(partner *fakeActor) func(*fakeActor, int) {
		return func(self *fakeActor, _ int) {
			p.turns.Add(1)
			if p.left.Add(-1) > 0 {
				partner.add()
				e.s.ReadyFrom(partner, self)
			}
		}
	}
	p.x.onTurn = turn(p.y)
	p.y.onTurn = turn(p.x)
	return p
}

// start lets the pair run n more turns, beginning with a turn of x readied from outside.
func (p *pingPong) start(e *Engine, n int64) {
	p.left.Store(n)
	p.x.add()
	e.s.Ready(p.x)
}

// TestATSCH07LocalPlacement covers SCH-060 and SCH-080 (AT-SCH-07): ReadyFrom during from's turn
// queues locally; ReadyFrom with an idle from and Ready queue globally.
func TestATSCH07LocalPlacement(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 1}) // one worker, so no thief moves entries
	from, to, x, u, v, y := newFake(t, 0), newFake(t, 1), newFake(t, 2), newFake(t, 3), newFake(t, 4), newFake(t, 5)

	var local1, global1 atomic.Int64
	from.onTurn = func(f *fakeActor, _ int) {
		e.s.ReadyFrom(to, f)
		local1.Store(int64(e.localLen(0)))
		global1.Store(int64(e.globalLen()))
	}
	from.add()
	e.s.Ready(from)
	waitFor(t, time.Second, "the turn of to", func() bool { return to.turns.Load() == 1 })
	if l, g := local1.Load(), global1.Load(); l != 1 || g != 0 {
		t.Errorf("after ReadyFrom(to, from) in from's turn: localLen(0) = %d, globalLen() = %d, want 1, 0", l, g)
	}

	var local0, global0, local2, global2 atomic.Int64
	y.onTurn = func(*fakeActor, int) {
		local0.Store(int64(e.localLen(0)))
		global0.Store(int64(e.globalLen()))
		e.s.ReadyFrom(u, x) // x is idle
		e.s.Ready(v)
		local2.Store(int64(e.localLen(0)))
		global2.Store(int64(e.globalLen()))
	}
	y.add()
	e.s.Ready(y)
	waitFor(t, time.Second, "the turns of u and v", func() bool { return u.turns.Load() == 1 && v.turns.Load() == 1 })
	if l0, l2 := local0.Load(), local2.Load(); l2 != l0 {
		t.Errorf("ReadyFrom with an idle from and Ready changed localLen(0) from %d to %d", l0, l2)
	}
	if g0, g2 := global0.Load(), global2.Load(); g2 != g0+2 {
		t.Errorf("ReadyFrom with an idle from and Ready changed globalLen() from %d to %d, want +2", g0, g2)
	}

	time.Sleep(10 * time.Millisecond) // let a wrong extra turn show
	for _, a := range []*fakeActor{from, to, u, v, y} {
		if n := a.turns.Load(); n != 1 {
			t.Errorf("fake %d ran %d turns, want 1", a.id, n)
		}
	}
	if n := x.turns.Load(); n != 0 {
		t.Errorf("idle fake %d ran %d turns, want 0", x.id, n)
	}
	if h := from.Slot().Hint.Load(); h != 1 {
		t.Errorf("from.Slot().Hint = %d after its turn, want 1", h)
	}
}

// TestATSCH08LocalOverflow covers SCH-060 (AT-SCH-08): a push to a full local queue moves its
// oldest half plus the new entry to the global queue, in order.
func TestATSCH08LocalOverflow(t *testing.T) {
	const n = 300
	e := newEngine(t, Config{Node: "n", Workers: 1})
	p := newFake(t, n)
	as := fakes(t, n) // as[i] is entry i+1

	var mu sync.Mutex
	var order []int
	for _, a := range as {
		a.onTurn = func(f *fakeActor, _ int) {
			mu.Lock()
			order = append(order, f.id)
			mu.Unlock()
		}
	}
	var local, global atomic.Int64
	p.onTurn = func(f *fakeActor, _ int) {
		for _, a := range as {
			e.s.ReadyFrom(a, f)
		}
		local.Store(int64(e.localLen(0)))
		global.Store(int64(e.globalLen()))
	}
	p.add()
	e.s.Ready(p)

	waitFor(t, 5*time.Second, "300 turns", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == n
	})
	if l, g := local.Load(), global.Load(); l != 171 || g != 129 {
		t.Errorf("after 300 ReadyFrom in one turn: localLen(0) = %d, globalLen() = %d, want 171, 129", l, g)
	}
	time.Sleep(10 * time.Millisecond) // let a wrong extra turn show
	for _, a := range as {
		if got := a.turns.Load(); got != 1 {
			t.Errorf("entry %d ran %d turns, want 1", a.id+1, got)
		}
	}
	mu.Lock()
	var spilled []int // entries 1-128 (ids 0-127) in the order they ran
	for _, id := range order {
		if id < LocalQueueSize/2 {
			spilled = append(spilled, id)
		}
	}
	mu.Unlock()
	if !slices.IsSorted(spilled) || len(spilled) != LocalQueueSize/2 {
		t.Errorf("entries 1-128 ran in order %v, want push order", spilled)
	}
}

// TestATSCH10GlobalCheckInterval covers SCH-070 and SCH-100 (AT-SCH-10): actors readying each
// other through the local queue delay an external Ready by at most GlobalCheckInterval turns.
func TestATSCH10GlobalCheckInterval(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 1})
	p := newPingPong(t, e)
	c := newFake(t, 2)
	var t1 atomic.Int64
	c.onTurn = func(*fakeActor, int) { t1.Store(p.turns.Load()) }

	p.start(e, math.MaxInt64)
	t.Cleanup(func() { p.left.Store(0) }) // runs before the engine's Stop
	waitFor(t, time.Second, "the first turn of x", func() bool { return p.x.turns.Load() >= 1 })
	c.add()
	e.s.Ready(c)
	t0 := p.turns.Load()
	waitFor(t, time.Second, "the turn of c", func() bool { return c.turns.Load() == 1 })
	if d := t1.Load() - t0; d > GlobalCheckInterval {
		t.Errorf("c's turn started %d ping-pong turns after Ready(c), want at most %d", d, GlobalCheckInterval)
	}
}

// TestATSCH11Stealing covers SCH-070 step 4 (AT-SCH-11): idle workers steal from a worker whose
// local queue holds slow turns.
func TestATSCH11Stealing(t *testing.T) {
	const n = 200
	e := newEngine(t, Config{Node: "n", Workers: 4})
	p := newFake(t, n)
	as := fakes(t, n)
	ran := make([]atomic.Int64, n) // ran[i] is 1 + the worker index of as[i]'s turn
	for _, a := range as {
		a.onTurn = func(f *fakeActor, _ int) {
			ran[f.id].Store(int64(f.Slot().Hint.Load()))
			time.Sleep(time.Millisecond) // test code, outside lockstep's rules
		}
	}
	var global atomic.Int64
	p.onTurn = func(f *fakeActor, _ int) {
		for _, a := range as {
			e.s.ReadyFrom(a, f)
		}
		global.Store(int64(e.globalLen()))
	}
	p.add()
	e.s.Ready(p)

	waitFor(t, 5*time.Second, "200 turns", func() bool {
		for _, a := range as {
			if a.turns.Load() == 0 {
				return false
			}
		}
		return true
	})
	time.Sleep(10 * time.Millisecond) // let a wrong extra turn show
	workers := map[int64]bool{}
	for i, a := range as {
		if got := a.turns.Load(); got != 1 {
			t.Errorf("fake %d ran %d turns, want 1", a.id, got)
		}
		workers[ran[i].Load()-1] = true
	}
	if g := global.Load(); g != 0 {
		t.Errorf("globalLen() = %d after 200 ReadyFrom in p's turn, want 0 (all local)", g)
	}
	if len(workers) < 2 {
		t.Errorf("200 slow turns ran on workers %v, want at least two", workers)
	}
}

// TestATSCH13NoAllocation covers SCH-110 and SCH-270 (AT-SCH-13): turns readied through the local
// queue, external Ready calls, parking, waking and publishing turns allocate nothing in steady
// state. The measured functions wait by polling atomics, so the test itself allocates nothing.
func TestATSCH13NoAllocation(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	const turns = 10_000
	e := newEngine(t, Config{Node: "n", Workers: 1})

	p := newPingPong(t, e)
	pingPongRun := func() {
		p.start(e, turns)
		deadline := time.Now().Add(10 * time.Second)
		for p.left.Load() > 0 {
			if time.Now().After(deadline) {
				t.Fatalf("ping-pong: %d turns left after 10s", p.left.Load())
			}
			runtime.Gosched()
		}
	}
	pingPongRun() // warm up
	if n := testing.AllocsPerRun(10, pingPongRun); n != 0 {
		t.Errorf("10,000 ping-pong turns: %v allocations, want 0", n)
	}
	// AllocsPerRun runs its function once more than asked, as a warm-up.
	if got, want := p.turns.Load(), int64(12*turns); got != want {
		t.Errorf("ping-pong ran %d turns, want %d", got, want)
	}

	c := newFake(t, 2)
	readyRun := func() {
		deadline := time.Now().Add(10 * time.Second)
		for range turns {
			c.add()
			e.s.Ready(c)
			for c.processed.Load() != c.added.Load() {
				if time.Now().After(deadline) {
					t.Fatalf("external Ready: %d of %d messages processed after 10s", c.processed.Load(), c.added.Load())
				}
				runtime.Gosched()
			}
		}
	}
	readyRun() // warm up
	if n := testing.AllocsPerRun(10, readyRun); n != 0 {
		t.Errorf("10,000 rounds of Ready plus turn: %v allocations, want 0", n)
	}
	if got, want := c.processed.Load(), int64(12*turns); got != want {
		t.Errorf("external Ready rounds processed %d messages, want %d", got, want)
	}
}
