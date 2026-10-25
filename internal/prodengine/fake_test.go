// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"bytes"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// fakeActor is the fake actor of SCH §10: an engine.ActorRef whose RunTurn processes
// min(budget, count) messages and reports whether messages remain. A test adds work with add and
// then calls Ready. RunTurn fails the test if it overlaps another turn of the same actor.
type fakeActor struct {
	t    testing.TB // nil for actors whose turns never run (the queue tests)
	id   int
	addr engine.Address
	slot engine.Slot

	count     atomic.Int64 // messages waiting
	added     atomic.Int64 // messages added with add
	processed atomic.Int64 // messages processed by RunTurn
	turns     atomic.Int64 // RunTurn calls
	inTurn    atomic.Bool  // set for the duration of RunTurn

	// onTurn, if set, runs at the start of every turn, inside RunTurn, with the turn's budget.
	onTurn func(f *fakeActor, budget int)
}

// newFake returns a fake actor whose turns report to t.
func newFake(t testing.TB, id int) *fakeActor {
	return &fakeActor{t: t, id: id, addr: engine.Address{Node: "n", Path: fmt.Sprintf("/fake/%d", id)}}
}

// fakes returns n fake actors with ids 0 … n-1 whose turns report to t.
func fakes(t testing.TB, n int) []*fakeActor {
	out := make([]*fakeActor, n)
	for i := range out {
		out[i] = newFake(t, i)
	}
	return out
}

func (f *fakeActor) Address() engine.Address { return f.addr }
func (f *fakeActor) Slot() *engine.Slot      { return &f.slot }

// add adds one message.
func (f *fakeActor) add() {
	f.added.Add(1)
	f.count.Add(1)
}

// RunTurn processes min(budget, count) messages and reports whether messages remain.
func (f *fakeActor) RunTurn(budget int) bool {
	if !f.inTurn.CompareAndSwap(false, true) {
		f.t.Errorf("fake %d: RunTurn overlaps another turn of the same actor", f.id)
		return false
	}
	f.turns.Add(1)
	if f.onTurn != nil {
		f.onTurn(f, budget)
	}
	n := min(int64(budget), f.count.Load())
	f.count.Add(-n)
	f.processed.Add(n)
	more := f.count.Load() > 0
	f.inTurn.Store(false)
	return more
}

// newEngine starts an engine for cfg and stops it when the test ends.
func newEngine(t testing.TB, cfg Config) *Engine {
	t.Helper()
	e := New(cfg)
	t.Cleanup(e.Stop)
	return e
}

// waitFor polls cond until it holds, failing the test after d.
func waitFor(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// workerGoroutines returns the number of worker goroutines of any engine that are alive.
func workerGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return bytes.Count(buf[:n], []byte("prodengine.(*worker).loop("))
		}
		buf = make([]byte, 2*len(buf))
	}
}

// settledGoroutines returns runtime.NumGoroutine() once it has not changed for 50 ms, or after
// 2 s, so goroutines that earlier tests left exiting do not count.
func settledGoroutines() int {
	n := runtime.NumGoroutine()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		time.Sleep(50 * time.Millisecond)
		m := runtime.NumGoroutine()
		if m == n {
			return n
		}
		n = m
	}
	return n
}

// panicText runs f and returns the text of the value it panicked with, or "" if it returned.
func panicText(f func()) (text string) {
	defer func() {
		if r := recover(); r != nil {
			text = fmt.Sprint(r)
		}
	}()
	f()
	return ""
}
