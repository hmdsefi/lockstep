// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// The engine microbenchmarks of SCH §9. They use the fake actor of SCH §10 (fakeActor) with an
// onTurn hook and no messages, so every turn reports no more work. Each builds its engine with
// newEngine, which stops it in the benchmark's cleanup, so TestMain's leak check passes after a
// -bench run.

// benchFake returns a fake actor whose turns call fn.
func benchFake(b *testing.B, id int, fn func(self *fakeActor)) *fakeActor {
	a := newFake(b, id)
	a.onTurn = func(self *fakeActor, _ int) { fn(self) }
	return a
}

// waitCount spins until c reaches n, yielding the processor between reads.
func waitCount(c *atomic.Int64, n int64) {
	for c.Load() < n {
		runtime.Gosched()
	}
}

// BenchmarkReadyGlobal measures Ready from a goroutine outside the pool plus the turn it
// queues, with Workers = GOMAXPROCS.
func BenchmarkReadyGlobal(b *testing.B) {
	e := newEngine(b, Config{Node: "n"})
	var turns atomic.Int64
	a := benchFake(b, 0, func(*fakeActor) { turns.Add(1) })
	b.ReportAllocs()
	var n int64
	for b.Loop() {
		n++
		e.s.Ready(a)
		waitCount(&turns, n)
	}
}

// BenchmarkReadyLocal measures ReadyFrom inside a turn (actor to actor) plus the turn it queues.
// One worker, so every ReadyFrom lands on its local queue and no thief moves it. A hub actor
// readies a batch of leaf actors per turn; the external Ready that starts the hub's turn is
// shared by the batch.
func BenchmarkReadyLocal(b *testing.B) {
	e := newEngine(b, Config{Node: "n", Workers: 1})
	const batch = LocalQueueSize / 2
	var turns atomic.Int64
	leaves := make([]*fakeActor, batch)
	for i := range leaves {
		leaves[i] = benchFake(b, i, func(*fakeActor) { turns.Add(1) })
	}
	var size int
	hub := benchFake(b, batch, func(self *fakeActor) {
		for _, l := range leaves[:size] {
			e.s.ReadyFrom(l, self)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	var n int64
	for left := b.N; left > 0; left -= size {
		size = min(batch, left)
		n += int64(size)
		e.s.Ready(hub)
		waitCount(&turns, n)
	}
}

// BenchmarkPingPong measures two actors readying each other through ReadyFrom, one turn per
// iteration, on one worker and on GOMAXPROCS workers.
func BenchmarkPingPong(b *testing.B) {
	for _, bc := range []struct {
		name    string
		workers int
	}{
		{"workers=1", 1},
		{"workers=procs", runtime.GOMAXPROCS(0)},
	} {
		b.Run(bc.name, func(b *testing.B) {
			e := newEngine(b, Config{Node: "n", Workers: bc.workers})
			var turns atomic.Int64
			var target int64
			var x, y *fakeActor
			pass := func(self, other *fakeActor) {
				if turns.Add(1) < target {
					e.s.ReadyFrom(other, self)
				}
			}
			x = benchFake(b, 0, func(self *fakeActor) { pass(self, y) })
			y = benchFake(b, 1, func(self *fakeActor) { pass(self, x) })
			b.ReportAllocs()
			b.ResetTimer()
			target = int64(b.N)
			e.s.Ready(x)
			waitCount(&turns, target)
		})
	}
}

// BenchmarkSteal measures stealing: one producer actor fills its worker's local queue with leaf
// turns, and the other workers steal them. At least two workers.
func BenchmarkSteal(b *testing.B) {
	e := newEngine(b, Config{Node: "n", Workers: max(2, runtime.GOMAXPROCS(0))})
	const batch = LocalQueueSize
	var turns atomic.Int64
	leaves := make([]*fakeActor, batch)
	for i := range leaves {
		leaves[i] = benchFake(b, i, func(*fakeActor) { turns.Add(1) })
	}
	var size int
	producer := benchFake(b, batch, func(self *fakeActor) {
		for _, l := range leaves[:size] {
			e.s.ReadyFrom(l, self)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	var n int64
	for left := b.N; left > 0; left -= size {
		size = min(batch, left)
		n += int64(size)
		e.s.Ready(producer)
		waitCount(&turns, n)
	}
}

// BenchmarkEffectStart measures Start plus the done callback of an effect whose Run returns at
// once.
func BenchmarkEffectStart(b *testing.B) {
	e := newEngine(b, Config{Node: "n"})
	fx := e.effects()
	req := engine.EffectRequest{Name: "noop", Run: func(context.Context) (any, error) { return nil, nil }}
	finished := make(chan struct{})
	done := func(any, error) { finished <- struct{}{} }
	b.ReportAllocs()
	for b.Loop() {
		fx.Start(req, done)
		<-finished
	}
}
