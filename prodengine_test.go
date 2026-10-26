// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/overload"
	"github.com/hmdsefi/lockstep/internal/prodengine"
)

// AT-SCH-03 (engine half): SCH-030. New builds the system on a production engine configured from
// the node name, WithWorkers, WithThroughput and WithEffectPool, with the overload Gate as its
// admission; a queue of 0 means no queue, not the default. TestNewRejectsSchedulerOptions is the
// panic half.
func TestATSCH03NewUsesProductionEngine(t *testing.T) {
	t.Run("configured", func(t *testing.T) {
		sys := newLocalSystem(t, WithWorkers(2), WithThroughput(7), WithEffectPool(3, 5))
		pe := prodOf(t, sys)
		got := pe.Config()
		if got.Node != "local" || got.Workers != 2 || got.Throughput != 7 || got.EffectWorkers != 3 || got.EffectQueue != 5 {
			t.Errorf("Config() = {Node %q, Workers %d, Throughput %d, EffectWorkers %d, EffectQueue %d}, want {local, 2, 7, 3, 5}",
				got.Node, got.Workers, got.Throughput, got.EffectWorkers, got.EffectQueue)
		}
		if _, ok := got.Admission.(*overload.Gate); !ok {
			t.Errorf("Config().Admission is %T, want *overload.Gate", got.Admission)
		}
		if b := pe.Engine(); sys.eng.Node != b.Node || sys.eng.Epoch != b.Epoch || sys.Incarnation() != b.Epoch {
			t.Errorf("system engine {Node %q, Epoch %d}, incarnation %d; production bundle {Node %q, Epoch %d}",
				sys.eng.Node, sys.eng.Epoch, sys.Incarnation(), b.Node, b.Epoch)
		}
	})
	t.Run("no effect queue", func(t *testing.T) {
		sys := newLocalSystem(t, WithEffectPool(3, 0))
		if got := prodOf(t, sys).Config().EffectQueue; got != 0 {
			t.Errorf("WithEffectPool(3, 0): EffectQueue = %d, want 0", got)
		}
	})
	t.Run("defaults", func(t *testing.T) {
		sys := newLocalSystem(t, WithNodeName("n1"))
		got := prodOf(t, sys).Config()
		if got.Node != "n1" || got.Workers != runtime.GOMAXPROCS(0) || got.Throughput != 32 ||
			got.EffectWorkers != 64 || got.EffectQueue != 1024 {
			t.Errorf("Config() = {Node %q, Workers %d, Throughput %d, EffectWorkers %d, EffectQueue %d}, want {n1, GOMAXPROCS, 32, 64, 1024}",
				got.Node, got.Workers, got.Throughput, got.EffectWorkers, got.EffectQueue)
		}
	})
	t.Run("built on a bundle", func(t *testing.T) {
		// SCH-035: the pool options are accepted and ignored; the given bundle's engine is used.
		sys, pe := newProdSystem(t, prodengine.Config{Node: "local", Workers: 1}, WithWorkers(3), WithThroughput(5))
		if sys.prod != nil {
			t.Error("a system built on a given bundle has a production engine of its own")
		}
		if got := pe.Config(); got.Workers != 1 || got.Throughput != 32 {
			t.Errorf("bundle engine Config() = {Workers %d, Throughput %d}, want {1, 32}", got.Workers, got.Throughput)
		}
	})
}

// prodOf returns the production engine New built sys on, failing the test if it has none.
func prodOf(t *testing.T, sys *System) *prodengine.Engine {
	t.Helper()
	if sys.prod == nil {
		t.Fatal("New built no production engine")
	}
	return sys.prod
}

// AT-SCH-02 (lockstep half): SCH-020. The number of goroutines does not depend on the number of
// actors or of asks: 10,000 more actors, each answering one Ask, leave it where 10 left it.
//
// The children are spread over ten parents per batch: Shutdown's cost grows faster than linearly
// with the number of children of one parent, and 10,000 under one parent took most of the
// cleanup's 30 s bound on a slow runner.
func TestATSCH02SystemGoroutines(t *testing.T) {
	const parents = 10
	sys := newLocalSystem(t, WithWorkers(2))
	// spawnAndAsk spawns parents new parents named prefix0 … prefix9, n/parents children under
	// each, asks every child at once and awaits the answers. It returns the parents.
	spawnAndAsk := func(prefix string, n int) []PID[any] {
		t.Helper()
		ps := make([]PID[any], parents)
		var pids []PID[ReplyTo[int]]
		for j := range ps {
			ps[j] = spawnQuiet(t, sys, fmt.Sprintf("%s%d", prefix, j))
			v := inProbe(t, sys, ps[j], func(ctx *Context[any]) any {
				out := make([]PID[ReplyTo[int]], 0, n/parents)
				for i := range n / parents {
					pid, err := Spawn(ctx, fmt.Sprintf("c%d", i), func() Actor[ReplyTo[int]] {
						return &fnActor[ReplyTo[int]]{receive: func(ctx *Context[ReplyTo[int]], rt ReplyTo[int]) {
							rt.Send(ctx, i)
						}}
					})
					if err != nil {
						return err
					}
					out = append(out, pid)
				}
				return out
			})
			batch, ok := v.([]PID[ReplyTo[int]])
			if !ok {
				t.Fatalf("spawning %d children under %s: %v", n/parents, ps[j], v)
			}
			pids = append(pids, batch...)
		}
		// Every ask is pending at once before the first answer is awaited.
		replies := make([]*Reply[int], len(pids))
		for i, pid := range pids {
			replies[i] = Ask(sys, pid, func(rt ReplyTo[int]) ReplyTo[int] { return rt }, WithTimeout(time.Minute))
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for i, r := range replies {
			if v, err := r.Await(ctx); err != nil || v != i%(n/parents) {
				t.Fatalf("Ask to child %d = %d, %v; want %d, nil", i, v, err, i%(n/parents))
			}
		}
		return ps
	}

	spawnAndAsk("w", 10)
	g1 := settledGoroutines()
	big := spawnAndAsk("a", 10_000)
	g2 := waitGoroutines(g1, 5*time.Second)
	if g2 < g1 {
		// Goroutines that earlier tests left behind exited during the run, so g1 counted them.
		// Stop the 10,000 actors first, so the repeat does not double Shutdown's work, then take
		// the baseline again once the count has settled and repeat with 10,000 new actors.
		stopParents(t, sys, big)
		g1 = settledGoroutines()
		spawnAndAsk("b", 10_000)
		g2 = waitGoroutines(g1, 5*time.Second)
	}
	if g2 != g1 {
		t.Errorf("runtime.NumGoroutine() = %d after 10,000 more actors answered an Ask, want %d", g2, g1)
	}
}

// stopParents stops each of pids and waits up to 30 s until none of them is alive.
func stopParents(t *testing.T, sys *System, pids []PID[any]) {
	t.Helper()
	for _, pid := range pids {
		if err := Stop(sys, pid.Address()); err != nil {
			t.Fatalf("Stop(%s): %v", pid, err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for _, pid := range pids {
		// Stop on an actor whose stop has started keeps the first request; once the actor is
		// gone it reports ErrNotFound.
		for !errors.Is(Stop(sys, pid.Address()), ErrNotFound) {
			if time.Now().After(deadline) {
				t.Fatalf("%s still alive 30 s after Stop", pid)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// TestContextFromHelperGoroutines checks that the engine stays correct when a Context is misused:
// a handler starts 64 goroutines that each Send through its Context to a distinct receiver and
// waits for them, which breaks the actor rules on purpose (LS100 reports both). On one worker,
// for 200 rounds, every one of the 12,800 messages arrives and Shutdown returns nil.
func TestContextFromHelperGoroutines(t *testing.T) {
	const rounds, helpers = 200, 64
	sys := newLocalSystem(t, WithWorkers(1))
	var received, failed atomic.Int64
	recv := make([]PID[int], helpers)
	for i := range recv {
		pid, err := Spawn(sys, fmt.Sprintf("r%d", i), func() Actor[int] {
			return &fnActor[int]{receive: func(*Context[int], int) { received.Add(1) }}
		}, WithMailbox(rounds, FailSend))
		if err != nil {
			t.Fatalf("Spawn r%d: %v", i, err)
		}
		recv[i] = pid
	}
	fan, err := Spawn(sys, "fan", func() Actor[int] {
		return &fnActor[int]{receive: func(ctx *Context[int], _ int) {
			var wg sync.WaitGroup
			for _, to := range recv {
				wg.Add(1)
				//lockstep:allow LS100 the test uses the Context from helper goroutines on purpose
				go func() {
					defer wg.Done()
					if err := Send(ctx, to, 1); err != nil {
						failed.Add(1)
					}
				}()
			}
			//lockstep:allow LS100 the handler waits for its helper goroutines on purpose
			wg.Wait()
		}}
	}, WithMailbox(rounds, FailSend))
	if err != nil {
		t.Fatalf("Spawn fan: %v", err)
	}
	for r := range rounds {
		send(t, sys, fan, r)
	}
	const want = rounds * helpers
	deadline := time.Now().Add(10 * time.Second)
	for received.Load()+failed.Load() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got, f := received.Load(), failed.Load(); got != want || f != 0 {
		t.Errorf("received %d of %d messages, %d sends failed: turns were lost", got, want, f)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// settledGoroutines returns runtime.NumGoroutine() once it has not changed for 50 ms, or after 2 s,
// so goroutines that earlier tests left exiting do not count.
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

// waitGoroutines polls runtime.NumGoroutine() until it is want or d has passed, and returns the
// last value it read.
func waitGoroutines(want int, d time.Duration) int {
	n := runtime.NumGoroutine()
	for deadline := time.Now().Add(d); n != want && time.Now().Before(deadline); n = runtime.NumGoroutine() {
		time.Sleep(10 * time.Millisecond)
	}
	return n
}

// AT-SCH-09: SCH-100. With one worker, an actor with a long backlog yields after every turn of 32
// messages, so a message to another actor waits for at most one more turn of it.
func TestATSCH09YieldFairness(t *testing.T) {
	sys := newLocalSystem(t, WithWorkers(1), WithThroughput(32))
	var count, stored atomic.Int64
	a, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(*Context[int], int) { count.Add(1) },
	}), WithMailbox(20_000, FailSend))
	if err != nil {
		t.Fatal(err)
	}
	ran := make(chan struct{}, 1)
	b, err := Spawn(sys, "b", actorOf[int](&fnActor[int]{receive: func(*Context[int], int) {
		stored.Store(count.Load())
		ran <- struct{}{}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10_000 {
		send(t, sys, a, i)
	}
	send(t, sys, b, 0)
	c0 := count.Load()
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("B did not run within 10 s")
	}
	if d := stored.Load() - c0; d > 32 {
		t.Errorf("A processed %d messages between B's Send and B's turn, want at most 32 (one turn)", d)
	}
}

// AT-SCH-22: SCH-230, ENG-080. The production bundle has no transport, so a message to another
// node is a dead letter with reason unreachable and Send returns nil.
func TestATSCH22NoTransport(t *testing.T) {
	if tr := newLocalSystem(t).eng.Transport; tr != nil {
		t.Errorf("New's engine has Transport %T, want nil", tr)
	}
	rec := enginetest.NewRecorder()
	sys, pe := newProdSystemWith(t, prodengine.Config{Node: "local", Workers: 2},
		func(e *engine.Engine) { e.Tracer = rec })
	if tr := pe.Engine().Transport; tr != nil {
		t.Errorf("Engine().Transport = %T, want nil", tr)
	}
	to := PIDAt[int]("other", "/user/x")
	if err := Send(sys, to, 7); err != nil {
		t.Fatalf("Send to another node = %v, want nil", err)
	}
	dead := recordsOf(rec.Records(), "lockstep.deadletter")
	if len(dead) != 1 {
		t.Fatalf("lockstep.deadletter records = %v, want one", dead)
	}
	if got := attrOf(dead[0], "to"); got != to.Address().String() {
		t.Errorf("dead letter to = %q, want %q", got, to.Address().String())
	}
	if got := attrOf(dead[0], "reason"); got != "unreachable" {
		t.Errorf("dead letter reason = %q, want unreachable", got)
	}
}

// AT-SCH-25 (production engine): SCH-040, ENG-310. An actor spawned with WithActorThroughput(4)
// processes at most 4 user messages per turn although the engine passes 32.
func TestATSCH25ActorThroughput(t *testing.T) {
	sys := newLocalSystem(t, WithWorkers(1), WithThroughput(32))
	var mu sync.Mutex
	var perTurn []int // the user count of each of a's turns
	total := 0
	sys.afterTurn = func(a Address, _, user int) {
		if a.Path() != "/user/a" {
			return
		}
		mu.Lock()
		perTurn = append(perTurn, user)
		total += user
		mu.Unlock()
	}
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{}), WithActorThroughput(4))
	if err != nil {
		t.Fatal(err)
	}
	release := hold(t, sys, pid.Address()) // the 100 messages are queued before any turn pops one
	for i := range 100 {
		send(t, sys, pid, i)
	}
	release()
	done := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return total == 100
	}
	for deadline := time.Now().Add(10 * time.Second); !done(); {
		if time.Now().After(deadline) {
			t.Fatal("a did not process 100 messages within 10 s")
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	most := 0
	for _, u := range perTurn {
		most = max(most, u)
	}
	if most != 4 {
		t.Errorf("user messages per turn = %v, want each at most 4 and some 4", perTurn)
	}

	_, err = Spawn(sys, "b", actorOf[int](&fnActor[int]{}), WithActorThroughput(0))
	if want := "lockstep: invalid option: WithActorThroughput(0): n must be at least 1"; err == nil ||
		!errors.Is(err, ErrInvalidOption) || err.Error() != want {
		t.Errorf("Spawn with WithActorThroughput(0) = %v, want %q wrapping ErrInvalidOption", err, want)
	}
}
