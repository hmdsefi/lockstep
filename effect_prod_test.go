// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/prodengine"
)

// nestedDoneEffects wraps an engine.Effects and counts every done that the engine calls on a
// goroutine that is inside a Start or a Cancel call of the wrapper (ENG-090, EFF-060). It keys the
// calls by goroutine ID, so a done on an effect worker while another goroutine is in Start is not
// a nested call.
type nestedDoneEffects struct {
	inner engine.Effects

	mu     sync.Mutex
	inCall map[uint64]int // goroutine ID -> Start and Cancel calls on its stack

	starts, dones, nested atomic.Int32
}

func (w *nestedDoneEffects) enter() uint64 {
	id := goroutineID()
	w.mu.Lock()
	w.inCall[id]++
	w.mu.Unlock()
	return id
}

func (w *nestedDoneEffects) leave(id uint64) {
	w.mu.Lock()
	if w.inCall[id]--; w.inCall[id] == 0 {
		delete(w.inCall, id)
	}
	w.mu.Unlock()
}

func (w *nestedDoneEffects) Start(req engine.EffectRequest, done func(v any, err error)) engine.Cancel {
	w.starts.Add(1)
	id := w.enter()
	defer w.leave(id)
	cancel := w.inner.Start(req, func(v any, err error) {
		w.dones.Add(1)
		w.mu.Lock()
		nested := w.inCall[goroutineID()] > 0
		w.mu.Unlock()
		if nested {
			w.nested.Add(1)
		}
		done(v, err)
	})
	return func() bool {
		id := w.enter()
		defer w.leave(id)
		return cancel()
	}
}

// prodFetched is the message of the production effect tests: tag names the effect whose outcome
// it carries.
type prodFetched struct {
	tag string
	v   int64
	err error
}

// recvProdFetched returns the next prodFetched of out, failing after 10 s.
func recvProdFetched(t *testing.T, out <-chan any) prodFetched {
	t.Helper()
	m := recvMsg(t, out)
	f, ok := m.(prodFetched)
	if !ok {
		t.Fatalf("received %#v, want a prodFetched", m)
	}
	return f
}

// AT-EFF-23: on the production engine a saturated pool resolves with ErrOverloaded, a queued
// effect's timeout counts from Do and removes it without running it, and no done runs inside a
// Start or Cancel call (EFF-300, EFF-310, EFF-060, EFF-090, EFF-120). One worker and one actor
// turn make the three Do calls, so B's timeout cannot free the queue slot before C's Start.
func TestATEFF23ProductionPool(t *testing.T) {
	rec := enginetest.NewRecorder()
	var wrap *nestedDoneEffects
	sys, _ := newProdSystemWith(t,
		prodengine.Config{Node: "local", Workers: 1, EffectWorkers: 1, EffectQueue: 1},
		func(e *engine.Engine) {
			wrap = &nestedDoneEffects{inner: e.Effects, inCall: make(map[uint64]int)}
			e.Effects = wrap
			e.Tracer = rec
		})
	pid, out := spawnLocalActor(t, sys)

	aRunning := make(chan struct{})
	releaseA := make(chan struct{})
	var bRan, cRan atomic.Int32
	effA := funcEffect{"a.block", func(ctx context.Context) (int64, error) {
		close(aRunning)
		select {
		case <-releaseA:
		case <-ctx.Done():
		}
		return 7, nil
	}}
	effB := funcEffect{"b.queued", func(context.Context) (int64, error) {
		bRan.Add(1)
		return 8, nil
	}}
	effC := funcEffect{"c.refused", func(context.Context) (int64, error) {
		cRan.Add(1)
		return 9, nil
	}}
	t.Cleanup(func() {
		select {
		case <-releaseA:
		default:
			close(releaseA)
		}
	})

	bStart := make(chan time.Time, 1) // when Do(B) was called
	send(t, sys, pid, any(func(ctx *Context[any]) {
		Do(ctx, effA, func(v int64, err error) any { return prodFetched{"a", v, err} }, WithEffectTimeout(time.Hour))
		// A holds the pool's one worker before B and C start: otherwise A could still sit in the
		// queue, B would find the worker free and C would take the queue slot.
		select {
		case <-aRunning:
		case <-time.After(10 * time.Second):
			t.Error("A's Run did not start within 10s")
		}
		bStart <- time.Now()
		Do(ctx, effB, func(v int64, err error) any { return prodFetched{"b", v, err} }, WithEffectTimeout(10*time.Millisecond))
		Do(ctx, effC, func(v int64, err error) any { return prodFetched{"c", v, err} }, WithEffectTimeout(time.Hour))
	}))

	// C is refused: its done comes first, with an error that matches both ErrOverloaded values.
	c := recvProdFetched(t, out)
	if c.tag != "c" || c.v != 0 || !errors.Is(c.err, ErrOverloaded) || !errors.Is(c.err, engine.ErrOverloaded) {
		t.Fatalf("first outcome = %+v, want C's zero value and an error matching lockstep.ErrOverloaded and engine.ErrOverloaded", c)
	}

	// B times out in the queue, counted from Do, and its Run never runs.
	b := recvProdFetched(t, out)
	if b.tag != "b" || b.v != 0 || !errors.Is(b.err, ErrTimeout) {
		t.Fatalf("second outcome = %+v, want B's zero value and ErrTimeout", b)
	}
	if elapsed := time.Since(<-bStart); elapsed < 10*time.Millisecond {
		t.Errorf("B timed out after %v, want at least its 10ms timeout counted from Do", elapsed)
	}
	noMsg(t, out, 50*time.Millisecond) // A is still running: nothing else arrives

	// A's result arrives after it is released. B, removed from the queue, does not run now that
	// the worker is free.
	close(releaseA)
	a := recvProdFetched(t, out)
	if a.tag != "a" || a.v != 7 || a.err != nil {
		t.Fatalf("third outcome = %+v, want A's result 7", a)
	}
	noMsg(t, out, 100*time.Millisecond)
	if n := bRan.Load(); n != 0 {
		t.Errorf("B's Run was called %d times, want 0: its timeout removed it from the queue", n)
	}
	if n := cRan.Load(); n != 0 {
		t.Errorf("C's Run was called %d times, want 0: the pool refused it", n)
	}

	if n := wrap.starts.Load(); n != 3 {
		t.Errorf("Start was called %d times, want 3", n)
	}
	if n := wrap.dones.Load(); n != 2 {
		t.Errorf("the engine called done %d times, want 2 (C refused, A's result); B's was cancelled", n)
	}
	if n := wrap.nested.Load(); n != 0 {
		t.Errorf("%d done calls ran inside a Start or Cancel call, want 0", n)
	}

	got := map[string]string{}
	for _, d := range effectDones(rec.Records()) {
		got[d["name"]] = d["outcome"]
	}
	want := map[string]string{"a.block": "ok", "b.queued": "timeout", "c.refused": "overloaded"}
	if len(got) != len(want) {
		t.Errorf("done records = %v, want %v", got, want)
	}
	for name, outcome := range want {
		if got[name] != outcome {
			t.Errorf("outcome of %s = %q, want %q", name, got[name], outcome)
		}
	}
}

// A panic in Run reaches the actor as an ordinary error with ENG-430's text, and the effect ends
// with outcome=error (EFF-300).
func TestEffectPanicOnProduction(t *testing.T) {
	rec := enginetest.NewRecorder()
	sys, _ := newProdSystemWith(t, prodengine.Config{Node: "local", Workers: 2},
		func(e *engine.Engine) { e.Tracer = rec })
	pid, out := spawnLocalActor(t, sys)

	e := funcEffect{"db.query", func(context.Context) (int64, error) { panic("db") }}
	send(t, sys, pid, any(func(ctx *Context[any]) {
		Do(ctx, e, func(v int64, err error) any { return prodFetched{"db", v, err} })
	}))
	got := recvProdFetched(t, out)
	want := `lockstep: effect "db.query" panicked: db`
	if got.v != 0 || got.err == nil || got.err.Error() != want {
		t.Fatalf("done got %+v, want the zero value and the error %q", got, want)
	}
	var pe *prodengine.EffectPanicError
	if !errors.As(got.err, &pe) || pe.Name != "db.query" || pe.Value != "db" {
		t.Errorf("error %v: errors.As to *prodengine.EffectPanicError gave %+v, want name db.query and value db", got.err, pe)
	}
	dones := effectDones(rec.Records())
	if len(dones) != 1 || dones[0]["outcome"] != "error" || dones[0]["err"] != want {
		t.Errorf("done records = %v, want one outcome=error with err %q", dones, want)
	}
}
