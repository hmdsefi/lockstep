// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package local_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/local"
)

// result is one done call.
type result struct {
	v      any
	err    error
	inTurn bool
}

// AT-ENG-34, ENG-090, ENG-430
func TestATENG34LocalEffects(t *testing.T) {
	t.Run("a value", func(t *testing.T) {
		e, b := start(t, local.Config{Node: "a"})
		results := make(chan result, 2)
		b.Effects.Start(engine.EffectRequest{ID: 1, Name: "seven", Run: func(context.Context) (any, error) {
			return 7, nil
		}}, func(v any, err error) { results <- result{v, err, b.Scheduler.InTurn()} })
		got := recv(t, "done", results)
		if got.v != 7 || got.err != nil || !got.inTurn {
			t.Errorf("done(%v, %v) with InTurn %v, want done(7, <nil>) with InTurn true", got.v, got.err, got.inTurn)
		}
		time.Sleep(20 * time.Millisecond)
		e.Stop()
		if n := len(results); n != 0 {
			t.Errorf("done ran %d more times", n)
		}
	})

	t.Run("b cancel", func(t *testing.T) {
		_, b := start(t, local.Config{Node: "a"})
		started := make(chan struct{})
		returned := make(chan error, 1)
		var called atomic.Bool
		cancel := b.Effects.Start(engine.EffectRequest{ID: 1, Name: "block", Run: func(ctx context.Context) (any, error) {
			close(started)
			<-ctx.Done()
			returned <- ctx.Err()
			return nil, ctx.Err()
		}}, func(any, error) { called.Store(true) })
		recv(t, "Run to start", started)
		if !cancel() {
			t.Error("Cancel() of a running effect = false")
		}
		if err := recv(t, "Run to return", returned); !errors.Is(err, context.Canceled) {
			t.Errorf("ctx.Err() = %v, want context.Canceled", err)
		}
		time.Sleep(50 * time.Millisecond)
		if called.Load() {
			t.Error("done ran for a cancelled effect")
		}
		if cancel() {
			t.Error("second Cancel() = true")
		}
	})

	t.Run("c panic", func(t *testing.T) {
		_, b := start(t, local.Config{Node: "a"})
		results := make(chan result, 1)
		b.Effects.Start(engine.EffectRequest{ID: 1, Name: "db.get", Run: func(context.Context) (any, error) {
			panic("boom")
		}}, func(v any, err error) { results <- result{v, err, b.Scheduler.InTurn()} })
		got := recv(t, "done", results)
		want := `lockstep: effect "db.get" panicked: boom`
		if got.v != nil || got.err == nil || got.err.Error() != want {
			t.Errorf("done(%v, %v), want done(<nil>, %s)", got.v, got.err, want)
		}
	})

	t.Run("d pool and queue", func(t *testing.T) {
		e, b := start(t, local.Config{Node: "a"})
		const total = local.EffectWorkers + local.EffectQueue
		release := make(chan struct{})
		started := make(chan int, total)
		var doneCalls atomic.Int32
		for i := range total {
			b.Effects.Start(engine.EffectRequest{ID: uint64(i + 1), Name: "wait", Run: func(context.Context) (any, error) {
				started <- i
				<-release
				return i, nil
			}}, func(v any, err error) {
				if v != i || err != nil {
					t.Errorf("effect %d: done(%v, %v), want done(%d, <nil>)", i, v, err, i)
				}
				doneCalls.Add(1)
			})
		}
		seen := make(map[int]bool, local.EffectWorkers)
		for range local.EffectWorkers {
			i := recv(t, "the first 64 effects to start", started)
			if i >= local.EffectWorkers || seen[i] {
				t.Fatalf("effect %d started among the first %d", i, local.EffectWorkers)
			}
			seen[i] = true
		}
		time.Sleep(20 * time.Millisecond)
		if n := len(started); n != 0 {
			t.Fatalf("%d queued effects started while the pool was full", n)
		}

		var overRan atomic.Bool
		over := make(chan error, 1)
		b.Effects.Start(engine.EffectRequest{ID: total + 1, Name: "over", Run: func(context.Context) (any, error) {
			overRan.Store(true)
			return nil, nil
		}}, func(v any, err error) { over <- err })
		if err := recv(t, "the overloaded done", over); !errors.Is(err, engine.ErrOverloaded) || err != engine.ErrOverloaded {
			t.Errorf("overloaded done error = %v, want engine.ErrOverloaded", err)
		}

		// Release one effect at a time: each frees a worker, which starts the oldest queued one.
		for n := range total {
			release <- struct{}{}
			if n < local.EffectQueue {
				if i := recv(t, fmt.Sprintf("queued effect %d to start", n), started); i != local.EffectWorkers+n {
					t.Fatalf("queued effect %d started at position %d", i, n)
				}
			}
		}
		waitFor(t, "every done call", func() bool { return doneCalls.Load() == total })
		time.Sleep(20 * time.Millisecond)
		e.Stop()
		if overRan.Load() {
			t.Error("Run of the overloaded effect was called")
		}
		if n := doneCalls.Load(); n != total {
			t.Errorf("done calls = %d, want %d", n, total)
		}
	})

	t.Run("cancel queued", func(t *testing.T) {
		e, b := start(t, local.Config{Node: "a"})
		release := make(chan struct{})
		var running atomic.Int32
		for i := range local.EffectWorkers {
			b.Effects.Start(engine.EffectRequest{ID: uint64(i + 1), Name: "wait", Run: func(context.Context) (any, error) {
				running.Add(1)
				<-release
				return nil, nil
			}}, func(any, error) {})
		}
		waitFor(t, "the pool to fill", func() bool { return running.Load() == local.EffectWorkers })
		var ran atomic.Bool
		cancel := b.Effects.Start(engine.EffectRequest{ID: 100, Name: "queued", Run: func(context.Context) (any, error) {
			ran.Store(true)
			return nil, nil
		}}, func(any, error) { ran.Store(true) })
		if !cancel() {
			t.Error("Cancel() of a queued effect = false")
		}
		if cancel() {
			t.Error("second Cancel() = true")
		}
		close(release)
		time.Sleep(50 * time.Millisecond)
		e.Stop()
		if ran.Load() {
			t.Error("a cancelled queued effect ran")
		}
	})

	t.Run("cancel overloaded", func(t *testing.T) {
		// A Cancel before the overloaded done runs keeps done from running.
		e, b := start(t, local.Config{Node: "a"})
		release := make(chan struct{})
		var running atomic.Int32
		for i := range local.EffectWorkers + local.EffectQueue {
			b.Effects.Start(engine.EffectRequest{ID: uint64(i + 1), Name: "wait", Run: func(ctx context.Context) (any, error) {
				running.Add(1)
				select { // Stop cancels ctx, so the workers exit with the test
				case <-release:
				case <-ctx.Done():
				}
				return nil, nil
			}}, func(any, error) {})
		}
		waitFor(t, "the pool to fill", func() bool { return running.Load() == local.EffectWorkers })
		g := newGate()
		g.hold(t, b)
		var called atomic.Bool
		cancel := b.Effects.Start(engine.EffectRequest{ID: 9999, Name: "over"}, func(any, error) { called.Store(true) })
		if !cancel() {
			t.Error("Cancel() before the overloaded done ran = false")
		}
		close(g.release)
		time.Sleep(20 * time.Millisecond)
		e.Stop()
		if called.Load() {
			t.Error("done ran after Cancel")
		}
	})
}

// After Stop, waiting effects are dropped, running effects' contexts are cancelled, and no done
// runs, even when Run returns afterwards (ENG-460).
func TestLocalStopEffects(t *testing.T) {
	e, b := start(t, local.Config{Node: "a"})
	var ran, called atomic.Int32
	returned := make(chan struct{}, local.EffectWorkers)
	for i := range local.EffectWorkers + 1 {
		b.Effects.Start(engine.EffectRequest{ID: uint64(i + 1), Name: "wait", Run: func(ctx context.Context) (any, error) {
			ran.Add(1)
			<-ctx.Done()
			returned <- struct{}{}
			return nil, nil
		}}, func(any, error) { called.Add(1) })
	}
	waitFor(t, "the pool to fill", func() bool { return ran.Load() == local.EffectWorkers })
	e.Stop()
	for range local.EffectWorkers {
		recv(t, "a running effect to return", returned)
	}
	time.Sleep(20 * time.Millisecond)
	if n := ran.Load(); n != local.EffectWorkers {
		t.Errorf("Run calls = %d, want %d (the queued effect was dropped)", n, local.EffectWorkers)
	}
	if n := called.Load(); n != 0 {
		t.Errorf("done calls after Stop = %d, want 0", n)
	}
}
