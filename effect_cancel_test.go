// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// cancelSpy wraps a manual engine's Clock and Effects and logs, in order, the cancellation of
// every effect timeout timer and every engine Cancel that did something, each with the number of
// lockstep.effect.done records emitted so far.
type cancelSpy struct {
	engine.Effects
	engine.Clock
	recs func() []engine.Record
	log  []string
}

func (s *cancelSpy) note(what string) {
	s.log = append(s.log, fmt.Sprintf("%s(%d)", what, len(effectDones(s.recs()))))
}

func (s *cancelSpy) AfterFunc(d time.Duration, label string, fn func()) engine.Cancel {
	c := s.Clock.AfterFunc(d, label, fn)
	if label != effectTimeoutLabel {
		return c
	}
	return func() bool {
		s.note("timer-cancel")
		return c()
	}
}

func (s *cancelSpy) Start(req engine.EffectRequest, done func(v any, err error)) engine.Cancel {
	c := s.Effects.Start(req, done)
	return func() bool {
		s.note("engine-cancel")
		return c()
	}
}

// newSpiedSystem returns a manual system whose engine's effect timers and Cancels are logged.
func newSpiedSystem(t *testing.T) (*System, *enginetest.Engine, *cancelSpy) {
	t.Helper()
	eng := enginetest.New("local", 1)
	b := eng.Engine()
	spy := &cancelSpy{Effects: b.Effects, Clock: b.Clock, recs: eng.Records}
	b.Effects, b.Clock = spy, spy
	sys, err := newSystem(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	return sys, eng, spy
}

// EFF-100: stop and restart cancel every pending effect of the ending incarnation in ID order:
// the timeout timer, then the engine's Cancel, then the cancelled record; the records come before
// the Stopped hook's and lockstep.stop, and the old effects never reach any incarnation.
func TestEFF100CancelOnStopAndRestart(t *testing.T) {
	wantLog := []string{
		"timer-cancel(0)", "engine-cancel(0)", // effect 1
		"timer-cancel(1)", "engine-cancel(1)", // effect 2
		"timer-cancel(2)", "engine-cancel(2)", // effect 3
	}
	startThree := func(t *testing.T, sys *System, eng *enginetest.Engine, pid PID[any]) {
		t.Helper()
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			doFetch(ctx, "A")
			doFetch(ctx, "B", WithEffectTimeout(time.Second))
			doFetch(ctx, "C", WithEffectTimeout(time.Minute))
		})
		if n := len(eng.Pending()); n != 3 {
			t.Fatalf("%d effects pending, want 3", n)
		}
	}
	checkCancelled := func(t *testing.T, eng *enginetest.Engine, inc string) {
		t.Helper()
		dones := effectDones(eng.Records())
		if len(dones) != 3 {
			t.Fatalf("%d lockstep.effect.done records, want 3: %v", len(dones), dones)
		}
		for i, d := range dones {
			want := fmt.Sprint(i + 1)
			if d["effect"] != want || d["outcome"] != "cancelled" || d["inc"] != inc || d["name"] != "prices.fetch" || d["err"] != "" {
				t.Errorf("done record %d = %v, want effect=%s inc=%s outcome=cancelled err=\"\"", i, d, want, inc)
			}
		}
		if n := len(eng.Pending()); n != 0 {
			t.Errorf("%d effects still pending in the engine, want 0", n)
		}
	}

	t.Run("stop", func(t *testing.T) {
		sys, eng, spy := newSpiedSystem(t)
		a := &effectActor{}
		donesInStopped := -1
		a.stopped = func(*Context[any], StopReason) { donesInStopped = len(effectDones(eng.Records())) }
		pid, err := Spawn(sys, "q", actorOf[any](a))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		startThree(t, sys, eng, pid)
		if err := Stop(sys, pid.Address()); err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		checkCancelled(t, eng, "1")
		if !slices.Equal(spy.log, wantLog) {
			t.Errorf("cancellations = %q, want %q", spy.log, wantLog)
		}
		if donesInStopped != 3 {
			t.Errorf("Stopped saw %d cancelled records, want 3 before it ran", donesInStopped)
		}
		// Every cancelled record precedes lockstep.stop.
		recs := eng.Records()
		stopAt, lastDone := -1, -1
		for i, r := range recs {
			switch r.Kind {
			case "lockstep.stop":
				stopAt = i
			case "lockstep.effect.done":
				lastDone = i
			}
		}
		if stopAt < 0 || lastDone > stopAt {
			t.Errorf("last effect.done at %d, lockstep.stop at %d: want the cancelled records first", lastDone, stopAt)
		}
		eng.Advance(time.Hour)
		eng.RunUntilIdle()
		if n := len(effectDones(eng.Records())); n != 3 {
			t.Errorf("%d done records after the timeouts' time passed, want 3", n)
		}
	})

	t.Run("restart", func(t *testing.T) {
		sys, eng, spy := newSpiedSystem(t)
		pid, a := spawnEffectActor(t, sys, eng)
		startThree(t, sys, eng, pid)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.Fail(errors.New("boom")) })
		eng.RunUntilIdle()
		checkCancelled(t, eng, "1")
		if !slices.Equal(spy.log, wantLog) {
			t.Errorf("cancellations = %q, want %q", spy.log, wantLog)
		}
		eng.Advance(time.Hour)
		eng.RunUntilIdle()
		if _, inc := cellOf(t, sys, pid).lifecycle(); inc != 2 {
			t.Fatalf("incarnation = %d, want 2", inc)
		}
		for id := uint64(1); id <= 3; id++ {
			if eng.CompleteEffect(id, int64(7), nil) {
				t.Errorf("the engine still held effect %d", id)
			}
		}
		eng.RunUntilIdle()
		if len(a.got) != 0 {
			t.Errorf("the new incarnation got %v for the old effects", a.got)
		}
		if n := len(effectDones(eng.Records())); n != 3 {
			t.Errorf("%d done records, want exactly the 3 cancelled", n)
		}
		if n := len(effectRecords(eng.Records(), "lockstep.drop")); n != 0 {
			t.Errorf("%d drop records, want none: the effects were cancelled, not dropped", n)
		}
		// The new incarnation's effects are its own, and its table starts empty.
		var id EffectID
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { id = doFetch(ctx, "D") })
		if id != 4 || len(eng.Pending()) != 1 {
			t.Errorf("Do after the restart = %d with %d pending, want 4 and 1", id, len(eng.Pending()))
		}
	})

	t.Run("a result already delivered", func(t *testing.T) {
		// The outcome is delivered, so it is the effect's one outcome: the stop leaves it alone and the
		// result entry of the ended incarnation is dropped when popped (MBX-170).
		sys, eng := newManualSystem(t)
		pid, a := spawnEffectActor(t, sys, eng)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { doFetch(ctx, "A"); doFetch(ctx, "B") })
		release := hold(t, sys, pid.Address())
		send(t, sys, pid, any(func(ctx *Context[any]) { ctx.Fail(errors.New("boom")) }))
		eng.CompleteEffect(1, int64(5), nil)
		release()
		eng.RunUntilIdle()
		eng.Advance(time.Second)
		eng.RunUntilIdle()
		dones := effectDones(eng.Records())
		if len(dones) != 2 || dones[0]["effect"] != "1" || dones[0]["outcome"] != "ok" || dones[1]["effect"] != "2" || dones[1]["outcome"] != "cancelled" {
			t.Errorf("done records = %v, want effect 1 ok then effect 2 cancelled", dones)
		}
		if len(a.got) != 0 {
			t.Errorf("the new incarnation got %v", a.got)
		}
	})
}

// EFF-110: CancelEffect cancels one pending effect of the calling incarnation, like EFF-100, and
// returns true; it returns false, and records nothing, for 0, a second call, an ID of another
// incarnation and an effect that has an outcome.
func TestEFF110CancelEffect(t *testing.T) {
	t.Run("one turn", func(t *testing.T) {
		sys, eng, spy := newSpiedSystem(t)
		pid, a := spawnEffectActor(t, sys, eng)
		var id EffectID
		var got [3]bool
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			id = doFetch(ctx, "X")
			got[0] = ctx.CancelEffect(id)
			got[1] = ctx.CancelEffect(id)
			got[2] = ctx.CancelEffect(0)
		})
		if id != 1 || got != [3]bool{true, false, false} {
			t.Errorf("Do = %d, CancelEffect = %v, want 1 and [true false false]", id, got)
		}
		dones := effectDones(eng.Records())
		if len(dones) != 1 || dones[0]["effect"] != "1" || dones[0]["outcome"] != "cancelled" || dones[0]["err"] != "" || dones[0]["inc"] != "1" {
			t.Errorf("done records = %v, want one outcome=cancelled for effect 1", dones)
		}
		if want := []string{"timer-cancel(0)", "engine-cancel(0)"}; !slices.Equal(spy.log, want) {
			t.Errorf("cancellations = %q, want %q", spy.log, want)
		}
		if n := len(eng.Pending()); n != 0 {
			t.Errorf("%d effects pending in the engine, want 0", n)
		}
		eng.Advance(time.Hour)
		eng.RunUntilIdle()
		if len(a.got) != 0 || len(effectDones(eng.Records())) != 1 {
			t.Errorf("after the timeout's time: got %v, %d done records; want nothing more", a.got, len(effectDones(eng.Records())))
		}
	})

	t.Run("only the named effect", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, a := spawnEffectActor(t, sys, eng)
		var ids [3]EffectID
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			for i := range ids {
				ids[i] = doFetch(ctx, "X")
			}
			ctx.CancelEffect(ids[1])
		})
		if len(eng.Pending()) != 2 {
			t.Fatalf("%d effects pending, want 2", len(eng.Pending()))
		}
		eng.CompleteEffect(1, int64(1), nil)
		eng.CompleteEffect(3, int64(3), nil)
		eng.RunUntilIdle()
		if len(a.got) != 2 || fetchedAt(t, a, 0).v != 1 || fetchedAt(t, a, 1).v != 3 {
			t.Errorf("got %v, want the results of effects 1 and 3 only", a.got)
		}
	})

	t.Run("an outcome exists", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, a := spawnEffectActor(t, sys, eng)
		var id EffectID
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { id = doFetch(ctx, "X") })
		// The result is delivered and waits in the mailbox: the effect has its outcome.
		release := hold(t, sys, pid.Address())
		eng.CompleteEffect(uint64(id), int64(9), nil)
		var cancelled bool
		send(t, sys, pid, any(func(ctx *Context[any]) { cancelled = ctx.CancelEffect(id) }))
		release()
		eng.RunUntilIdle()
		if cancelled {
			t.Error("CancelEffect of an effect with a delivered outcome = true")
		}
		if len(a.got) != 1 || fetchedAt(t, a, 0).v != 9 {
			t.Errorf("got %v, want the result 9 still delivered", a.got)
		}
		if dones := effectDones(eng.Records()); len(dones) != 1 || dones[0]["outcome"] != "ok" {
			t.Errorf("done records = %v, want the one outcome=ok", dones)
		}
	})

	t.Run("timed out", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, _ := spawnEffectActor(t, sys, eng)
		var id EffectID
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { id = doFetch(ctx, "X", WithEffectTimeout(time.Second)) })
		eng.Advance(time.Second)
		eng.RunUntilIdle()
		var cancelled bool
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { cancelled = ctx.CancelEffect(id) })
		if cancelled {
			t.Error("CancelEffect of a timed-out effect = true")
		}
		if dones := effectDones(eng.Records()); len(dones) != 1 || dones[0]["outcome"] != "timeout" {
			t.Errorf("done records = %v, want the one outcome=timeout", dones)
		}
	})

	t.Run("another incarnation", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, _ := spawnEffectActor(t, sys, eng)
		var old EffectID
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { old = doFetch(ctx, "X") })
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.Fail(errors.New("boom")) })
		eng.Advance(time.Second)
		eng.RunUntilIdle()
		var cancelled bool
		var fresh EffectID
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			cancelled = ctx.CancelEffect(old)
			fresh = doFetch(ctx, "Y")
		})
		if cancelled {
			t.Error("CancelEffect of the previous incarnation's effect = true")
		}
		// The effect's own incarnation is checked too: the ID of an effect that belongs to another
		// incarnation is not pending for this one.
		if fresh != 2 || len(eng.Pending()) != 1 {
			t.Errorf("fresh effect = %d with %d pending, want 2 and 1", fresh, len(eng.Pending()))
		}
		if dones := effectDones(eng.Records()); len(dones) != 1 || dones[0]["effect"] != "1" {
			t.Errorf("done records = %v, want only the restart's cancelled effect 1", dones)
		}
	})

	t.Run("outside the turn panics", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var kept *Context[any]
		pid, _ := spawnEffectActor(t, sys, eng)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { kept = ctx })
		defer func() {
			if recover() == nil {
				t.Error("CancelEffect outside the turn did not panic")
			}
		}()
		kept.CancelEffect(1)
	})
}
