// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// fnActor is a test actor whose handlers are functions; a nil function does nothing.
type fnActor[M any] struct {
	started func(ctx *Context[M])
	receive func(ctx *Context[M], msg M)
}

func (a *fnActor[M]) Started(ctx *Context[M]) {
	if a.started != nil {
		a.started(ctx)
	}
}

func (a *fnActor[M]) Receive(ctx *Context[M], msg M) {
	if a.receive != nil {
		a.receive(ctx, msg)
	}
}

// idle returns a factory of actors that do nothing.
func idle[M any]() func() Actor[M] {
	return func() Actor[M] { return &fnActor[M]{} }
}

// actorOf returns a factory that returns a every time.
func actorOf[M any](a Actor[M]) func() Actor[M] {
	return func() Actor[M] { return a }
}

// cellOf returns the cell registered for pid.
func cellOf[M any](t *testing.T, sys *System, pid PID[M]) *cell[M] {
	t.Helper()
	uid, a, ok := sys.reg.Lookup(pid.Address().Path())
	if !ok || uid != pid.Address().UID() {
		t.Fatalf("%v is not registered", pid)
	}
	c, ok := a.(*cell[M])
	if !ok {
		t.Fatalf("registry holds %T for %v", a, pid)
	}
	return c
}

// send puts msg in pid's user mailbox as a sent message from the system (until Send exists).
func send[M any](t *testing.T, sys *System, pid PID[M], msg M) {
	t.Helper()
	cellOf(t, sys, pid).enqueueUser(entry{msg: msg, from: sys.user, kind: entrySent})
}

// recordsOf returns the attributes of recs' records of kind, each as key=value strings.
func recordsOf(recs []engine.Record, kind string) [][]string {
	var out [][]string
	for _, r := range recs {
		if r.Kind != kind {
			continue
		}
		attrs := make([]string, 0, len(r.Attrs))
		for _, a := range r.Attrs {
			attrs = append(attrs, a.Key+"="+a.Value)
		}
		out = append(out, attrs)
	}
	return out
}

// ACT-110, ACT-120: one *Context per incarnation, passed to every turn; Self and Parent work at any
// time; Spawn with the context outside its turn panics.
func TestContextIdentityAndInTurn(t *testing.T) {
	sys, eng := newManualSystem(t)
	var ctxs []*Context[int]
	var inTurn []bool
	a := &fnActor[int]{
		started: func(ctx *Context[int]) {
			ctxs = append(ctxs, ctx)
			inTurn = append(inTurn, ctx.inTurn.Load())
		},
		receive: func(ctx *Context[int], _ int) {
			ctxs = append(ctxs, ctx)
			inTurn = append(inTurn, ctx.inTurn.Load())
		},
	}
	pid, err := Spawn(sys, "a", actorOf[int](a))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, pid, 1)
	eng.RunUntilIdle()
	send(t, sys, pid, 2)
	eng.RunUntilIdle()

	if len(ctxs) != 3 {
		t.Fatalf("%d handler calls, want 3 (Started and two Receive)", len(ctxs))
	}
	for i, ctx := range ctxs {
		if ctx != ctxs[0] {
			t.Errorf("call %d got context %p, want %p", i, ctx, ctxs[0])
		}
		if !inTurn[i] {
			t.Errorf("call %d: in-turn flag clear during the handler", i)
		}
	}
	ctx := ctxs[0]
	if ctx.inTurn.Load() {
		t.Error("in-turn flag set after the turn")
	}
	if got := ctx.Self(); got != pid {
		t.Errorf("Self() = %v, want %v", got, pid)
	}
	if got := ctx.Parent(); got != sys.user {
		t.Errorf("Parent() = %v, want the user guardian %v", got, sys.user)
	}
	v := mustPanic(t, func() { _, _ = Spawn(ctx, "c", idle[int]()) })
	if want := "lockstep: Context of " + pid.String() + " used outside its turn"; v != want {
		t.Errorf("Spawn outside the turn panicked with %v, want %q", v, want)
	}
	if n := len(recordsOf(eng.Records(), "lockstep.spawn")); n != 1 {
		t.Errorf("%d lockstep.spawn records, want 1 (the panicking Spawn records nothing)", n)
	}
}
