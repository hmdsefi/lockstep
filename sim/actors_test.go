// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"slices"
	"testing"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// fullTrace returns Options that run seeds seeds and keep every record (TraceFull), so tests can
// read w.Sim.Records().
func fullTrace(seeds int) sim.Options {
	return sim.Options{Options: faultline.Options{Seeds: seeds, Trace: kernel.TraceConfig{Level: kernel.TraceFull}}}
}

// actor is an actor built from optional functions.
type actor[M any] struct {
	started func(ctx *lockstep.Context[M])
	receive func(ctx *lockstep.Context[M], m M)
}

func (a *actor[M]) Started(ctx *lockstep.Context[M]) {
	if a.started != nil {
		a.started(ctx)
	}
}

func (a *actor[M]) Receive(ctx *lockstep.Context[M], m M) {
	if a.receive != nil {
		a.receive(ctx, m)
	}
}

// spawn spawns a at /user/<name> from sp and returns its PID. It panics on an error, which fails
// the seed: it runs in kernel callbacks, where a test must not stop the goroutine.
func spawn[M any](sp lockstep.Spawner, name string, a *actor[M], opts ...lockstep.SpawnOption) lockstep.PID[M] {
	pid, err := lockstep.Spawn(sp, name, func() lockstep.Actor[M] { return a }, opts...)
	if err != nil {
		panic(err)
	}
	return pid
}

// Ping asks for N back.
type Ping struct {
	N       int
	ReplyTo lockstep.ReplyTo[int]
}

// buildPing returns the build function of an ask for n.
func buildPing(n int) func(lockstep.ReplyTo[int]) Ping {
	return func(r lockstep.ReplyTo[int]) Ping { return Ping{N: n, ReplyTo: r} }
}

// echo answers every Ping with its N.
func echo() *actor[Ping] {
	return &actor[Ping]{receive: func(ctx *lockstep.Context[Ping], m Ping) { m.ReplyTo.Send(ctx, m.N) }}
}

// Deposit is a plain user message.
type Deposit struct{ N int }

// noSetup is a node setup that spawns nothing.
func noSetup(*lockstep.System) {}

// expectPanic calls fn and fails t unless it panics with want.
func expectPanic(t testing.TB, want string, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		if got := recover(); got != want {
			t.Errorf("panic %v, want %q", got, want)
		}
	}()
	fn()
}

// ofKind returns the records of kind, in order.
func ofKind(recs []kernel.Record, kind string) []kernel.Record {
	var out []kernel.Record
	for _, r := range recs {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// attrKeys returns the attribute keys of r, in order.
func attrKeys(r kernel.Record) []string {
	keys := make([]string, len(r.Attrs))
	for i, a := range r.Attrs {
		keys[i] = a.Key
	}
	return keys
}

// attr returns the value of r's attribute key, or "".
func attr(r kernel.Record, key string) string {
	i := slices.IndexFunc(r.Attrs, func(a kernel.Attr) bool { return a.Key == key })
	if i < 0 {
		return ""
	}
	return r.Attrs[i].Value
}
