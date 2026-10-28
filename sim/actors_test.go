// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"bytes"
	"log/slog"
	"slices"
	"testing"
	"time"

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

// AT-ACT-05, ACT-040, ACT-045, ACT-420: kv, the first actor spawned in n1's setup, has uid number 3
// in every incarnation of the node (after the two guardians), and the incarnation number in the
// high 32 bits. A message to the old PID after the restart is a dead letter on n1 and reaches no
// actor.
func TestATACT05UidEpochInSimulation(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var kv lockstep.PID[Deposit]
		received := 0
		n1 := s.Node("n1", func(sys *lockstep.System) {
			kv = spawn(sys, "kv", &actor[Deposit]{receive: func(*lockstep.Context[Deposit], Deposit) { received++ }})
		})
		s.Node("n2", noSetup)
		w.RunFor(time.Millisecond)
		first, inc1 := kv, s.System("n1").Incarnation()

		n1.Crash()
		w.RunFor(time.Millisecond)
		n1.Restart()
		w.RunFor(time.Millisecond)
		second, inc2 := kv, s.System("n1").Incarnation()

		u1, u2 := first.Address().UID(), second.Address().UID()
		if u1>>32 != 1 || inc1 != 1 || u2>>32 != 2 || inc2 != 2 {
			tt.Errorf("uids %#x and %#x with incarnations %d and %d; want high 32 bits and incarnation 1, then 2", u1, u2, inc1, inc2)
		}
		if u1&0xffffffff != 3 || u2&0xffffffff != 3 {
			tt.Errorf("uids %#x and %#x, want the low 32 bits to be 3 in both", u1, u2)
		}

		if err := sim.Send(s, "n2", first, Deposit{1}); err != nil {
			tt.Fatalf("Send to the old PID: %v", err)
		}
		w.RunFor(time.Second)
		dead := 0
		for _, r := range ofKind(w.Sim.Records(), "lockstep.deadletter") {
			if r.Node == n1.ID() && attr(r, "to") == first.String() {
				dead++
			}
		}
		if dead != 1 || received != 0 {
			tt.Errorf("%d dead letters on n1 for the old PID and %d deliveries to kv, want 1 and 0", dead, received)
		}
	})
}

// AT-ACT-12 (sim clause), ACT-130: ctx.Logf in a simulated actor adds a lockstep.log record,
// reaches the node's kernel log (OBS-160) and writes nothing to slog.Default().
func TestATACT12LogfInSimulation(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	slog.Info("control") // the handler writes
	if buf.Len() == 0 {
		t.Fatal("the buffer handler wrote nothing for a control entry")
	}
	buf.Reset()

	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var a lockstep.PID[Deposit]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			a = spawn(sys, "a", &actor[Deposit]{receive: func(ctx *lockstep.Context[Deposit], m Deposit) {
				ctx.Logf("n=%d", m.N)
			}})
		})
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", a, Deposit{3}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Millisecond)

		logs := ofKind(w.Sim.Records(), "lockstep.log")
		if len(logs) != 1 || attr(logs[0], "addr") != a.Address().String() || attr(logs[0], "text") != "n=3" {
			tt.Errorf("lockstep.log records %v, want one with addr %s and text n=3", logs, a.Address())
		}
		want := []string{"INFO n=3 " + actorAttrs(a)}
		if got := kernelLogs(w.Sim.Records(), n1.ID()); !slices.Equal(got, want) {
			tt.Errorf("kernel.log texts of n1 %q, want %q", got, want)
		}
	})
	if buf.Len() != 0 {
		t.Errorf("slog.Default() received %q, want nothing in simulation", buf.String())
	}
}
