// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// T is a payload with no Describe, String or Error method, so kernel.Describe prints its type.
type T struct{}

// seven is a slog.LogValuer whose value is 7.
type seven struct{}

func (seven) LogValue() slog.Value { return slog.IntValue(7) }

// kernelLogs returns the texts of node's kernel.log records, in order.
func kernelLogs(recs []kernel.Record, node kernel.NodeID) []string {
	var out []string
	for _, r := range ofKind(recs, "kernel.log") {
		if r.Node == node {
			out = append(out, r.Text)
		}
	}
	return out
}

// actorAttrs returns the actor attributes of pid on node n1 as the kernel log prints them.
func actorAttrs[M any](pid lockstep.PID[M]) string {
	return fmt.Sprintf("node=n1 path=%s uid=%d", pid.Address().Path(), pid.Address().UID())
}

// logOnce runs a simulation in which node n1's actor /user/a calls logs in its first turn, and
// returns the texts of n1's kernel.log records, the actor attributes and the trace hash.
func logOnce(t *testing.T, opts sim.Options, seed uint64, logs func(ctx *lockstep.Context[Deposit])) (texts []string, ids string, hash uint64) {
	t.Helper()
	sim.SimulateSeeds(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var a lockstep.PID[Deposit]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			a = spawn(sys, "a", &actor[Deposit]{receive: func(ctx *lockstep.Context[Deposit], _ Deposit) { logs(ctx) }})
		})
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", a, Deposit{1}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Millisecond)
		texts, ids, hash = kernelLogs(w.Sim.Records(), n1.ID()), actorAttrs(a), w.Sim.TraceHash()
	}, seed)
	return texts, ids, hash
}

// AT-OBS-13, OBS-155, OBS-160: in simulation ctx.Logger() and ctx.Logf write their Info records
// to the node's kernel log with the actor attributes first, a Debug record emits nothing, a
// WithLogger in Options.System is overridden, and two runs of a seed give the same trace hash.
func TestATOBS13SimulationLogger(t *testing.T) {
	var buf bytes.Buffer
	opts := fullTrace(0)
	opts.System = []lockstep.Option{lockstep.WithLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))}
	logs := func(ctx *lockstep.Context[Deposit]) {
		ctx.Logger().Info("hi", "k", 1, "p", &T{})
		ctx.Logf("x=%d", 2)
		ctx.Logger().Debug("debug")
	}

	var hashes []uint64
	for i := range 2 {
		t.Run(fmt.Sprintf("run-%d", i+1), func(t *testing.T) {
			texts, ids, hash := logOnce(t, opts, 7, logs)
			want := []string{
				"INFO hi " + ids + " k=1 p=" + kernel.Describe(&T{}),
				"INFO x=2 " + ids,
			}
			if !slices.Equal(texts, want) {
				t.Errorf("kernel.log texts of n1:\n%q\nwant\n%q", texts, want)
			}
			hashes = append(hashes, hash)
		})
	}
	if len(hashes) != 2 || hashes[0] != hashes[1] {
		t.Errorf("trace hashes of seed 7: %#x, want two equal ones", hashes)
	}
	if buf.Len() != 0 {
		t.Errorf("the logger in Options.System received %q, want nothing", buf.String())
	}
}

// OBS-160: the kernel-log text of every attribute form. Attributes of With come before the
// record's, groups are flattened as group.key, a group with an empty key is inlined, an empty
// attribute or group is left out, a LogValuer is resolved, KindAny values print with
// kernel.Describe and the others with slog.Value.String, LEVEL is slog.Level.String(), and a
// record below Info emits nothing.
func TestKernelLogText(t *testing.T) {
	texts, ids, _ := logOnce(t, fullTrace(0), 3, func(ctx *lockstep.Context[Deposit]) {
		l := ctx.Logger()
		l.Warn("w", slog.Group("g", "a", 1, slog.Group("h", "b", true)), slog.Group("", "c", "x y"))
		l.WithGroup("q").With("r", 2*time.Second).Log(context.Background(), slog.LevelError+2, "e",
			"err", errors.New("boom"), "v", seven{}, slog.Attr{}, slog.Group("none"))
		l.With("s", "t").WithGroup("empty").Info("eg")
		l.WithGroup("").Error("same", "u", uint64(5), "f", 1.5, "d", Deposit{4})
		l.Log(context.Background(), slog.LevelInfo-1, "below")
	})
	want := []string{
		"WARN w " + ids + " g.a=1 g.h.b=true c=x y",
		"ERROR+2 e " + ids + " q.r=2s q.err=boom q.v=7",
		"INFO eg " + ids + " s=t",
		"ERROR same " + ids + " u=5 f=1.5 d=" + kernel.Describe(Deposit{4}),
	}
	if !slices.Equal(texts, want) {
		t.Errorf("kernel.log texts of n1:\n%q\nwant\n%q", texts, want)
	}
}

// incLog is one kernel.log record of a node: its incarnation and text.
type incLog struct {
	inc  uint32
	text string
}

// kernelLogsByInc returns node's kernel.log records with their incarnations, in order.
func kernelLogsByInc(recs []kernel.Record, node kernel.NodeID) []incLog {
	var out []incLog
	for _, r := range ofKind(recs, "kernel.log") {
		if r.Node == node {
			out = append(out, incLog{r.Inc, r.Text})
		}
	}
	return out
}

// SIM-120, OBS-160: a logger kept from a node's first incarnation writes nothing once the node
// crashed, neither while it is down nor after its restart, when the kernel would stamp its
// records with the new incarnation. The actor of the new incarnation logs through its own logger.
func TestKernelLogAfterCrash(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var a lockstep.PID[Deposit]
		var kept *slog.Logger
		n1 := s.Node("n1", func(sys *lockstep.System) {
			a = spawn(sys, "a", &actor[Deposit]{receive: func(ctx *lockstep.Context[Deposit], m Deposit) {
				ctx.Logger().Info("turn", "n", m.N)
				if kept == nil {
					kept = ctx.Logger()
				}
			}})
		})
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", a, Deposit{1}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Millisecond)
		first := a
		kept.Info("up")

		now := w.Sim.Now()
		w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
		w.Sim.At(now.Add(2*time.Millisecond), "test/restart", n1.Restart)
		w.RunFor(1500 * time.Microsecond)
		kept.Info("down")
		w.RunFor(time.Millisecond)
		if n1.Incarnation() != 2 || a == first {
			tt.Fatalf("n1 is in incarnation %d with /user/a %v (first %v), want a restart to 2 with a new PID", n1.Incarnation(), a, first)
		}
		kept.Info("restarted")
		if err := sim.Send(s, "n1", a, Deposit{2}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Millisecond)

		want := []incLog{
			{1, "INFO turn " + actorAttrs(first) + " n=1"},
			{1, "INFO up " + actorAttrs(first)},
			{2, "INFO turn " + actorAttrs(a) + " n=2"},
		}
		if got := kernelLogsByInc(w.Sim.Records(), n1.ID()); !slices.Equal(got, want) {
			tt.Errorf("kernel.log records of n1 %v, want %v", got, want)
		}
	})
}

// OBS-155, OBS-060: a node system logs no runtime event, although its logger writes to the
// kernel log. An actor that panics and is restarted, a child that panics and is stopped by its
// parent's decider, and the system's shutdown leave their lockstep.* records, and the kernel log
// holds only the lines the actors log themselves: no actor failed, actor restarted or shutdown
// started line.
func TestKernelLogNoRuntimeEvents(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var r, c lockstep.PID[Deposit]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			r = spawn(sys, "r", &actor[Deposit]{
				started: func(ctx *lockstep.Context[Deposit]) { ctx.Logger().Info("started") },
				receive: func(*lockstep.Context[Deposit], Deposit) { panic("boom") },
			})
			stop := lockstep.OneForOne(func(error) lockstep.Directive { return lockstep.DirectiveStop })
			spawn(sys, "p", &actor[Deposit]{started: func(ctx *lockstep.Context[Deposit]) {
				c = spawn(ctx, "c", &actor[Deposit]{receive: func(*lockstep.Context[Deposit], Deposit) { panic("boom") }})
			}}, lockstep.WithStrategy(stop))
		})
		w.RunFor(time.Millisecond)
		for _, pid := range []lockstep.PID[Deposit]{r, c} {
			if err := sim.Send(s, "n1", pid, Deposit{1}); err != nil {
				tt.Fatal(err)
			}
		}
		w.RunFor(time.Second)
		if err := s.System("n1").Shutdown(context.Background()); err != nil {
			tt.Fatalf("Shutdown: %v", err)
		}
		w.RunFor(time.Second)

		recs := w.Sim.Records()
		if n := len(ofKind(recs, "lockstep.fail")); n != 2 {
			tt.Errorf("%d lockstep.fail records, want 2", n)
		}
		if rs := ofKind(recs, "lockstep.restart"); len(rs) != 1 || attr(rs[0], "actor") != r.String() {
			tt.Errorf("lockstep.restart records %v, want one for %v", rs, r)
		}
		failed := slices.IndexFunc(ofKind(recs, "lockstep.stop"), func(rec kernel.Record) bool {
			return attr(rec, "actor") == c.String() && attr(rec, "reason") == "failed"
		})
		if failed < 0 {
			tt.Errorf("no lockstep.stop record with reason failed for %v", c)
		}
		started := "INFO started " + actorAttrs(r)
		if got, want := kernelLogs(recs, n1.ID()), []string{started, started}; !slices.Equal(got, want) {
			tt.Errorf("kernel.log texts of n1 %q, want only the actor's own lines %q", got, want)
		}
	})
}

// AT-OBS-16 (simulation half), OBS-145: on the paths of AT-OBS-05 (a full FailSend mailbox, a
// full DropNewest mailbox, a send to an actor whose stop has completed), the policy and reason
// attributes of the lockstep.mailbox.full, lockstep.deadletter and lockstep.stop records are
// values of OBS-145's table, and each path's value is there.
func TestATOBS16SimulationSpellings(t *testing.T) {
	policies := []string{"fail-send", "drop-newest", "drop-oldest", "block-sender"}
	deadReasons := []string{"stopped", "not-found", "unreachable"}
	stopReasons := []string{"normal", "failed", "parent-stopping", "shutdown"}

	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var fs, dn, gone lockstep.PID[Deposit]
		s.Node("n1", func(sys *lockstep.System) {
			fs = spawn(sys, "fs", &actor[Deposit]{}, lockstep.WithMailbox(1, lockstep.FailSend))
			dn = spawn(sys, "dn", &actor[Deposit]{}, lockstep.WithMailbox(1, lockstep.DropNewest))
			gone = spawn(sys, "gone", &actor[Deposit]{})
		})
		w.RunFor(time.Millisecond)

		// Two sends in one step: the first fills the mailbox before the actor's turn runs.
		if err := sim.Send(s, "n1", fs, Deposit{1}); err != nil {
			tt.Fatal(err)
		}
		if err := sim.Send(s, "n1", fs, Deposit{2}); !errors.Is(err, lockstep.ErrMailboxFull) {
			tt.Errorf("second Send to the full FailSend mailbox = %v, want ErrMailboxFull", err)
		}
		for i := range 2 {
			if err := sim.Send(s, "n1", dn, Deposit{i}); err != nil {
				tt.Fatal(err)
			}
		}
		if err := lockstep.Stop(s.System("n1"), gone.Address()); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Millisecond)
		_ = sim.Send(s, "n1", gone, Deposit{3}) // a dead letter
		w.RunFor(time.Millisecond)

		recs := w.Sim.Records()
		check := func(kind, key string, table []string, want ...string) {
			var got []string
			for _, r := range ofKind(recs, kind) {
				v := attr(r, key)
				if !slices.Contains(table, v) {
					tt.Errorf("%s record with %s=%q, not a value of OBS-145's table %q", kind, key, v, table)
				}
				got = append(got, v)
			}
			for _, v := range want {
				if !slices.Contains(got, v) {
					tt.Errorf("%s records have %s values %q, want %q among them", kind, key, got, v)
				}
			}
		}
		check("lockstep.mailbox.full", "policy", policies, "fail-send", "drop-newest")
		check("lockstep.deadletter", "reason", deadReasons, "stopped")
		check("lockstep.stop", "reason", stopReasons, "normal")
	})
}
