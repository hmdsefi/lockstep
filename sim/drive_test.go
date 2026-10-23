// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/sim"
)

// askInside is the start of the panic message of an Ask inside the simulation (SIM-200).
const askInside = "lockstep: sim: Ask called inside the simulation"

// AT-SIM-01, SIM-070, SIM-150, SIM-185, SIM-200: Ask from body returns Echo's answer; the ask,
// its delivery, the reply and Echo's turn are recorded on n1 in that order after the spawn and
// start, and every turn record ends a lockstep/turn kernel event.
func TestATSIM01EchoAsk(t *testing.T) {
	sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var pid lockstep.PID[Ping]
		n1 := s.Node("n1", func(sys *lockstep.System) { pid = spawn(sys, "echo", echo()) })
		w.RunFor(10 * time.Millisecond)
		v, err := sim.Ask(s, "n1", pid, buildPing(7))
		if v != 7 || err != nil {
			tt.Fatalf("Ask = %d, %v; want 7, nil", v, err)
		}

		recs := w.Sim.Records()
		echoAddr := pid.String()
		want := []struct {
			kind string
			of   func(kernel.Record) bool
		}{
			{"lockstep.spawn", func(r kernel.Record) bool { return attr(r, "addr") == echoAddr }},
			{"lockstep.start", func(r kernel.Record) bool { return attr(r, "actor") == echoAddr }},
			{"lockstep.ask", func(r kernel.Record) bool { return attr(r, "to") == echoAddr }},
			{"lockstep.deliver", func(r kernel.Record) bool { return attr(r, "to") == echoAddr }},
			{"lockstep.reply", func(r kernel.Record) bool { return attr(r, "from") == echoAddr }},
			{"lockstep.turn", func(r kernel.Record) bool { return attr(r, "actor") == echoAddr && attr(r, "user") == "1" }},
		}
		i := 0
		for _, r := range recs {
			if i < len(want) && r.Kind == want[i].kind && want[i].of(r) {
				if r.Node != n1.ID() {
					tt.Errorf("%s record on node %d, want n1 (%d)", r.Kind, r.Node, n1.ID())
				}
				i++
			}
		}
		if i != len(want) {
			tt.Errorf("records hold only the first %d of spawn, start, ask, deliver, reply, turn in order", i)
		}

		var last kernel.Record
		turns := 0
		for _, r := range recs {
			switch r.Kind {
			case "kernel.event":
				last = r
			case "lockstep.turn":
				turns++
				if last.Kind != "kernel.event" || last.Text != "lockstep/turn" {
					tt.Errorf("turn record %d follows %s %q, want a kernel.event labeled lockstep/turn", r.Seq, last.Kind, last.Text)
				}
				if got := attrKeys(r); !slices.Equal(got, []string{"actor", "system", "user", "msg"}) {
					tt.Errorf("turn record %d attribute keys %q, want actor, system, user, msg", r.Seq, got)
				}
				if want := "turn " + attr(r, "actor"); r.Text != want {
					tt.Errorf("turn record %d text %q, want %q", r.Seq, r.Text, want)
				}
			}
		}
		if turns == 0 {
			tt.Error("no lockstep.turn records")
		}
	})
}

// silentSetup spawns an actor at /user/silent that never answers and stores its PID in pid.
func silentSetup(pid *lockstep.PID[Ping]) func(*lockstep.System) {
	return func(sys *lockstep.System) { *pid = spawn(sys, "silent", &actor[Ping]{}) }
}

// AT-SIM-16, SIM-200: an Ask that nobody answers returns ErrTimeout at the ask's timeout, and
// ErrRunEnded at the end of the run when the run ends first.
func TestATSIM16AskTimeoutAndRunEnd(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var pid lockstep.PID[Ping]
			s.Node("n1", silentSetup(&pid))
			w.RunFor(time.Second)
			v, err := sim.Ask(s, "n1", pid, buildPing(7))
			if v != 0 || !errors.Is(err, lockstep.ErrTimeout) {
				tt.Errorf("Ask = %d, %v; want 0 and an error wrapping ErrTimeout", v, err)
			}
			if now, want := w.Sim.Now(), kernel.Time(6*time.Second); now != want {
				tt.Errorf("Now() = %v after Ask, want %v", now, want)
			}
		})
	})
	t.Run("run-ended", func(t *testing.T) {
		opts := fullTrace(5)
		opts.Duration = 3 * time.Second
		sim.Simulate(t, opts, func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var pid lockstep.PID[Ping]
			s.Node("n1", silentSetup(&pid))
			w.RunFor(time.Second)
			v, err := sim.Ask(s, "n1", pid, buildPing(7))
			if v != 0 || err != sim.ErrRunEnded { //nolint:errorlint // Ask returns ErrRunEnded itself
				tt.Errorf("Ask = %d, %v; want 0, ErrRunEnded", v, err)
			}
			if now, want := w.Sim.Now(), w.End(); now != want || want != kernel.Time(3*time.Second) {
				tt.Errorf("Now() = %v after Ask, End() = %v; want both 3s", now, want)
			}
		})
	})
}

// SIM-200: Ask returns the node-down error for a node that is not booted yet, and stops
// running the simulation as soon as the node goes down while the reply is pending.
func TestSIM200NodeDown(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var pid lockstep.PID[Ping]
		n1 := s.Node("n1", silentSetup(&pid))
		const want = "lockstep: sim: node n1 is down: lockstep: stopped"
		if _, err := sim.Ask(s, "n1", pid, buildPing(1)); !errors.Is(err, lockstep.ErrStopped) || err.Error() != want {
			tt.Errorf("Ask before boot: %v, want %q wrapping ErrStopped", err, want)
		}
		if w.Sim.Now() != 0 || w.Sim.Executed() != 0 {
			tt.Errorf("Ask before boot ran the simulation to %v (%d events)", w.Sim.Now(), w.Sim.Executed())
		}
		expectPanic(tt, `lockstep: sim: unknown node "nope"`, func() { _, _ = sim.Ask(s, "nope", pid, buildPing(1)) })

		w.RunFor(time.Second)
		n1.After(time.Second, "test/crash", n1.Crash)
		_, err := sim.Ask(s, "n1", pid, buildPing(1))
		if !errors.Is(err, lockstep.ErrStopped) || err.Error() != want {
			tt.Errorf("Ask across a crash: %v, want %q wrapping ErrStopped", err, want)
		}
		if now, want := w.Sim.Now(), kernel.Time(2*time.Second); now != want {
			tt.Errorf("Now() = %v after Ask, want the crash time %v", now, want)
		}
	})
}

// ask-in-setup calls Ask inside the simulation, from a node's setup, which runs in the node's
// boot event.
var _ = scenario("ask-in-setup", func(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		s.Node("n1", func(sys *lockstep.System) {
			pid := spawn(sys, "a", &actor[Ping]{})
			_, _ = sim.Ask(s, "n1", pid, buildPing(1))
		})
		s.World().RunFor(time.Second)
	})
})

// AT-SIM-17, SIM-200: Ask inside the simulation panics. In a node's setup the panic fails the
// seed; in an actor's Started it is recovered as the actor's failure (SUP-140).
func TestATSIM17AskInsideSimulation(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		got := failingSeed(t, "ask-in-setup")
		if got.Kind != "panic" || !strings.Contains(got.Message, askInside) {
			t.Errorf("failure %+v, want kind panic and a message containing %q", got, askInside)
		}
	})
	t.Run("started", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var panics []string
			s.Node("n1", func(sys *lockstep.System) {
				spawn(sys, "a", &actor[Ping]{started: func(ctx *lockstep.Context[Ping]) {
					defer func() {
						p := recover()
						msg, _ := p.(string)
						panics = append(panics, msg)
						panic(p)
					}()
					_, _ = sim.Ask(s, "n1", ctx.Self(), buildPing(1))
				}})
			})
			w.RunFor(5 * time.Millisecond) // the first start only; restarts back off 10ms
			want := askInside + "; use lockstep.Ask with lockstep.Pipe in actors"
			if !slices.Equal(panics, []string{want}) {
				tt.Errorf("Started panicked with %q, want [%q]", panics, want)
			}
			fails := ofKind(w.Sim.Records(), "lockstep.fail")
			if len(fails) != 1 || attr(fails[0], "err") != "lockstep: panic: "+want {
				tt.Errorf("lockstep.fail records %v, want one with err %q", fails, "lockstep: panic: "+want)
			}
		})
	})
}

// AT-SIM-20, SIM-195, SIM-120: Shutdown from body returns at once; the actors stop in later
// events, after which the engine is dead: the node keeps its stopped system, no turn runs, the
// Tracer records nothing, and AfterFunc and Start schedule nothing.
func TestATSIM20Shutdown(t *testing.T) {
	sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var inReceive []bool
		var pids []lockstep.PID[Deposit]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			pids = pids[:0]
			for _, name := range []string{"a", "b", "c"} {
				pids = append(pids, spawn(sys, name, &actor[Deposit]{receive: func(*lockstep.Context[Deposit], Deposit) {
					inReceive = append(inReceive, s.Engine("n1").Scheduler.InTurn())
				}}))
			}
		})
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", pids[0], Deposit{1}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Millisecond)
		bundle := s.Engine("n1")
		if bundle.Scheduler.InTurn() || !slices.Equal(inReceive, []bool{true}) {
			tt.Errorf("InTurn() in body %t, in Receive %v; want false, [true]", bundle.Scheduler.InTurn(), inReceive)
		}

		sys := s.System("n1")
		executed := w.Sim.Executed()
		if err := sys.Shutdown(context.Background()); err != nil {
			tt.Fatalf("Shutdown: %v", err)
		}
		if n := len(ofKind(w.Sim.Records(), "lockstep.stop")); n != 0 || w.Sim.Executed() != executed {
			tt.Errorf("Shutdown returned after %d lockstep.stop records and %d events, want none", n, w.Sim.Executed()-executed)
		}

		w.RunFor(time.Second)
		var stopped []string
		for _, r := range ofKind(w.Sim.Records(), "lockstep.stop") {
			stopped = append(stopped, attr(r, "actor"))
		}
		slices.Sort(stopped)
		want := []string{pids[0].String(), pids[1].String(), pids[2].String()}
		slices.Sort(want)
		if !slices.Equal(stopped, want) {
			tt.Errorf("lockstep.stop records for %q, want %q", stopped, want)
		}
		if got := s.System("n1"); got != sys {
			tt.Errorf("System(n1) after the shutdown = %p, want the stopped system %p", got, sys)
		}

		// The engine is dead: nothing it is asked to do records or schedules anything.
		recs, next := len(w.Sim.Records()), w.Sim.Executed()
		bundle.Tracer.Record("lockstep.send", engine.Attr{Key: "msg", Value: "x"})
		ran := false
		cancelTimer := bundle.Clock.AfterFunc(time.Millisecond, "lockstep/timer", func() { ran = true })
		cancelEffect := bundle.Effects.Start(engine.EffectRequest{Name: "x"}, func(any, error) {})
		if err := s.Stored(sim.CheckNoEffectHandler); err != nil {
			tt.Errorf("Start on a dead engine stored %v", err)
		}
		if !bundle.Clock.Now().Equal(n1.Now()) {
			tt.Errorf("dead engine Now() = %v, want n1.Now() %v", bundle.Clock.Now(), n1.Now())
		}
		if err := sim.Send(s, "n1", pids[0], Deposit{2}); !errors.Is(err, lockstep.ErrStopped) {
			tt.Errorf("Send to the stopped system: %v, want an error wrapping ErrStopped", err)
		}
		w.RunFor(time.Second)
		if ran || len(w.Sim.Records()) != recs || w.Sim.Executed() != next {
			tt.Errorf("the dead engine ran a timer (%t), emitted %d records and ran %d events; want none",
				ran, len(w.Sim.Records())-recs, w.Sim.Executed()-next)
		}
		if cancelTimer() || cancelEffect() {
			tt.Error("a Cancel of the dead engine reported true")
		}

		// No lockstep/turn event of n1 runs after the shutdown completed.
		all := w.Sim.Records()
		done := slices.IndexFunc(all, func(r kernel.Record) bool {
			return r.Kind == "lockstep.shutdown" && attr(r, "phase") == "done"
		})
		if done < 0 {
			tt.Fatal("no lockstep.shutdown phase=done record")
		}
		for _, r := range all[done+1:] {
			if r.Kind == "kernel.event" && r.Node == n1.ID() && r.Text == "lockstep/turn" {
				tt.Errorf("lockstep/turn event %d runs after the shutdown completed", r.Seq)
			}
		}
	})
}
