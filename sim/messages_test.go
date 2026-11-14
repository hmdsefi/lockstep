// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// The simulation clauses of the message acceptance tests share the helpers below. A body runs
// again for a determinism check, so every test declares what it records inside its body.

// askStep is the message of an asker. With done unset it tells the asker to ask to for n, with
// timeout when that is above 0; with done set it carries the resolution of that ask back from Pipe.
type askStep struct {
	to      lockstep.PID[Ping]
	n       int
	timeout time.Duration
	done    bool
	v       int
	err     error
	at      kernel.Time // when the resolution reached the asker
}

// asker returns an actor that asks for what its askSteps say, pipes each reply back to itself and
// appends the resolutions to *out. now reads the virtual time.
func asker(now func() kernel.Time, out *[]askStep) *actor[askStep] {
	return &actor[askStep]{receive: func(ctx *lockstep.Context[askStep], m askStep) {
		if m.done {
			m.at = now()
			*out = append(*out, m)
			return
		}
		var opts []lockstep.AskOption
		if m.timeout > 0 {
			opts = append(opts, lockstep.WithTimeout(m.timeout))
		}
		r := lockstep.Ask(ctx, m.to, buildPing(m.n), opts...)
		lockstep.Pipe(ctx, r, func(v int, err error) askStep { return askStep{n: m.n, done: true, v: v, err: err} })
	}}
}

// slowEcho answers every Ping with its N, delay after it arrived, through a timer message with
// the N negated.
func slowEcho(delay time.Duration) *actor[Ping] {
	return &actor[Ping]{receive: func(ctx *lockstep.Context[Ping], m Ping) {
		if m.N > 0 {
			ctx.After(delay, Ping{N: -m.N, ReplyTo: m.ReplyTo})
			return
		}
		m.ReplyTo.Send(ctx, -m.N)
	}}
}

// askFromN1 sends the asker /user/c on n1 a step that asks to for n, with timeout when it is above 0.
func askFromN1(s *sim.Sim, to lockstep.PID[Ping], n int, timeout time.Duration) {
	c := lockstep.PIDAt[askStep]("n1", "/user/c")
	if err := sim.Send(s, "n1", c, askStep{to: to, n: n, timeout: timeout}); err != nil {
		panic(err)
	}
}

// box is a message that a test compares by identity.
type box struct{ N int }

// AT-MSG-02 (sim clause), MSG-010, MSG-030, REM-345: a message sent across nodes is encoded, so
// the receiver on n2 gets a decoded copy: an equal value behind another pointer, a slice with the
// same elements in another array, and a map of its own.
func TestATMSG02DecodedCopyAcrossNodes(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []any
		s.Node("n1", noSetup)
		s.Node("n2", func(sys *lockstep.System) {
			spawn(sys, "store", &actor[any]{receive: func(_ *lockstep.Context[any], m any) { got = append(got, m) }})
		})
		w.RunFor(time.Millisecond)

		p := &box{N: 1}
		sl := append(make([]int, 0, 4), 5, 6)
		mp := map[string]int{"a": 1}
		store := lockstep.PIDAt[any]("n2", "/user/store")
		for _, m := range []any{p, sl, mp} {
			if err := sim.Send(s, "n1", store, m); err != nil {
				tt.Fatalf("Send %T: %v", m, err)
			}
		}
		w.RunFor(time.Second)

		if len(got) != 3 {
			tt.Fatalf("n2 received %d messages, want 3", len(got))
		}
		if gp, ok := got[0].(*box); !ok || gp == p || *gp != *p {
			tt.Errorf("received pointer %v, want a copy of %v behind another pointer", got[0], *p)
		}
		if gs, ok := got[1].([]int); !ok || !slices.Equal(gs, sl) || &gs[0] == &sl[0] {
			tt.Errorf("received slice %v, want a copy of %v in another array", got[1], sl)
		}
		gm, ok := got[2].(map[string]int)
		if !ok || !maps.Equal(gm, mp) {
			tt.Fatalf("received %v, want a copy of %v", got[2], mp)
		}
		gm["b"] = 2
		if _, ok := mp["b"]; ok {
			tt.Error("a write through the received map is visible in the sender's map")
		}
	})
}

// probeStep is the message of AT-MSG-15's probe: with Reply set it carries the pending reply to
// pipe; with Done set it is the resolution.
type probeStep struct {
	Reply *lockstep.Reply[int]
	Done  bool
	Err   error
}

// AT-MSG-15, MSG-320: an ask with the default timeout resolves with ErrTimeout when the node's
// clock is exactly 5 s past the ask's record, and the seed takes under 1 s of wall time. The
// body hands the pending reply to a probe on n1 (a reply may be sent in a message, MSG-030), which
// pipes it.
func TestATMSG15VirtualTimeout(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var silent lockstep.PID[Ping]
		var probe lockstep.PID[probeStep]
		var firstLocal, doneLocal time.Time
		var doneAt kernel.Time
		var gotErr error
		turns := 0
		n1 := s.Node("n1", func(sys *lockstep.System) {
			silent = spawn(sys, "silent", &actor[Ping]{})
			probe = spawn(sys, "probe", &actor[probeStep]{receive: func(ctx *lockstep.Context[probeStep], m probeStep) {
				turns++
				if !m.Done {
					firstLocal = ctx.Now()
					lockstep.Pipe(ctx, m.Reply, func(_ int, err error) probeStep { return probeStep{Done: true, Err: err} })
					return
				}
				doneLocal, doneAt, gotErr = ctx.Now(), w.Sim.Now(), m.Err
			}})
		})
		w.RunFor(time.Millisecond)

		r := lockstep.Ask(s.System("n1"), silent, buildPing(1)) // the default timeout
		if err := sim.Send(s, "n1", probe, probeStep{Reply: r}); err != nil {
			tt.Fatal(err)
		}
		start := time.Now()
		w.RunFor(6 * time.Second)
		wall := time.Since(start)

		asks := ofKind(w.Sim.Records(), "lockstep.ask")
		if len(asks) != 1 || asks[0].Node != n1.ID() || attr(asks[0], "timeout") != "5s" {
			tt.Fatalf("lockstep.ask records %v, want one on n1 with timeout 5s", asks)
		}
		if turns != 2 || !errors.Is(gotErr, lockstep.ErrTimeout) {
			tt.Fatalf("the probe ran %d turns and got %v, want 2 turns and ErrTimeout", turns, gotErr)
		}
		if d := time.Duration(doneAt - asks[0].At); d != 5*time.Second {
			tt.Errorf("the probe got ErrTimeout %v after the ask's record, want 5s", d)
		}
		if d := doneLocal.Sub(firstLocal); d != 5*time.Second {
			tt.Errorf("n1's clock advanced %v between the probe's turns, want 5s", d)
		}
		if wall >= time.Second {
			tt.Errorf("6s of virtual time took %v of wall time, want under 1s", wall)
		}
	})
}

// AT-MSG-29, MSG-110, MSG-340, MSG-350, MSG-410: an ask from c on n1 to kv on n2 gets the answer;
// when kv is stopped before it answers, ErrTerminated names kv by its full address; when n1 and n2 are partitioned
// before the answer, the ask times out, and the answer after the heal is recorded as late.
func TestATMSG29CrossNodeAsk(t *testing.T) {
	kv := lockstep.PIDAt[Ping]("n2", "/user/kv")
	boot := func(s *sim.Sim, delay time.Duration, got *[]askStep, real *lockstep.PID[Ping]) *kernel.Node {
		w := s.World()
		n1 := s.Node("n1", func(sys *lockstep.System) { spawn(sys, "c", asker(w.Sim.Now, got)) })
		s.Node("n2", func(sys *lockstep.System) {
			if delay == 0 {
				*real = spawn(sys, "kv", echo())
				return
			}
			*real = spawn(sys, "kv", slowEcho(delay))
		})
		w.RunFor(time.Millisecond)
		return n1
	}

	t.Run("answer", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var got []askStep
			var real lockstep.PID[Ping]
			boot(s, 0, &got, &real)
			askFromN1(s, kv, 7, 0)
			w.RunFor(time.Second)
			if len(got) != 1 || got[0].v != 7 || got[0].err != nil {
				tt.Errorf("c got %+v, want 7 and no error", got)
			}
		})
	})

	t.Run("kv-stopped", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var got []askStep
			var real lockstep.PID[Ping]
			n1 := boot(s, time.Second, &got, &real)
			askFromN1(s, kv, 7, 0)
			w.RunFor(100 * time.Millisecond)
			if err := s.StopActor(kv.Address()); err != nil {
				tt.Fatalf("StopActor: %v", err)
			}
			w.RunFor(time.Second)
			// MSG-340 names kv by its full address, with the uid of the incarnation that stopped;
			// n2 sends the failure back as FAIL terminated, which n1 reports as a *remote.Error.
			want := "remote: n2: lockstep: actor terminated: " + real.Address().String()
			var re *remote.Error
			if len(got) != 1 || got[0].v != 0 || !errors.Is(got[0].err, lockstep.ErrTerminated) ||
				!errors.As(got[0].err, &re) || re.Code != remote.CodeTerminated || got[0].err.Error() != want {
				tt.Fatalf("c got %+v, want 0 and a *remote.Error with code terminated and text %q", got, want)
			}
			if d := got[0].at; d > kernel.Time(time.Second) {
				tt.Errorf("c got the error at %v, want soon after the stop at about 101ms, not the 5s timeout", d)
			}
			for _, r := range ofKind(w.Sim.Records(), "lockstep.timeout") {
				if r.Node == n1.ID() {
					tt.Errorf("n1 recorded %s %q, want no timeout", r.Kind, r.Text)
				}
			}
		})
	})

	t.Run("partition", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var got []askStep
			var real lockstep.PID[Ping]
			n1 := boot(s, time.Second, &got, &real)
			askFromN1(s, kv, 7, 500*time.Millisecond)
			w.RunFor(100 * time.Millisecond) // kv has the request and answers at 1s
			w.Faults.Inject(fault.Event{Kind: fault.KindPartition, Groups: [][]string{{"n1"}, {"n2"}}})
			w.RunFor(600 * time.Millisecond)
			if len(got) != 1 || got[0].v != 0 || !errors.Is(got[0].err, lockstep.ErrTimeout) {
				tt.Fatalf("c got %+v before the heal, want 0 and ErrTimeout", got)
			}
			w.Faults.Inject(fault.Event{Kind: fault.KindHeal})
			w.RunFor(time.Second) // past kv's answer
			late := 0
			for _, r := range ofKind(w.Sim.Records(), "lockstep.reply") {
				if r.Node == n1.ID() && attr(r, "outcome") == "late" {
					late++
				}
			}
			if late != 1 {
				tt.Errorf("%d lockstep.reply outcome=late records on n1, want 1", late)
			}
			if len(got) != 1 {
				tt.Errorf("c got %d resolutions, want 1: the late answer does not resolve the ask again", len(got))
			}
		})
	})
}

// AT-MSG-30, MSG-310, MSG-350: an ask to an actor of n2 that completed its stop resolves with
// ErrTerminated from n2, for the PID with its uid and for the path address, and not with
// ErrTimeout. n2 records one dead letter for each ask, addressed to the asked address, so the
// failure is n2's and not a local dead letter on n1.
func TestATMSG30AskAbsentRemoteActor(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []askStep
		var kv lockstep.PID[Ping]
		n1 := s.Node("n1", func(sys *lockstep.System) { spawn(sys, "c", asker(w.Sim.Now, &got)) })
		n2 := s.Node("n2", func(sys *lockstep.System) { kv = spawn(sys, "kv", echo()) })
		w.RunFor(time.Millisecond)
		if err := s.StopActor(kv.Address()); err != nil {
			tt.Fatalf("StopActor: %v", err)
		}
		w.RunFor(10 * time.Millisecond) // the stop completes

		byPath := lockstep.PIDAt[Ping]("n2", "/user/kv")
		askFromN1(s, kv, 1, 0)
		askFromN1(s, byPath, 2, 0)
		w.RunFor(time.Second)

		for _, to := range []lockstep.Address{kv.Address(), byPath.Address()} {
			dead := 0
			for _, r := range ofKind(w.Sim.Records(), "lockstep.deadletter") {
				if r.Node == n2.ID() && attr(r, "to") == to.String() {
					dead++
				}
			}
			if dead != 1 {
				tt.Errorf("%d lockstep.deadletter records on n2 for %v, want 1", dead, to)
			}
		}

		if len(got) != 2 {
			tt.Fatalf("c got %d resolutions, want 2", len(got))
		}
		for _, g := range got {
			if g.v != 0 || !errors.Is(g.err, lockstep.ErrTerminated) || errors.Is(g.err, lockstep.ErrTimeout) {
				tt.Errorf("ask %d resolved with %d, %v; want 0 and an error wrapping ErrTerminated", g.n, g.v, g.err)
			}
			if g.at > kernel.Time(time.Second) {
				tt.Errorf("ask %d resolved at %v, want one network round trip after the ask, not its 5s timeout", g.n, g.at)
			}
		}
		for _, r := range ofKind(w.Sim.Records(), "lockstep.timeout") {
			if r.Node == n1.ID() {
				tt.Errorf("n1 recorded %s %q, want no timeout", r.Kind, r.Text)
			}
		}
	})
}

// AT-MSG-34 (sim clause), MSG-115, MSG-330: with the simulation engine (Blocking false) an Ask to
// a BlockSender mailbox that is full returns a reply that is already resolved with ErrMailboxFull,
// without waiting; the target receives only the first message. The body sends one message and
// asks before any event runs, so the mailbox still holds that message.
func TestATMSG34AskBlockSenderInSimulation(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var received []int
		var target lockstep.PID[Ping]
		s.Node("n1", func(sys *lockstep.System) {
			target = spawn(sys, "t", &actor[Ping]{receive: func(_ *lockstep.Context[Ping], m Ping) {
				received = append(received, m.N)
			}}, lockstep.WithMailbox(1, lockstep.BlockSender))
		})
		w.RunFor(time.Millisecond)

		if err := sim.Send(s, "n1", target, Ping{N: 1}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		now, executed := w.Sim.Now(), w.Sim.Executed()
		r := lockstep.Ask(s.System("n1"), target, buildPing(2), lockstep.WithTimeout(50*time.Millisecond))
		if !hooks.AskResolved(r) {
			tt.Fatal("the ask to a full BlockSender mailbox is pending, want it resolved already")
		}
		if v, err := r.Await(context.Background()); v != 0 || !errors.Is(err, lockstep.ErrMailboxFull) {
			tt.Errorf("Await = %d, %v; want 0 and an error wrapping ErrMailboxFull", v, err)
		}
		if w.Sim.Now() != now || w.Sim.Executed() != executed {
			tt.Errorf("the ask ran the simulation: now %v and %d events, were %v and %d", w.Sim.Now(), w.Sim.Executed(), now, executed)
		}

		w.RunFor(time.Second)
		if len(received) != 1 || received[0] != 1 {
			tt.Errorf("the target received %v, want only the first message [1]", received)
		}
	})
}

// AT-MSG-36 (sim clause), MSG-110, MSG-350, REM-294: c on n1 asks PIDAt("n2", "/user/kv"). After
// n2 crashes and restarts and its setup respawns kv, an ask through the same PID reaches the new
// kv. The asks between are lost on n1's session to n2's old incarnation, or fail with
// remote.ErrPeerRestarted once n1 sees the restart; c asks again, with a timeout, until one is
// answered.
func TestATMSG36PathAddressAcrossRestart(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []askStep
		boots := 0
		s.Node("n1", func(sys *lockstep.System) { spawn(sys, "c", asker(w.Sim.Now, &got)) })
		n2 := s.Node("n2", func(sys *lockstep.System) {
			boots++
			boot := boots
			// kv answers every ask with the number of the boot that spawned it.
			spawn(sys, "kv", &actor[Ping]{receive: func(ctx *lockstep.Context[Ping], m Ping) { m.ReplyTo.Send(ctx, boot) }})
		})
		w.RunFor(time.Millisecond)
		kv := lockstep.PIDAt[Ping]("n2", "/user/kv")

		askFromN1(s, kv, 1, 0)
		w.RunFor(100 * time.Millisecond)
		n2.Crash()
		w.RunFor(100 * time.Millisecond)
		n2.Restart()
		w.RunFor(100 * time.Millisecond)
		for i := 0; i < 5 && got[len(got)-1].v != 2; i++ {
			askFromN1(s, kv, 2, 50*time.Millisecond)
			w.RunFor(100 * time.Millisecond)
		}

		last := len(got) - 1
		if len(got) < 2 || got[0].v != 1 || got[0].err != nil || got[last].v != 2 || got[last].err != nil {
			tt.Fatalf("c got %+v, want the answer of boot 1, then failures, then the answer of boot 2", got)
		}
		for _, g := range got[1:last] {
			if !errors.Is(g.err, lockstep.ErrTimeout) && !errors.Is(g.err, remote.ErrPeerRestarted) {
				tt.Errorf("c got %+v between the answers, want a timeout or remote.ErrPeerRestarted", g)
			}
		}
	})
}
