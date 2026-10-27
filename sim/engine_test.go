// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/artifact"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/sim"
)

// tick is the message of the actor of TestATSIM09ClockAndLabels: a timer tick, or a resolved ask.
type tick struct{ timer bool }

// AT-SIM-09, SIM-155: an actor timer runs on the node's local clock at the same virtual time as
// a node event of the same local delay, ctx.Now() is the node's clock, and timers, ask timeouts,
// restart backoffs and effect timeouts run as kernel events labeled with the label passed to
// AfterFunc.
func TestATSIM09ClockAndLabels(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var n1 *kernel.Node
		var tickAt, refAt kernel.Time
		var tickNow, nodeNow time.Time
		ticks, refs, flakyStarts := 0, 0, 0
		s.HandleEffect("prices.fetch", func(sim.EffectCall) sim.EffectResult { return sim.EffectResult{Latency: time.Hour} })
		n1 = s.Node("n1", func(sys *lockstep.System) {
			silent := spawn(sys, "silent", &actor[Ping]{})
			spawn(sys, "a", &actor[tick]{
				started: func(ctx *lockstep.Context[tick]) {
					ctx.After(100*time.Millisecond, tick{timer: true})
					n1.After(100*time.Millisecond, "ref", func() { refs++; refAt = w.Sim.Now() })
					r := lockstep.Ask(ctx, silent, buildPing(1), lockstep.WithTimeout(50*time.Millisecond))
					lockstep.Pipe(ctx, r, func(int, error) tick { return tick{} })
					lockstep.Do(ctx, fetch{}, func(int, error) tick { return tick{} }, lockstep.WithEffectTimeout(50*time.Millisecond))
				},
				receive: func(ctx *lockstep.Context[tick], m tick) {
					if m.timer {
						ticks++
						tickAt, tickNow, nodeNow = w.Sim.Now(), ctx.Now(), n1.Now()
					}
				},
			})
			spawn(sys, "flaky", &actor[tick]{started: func(*lockstep.Context[tick]) {
				flakyStarts++
				if flakyStarts == 1 {
					panic("first start fails") // restarted after the default backoff
				}
			}})
		}, kernel.WithClock(0, 100_000))
		w.RunFor(time.Second)

		if ticks != 1 || refs != 1 {
			tt.Fatalf("tick ran %d times, ref %d times; want 1 each", ticks, refs)
		}
		if tickAt != refAt {
			tt.Errorf("lockstep/timer ran at %v, ref at %v; want the same time", tickAt, refAt)
		}
		if !tickNow.Equal(nodeNow) {
			tt.Errorf("ctx.Now() = %v, n1.Now() = %v; want equal", tickNow, nodeNow)
		}
		if flakyStarts != 2 {
			tt.Errorf("flaky started %d times, want 2 (one restart)", flakyStarts)
		}
		labels := map[string]int{}
		for _, r := range ofKind(w.Sim.Records(), "kernel.event") {
			if r.Node == n1.ID() {
				labels[r.Text]++
			}
		}
		for _, label := range []string{"lockstep/timer", "lockstep/ask-timeout", "lockstep/backoff", "lockstep/effect-timeout"} {
			if labels[label] != 1 {
				tt.Errorf("%d kernel events labeled %s on n1, want 1", labels[label], label)
			}
		}
	})
}

// countedKinds lists the record kinds that Counts counts, in the order of its fields.
var countedKinds = []string{
	"lockstep.spawn", "lockstep.send", "lockstep.ask", "lockstep.deliver", "lockstep.turn",
	"lockstep.drop", "lockstep.deadletter", "lockstep.mailbox.full", "lockstep.fail",
	"lockstep.restart", "lockstep.timeout", "lockstep.effect.start",
}

// countsOf returns the Counts that the records recs give.
func countsOf(recs []kernel.Record) sim.Counts {
	var n [12]uint64
	for _, r := range recs {
		i := slices.Index(countedKinds, r.Kind)
		if i < 0 || (r.Kind == "lockstep.spawn" && attr(r, "result") != "ok") {
			continue
		}
		n[i]++
	}
	return sim.Counts{
		Spawns: n[0], Sends: n[1], Asks: n[2], Deliveries: n[3], Turns: n[4], Drops: n[5],
		DeadLetters: n[6], MailboxFull: n[7], Failures: n[8], Restarts: n[9], Timeouts: n[10], Effects: n[11],
	}
}

// AT-SIM-14, SIM-150, SIM-185: a body Send records lockstep.send on the node with MSG §8's
// attributes, Counts matches the records, record texts follow SIM-185, and every turn is one
// kernel event labeled lockstep/turn.
func TestATSIM14TracerAndCounts(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var acct lockstep.PID[Deposit]
		var echoPID lockstep.PID[Ping]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			acct = spawn(sys, "acct", &actor[Deposit]{})
			echoPID = spawn(sys, "echo", echo())
		})
		w.RunFor(time.Millisecond)
		before := s.Counts()
		if err := sim.Send(s, "n1", acct, Deposit{5}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Millisecond)
		after := s.Counts()
		if after.Sends-before.Sends != 1 || after.Deliveries-before.Deliveries != 1 || after.Sends != 1 || after.Deliveries != 1 {
			tt.Errorf("Counts before %+v, after %+v; want Sends 1 and Deliveries 1", before, after)
		}

		recs := w.Sim.Records()
		sends := ofKind(recs, "lockstep.send")
		if len(sends) != 1 {
			tt.Fatalf("%d lockstep.send records, want 1", len(sends))
		}
		send := sends[0]
		if send.Node != n1.ID() || send.Text != "send" {
			tt.Errorf("send record node %d text %q, want %d %q", send.Node, send.Text, n1.ID(), "send")
		}
		if got := attrKeys(send); !slices.Equal(got, []string{"from", "to", "msg"}) {
			tt.Errorf("send attribute keys %q, want from, to, msg", got)
		}
		if got, want := attr(send, "from"), "lockstep://n1/user#100000001"; got != want {
			tt.Errorf("send from %q, want the user guardian %q", got, want)
		}
		if got, want := attr(send, "to"), acct.String(); got != want {
			tt.Errorf("send to %q, want %q", got, want)
		}
		if got, want := attr(send, "msg"), kernel.Describe(Deposit{5}); got != want {
			tt.Errorf("send msg %q, want %q", got, want)
		}

		// Some more kinds: an ask, its reply and an ask timeout.
		r := lockstep.Ask(s.System("n1"), echoPID, buildPing(7))
		//lockstep:allow LS220 the ask times out on purpose, to record an ask timeout
		lockstep.Ask(s.System("n1"), acct, func(lockstep.ReplyTo[int]) Deposit { return Deposit{} }, lockstep.WithTimeout(time.Millisecond))
		w.RunFor(10 * time.Millisecond)
		if v, err := r.Await(context.Background()); v != 7 || err != nil {
			tt.Errorf("ask: %d, %v; want 7, nil", v, err)
		}
		recs = w.Sim.Records()
		if got, want := s.Counts(), countsOf(recs); got != want {
			tt.Errorf("Counts() = %+v, records give %+v", got, want)
		}
		if c := s.Counts(); c.Spawns != 2 || c.Asks != 2 || c.Timeouts != 1 {
			tt.Errorf("Counts() = %+v, want Spawns 2, Asks 2, Timeouts 1", c)
		}

		lastEvent := kernel.Record{}
		turnsInEvent := 0
		for _, r := range recs {
			if r.Kind == "kernel.event" {
				lastEvent, turnsInEvent = r, 0
				continue
			}
			if !strings.HasPrefix(r.Kind, "lockstep.") {
				continue
			}
			if r.Node != n1.ID() {
				tt.Errorf("%s record on node %d, want n1 (%d)", r.Kind, r.Node, n1.ID())
			}
			want := strings.TrimPrefix(r.Kind, "lockstep.")
			if r.Kind == "lockstep.turn" {
				want = "turn " + attr(r, "actor")
				turnsInEvent++
				if lastEvent.Text != "lockstep/turn" || turnsInEvent != 1 {
					tt.Errorf("turn record %d follows event %q with %d turns, want one turn per lockstep/turn event", r.Seq, lastEvent.Text, turnsInEvent)
				}
				if got := attrKeys(r); !slices.Equal(got, []string{"actor", "system", "user", "msg"}) {
					tt.Errorf("turn attribute keys %q", got)
				}
			}
			if r.Text != want {
				tt.Errorf("%s record text %q, want %q", r.Kind, r.Text, want)
			}
		}
	})
}

// AT-SIM-19, SIM-190, SIM-210: Await on a pending reply from body neither waits nor runs an event,
// records lockstep.misuse and fails the seed with check lockstep/misuse.
var _ = scenario("await-from-body", func(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		w := s.World()
		var silent lockstep.PID[Ping]
		n1 := s.Node("n1", func(sys *lockstep.System) { silent = spawn(sys, "silent", &actor[Ping]{}) })
		w.RunFor(time.Millisecond)
		r := lockstep.Ask(s.System("n1"), silent, buildPing(1))
		now, executed := w.Sim.Now(), w.Sim.Executed()
		_, err := r.Await(context.Background())
		// A failed check panics in body, which turns the report's kind into panic.
		if !errors.Is(err, lockstep.ErrAwaitInActor) {
			panic(fmt.Sprintf("Await returned %v, want ErrAwaitInActor", err))
		}
		if w.Sim.Now() != now || w.Sim.Executed() != executed {
			panic("an event ran during Await")
		}
		recs := w.Sim.Records()
		last := recs[len(recs)-1]
		if last.Kind != "lockstep.misuse" || last.Node != n1.ID() || last.Text != "misuse" ||
			len(last.Attrs) != 1 || last.Attrs[0] != (kernel.Attr{Key: "error", Value: err.Error()}) {
			panic(fmt.Sprintf("last record %+v, want lockstep.misuse with the error", last))
		}
		w.RunFor(time.Second)
	})
})

// awaitedInReceive is printed by the await-in-receive scenario when Await inside Receive returned
// ErrAwaitInActor.
const awaitedInReceive = "AWAIT IN RECEIVE RETURNED ErrAwaitInActor"

// Await inside Receive returns ErrAwaitInActor and fails the seed.
var _ = scenario("await-in-receive", func(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		w := s.World()
		var a lockstep.PID[Deposit]
		s.Node("n1", func(sys *lockstep.System) {
			silent := spawn(sys, "silent", &actor[Ping]{})
			a = spawn(sys, "a", &actor[Deposit]{receive: func(ctx *lockstep.Context[Deposit], _ Deposit) {
				_, err := lockstep.Ask(ctx, silent, buildPing(1)).Await(context.Background())
				if errors.Is(err, lockstep.ErrAwaitInActor) {
					fmt.Println(awaitedInReceive)
				}
			}})
		})
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", a, Deposit{1}); err != nil {
			panic(err)
		}
		w.RunFor(time.Second)
	})
})

// misuseRecords returns the lockstep.misuse records of the failing seed that a.Trace holds.
func misuseRecords(t *testing.T, a *artifact.Artifact) []kernel.Record {
	t.Helper()
	if a.Trace == nil {
		t.Fatal("the failing seed left no trace")
	}
	return ofKind(a.Trace.Records, "lockstep.misuse")
}

// AT-SIM-19, AT-MSG-23 (sim clause), SIM-190, SIM-210, MSG-460, MSG-470: Await on a pending reply
// fails the seed from body and from Receive, with check lockstep/misuse and exactly one misuse
// entry for the call; Await on a resolved reply returns its value.
func TestATSIM19AwaitMisuse(t *testing.T) {
	t.Run("body", func(t *testing.T) {
		a, _ := failingSeedArtifact(t, "await-from-body")
		f := a.Report.Failure
		got := failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}
		want := failure{
			Kind:      "invariant",
			Check:     "lockstep/misuse",
			Signature: "invariant:lockstep/misuse",
			Message:   "lockstep: sim: wait on a pending reply from the test body; use sim.Ask: lockstep: blocking wait inside an actor",
		}
		if got != want {
			t.Errorf("failure %+v, want %+v", got, want)
		}
		if n := len(misuseRecords(t, a)); n != 1 {
			t.Errorf("%d lockstep.misuse records, want 1", n)
		}
	})
	t.Run("receive", func(t *testing.T) {
		a, out := failingSeedArtifact(t, "await-in-receive")
		f := a.Report.Failure
		if f.Kind != "invariant" || f.Check != "lockstep/misuse" || !strings.Contains(f.Message, lockstep.ErrAwaitInActor.Error()) {
			t.Errorf("failure %+v, want kind invariant, check lockstep/misuse and a message with %q", f, lockstep.ErrAwaitInActor)
		}
		if !strings.Contains(out, awaitedInReceive) {
			t.Errorf("output lacks %q:\n%s", awaitedInReceive, out)
		}
		if n := len(misuseRecords(t, a)); n != 1 {
			t.Errorf("%d lockstep.misuse records, want 1", n)
		}
	})
	t.Run("resolved", func(t *testing.T) {
		sim.Simulate(t, fullTrace(2), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var e lockstep.PID[Ping]
			s.Node("n1", func(sys *lockstep.System) { e = spawn(sys, "echo", echo()) })
			w.RunFor(time.Millisecond)
			r := lockstep.Ask(s.System("n1"), e, buildPing(7))
			w.RunFor(time.Millisecond)
			if v, err := r.Await(context.Background()); v != 7 || err != nil {
				tt.Errorf("Await = %d, %v; want 7, nil", v, err)
			}
			if n := len(ofKind(w.Sim.Records(), "lockstep.misuse")); n != 0 {
				tt.Errorf("%d lockstep.misuse records, want 0", n)
			}
		})
	})
}

// SIM-195: InTurn is true in kernel callbacks and false in body.
func TestSIM195InTurn(t *testing.T) {
	sim.Simulate(t, sim.Options{}, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var inReceive []bool
		var a lockstep.PID[Deposit]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			a = spawn(sys, "a", &actor[Deposit]{receive: func(*lockstep.Context[Deposit], Deposit) {
				inReceive = append(inReceive, s.Engine("n1").Scheduler.InTurn())
			}})
		})
		w.RunFor(time.Millisecond)
		if s.Engine("n1").Scheduler.InTurn() {
			tt.Error("InTurn() is true in body")
		}
		_ = sim.Send(s, "n1", a, Deposit{1})
		inEvent := false
		n1.Post("test/in-turn", func() { inEvent = s.Engine("n1").Scheduler.InTurn() })
		w.RunFor(time.Millisecond)
		if !slices.Equal(inReceive, []bool{true}) || !inEvent {
			tt.Errorf("InTurn() in Receive %v, in a node event %t; want true", inReceive, inEvent)
		}
	})
}

// SIM-180: the engine hands out the kernel's stream for each lockstep label, and panics for any
// other label.
func TestSIM180EntropyPrefix(t *testing.T) {
	sim.Simulate(t, sim.Options{}, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var draws []uint64
		s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "a", &actor[Deposit]{started: func(ctx *lockstep.Context[Deposit]) {
				for range 3 {
					draws = append(draws, ctx.Rand().Uint64())
				}
			}})
		})
		w.RunFor(time.Millisecond)
		ref := kernel.New(kernel.Config{Seed: w.Seed()}).Rand("lockstep/n1/1/user/a/1")
		want := []uint64{ref.Uint64(), ref.Uint64(), ref.Uint64()}
		if !slices.Equal(draws, want) {
			tt.Errorf("draws %v, want those of lockstep/n1/1/user/a/1 %v", draws, want)
		}
		ent := s.Engine("n1").Entropy
		for _, label := range []string{"kernel/x", "node/n1/1", "workload/x", "lockstep"} {
			expectPanic(tt, fmt.Sprintf("sim: Entropy label %q does not start with \"lockstep/\"", label), func() { ent.Rand(label) })
		}
		if ent.Rand("lockstep/x") != w.Sim.Rand("lockstep/x") {
			tt.Error(`Rand("lockstep/x") is not the kernel's stream`)
		}
	})
}

// fetch is an effect no handler serves.
type fetch struct{}

func (fetch) Name() string                     { return "prices.fetch" }
func (fetch) Run(context.Context) (int, error) { return 0, nil }

// An actor starts an effect that has no handler. Each in-process check that fails stores a misuse
// failure, which the report then shows instead of lockstep/no-effect-handler.
var _ = scenario("no-effect-handler", func(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		w := s.World()
		s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "a", &actor[int]{started: func(ctx *lockstep.Context[int]) {
				lockstep.Do(ctx, fetch{}, func(int, error) int { return 0 })
				if err := s.Stored(sim.CheckNoEffectHandler); !errors.Is(err, lockstep.ErrNoEffectHandler) {
					s.Store(sim.CheckMisuse, fmt.Errorf("stored %v, want an error wrapping ErrNoEffectHandler", err))
				}
				if n := len(ofKind(w.Sim.Records(), "lockstep.misuse")); n != 0 {
					s.Store(sim.CheckMisuse, fmt.Errorf("%d lockstep.misuse records, want 0", n))
				}
				req := engine.EffectRequest{ID: 99, Name: "other", Actor: engine.Address{Node: "n1", Path: "/user/a"}}
				cancel := s.Engine("n1").Effects.Start(req, func(any, error) { s.Store(sim.CheckMisuse, errors.New("done called")) })
				if !cancel() {
					s.Store(sim.CheckMisuse, errors.New("Cancel reported false"))
				}
			}})
		})
		w.RunFor(time.Second)
	})
})

// EFF-450: with no handler, the seed fails with check lockstep/no-effect-handler and EFF-450's
// text.
func TestEFF450NoHandler(t *testing.T) {
	got := failingSeed(t, "no-effect-handler")
	want := failure{
		Kind:      "invariant",
		Check:     "lockstep/no-effect-handler",
		Signature: "invariant:lockstep/no-effect-handler",
		Message:   `lockstep: no effect handler: effect "prices.fetch" called by lockstep://n1/user/a#100000003`,
	}
	if got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
}
