// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/sim"
)

// The tests below drive the internal/hooks additions that persistence uses (PER §4.3): an actor
// reaches its controls through hooks.Control, and the test body wakes it through hooks.Signal.

// PMsg is the user message of the paused-delivery tests.
type PMsg struct{ N int }

// controlled is an actor built from optional functions that also receives signals.
type controlled[M any] struct {
	started func(ctx *lockstep.Context[M])
	receive func(ctx *lockstep.Context[M], m M)
	signal  func(ctx *lockstep.Context[M], sig lockstep.Signal)
}

func (a *controlled[M]) Started(ctx *lockstep.Context[M]) {
	if a.started != nil {
		a.started(ctx)
	}
}

func (a *controlled[M]) Receive(ctx *lockstep.Context[M], m M) {
	if a.receive != nil {
		a.receive(ctx, m)
	}
}

func (a *controlled[M]) Signal(ctx *lockstep.Context[M], sig lockstep.Signal) {
	if a.signal != nil {
		a.signal(ctx, sig)
	}
}

// spawnControlled spawns a at /user/<name> from sp and returns its PID. It panics on an error,
// which fails the seed.
func spawnControlled[M any](sp lockstep.Spawner, name string, a *controlled[M], opts ...lockstep.SpawnOption) lockstep.PID[M] {
	pid, err := lockstep.Spawn(sp, name, func() lockstep.Actor[M] { return a }, opts...)
	if err != nil {
		panic(err)
	}
	return pid
}

// engineAddr converts a to the engine's address type, which hooks.Signal takes.
func engineAddr(a lockstep.Address) engine.Address {
	return engine.Address{Node: a.Node(), Path: a.Path(), UID: a.UID()}
}

// wake is the signal the tests send through hooks.Signal: a Terminated for an address nobody
// watches.
func wake() lockstep.Signal {
	return lockstep.Terminated{Addr: lockstep.PIDAt[PMsg]("n1", "/user/nobody").Address()}
}

// sendPMsgs sends a PMsg for each n to to from n1's System and fails the seed on an error.
func sendPMsgs(tt testing.TB, s *sim.Sim, to lockstep.PID[PMsg], ns ...int) {
	tt.Helper()
	for _, n := range ns {
		if err := sim.Send(s, "n1", to, PMsg{N: n}); err != nil {
			tt.Fatalf("Send %d: %v", n, err)
		}
	}
}

// AT-MBX-28, MBX-190, PER-250: while A's user delivery is paused its mailbox takes m1 and m2 and
// drops m3 under DropNewest, and A's Receive runs for none of them; resuming from a signal hands
// it m1 then m2.
func TestATMBX28PausedDelivery(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []int
		paused, whilePaused := false, 0
		var a lockstep.PID[PMsg]
		s.Node("n1", func(sys *lockstep.System) {
			a = spawnControlled(sys, "a", &controlled[PMsg]{
				started: func(ctx *lockstep.Context[PMsg]) {
					hooks.Control(ctx).PauseUser(true)
					paused = true
				},
				receive: func(_ *lockstep.Context[PMsg], m PMsg) {
					if paused {
						whilePaused++
					}
					got = append(got, m.N)
				},
				signal: func(ctx *lockstep.Context[PMsg], _ lockstep.Signal) {
					hooks.Control(ctx).PauseUser(false)
					paused = false
				},
			}, lockstep.WithMailbox(2, lockstep.DropNewest))
		})
		w.RunFor(time.Millisecond)
		sendPMsgs(tt, s, a, 1, 2, 3)
		w.RunFor(time.Second)

		if len(got) != 0 {
			tt.Fatalf("Receive got %v while paused, want nothing", got)
		}
		recs := w.Sim.Records()
		var delivered []string
		for _, r := range ofKind(recs, "lockstep.deliver") {
			if attr(r, "to") == a.String() {
				delivered = append(delivered, attr(r, "msg"))
			}
		}
		if want := []string{kernel.Describe(PMsg{N: 1}), kernel.Describe(PMsg{N: 2})}; !slices.Equal(delivered, want) {
			tt.Errorf("lockstep.deliver to A: %q, want %q", delivered, want)
		}
		full := ofKind(recs, "lockstep.mailbox.full")
		if len(full) != 1 || attr(full[0], "action") != "dropped-newest" || attr(full[0], "msg") != kernel.Describe(PMsg{N: 3}) {
			tt.Errorf("lockstep.mailbox.full records %v, want one for m3 with action dropped-newest", full)
		}

		if !hooks.Signal(s.System("n1"), engineAddr(a.Address()), wake()) {
			tt.Fatal("hooks.Signal to the live A reported false")
		}
		w.RunFor(time.Second)
		if !slices.Equal(got, []int{1, 2}) || whilePaused != 0 {
			tt.Errorf("after resuming, Receive got %v (%d while paused), want [1 2] (0)", got, whilePaused)
		}
	})
}

// PER-250: pausing twice is pausing once, so a single resume delivers again; resuming twice is
// resuming once, so a single pause stops delivery again; and the incarnation after a failure
// starts unpaused.
func TestPER250PauseUserIdempotent(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []int
		starts, signals := 0, 0
		var a lockstep.PID[PMsg]
		s.Node("n1", func(sys *lockstep.System) {
			a = spawnControlled(sys, "a", &controlled[PMsg]{
				started: func(ctx *lockstep.Context[PMsg]) {
					starts++
					if starts == 1 {
						hooks.Control(ctx).PauseUser(true)
						hooks.Control(ctx).PauseUser(true)
					}
				},
				receive: func(ctx *lockstep.Context[PMsg], m PMsg) {
					got = append(got, m.N)
					if m.N == 3 || m.N == 4 {
						hooks.Control(ctx).PauseUser(true) // a single pause
					}
				},
				signal: func(ctx *lockstep.Context[PMsg], _ lockstep.Signal) {
					signals++
					hooks.Control(ctx).PauseUser(false)
					if signals >= 2 {
						hooks.Control(ctx).PauseUser(false) // resuming twice from the second signal on
					}
				},
			})
		})
		w.RunFor(time.Millisecond)
		sys := s.System("n1")
		expect := func(when string, want ...int) {
			tt.Helper()
			w.RunFor(time.Second)
			if !slices.Equal(got, want) {
				tt.Fatalf("%s: Receive got %v, want %v", when, got, want)
			}
		}

		sendPMsgs(tt, s, a, 1, 2, 3)
		expect("after two pauses")
		hooks.Signal(sys, engineAddr(a.Address()), wake())
		expect("after one resume following two pauses", 1, 2, 3)

		// m3 paused once more.
		sendPMsgs(tt, s, a, 4)
		expect("after one pause", 1, 2, 3)
		hooks.Signal(sys, engineAddr(a.Address()), wake())
		expect("after a double resume", 1, 2, 3, 4)

		// m4 paused once after the double resume: delivery stops again.
		sendPMsgs(tt, s, a, 5)
		expect("after one pause following two resumes", 1, 2, 3, 4)

		// The next incarnation starts unpaused without a PauseUser call: it gets m5, still queued,
		// and m6.
		if err := s.InjectFail(a.Address(), errors.New("boom")); err != nil {
			tt.Fatalf("InjectFail: %v", err)
		}
		w.RunFor(time.Second)
		sendPMsgs(tt, s, a, 6)
		expect("after the restart", 1, 2, 3, 4, 5, 6)
		if starts != 2 {
			tt.Errorf("%d starts, want 2", starts)
		}
	})
}

// PER §4.3: hooks.Signal queues a signal on the system queue of a live actor, in arrival order
// with the signals the runtime queues, whether those arrive after or before it, and reports false,
// changing nothing, for a stopped actor and for an address with no actor.
func TestHooksSignal(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var sigs []string
		var watcher lockstep.PID[PMsg]
		var x lockstep.PID[Deposit]
		third := lockstep.Terminated{Addr: lockstep.PIDAt[PMsg]("n1", "/user/third").Address()}
		fourth := lockstep.Terminated{Addr: lockstep.PIDAt[PMsg]("n1", "/user/fourth").Address()}
		s.Node("n1", func(sys *lockstep.System) {
			x = spawn(sys, "x", &actor[Deposit]{})
			watcher = spawnControlled(sys, "w", &controlled[PMsg]{
				started: func(ctx *lockstep.Context[PMsg]) {
					if err := lockstep.Watch(ctx, x.Address()); err != nil {
						panic(err)
					}
				},
				// Watching the stopped x queues the runtime's Terminated{x, NoActor} before Watch
				// returns (SUP-335), so it is on the system queue ahead of the two hook signals.
				receive: func(ctx *lockstep.Context[PMsg], _ PMsg) {
					if err := lockstep.Watch(ctx, x.Address()); err != nil {
						panic(err)
					}
					self := engineAddr(ctx.Self().Address())
					if !hooks.Signal(sys, self, third) || !hooks.Signal(sys, self, fourth) {
						panic("hooks.Signal to the running watcher reported false")
					}
				},
				signal: func(_ *lockstep.Context[PMsg], sig lockstep.Signal) {
					if term, ok := sig.(lockstep.Terminated); ok {
						sigs = append(sigs, term.Addr.Path())
					}
				},
			})
		})
		w.RunFor(time.Millisecond)
		sys := s.System("n1")
		to := engineAddr(watcher.Address())
		first := lockstep.Terminated{Addr: lockstep.PIDAt[PMsg]("n1", "/user/first").Address()}
		second := lockstep.Terminated{Addr: lockstep.PIDAt[PMsg]("n1", "/user/second").Address()}

		if !hooks.Signal(sys, to, first) {
			tt.Fatal("hooks.Signal of the first signal reported false")
		}
		if err := s.StopActor(x.Address()); err != nil {
			tt.Fatalf("StopActor x: %v", err)
		}
		if !hooks.Signal(sys, to, second) {
			tt.Fatal("hooks.Signal of the second signal reported false")
		}
		w.RunFor(time.Second)
		// x's Terminated reaches the watcher's system queue only when x's stop completes, after
		// both signals of the test.
		if want := []string{"/user/first", "/user/second", "/user/x"}; !slices.Equal(sigs, want) {
			tt.Fatalf("the watcher's Signal got %q, want %q", sigs, want)
		}

		before := len(w.Sim.Records())
		if hooks.Signal(sys, engineAddr(x.Address()), first) {
			tt.Error("hooks.Signal to the stopped x reported true")
		}
		if hooks.Signal(sys, engineAddr(lockstep.PIDAt[PMsg]("n1", "/user/none").Address()), first) {
			tt.Error("hooks.Signal to an address with no actor reported true")
		}
		w.RunFor(time.Second)
		if recs := w.Sim.Records(); len(recs) != before {
			tt.Errorf("hooks.Signal to no live actor added records %v", recs[before:])
		}
		if want := []string{"/user/first", "/user/second", "/user/x"}; !slices.Equal(sigs, want) {
			tt.Fatalf("the watcher's Signal got %q afterwards, want %q", sigs, want)
		}

		sendPMsgs(tt, s, watcher, 1)
		w.RunFor(time.Second)
		if want := []string{"/user/first", "/user/second", "/user/x", "/user/x", "/user/third", "/user/fourth"}; !slices.Equal(sigs, want) {
			tt.Errorf("with the runtime's Terminated queued first, the watcher's Signal got %q, want %q", sigs, want)
		}
	})
}

// PER §4.3: an actor's controls name its address as its PID prints it, report tracing on in
// simulation, and emit a record with the kind and attributes given.
func TestHooksControlRecord(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var addr string
		var enabled bool
		var a lockstep.PID[PMsg]
		s.Node("n1", func(sys *lockstep.System) {
			a = spawnControlled(sys, "a", &controlled[PMsg]{started: func(ctx *lockstep.Context[PMsg]) {
				ctl := hooks.Control(ctx)
				addr, enabled = ctl.Address(), ctl.TraceEnabled()
				ctl.Record("lockstep.persist.test", "a", "1")
			}})
		})
		w.RunFor(time.Millisecond)

		if addr != a.String() {
			tt.Errorf("Address() = %q, want the PID's String() %q", addr, a.String())
		}
		if !enabled {
			tt.Error("TraceEnabled() = false in simulation")
		}
		recs := ofKind(w.Sim.Records(), "lockstep.persist.test")
		if len(recs) != 1 || !slices.Equal(attrKeys(recs[0]), []string{"a"}) || attr(recs[0], "a") != "1" {
			tt.Errorf("lockstep.persist.test records %v, want one with a=1", recs)
		}
	})
}

// inlineName is the name of the inline internal effects below. It is not reserved, so a test can
// register a handler for it and check that the handler is never called.
const inlineName = "test.inline"

// inline returns an inline internal effect named inlineName with run and done; a nil run returns
// (nil, nil).
func inline(run func() (any, error), done func(ctx any, v any, err error)) hooks.InternalEffect {
	if run == nil {
		run = func() (any, error) { return nil, nil }
	}
	return hooks.InternalEffect{
		Name:   inlineName,
		Inline: true,
		Run:    func(context.Context) (any, error) { return run() },
		Done:   done,
	}
}

// lastEvent returns the last kernel.event record of recs: the event running when recs was read.
func lastEvent(recs []kernel.Record) kernel.Record {
	for i := len(recs) - 1; i >= 0; i-- {
		if recs[i].Kind == "kernel.event" {
			return recs[i]
		}
	}
	return kernel.Record{}
}

// stepUntil runs the kernel's events one at a time until cond holds, failing the seed if the
// queue runs dry first.
func stepUntil(tt testing.TB, w *kernel.Sim, cond func() bool) {
	tt.Helper()
	for !cond() {
		if !w.Step() {
			tt.Fatal("the event queue ran dry")
		}
	}
}

// AT-PER-26, PER-250: two pauses in Started hold back three user messages; a resume from an
// internal effect's Done hands them over in send order; the incarnation after a failure handles
// messages without any PauseUser call.
func TestATPER26PauseUser(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []int
		starts, pauses, dones := 0, 0, 0
		var a lockstep.PID[PMsg]
		s.Node("n1", func(sys *lockstep.System) {
			a = spawnControlled(sys, "a", &controlled[PMsg]{
				started: func(ctx *lockstep.Context[PMsg]) {
					starts++
					if starts == 1 {
						hooks.Control(ctx).PauseUser(true)
						hooks.Control(ctx).PauseUser(true)
						pauses += 2
					}
				},
				receive: func(_ *lockstep.Context[PMsg], m PMsg) { got = append(got, m.N) },
				signal: func(ctx *lockstep.Context[PMsg], _ lockstep.Signal) {
					hooks.Control(ctx).StartInternal(inline(nil, func(ctx any, _ any, _ error) {
						dones++
						hooks.Control(ctx).PauseUser(false)
					}))
				},
			})
		})
		w.RunFor(time.Millisecond)

		sendPMsgs(tt, s, a, 1, 2, 3)
		w.RunFor(time.Second)
		if len(got) != 0 {
			tt.Fatalf("Receive got %v while paused, want nothing", got)
		}
		hooks.Signal(s.System("n1"), engineAddr(a.Address()), wake())
		w.RunFor(time.Second)
		if dones != 1 || !slices.Equal(got, []int{1, 2, 3}) {
			tt.Fatalf("after Done resumed (%d Done calls): Receive got %v, want [1 2 3]", dones, got)
		}

		if err := s.InjectFail(a.Address(), errors.New("boom")); err != nil {
			tt.Fatalf("InjectFail: %v", err)
		}
		w.RunFor(time.Second)
		sendPMsgs(tt, s, a, 4)
		w.RunFor(time.Second)
		if starts != 2 || pauses != 2 || !slices.Equal(got, []int{1, 2, 3, 4}) {
			tt.Errorf("after the restart: %d starts, %d PauseUser calls, Receive got %v; want 2, 2 and [1 2 3 4]",
				starts, pauses, got)
		}
	})
}

// AT-PER-25 (second sentence), PER-240: a panic in an internal effect's Done fails the actor like
// a panic in Receive: lockstep.fail with source receive, and the supervisor decides on the panic.
func TestATPER25DonePanicFailsActor(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var decided []error
		var child lockstep.PID[PMsg]
		s.Node("n1", func(sys *lockstep.System) {
			strategy := lockstep.OneForOne(func(err error) lockstep.Directive {
				decided = append(decided, err)
				return lockstep.DirectiveStop
			})
			spawnControlled(sys, "p", &controlled[PMsg]{started: func(ctx *lockstep.Context[PMsg]) {
				child = spawnControlled(ctx, "c", &controlled[PMsg]{started: func(ctx *lockstep.Context[PMsg]) {
					hooks.Control(ctx).StartInternal(inline(nil, func(any, any, error) { panic("done panics") }))
				}})
			}}, lockstep.WithStrategy(strategy))
		})
		w.RunFor(time.Second)

		var fails []kernel.Record
		for _, r := range ofKind(w.Sim.Records(), "lockstep.fail") {
			if attr(r, "actor") == child.String() {
				fails = append(fails, r)
			}
		}
		if len(fails) != 1 || attr(fails[0], "source") != "receive" || attr(fails[0], "err") != "lockstep: panic: done panics" {
			tt.Errorf("lockstep.fail records of the child %v, want one with source receive for the panic in Done", fails)
		}
		var pe *lockstep.PanicError
		if len(decided) != 1 || !errors.As(decided[0], &pe) || pe.Value != "done panics" {
			tt.Errorf("the supervisor decided on %v, want one *lockstep.PanicError with value \"done panics\"", decided)
		}
	})
}

// PER-520, PER-240: an inline internal effect started in a turn at time t runs Run in its own
// lockstep/effect event at t, outside the turn, and its completion in a second lockstep/effect
// event, followed by the turn that calls Done. A handler registered for its name is never called,
// and the effect records carry its name.
func TestPER520InlineEvents(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		handled := 0
		s.HandleEffect(inlineName, func(sim.EffectCall) sim.EffectResult {
			handled++
			return sim.EffectResult{}
		})
		var inTurn, runInTurn bool
		var startEvent, runEvent, doneEvent kernel.Record
		var runAt, doneAt int // the number of records when Run and Done ran
		var dones []any
		var a lockstep.PID[PMsg]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			a = spawnControlled(sys, "a", &controlled[PMsg]{receive: func(ctx *lockstep.Context[PMsg], _ PMsg) {
				inTurn = true
				startEvent = lastEvent(w.Sim.Records())
				hooks.Control(ctx).StartInternal(inline(func() (any, error) {
					runInTurn = inTurn
					recs := w.Sim.Records()
					runAt, runEvent = len(recs), lastEvent(recs)
					return 7, nil
				}, func(dctx any, v any, err error) {
					if _, ok := dctx.(*lockstep.Context[PMsg]); !ok || err != nil {
						tt.Errorf("Done got ctx %T and err %v, want a *lockstep.Context[PMsg] and nil", dctx, err)
					}
					recs := w.Sim.Records()
					doneAt, doneEvent = len(recs), lastEvent(recs)
					dones = append(dones, v)
				}))
				inTurn = false
			}})
		})
		w.RunFor(time.Millisecond)
		sendPMsgs(tt, s, a, 1)
		w.RunFor(time.Second)

		if runEvent.Kind == "" || runInTurn {
			tt.Fatalf("Run called: %v, inside the starting turn: %v; want called outside it", runEvent.Kind != "", runInTurn)
		}
		if startEvent.Text != "lockstep/turn" || runEvent.Text != "lockstep/effect" || runEvent.Node != n1.ID() || runEvent.At != startEvent.At {
			tt.Errorf("Run ran in event %q on node %d at %v, started in %q at %v; want lockstep/effect on n1 at the start time",
				runEvent.Text, runEvent.Node, runEvent.At, startEvent.Text, startEvent.At)
		}
		if !slices.Equal(dones, []any{7}) {
			tt.Fatalf("Done got %v, want [7]", dones)
		}
		recs := w.Sim.Records()
		var between []string
		for _, r := range recs[runAt:doneAt] {
			if r.Kind == "kernel.event" && r.Node == n1.ID() {
				between = append(between, r.Text)
			}
		}
		if !slices.Equal(between, []string{"lockstep/effect", "lockstep/turn"}) || doneEvent.Text != "lockstep/turn" || doneEvent.At != startEvent.At {
			tt.Errorf("events of n1 from Run to Done %q, Done in %q at %v; want the completion event, then the turn that calls Done, at %v",
				between, doneEvent.Text, doneEvent.At, startEvent.At)
		}
		if n := ran(recs, n1, "lockstep/effect"); n != 2 || handled != 0 {
			tt.Errorf("%d lockstep/effect events and %d handler calls, want 2 and 0", n, handled)
		}
		starts, ends := ofKind(recs, "lockstep.effect.start"), ofKind(recs, "lockstep.effect.done")
		if len(starts) != 1 || attr(starts[0], "name") != inlineName {
			tt.Errorf("lockstep.effect.start records %v, want one named %q", starts, inlineName)
		}
		if len(ends) != 1 || attr(ends[0], "name") != inlineName || attr(ends[0], "outcome") != "ok" {
			tt.Errorf("lockstep.effect.done records %v, want one named %q with outcome ok", ends, inlineName)
		}
	})
}

// PER-160, PER-520: an incarnation that ends between an inline effect's run and completion events
// never runs Done, and a node crash discards the events still pending.
func TestPER160InternalEffectCancelled(t *testing.T) {
	// setup adds n1 with an actor that starts an inline effect on every user message; started
	// and ran report that StartInternal returned and that Run ran, and dones counts Done's calls.
	type probe struct {
		n1           *kernel.Node
		a            lockstep.PID[PMsg]
		started, ran bool
		dones        int
	}
	setup := func(s *sim.Sim) *probe {
		p := &probe{}
		p.n1 = s.Node("n1", func(sys *lockstep.System) {
			p.a = spawnControlled(sys, "a", &controlled[PMsg]{receive: func(ctx *lockstep.Context[PMsg], _ PMsg) {
				hooks.Control(ctx).StartInternal(inline(func() (any, error) {
					p.ran = true
					return nil, nil
				}, func(any, any, error) { p.dones++ }))
				p.started = true
			}})
		})
		s.World().RunFor(time.Millisecond)
		sendPMsgs(s.World().T(), s, p.a, 1)
		return p
	}

	// The seeded tie-break orders the completion event among the same-time turns of the
	// restart, so some seeds end the incarnation first (the effect is cancelled) and others
	// deliver the completion to the failed incarnation (it keeps it, and the restart drops it).
	// Done runs in neither.
	t.Run("inject fail", func(t *testing.T) {
		cancelled := 0
		sim.Simulate(t, fullTrace(48), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			p := setup(s)
			stepUntil(tt, w.Sim, func() bool { return p.ran })
			if err := s.InjectFail(p.a.Address(), errors.New("boom")); err != nil {
				tt.Fatalf("InjectFail: %v", err)
			}
			w.RunFor(time.Second)

			recs := w.Sim.Records()
			if p.dones != 0 {
				tt.Errorf("Done ran %d times, want 0", p.dones)
			}
			ends := ofKind(recs, "lockstep.effect.done")
			if len(ends) != 1 {
				tt.Fatalf("lockstep.effect.done records %v, want one", ends)
			}
			var drops []kernel.Record
			for _, r := range ofKind(recs, "lockstep.drop") {
				if attr(r, "to") == p.a.String() {
					drops = append(drops, r)
				}
			}
			switch ran(recs, p.n1, "lockstep/effect") {
			case 1: // the incarnation ended first: the completion event never ran
				cancelled++
				if attr(ends[0], "outcome") != "cancelled" || len(drops) != 0 {
					tt.Errorf("effect.done %v and drops %v, want outcome=cancelled and no drop", ends[0], drops)
				}
			case 2: // the completion reached the failed incarnation, which kept it until its restart
				if attr(ends[0], "outcome") != "ok" || len(drops) != 1 || attr(drops[0], "reason") != "incarnation-ended" {
					tt.Errorf("effect.done %v and drops %v, want outcome=ok and one incarnation-ended drop", ends[0], drops)
				}
			default:
				tt.Errorf("%d lockstep/effect events, want 1 or 2", ran(recs, p.n1, "lockstep/effect"))
			}
		})
		if cancelled == 0 {
			t.Error("no seed ended the incarnation before the completion event: outcome=cancelled is untested")
		}
		t.Logf("%d of 48 seeds ended the incarnation before the completion event", cancelled)
	})

	t.Run("crash before run", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			p := setup(s)
			stepUntil(tt, w.Sim, func() bool { return p.started })
			w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n1"})
			w.RunFor(time.Second)
			if n := ran(w.Sim.Records(), p.n1, "lockstep/effect"); n != 0 || p.ran || p.dones != 0 {
				tt.Errorf("after a crash before the run event: %d lockstep/effect events, Run ran %v, %d Done calls; want 0, false, 0",
					n, p.ran, p.dones)
			}
		})
	})

	t.Run("crash after run", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			p := setup(s)
			stepUntil(tt, w.Sim, func() bool { return p.ran })
			w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n1"})
			w.RunFor(time.Second)
			if n := ran(w.Sim.Records(), p.n1, "lockstep/effect"); n != 1 || p.dones != 0 {
				tt.Errorf("after a crash between the events: %d lockstep/effect events, %d Done calls; want 1 and 0", n, p.dones)
			}
		})
	})
}

// PER-520, PER-240: a panic in an inline Run reaches Done as the error the other engines give it,
// with outcome=error, and does not fail the seed.
func TestPER520InlineRunPanic(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var errs []error
		s.Node("n1", func(sys *lockstep.System) {
			spawnControlled(sys, "a", &controlled[PMsg]{started: func(ctx *lockstep.Context[PMsg]) {
				hooks.Control(ctx).StartInternal(inline(func() (any, error) { panic("disk on fire") },
					func(_ any, _ any, err error) { errs = append(errs, err) }))
			}})
		})
		w.RunFor(time.Second)

		want := `lockstep: effect "test.inline" panicked: disk on fire`
		if len(errs) != 1 || errs[0] == nil || errs[0].Error() != want {
			tt.Fatalf("Done got errors %v, want one %q", errs, want)
		}
		ends := ofKind(w.Sim.Records(), "lockstep.effect.done")
		if len(ends) != 1 || attr(ends[0], "outcome") != "error" || attr(ends[0], "err") != want {
			tt.Errorf("lockstep.effect.done records %v, want one with outcome error and err %q", ends, want)
		}
	})
}
