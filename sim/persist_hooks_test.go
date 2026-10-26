// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
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
