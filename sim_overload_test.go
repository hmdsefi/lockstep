// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/overload"
	"github.com/hmdsefi/lockstep/sim"
)

// The overload acceptance tests of the simulation: they run under sim.Simulate with full traces
// and the helpers of sim_mailbox_test.go. Those that are rerun for AT-OVL-45 are functions of the
// options and of a counter of body runs, so that the rerun reads LOCKSTEP_CHECK_DETERMINISM.

// ovlCount adds one to *c unless c is nil: it counts the runs of a simulation body.
func ovlCount(c *int) {
	if c != nil {
		*c++
	}
}

// ovlEvents returns the lockstep.overload records of node n with event ev, in order.
func ovlEvents(w *faultline.World, n *kernel.Node, ev string) []kernel.Record {
	var out []kernel.Record
	for _, r := range simRecords(w, n, "lockstep.overload") {
		if simAttr(r, "event") == ev {
			out = append(out, r)
		}
	}
	return out
}

// ovlAttrs returns the attributes of r as "key=value" pairs in order, space separated.
func ovlAttrs(r kernel.Record) string {
	parts := make([]string, len(r.Attrs))
	for i, a := range r.Attrs {
		parts[i] = a.Key + "=" + a.Value
	}
	return strings.Join(parts, " ")
}

// ovlShedText is the text of the error of a message to addr that pressure shed (OVL-120).
func ovlShedText(addr string) string {
	return "lockstep: send to " + addr + ": overloaded (pressure), mailbox treated as full"
}

// checkShed fails tt unless err is the error of OVL-120 for a message to addr shed by pressure:
// it matches ErrOverloaded and ErrMailboxFull and has the specified text.
func checkShed(tt testing.TB, what string, err error, addr string) {
	tt.Helper()
	if !errors.Is(err, lockstep.ErrOverloaded) || !errors.Is(err, lockstep.ErrMailboxFull) {
		tt.Errorf("%s returned %v, want an error matching ErrOverloaded and ErrMailboxFull", what, err)
		return
	}
	if got, want := err.Error(), ovlShedText(addr); got != want {
		tt.Errorf("%s error text %q, want %q", what, got, want)
	}
}

// ovlRecorder returns an actor that appends every message to *got.
func ovlRecorder(got *[]int) *simActor[int] {
	return &simActor[int]{receive: func(_ *lockstep.Context[int], m int) { *got = append(*got, m) }}
}

// ovlRequest asks for a number; ovlWait is the timer message with which a responder answers later.
type ovlRequest struct{ rt lockstep.ReplyTo[int] }

type ovlWait struct {
	rt lockstep.ReplyTo[int]
	v  int
}

// ovlBuild builds an ovlRequest for the message type any.
func ovlBuild(rt lockstep.ReplyTo[int]) any { return ovlRequest{rt: rt} }

// ovlResponder returns an actor that answers every ovlRequest with v after delay, from a timer.
func ovlResponder(delay time.Duration, v int) *simActor[any] {
	return &simActor[any]{receive: func(ctx *lockstep.Context[any], m any) {
		switch m := m.(type) {
		case ovlRequest:
			ctx.After(delay, ovlWait{rt: m.rt, v: v})
		case ovlWait:
			m.rt.Send(ctx, m.v)
		}
	}}
}

// ovlFetch is the effect of the tests; simulation never calls its Run.
type ovlFetch struct{}

func (ovlFetch) Name() string                     { return "ovl.fetch" }
func (ovlFetch) Run(context.Context) (int, error) { panic("Run called") }

// ovlNote is a message an actor observes: a timer message, a reply or an effect result.
type ovlNote struct {
	kind string
	v    int
}

// AT-OVL-10, OVL-050, §8: with WithMaxActors(2) (reserve 1) the second spawn under /user is refused
// with the cap text, and one lockstep.overload record event=cap says so.
func TestATOVL10CapRecord(t *testing.T) {
	opts := simOpts(3)
	opts.System = []lockstep.Option{lockstep.WithMaxActors(2)}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		var first, second error
		n1 := s.Node("n1", func(sys *lockstep.System) {
			spawn := func(name string) error {
				_, err := lockstep.Spawn(sys, name, func() lockstep.Actor[int] { return &simActor[int]{} })
				return err
			}
			first, second = spawn("a"), spawn("b")
		})
		w := s.World()
		w.RunFor(100 * time.Millisecond)

		tt := w.T()
		if first != nil {
			tt.Errorf("first spawn returned %v, want nil", first)
		}
		if want := "lockstep: spawn in /user: actor cap reached (live 1, limit 2, reserve 1)"; second == nil || second.Error() != want || !errors.Is(second, lockstep.ErrActorCap) {
			tt.Errorf("second spawn returned %v, want ErrActorCap with text %q", second, want)
		}
		recs := ovlEvents(w, n1, "cap")
		if len(recs) != 1 {
			tt.Fatalf("%d event=cap records, want 1", len(recs))
		}
		if got, want := ovlAttrs(recs[0]), "event=cap parent=/user live=1 limit=2 reserve=1 reserved=false"; got != want {
			tt.Errorf("record %q, want %q", got, want)
		}
	})
}

// runATOVL12 is AT-OVL-12, OVL-100, OVL-210: under pressure from 1 s to 2 s a normal spawn at
// 1.5 s is refused with OVL-100's text, a spawn from the Started of a restarted actor (a rebuild,
// a reserve spawn) is admitted, and a normal spawn at 2.5 s is admitted.
func runATOVL12(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		ovlCount(runs)
		w, tt := s.World(), s.World().T()
		starts := 0
		var rebuildErr error
		s.Node("n1", func(sys *lockstep.System) {
			simSpawn(sys, "p", &simActor[int]{
				started: func(ctx *lockstep.Context[int]) {
					starts++
					if starts == 1 {
						ctx.After(1500*time.Millisecond, 0)
						return
					}
					_, rebuildErr = lockstep.Spawn(ctx, "k", func() lockstep.Actor[int] { return &simActor[int]{} })
				},
				receive: func(*lockstep.Context[int], int) { panic("p fails") },
			})
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(500 * time.Millisecond)

		_, err := lockstep.Spawn(s.System("n1"), "a", func() lockstep.Actor[int] { return &simActor[int]{} })
		if want := "lockstep: spawn in /user: overloaded (pressure)"; err == nil || err.Error() != want || !errors.Is(err, lockstep.ErrOverloaded) {
			tt.Errorf("normal spawn at 1.5s = %v, want ErrOverloaded with text %q", err, want)
		}
		w.RunFor(100 * time.Millisecond)
		if starts != 2 || rebuildErr != nil {
			tt.Errorf("p started %d times, the rebuild's spawn returned %v; want 2 starts and the spawn admitted", starts, rebuildErr)
		}
		w.RunFor(900 * time.Millisecond)
		if _, err := lockstep.Spawn(s.System("n1"), "b", func() lockstep.Actor[int] { return &simActor[int]{} }); err != nil {
			tt.Errorf("normal spawn at 2.5s: %v", err)
		}
	})
}

// runATOVL13 is AT-OVL-13, OVL-110, OVL-120: under pressure an actor sends to four targets, one
// per policy, and the test body sends to the BlockSender target from the System. FailSend and
// BlockSender return the error of OVL-120, the latter without waiting (R22); DropNewest returns
// nil and drops the message; DropOldest returns nil, drops the queued message and delivers the new
// one. The event=shed records name each policy.
func runATOVL13(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		ovlCount(runs)
		w, tt := s.World(), s.World().T()
		var gotFS, gotDN, gotDO, gotBS []int
		var fs, dn, do, bs lockstep.PID[int]
		var release func()
		var errs [4]error // the actor's sends, in target order
		n1 := s.Node("n1", func(sys *lockstep.System) {
			fs = simSpawn(sys, "fs", ovlRecorder(&gotFS), lockstep.WithMailbox(4, lockstep.FailSend))
			dn = simSpawn(sys, "dn", ovlRecorder(&gotDN), lockstep.WithMailbox(4, lockstep.DropNewest))
			do = simSpawn(sys, "do", ovlRecorder(&gotDO), lockstep.WithMailbox(2, lockstep.DropOldest))
			bs = simSpawn(sys, "bs", ovlRecorder(&gotBS), lockstep.WithMailbox(4, lockstep.BlockSender))
			release = lockstep.Hold(sys, do.Address())
			if err := lockstep.Send(sys, do, 1); err != nil { // the queued message
				panic(err)
			}
			simSpawn(sys, "from", &simActor[struct{}]{
				started: func(ctx *lockstep.Context[struct{}]) { ctx.After(1500*time.Millisecond, struct{}{}) },
				receive: func(ctx *lockstep.Context[struct{}], _ struct{}) {
					errs[0] = lockstep.Send(ctx, fs, 2)
					errs[1] = lockstep.Send(ctx, dn, 2)
					errs[2] = lockstep.Send(ctx, do, 2)
					errs[3] = lockstep.Send(ctx, bs, 2)
				},
			})
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(600 * time.Millisecond)
		before := w.Sim.Now()
		errSys := sim.Send(s, "n1", bs, 3)
		after := w.Sim.Now()
		release()
		w.RunFor(time.Second)

		checkShed(tt, "Send to the FailSend target", errs[0], fs.Address().String())
		if errs[1] != nil || errs[2] != nil {
			tt.Errorf("Send to the DropNewest and DropOldest targets returned %v and %v, want nil", errs[1], errs[2])
		}
		checkShed(tt, "Send from an actor to the BlockSender target", errs[3], bs.Address().String())
		checkShed(tt, "Send from the System to the BlockSender target", errSys, bs.Address().String())
		if before != after {
			tt.Errorf("virtual time moved from %v to %v during the Send to the BlockSender target, want it not to wait", before, after)
		}
		if len(gotFS) != 0 || len(gotDN) != 0 || len(gotBS) != 0 {
			tt.Errorf("FailSend, DropNewest and BlockSender targets received %v, %v and %v, want nothing", gotFS, gotDN, gotBS)
		}
		if !slices.Equal(gotDO, []int{2}) {
			tt.Errorf("DropOldest target received %v, want [2]: the queued message dropped, the new one delivered", gotDO)
		}

		var got []string
		for _, r := range ovlEvents(w, n1, "shed") {
			got = append(got, fmt.Sprintf("%s %s %s %s", simAttr(r, "to"), simAttr(r, "reason"), simAttr(r, "policy"), simAttr(r, "dropped")))
		}
		want := []string{
			fs.Address().String() + " pressure fail-send new",
			dn.Address().String() + " pressure drop-newest new",
			do.Address().String() + " pressure drop-oldest oldest",
			bs.Address().String() + " pressure block-sender new",
			bs.Address().String() + " pressure block-sender new",
		}
		if !slices.Equal(got, want) {
			tt.Errorf("event=shed records %q, want %q", got, want)
		}
		if n := len(simRecords(w, n1, "lockstep.mailbox.full")); n != 0 {
			tt.Errorf("%d lockstep.mailbox.full records, want 0", n)
		}
	})
}

func TestATOVL13ShedPolicies(t *testing.T) { runATOVL13(t, simOpts(3), nil) }

// AT-OVL-14, OVL-110: what is exempt from shedding is delivered under pressure: the reply of an
// Ask that was pending when pressure began, a timer message, an effect result, a stop and a send to
// a system actor. No event=shed record names any of them.
func TestATOVL14ExemptDeliveries(t *testing.T) {
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var notes []string
		var victimStopped bool
		var sysGot []int
		var sysPID lockstep.PID[int]
		var victim lockstep.PID[struct{}]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			rp := simSpawn(sys, "r", ovlResponder(1500*time.Millisecond, 0))
			simSpawn(sys, "a", &simActor[ovlNote]{
				started: func(ctx *lockstep.Context[ovlNote]) {
					ctx.After(1500*time.Millisecond, ovlNote{kind: "timer"})
					r := lockstep.Ask(ctx, rp, ovlBuild)
					lockstep.Pipe(ctx, r, func(v int, _ error) ovlNote { return ovlNote{kind: "reply", v: v} })
					lockstep.Do(ctx, ovlFetch{}, func(v int, _ error) ovlNote { return ovlNote{kind: "effect", v: v} })
				},
				receive: func(_ *lockstep.Context[ovlNote], m ovlNote) { notes = append(notes, m.kind+" "+strconv.Itoa(m.v)) },
			})
			victim = simSpawn(sys, "v", &simActor[struct{}]{stopped: func(*lockstep.Context[struct{}], lockstep.StopReason) { victimStopped = true }})
			var err error
			sysPID, err = lockstep.SpawnSystem(sys, "sa", func() lockstep.Actor[int] {
				return &simActor[int]{receive: func(_ *lockstep.Context[int], m int) { sysGot = append(sysGot, m) }}
			})
			if err != nil {
				panic(err)
			}
		})
		s.HandleEffect("ovl.fetch", func(sim.EffectCall) sim.EffectResult {
			return sim.EffectResult{Value: 9, Latency: 1500 * time.Millisecond}
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(200 * time.Millisecond)
		stopErr := lockstep.Stop(s.System("n1"), victim.Address())
		w.RunFor(100 * time.Millisecond)
		sendErr := sim.Send(s, "n1", sysPID, 5)
		w.RunFor(time.Second)

		if stopErr != nil || sendErr != nil {
			tt.Errorf("Stop returned %v and Send to the system actor %v, want nil", stopErr, sendErr)
		}
		slices.Sort(notes)
		if want := []string{"effect 9", "reply 0", "timer 0"}; !slices.Equal(notes, want) {
			tt.Errorf("the actor observed %q, want %q", notes, want)
		}
		if !victimStopped {
			tt.Errorf("the actor was not stopped")
		}
		if !slices.Equal(sysGot, []int{5}) {
			tt.Errorf("the system actor received %v, want [5]", sysGot)
		}
		if shed := ovlEvents(w, n1, "shed"); len(shed) != 0 {
			tt.Errorf("%d event=shed records, first %q, want none", len(shed), ovlAttrs(shed[0]))
		}
		if len(ovlEvents(w, n1, "on")) != 1 {
			tt.Errorf("%d event=on records, want 1: pressure never began", len(ovlEvents(w, n1, "on")))
		}
	})
}

// AT-OVL-15, OVL-120, §8: a Send to a DropOldest target with two queued messages emits one
// event=shed record with reason pressure, policy drop-oldest and dropped oldest, in §8's attribute
// order, and no lockstep.mailbox.full record.
func TestATOVL15ShedRecord(t *testing.T) {
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var tp lockstep.PID[int]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			tp = simSpawn(sys, "t", &simActor[int]{}, lockstep.WithMailbox(2, lockstep.DropOldest))
			_ = lockstep.Hold(sys, tp.Address())
			for i := 1; i <= 2; i++ {
				if err := lockstep.Send(sys, tp, i); err != nil {
					panic(err)
				}
			}
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		err := sim.Send(s, "n1", tp, 3)
		w.RunFor(100 * time.Millisecond)

		if err != nil {
			tt.Errorf("Send returned %v, want nil", err)
		}
		recs := ovlEvents(w, n1, "shed")
		if len(recs) != 1 {
			tt.Fatalf("%d event=shed records, want 1", len(recs))
		}
		r := recs[0]
		var keys []string
		for _, a := range r.Attrs {
			keys = append(keys, a.Key)
		}
		if want := []string{"event", "from", "to", "reason", "policy", "dropped"}; !slices.Equal(keys, want) {
			tt.Errorf("attribute keys %q, want %q", keys, want)
		}
		if got, want := simAttr(r, "from"), "lockstep://n1/user#"; !strings.HasPrefix(got, want) {
			tt.Errorf("from %q, want the System's user guardian, an address starting %q", got, want)
		}
		if got, want := simAttr(r, "to")+" "+simAttr(r, "reason")+" "+simAttr(r, "policy")+" "+simAttr(r, "dropped"),
			tp.Address().String()+" pressure drop-oldest oldest"; got != want {
			tt.Errorf("record names %q, want %q", got, want)
		}
		if n := len(simRecords(w, n1, "lockstep.mailbox.full")); n != 0 {
			tt.Errorf("%d lockstep.mailbox.full records, want 0", n)
		}
	})
}

// AT-OVL-16, OVL-130: under pressure an Ask from the test body to a FailSend target and one to a
// DropNewest target resolve at once, with no virtual time passing, with the error of OVL-120; for
// the dropped request it is the same error, which wraps ErrMailboxFull. Neither is ErrTimeout.
func TestATOVL16AsksResolveAtOnce(t *testing.T) {
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var t1, t2 lockstep.PID[ovlRequest]
		s.Node("n1", func(sys *lockstep.System) {
			t1 = simSpawn(sys, "t1", &simActor[ovlRequest]{}, lockstep.WithMailbox(4, lockstep.FailSend))
			t2 = simSpawn(sys, "t2", &simActor[ovlRequest]{}, lockstep.WithMailbox(4, lockstep.DropNewest))
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		build := func(rt lockstep.ReplyTo[int]) ovlRequest { return ovlRequest{rt: rt} }
		before := w.Sim.Now()
		_, err1 := sim.Ask(s, "n1", t1, build, lockstep.WithTimeout(time.Second))
		_, err2 := sim.Ask(s, "n1", t2, build, lockstep.WithTimeout(time.Second))
		after := w.Sim.Now()

		checkShed(tt, "the ask to the FailSend target", err1, t1.Address().String())
		checkShed(tt, "the ask to the DropNewest target", err2, t2.Address().String())
		for _, err := range []error{err1, err2} {
			if errors.Is(err, lockstep.ErrTimeout) {
				tt.Errorf("ask error %v is ErrTimeout, want it resolved by the shed", err)
			}
		}
		if before != after {
			tt.Errorf("virtual time moved from %v to %v, want the asks resolved at once", before, after)
		}
	})
}

// AT-OVL-17, OVL-140: a message from an actor on a to a FailSend actor on b, which is under
// pressure, is dropped on b: an event=shed record of b names policy fail-send and dropped new, and
// the sender's Send returned nil.
func TestATOVL17InboundShed(t *testing.T) {
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []int
		var sendErr error
		tp := lockstep.PIDAt[int]("b", "/user/t")
		na := s.Node("a", func(sys *lockstep.System) {
			simSpawn(sys, "s", &simActor[int]{
				started: func(ctx *lockstep.Context[int]) { ctx.After(1500*time.Millisecond, 0) },
				receive: func(ctx *lockstep.Context[int], _ int) { sendErr = lockstep.Send(ctx, tp, 1) },
			})
		})
		nb := s.Node("b", func(sys *lockstep.System) {
			simSpawn(sys, "t", ovlRecorder(&got), lockstep.WithMailbox(4, lockstep.FailSend))
		})
		w.RunFor(time.Second)
		s.PressureFor("b", time.Second)
		w.RunFor(time.Second)

		if sendErr != nil {
			tt.Errorf("the sender's Send returned %v, want nil", sendErr)
		}
		if len(got) != 0 {
			tt.Errorf("the target received %v, want the message dropped", got)
		}
		recs := ovlEvents(w, nb, "shed")
		if len(recs) != 1 {
			tt.Fatalf("%d event=shed records on b, want 1", len(recs))
		}
		if got, want := simAttr(recs[0], "policy")+" "+simAttr(recs[0], "dropped")+" "+simAttr(recs[0], "to"),
			"fail-send new "+tp.Address().String(); got != want {
			tt.Errorf("record names %q, want %q", got, want)
		}
		if n := len(ovlEvents(w, na, "shed")); n != 0 {
			tt.Errorf("%d event=shed records on a, want 0", n)
		}
	})
}

// ovlSub is a subscriber: it subscribes to Overloaded signals when its timer fires and logs the
// signals it gets. The message "fail" makes it panic, which restarts it.
type ovlSub struct {
	who   string
	delay time.Duration // from its first Started to its subscription
	log   *[]string
	w     *faultline.World
	start int
}

func (a *ovlSub) Started(ctx *lockstep.Context[string]) {
	a.start++
	if a.start == 1 {
		ctx.After(a.delay, "subscribe")
	}
}

func (a *ovlSub) Receive(ctx *lockstep.Context[string], m string) {
	if m == "fail" {
		panic("restart " + a.who)
	}
	ctx.SubscribeOverload()
}

func (a *ovlSub) Stopped(*lockstep.Context[string], lockstep.StopReason) {}

func (a *ovlSub) Signal(_ *lockstep.Context[string], sig lockstep.Signal) {
	if o, ok := sig.(lockstep.Overloaded); ok {
		*a.log = append(*a.log, fmt.Sprintf("%s %s %t %s %d %d %d", time.Duration(a.w.Sim.Now()), a.who, o.Active, o.Reason, o.Live, o.High, o.Low))
	}
}

// runATOVL20 is AT-OVL-20, OVL-170, OVL-180: S1 and S2 subscribe, in that order, and S1 restarts
// between two pressure periods. S1 gets the signals of period 1 only; S2 gets {true, pressure}
// and {false, pressure} for both; within a change S1's signal comes before S2's.
func runATOVL20(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		ovlCount(runs)
		w, tt := s.World(), s.World().T()
		var log []string
		var s1 lockstep.PID[string]
		sub1 := &ovlSub{who: "s1", delay: 10 * time.Millisecond, log: &log, w: w}
		s.Node("n1", func(sys *lockstep.System) {
			s1 = simSpawnActor(sys, "s1", sub1)
			simSpawnActor(sys, "s2", &ovlSub{who: "s2", delay: 20 * time.Millisecond, log: &log, w: w})
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(1500 * time.Millisecond)
		if err := sim.Send(s, "n1", s1, "fail"); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(500 * time.Millisecond)
		s.PressureFor("n1", time.Second)
		w.RunFor(2 * time.Second)

		// The signals of one change are queued in subscription order (OVL-180), but the turns that
		// handle them are simultaneous kernel events that a seed orders, so the log is compared
		// per change, in subscriber order.
		if sub1.start != 2 {
			tt.Errorf("S1 started %d times, want 2: one restart between the periods", sub1.start)
		}
		slices.Sort(log)
		want := []string{
			"1s s1 true pressure 0 0 0",
			"1s s2 true pressure 0 0 0",
			"2s s1 false pressure 0 0 0",
			"2s s2 false pressure 0 0 0",
			"3s s2 true pressure 0 0 0",
			"4s s2 false pressure 0 0 0",
		}
		if !slices.Equal(log, want) {
			tt.Errorf("signals %q, want %q", log, want)
		}
	})
}

func TestATOVL20Subscribers(t *testing.T) { runATOVL20(t, simOpts(3), nil) }

// simSpawnActor spawns a, any actor, at /user/<name> and returns its PID; it panics on an error,
// as simSpawn does.
func simSpawnActor[M any](sp lockstep.Spawner, name string, a lockstep.Actor[M], opts ...lockstep.SpawnOption) lockstep.PID[M] {
	pid, err := lockstep.Spawn(sp, name, func() lockstep.Actor[M] { return a }, opts...)
	if err != nil {
		panic(err)
	}
	return pid
}

// ovlTwice is an actor that subscribes twice in every turn of its timer messages.
type ovlTwice struct {
	log *[]string
	w   *faultline.World
}

func (a *ovlTwice) Started(ctx *lockstep.Context[int]) {
	ctx.After(1500*time.Millisecond, 0)
	ctx.After(1800*time.Millisecond, 0)
}

func (a *ovlTwice) Receive(ctx *lockstep.Context[int], _ int) {
	ctx.SubscribeOverload()
	ctx.SubscribeOverload()
}

func (a *ovlTwice) Stopped(*lockstep.Context[int], lockstep.StopReason) {}

func (a *ovlTwice) Signal(_ *lockstep.Context[int], sig lockstep.Signal) {
	if o, ok := sig.(lockstep.Overloaded); ok {
		*a.log = append(*a.log, fmt.Sprintf("%s %t", time.Duration(a.w.Sim.Now()), o.Active))
	}
}

// runATOVL21 is AT-OVL-21, OVL-190: an actor that subscribes twice during pressure, in one turn
// and again in a later one, gets exactly one Overloaded{Active: true} and, when pressure ends,
// one Overloaded{Active: false}.
func runATOVL21(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		ovlCount(runs)
		w, tt := s.World(), s.World().T()
		var log []string
		s.Node("n1", func(sys *lockstep.System) { simSpawnActor(sys, "x", &ovlTwice{log: &log, w: w}) })
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(3 * time.Second)

		if want := []string{"1.5s true", "2s false"}; !slices.Equal(log, want) {
			tt.Errorf("signals %q, want %q", log, want)
		}
	})
}

func TestATOVL21SubscribeTwice(t *testing.T) { runATOVL21(t, simOpts(3), nil) }

// AT-OVL-22, OVL-200: an actor without a Signal method may subscribe: pressure beginning and
// ending fails nothing, and nothing reaches its Receive.
func TestATOVL22SubscriberWithoutSignal(t *testing.T) {
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		starts, received := 0, 0
		n1 := s.Node("n1", func(sys *lockstep.System) {
			simSpawn(sys, "x", &simActor[int]{
				started: func(ctx *lockstep.Context[int]) {
					starts++
					ctx.SubscribeOverload()
				},
				receive: func(*lockstep.Context[int], int) { received++ },
			})
		})
		w.RunFor(time.Second)
		s.PressureFor("n1", time.Second)
		w.RunFor(2 * time.Second)

		if starts != 1 || received != 0 {
			tt.Errorf("the actor started %d times and received %d messages, want 1 and 0", starts, received)
		}
		if on, off := len(ovlEvents(w, n1, "on")), len(ovlEvents(w, n1, "off")); on != 1 || off != 1 {
			tt.Errorf("%d event=on and %d event=off records, want 1 and 1: pressure did not begin and end", on, off)
		}
	})
}

// liveHeap returns the live heap in bytes, read with runtime/metrics.
func liveHeap(tb testing.TB) uint64 {
	tb.Helper()
	runtime.GC()
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		tb.Fatalf("metric %s has kind %v", sample[0].Name, sample[0].Value.Kind())
	}
	return sample[0].Value.Uint64()
}

// settledGoroutines returns runtime.NumGoroutine() once it has not changed for 20 ms, so that the
// goroutine of a finished subtest that is still returning does not count. If the count keeps
// changing it returns the last count after 10 s.
func settledGoroutines() int {
	n := runtime.NumGoroutine()
	deadline := time.Now().Add(10 * time.Second)
	for stable := 0; stable < 20 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
		if m := runtime.NumGoroutine(); m != n {
			n, stable = m, 0
		} else {
			stable++
		}
	}
	return n
}

// AT-OVL-25, OVL-230: a simulation with every host-state option set, a Go memory limit below the
// live heap, and metrics.Read, runtime.NumGoroutine, the goroutine profile and runtime.Stack
// replaced by panicking hooks, runs spawning and sending for 10 s of virtual time and passes; no
// admission source turns on, no kernel event is labeled lockstep/overload-sample, and the host's
// goroutine count and Go memory limit are the same after Simulate as before.
func TestATOVL25NoHostState(t *testing.T) {
	t.Cleanup(overload.SetReadMetrics(func([]metrics.Sample) { panic("simulation called metrics.Read") }))
	t.Cleanup(overload.SetNumGoroutine(func() int { panic("simulation called runtime.NumGoroutine") }))
	t.Cleanup(overload.SetGoroutineProfile(func(io.Writer) error { panic("simulation read the goroutine profile") }))
	t.Cleanup(overload.SetStack(func([]byte, bool) int { panic("simulation called runtime.Stack") }))
	limit := int64(max(liveHeap(t)/2, 1)) //nolint:gosec // half a heap size is far below math.MaxInt64
	old := debug.SetMemoryLimit(limit)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
	if got := debug.SetMemoryLimit(-1); got != limit {
		t.Fatalf("the Go memory limit is %d, want %d, below the live heap", got, limit)
	}

	opts := simOpts(2)
	opts.System = []lockstep.Option{
		lockstep.WithAdmission(1000, 500),
		lockstep.WithWatchdog(time.Millisecond),
		lockstep.WithGoroutineWarning(1),
	}
	before := settledGoroutines()
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var sink lockstep.PID[int]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			sink = simSpawn(sys, "sink", &simActor[int]{})
			simSpawn(sys, "ticker", &simActor[int]{
				started: func(ctx *lockstep.Context[int]) { ctx.After(100*time.Millisecond, 1) },
				receive: func(ctx *lockstep.Context[int], n int) {
					if err := lockstep.Send(ctx, sink, n); err != nil {
						panic(err)
					}
					if n%10 == 0 {
						simSpawn(ctx, "c"+strconv.Itoa(n), &simActor[int]{})
					}
					ctx.After(100*time.Millisecond, n+1)
				},
			})
		})
		w.RunFor(10 * time.Second)

		if recs := ovlEvents(w, n1, "on"); len(recs) != 0 {
			tt.Errorf("%d event=on records, first %q, want none", len(recs), ovlAttrs(recs[0]))
		}
		for _, r := range w.Sim.Records() {
			if r.Kind == "kernel.event" && r.Text == "lockstep/overload-sample" {
				tt.Errorf("a kernel event labeled lockstep/overload-sample at %v", r.At)
				break
			}
		}
		if got := len(simRecords(w, n1, "lockstep.spawn")); got < 11 {
			tt.Errorf("%d spawn records, want the actors of the run spawned", got)
		}
	})
	// The goroutine of a seed's subtest may still be returning when Simulate does: give it time.
	after := settledGoroutines()
	for deadline := time.Now().Add(10 * time.Second); after != before && time.Now().Before(deadline); after = settledGoroutines() {
		time.Sleep(time.Millisecond)
	}
	if after != before {
		t.Errorf("runtime.NumGoroutine() is %d after Simulate, was %d before", after, before)
	}
	if got := debug.SetMemoryLimit(-1); got != limit {
		t.Errorf("the Go memory limit is %d after Simulate, want %d: simulation must not set it", got, limit)
	}
}

// ovlSized is a message that reports its size.
type ovlSized struct{ n int }

func (m ovlSized) Size() int { return m.n }

// ovlPlain is a message without a Size.
type ovlPlain struct{}

// AT-OVL-33, OVL-410: A sends B, in one turn, messages of Size 10 and -5 and one without a Size. A
// World.Invariant records B's MailboxBytes after every event: 10 after A's turn (the other two
// count 0), 0 after B's first turn, and 0 at the end.
func TestATOVL33MailboxBytes(t *testing.T) {
	opts := simOpts(3)
	opts.System = []lockstep.Option{lockstep.WithMemoryEstimates()}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var seen []int64 // B's MailboxBytes after every event in which B is live, consecutive repeats once
		var bAddr string
		var a lockstep.PID[struct{}]
		s.Node("n1", func(sys *lockstep.System) {
			bp := simSpawn(sys, "b", &simActor[any]{})
			bAddr = bp.Address().String()
			a = simSpawn(sys, "a", &simActor[struct{}]{receive: func(ctx *lockstep.Context[struct{}], _ struct{}) {
				for _, m := range []any{ovlSized{10}, ovlSized{-5}, ovlPlain{}} {
					if err := lockstep.Send(ctx, bp, m); err != nil {
						panic(err)
					}
				}
			}})
		})
		w.Invariant("ovl/mailbox-bytes", func() error {
			sys := s.System("n1")
			if sys == nil {
				return nil
			}
			for _, e := range sys.Estimates(10) {
				if e.Addr.String() == bAddr && (len(seen) == 0 || seen[len(seen)-1] != e.MailboxBytes) {
					seen = append(seen, e.MailboxBytes)
				}
			}
			return nil
		})
		w.RunFor(10 * time.Millisecond)
		if err := sim.Send(s, "n1", a, struct{}{}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)

		// B exists from the boot, so seen starts with 0; the repeats are gone.
		if want := []int64{0, 10, 0}; !slices.Equal(seen, want) {
			tt.Errorf("B's MailboxBytes after events %v, want %v: 10 after A's turn, 0 after B's first turn and at the end", seen, want)
		}
	})
}

// ovlMem is an actor whose state size is mem.
type ovlMem struct{ mem int }

func (*ovlMem) Started(*lockstep.Context[int])                      {}
func (*ovlMem) Receive(*lockstep.Context[int], int)                 {}
func (*ovlMem) Stopped(*lockstep.Context[int], lockstep.StopReason) {}
func (a *ovlMem) MemSize() int                                      { return a.mem }

// AT-OVL-36, OVL-430: with estimates on, four actors that report 30, 50, 50 and 10 bytes are
// listed by Estimates(3) as the 50s in address order, then the 30; Estimates(0) is nil; with
// estimates off Estimates(3) is nil.
func TestATOVL36EstimatesOrder(t *testing.T) {
	run := func(t *testing.T, opts sim.Options, check func(tt testing.TB, sys *lockstep.System)) {
		sim.Simulate(t, opts, func(s *sim.Sim) {
			w := s.World()
			var pids []lockstep.PID[int]
			s.Node("n1", func(sys *lockstep.System) {
				pids = nil
				for _, c := range []struct {
					name string
					mem  int
				}{{"a30", 30}, {"b50", 50}, {"c50", 50}, {"d10", 10}} {
					pids = append(pids, simSpawnActor(sys, c.name, lockstep.Actor[int](&ovlMem{mem: c.mem})))
				}
			})
			w.RunFor(10 * time.Millisecond)
			for _, p := range pids { // one turn each reports the state size
				if err := sim.Send(s, "n1", p, 1); err != nil {
					w.T().Fatal(err)
				}
			}
			w.RunFor(100 * time.Millisecond)
			check(w.T(), s.System("n1"))
		})
	}
	on := simOpts(3)
	on.System = []lockstep.Option{lockstep.WithMemoryEstimates()}
	run(t, on, func(tt testing.TB, sys *lockstep.System) {
		var got []string
		for _, e := range sys.Estimates(3) {
			got = append(got, e.Addr.Path()+" "+strconv.FormatInt(e.StateBytes, 10))
		}
		if want := []string{"/user/b50 50", "/user/c50 50", "/user/a30 30"}; !slices.Equal(got, want) {
			tt.Errorf("Estimates(3) = %q, want %q", got, want)
		}
		if got := sys.Estimates(0); got != nil {
			tt.Errorf("Estimates(0) = %v, want nil", got)
		}
	})
	t.Run("estimates off", func(t *testing.T) {
		run(t, simOpts(3), func(tt testing.TB, sys *lockstep.System) {
			if got := sys.Estimates(3); got != nil {
				tt.Errorf("Estimates(3) with estimates off = %v, want nil", got)
			}
		})
	})
}

// AT-OVL-45, §6: AT-OVL-12, -13, -20 and -21 run under LOCKSTEP_CHECK_DETERMINISM=1, which sim
// reads once per Simulate call (SIM-030): every seed runs twice and faultline compares the two
// traces, so a body runs twice per seed. Each case runs without the Options switch, so only the
// environment can turn the comparison on.
func TestATOVL45Determinism(t *testing.T) {
	t.Setenv("LOCKSTEP_CHECK_DETERMINISM", "1")
	const seeds = 3
	opts := func() sim.Options {
		o := simOpts(seeds)
		o.CheckDeterminism = false
		return o
	}
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, opts sim.Options, runs *int)
	}{
		{"AT-OVL-12", runATOVL12},
		{"AT-OVL-13", runATOVL13},
		{"AT-OVL-20", runATOVL20},
		{"AT-OVL-21", runATOVL21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runs := 0
			tc.run(t, opts(), &runs)
			if runs != 2*seeds {
				t.Errorf("the body ran %d times for %d seeds, want %d: the determinism check did not run every seed twice", runs, seeds, 2*seeds)
			}
		})
	}
}

// AT-MBX-25, MBX-035: an actor A whose FailSend mailbox of size 1 is held and full has a pending
// timer, a piped ask answered at 5 ms and an effect with a latency of 7 ms, and pressure begins at
// 1 ms. Released at 20 ms, A receives the reply, the effect result and the timer message, and no
// lockstep.mailbox.full or event=shed record names A.
func TestATMBX25SelfEntriesUnderPressure(t *testing.T) {
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []string
		var a lockstep.PID[ovlNote]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			rp := simSpawn(sys, "b", ovlResponder(5*time.Millisecond, 0))
			a = simSpawn(sys, "a", &simActor[ovlNote]{
				started: func(ctx *lockstep.Context[ovlNote]) {
					ctx.After(10*time.Millisecond, ovlNote{kind: "tick"})
					r := lockstep.Ask(ctx, rp, ovlBuild)
					lockstep.Pipe(ctx, r, func(v int, _ error) ovlNote { return ovlNote{kind: "reply", v: v} })
					lockstep.Do(ctx, ovlFetch{}, func(v int, _ error) ovlNote { return ovlNote{kind: "effect", v: v} })
				},
				receive: func(_ *lockstep.Context[ovlNote], m ovlNote) { got = append(got, m.kind) },
			}, lockstep.WithMailbox(1, lockstep.FailSend))
		})
		s.HandleEffect("ovl.fetch", func(sim.EffectCall) sim.EffectResult {
			return sim.EffectResult{Value: 3, Latency: 7 * time.Millisecond}
		})
		w.RunFor(time.Millisecond)
		release := lockstep.Hold(s.System("n1"), a.Address())
		if err := sim.Send(s, "n1", a, ovlNote{kind: "filler"}); err != nil { // the mailbox is full
			tt.Fatal(err)
		}
		s.PressureFor("n1", time.Second)
		w.RunFor(19 * time.Millisecond)
		if len(got) != 0 {
			tt.Errorf("A received %q while held, want nothing", got)
		}
		release()
		w.RunFor(100 * time.Millisecond)

		slices.Sort(got)
		if want := []string{"effect", "filler", "reply", "tick"}; !slices.Equal(got, want) {
			tt.Errorf("A received %q, want %q", got, want)
		}
		if n := len(simRecords(w, n1, "lockstep.mailbox.full")); n != 0 {
			tt.Errorf("%d lockstep.mailbox.full records, want 0", n)
		}
		if shed := ovlEvents(w, n1, "shed"); len(shed) != 0 {
			tt.Errorf("%d event=shed records, first %q, want none", len(shed), ovlAttrs(shed[0]))
		}
	})
}

// AT-MBX-27, MBX-090: a Send from the test body to a FailSend actor with room, during a pressure
// episode, returns an error that matches ErrOverloaded and ErrMailboxFull with OVL-120's text, and
// records one event=shed and no lockstep.mailbox.full.
func TestATMBX27ShedMessages(t *testing.T) {
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var tp lockstep.PID[int]
		n1 := s.Node("n1", func(sys *lockstep.System) {
			tp = simSpawn(sys, "t", &simActor[int]{}, lockstep.WithMailbox(4, lockstep.FailSend))
		})
		w.RunFor(100 * time.Millisecond)
		s.PressureFor("n1", time.Second)
		err := sim.Send(s, "n1", tp, 1)
		w.RunFor(100 * time.Millisecond)

		checkShed(tt, "Send during the episode", err, tp.Address().String())
		if n := len(ovlEvents(w, n1, "shed")); n != 1 {
			tt.Errorf("%d event=shed records, want 1", n)
		}
		if n := len(simRecords(w, n1, "lockstep.mailbox.full")); n != 0 {
			tt.Errorf("%d lockstep.mailbox.full records, want 0", n)
		}
	})
}
