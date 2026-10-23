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
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/sim"
)

// Tick is the timer message of tickProbe.
type Tick struct{}

// tickProbe is an actor that sets *stopped in Stopped, asks the silent actor on n2 in Started,
// keeping the reply in *reply, and ticks every 100ms, so its node always has work scheduled.
type tickProbe struct {
	stopped *bool
	reply   **lockstep.Reply[int]
}

func (p *tickProbe) Started(ctx *lockstep.Context[Tick]) {
	*p.reply = lockstep.Ask(ctx, lockstep.PIDAt[Ping]("n2", "/user/silent"), buildPing(1))
	ctx.After(100*time.Millisecond, Tick{})
}

func (p *tickProbe) Receive(ctx *lockstep.Context[Tick], _ Tick) {
	ctx.After(100*time.Millisecond, Tick{})
}

func (p *tickProbe) Stopped(*lockstep.Context[Tick], lockstep.StopReason) { *p.stopped = true }

// crashRun is the state of AT-SIM-06's run, built anew at every body call. pid and reply belong
// to the latest boot of n1; old and oldPID to the one that crashed.
type crashRun struct {
	n1, n2  *kernel.Node
	setups  int
	pid     lockstep.PID[Tick]
	reply   *lockstep.Reply[int]
	stopped bool

	old     *lockstep.System
	oldPID  lockstep.PID[Tick]
	crashAt kernel.Time
}

// crashN1 adds n1, which runs a tickProbe at /user/p, and n2, which runs the silent actor; runs
// to t=1s, and crashes n1 with w.Faults.Inject.
func crashN1(s *sim.Sim) *crashRun {
	w, tt := s.World(), s.World().T()
	c := &crashRun{}
	c.n1 = s.Node("n1", func(sys *lockstep.System) {
		c.setups++
		p := &tickProbe{stopped: &c.stopped, reply: &c.reply}
		pid, err := lockstep.Spawn(sys, "p", func() lockstep.Actor[Tick] { return p })
		if err != nil {
			panic(err)
		}
		c.pid = pid
	})
	var silent lockstep.PID[Ping]
	c.n2 = s.Node("n2", silentSetup(&silent))
	w.RunFor(time.Second)
	if c.reply == nil || hooks.AskResolved(c.reply) {
		tt.Fatal("the probe's ask is not pending before the crash")
	}
	c.old, c.oldPID = s.System("n1"), c.pid
	w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n1"})
	c.crashAt = w.Sim.Now()
	return c
}

// afterCrash returns the records after the kernel.crash record of node n.
func afterCrash(tt testing.TB, recs []kernel.Record, n *kernel.Node) []kernel.Record {
	tt.Helper()
	i := slices.IndexFunc(recs, func(r kernel.Record) bool { return r.Kind == "kernel.crash" && r.Node == n.ID() })
	if i < 0 {
		tt.Fatalf("no kernel.crash record of %s", n.Name())
	}
	return recs[i+1:]
}

// noWorkAfter reports an error on tt for every kernel event run on node n and every lockstep record
// of n in recs.
func noWorkAfter(tt testing.TB, recs []kernel.Record, n *kernel.Node) {
	tt.Helper()
	for _, r := range recs {
		if r.Node != n.ID() {
			continue
		}
		if r.Kind == "kernel.event" || strings.HasPrefix(r.Kind, "lockstep.") {
			tt.Errorf("after the crash of %s: %s %q at %v", n.Name(), r.Kind, r.Text, r.At)
		}
	}
}

// AT-SIM-06, SIM-090, SIM-110, SIM-120: a crash discards n1's system. No Stopped hook runs and
// nothing is recorded or run on n1 afterwards, neither the probe's turns and timers nor its
// ask's timeout; the old system refuses sends, and its pending ask resolved with ErrStopped.
func TestATSIM06Crash(t *testing.T) {
	sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := crashN1(s)
		if sys := s.System("n1"); sys != nil {
			tt.Errorf("System(n1) = %p after the crash, want nil", sys)
		}
		w.RunFor(9 * time.Second) // past the ask's 5s timeout and many ticks

		if c.stopped {
			tt.Error("the probe's Stopped ran")
		}
		after := afterCrash(tt, w.Sim.Records(), c.n1)
		for _, r := range after {
			if r.Kind == "lockstep.stop" {
				tt.Errorf("lockstep.stop record %d after the crash", r.Seq)
			}
		}
		noWorkAfter(tt, after, c.n1)
		if err := lockstep.Send(c.old, c.oldPID, Tick{}); !errors.Is(err, lockstep.ErrStopped) {
			tt.Errorf("Send with the old system: %v, want an error wrapping ErrStopped", err)
		}
		if !hooks.AskResolved(c.reply) {
			tt.Fatal("the probe's ask is still pending after the crash")
		}
		if v, err := c.reply.Await(context.Background()); v != 0 || !errors.Is(err, lockstep.ErrStopped) {
			tt.Errorf("the probe's ask resolved with %d, %v; want 0 and an error wrapping ErrStopped", v, err)
		}
		if err := s.Stored(sim.CheckMisuse); err != nil {
			tt.Errorf("misuse stored: %v", err)
		}
	})
}

// AT-SIM-07, SIM-070, SIM-130: a restart boots a new system with epoch 2. Its actor has the old
// PID but for the uid, and a message to the old PID is a dead letter on n1.
func TestATSIM07Restart(t *testing.T) {
	sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := crashN1(s)
		w.RunFor(time.Second)
		w.Faults.Inject(fault.Event{Kind: fault.KindRestart, Node: "n1"})

		if c.setups != 2 {
			tt.Errorf("setup ran %d times, want 2", c.setups)
		}
		sys := s.System("n1")
		if sys == nil || sys == c.old || sys.Incarnation() != 2 || c.old.Incarnation() != 1 {
			tt.Fatalf("System(n1) = %p (old %p), incarnations %v and %d; want a new system with incarnation 2",
				sys, c.old, sys != nil && sys.Incarnation() == 2, c.old.Incarnation())
		}
		o, n := c.oldPID.Address(), c.pid.Address()
		if n.Node() != o.Node() || n.Path() != o.Path() || n.UID() == o.UID() || n.UID()>>32 != 2 || o.UID()>>32 != 1 {
			tt.Errorf("new PID %v, old %v; want the same PID but for a uid whose high 32 bits are 2", c.pid, c.oldPID)
		}

		if err := sim.Send(s, "n2", c.oldPID, Tick{}); err != nil {
			tt.Fatalf("Send from n2 to the old PID: %v", err)
		}
		w.RunFor(time.Second)
		dead := 0
		for _, r := range ofKind(w.Sim.Records(), "lockstep.deadletter") {
			if r.Node == c.n1.ID() && attr(r, "to") == c.oldPID.String() && attr(r, "reason") == "stopped" {
				dead++
			}
		}
		if dead != 1 {
			tt.Errorf("%d lockstep.deadletter records on n1 for the old PID with reason stopped, want 1", dead)
		}
	})
}

// AT-SIM-13, SIM-180: an actor's stream at each boot is the one labeled with that boot's epoch.
func TestATSIM13StreamsAcrossRestart(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var draws [][]uint64
		n1 := s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "a", &actor[Deposit]{started: func(ctx *lockstep.Context[Deposit]) {
				var d []uint64
				for range 3 {
					d = append(d, ctx.Rand().Uint64())
				}
				draws = append(draws, d)
			}})
		})
		w.RunFor(time.Millisecond)
		n1.Crash()
		n1.Restart()
		w.RunFor(time.Millisecond)

		ref := kernel.New(kernel.Config{Seed: w.Seed()})
		var want [][]uint64
		for _, label := range []string{"lockstep/n1/1/user/a/1", "lockstep/n1/2/user/a/1"} {
			r := ref.Rand(label)
			want = append(want, []uint64{r.Uint64(), r.Uint64(), r.Uint64()})
		}
		if !slices.EqualFunc(draws, want, slices.Equal) {
			tt.Errorf("draws %v, want those of lockstep/n1/1/user/a/1 then lockstep/n1/2/user/a/1 %v", draws, want)
		}
	})
}

// AT-SIM-18, SIM-200: a crash of the asking node ends sim.Ask at the crash time with ErrStopped.
func TestATSIM18CrashDuringAsk(t *testing.T) {
	sim.Simulate(t, fullTrace(5), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("n1", noSetup)
		s.Node("n2", func(sys *lockstep.System) {
			// A Ping with N > 0 is answered with N 2s later, through a timer Ping with -N.
			spawn(sys, "slow", &actor[Ping]{receive: func(ctx *lockstep.Context[Ping], m Ping) {
				if m.N > 0 {
					ctx.After(2*time.Second, Ping{N: -m.N, ReplyTo: m.ReplyTo})
					return
				}
				m.ReplyTo.Send(ctx, -m.N)
			}})
		})
		w.RunFor(time.Second)
		crashAt := w.Sim.Now().Add(time.Second)
		w.Plan(fault.Script(fault.Event{At: crashAt, Kind: fault.KindCrash, Node: "n1"}))
		v, err := sim.Ask(s, "n1", lockstep.PIDAt[Ping]("n2", "/user/slow"), buildPing(3))
		if v != 0 || !errors.Is(err, lockstep.ErrStopped) {
			tt.Errorf("Ask = %d, %v; want 0 and an error wrapping ErrStopped", v, err)
		}
		if now := w.Sim.Now(); now != crashAt {
			tt.Errorf("Now() = %v after Ask, want the crash time %v", now, crashAt)
		}
	})
}

// resource is an io.Closer that counts its Close calls.
type resource struct{ closed *int }

func (r resource) Close() error { *r.closed++; return nil }

// ownProbe owns a resource from Started and observes Stopped.
type ownProbe struct {
	res resource
	obs *[]string
}

func (p *ownProbe) Started(ctx *lockstep.Context[Tick]) { ctx.Own(p.res) }

func (p *ownProbe) Receive(*lockstep.Context[Tick], Tick) {}

func (p *ownProbe) Stopped(_ *lockstep.Context[Tick], reason lockstep.StopReason) {
	*p.obs = append(*p.obs, "stopped "+reason.String())
}

// AT-SUP-34, SUP-360: a node crash runs no Stopped hook, closes no resource and records no stop.
func TestATSUP34NodeCrash(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var obs []string
		closed := 0
		n1 := s.Node("n1", func(sys *lockstep.System) {
			p := &ownProbe{res: resource{closed: &closed}, obs: &obs}
			if _, err := lockstep.Spawn(sys, "p", func() lockstep.Actor[Tick] { return p }); err != nil {
				panic(err)
			}
		})
		w.RunFor(time.Second)
		n1.Crash()
		w.RunFor(time.Second)

		if len(obs) != 0 {
			tt.Errorf("observations %q, want none", obs)
		}
		if closed != 0 {
			tt.Errorf("the resource was closed %d times, want 0", closed)
		}
		for _, r := range afterCrash(tt, w.Sim.Records(), n1) {
			if r.Kind == "lockstep.stop" || r.Kind == "lockstep.close" {
				tt.Errorf("%s record %d after the crash", r.Kind, r.Seq)
			}
		}
	})
}

// fakeActor is an ActorRef that is not a cell: it reports whether a turn ran.
type fakeActor struct {
	slot engine.Slot
	ran  bool
}

func (a *fakeActor) Address() engine.Address { return engine.Address{Node: "n1", Path: "/user/fake"} }
func (a *fakeActor) Slot() *engine.Slot      { return &a.slot }
func (a *fakeActor) RunTurn(int) bool        { a.ran = true; return false }

// SIM-120: the engine of a crashed node ignores every seam call. Ready queues nothing, Deliver
// sends and records nothing, AfterFunc and Start schedule nothing and return a Cancel that reports
// false, Fail, Wait and the Tracer record and store nothing, and Now is still the node's clock.
func TestSIM120DeadEngine(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		n1 := s.Node("n1", noSetup)
		s.Node("n2", func(sys *lockstep.System) { spawn(sys, "r", &actor[Deposit]{}) })
		w.RunFor(time.Second)
		n1.Crash()
		before := len(w.Sim.Records())

		e := s.Engine("n1")
		a := &fakeActor{}
		e.Scheduler.Ready(a)
		if st := a.slot.State(); st != engine.SlotIdle {
			tt.Errorf("Ready on a dead engine left the slot %v, want idle", st)
		}
		for _, node := range []string{"n2", "nowhere"} {
			env := engine.Envelope{From: engine.Address{Node: "n1", Path: "/user"}, To: engine.Address{Node: node, Path: "/user/r"}, Payload: Deposit{1}}
			if err := e.Transport.Deliver(env); err != nil {
				tt.Errorf("Deliver to %s: %v, want nil", node, err)
			}
		}
		fired := false
		if e.Clock.AfterFunc(0, "test/timer", func() { fired = true })() {
			tt.Error("AfterFunc's Cancel reported true")
		}
		req := engine.EffectRequest{ID: 1, Name: "prices.fetch", Actor: engine.Address{Node: "n1", Path: "/user/a"}}
		if e.Effects.Start(req, func(any, error) { tt.Error("done called") })() {
			tt.Error("Start's Cancel reported true")
		}
		e.Tracer.Record("lockstep.send", engine.Attr{Key: "from", Value: "x"})
		e.Failer.Fail(errors.New("misuse after the crash"))
		if err := e.Waiter.Wait(context.Background(), make(chan struct{})); !errors.Is(err, lockstep.ErrAwaitInActor) {
			tt.Errorf("Wait on an open channel: %v, want an error wrapping ErrAwaitInActor", err)
		}
		if got, want := e.Clock.Now(), n1.Now(); !got.Equal(want) {
			tt.Errorf("Now() = %v, want n1.Now() %v", got, want)
		}

		w.RunFor(time.Second)
		if fired || a.ran {
			tt.Errorf("timer fired %v, turn ran %v; want neither", fired, a.ran)
		}
		if recs := w.Sim.Records()[before:]; len(recs) != 0 {
			tt.Errorf("%d records after the crash, the first %s %q; want none", len(recs), recs[0].Kind, recs[0].Text)
		}
		for _, check := range []string{sim.CheckMisuse, sim.CheckNoEffectHandler} {
			if err := s.Stored(check); err != nil {
				tt.Errorf("%s stored %v, want nothing", check, err)
			}
		}
	})
}

// SIM-120: an actor that crashes its own node in a turn leaves nothing behind. The rest of the
// turn records nothing, and neither the turn its self-send queued nor any other runs.
func TestSIM120CrashInsideTurn(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []string
		var n1 *kernel.Node
		var pid lockstep.PID[string]
		n1 = s.Node("n1", func(sys *lockstep.System) {
			pid = spawn(sys, "a", &actor[string]{receive: func(ctx *lockstep.Context[string], m string) {
				got = append(got, m)
				if m == "crash" {
					if err := lockstep.Send(ctx, ctx.Self(), "after"); err != nil {
						panic(err)
					}
					n1.Crash()
				}
			}})
		})
		w.RunFor(time.Second)
		if err := sim.Send(s, "n1", pid, "crash"); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Second)
		if !slices.Equal(got, []string{"crash"}) {
			tt.Errorf("received %q, want only crash", got)
		}
		noWorkAfter(tt, afterCrash(tt, w.Sim.Records(), n1), n1)
	})
}

// SIM-120, SIM-140: an envelope a paused node deferred is never sent once the node crashed, even
// after a restart; the new incarnation's sends start afresh.
func TestSIM120CrashDropsDeferredSend(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []int
		n1 := s.Node("n1", noSetup)
		n2 := s.Node("n2", func(sys *lockstep.System) {
			spawn(sys, "r", &actor[Deposit]{receive: func(_ *lockstep.Context[Deposit], m Deposit) { got = append(got, m.N) }})
		})
		r := lockstep.PIDAt[Deposit]("n2", "/user/r")
		w.RunFor(time.Second)
		n1.Pause()
		for i := 1; i <= 2; i++ {
			if err := sim.Send(s, "n1", r, Deposit{i}); err != nil {
				tt.Fatalf("Send %d from the paused node: %v", i, err)
			}
			w.RunFor(100 * time.Millisecond)
		}
		n1.Crash()
		n1.Restart()
		w.RunFor(time.Second)
		if err := sim.Send(s, "n1", r, Deposit{3}); err != nil {
			tt.Fatalf("Send after the restart: %v", err)
		}
		w.RunFor(time.Second)

		if !slices.Equal(got, []int{3}) {
			tt.Errorf("n2 received %v, want only 3", got)
		}
		recs := w.Sim.Records()
		for _, rec := range ofKind(recs, "kernel.event") {
			if rec.Text == "lockstep/send" {
				tt.Errorf("a lockstep/send event ran at %v", rec.At)
			}
		}
		if seqs := delivered(recs, n2); !slices.Equal(seqs, []string{"1"}) {
			tt.Errorf("lockstep.deliver seq on n2 %v, want 1", seqs)
		}
	})
}
