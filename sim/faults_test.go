// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/artifact"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// CMsg is the message type of the Counter.
type CMsg interface{ cmsg() }

// Inc adds one to the counter.
type Inc struct{}

// Count asks for the counter's value.
type Count struct{ ReplyTo lockstep.ReplyTo[int] }

func (Inc) cmsg()   {}
func (Count) cmsg() {}

// counter counts Inc messages. Each incarnation starts at 0. It appends 1 to *starts when it
// starts and 1 to *stopped when its Stopped hook runs.
type counter struct {
	n       int
	starts  *int
	stopped *int
}

func (c *counter) Started(*lockstep.Context[CMsg]) {
	if c.starts != nil {
		*c.starts++
	}
}

func (c *counter) Receive(ctx *lockstep.Context[CMsg], m CMsg) {
	switch m := m.(type) {
	case Inc:
		c.n++
	case Count:
		m.ReplyTo.Send(ctx, c.n)
	}
}

func (c *counter) Stopped(*lockstep.Context[CMsg], lockstep.StopReason) {
	if c.stopped != nil {
		*c.stopped++
	}
}

var errBoom = errors.New("boom")

// counterNode adds node name whose setup spawns a counter at /user/counter, sharing the observers
// of c, and records its PID in *pid. A node restart runs setup again, so *pid follows the latest
// boot.
func counterNode(s *sim.Sim, name string, c *counter, pid *lockstep.PID[CMsg]) *kernel.Node {
	return s.Node(name, func(sys *lockstep.System) {
		p, err := lockstep.Spawn(sys, "counter", func() lockstep.Actor[CMsg] {
			// Each incarnation counts from 0 and shares the observers of c.
			return &counter{starts: c.starts, stopped: c.stopped}
		})
		if err != nil {
			panic(err)
		}
		*pid = p
	})
}

// count asks the counter at pid on n1 for its value and fails the test on an error.
func count(tt testing.TB, s *sim.Sim, pid lockstep.PID[CMsg]) int {
	tt.Helper()
	n, err := sim.Ask(s, "n1", pid, func(r lockstep.ReplyTo[int]) CMsg { return Count{r} })
	if err != nil {
		tt.Fatalf("Ask(Count): %v", err)
	}
	return n
}

// faultRecords returns the lockstep.fault records with op, in order.
func faultRecords(recs []kernel.Record, op string) []kernel.Record {
	return slices.DeleteFunc(ofKind(recs, "lockstep.fault"), func(r kernel.Record) bool { return attr(r, "op") != op })
}

// indexOf returns the index in recs of the first record for which match is true, or -1.
func indexOf(recs []kernel.Record, match func(r kernel.Record) bool) int {
	return slices.IndexFunc(recs, match)
}

// AT-SFT-03, SFT-030, SFT-040: InjectFail fails the counter at its next turn with source injected;
// the message queued before that turn is not consumed and is delivered to the new incarnation,
// which starts at 0.
func TestATSFT03InjectFail(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var pid lockstep.PID[CMsg]
		starts := 0
		n1 := counterNode(s, "n1", &counter{starts: &starts}, &pid)
		w.RunFor(time.Second)
		for range 3 {
			if err := sim.Send(s, "n1", pid, CMsg(Inc{})); err != nil {
				tt.Fatal(err)
			}
		}
		if got := count(tt, s, pid); got != 3 {
			tt.Fatalf("count %d before the injection, want 3", got)
		}

		// An Inc is in the mailbox when the command is pushed: neither runs before the next turn.
		if err := sim.Send(s, "n1", pid, CMsg(Inc{})); err != nil {
			tt.Fatal(err)
		}
		if err := s.InjectFail(pid.Address(), errBoom); err != nil {
			tt.Errorf("InjectFail = %v, want nil", err)
		}
		w.RunFor(time.Second)
		if got := count(tt, s, pid); got != 1 {
			tt.Errorf("count %d after the restart, want 1: the new incarnation starts at 0 and gets the queued Inc", got)
		}
		if starts != 2 {
			tt.Errorf("the counter started %d times, want 2", starts)
		}

		// A second injection, with nothing queued, leaves a counter at 0.
		if err := s.InjectFail(pid.Address(), errBoom); err != nil {
			tt.Errorf("second InjectFail = %v, want nil", err)
		}
		w.RunFor(time.Second)
		if got := count(tt, s, pid); got != 0 {
			tt.Errorf("count %d after the second restart, want 0", got)
		}

		recs := w.Sim.Records()
		faults := faultRecords(recs, "inject-fail")
		if len(faults) != 2 {
			tt.Fatalf("%d lockstep.fault op=inject-fail records, want 2", len(faults))
		}
		f := faults[0]
		if f.Node != n1.ID() || f.Text != "fault inject-fail" ||
			!slices.Equal(attrKeys(f), []string{"op", "actor", "err", "result"}) ||
			attr(f, "actor") != pid.Address().String() || attr(f, "err") != "boom" || attr(f, "result") != "ok" {
			tt.Errorf("fault record %+v, want inject-fail of %s with err boom and result ok on n1", f, pid.Address())
		}
		fails := ofKind(recs, "lockstep.fail")
		if len(fails) != 2 {
			tt.Fatalf("%d lockstep.fail records, want 2", len(fails))
		}
		if attr(fails[0], "source") != "injected" || attr(fails[0], "err") != errBoom.Error() ||
			attr(fails[0], "actor") != pid.Address().String() || attr(fails[0], "inc") != "1" {
			tt.Errorf("lockstep.fail %v, want source injected, err boom and inc 1", fails[0].Attrs)
		}
		if got := attr(fails[1], "inc"); got != "2" {
			tt.Errorf("second lockstep.fail has inc %s, want 2", got)
		}

		// The queued Inc is consumed only after the failure: the turn that delivers it starts the
		// new incarnation first.
		failAt := indexOf(recs, func(r kernel.Record) bool { return r.Seq == fails[0].Seq })
		deliver := indexOf(recs[failAt:], func(r kernel.Record) bool {
			return r.Kind == "lockstep.turn" && attr(r, "user") == "1" && attr(r, "msg") == "sim_test.Inc"
		})
		start2 := indexOf(recs[failAt:], func(r kernel.Record) bool {
			return r.Kind == "lockstep.start" && attr(r, "inc") == "2"
		})
		if deliver < 0 || start2 < 0 || start2 > deliver {
			tt.Errorf("after the failure: start of incarnation 2 at %d, delivery turn at %d, want the start first", start2, deliver)
		}
		if err := s.Stored(sim.CheckMisuse); err != nil {
			tt.Errorf("misuse stored: %v", err)
		}
	})
}

// AT-SFT-04, SFT-030, SFT-050: the errors of InjectFail and StopActor, each recorded in the result
// attribute of lockstep.fault on the actor's node; a nil error panics.
func TestATSFT04InjectFailErrors(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var pid, other lockstep.PID[CMsg]
		n1 := counterNode(s, "n1", &counter{}, &pid)
		n2 := counterNode(s, "n2", &counter{}, &other)
		w.RunFor(time.Second)

		// A stopped actor's address.
		stopped := pid.Address()
		if err := s.StopActor(stopped); err != nil {
			tt.Fatalf("StopActor = %v", err)
		}
		w.RunFor(time.Second)
		wantNF := "lockstep: not found: " + stopped.String()
		if err := s.InjectFail(stopped, errBoom); !errors.Is(err, lockstep.ErrNotFound) || err.Error() != wantNF {
			tt.Errorf("InjectFail of a stopped actor = %v, want ErrNotFound with text %q", err, wantNF)
		}

		// An actor of a crashed node.
		down := other.Address()
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n2"})
		wantDown := "lockstep: sim: node n2 is down: lockstep: stopped"
		if err := s.InjectFail(down, errBoom); !errors.Is(err, lockstep.ErrStopped) || err.Error() != wantDown {
			tt.Errorf("InjectFail on a crashed node = %v, want ErrStopped with text %q", err, wantDown)
		}
		if err := s.StopActor(down); !errors.Is(err, lockstep.ErrStopped) || err.Error() != wantDown {
			tt.Errorf("StopActor on a crashed node = %v, want ErrStopped with text %q", err, wantDown)
		}

		// The zero address names no node: the record goes to node 0.
		if err := s.InjectFail(lockstep.Address{}, errBoom); !errors.Is(err, lockstep.ErrNotFound) {
			tt.Errorf("InjectFail(zero) = %v, want ErrNotFound", err)
		}

		// Misuse panics and records nothing.
		before := len(ofKind(w.Sim.Records(), "lockstep.fault"))
		expectPanic(tt, "lockstep: sim: InjectFail: nil error", func() { _ = s.InjectFail(pid.Address(), nil) })
		expectPanic(tt, `lockstep: sim: unknown node "zz"`, func() {
			_ = s.InjectFail(lockstep.PIDAt[CMsg]("zz", "/user/counter").Address(), errBoom)
		})
		if got := len(ofKind(w.Sim.Records(), "lockstep.fault")); got != before {
			tt.Errorf("%d lockstep.fault records after the panics, want %d", got, before)
		}

		type want struct {
			op     string
			node   kernel.NodeID
			actor  lockstep.Address
			result string
		}
		for _, c := range []want{
			{"stop", n1.ID(), stopped, "ok"},
			{"inject-fail", n1.ID(), stopped, wantNF},
			{"inject-fail", n2.ID(), down, wantDown},
			{"stop", n2.ID(), down, wantDown},
			{"inject-fail", 0, lockstep.Address{}, "lockstep: not found: <zero>"},
		} {
			rs := faultRecords(w.Sim.Records(), c.op)
			i := indexOf(rs, func(r kernel.Record) bool {
				return attr(r, "actor") == c.actor.String() && attr(r, "result") == c.result
			})
			if i < 0 {
				tt.Errorf("no lockstep.fault op=%s actor=%s result=%q in %v", c.op, c.actor, c.result, rs)
				continue
			}
			if rs[i].Node != c.node {
				tt.Errorf("lockstep.fault op=%s actor=%s on node %d, want %d", c.op, c.actor, rs[i].Node, c.node)
			}
		}
	})
}

// AT-SFT-05, SFT-040: two injections before the actor's next turn fail it once; the second is
// processed while the actor is Suspended and dropped with reason not-running.
func TestATSFT05DoubleInject(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var pid lockstep.PID[CMsg]
		starts := 0
		counterNode(s, "n1", &counter{starts: &starts}, &pid)
		w.RunFor(time.Second)
		for range 2 {
			if err := s.InjectFail(pid.Address(), errBoom); err != nil {
				tt.Errorf("InjectFail = %v, want nil", err)
			}
		}
		w.RunFor(time.Second)

		recs := w.Sim.Records()
		if n := len(ofKind(recs, "lockstep.fail")); n != 1 {
			tt.Errorf("%d lockstep.fail records, want 1", n)
		}
		drops := ofKind(recs, "lockstep.drop")
		if len(drops) != 1 {
			tt.Fatalf("%d lockstep.drop records, want 1", len(drops))
		}
		d := drops[0]
		if attr(d, "to") != pid.Address().String() || attr(d, "reason") != "not-running" || attr(d, "msg") != "inject" {
			tt.Errorf("lockstep.drop %v, want to %s, reason not-running, msg inject", d.Attrs, pid.Address())
		}
		fail := ofKind(recs, "lockstep.fail")[0]
		if d.Seq < fail.Seq {
			tt.Errorf("the drop (seq %d) precedes the failure (seq %d)", d.Seq, fail.Seq)
		}
		if starts != 2 {
			tt.Errorf("the counter started %d times, want 2: one restart", starts)
		}
		if got := len(faultRecords(recs, "inject-fail")); got != 2 {
			tt.Errorf("%d inject-fail fault records, want 2", got)
		}
		if got := s.Counts().Drops; got != 1 {
			tt.Errorf("Counts().Drops = %d, want 1", got)
		}
	})
}

// watcher records the Terminated signals it gets in *got, for the actor at target.
type watcher struct {
	target lockstep.Address
	got    *[]lockstep.Terminated
}

func (w *watcher) Started(ctx *lockstep.Context[Tick]) {
	if err := lockstep.Watch(ctx, w.target); err != nil {
		panic(err)
	}
}

func (*watcher) Receive(*lockstep.Context[Tick], Tick) {}

func (w *watcher) Signal(_ *lockstep.Context[Tick], sig lockstep.Signal) {
	if t, ok := sig.(lockstep.Terminated); ok {
		*w.got = append(*w.got, t)
	}
}

// AT-SFT-06, SFT-050: StopActor stops the counter as lockstep.Stop does: the watcher gets
// Terminated with reason Normal, Stopped runs, lockstep.fault op=stop result=ok; a second call
// returns the error of Stop for a dead address.
func TestATSFT06StopActor(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var pid lockstep.PID[CMsg]
		var got []lockstep.Terminated
		stopped := 0
		n1 := s.Node("n1", func(sys *lockstep.System) {
			p, err := lockstep.Spawn(sys, "counter", func() lockstep.Actor[CMsg] { return &counter{stopped: &stopped} })
			if err != nil {
				panic(err)
			}
			pid = p
			if _, err := lockstep.Spawn(sys, "watcher", func() lockstep.Actor[Tick] {
				return &watcher{target: p.Address(), got: &got}
			}); err != nil {
				panic(err)
			}
		})
		w.RunFor(time.Second)
		addr := pid.Address()
		if err := s.StopActor(addr); err != nil {
			tt.Errorf("StopActor = %v, want nil", err)
		}
		w.RunFor(time.Second)

		if stopped != 1 {
			tt.Errorf("Stopped ran %d times, want 1", stopped)
		}
		if len(got) != 1 || got[0].Addr != addr || got[0].Reason != lockstep.Normal {
			tt.Errorf("watcher got %v, want one Terminated of %s with reason Normal", got, addr)
		}
		recs := w.Sim.Records()
		faults := faultRecords(recs, "stop")
		if len(faults) != 1 {
			tt.Fatalf("%d lockstep.fault op=stop records, want 1", len(faults))
		}
		f := faults[0]
		if f.Node != n1.ID() || f.Text != "fault stop" || !slices.Equal(attrKeys(f), []string{"op", "actor", "result"}) ||
			attr(f, "actor") != addr.String() || attr(f, "result") != "ok" {
			tt.Errorf("fault record %+v, want stop of %s with result ok on n1", f, addr)
		}
		stops := ofKind(recs, "lockstep.stop")
		if i := indexOf(stops, func(r kernel.Record) bool { return attr(r, "actor") == addr.String() }); i < 0 {
			tt.Error("no lockstep.stop record of the counter")
		}

		want := "lockstep: not found: " + addr.String()
		if err := s.StopActor(addr); !errors.Is(err, lockstep.ErrNotFound) || err.Error() != want {
			tt.Errorf("second StopActor = %v, want ErrNotFound with text %q", err, want)
		}
		second := faultRecords(w.Sim.Records(), "stop")
		if len(second) != 2 || attr(second[1], "result") != want {
			tt.Errorf("fault records after the second call: %v, want a second one with result %q", second, want)
		}
	})
}

// SFT-040, SUP-060: an inject queued behind a stop is discarded by the final stop's drain with a
// record. StopActor then InjectFail in one body step: InjectFail returns nil, the counter does not
// fail, and the trace holds exactly one lockstep.drop to=<counter> reason=not-running msg=inject,
// after both lockstep.fault records and, as SUP-060 step 5 drains before step 6 records the stop,
// before the counter's lockstep.stop.
func TestSFT040InjectQueuedBehindStop(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var pid lockstep.PID[CMsg]
		counterNode(s, "n1", &counter{}, &pid)
		w.RunFor(time.Second)
		addr := pid.Address()
		if err := s.StopActor(addr); err != nil {
			tt.Errorf("StopActor = %v, want nil", err)
		}
		if err := s.InjectFail(addr, errBoom); err != nil {
			tt.Errorf("InjectFail after StopActor = %v, want nil", err)
		}
		w.RunFor(time.Second)

		recs := w.Sim.Records()
		if n := len(ofKind(recs, "lockstep.fail")); n != 0 {
			tt.Errorf("%d lockstep.fail records, want 0", n)
		}
		drops := ofKind(recs, "lockstep.drop")
		if len(drops) != 1 {
			tt.Fatalf("%d lockstep.drop records, want 1", len(drops))
		}
		d := drops[0]
		if attr(d, "to") != addr.String() || attr(d, "reason") != "not-running" || attr(d, "msg") != "inject" {
			tt.Errorf("lockstep.drop %v, want to %s, reason not-running, msg inject", d.Attrs, addr)
		}
		for _, op := range []string{"stop", "inject-fail"} {
			if f := faultRecords(recs, op); len(f) != 1 || attr(f[0], "result") != "ok" || d.Seq < f[0].Seq {
				tt.Errorf("lockstep.fault op=%s records %v, want one with result ok before the drop (seq %d)", op, f, d.Seq)
			}
		}
		stops := ofKind(recs, "lockstep.stop")
		i := indexOf(stops, func(r kernel.Record) bool { return attr(r, "actor") == addr.String() })
		if i < 0 {
			tt.Fatal("no lockstep.stop record of the counter")
		}
		if d.Seq > stops[i].Seq {
			tt.Errorf("the drop (seq %d) follows the counter's lockstep.stop (seq %d)", d.Seq, stops[i].Seq)
		}
		if got := s.Counts().Drops; got != 1 {
			tt.Errorf("Counts().Drops = %d, want 1", got)
		}
	})
}

// AT-EFF-09, EFF-100: StopActor at +1s cancels the Quoter's pending effect (latency 5s): the
// cancellation is recorded before the Stopped hook and lockstep.stop, the Quoter never gets a
// Fetched, and the effect's lockstep/effect event never runs.
func TestATEFF09CancelOnStop(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		var n1 *kernel.Node
		s.HandleEffect(fetchName, constLatency(int64(100), 5*time.Second))
		n1, q := addQuoter(s, &quoter{
			got: &got,
			stopped: func(*lockstep.Context[QMsg]) {
				w.Sim.Emit(kernel.Record{Node: n1.ID(), Kind: "test.stopped", Text: "Stopped hook"})
			},
		})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{"X"})
		w.RunFor(time.Second)
		if err := s.StopActor(q.Address()); err != nil {
			tt.Fatalf("StopActor = %v", err)
		}
		w.RunFor(10 * time.Second)

		recs := w.Sim.Records()
		if len(got) != 0 {
			tt.Errorf("Fetched %v, want none", got)
		}
		done := indexOf(recs, func(r kernel.Record) bool {
			return r.Kind == "lockstep.effect.done" && attr(r, "outcome") == "cancelled"
		})
		hook := indexOf(recs, func(r kernel.Record) bool { return r.Kind == "test.stopped" })
		stop := indexOf(recs, func(r kernel.Record) bool { return r.Kind == "lockstep.stop" })
		if done < 0 || hook < 0 || stop < 0 || done > hook || hook > stop {
			tt.Errorf("record positions: outcome=cancelled %d, Stopped hook %d, lockstep.stop %d; want them in this order", done, hook, stop)
		}
		if n := len(ofKind(recs, "lockstep.effect.done")); n != 1 {
			tt.Errorf("%d lockstep.effect.done records, want 1", n)
		}
		if n := ran(recs, n1, "lockstep/effect") + ran(recs, n1, "lockstep/effect-timeout"); n != 0 {
			tt.Errorf("%d lockstep/effect and lockstep/effect-timeout events ran, want 0", n)
		}
	})
}

// crashServers are the servers of AT-SFT-01.
var crashServers = []string{"n1", "n2", "n3"}

// kernelCrashes is the world of AT-SFT-01: servers n1 to n3, each with a counter, a client that
// sends Inc to all three every 100ms, and fault.Random crashing servers. It runs to the end of the
// run. setups counts the boots of every server, in crashServers order, and *systems collects the
// System of every boot.
func kernelCrashes(s *sim.Sim, setups *[3]int, systems *[]*lockstep.System) {
	w := s.World()
	for i, name := range crashServers {
		s.Node(name, func(sys *lockstep.System) {
			setups[i]++
			*systems = append(*systems, sys)
			spawn(sys, "counter", &actor[CMsg]{receive: func(*lockstep.Context[CMsg], CMsg) {}})
		})
	}
	s.Client("c", func(sys *lockstep.System) {
		spawn(sys, "driver", &actor[Tick]{
			started: func(ctx *lockstep.Context[Tick]) { ctx.After(100*time.Millisecond, Tick{}) },
			receive: func(ctx *lockstep.Context[Tick], _ Tick) {
				for _, name := range crashServers {
					//lockstep:allow LS220 a send to a crashed server may fail; the run needs only the attempts
					_ = lockstep.Send(ctx, lockstep.PIDAt[CMsg](name, "/user/counter"), CMsg(Inc{}))
				}
				ctx.After(100*time.Millisecond, Tick{})
			},
		})
	})
	w.Plan(&fault.Random{Rules: []fault.Rule{{Kind: fault.KindCrash, Every: 2 * time.Second, MaxFor: time.Second}}})
	w.RunFor(w.End().Sub(w.Sim.Now()))
}

// AT-SFT-01, SFT-010: kernel crashes reach lockstep nodes through faultline: every crash is
// followed by a fresh System (one setup per boot, one more per restart), and sim records no
// lockstep.fault of its own.
func TestATSFT01KernelCrashes(t *testing.T) {
	opts := fullTrace(3)
	opts.Duration = 30 * time.Second
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var setups [3]int
		var systems []*lockstep.System
		kernelCrashes(s, &setups, &systems)

		recs := w.Sim.Records()
		crashes := len(ofKind(recs, "kernel.crash"))
		if crashes == 0 {
			tt.Fatal("no kernel.crash record: the rule never fired")
		}
		// Every crash is followed by a boot of the same node, so each node boots once and once more
		// per crash, and runs setup at every boot.
		total := 0
		for i, name := range crashServers {
			id := s.World().Sim.Lookup(name).ID()
			onNode := func(kind string) int {
				return len(slices.DeleteFunc(ofKind(recs, kind), func(r kernel.Record) bool { return r.Node != id }))
			}
			if boots, crashes := onNode("kernel.boot"), onNode("kernel.crash"); boots != 1+crashes || setups[i] != boots {
				tt.Errorf("node %s: %d setup runs, %d boots and %d crashes, want one boot and one setup run per crash plus the first", name, setups[i], boots, crashes)
			}
			total += setups[i]
		}
		if want := len(crashServers) + crashes; total != want {
			tt.Errorf("%d setup runs, want %d", total, want)
		}
		for i, sys := range systems {
			if sys == nil || slices.Index(systems, sys) != i {
				tt.Errorf("boot %d reused the System of an earlier boot", i)
			}
		}
		for _, c := range crashServers {
			if s.System(c) == nil {
				tt.Errorf("node %s has no System at the end", c)
			}
		}
		if n := len(ofKind(recs, "lockstep.fault")); n != 0 {
			tt.Errorf("%d lockstep.fault records, want none: sim injects no fault of its own", n)
		}
		if err := s.Stored(sim.CheckMisuse); err != nil {
			tt.Errorf("misuse stored: %v", err)
		}
	})
}

// The scenario of TestATSFT01ScheduleReplay: AT-SFT-01's world, one seed.
var _ = scenario("sft-kernel-crashes", func(t *testing.T) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1, Duration: 30 * time.Second}}, func(s *sim.Sim) {
		var setups [3]int
		var systems []*lockstep.System
		kernelCrashes(s, &setups, &systems)
	})
})

// artifactOf finds the one artifact directory under dir and returns its report and the directory.
func artifactOf(t *testing.T, dir string) (*artifact.Report, string) {
	t.Helper()
	var reports []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "report.json" {
			reports = append(reports, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 {
		t.Fatalf("%d report.json files under %s, want 1", len(reports), dir)
	}
	f, err := os.Open(reports[0])
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read-only
	r, err := artifact.ReadReport(f)
	if err != nil {
		t.Fatal(err)
	}
	return r, filepath.Dir(reports[0])
}

// traceRecords returns the records of the trace.jsonl in dir.
func traceRecords(t *testing.T, dir string) []kernel.Record {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "trace.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read-only
	tr, err := artifact.ReadTrace(f)
	if err != nil {
		t.Fatal(err)
	}
	return tr.Records
}

// lockstepTrace returns the lockstep records and the crashes and boots of the trace.jsonl in dir,
// as text without sequence numbers and causes, which a replay may number differently.
func lockstepTrace(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	for _, r := range traceRecords(t, dir) {
		if strings.HasPrefix(r.Kind, "lockstep.") || r.Kind == "kernel.crash" || r.Kind == "kernel.boot" {
			out = append(out, fmt.Sprintf("%v node=%d inc=%d %s %q %v", r.At, r.Node, r.Inc, r.Kind, r.Text, r.Attrs))
		}
	}
	return out
}

// scheduleKinds returns the kind of every event of the schedule.json at path.
func scheduleKinds(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Events []struct {
			Kind string `json:"kind"`
		} `json:"events"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	kinds := make([]string, len(doc.Events))
	for i, e := range doc.Events {
		kinds[i] = e.Kind
	}
	return kinds
}

// AT-SFT-01, SFT-010, SFT-180: schedule.json lists only kernel faults, and replaying it with
// FAULTLINE_SCHEDULE runs the same crashes: faultline's replay has no planner records, so its
// trace hash differs from the planner run's, but the replay itself is deterministic and every
// lockstep record is the same.
func TestATSFT01ScheduleReplay(t *testing.T) {
	run := func(extra ...string) (*artifact.Report, string) {
		dir := t.TempDir()
		env := append([]string{"FAULTLINE_ARTIFACTS=" + dir, "FAULTLINE_TRACE=full"}, extra...)
		if out, code := runScenario(t, "sft-kernel-crashes", env...); code != 0 {
			t.Fatalf("scenario exited %d\n%s", code, out)
		}
		return artifactOf(t, dir)
	}
	orig, origDir := run()
	kinds := scheduleKinds(t, filepath.Join(origDir, "schedule.json"))
	if len(kinds) == 0 {
		t.Fatal("schedule.json lists no fault")
	}
	for _, k := range kinds {
		if !slices.Contains(fault.Kinds(), fault.Kind(k)) || strings.HasPrefix(k, "lockstep") {
			t.Errorf("schedule.json lists kind %q, want kernel faults only", k)
		}
	}
	if !slices.Contains(kinds, string(fault.KindCrash)) {
		t.Errorf("schedule.json kinds %q list no crash", kinds)
	}

	replayEnv := "FAULTLINE_SCHEDULE=" + filepath.Join(origDir, "schedule.json")
	first, firstDir := run(replayEnv)
	second, _ := run(replayEnv)
	if first.Run.TraceHash != second.Run.TraceHash {
		t.Errorf("two replays of the schedule have trace hashes %s and %s, want equal", first.Run.TraceHash, second.Run.TraceHash)
	}
	if first.Run.Events != orig.Run.Events {
		t.Errorf("the replay ran %d events, the original %d, want the same", first.Run.Events, orig.Run.Events)
	}
	want, got := lockstepTrace(t, origDir), lockstepTrace(t, firstDir)
	if !slices.Equal(got, want) {
		t.Errorf("the replay's lockstep records and crashes differ from the original's (%d and %d records)", len(got), len(want))
	}
}
