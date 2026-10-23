// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-SIM-08, SIM-140, SIM-160: a send from a paused node leaves the node once it resumes, from a
// node event labeled lockstep/send, and reaches the other node after that.
func TestATSIM08PausedSender(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []kernel.Time
		n1 := s.Node("n1", noSetup)
		s.Node("n2", func(sys *lockstep.System) {
			spawn(sys, "r", &actor[Deposit]{receive: func(*lockstep.Context[Deposit], Deposit) {
				got = append(got, w.Sim.Now())
			}})
		})
		start := w.Sim.Now()
		w.RunFor(time.Second)
		n1.Pause()
		w.RunFor(500 * time.Millisecond)
		if err := sim.Send(s, "n1", lockstep.PIDAt[Deposit]("n2", "/user/r"), Deposit{1}); err != nil {
			tt.Fatalf("Send from the paused node: %v, want nil", err)
		}
		w.RunFor(500 * time.Millisecond)
		n1.Resume()
		w.RunFor(time.Second)

		resumed := start.Add(2 * time.Second)
		recs := w.Sim.Records()
		i := slices.IndexFunc(recs, func(r kernel.Record) bool { return r.Kind == "net.send" })
		if i < 0 {
			tt.Fatal("no net.send record")
		}
		if n := len(ofKind(recs, "net.send")); n != 1 {
			tt.Errorf("%d net.send records, want 1", n)
		}
		if recs[i].Node != n1.ID() || recs[i].At != resumed {
			tt.Errorf("net.send on node %d at %v, want node %d at %v", recs[i].Node, recs[i].At, n1.ID(), resumed)
		}
		last := lastOfKind(recs[:i], "kernel.event")
		if last == nil || last.Text != "lockstep/send" || last.Node != n1.ID() || last.At != resumed {
			tt.Errorf("the last kernel.event before net.send is %+v, want lockstep/send on n1 at %v", last, resumed)
		}
		if len(got) != 1 || got[0] <= resumed {
			tt.Errorf("r received at %v, want once after %v", got, resumed)
		}
	})
}

// receiver returns the setup of a node with an actor r at /user/r that appends the number of
// every Num it receives to *got.
func receiver(got *[]int) func(sys *lockstep.System) {
	return func(sys *lockstep.System) {
		spawn(sys, "r", &actor[Num]{receive: func(_ *lockstep.Context[Num], m Num) { *got = append(*got, m.N) }})
	}
}

// SIM-140, SIM-160, ENG-080: envelopes from a paused node leave in Deliver order, and one
// delivered after the node resumes, while earlier ones still wait for their lockstep/send events,
// queues behind them: on FIFO links every envelope arrives, in order, with no stale drop.
func TestSIM140DeferredSendsKeepOrder(t *testing.T) {
	for _, tc := range []struct {
		name          string
		paused, after int // envelopes delivered while paused, and right after Resume
	}{
		{"one-then-one", 1, 1},
		{"several-paused", 5, 0},
		{"several-then-several", 5, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
				w, tt := s.World(), s.World().T()
				var got []int
				n1 := s.Node("n1", noSetup)
				n2 := s.Node("n2", receiver(&got))
				w.RunFor(time.Second)
				r := lockstep.PIDAt[Num]("n2", "/user/r")
				n1.Pause()
				for n := 1; n <= tc.paused+tc.after; n++ {
					if n == tc.paused+1 {
						n1.Resume()
					}
					if err := sim.Send(s, "n1", r, Num{n}); err != nil {
						tt.Fatalf("Send %d: %v", n, err)
					}
				}
				n1.Resume() // no-op unless every send ran while paused
				w.RunFor(time.Second)
				if want := numbers(1, tc.paused+tc.after); !slices.Equal(got, want) {
					tt.Errorf("r got %v, want %v", got, want)
				}
				if n := len(staleDrops(w.Sim.Records(), n2)); n != 0 {
					tt.Errorf("%d stale-seq drops, want 0", n)
				}
			})
		})
	}
}

// SIM-120, SIM-170, SIM-195: the simnet handler of a dead engine, whose node stays up after its
// system shut down, ignores every arrival: an envelope reaches no actor and a payload that is not
// lockstep's fails nothing.
func TestSIM170DeadEngineIgnoresArrivals(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []int
		n1 := s.Node("n1", noSetup)
		n2 := s.Node("n2", receiver(&got))
		w.RunFor(time.Millisecond)
		if err := s.System("n2").Shutdown(context.Background()); err != nil {
			tt.Fatalf("Shutdown: %v", err)
		}
		w.RunFor(time.Millisecond)
		if s.Engine("n2").Clock.AfterFunc(time.Hour, "test/probe", func() {})() {
			tt.Fatal("n2's engine is not dead after its system shut down")
		}
		if err := sim.Send(s, "n1", lockstep.PIDAt[Num]("n2", "/user/r"), Num{1}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.Net.Send(n1, n2.ID(), "hello")
		w.RunFor(time.Second)

		if len(got) != 0 {
			tt.Errorf("r on the dead engine's node got %v, want nothing", got)
		}
		if err := s.Stored(sim.CheckMisuse); err != nil {
			tt.Errorf("lockstep/misuse holds %v, want nothing", err)
		}
		recs := w.Sim.Records()
		if n := len(ofKind(recs, "net.deliver")); n != 2 {
			tt.Errorf("%d net.deliver records, want 2: both payloads reached n2's handler", n)
		}
		for _, kind := range []string{"lockstep.deliver", "lockstep.drop", "lockstep.misuse"} {
			for _, r := range ofKind(recs, kind) {
				if r.Node == n2.ID() {
					tt.Errorf("%s record on n2: %+v, want none", kind, r)
				}
			}
		}
	})
}

// lastOfKind returns the last record of kind in recs, or nil.
func lastOfKind(recs []kernel.Record, kind string) *kernel.Record {
	for i := len(recs) - 1; i >= 0; i-- {
		if recs[i].Kind == kind {
			return &recs[i]
		}
	}
	return nil
}

// Num is a numbered message.
type Num struct{ N int }

// burst makes the sender of TestATSIM10PerPairOrder send N numbered messages, From first, in one
// turn.
type burst struct{ From, N int }

// AT-SIM-10, SIM-160, SIM-170: on FIFO links the messages of one pair arrive in send order, each
// numbered with its position. On a jittered link that reorders, the receiver still sees
// increasing numbers: a copy that arrives after a later one is dropped as stale.
func TestATSIM10PerPairOrder(t *testing.T) {
	staleSeeds := map[uint64]bool{} // read by key; a seed that runs again sets the same entry
	sim.SimulateSeeds(t, fullTrace(0), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []int
		var sender lockstep.PID[burst]
		r := lockstep.PIDAt[Num]("n2", "/user/r")
		n1 := s.Node("n1", func(sys *lockstep.System) {
			sender = spawn(sys, "s", &actor[burst]{receive: func(ctx *lockstep.Context[burst], b burst) {
				for n := b.From; n < b.From+b.N; n++ {
					if err := lockstep.Send(ctx, r, Num{n}); err != nil {
						panic(err)
					}
				}
			}})
		})
		n2 := s.Node("n2", func(sys *lockstep.System) {
			spawn(sys, "r", &actor[Num]{receive: func(_ *lockstep.Context[Num], m Num) { got = append(got, m.N) }})
		})
		w.RunFor(time.Millisecond)

		if err := sim.Send(s, "n1", sender, burst{From: 1, N: 100}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Second)
		var want []string
		for n := 1; n <= 100; n++ {
			want = append(want, strconv.Itoa(n))
		}
		if !slices.Equal(got, numbers(1, 100)) {
			tt.Errorf("FIFO links: r got %v, want 1..100 in order", got)
		}
		if seqs := delivered(w.Sim.Records(), n2); !slices.Equal(seqs, want) {
			tt.Errorf("FIFO links: lockstep.deliver seq on n2 %v, want 1..100", seqs)
		}
		if n := len(staleDrops(w.Sim.Records(), n2)); n != 0 {
			tt.Errorf("FIFO links: %d stale-seq drops, want 0", n)
		}

		w.Net.SetLink(n1.ID(), n2.ID(), simnet.Link{Jitter: 50 * time.Millisecond})
		got = nil
		if err := sim.Send(s, "n1", sender, burst{From: 101, N: 100}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Second)
		for i := 1; i < len(got); i++ {
			if got[i] <= got[i-1] {
				tt.Errorf("jittered link: r got %v, want strictly increasing numbers", got)
				break
			}
		}
		drops := staleDrops(w.Sim.Records(), n2)
		if len(drops)+len(got) != 100 {
			tt.Errorf("jittered link: %d stale-seq drops and %d delivered, want 100 in all", len(drops), len(got))
		}
		for _, d := range drops {
			if keys := attrKeys(d); !slices.Equal(keys, []string{"to", "reason", "from", "seq", "msg"}) {
				tt.Errorf("stale-seq drop attributes %v, want to, reason, from, seq, msg", keys)
			}
			if d.Text != "drop to "+r.String() || attr(d, "to") != r.String() || attr(d, "from") != sender.String() {
				tt.Errorf("stale-seq drop %q to %q from %q, want to %s from %s", d.Text, attr(d, "to"), attr(d, "from"), r, sender)
			}
			if n, err := strconv.Atoi(attr(d, "seq")); err != nil || n < 101 || n > 200 {
				tt.Errorf("stale-seq drop seq %q, want 101..200", attr(d, "seq"))
			}
		}
		if len(drops) > 0 {
			staleSeeds[w.Seed()] = true
		}
	}, 1, 2, 3, 4, 5)
	if len(staleSeeds) == 0 {
		t.Error("no seed of five recorded a stale-seq drop on the jittered link")
	}
}

// numbers returns from..to.
func numbers(from, to int) []int {
	var ns []int
	for n := from; n <= to; n++ {
		ns = append(ns, n)
	}
	return ns
}

// staleDrops returns the lockstep.drop records with reason stale-seq on node n, in order.
func staleDrops(recs []kernel.Record, n *kernel.Node) []kernel.Record {
	var out []kernel.Record
	for _, r := range ofKind(recs, "lockstep.drop") {
		if r.Node == n.ID() && attr(r, "reason") == "stale-seq" {
			out = append(out, r)
		}
	}
	return out
}

// delivered returns the seq attributes of the lockstep.deliver records on node n, in order.
func delivered(recs []kernel.Record, n *kernel.Node) []string {
	var seqs []string
	for _, r := range ofKind(recs, "lockstep.deliver") {
		if r.Node == n.ID() {
			seqs = append(seqs, attr(r, "seq"))
		}
	}
	return seqs
}

// sendAll returns an actor that sends one Num to each PID of every []PID it receives, in order.
func sendAll() *actor[[]lockstep.PID[Num]] {
	return &actor[[]lockstep.PID[Num]]{receive: func(ctx *lockstep.Context[[]lockstep.PID[Num]], to []lockstep.PID[Num]) {
		for i, pid := range to {
			if err := lockstep.Send(ctx, pid, Num{i}); err != nil {
				panic(err)
			}
		}
	}}
}

// SIM-160, ENG-080: each engine numbers its envelopes per destination node, so the sends of one
// sender to two nodes, interleaved, are numbered 1, 2, 3 at each of them.
func TestSIM160SeqPerDestination(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var sender lockstep.PID[[]lockstep.PID[Num]]
		s.Node("n1", func(sys *lockstep.System) { sender = spawn(sys, "s", sendAll()) })
		n2 := s.Node("n2", func(sys *lockstep.System) { spawn(sys, "r", &actor[Num]{}) })
		n3 := s.Node("n3", func(sys *lockstep.System) { spawn(sys, "r", &actor[Num]{}) })
		w.RunFor(time.Millisecond)
		r2, r3 := lockstep.PIDAt[Num]("n2", "/user/r"), lockstep.PIDAt[Num]("n3", "/user/r")
		if err := sim.Send(s, "n1", sender, []lockstep.PID[Num]{r2, r3, r2, r3, r2, r3}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Second)
		recs := w.Sim.Records()
		for _, n := range []*kernel.Node{n2, n3} {
			if got := delivered(recs, n); !slices.Equal(got, []string{"1", "2", "3"}) {
				tt.Errorf("lockstep.deliver seq on %s %v, want 1, 2, 3", n.Name(), got)
			}
		}
	})
}

// SIM-170: the stream key holds the sender's incarnation, so after the sender restarts, its new
// system's envelopes, numbered from 1 again, are delivered rather than dropped as stale.
func TestSIM170StreamPerIncarnation(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var sender lockstep.PID[[]lockstep.PID[Num]]
		n1 := s.Node("n1", func(sys *lockstep.System) { sender = spawn(sys, "s", sendAll()) })
		n2 := s.Node("n2", func(sys *lockstep.System) { spawn(sys, "r", &actor[Num]{}) })
		w.RunFor(time.Millisecond)
		r := lockstep.PIDAt[Num]("n2", "/user/r")
		for boot := 1; boot <= 2; boot++ {
			if boot == 2 {
				n1.Crash()
				n1.Restart()
				w.RunFor(time.Millisecond)
			}
			if err := sim.Send(s, "n1", sender, []lockstep.PID[Num]{r, r, r}); err != nil {
				tt.Fatalf("boot %d: Send: %v", boot, err)
			}
			w.RunFor(time.Second)
		}
		recs := w.Sim.Records()
		if got := delivered(recs, n2); !slices.Equal(got, []string{"1", "2", "3", "1", "2", "3"}) {
			tt.Errorf("lockstep.deliver seq on n2 %v, want 1, 2, 3 from each incarnation of n1", got)
		}
		if n := len(staleDrops(recs, n2)); n != 0 {
			tt.Errorf("%d stale-seq drops, want 0", n)
		}
	})
}

// AT-SIM-11, SIM-160: a send to a node that is not a lockstep node returns nil and is dropped
// with a lockstep.drop record, without a network send.
func TestATSIM11UnknownNode(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		to := lockstep.PIDAt[Deposit]("nowhere", "/user/x")
		var from string
		sent := false
		var sendErr error
		n1 := s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "a", &actor[Deposit]{started: func(ctx *lockstep.Context[Deposit]) {
				from, sent = ctx.Self().String(), true
				sendErr = lockstep.Send(ctx, to, Deposit{1})
			}})
		})
		s.Node("n2", noSetup)
		w.RunFor(time.Second)

		if !sent || sendErr != nil {
			tt.Fatalf("Send ran %t and returned %v, want nil", sent, sendErr)
		}
		recs := w.Sim.Records()
		var drops []kernel.Record
		for _, r := range ofKind(recs, "lockstep.drop") {
			if attr(r, "reason") == "unknown-node" {
				drops = append(drops, r)
			}
		}
		if len(drops) != 1 {
			tt.Fatalf("%d unknown-node drops, want 1", len(drops))
		}
		d := drops[0]
		if keys := attrKeys(d); !slices.Equal(keys, []string{"to", "reason", "from", "msg"}) {
			tt.Errorf("drop attributes %v, want to, reason, from, msg", keys)
		}
		if d.Node != n1.ID() || d.Text != "drop to "+to.String() || attr(d, "to") != to.String() ||
			attr(d, "from") != from || attr(d, "msg") == "" {
			tt.Errorf("drop on node %d %q to %q from %q msg %q, want node %d, to %s, from %s and a msg",
				d.Node, d.Text, attr(d, "to"), attr(d, "from"), attr(d, "msg"), n1.ID(), to, from)
		}
		if n := len(ofKind(recs, "net.send")); n != 0 {
			tt.Errorf("%d net.send records, want 0", n)
		}
	})
}

var _ = scenario("non-lockstep-payload", func(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		w := s.World()
		n1 := s.Node("n1", noSetup)
		n2 := s.Node("n2", noSetup)
		w.RunFor(time.Millisecond)
		w.Net.Send(n2, n1.ID(), "hello")
		w.RunFor(time.Second)
	})
})

// AT-SIM-12, SIM-170: a simnet payload that is not lockstep's fails the seed with check
// lockstep/misuse.
func TestATSIM12NonLockstepPayload(t *testing.T) {
	got := failingSeed(t, "non-lockstep-payload")
	want := failure{
		Kind: "invariant", Check: "lockstep/misuse", Signature: "invariant:lockstep/misuse",
		Message: `lockstep: node n1 received a non-lockstep payload "hello"`,
	}
	if got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
}
