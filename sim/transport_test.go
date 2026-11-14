// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-SIM-08, SIM-140, REM-310: a send from a paused node leaves the node once it resumes, from a
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

		// The send dials n2: the HELLO, written while n1 was paused, is the first datagram, and
		// leaves from a lockstep/send event once n1 resumes; the message follows the handshake.
		resumed := start.Add(2 * time.Second)
		recs := w.Sim.Records()
		i := slices.IndexFunc(recs, func(r kernel.Record) bool { return r.Kind == "net.send_raw" })
		if i < 0 {
			tt.Fatal("no net.send_raw record")
		}
		if recs[i].Node != n1.ID() || recs[i].At != resumed || !strings.Contains(attr(recs[i], "payload"), " HELLO ") {
			tt.Errorf("first net.send_raw %q on node %d at %v, want the HELLO on node %d at %v", recs[i].Text, recs[i].Node, recs[i].At, n1.ID(), resumed)
		}
		last := lastOfKind(recs[:i], "kernel.event")
		if last == nil || last.Text != "lockstep/send" || last.Node != n1.ID() || last.At != resumed {
			tt.Errorf("the last kernel.event before net.send_raw is %+v, want lockstep/send on n1 at %v", last, resumed)
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

// SIM-140, REM-310, ENG-080: the datagrams of a paused node leave in the order they were written,
// and one written after the node resumes, while earlier ones still wait for their lockstep/send
// events, queues behind them: every envelope arrives, in order, with no gap.
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
				if n := len(recordsOn(w.Sim.Records(), "lockstep.remote.gap", n2.ID())); n != 0 {
					tt.Errorf("%d lockstep.remote.gap records on n2, want 0", n)
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

// AT-SIM-10, REM-160, REM-320: on FIFO links the messages of one pair arrive in send order, each
// numbered with its position among the envelopes from the sender's incarnation. On a jittered link
// that would reorder, remoting still sends its datagrams in FIFO order, so the messages arrive in
// order, without a gap.
func TestATSIM10PerPairOrder(t *testing.T) {
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
		w.Net.SetLink(n1.ID(), n2.ID(), simnet.Link{Jitter: 50 * time.Millisecond})
		got = nil
		if err := sim.Send(s, "n1", sender, burst{From: 101, N: 100}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Second)
		if !slices.Equal(got, numbers(101, 200)) {
			tt.Errorf("jittered link: r got %v, want 101..200 in order", got)
		}
		for n := 101; n <= 200; n++ {
			want = append(want, strconv.Itoa(n))
		}
		if seqs := delivered(w.Sim.Records(), n2); !slices.Equal(seqs, want) {
			tt.Errorf("lockstep.deliver seq on n2 %v, want 1..200", seqs)
		}
		if n := len(ofKind(w.Sim.Records(), "lockstep.remote.gap")); n != 0 {
			tt.Errorf("%d lockstep.remote.gap records, want 0", n)
		}
	}, 1, 2, 3, 4, 5)
}

// numbers returns from..to.
func numbers(from, to int) []int {
	var ns []int
	for n := from; n <= to; n++ {
		ns = append(ns, n)
	}
	return ns
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

// REM-160, ENG-080: each node numbers the envelopes it receives per sending node, so the sends of
// one sender to two nodes, interleaved, are numbered 1, 2, 3 at each of them.
func TestREM160SeqPerDestination(t *testing.T) {
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

// REM-160, REM-294: the receiver numbers envelopes per incarnation of the sender, so after the
// sender restarts, its new system's envelopes are numbered from 1 again and delivered.
func TestREM160SeqPerIncarnation(t *testing.T) {
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
	})
}

// AT-SIM-11, REM-135: a send to a node the world does not have returns nil and is a dead letter
// with reason unknown-node, without a network send.
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
		var dead []kernel.Record
		for _, r := range ofKind(recs, "lockstep.deadletter") {
			if attr(r, "reason") == "unknown-node" {
				dead = append(dead, r)
			}
		}
		if len(dead) != 1 {
			tt.Fatalf("%d unknown-node dead letters, want 1", len(dead))
		}
		d := dead[0]
		if d.Node != n1.ID() || attr(d, "to") != to.String() || attr(d, "from") != from || attr(d, "msg") == "" {
			tt.Errorf("dead letter on node %d to %q from %q msg %q, want node %d, to %s, from %s and a msg",
				d.Node, attr(d, "to"), attr(d, "from"), attr(d, "msg"), n1.ID(), to, from)
		}
		if n := len(ofKind(recs, "net.send_raw")) + len(ofKind(recs, "net.send")); n != 0 {
			tt.Errorf("%d network sends, want 0", n)
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
