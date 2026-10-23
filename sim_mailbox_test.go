// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// The simulation acceptance tests of the mailbox, lifecycle and timer specs share the helpers
// below. A body runs again for a determinism check and for a failing seed's artifacts, so every
// test resets what it owns at the start of its body, and assertions on records need the full trace.

// simOpts returns sim.Options that run seeds seeds of 10 s of virtual time and keep every record.
func simOpts(seeds int) sim.Options {
	return sim.Options{Options: faultline.Options{
		Seeds:    seeds,
		Duration: 10 * time.Second,
		Trace:    kernel.TraceConfig{Level: kernel.TraceFull},
	}}
}

// simActor is an actor built from optional functions.
type simActor[M any] struct {
	started func(ctx *lockstep.Context[M])
	receive func(ctx *lockstep.Context[M], m M)
	stopped func(ctx *lockstep.Context[M], reason lockstep.StopReason)
}

func (a *simActor[M]) Started(ctx *lockstep.Context[M]) {
	if a.started != nil {
		a.started(ctx)
	}
}

func (a *simActor[M]) Receive(ctx *lockstep.Context[M], m M) {
	if a.receive != nil {
		a.receive(ctx, m)
	}
}

func (a *simActor[M]) Stopped(ctx *lockstep.Context[M], reason lockstep.StopReason) {
	if a.stopped != nil {
		a.stopped(ctx, reason)
	}
}

// simSpawn spawns a at /user/<name> (or under the actor of sp) and returns its PID. It panics on
// an error, which fails the seed: it runs in kernel callbacks, where a test must not stop the
// goroutine.
func simSpawn[M any](sp lockstep.Spawner, name string, a *simActor[M], opts ...lockstep.SpawnOption) lockstep.PID[M] {
	pid, err := lockstep.Spawn(sp, name, func() lockstep.Actor[M] { return a }, opts...)
	if err != nil {
		panic(err)
	}
	return pid
}

// simRecords returns the records of kind on node n, in order.
func simRecords(w *faultline.World, n *kernel.Node, kind string) []kernel.Record {
	var out []kernel.Record
	for _, r := range w.Sim.Records() {
		if r.Node == n.ID() && r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// simAttr returns the value of r's attribute key, or "".
func simAttr(r kernel.Record, key string) string {
	i := slices.IndexFunc(r.Attrs, func(a kernel.Attr) bool { return a.Key == key })
	if i < 0 {
		return ""
	}
	return r.Attrs[i].Value
}

// simExpectStream returns the stream of label as a kernel with the run's seed derives it. A
// separate kernel keeps the test from consuming the run's own stream (KRN-010).
func simExpectStream(w *faultline.World, label string) *simStream {
	return &simStream{k: kernel.New(kernel.Config{Seed: w.Seed()}), label: label}
}

// simStream draws the values of one expected stream.
type simStream struct {
	k     *kernel.Sim
	label string
}

// next returns the next Uint64 of the stream.
func (s *simStream) next() uint64 { return s.k.Rand(s.label).Uint64() }

// mailboxFullText is the text of an error that wraps ErrMailboxFull for addr (MBX-090).
func mailboxFullText(addr string) string { return "lockstep: mailbox full: " + addr }

// checkRefused fails tt unless err is the mailbox-full refusal of addr, with the text of MBX-090.
func checkRefused(tt testing.TB, what string, err error, addr string) {
	tt.Helper()
	if !errors.Is(err, lockstep.ErrMailboxFull) {
		tt.Errorf("%s returned %v, want an error wrapping ErrMailboxFull", what, err)
		return
	}
	if got, want := err.Error(), mailboxFullText(addr); got != want {
		tt.Errorf("%s error text %q, want %q", what, got, want)
	}
}

// checkRejectedRecords fails tt unless the mailbox.full records of node n are exactly want
// refusals of to under BlockSender: action rejected, policy block-sender, never blocked.
func checkRejectedRecords(tt testing.TB, w *faultline.World, n *kernel.Node, to string, want int) {
	tt.Helper()
	recs := simRecords(w, n, "lockstep.mailbox.full")
	if len(recs) != want {
		tt.Errorf("%d lockstep.mailbox.full records, want %d", len(recs), want)
	}
	for _, r := range recs {
		if simAttr(r, "to") != to || simAttr(r, "action") != "rejected" || simAttr(r, "policy") != "block-sender" {
			tt.Errorf("record %v, want to=%s action=rejected policy=block-sender", r.Attrs, to)
		}
	}
}

// AT-MBX-13, MBX-090: under BlockSender a full mailbox refuses a send at once on the simulation
// engine, for the system and for an actor as the sender, with one record per refusal.
func TestATMBX13BlockSenderInSimulation(t *testing.T) {
	t.Run("system sender", func(t *testing.T) {
		var first, second error
		var tAddr string
		sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
			first, second, tAddr = nil, nil, ""
			n1 := s.Node("n1", func(sys *lockstep.System) {
				tp := simSpawn(sys, "t", &simActor[int]{}, lockstep.WithMailbox(1, lockstep.BlockSender))
				tAddr = tp.Address().String()
				first = lockstep.Send(sys, tp, 1)
				second = lockstep.Send(sys, tp, 2)
			})
			s.World().RunFor(100 * time.Millisecond)

			tt := s.World().T()
			if first != nil {
				tt.Errorf("first Send returned %v, want nil", first)
			}
			checkRefused(tt, "second Send", second, tAddr)
			checkRejectedRecords(tt, s.World(), n1, tAddr, 1)
		})
	})

	t.Run("actor sender", func(t *testing.T) {
		var first, second error
		var tAddr string
		sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
			first, second, tAddr = nil, nil, ""
			n1 := s.Node("n1", func(sys *lockstep.System) {
				tp := simSpawn(sys, "t", &simActor[int]{}, lockstep.WithMailbox(1, lockstep.BlockSender))
				tAddr = tp.Address().String()
				// Both sends are in one turn of the sender, so t cannot take a message between them.
				sp := simSpawn(sys, "s", &simActor[struct{}]{receive: func(ctx *lockstep.Context[struct{}], _ struct{}) {
					first = lockstep.Send(ctx, tp, 1)
					second = lockstep.Send(ctx, tp, 2)
				}})
				if err := lockstep.Send(sys, sp, struct{}{}); err != nil {
					panic(err)
				}
			})
			s.World().RunFor(100 * time.Millisecond)

			tt := s.World().T()
			if first != nil {
				tt.Errorf("first Send returned %v, want nil", first)
			}
			checkRefused(tt, "second Send", second, tAddr)
			checkRejectedRecords(tt, s.World(), n1, tAddr, 1)
		})
	})
}

// AT-SIM-34, SIM-220: the engine's Blocking is false, and a second Send and a SendContext into a
// full BlockSender mailbox return at once, with no virtual time passing.
func TestATSIM34NoWaitingForRoom(t *testing.T) {
	var blocking bool
	var first, second, third error
	var before, after, beforeNode, afterNode time.Time
	var tAddr string
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		blocking, first, second, third, tAddr = true, nil, nil, nil, ""
		var n1 *kernel.Node
		n1 = s.Node("n1", func(sys *lockstep.System) {
			tp := simSpawn(sys, "t", &simActor[int]{}, lockstep.WithMailbox(1, lockstep.BlockSender))
			tAddr = tp.Address().String()
			blocking = lockstep.EngineBlocking(sys)
			before, beforeNode = kernel.Epoch.Add(time.Duration(s.World().Sim.Now())), n1.Now()
			first = lockstep.Send(sys, tp, 1)
			second = lockstep.Send(sys, tp, 2)
			third = lockstep.SendContext(context.Background(), sys, tp, 3)
			after, afterNode = kernel.Epoch.Add(time.Duration(s.World().Sim.Now())), n1.Now()
		})
		s.World().RunFor(100 * time.Millisecond)

		tt := s.World().T()
		if blocking {
			tt.Errorf("the engine's Blocking is true, want false")
		}
		if first != nil {
			tt.Errorf("first Send returned %v, want nil", first)
		}
		checkRefused(tt, "second Send", second, tAddr)
		checkRefused(tt, "SendContext", third, tAddr)
		if !after.Equal(before) || !afterNode.Equal(beforeNode) {
			tt.Errorf("virtual time moved from %v to %v (node clock %v to %v) during the sends", before, after, beforeNode, afterNode)
		}
		checkRejectedRecords(tt, s.World(), n1, tAddr, 2)
	})
}

// fifoGo tells a sender to send its numbered messages.
type fifoGo struct{}

// fifoMsg is a numbered message of one sender.
type fifoMsg struct{ from, n int }

// AT-MBX-14, MBX-095: two actors that each send 100 numbered messages to one receiver in one turn
// keep their own order in every seed, and the senders interleave differently across seeds.
func TestATMBX14LocalPairFIFO(t *testing.T) {
	orders := make(map[uint64]string) // seed -> sender of each observed message, in order
	sim.Simulate(t, simOpts(50), func(s *sim.Sim) {
		var got []fifoMsg
		s.Node("n1", func(sys *lockstep.System) {
			r := simSpawn(sys, "r", &simActor[fifoMsg]{receive: func(_ *lockstep.Context[fifoMsg], m fifoMsg) {
				got = append(got, m)
			}})
			for i := 1; i <= 2; i++ {
				sp := simSpawn(sys, "s"+strconv.Itoa(i), &simActor[fifoGo]{receive: func(ctx *lockstep.Context[fifoGo], _ fifoGo) {
					for n := 1; n <= 100; n++ {
						if err := lockstep.Send(ctx, r, fifoMsg{from: i, n: n}); err != nil {
							panic(err)
						}
					}
				}})
				if err := lockstep.Send(sys, sp, fifoGo{}); err != nil {
					panic(err)
				}
			}
		})
		s.World().RunFor(time.Second)

		tt := s.World().T()
		next := map[int]int{1: 1, 2: 1}
		var order strings.Builder
		for _, m := range got {
			if m.n != next[m.from] {
				tt.Errorf("sender %d: observed %d, want %d", m.from, m.n, next[m.from])
				return
			}
			next[m.from]++
			order.WriteString(strconv.Itoa(m.from))
		}
		if len(got) != 200 {
			tt.Errorf("observed %d messages, want 200", len(got))
		}
		orders[s.World().Seed()] = order.String()
	})
	distinct := make(map[string]bool)
	for _, o := range orders {
		distinct[o] = true
	}
	if len(orders) != 50 || len(distinct) < 2 {
		t.Errorf("%d seeds observed, %d distinct interleavings; want 50 seeds and at least 2 interleavings", len(orders), len(distinct))
	}
}

// seqGo tells a sender to send count numbered messages, from from on, to the receiver on n2.
type seqGo struct{ from, count int }

// crossNode adds the nodes of AT-MBX-15: n1 runs a sender at /user/s that sends, on every seqGo,
// numbered messages to the receiver at /user/r on n2. n2 runs setupR, which spawns r. It returns
// the nodes and the sender's PID.
func crossNode(s *sim.Sim, setupR func(sys *lockstep.System), sendErrs *[]error) (n1, n2 *kernel.Node, sender lockstep.PID[seqGo]) {
	rp := lockstep.PIDAt[int]("n2", "/user/r")
	n1 = s.Node("n1", func(sys *lockstep.System) {
		simSpawn(sys, "s", &simActor[seqGo]{receive: func(ctx *lockstep.Context[seqGo], m seqGo) {
			for i := range m.count {
				if err := lockstep.Send(ctx, rp, m.from+i); err != nil {
					*sendErrs = append(*sendErrs, err)
				}
			}
		}})
	})
	n2 = s.Node("n2", setupR)
	return n1, n2, lockstep.PIDAt[seqGo]("n1", "/user/s")
}

// AT-MBX-15, MBX-115: a sender on n1 keeps its order to a receiver on n2, with FIFO links in
// every seed; on a link with jitter and duplicates, the receiver sees a strictly increasing
// subsequence and no repeated delivery.
func TestATMBX15CrossNodePairFIFO(t *testing.T) {
	t.Run("fifo links", func(t *testing.T) {
		sim.Simulate(t, simOpts(20), func(s *sim.Sim) {
			var got []int
			var sendErrs []error
			_, _, sp := crossNode(s, func(sys *lockstep.System) {
				simSpawn(sys, "r", &simActor[int]{receive: func(_ *lockstep.Context[int], m int) { got = append(got, m) }})
			}, &sendErrs)
			w := s.World()
			w.RunFor(10 * time.Millisecond)
			if err := sim.Send(s, "n1", sp, seqGo{from: 1, count: 200}); err != nil {
				w.T().Fatal(err)
			}
			w.RunFor(100 * time.Millisecond)
			if err := sim.Send(s, "n1", sp, seqGo{from: 201, count: 200}); err != nil { // a later turn
				w.T().Fatal(err)
			}
			w.RunFor(time.Second)

			tt := w.T()
			if len(sendErrs) != 0 {
				tt.Errorf("Send returned %v, want nil every time", sendErrs)
			}
			want := make([]int, 400)
			for i := range want {
				want[i] = i + 1
			}
			if !slices.Equal(got, want) {
				tt.Errorf("receiver observed %d numbers %v, want 1..400 in order", len(got), got)
			}
		})
	})

	t.Run("jitter and duplicates", func(t *testing.T) {
		stale := make(map[uint64]int) // seed -> stale-seq drops of late or duplicate envelopes
		sim.Simulate(t, simOpts(20), func(s *sim.Sim) {
			var got []int
			var sendErrs []error
			n1, n2, sp := crossNode(s, func(sys *lockstep.System) {
				simSpawn(sys, "r", &simActor[int]{receive: func(_ *lockstep.Context[int], m int) { got = append(got, m) }})
			}, &sendErrs)
			w := s.World()
			w.Net.SetLink(n1.ID(), n2.ID(), simnet.Link{Jitter: 20 * time.Millisecond, FIFO: false, DupPPM: 500000})
			w.RunFor(10 * time.Millisecond)
			for _, from := range []int{1, 201} {
				if err := sim.Send(s, "n1", sp, seqGo{from: from, count: 200}); err != nil {
					w.T().Fatal(err)
				}
				w.RunFor(500 * time.Millisecond)
			}

			tt := w.T()
			if len(sendErrs) != 0 {
				tt.Errorf("Send returned %v, want nil every time", sendErrs)
			}
			if len(got) == 0 {
				tt.Errorf("receiver observed no number")
			}
			for i := 1; i < len(got); i++ {
				if got[i] <= got[i-1] {
					tt.Errorf("observed numbers not strictly increasing at %d: %d after %d", i, got[i], got[i-1])
					break
				}
			}
			seen := make(map[string]bool)
			delivers := simRecords(w, n2, "lockstep.deliver")
			for _, r := range delivers {
				if m := simAttr(r, "msg"); seen[m] {
					tt.Errorf("lockstep.deliver repeats message %s", m)
				} else {
					seen[m] = true
				}
			}
			if len(delivers) != len(got) {
				tt.Errorf("%d lockstep.deliver records for %d observed numbers", len(delivers), len(got))
			}
			n := 0
			for _, r := range simRecords(w, n2, "lockstep.drop") {
				if simAttr(r, "reason") == "stale-seq" {
					n++
				}
			}
			stale[w.Seed()] = n
		})
		// The scenario must exercise what it tests: the transport dropped late and duplicate copies.
		total := 0
		for _, n := range stale {
			total += n
		}
		if total == 0 {
			t.Errorf("no seed dropped a late or duplicate copy; the link does not reorder or duplicate")
		}
	})
}

// AT-MBX-18, MBX-120: inbound envelopes meet a full mailbox on n2 as local sends do: the first
// two are delivered, the rest are dropped (DropNewest) or rejected (BlockSender), never blocked.
func TestATMBX18InboundFullMailbox(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy lockstep.MailboxPolicy
		want   []string
	}{
		{"drop-newest", lockstep.DropNewest, []string{
			"deliver seq=1", "deliver seq=2",
			"mailbox.full dropped-newest", "mailbox.full dropped-newest", "mailbox.full dropped-newest",
		}},
		{"block-sender", lockstep.BlockSender, []string{
			"deliver seq=1", "deliver seq=2",
			"mailbox.full rejected", "mailbox.full rejected", "mailbox.full rejected",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim.Simulate(t, simOpts(10), func(s *sim.Sim) {
				// The sender names r by path, so the records name its address without a uid.
				rAddr := lockstep.PIDAt[int]("n2", "/user/r").Address().String()
				var sendErrs []error
				_, n2, sp := crossNode(s, func(sys *lockstep.System) {
					rp := simSpawn(sys, "r", &simActor[int]{}, lockstep.WithMailbox(2, tc.policy))
					_ = lockstep.Hold(sys, rp.Address()) // r never takes a message
				}, &sendErrs)
				w := s.World()
				w.RunFor(10 * time.Millisecond)
				if err := sim.Send(s, "n1", sp, seqGo{from: 1, count: 5}); err != nil {
					w.T().Fatal(err)
				}
				w.RunFor(time.Second)

				tt := w.T()
				if len(sendErrs) != 0 {
					tt.Errorf("Send returned %v, want nil every time", sendErrs)
				}
				var got []string
				for _, r := range w.Sim.Records() {
					if r.Node != n2.ID() || (r.Kind != "lockstep.deliver" && r.Kind != "lockstep.mailbox.full") {
						continue
					}
					if to := simAttr(r, "to"); to != rAddr {
						tt.Errorf("%s record names %s, want %s", r.Kind, to, rAddr)
					}
					if r.Kind == "lockstep.deliver" {
						got = append(got, "deliver seq="+simAttr(r, "seq"))
					} else {
						got = append(got, "mailbox.full "+simAttr(r, "action"))
					}
				}
				if !slices.Equal(got, tc.want) {
					tt.Errorf("records on n2 %q, want %q", got, tc.want)
				}
			})
		})
	}
}

// AT-MBX-20 (simulation), MBX-150: a send from n1 to a PID on n2 whose actor has stopped and a
// send to a path on n2 with no actor both return nil; the dead letters are recorded on n2, on
// arrival, with reasons stopped and not-found.
func TestATMBX20DeadLettersInSimulation(t *testing.T) {
	sim.Simulate(t, simOpts(5), func(s *sim.Sim) {
		var stopped lockstep.PID[int]
		n1 := s.Node("n1", func(*lockstep.System) {})
		n2 := s.Node("n2", func(sys *lockstep.System) {
			stopped = simSpawn(sys, "a", &simActor[int]{})
			if err := lockstep.Stop(sys, stopped.Address()); err != nil {
				panic(err)
			}
		})
		w := s.World()
		w.RunFor(100 * time.Millisecond)
		if got := len(simRecords(w, n2, "lockstep.stop")); got != 1 {
			w.T().Fatalf("%d lockstep.stop records on n2 before the sends, want 1", got)
		}
		none := lockstep.PIDAt[int]("n2", "/user/none")
		errStopped := sim.Send(s, "n1", stopped, 1)
		errNone := sim.Send(s, "n1", none, 2)
		w.RunFor(time.Second)

		tt := w.T()
		if errStopped != nil || errNone != nil {
			tt.Errorf("Send returned %v and %v, want nil", errStopped, errNone)
		}
		if got := simRecords(w, n1, "lockstep.deadletter"); len(got) != 0 {
			tt.Errorf("%d dead letters on n1, want them recorded on n2 only", len(got))
		}
		var got []string
		for _, r := range simRecords(w, n2, "lockstep.deadletter") {
			got = append(got, simAttr(r, "reason")+" "+simAttr(r, "to"))
		}
		want := []string{"stopped " + stopped.Address().String(), "not-found " + none.Address().String()}
		if !slices.Equal(got, want) {
			tt.Errorf("dead letters on n2 %q, want %q", got, want)
		}
	})
}
