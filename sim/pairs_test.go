// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// pidA is the sender A of the pair tests: /user/a on n1, taking bursts of numbered messages.
func pidA() lockstep.PID[burst] { return lockstep.PIDAt[burst]("n1", "/user/a") }

// pidB is the receiver B of the pair tests: /user/b on node.
func pidB(node string) lockstep.PID[Num] { return lockstep.PIDAt[Num](node, "/user/b") }

// burstTo returns an actor that, for every burst it receives, sends the numbers From to
// From+N-1 to to in one turn.
func burstTo(to lockstep.PID[Num]) *actor[burst] {
	return &actor[burst]{receive: func(ctx *lockstep.Context[burst], b burst) {
		for n := b.From; n < b.From+b.N; n++ {
			if err := lockstep.Send(ctx, to, Num{n}); err != nil {
				panic(err)
			}
		}
	}}
}

// burstVia is burstTo for a target that changes: it sends to *to, read at every send, so a test
// can address the PID of B's latest incarnation.
func burstVia(to *lockstep.PID[Num]) *actor[burst] {
	return &actor[burst]{receive: func(ctx *lockstep.Context[burst], b burst) {
		for n := b.From; n < b.From+b.N; n++ {
			if err := lockstep.Send(ctx, *to, Num{n}); err != nil {
				panic(err)
			}
		}
	}}
}

// arrival is one message B received, and when.
type arrival struct {
	n  int
	at kernel.Time
}

// ns returns the numbers of arrivals, in order.
func ns(as []arrival) []int {
	out := make([]int, len(as))
	for i, a := range as {
		out[i] = a.n
	}
	return out
}

// timedReceiver returns an actor that appends every Num it receives, with the virtual time of its
// turn, to *got.
func timedReceiver(w *faultline.World, got *[]arrival) *actor[Num] {
	return &actor[Num]{receive: func(_ *lockstep.Context[Num], m Num) {
		*got = append(*got, arrival{n: m.N, at: w.Sim.Now()})
	}}
}

// parseTime parses the text of a kernel.Time ("1.500000000s").
func parseTime(t testing.TB, s string) kernel.Time {
	t.Helper()
	digits := strings.ReplaceAll(strings.TrimSuffix(s, "s"), ".", "")
	v, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || !strings.HasSuffix(s, "s") {
		t.Fatalf("kernel.Time text %q does not parse", s)
	}
	return kernel.Time(v)
}

// recordsOn returns the records of kind on node id, in order.
func recordsOn(recs []kernel.Record, kind string, id kernel.NodeID) []kernel.Record {
	var out []kernel.Record
	for _, r := range ofKind(recs, kind) {
		if r.Node == id {
			out = append(out, r)
		}
	}
	return out
}

// withAttr returns the records of recs whose attribute key is value.
func withAttr(recs []kernel.Record, key, value string) []kernel.Record {
	var out []kernel.Record
	for _, r := range recs {
		if attr(r, key) == value {
			out = append(out, r)
		}
	}
	return out
}

// SFT-020, AT-SFT-02: a link fault without FIFO makes the link unordered while it lasts, and
// link-reset restores FIFO; sim re-applies nothing. B receives a strictly increasing subsequence,
// every message sent once the link is FIFO again arrives, and each missing one is a stale-seq drop.
func TestATSFT02UnorderedLinkWindow(t *testing.T) {
	staleSeeds := map[uint64]bool{} // read by key; a seed that runs again sets the same entry
	sim.SimulateSeeds(t, fullTrace(0), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []arrival
		n1 := s.Node("n1", func(sys *lockstep.System) { spawn(sys, "a", burstTo(pidB("n2"))) })
		n2 := s.Node("n2", func(sys *lockstep.System) { spawn(sys, "b", timedReceiver(w, &got)) })
		w.Plan(fault.Script(
			fault.Event{At: kernel.Time(time.Second), Kind: fault.KindLink, Node: "n1", Peer: "n2", Link: &simnet.Link{Jitter: 30 * time.Millisecond}},
			fault.Event{At: kernel.Time(2 * time.Second), Kind: fault.KindLinkReset, Node: "n1", Peer: "n2"},
		))
		w.RunFor(time.Millisecond)

		const total = 500 // one every 5ms, 200 of them inside the window
		sentAt := make([]kernel.Time, total+1)
		for n := 1; n <= total; n++ {
			now := w.Sim.Now()
			sentAt[n] = now
			wantFIFO := now < kernel.Time(time.Second) || now >= kernel.Time(2*time.Second)
			if fifo := w.Net.Link(n1.ID(), n2.ID()).FIFO; fifo != wantFIFO {
				tt.Fatalf("at %v the link n1->n2 has FIFO %v, want %v", now, fifo, wantFIFO)
			}
			if err := sim.Send(s, "n1", pidA(), burst{From: n, N: 1}); err != nil {
				tt.Fatalf("Send %d: %v", n, err)
			}
			w.RunFor(5 * time.Millisecond)
		}
		w.RunFor(time.Second)

		gotN := ns(got)
		for i := 1; i < len(gotN); i++ {
			if gotN[i] <= gotN[i-1] {
				tt.Fatalf("B got %v, want a strictly increasing subsequence", gotN)
			}
		}
		for n := 1; n <= total; n++ {
			if sentAt[n] >= kernel.Time(2*time.Second) && !slices.Contains(gotN, n) {
				tt.Errorf("message %d, sent at %v after the link was reset, did not arrive", n, sentAt[n])
			}
		}
		drops := staleDrops(w.Sim.Records(), n2)
		if len(drops)+len(got) != total {
			tt.Errorf("%d stale-seq drops and %d arrivals, want %d in all", len(drops), len(got), total)
		}
		if len(drops) > 0 {
			staleSeeds[w.Seed()] = true
		}
	}, 1, 2, 3, 4, 5)
	if len(staleSeeds) == 0 {
		t.Error("no seed of five recorded a stale-seq drop in the unordered window")
	}
}

// SFT-060, SFT-080, AT-SFT-07: DropBetween drops the messages that the pair's fresh drop stream
// says, one lockstep.drop reason=injected each, and B receives the rest in order.
func TestATSFT07DropBetween(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []arrival
		s.Node("n1", func(sys *lockstep.System) { spawn(sys, "a", burstTo(pidB("n2"))) })
		n2 := s.Node("n2", func(sys *lockstep.System) { spawn(sys, "b", timedReceiver(w, &got)) })
		s.DropBetween(pidA().Address(), pidB("n2").Address(), 500_000)
		w.RunFor(time.Millisecond)
		a := pidA()
		if err := sim.Send(s, "n1", a, burst{From: 1, N: 1000}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(5 * time.Second)

		fresh := kernel.New(w.Sim.Config()).Rand("lockstep/fault/drop/n1/user/a->n2/user/b")
		var wantKept []int
		var wantDropped []string
		for n := 1; n <= 1000; n++ {
			if kernel.Chance(fresh, 500_000) {
				wantDropped = append(wantDropped, kernel.Describe(Num{n}))
			} else {
				wantKept = append(wantKept, n)
			}
		}
		if len(wantDropped) == 0 || len(wantKept) == 0 {
			tt.Fatalf("%d dropped and %d kept in the fresh stream, want both non-zero", len(wantDropped), len(wantKept))
		}
		if !slices.Equal(ns(got), wantKept) {
			tt.Errorf("B got %d messages %v..., want the %d that the stream keeps, in order", len(got), ns(got)[:min(len(got), 10)], len(wantKept))
		}
		drops := withAttr(recordsOn(w.Sim.Records(), "lockstep.drop", n2.ID()), "reason", "injected")
		var msgs []string
		for _, d := range drops {
			msgs = append(msgs, attr(d, "msg"))
			if keys := attrKeys(d); !slices.Equal(keys, []string{"to", "reason", "from", "msg"}) {
				tt.Errorf("injected drop attributes %v, want to, reason, from, msg", keys)
			}
			// The record names the target as the sender named it: by path, without a uid.
			if b := pidB("n2").String(); d.Text != "drop to "+b || attr(d, "to") != b {
				tt.Errorf("injected drop %q to %q, want to %s", d.Text, attr(d, "to"), b)
			}
		}
		if !slices.Equal(msgs, wantDropped) {
			tt.Errorf("%d injected drops, want %d, one for each message the stream drops, in order", len(msgs), len(wantDropped))
		}
		if n := len(withAttr(ofKind(w.Sim.Records(), "lockstep.drop"), "reason", "stale-seq")); n != 0 {
			tt.Errorf("%d stale-seq drops, want 0", n)
		}
	})
}

// SFT-080, SFT-090, AT-SFT-08: a local pair under DelayBetween(0, 10ms): the 100 messages of one
// turn arrive in order, none before its release time, with one lockstep/delay event pending for
// the pair at a time: each event offers every message due, so the events are as many as the
// distinct release times.
func TestATSFT08DelayBetweenKeepsOrder(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []arrival
		s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "a", burstTo(pidB("n1")))
			spawn(sys, "b", timedReceiver(w, &got))
		})
		s.DelayBetween(pidA().Address(), pidB("n1").Address(), 0, 10*time.Millisecond)
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", pidA(), burst{From: 1, N: 100}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Second)

		if !slices.Equal(ns(got), numbers(1, 100)) {
			tt.Fatalf("B got %v, want 1..100 in order", ns(got))
		}
		delays := ofKind(w.Sim.Records(), "lockstep.delay")
		if len(delays) != 100 {
			tt.Fatalf("%d lockstep.delay records, want 100", len(delays))
		}
		var releases []kernel.Time
		for i, d := range delays {
			rel := parseTime(tt, attr(d, "release"))
			releases = append(releases, rel)
			if got[i].at < rel {
				tt.Errorf("message %d arrived at %v, before its release %v", i+1, got[i].at, rel)
			}
			if i > 0 && rel < releases[i-1] {
				tt.Errorf("release %v of message %d is before the previous release %v", rel, i+1, releases[i-1])
			}
			if d := parseDuration(tt, attr(d, "delay")); d < 0 || d > 10*time.Millisecond {
				tt.Errorf("message %d delay %v, want in [0s, 10ms]", i+1, d)
			}
		}
		distinct := len(slices.Compact(slices.Clone(releases)))
		events := 0
		for _, r := range ofKind(w.Sim.Records(), "kernel.event") {
			if r.Text == "lockstep/delay" {
				events++
			}
		}
		if events != distinct {
			tt.Errorf("%d lockstep/delay events for %d distinct release times, want one event per release time", events, distinct)
		}
	})
}

// parseDuration parses the text of a time.Duration.
func parseDuration(t testing.TB, s string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(s)
	if err != nil {
		t.Fatalf("duration text %q does not parse: %v", s, err)
	}
	return d
}

// SFT-060, SFT-080, SFT-090, AT-SFT-09: a rule whose parts were removed while a message is held
// still holds the messages sent behind it, in order and to the same release time. Once the queue
// is empty the rule is gone and a later message is offered at once.
func TestATSFT09RuleRemovedWhileHeld(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []arrival
		s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "a", burstTo(pidB("n1")))
			spawn(sys, "b", timedReceiver(w, &got))
		})
		from, to := pidA().Address(), pidB("n1").Address()
		w.RunFor(time.Millisecond)
		t0 := w.Sim.Now()
		s.DelayBetween(from, to, 5*time.Millisecond, 5*time.Millisecond)
		if err := sim.Send(s, "n1", pidA(), burst{From: 1, N: 1}); err != nil {
			tt.Fatalf("Send 1: %v", err)
		}
		w.RunFor(time.Millisecond)
		s.DelayBetween(from, to, 0, 0)
		if err := sim.Send(s, "n1", pidA(), burst{From: 2, N: 1}); err != nil {
			tt.Fatalf("Send 2: %v", err)
		}
		w.RunFor(20 * time.Millisecond)
		t3 := w.Sim.Now()
		if err := sim.Send(s, "n1", pidA(), burst{From: 3, N: 1}); err != nil {
			tt.Fatalf("Send 3: %v", err)
		}
		w.RunFor(20 * time.Millisecond)

		release := t0.Add(5 * time.Millisecond)
		want := []arrival{{1, release}, {2, release}, {3, t3}}
		if !slices.Equal(got, want) {
			tt.Errorf("B got %v, want %v: 1 and 2 together at the release time, 3 at once", got, want)
		}
		delays := ofKind(w.Sim.Records(), "lockstep.delay")
		if len(delays) != 2 {
			tt.Fatalf("%d lockstep.delay records, want 2 (messages 1 and 2)", len(delays))
		}
		for i, d := range delays {
			if parseTime(tt, attr(d, "release")) != release {
				tt.Errorf("delay record %d release %s, want %v", i+1, attr(d, "release"), release)
			}
		}
		if attr(delays[1], "delay") != "0s" {
			tt.Errorf("delay record 2 delay %s, want 0s", attr(delays[1], "delay"))
		}
	})
}

// SFT-090, AT-SFT-10: held messages of a node that crashes are discarded without a delivery, and
// messages sent to the node's new incarnation match the rule by location and are delayed. This
// holds whether the node restarts after the old release time or before it.
func TestATSFT10CrashDiscardsHeld(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart time.Duration // after the two messages were sent; the release is 50ms after them
	}{
		{"restart after the release time", 100 * time.Millisecond},
		{"restart before the release time", 20 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
				w, tt := s.World(), s.World().T()
				var got []arrival
				var b lockstep.PID[Num] // the PID of B's latest incarnation, which A sends to
				s.Node("n1", func(sys *lockstep.System) { spawn(sys, "a", burstVia(&b)) })
				n2 := s.Node("n2", func(sys *lockstep.System) { b = spawn(sys, "b", timedReceiver(w, &got)) })
				s.DelayBetween(pidA().Address(), pidB("n2").Address(), 50*time.Millisecond, 50*time.Millisecond)
				w.RunFor(time.Millisecond)
				if err := sim.Send(s, "n1", pidA(), burst{From: 1, N: 2}); err != nil {
					tt.Fatalf("Send: %v", err)
				}
				w.RunFor(10 * time.Millisecond)
				if n := len(ofKind(w.Sim.Records(), "lockstep.delay")); n != 2 {
					tt.Fatalf("%d lockstep.delay records before the crash, want 2", n)
				}
				oldB := b
				n2.Crash()
				w.RunFor(tc.restart - 10*time.Millisecond)
				n2.Restart()
				w.RunFor(5 * time.Millisecond)
				if b == oldB {
					tt.Fatalf("B's new incarnation has the old PID %s", b)
				}
				sentAt := w.Sim.Now()
				if err := sim.Send(s, "n1", pidA(), burst{From: 3, N: 1}); err != nil {
					tt.Fatalf("Send 3: %v", err)
				}
				w.RunFor(time.Second)

				want := []arrival{{3, sentAt.Add(50 * time.Millisecond)}}
				if len(got) != 1 || got[0].n != 3 || got[0].at < want[0].at {
					tt.Errorf("B got %v, want only message 3, no earlier than %v", got, want[0].at)
				}
				delays := ofKind(w.Sim.Records(), "lockstep.delay")
				if len(delays) != 3 || attr(delays[2], "msg") != kernel.Describe(Num{3}) {
					tt.Errorf("%d lockstep.delay records, want 3, the last one for message 3", len(delays))
				}
				if n := len(recordsOn(w.Sim.Records(), "lockstep.deliver", n2.ID())); n != 1 {
					tt.Errorf("%d lockstep.deliver records on n2, want 1 (message 3)", n)
				}
			})
		})
	}
}

// relay is the message of the actor that asks: Start sends one ask to its target, and the reply
// comes back as a relay with Reply set.
type relay struct {
	Start bool
	V     int
}

// SFT-070, AT-SFT-11: a reply is never matched by a pair rule, while the request is. A asks B and
// B answers at once.
func TestATSFT11RepliesNotMatched(t *testing.T) {
	const wait = 100 * time.Millisecond // far below the 1s delays
	a := lockstep.PIDAt[relay]("n1", "/user/a").Address()
	b := lockstep.PIDAt[Ping]("n2", "/user/b").Address()
	run := func(t *testing.T, from, to lockstep.Address, delayed bool) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var repliedAt kernel.Time
			var sentAt kernel.Time
			s.Node("n1", func(sys *lockstep.System) {
				spawn(sys, "a", &actor[relay]{receive: func(ctx *lockstep.Context[relay], m relay) {
					if m.Start {
						sentAt = w.Sim.Now()
						r := lockstep.Ask(ctx, lockstep.PIDAt[Ping]("n2", "/user/b"), buildPing(7))
						lockstep.Pipe(ctx, r, func(v int, err error) relay {
							if err != nil {
								panic(err)
							}
							return relay{V: v}
						})
						return
					}
					repliedAt = w.Sim.Now()
				}})
			})
			s.Node("n2", func(sys *lockstep.System) { spawn(sys, "b", echo()) })
			s.DelayBetween(from, to, time.Second, time.Second)
			w.RunFor(time.Millisecond)
			if err := sim.Send(s, "n1", lockstep.PIDAt[relay]("n1", "/user/a"), relay{Start: true}); err != nil {
				tt.Fatalf("Send: %v", err)
			}
			w.RunFor(3 * time.Second)

			if repliedAt == 0 {
				tt.Fatal("A never got the reply")
			}
			took := repliedAt.Sub(sentAt)
			delays := ofKind(w.Sim.Records(), "lockstep.delay")
			if delayed {
				if took < time.Second || took > time.Second+wait || len(delays) != 1 || attr(delays[0], "delay") != "1s" {
					tt.Errorf("the request was answered after %v with %d delay records (%v), want it delayed by 1s", took, len(delays), delays)
				}
			} else if took > wait || len(delays) != 0 {
				tt.Errorf("the reply came after %v with %d delay records, want it undelayed", took, len(delays))
			}
		})
	}
	t.Run("rule on the reply direction", func(t *testing.T) { run(t, b, a, false) })
	t.Run("rule on the request direction", func(t *testing.T) { run(t, a, b, true) })
}

// SFT-060, AT-SFT-12: misuse panics with the spec's messages.
func TestATSFT12InvalidArguments(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		tt := s.World().T()
		a, b := pidA().Address(), pidB("n2").Address()
		expectPanic(tt, "lockstep: sim: DelayBetween: invalid range [-1ns, 0s]", func() { s.DelayBetween(a, b, -1, 0) })
		expectPanic(tt, "lockstep: sim: DelayBetween: invalid range [2ms, 1ms]", func() { s.DelayBetween(a, b, 2*time.Millisecond, time.Millisecond) })
		expectPanic(tt, "lockstep: sim: DropBetween: ppm 1000001 > 1000000", func() { s.DropBetween(a, b, 1_000_001) })
		if n := len(ofKind(s.World().Sim.Records(), "lockstep.fault")); n != 0 {
			tt.Errorf("%d lockstep.fault records after the panicking calls, want 0", n)
		}
		s.DropBetween(a, b, 1_000_000) // the largest valid value does not panic
	})
}

// SFT-060, §8: every call records lockstep.fault on node 0 with op first and the arguments'
// addresses, also when it removes a part.
func TestSFT060FaultRecords(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("n1", noSetup)
		s.Node("n2", noSetup)
		a, b := pidA().Address(), pidB("n2").Address()
		s.DelayBetween(a, b, time.Millisecond, 3*time.Millisecond)
		s.DropBetween(a, b, 250_000)
		s.DelayBetween(a, b, 0, 0)
		s.DropBetween(a, b, 0)

		recs := ofKind(w.Sim.Records(), "lockstep.fault")
		type want struct {
			text string
			keys []string
			vals []string
		}
		for i, wnt := range []want{
			{"fault delay", []string{"op", "from", "to", "min", "max"}, []string{"delay", a.String(), b.String(), "1ms", "3ms"}},
			{"fault drop", []string{"op", "from", "to", "ppm"}, []string{"drop", a.String(), b.String(), "250000"}},
			{"fault delay", []string{"op", "from", "to", "min", "max"}, []string{"delay", a.String(), b.String(), "0s", "0s"}},
			{"fault drop", []string{"op", "from", "to", "ppm"}, []string{"drop", a.String(), b.String(), "0"}},
		} {
			if i >= len(recs) {
				tt.Fatalf("%d lockstep.fault records, want 4", len(recs))
			}
			r := recs[i]
			if r.Node != 0 || r.Text != wnt.text || !slices.Equal(attrKeys(r), wnt.keys) {
				tt.Errorf("record %d: node %d text %q attributes %v, want node 0 text %q attributes %v", i, r.Node, r.Text, attrKeys(r), wnt.text, wnt.keys)
				continue
			}
			for j, k := range wnt.keys {
				if attr(r, k) != wnt.vals[j] {
					tt.Errorf("record %d attribute %s = %q, want %q", i, k, attr(r, k), wnt.vals[j])
				}
			}
		}
	})
}

// SFT-080: a rule with both parts drops first: a message that survives the drop draw is held, and
// a dropped one is neither held nor drawn a delay for. Removing the drop part leaves the delay.
func TestSFT080DropBeforeDelay(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []arrival
		s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "a", burstTo(pidB("n1")))
			spawn(sys, "b", timedReceiver(w, &got))
		})
		from, to := pidA().Address(), pidB("n1").Address()
		s.DropBetween(from, to, 500_000)
		s.DelayBetween(from, to, 2*time.Millisecond, 4*time.Millisecond)
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", pidA(), burst{From: 1, N: 200}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Second)

		cfg := w.Sim.Config()
		drop := kernel.New(cfg).Rand("lockstep/fault/drop/n1/user/a->n1/user/b")
		delay := kernel.New(cfg).Rand("lockstep/fault/delay/n1/user/a->n1/user/b")
		var kept []int
		var ds []string
		for n := 1; n <= 200; n++ {
			if kernel.Chance(drop, 500_000) {
				continue
			}
			kept = append(kept, n)
			ds = append(ds, kernel.Uniform(delay, 2*time.Millisecond, 4*time.Millisecond).String())
		}
		if !slices.Equal(ns(got), kept) {
			tt.Errorf("B got %d messages, want the %d the drop stream keeps, in order", len(got), len(kept))
		}
		var gotDelays []string
		for _, d := range ofKind(w.Sim.Records(), "lockstep.delay") {
			gotDelays = append(gotDelays, attr(d, "delay"))
		}
		if !slices.Equal(gotDelays, ds) {
			tt.Errorf("delays %v, want one draw per kept message %v", gotDelays, ds)
		}
		if n := len(withAttr(ofKind(w.Sim.Records(), "lockstep.drop"), "reason", "injected")); n != 200-len(kept) {
			tt.Errorf("%d injected drops, want %d", n, 200-len(kept))
		}
	})
}

// SFT-060: a rule is keyed by the ordered pair of locations: the opposite direction, another
// sender path and another receiver path are not matched.
func TestSFT060RuleKey(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []arrival
		s.Node("n1", func(sys *lockstep.System) {
			spawn(sys, "a", burstTo(pidB("n1")))
			spawn(sys, "c", burstTo(pidB("n1")))
			spawn(sys, "b", timedReceiver(w, &got))
		})
		s.DropBetween(pidB("n1").Address(), pidA().Address(), 1_000_000)                               // the other direction
		s.DropBetween(lockstep.PIDAt[Num]("n1", "/user/c").Address(), pidB("n1").Address(), 1_000_000) // c is not a
		s.DelayBetween(pidA().Address(), lockstep.PIDAt[Num]("n1", "/user/z").Address(), time.Second, time.Second)
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", pidA(), burst{From: 1, N: 3}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(10 * time.Millisecond)
		if !slices.Equal(ns(got), []int{1, 2, 3}) {
			tt.Errorf("B got %v, want 1, 2, 3 undelayed", ns(got))
		}
		if n := len(ofKind(w.Sim.Records(), "lockstep.drop")) + len(ofKind(w.Sim.Records(), "lockstep.delay")); n != 0 {
			tt.Errorf("%d drop and delay records, want 0", n)
		}
	})
}

// SFT-090: a node that is paused when a release event runs gets a lockstep/delay node event that
// runs on resume, and the held message is offered then.
func TestSFT090PausedReceiver(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []arrival
		s.Node("n1", func(sys *lockstep.System) { spawn(sys, "a", burstTo(pidB("n2"))) })
		n2 := s.Node("n2", func(sys *lockstep.System) { spawn(sys, "b", timedReceiver(w, &got)) })
		s.DelayBetween(pidA().Address(), pidB("n2").Address(), 50*time.Millisecond, 50*time.Millisecond)
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", pidA(), burst{From: 1, N: 1}); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(10 * time.Millisecond)
		n2.Pause()
		w.RunFor(200 * time.Millisecond)
		if len(got) != 0 {
			tt.Fatalf("B got %v while its node was paused, want nothing", ns(got))
		}
		resumed := w.Sim.Now()
		n2.Resume()
		w.RunFor(10 * time.Millisecond)
		if len(got) != 1 || got[0].at < resumed {
			tt.Errorf("B got %v, want message 1 at or after the resume at %v", got, resumed)
		}
		found := false
		for _, r := range ofKind(w.Sim.Records(), "kernel.event") {
			if r.Text == "lockstep/delay" && r.Node == n2.ID() {
				found = true
			}
		}
		if !found {
			tt.Error("no lockstep/delay event on n2 after the resume")
		}
	})
}

// SFT-090: a node that is paused when the release event runs, and crashes before it resumes, loses
// the node event that would continue the release. The next message for the pair, sent to the new
// incarnation, is held behind nothing, gets its own release and arrives after its delay; the
// message held for the old incarnation is never offered.
func TestSFT090PausedThenCrashedReceiver(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []arrival
		var b lockstep.PID[Num]
		s.Node("n1", func(sys *lockstep.System) { spawn(sys, "a", burstVia(&b)) })
		n2 := s.Node("n2", func(sys *lockstep.System) { b = spawn(sys, "b", timedReceiver(w, &got)) })
		s.DelayBetween(pidA().Address(), pidB("n2").Address(), 50*time.Millisecond, 50*time.Millisecond)
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "n1", pidA(), burst{From: 1, N: 1}); err != nil {
			tt.Fatalf("Send 1: %v", err)
		}
		w.RunFor(10 * time.Millisecond)
		n2.Pause()
		w.RunFor(100 * time.Millisecond) // past the release time: the release is posted to the node
		n2.Crash()
		n2.Restart()
		w.RunFor(5 * time.Millisecond)
		sentAt := w.Sim.Now()
		if err := sim.Send(s, "n1", pidA(), burst{From: 2, N: 1}); err != nil {
			tt.Fatalf("Send 2: %v", err)
		}
		w.RunFor(time.Second)

		if len(got) != 1 || got[0].n != 2 || got[0].at < sentAt.Add(50*time.Millisecond) {
			tt.Errorf("B got %v, want only message 2, no earlier than %v", got, sentAt.Add(50*time.Millisecond))
		}
	})
}

// askCycleDelayed is the scenario of TestSIM250DelayedRequestKeepsWorldBusy: A on n1 asks B on n2
// at 10ms, B keeps the request and asks A, and B's request to A is held for 1s.
var _ = scenario("deadlock-delayed-request", func(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		p := &pairRun{}
		addPair(s, p)
		s.DelayBetween(lockstep.PIDAt[Wait]("n2", "/user/b").Address(), lockstep.PIDAt[Wait]("n1", "/user/a").Address(), time.Second, time.Second)
		start(s, p.pidA)
		s.World().RunFor(10 * time.Second)
	})
})

// SIM-250, SFT-090: a request held by DelayBetween keeps the world from being quiescent, so an ask
// cycle that exists only as asks pending is not reported before the release: both asks are
// pending at about 10ms, but the failure comes after B's request is released at 1s.
func TestSIM250DelayedRequestKeepsWorldBusy(t *testing.T) {
	a, _ := failingSeedArtifact(t, "deadlock-delayed-request")
	if f := a.Report.Failure; f.Check != sim.CheckDeadlock {
		t.Fatalf("failure check %q, want %q", f.Check, sim.CheckDeadlock)
	}
	if at := time.Duration(a.Report.Failure.AtNS); at < time.Second+10*time.Millisecond || at >= 5*time.Second {
		t.Errorf("deadlock reported at %v, want after the 1s release of B's request and before the ask timeouts", at)
	}
}
