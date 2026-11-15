// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-CLU-05, CLU-200, CLU-210, CLU-220, CLU-250, §6 draw 1. "draw": a probe list is the up
// members other than self in name order, shuffled with r.Shuffle, and no draw is made without
// one. "rounds": five nodes that seed each other probe once per ProbeInterval, each round of four
// probes a permutation of the other four, every probe acked directly with the probe's seq, and
// the seqs count from 1. "answer": a ping from a node that never joined is answered with an ack
// of its seq whatever its TargetID or its state in the table, the ack carries the forced update
// about the pinger first, and a From with a new ID supersedes the entry. "hearsay": n1 has x with a
// stale ID; news of x's current ID from another node changes nothing, but n1's next probe of x
// reaches the current process, whose ack supersedes the entry; that ack does not ack the probe of
// the stale ID, which fails without a suspicion and without an indirect probe; later hearsay of
// another ID changes nothing either. "retired": n1 has x with ID A, its process with ID B retired;
// B's acks to the pings by name ack nothing, so A is suspected. "restart": n1 to n4 converge
// (seeds: n1 {n1, n3}, n2 {n1, n4}, n3 and n4 {n3}); a partition splits {n1, n2} from {n3, n4},
// which crash and restart with new IDs while n1 and n2 are told the old IDs are Dead; after the
// heal, frames that n1 or n2 queued before the restart carry the old IDs as Alive hearsay to n3 or
// n4, which know only the new IDs: that hearsay supersedes nothing, and every view ends with the
// four Alive at their current IDs. "regressions" runs two seeds where it once did.
func TestATCLU05ProbeOrder(t *testing.T) {
	t.Run("draw", func(t *testing.T) {
		members := []Member{
			alive("a", 1), {Node: "b", ID: 2, State: Suspect}, alive("c", 3), {Node: "d", ID: 4, State: Dead},
			alive("e", 5), {Node: "f", ID: 6, State: Left}, alive("self", 7), {Node: "g", ID: 8, State: Suspect},
		}
		up := []string{"a", "b", "c", "e", "g"}
		got, want := rand.New(rand.NewPCG(3, 4)), rand.New(rand.NewPCG(3, 4)) //nolint:gosec // G404: seeded on purpose, to compare the draws
		orders := map[string]bool{}                                           // read by key
		for i := range 50 {
			list := probeOrder(got, members, "self")
			w := slices.Clone(up)
			want.Shuffle(len(w), func(i, j int) { w[i], w[j] = w[j], w[i] })
			if !slices.Equal(list, w) {
				t.Fatalf("list %d: probeOrder = %v, want %v", i, list, w)
			}
			orders[strings.Join(list, ",")] = true
		}
		if len(orders) < 2 {
			t.Errorf("50 probe lists had %d orders, want the shuffle to vary them", len(orders))
		}
		for _, ms := range [][]Member{nil, {alive("self", 7)}, {members[3], members[5], alive("self", 7)}} {
			if list := probeOrder(got, ms, "self"); len(list) != 0 {
				t.Errorf("probeOrder(%v) = %v, want none", ms, list)
			}
		}
		if got.Uint64() != want.Uint64() {
			t.Error("probeOrder drew a value without a member to probe")
		}
	})

	t.Run("rounds", func(t *testing.T) {
		names := []string{"n1", "n2", "n3", "n4", "n5"}
		sim.Simulate(t, simOpts(5, 14*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			for _, n := range names {
				c.node(n, simConfig(names...))
			}
			w.RunFor(13500 * time.Millisecond)

			orders := map[string]bool{} // read by key
			for _, n := range names {
				start := c.recs(n, "start")[0].At
				probes := slices.DeleteFunc(c.recs(n, "probe"), func(r kernel.Record) bool { return r.At > vt(13*time.Second) })
				if want := int(vt(13*time.Second).Sub(start) / time.Second); len(probes) != want || want < 12 {
					tt.Fatalf("%s made %d probes by 13s, want %d, one per second from its start at %s: %s", n, len(probes), want, start, describe(probes))
				}
				acks := bySeq(c.recs(n, "ack"))
				others := slices.DeleteFunc(slices.Clone(names), func(o string) bool { return o == n })
				for i, p := range probes {
					seq := strconv.Itoa(i + 1)
					if p.At != start.Add(time.Duration(i+1)*time.Second) || attr(p, "seq") != seq {
						tt.Errorf("%s's probe %d: %s, want seq %s at %s", n, i, describe([]kernel.Record{p}), seq, start.Add(time.Duration(i+1)*time.Second))
					}
					a := acks[seq]
					if len(a) != 1 || attrs(a[0]) != "target="+attr(p, "target")+" seq="+seq+" via=direct" || a[0].At.Sub(p.At) >= DefaultProbeTimeout {
						tt.Errorf("%s's acks of probe %s: %s, want one direct ack within ProbeTimeout", n, describe([]kernel.Record{p}), describe(a))
					}
				}
				for r := 0; r+len(others) <= len(probes); r += len(others) {
					var round []string
					for _, p := range probes[r : r+len(others)] {
						round = append(round, attr(p, "target"))
					}
					orders[strings.Join(round, ",")] = true
					if sorted := slices.Sorted(slices.Values(round)); !slices.Equal(sorted, others) {
						tt.Errorf("%s's round %d probed %v, want each of %v once", n, r/len(others)+1, round, others)
					}
				}
				for _, kind := range []string{"probe.indirect", "probe.fail", "refute"} {
					if recs := c.recs(n, kind); len(recs) != 0 {
						tt.Errorf("%s recorded %s without faults: %s", n, kind, describe(recs))
					}
				}
			}
			if len(orders) < 2 {
				tt.Errorf("every round of every node probed in the order %v, want the shuffles to differ", orders)
			}
		})
	})

	t.Run("answer", func(t *testing.T) {
		sim.Simulate(t, simOpts(2, 2*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			c.node("n1", Config{})
			x := c.fake("x", false)
			w.RunFor(10 * time.Millisecond)
			n1 := lockstep.PIDAt[memberMsg]("n1", actorPath)
			self := c.view("n1").Self
			idA := c.id("x")
			idB := idA + 1
			// pingFrom sends n1 a ping of seq from x with From from, runs 50ms and returns the
			// messages x received meanwhile.
			pingFrom := func(from update, seq uint64) []fakeMsg {
				tt.Helper()
				seen := len(x.got)
				if err := sim.Send(s, "x", n1, memberMsg(ping{From: from, Seq: seq, Target: "n1", TargetID: self.ID + 1})); err != nil {
					tt.Fatal(err)
				}
				w.RunFor(50 * time.Millisecond)
				return x.got[seen:]
			}

			fromA := update{Node: "x", ID: idA, State: Alive}
			got := pingFrom(fromA, 77)
			if want := (ack{From: toUpdate(self), Seq: 77, Target: "n1", Updates: []update{fromA}}); len(got) != 1 || !equalAck(got[0].msg, want) {
				tt.Errorf("x received %v, want %+v", got, want)
			}
			if m, ok := c.view("n1").Member("x"); !ok || m != fromA.member() {
				tt.Errorf("n1's view has x %v, %t, want %v", m, ok, fromA.member())
			}

			// n1 believes x Dead at incarnation 9: x's next ping is answered all the same, with
			// that belief first, and changes nothing.
			deadA := update{Node: "x", ID: idA, Inc: 9, State: Dead}
			if err := sim.Send(s, "n1", n1, memberMsg(syncResp{From: toUpdate(self), Members: []update{deadA}})); err != nil {
				tt.Fatal(err)
			}
			w.RunFor(10 * time.Millisecond)
			got = pingFrom(fromA, 78)
			if len(got) != 1 {
				tt.Fatalf("x received %v, want one ack", got)
			}
			if a, ok := got[0].msg.(ack); !ok || a.Seq != 78 || a.Target != "n1" || len(a.Updates) == 0 || a.Updates[0] != deadA {
				tt.Errorf("x received %+v, want an ack of seq 78 whose first update is %+v", got[0].msg, deadA)
			}
			if m, _ := c.view("n1").Member("x"); m != deadA.member() {
				tt.Errorf("n1's view has x %v, want %v", m, deadA.member())
			}

			// A From with another ID supersedes the entry.
			fromB := update{Node: "x", ID: idB, State: Alive}
			if got = pingFrom(fromB, 79); len(got) != 1 || !equalAckIgnoringUpdates(got[0].msg, ack{From: toUpdate(self), Seq: 79, Target: "n1"}) {
				tt.Errorf("x received %v, want an ack of seq 79", got)
			}
			if m, _ := c.view("n1").Member("x"); m != fromB.member() {
				tt.Errorf("n1's view has x %v, want %v", m, fromB.member())
			}
			members := c.recs("n1", "member")
			if last := members[len(members)-1]; attrs(last) != "node=x id="+uitoa(idB)+" inc=0 from=dead to=alive source=ping" {
				tt.Errorf("n1's last member record %s, want the supersede", describe([]kernel.Record{last}))
			}
		})
	})

	// staleSetup adds n1 and x, an answering fake member, runs 10ms and returns x and a function
	// that sends n1 a local syncResp with from and members, then runs 10ms.
	staleSetup := func(s *sim.Sim, c *simCluster) (*fakeMember, func(from update, members ...update)) {
		w, tt := s.World(), s.World().T()
		c.node("n1", Config{})
		x := c.fake("x", false)
		w.RunFor(10 * time.Millisecond)
		n1 := lockstep.PIDAt[memberMsg]("n1", actorPath)
		return x, func(from update, members ...update) {
			tt.Helper()
			if err := sim.Send(s, "n1", n1, memberMsg(syncResp{From: from, Members: members})); err != nil {
				tt.Fatal(err)
			}
			w.RunFor(10 * time.Millisecond)
		}
	}
	// memberLines returns n1's member records as "<whole seconds since start> <attrs>".
	memberLines := func(c *simCluster, start kernel.Time) []string {
		var out []string
		for _, r := range c.recs("n1", "member") {
			out = append(out, r.At.Sub(start).Truncate(time.Second).String()+" "+attrs(r))
		}
		return out
	}

	t.Run("hearsay", func(t *testing.T) {
		sim.Simulate(t, simOpts(3, 3*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			x, tell := staleSetup(s, c)
			start := c.recs("n1", "start")[0].At
			self := toUpdate(c.view("n1").Self)
			idB := x.self.ID
			idA := idB + 1
			tell(update{Node: "x", ID: idA, State: Alive}) // direct: the From of a former process of x
			tell(self, update{Node: "x", ID: idB, State: Alive})
			if m, _ := c.view("n1").Member("x"); m.ID != idA {
				tt.Fatalf("n1's view has x %v after hearsay of ID %d, want ID %d still", m, idB, idA)
			}
			w.RunFor(start.Add(2100 * time.Millisecond).Sub(w.Sim.Now()))
			tell(self, update{Node: "x", ID: idB + 7, Inc: 9, State: Alive})
			w.RunFor(start.Add(2500 * time.Millisecond).Sub(w.Sim.Now()))

			want := []string{
				"0s node=x id=" + uitoa(idA) + " inc=0 from=none to=alive source=syncresp",
				"1s node=x id=" + uitoa(idB) + " inc=0 from=alive to=alive source=ack",
			}
			if got := memberLines(c, start); !slices.Equal(got, want) {
				tt.Errorf("n1's member records %q, want %q", got, want)
			}
			if m, _ := c.view("n1").Member("x"); m != (Member{Node: "x", ID: idB, State: Alive}) {
				tt.Errorf("n1's view has x %v, want ID %d Alive", m, idB)
			}
			fails, acks := c.recs("n1", "probe.fail"), c.recs("n1", "ack")
			if len(fails) != 1 || attrs(fails[0]) != "target=x seq=1" || fails[0].At != start.Add(2*time.Second) {
				tt.Errorf("n1's failed probes %s, want the probe of the stale ID, at 2s", describe(fails))
			}
			if len(acks) != 1 || attrs(acks[0]) != "target=x seq=2 via=direct" {
				tt.Errorf("n1's acks %s, want only that of the probe of 2s", describe(acks))
			}
			if in := c.recs("n1", "probe.indirect"); len(in) != 0 {
				tt.Errorf("n1 probed indirectly %s, want no indirect probe of a changed ID", describe(in))
			}
		})
	})

	t.Run("retired", func(t *testing.T) {
		sim.Simulate(t, simOpts(3, 3*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			x, tell := staleSetup(s, c)
			start := c.recs("n1", "start")[0].At
			idB := x.self.ID
			idA := idB + 1
			tell(update{Node: "x", ID: idB, State: Alive})
			tell(update{Node: "x", ID: idA, State: Alive}) // supersedes, and retires idB
			w.RunFor(start.Add(2500 * time.Millisecond).Sub(w.Sim.Now()))

			want := []string{
				"0s node=x id=" + uitoa(idB) + " inc=0 from=none to=alive source=syncresp",
				"0s node=x id=" + uitoa(idA) + " inc=0 from=alive to=alive source=syncresp",
				"2s node=x id=" + uitoa(idA) + " inc=0 from=alive to=suspect source=probe",
			}
			if got := memberLines(c, start); !slices.Equal(got, want) {
				tt.Errorf("n1's member records %q, want %q", got, want)
			}
			if acks := c.recs("n1", "ack"); len(acks) != 0 {
				tt.Errorf("n1's acks %s, want none from the retired ID", describe(acks))
			}
			pinged := 0
			for _, g := range x.got {
				if _, ok := g.msg.(ping); ok {
					pinged++
				}
			}
			if pinged < 2 {
				tt.Errorf("x received %d pings, want the probes of 1s and 2s", pinged)
			}
		})
	})

	t.Run("restart", func(t *testing.T) {
		const partitionAt, crashAt, restartAt, healAt, end = 4 * time.Second, 4100 * time.Millisecond, 4500 * time.Millisecond, 6 * time.Second, 12 * time.Second
		body := func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			c.node("n1", simConfig("n1", "n3"))
			c.node("n2", simConfig("n1", "n4"))
			c.node("n3", simConfig("n3"))
			c.node("n4", simConfig("n3"))
			names, halfA, halfB := []string{"n1", "n2", "n3", "n4"}, []string{"n1", "n2"}, []string{"n3", "n4"}
			w.RunFor(partitionAt)
			w.Faults.Inject(fault.Event{Kind: fault.KindPartition, Groups: [][]string{halfA, halfB}})
			w.RunFor(crashAt - partitionAt)
			oldB := []update{{Node: "n3", ID: c.id("n3"), State: Dead}, {Node: "n4", ID: c.id("n4"), State: Dead}}
			for _, n := range halfB {
				w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: n})
			}
			for _, n := range halfA {
				dead := syncResp{From: toUpdate(c.view(n).Self), Members: oldB}
				if err := sim.Send(s, n, lockstep.PIDAt[memberMsg](n, actorPath), memberMsg(dead)); err != nil {
					tt.Fatal(err)
				}
			}
			w.RunFor(restartAt - crashAt)
			for _, n := range halfB {
				w.Faults.Inject(fault.Event{Kind: fault.KindRestart, Node: n})
			}
			w.RunFor(healAt - restartAt)
			w.Faults.Inject(fault.Event{Kind: fault.KindHeal})
			w.RunFor(end - healAt)

			var want []Member
			for _, n := range names {
				want = append(want, alive(n, c.id(n)))
			}
			for _, n := range names {
				expectView(tt, n, c.view(n), want)
			}
			for _, n := range halfB {
				for _, r := range c.recs(n, "member") {
					if r.At > vt(restartAt) && slices.ContainsFunc(oldB, func(u update) bool { return attr(r, "node") == u.Node && attr(r, "id") == uitoa(u.ID) }) {
						tt.Errorf("%s took up an old ID of half B: %s", n, describe([]kernel.Record{r}))
					}
				}
			}
		}
		t.Run("seeds", func(t *testing.T) { sim.Simulate(t, simOpts(5, end+time.Second), body) })
		t.Run("regressions", func(t *testing.T) {
			sim.SimulateSeeds(t, simOpts(1, end+time.Second), body, 0x172e125e5e72027e, 0xd1fa1eb55a6eaee4)
		})
	})
}

// AT-CLU-06, CLU-230, CLU-240, §6 draw 2. "draw": the helpers are min(n, len) of the Alive members
// other than self and the target, by a partial Fisher-Yates shuffle over them in name order.
// "bridge": every message between the membership actors of n1 and n3 is dropped from 500ms on;
// each probe between them gets no direct ack, asks both other nodes at ProbeTimeout and is acked
// once, indirectly, although both helpers relay. No node is ever suspected. "relay": n1 is the
// helper of a node r for a node t: it pings t with the next seq of its own counter, which its
// probes use too, sends t's ack on to r with r's seq once, and drops a relay ProbeTimeout after
// the pingReq, so a later ack is not sent on; an ack from another ID of t than r probed is not
// sent on either.
func TestATCLU06IndirectProbe(t *testing.T) {
	t.Run("draw", func(t *testing.T) {
		members := []Member{
			alive("a", 1), alive("b", 2), {Node: "c", ID: 3, State: Suspect}, alive("d", 4), {Node: "e", ID: 5, State: Dead},
			alive("f", 6), alive("self", 7), alive("t", 8), {Node: "u", ID: 9, State: Left}, alive("v", 10),
		}
		cands := []string{"a", "b", "d", "f", "v"}
		got, want := rand.New(rand.NewPCG(5, 6)), rand.New(rand.NewPCG(5, 6)) //nolint:gosec // G404: seeded on purpose, to compare the draws
		for _, n := range []int{3, 3, 1, 5, 9, 2, 0, 4} {
			helpers := drawHelpers(got, members, "self", "t", n)
			w := slices.Clone(cands)
			k := min(n, len(w))
			for i := range k {
				j := i + want.IntN(len(w)-i)
				w[i], w[j] = w[j], w[i]
			}
			if !slices.Equal(helpers, w[:k]) {
				t.Fatalf("drawHelpers(%d) = %v, want %v", n, helpers, w[:k])
			}
		}
		for _, ms := range [][]Member{nil, {alive("self", 7), alive("t", 8)}, members[2:3]} {
			if helpers := drawHelpers(got, ms, "self", "t", 3); len(helpers) != 0 {
				t.Errorf("drawHelpers(%v) = %v, want none", ms, helpers)
			}
		}
		if got.Uint64() != want.Uint64() {
			t.Error("drawHelpers drew a value without a helper to choose")
		}
	})

	t.Run("bridge", func(t *testing.T) {
		names := []string{"n1", "n2", "n3", "n4"}
		sim.Simulate(t, simOpts(5, 14*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			for _, n := range names {
				c.node(n, simConfig(names...))
			}
			w.RunFor(500 * time.Millisecond)
			a1, a3 := lockstep.PIDAt[memberMsg]("n1", actorPath).Address(), lockstep.PIDAt[memberMsg]("n3", actorPath).Address()
			s.DropBetween(a1, a3, 1_000_000)
			s.DropBetween(a3, a1, 1_000_000)
			w.RunFor(12 * time.Second)
			end := w.Sim.Now() // the probes up to here; their acks arrive by the end of the run
			w.RunFor(500 * time.Millisecond)

			for _, pair := range [][2]string{{"n1", "n3"}, {"n3", "n1"}} {
				n, target := pair[0], pair[1]
				indirect, acks := bySeq(c.recs(n, "probe.indirect")), bySeq(c.recs(n, "ack"))
				count := 0
				for _, p := range c.recs(n, "probe") {
					if p.At > end {
						continue
					}
					seq := attr(p, "seq")
					switch attr(p, "target") {
					case target:
						count++
						in, a := indirect[seq], acks[seq]
						if len(in) != 1 || in[0].At != p.At.Add(DefaultProbeTimeout) || attrs(in[0]) != "target="+target+" seq="+seq+" helpers=2" {
							tt.Errorf("%s's indirect probes for %s: %s, want one with 2 helpers at ProbeTimeout", n, describe([]kernel.Record{p}), describe(in))
						}
						if len(a) != 1 || attrs(a[0]) != "target="+target+" seq="+seq+" via=indirect" || a[0].At.Sub(p.At) >= DefaultProbeInterval {
							tt.Errorf("%s's acks of %s: %s, want one indirect ack before the next tick", n, describe([]kernel.Record{p}), describe(a))
						}
					default:
						if a := acks[seq]; len(a) != 1 || attr(a[0], "via") != viaDirect || len(indirect[seq]) != 0 {
							tt.Errorf("%s's probe %s: acks %s, indirect %s, want one direct ack", n, describe([]kernel.Record{p}), describe(a), describe(indirect[seq]))
						}
					}
				}
				if count < 3 {
					tt.Errorf("%s probed %s %d times, want at least 3", n, target, count)
				}
			}
			for _, n := range names {
				if fails := c.recs(n, "probe.fail"); len(fails) != 0 {
					tt.Errorf("%s's probes failed: %s", n, describe(fails))
				}
				for _, r := range c.recs(n, "member") {
					if attr(r, "to") != "alive" {
						tt.Errorf("%s recorded %s, want no suspicion", n, describe([]kernel.Record{r}))
					}
				}
			}
		})
	})

	t.Run("relay", func(t *testing.T) {
		sim.Simulate(t, simOpts(2, 2*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			c.node("n1", Config{})
			r := c.fake("r", true)
			tg := c.fake("t", true)
			w.RunFor(10 * time.Millisecond)
			n1 := lockstep.PIDAt[memberMsg]("n1", actorPath)
			self := c.view("n1").Self
			start := c.recs("n1", "start")[0].At
			send := func(from string, msg memberMsg) {
				tt.Helper()
				if err := sim.Send(s, from, n1, msg); err != nil {
					tt.Fatal(err)
				}
				w.RunFor(50 * time.Millisecond)
			}
			// pinged returns the ping t received last; it fails tt unless t received one ping
			// since seen.
			pinged := func(seen int) ping {
				tt.Helper()
				if len(tg.got) != seen+1 {
					tt.Fatalf("t received %v, want one ping", tg.got[seen:])
				}
				p, ok := tg.got[seen].msg.(ping)
				if !ok || p.From != toUpdate(self) || p.Target != "t" || p.TargetID != tg.self.ID {
					tt.Fatalf("t received %+v, want a ping from n1 for t", tg.got[seen].msg)
				}
				return p
			}

			send("r", pingReq{From: r.self, Seq: 41, Target: "t", TargetID: tg.self.ID})
			p1 := pinged(0)
			send("t", ack{From: tg.self, Seq: p1.Seq, Target: "t"})
			want := ack{From: toUpdate(self), Seq: 41, Target: "t"}
			if len(r.got) != 1 || !equalAckIgnoringUpdates(r.got[0].msg, want) {
				tt.Fatalf("r received %v, want %+v", r.got, want)
			}
			send("t", ack{From: tg.self, Seq: p1.Seq, Target: "t"}) // the relay is gone: not sent on again
			if len(r.got) != 1 {
				tt.Errorf("r received %v after a second ack, want nothing more", r.got[1:])
			}

			send("r", pingReq{From: r.self, Seq: 42, Target: "t", TargetID: tg.self.ID})
			p2 := pinged(1)
			w.RunFor(DefaultProbeTimeout) // the relay expires ProbeTimeout after the pingReq
			send("t", ack{From: tg.self, Seq: p2.Seq, Target: "t"})
			if len(r.got) != 1 {
				tt.Errorf("r received %v after the relay expired, want nothing more", r.got[1:])
			}

			// r probed another ID of t: the ack of t's process is not sent on.
			send("r", pingReq{From: r.self, Seq: 43, Target: "t", TargetID: tg.self.ID + 5})
			if len(tg.got) != 3 {
				tt.Fatalf("t received %v, want a third ping", tg.got)
			}
			p3, _ := tg.got[2].msg.(ping)
			send("t", ack{From: tg.self, Seq: p3.Seq, Target: "t"})
			if len(r.got) != 1 {
				tt.Errorf("r received %v for another ID of t, want nothing more", r.got[1:])
			}
			w.RunFor(start.Add(1050 * time.Millisecond).Sub(w.Sim.Now()))
			probes := c.recs("n1", "probe")
			if p1.Seq != 1 || p2.Seq != 2 || p3.Seq != 3 || len(probes) != 1 || attr(probes[0], "seq") != "4" || probes[0].At != start.Add(time.Second) {
				tt.Errorf("relays with seqs %d, %d and %d, then probes %s, want seqs 1 to 3, then probe seq 4 at 1s", p1.Seq, p2.Seq, p3.Seq, describe(probes))
			}
		})
	})
}

// AT-CLU-07, CLU-240, CLU-300, CLU-800's bound: n5 of five nodes crashes at 3s plus an offset
// below ProbeInterval drawn per seed. Its probes fail, it goes Suspect and SuspicionTimeout later
// Dead, and every live node has it Dead within D(N) = 2*(N-1)*PI + PT + ST + PI of the crash, at
// its probed ID and incarnation. Every live node itself probes n5 within two rounds,
// 2*(N-1)*PI, of the crash, unless it has it Dead by then. No node suspects anyone else, or n5
// before the crash, and none probes n5 once it has it Dead.
func TestATCLU07CrashDetection(t *testing.T) {
	names := []string{"n1", "n2", "n3", "n4", "n5"}
	rounds := 2 * time.Duration(len(names)-1) * DefaultProbeInterval
	bound := rounds + DefaultProbeTimeout + DefaultSuspicionTimeout + DefaultProbeInterval
	sim.Simulate(t, simOpts(5, 3*time.Second+DefaultProbeInterval+bound+2*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		for _, n := range names {
			c.node(n, simConfig(names...))
		}
		crashAt := 3*time.Second + time.Duration(w.Sim.Rand("cluster/test/crash").Int64N(int64(DefaultProbeInterval)))
		w.RunFor(crashAt)
		ids := map[string]uint32{} // read by key
		for _, n := range names {
			ids[n] = c.id(n)
		}
		crash := w.Sim.Now()
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n5"})
		w.RunFor(bound + time.Second)

		var bySuspicion, byProbe int
		for _, n := range names[:4] {
			var suspect, dead []kernel.Record
			for _, r := range c.recs(n, "member") {
				switch {
				case attr(r, "to") == "alive":
				case attr(r, "node") != "n5" || r.At < crash:
					tt.Errorf("%s recorded %s, want only n5 to go down, after the crash", n, describe([]kernel.Record{r}))
				case attr(r, "to") == "suspect":
					suspect = append(suspect, r)
				case attr(r, "to") == "dead":
					dead = append(dead, r)
				}
			}
			if len(dead) != 1 || dead[0].At > crash.Add(bound) || attr(dead[0], "id") != uitoa(ids["n5"]) {
				tt.Fatalf("%s's records of n5 down: %s%s, want one Dead within %s of the crash at %s", n, describe(suspect), describe(dead), bound, crash)
			}
			if len(suspect) > 1 || (len(suspect) == 1 && suspect[0].At > dead[0].At) {
				tt.Errorf("%s's records of n5 Suspect: %s, want at most one before Dead", n, describe(suspect))
			}
			if attr(dead[0], "source") == sourceSuspicion {
				bySuspicion++
				if len(suspect) != 1 || dead[0].At != suspect[0].At.Add(DefaultSuspicionTimeout) || attr(dead[0], "inc") != attr(suspect[0], "inc") {
					tt.Errorf("%s declared n5 dead %s, want SuspicionTimeout after it suspected it %s", n, describe(dead), describe(suspect))
				}
			}
			if len(suspect) == 1 && attr(suspect[0], "source") == sourceProbe {
				byProbe++
				fails := c.recs(n, "probe.fail")
				if !slices.ContainsFunc(fails, func(f kernel.Record) bool { return f.At == suspect[0].At && attr(f, "target") == "n5" }) {
					tt.Errorf("%s suspected n5 %s without a failed probe then: %s", n, describe(suspect), describe(fails))
				}
			}
			probed := false // n probed n5 within two rounds of the crash
			for _, r := range c.recs(n, "probe") {
				if attr(r, "target") != "n5" || r.At <= crash {
					continue
				}
				probed = probed || r.At <= crash.Add(rounds)
				if r.At > dead[0].At {
					tt.Errorf("%s probed n5 %s after it had it Dead at %s", n, describe([]kernel.Record{r}), dead[0].At)
				}
			}
			if !probed && dead[0].At > crash.Add(rounds) {
				tt.Errorf("%s did not probe n5 within %s of the crash at %s, and had it Dead only at %s", n, rounds, crash, dead[0].At)
			}
			for _, r := range c.recs(n, "probe.indirect") {
				if attr(r, "target") != "n5" || attr(r, "helpers") != "3" {
					tt.Errorf("%s's indirect probe %s, want only of n5, with 3 helpers", n, describe([]kernel.Record{r}))
				}
			}
			var want []Member
			for _, m := range names[:4] {
				want = append(want, alive(m, ids[m]))
			}
			want = append(want, Member{Node: "n5", ID: ids["n5"], State: Dead})
			expectView(tt, n, c.view(n), want)
		}
		if bySuspicion == 0 || byProbe == 0 {
			tt.Errorf("%d nodes suspected n5 by a probe and %d declared it Dead by suspicion, want both", byProbe, bySuspicion)
		}
	})
}

// AT-CLU-11, CLU-310, CLU-320. n2 is told it is Suspect at its own incarnation 0 and refutes with
// incarnation 1; a Suspect below its incarnation, a Left, an Alive and an update about another ID
// of n2 change nothing; a Dead at its incarnation is refuted with incarnation 2. n1 learns
// incarnation 2. Then n1 is told n2 is Dead at incarnation 2: n2's messages at incarnation 2 do
// not revive it, n1's forced update makes n2 refute with incarnation 3, and Alive at 3 revives n2
// at n1.
func TestATCLU11Refute(t *testing.T) {
	sim.Simulate(t, simOpts(5, 8*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		c.node("n1", simConfig("n1", "n2"))
		c.node("n2", simConfig("n1", "n2"))
		w.RunFor(1500 * time.Millisecond)
		id1, id2 := c.id("n1"), c.id("n2")
		seen := len(clusterRecs(c, "n2")) // n2's records checked so far
		// tell sends node a syncResp from itself with us and runs 10ms. When node is n2, it checks
		// that n2 recorded exactly want (member, refute and view records) meanwhile.
		tell := func(node string, us []update, want ...string) {
			tt.Helper()
			msg := syncResp{From: toUpdate(c.view(node).Self), Members: us}
			if err := sim.Send(s, node, lockstep.PIDAt[memberMsg](node, actorPath), memberMsg(msg)); err != nil {
				tt.Fatal(err)
			}
			w.RunFor(10 * time.Millisecond)
			if node != "n2" {
				return
			}
			var got []string
			recs := clusterRecs(c, "n2")
			for _, r := range recs[seen:] {
				if k := strings.TrimPrefix(r.Kind, recordPrefix); k == "member" || k == "refute" || k == "view" {
					got = append(got, k+" "+attrs(r))
				}
			}
			seen = len(recs)
			if !slices.Equal(got, want) {
				tt.Errorf("n2 recorded %q, want %q", got, want)
			}
		}
		about := func(inc uint64, state MemberState) []update {
			return []update{{Node: "n2", ID: id2, Inc: inc, State: state}}
		}
		tell("n2", nil)
		tell("n2", about(0, Suspect),
			"member node=n2 id="+uitoa(id2)+" inc=1 from=alive to=alive source=self", "refute inc=1 about=suspect", "view version=3 members=2 up=2")
		tell("n2", about(0, Suspect))
		tell("n2", about(0, Dead))
		tell("n2", about(5, Left))
		tell("n2", about(7, Alive))
		tell("n2", []update{{Node: "n2", ID: id2 + 1, Inc: 4, State: Suspect}})
		tell("n2", about(1, Dead),
			"member node=n2 id="+uitoa(id2)+" inc=2 from=alive to=alive source=self", "refute inc=2 about=dead", "view version=4 members=2 up=2")
		if v := c.view("n2"); v.Self != (Member{Node: "n2", ID: id2, Incarnation: 2, State: Alive}) {
			tt.Errorf("n2's view has Self %v, want incarnation 2", v.Self)
		}
		w.RunFor(2 * time.Second)
		if m, _ := c.view("n1").Member("n2"); m.Incarnation != 2 || m.State != Alive {
			tt.Fatalf("n1's view has n2 %v, want Alive at incarnation 2", m)
		}

		deadAt := w.Sim.Now()
		tell("n1", about(2, Dead))
		w.RunFor(4 * time.Second)
		var after []string
		for _, r := range c.recs("n1", "member") {
			if r.At >= deadAt {
				after = append(after, attrs(r))
			}
		}
		if len(after) != 2 || after[0] != "node=n2 id="+uitoa(id2)+" inc=2 from=alive to=dead source=syncresp" ||
			!strings.HasPrefix(after[1], "node=n2 id="+uitoa(id2)+" inc=3 from=dead to=alive source=") {
			tt.Errorf("n1's member records after it was told n2 is Dead: %q, want Dead at 2, then Alive at 3", after)
		}
		refutes := c.recs("n2", "refute")
		if len(refutes) != 3 || attrs(refutes[2]) != "inc=3 about=dead" || refutes[2].At < deadAt {
			tt.Errorf("n2's refutations: %s, want a third one, inc=3 about=dead", describe(refutes))
		}
		want := []Member{alive("n1", id1), {Node: "n2", ID: id2, Incarnation: 3, State: Alive}}
		expectView(tt, "n1", c.view("n1"), want)
		expectView(tt, "n2", c.view("n2"), want)
	})
}

// AT-CLU-13, CLU-310, CLU-820: with ProbeInterval 500ms, ProbeTimeout 200ms and SuspicionTimeout
// 6s, n4 of four nodes is cut off from 2s to 5.5s: less than SuspicionTimeout - ProbeInterval, and
// long enough for two rounds of probes, so that every other node probes n4 in vain and has it
// Suspect. n4 learns it through the forced updates after the heal and refutes, and no node is ever
// Dead in any view. Every view ends with the four Alive, at the same incarnations.
func TestATCLU13TransientCut(t *testing.T) {
	names := []string{"n1", "n2", "n3", "n4"}
	const pi, st = 500 * time.Millisecond, 6 * time.Second
	const cutAt, healAt = 2 * time.Second, 5500 * time.Millisecond
	if healAt-cutAt >= st-pi || healAt-cutAt < time.Duration(2*len(names)-1)*pi {
		t.Fatal("the cut is not shorter than SuspicionTimeout - ProbeInterval, or shorter than two rounds")
	}
	cfg := simConfig(names...)
	cfg.ProbeInterval, cfg.ProbeTimeout, cfg.SuspicionTimeout = pi, 200*time.Millisecond, st
	sim.Simulate(t, simOpts(5, 16*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		for _, n := range names {
			c.node(n, cfg)
		}
		w.RunFor(cutAt)
		w.Faults.Inject(fault.Event{Kind: fault.KindIsolate, Node: "n4"})
		w.RunFor(healAt - cutAt)
		w.Faults.Inject(fault.Event{Kind: fault.KindHeal})
		w.RunFor(15*time.Second - healAt)

		for _, n := range names {
			suspected := false
			for _, r := range c.recs(n, "member") {
				switch attr(r, "to") {
				case "suspect":
					suspected = suspected || attr(r, "node") == "n4"
					if r.At < vt(cutAt) {
						tt.Errorf("%s recorded %s before the cut", n, describe([]kernel.Record{r}))
					}
				case "dead", "left":
					tt.Errorf("%s recorded %s, want no member down", n, describe([]kernel.Record{r}))
				}
			}
			if n != "n4" && !suspected {
				tt.Errorf("%s never had n4 Suspect", n)
			}
		}
		refutes := c.recs("n4", "refute")
		if len(refutes) == 0 || attr(refutes[0], "about") != "suspect" || refutes[0].At < vt(healAt) {
			tt.Errorf("n4's refutations: %s, want one about suspect after the heal", describe(refutes))
		}
		want := c.view("n1").Members
		for _, m := range want {
			if m.State != Alive {
				tt.Errorf("n1's view has %v, want every member Alive", m)
			}
		}
		if m, _ := c.view("n1").Member("n4"); m.Incarnation == 0 {
			tt.Errorf("n1's view has n4 %v, want a refuted incarnation", m)
		}
		for _, n := range names {
			expectView(tt, n, c.view(n), want)
		}
	})
}

// AT-CLU-16, CLU-240, CLU-300: n1 probes x, a node that never answers, with SuspicionTimeout
// 2.5s. "incarnation": x goes Suspect at incarnation 0 at 2s and is Alive at incarnation 1 at
// 2.25s, so the probe of 2s fails at 3s and x goes Suspect at 1. The timer of incarnation 0 fires
// at 4.5s, while x is Suspect at 1, and does nothing; x goes Dead at 5.5s. "id": x goes Suspect
// with ID 7 at 2s and is superseded by ID 8 at 2.25s, by a message from ID 8 itself, so the probe
// of ID 7 fails at 3s without a suspicion, the probe of ID 8 fails at 4s, the timer of ID 7 fires
// at 4.5s while ID 8 is Suspect at the same incarnation and does nothing, and x goes Dead at 6.5s.
// "hearsay": the same news of ID 8, from another node's Members, supersedes nothing, so ID 7 goes
// Dead at 4.5s.
func TestATCLU16StaleSuspicion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		update update // what n1 is told at 2.25s
		direct bool   // told as the From of a message, else as hearsay in Members
		want   []string
	}{
		{"incarnation", update{Node: "x", ID: 7, Inc: 1, State: Alive}, false, []string{
			"100ms node=x id=7 inc=0 from=none to=alive source=syncresp",
			"2s node=x id=7 inc=0 from=alive to=suspect source=probe",
			"2.25s node=x id=7 inc=1 from=suspect to=alive source=syncresp",
			"3s node=x id=7 inc=1 from=alive to=suspect source=probe",
			"5.5s node=x id=7 inc=1 from=suspect to=dead source=suspicion",
		}},
		{"id", update{Node: "x", ID: 8, State: Alive}, true, []string{
			"100ms node=x id=7 inc=0 from=none to=alive source=syncresp",
			"2s node=x id=7 inc=0 from=alive to=suspect source=probe",
			"2.25s node=x id=8 inc=0 from=suspect to=alive source=syncresp",
			"4s node=x id=8 inc=0 from=alive to=suspect source=probe",
			"6.5s node=x id=8 inc=0 from=suspect to=dead source=suspicion",
		}},
		{"hearsay", update{Node: "x", ID: 8, State: Alive}, false, []string{
			"100ms node=x id=7 inc=0 from=none to=alive source=syncresp",
			"2s node=x id=7 inc=0 from=alive to=suspect source=probe",
			"4.5s node=x id=7 inc=0 from=suspect to=dead source=suspicion",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim.Simulate(t, simOpts(2, 9*time.Second), func(s *sim.Sim) {
				w, tt := s.World(), s.World().T()
				c := newSimCluster(s)
				c.node("n1", Config{SuspicionTimeout: 2500 * time.Millisecond})
				c.fake("x", true)
				w.RunFor(time.Millisecond)
				start := c.recs("n1", "start")[0].At
				n1 := lockstep.PIDAt[memberMsg]("n1", actorPath)
				tell := func(at time.Duration, u update, direct bool) {
					tt.Helper()
					w.RunFor(start.Add(at).Sub(w.Sim.Now()))
					msg := syncResp{From: toUpdate(c.view("n1").Self), Members: []update{u}}
					if direct {
						msg = syncResp{From: u}
					}
					if err := sim.Send(s, "n1", n1, memberMsg(msg)); err != nil {
						tt.Fatal(err)
					}
				}
				tell(100*time.Millisecond, update{Node: "x", ID: 7, State: Alive}, false)
				tell(2250*time.Millisecond, tc.update, tc.direct)
				w.RunFor(start.Add(8 * time.Second).Sub(w.Sim.Now()))

				var got []string
				for _, r := range c.recs("n1", "member") {
					got = append(got, r.At.Sub(start).String()+" "+attrs(r))
				}
				if !slices.Equal(got, tc.want) {
					tt.Errorf("n1's member records:\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
				}
				var fails []string
				for _, r := range c.recs("n1", "probe.fail") {
					fails = append(fails, r.At.Sub(start).String()+" "+attrs(r))
				}
				if want := []string{"2s target=x seq=1", "3s target=x seq=2", "4s target=x seq=3", "5s target=x seq=4"}; len(fails) < 4 || !slices.Equal(fails[:4], want) {
					tt.Errorf("n1's failed probes %q, want %q first", fails, want)
				}
				var indirect []string
				for _, r := range c.recs("n1", "probe.indirect") {
					indirect = append(indirect, r.At.Sub(start).String()+" "+attrs(r))
				}
				if len(indirect) == 0 || indirect[0] != "1.5s target=x seq=1 helpers=0" {
					tt.Errorf("n1's indirect probes %q, want the first at 1.5s without helpers", indirect)
				}
			})
		})
	}
}

// The helpers of the probing tests.

// fakeMsg is a message a fake member received, with the time.
type fakeMsg struct {
	at  kernel.Time
	msg memberMsg
}

// fakeMember is the state of a stand-in membership actor at /system/cluster of a node that never
// joins: it keeps every message it receives and, unless mute, answers each ping with an ack.
type fakeMember struct {
	now  func() kernel.Time
	self update // its own entry: its node, the System's incarnation, Alive
	mute bool
	got  []fakeMsg
}

// fakeActor is the actor of a fakeMember.
type fakeActor struct{ f *fakeMember }

// Receive keeps msg and answers a ping unless the member is mute.
func (a *fakeActor) Receive(ctx *lockstep.Context[memberMsg], msg memberMsg) {
	a.f.got = append(a.f.got, fakeMsg{a.f.now(), msg})
	switch m := msg.(type) {
	case ping:
		if a.f.mute {
			return
		}
		if err := lockstep.Send(ctx, lockstep.PIDAt[memberMsg](m.From.Node, actorPath), memberMsg(ack{From: a.f.self, Seq: m.Seq, Target: a.f.self.Node})); err != nil {
			panic(err)
		}
	case update, pingReq, ack, syncReq, syncResp, leave, joinTick, syncTick, reapTimer, probeTick, probeTimeout, relayExpire, suspectTimeout:
		// Only kept.
	}
}

// fake adds node name with a fake member and returns it.
func (c *simCluster) fake(name string, mute bool) *fakeMember {
	f := &fakeMember{now: c.s.World().Sim.Now, mute: mute}
	c.n[name] = c.s.Node(name, func(sys *lockstep.System) {
		sp, _ := hooks.SystemSpawner(sys).(lockstep.Spawner)
		if _, err := lockstep.Spawn(sp, actorName, func() lockstep.Actor[memberMsg] { return &fakeActor{f} }); err != nil {
			panic(err)
		}
		f.self = update{Node: name, ID: sys.Incarnation(), State: Alive}
	})
	return f
}

// equalAck reports whether msg is the ack want, updates included.
func equalAck(msg memberMsg, want ack) bool {
	a, ok := msg.(ack)
	return ok && equalAckIgnoringUpdates(a, want) && slices.Equal(a.Updates, want.Updates)
}

// equalAckIgnoringUpdates reports whether msg is the ack want, apart from its updates.
func equalAckIgnoringUpdates(msg memberMsg, want ack) bool {
	a, ok := msg.(ack)
	return ok && a.From == want.From && a.Seq == want.Seq && a.Target == want.Target
}

// bySeq returns recs by their seq attribute.
func bySeq(recs []kernel.Record) map[string][]kernel.Record {
	out := map[string][]kernel.Record{}
	for _, r := range recs {
		out[attr(r, "seq")] = append(out[attr(r, "seq")], r)
	}
	return out
}

// clusterRecs returns node name's records of the package, in order.
func clusterRecs(c *simCluster, name string) []kernel.Record {
	id := c.n[name].ID()
	var out []kernel.Record
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && strings.HasPrefix(r.Kind, recordPrefix) {
			out = append(out, r)
		}
	}
	return out
}
