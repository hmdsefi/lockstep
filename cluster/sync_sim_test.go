// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-CLU-03, CLU-030, CLU-040, CLU-420, CLU-430. "join": n1, n2 and n3 all have the seed n1. n1
// has joined at start; n2 and n3 each send one join attempt to n1 and join on its answer. n3 learns
// of n2 (and n2 of n3) through the periodic syncs, so every view converges to the three Alive.
// "answer": a probe at /system/cluster on n2 sends n1 a syncReq; n1 merges its From and answers
// with a syncResp from n1's own entry holding every entry of its table, sorted by name.
func TestATCLU03JoinThroughSeed(t *testing.T) {
	t.Run("join", func(t *testing.T) { runATCLU03Join(t) })
	t.Run("answer", func(t *testing.T) { runATCLU03Answer(t) })
}

func runATCLU03Join(t *testing.T) {
	sim.Simulate(t, simOpts(5, 6*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		cfg := simConfig("n1")
		names := []string{"n1", "n2", "n3"}
		for _, n := range names {
			c.node(n, cfg)
		}
		w.RunFor(5 * time.Second)

		want := []Member{alive("n1", c.id("n1")), alive("n2", c.id("n2")), alive("n3", c.id("n3"))}
		for _, n := range names {
			expectView(tt, n, c.view(n), want)
			start := c.recs(n, "start")
			if len(start) != 1 || attrs(start[0]) != "node="+n+" id="+uitoa(c.id(n))+" inc=0 seeds=1 restart=false" {
				tt.Errorf("%s's start records: %s", n, describe(start))
			}
			joined := c.recs(n, "joined")
			if len(joined) != 1 {
				tt.Fatalf("%s's joined records: %s, want one", n, describe(joined))
			}
			syncs := c.recs(n, "sync")
			var joins []string
			for _, r := range syncs {
				if attr(r, "reason") == syncJoin {
					joins = append(joins, attrs(r))
					if r.At > joined[0].At {
						tt.Errorf("%s sent a join attempt at %s, after it joined at %s", n, r.At, joined[0].At)
					}
				}
			}
			if n == "n1" {
				// n1's only seed is itself: it has joined at start, with only itself.
				if joined[0].At != start[0].At || attrs(joined[0]) != "members=1" || len(joins) != 0 {
					tt.Errorf("n1 joined %s with join attempts %q, want at its start %s, alone and without attempts", describe(joined), joins, start[0].At)
				}
				continue
			}
			if len(joins) != 1 || joins[0] != "peer=n1 reason=join attempt=1" {
				tt.Errorf("%s's join attempts %q, want one to n1", n, joins)
			}
			if m := attr(joined[0], "members"); m != "2" && m != "3" {
				tt.Errorf("%s joined with %s members, want 2 or 3", n, m)
			}
		}
	})
}

// syncProbe is an actor at /system/cluster that sends one syncReq from from to n1 at its first
// joinTick and keeps every syncResp it receives.
type syncProbe struct {
	from update
	got  *[]syncResp
}

func (p *syncProbe) Started(ctx *lockstep.Context[memberMsg]) {
	ctx.After(100*time.Millisecond, joinTick{})
}

func (p *syncProbe) Receive(ctx *lockstep.Context[memberMsg], msg memberMsg) {
	switch m := msg.(type) {
	case joinTick:
		if err := lockstep.Send(ctx, lockstep.PIDAt[memberMsg]("n1", actorPath), memberMsg(syncReq{From: p.from})); err != nil {
			panic(err)
		}
	case syncResp:
		*p.got = append(*p.got, m)
	case update, ping, pingReq, ack, syncReq, leave, syncTick, reapTimer:
		// The probe ignores every other message.
	}
}

func runATCLU03Answer(t *testing.T) {
	sim.Simulate(t, simOpts(3, 2*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		c.node("n1", simConfig())
		var got []syncResp
		var probeFrom update
		s.Node("n2", func(sys *lockstep.System) {
			sp, _ := hooks.SystemSpawner(sys).(lockstep.Spawner)
			probeFrom = update{Node: "n2", ID: sys.Incarnation(), State: Alive}
			if _, err := lockstep.Spawn(sp, actorName, func() lockstep.Actor[memberMsg] { return &syncProbe{from: probeFrom, got: &got} }); err != nil {
				panic(err)
			}
		})
		w.RunFor(10 * time.Millisecond)
		// n1's table gets entries out of name order before the probe asks.
		self := toUpdate(c.view("n1").Self)
		known := syncResp{From: self, Members: []update{
			{Node: "c", ID: 3, State: Alive}, {Node: "a", ID: 1, Inc: 2, State: Suspect}, {Node: "b", ID: 2, State: Alive},
			{Node: "b", ID: 2, State: Dead},
		}}
		if err := sim.Send(s, "n1", lockstep.PIDAt[memberMsg]("n1", actorPath), memberMsg(known)); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)

		v := c.view("n1")
		want := []update{known.Members[1], known.Members[3], known.Members[0], self, probeFrom}
		if gotView := v.Members; len(gotView) != len(want) {
			tt.Fatalf("n1's view %v, want the five entries %v", gotView, want)
		}
		if len(got) != 1 {
			tt.Fatalf("the probe got %d syncResps, want 1", len(got))
		}
		if got[0].From != self || !slices.Equal(got[0].Members, want) {
			tt.Errorf("n1 answered with From %v and Members %v, want From %v and %v", got[0].From, got[0].Members, self, want)
		}
		for i, m := range v.Members {
			if toUpdate(m) != got[0].Members[i] {
				tt.Errorf("the answer's entry %d is %v, n1's table has %v", i, got[0].Members[i], m)
			}
		}
	})
}

// AT-CLU-04, CLU-040: n2's seeds are n3, n1 and itself; n1 and n3 are their own only seeds, so
// they have joined at start. A partition cuts n2 off from 0 to 3.025s. Until it joins, n2 sends
// join attempt k to n3, then n1 (the order of Seeds, without itself), at (k-1)*JoinRetryInterval.
// After the heal the attempts reach the seeds, n2 joins on the first answer and sends no more,
// and the periodic syncs bring every view to the three Alive.
func TestATCLU04SeedRetry(t *testing.T) {
	const healAt = 3025 * time.Millisecond
	sim.Simulate(t, simOpts(5, 8*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		c.node("n1", simConfig("n1"))
		c.node("n2", simConfig("n3", "n1", "n2"))
		c.node("n3", simConfig("n3"))
		w.Faults.Inject(fault.Event{Kind: fault.KindPartition, Groups: [][]string{{"n2"}, {"n1", "n3"}}})
		w.RunFor(healAt)
		if v := c.view("n2"); v.Version != 1 {
			tt.Errorf("n2's view before the heal is version %d with %v, want version 1", v.Version, v.Members)
		}
		w.Faults.Inject(fault.Event{Kind: fault.KindHeal})
		w.RunFor(7*time.Second - healAt)

		joined := c.recs("n2", "joined")
		if len(joined) != 1 || joined[0].At < vt(healAt) || joined[0].At > vt(healAt+time.Second) {
			tt.Fatalf("n2's joined records: %s, want one within 1s after the heal at %s", describe(joined), vt(healAt))
		}
		var joins []kernel.Record
		for _, r := range c.recs("n2", "sync") {
			if attr(r, "reason") == syncJoin {
				joins = append(joins, r)
			}
		}
		attempts := len(joins) / 2
		if attempts < 7 || len(joins) != 2*attempts {
			tt.Fatalf("n2's join attempts: %s, want two per attempt and at least 7 attempts", describe(joins))
		}
		for i, r := range joins {
			k := i/2 + 1
			peer := []string{"n3", "n1"}[i%2]
			at := vt(time.Duration(k-1) * 500 * time.Millisecond)
			if attrs(r) != "peer="+peer+" reason=join attempt="+strconv.Itoa(k) || r.At != at {
				tt.Errorf("n2's join record %d: %s, want peer=%s reason=join attempt=%d at %s", i, describe([]kernel.Record{r}), peer, k, at)
			}
		}
		if last := joins[len(joins)-1].At; last > joined[0].At || last < vt(healAt-500*time.Millisecond) {
			tt.Errorf("n2's last join attempt at %s, want the one before it joined at %s", last, joined[0].At)
		}
		want := []Member{alive("n1", c.id("n1")), alive("n2", c.id("n2")), alive("n3", c.id("n3"))}
		for _, n := range []string{"n1", "n2", "n3"} {
			expectView(tt, n, c.view(n), want)
		}
	})
}

// AT-CLU-25, CLU-440, §6 draw 3. "draw": the periodic target is r.IntN(len) over the Alive members
// other than self, in name order, one draw per call, and no draw without a candidate. "sim": n1 to
// n4 join through n1; every node sends one periodic sync every SyncInterval from when it joined,
// each to a member it knew as Alive by then, and over its rounds to more than one member. "alone":
// a cluster of one sends no sync at all.
func TestATCLU25PeriodicSync(t *testing.T) {
	t.Run("draw", func(t *testing.T) {
		members := []Member{
			alive("a", 1), {Node: "b", ID: 2, State: Suspect}, alive("c", 3), {Node: "d", ID: 4, State: Dead},
			alive("e", 5), {Node: "f", ID: 6, State: Left}, alive("self", 7),
		}
		cands := []string{"a", "c", "e"}
		got, want := rand.New(rand.NewPCG(1, 2)), rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404: seeded on purpose, to compare the draws
		seen := map[string]bool{}
		for i := range 100 {
			peer, ok := periodicTarget(got, members, "self")
			if w := cands[want.IntN(len(cands))]; !ok || peer != w {
				t.Fatalf("draw %d: periodicTarget = %q, %t, want %q, true", i, peer, ok, w)
			}
			seen[peer] = true
		}
		if len(seen) != len(cands) {
			t.Errorf("100 draws chose %v, want every one of %v", seen, cands)
		}
		for _, ms := range [][]Member{nil, {alive("self", 7)}, members[1:2], {members[3], members[5], alive("self", 7)}} {
			if peer, ok := periodicTarget(got, ms, "self"); ok {
				t.Errorf("periodicTarget(%v) = %q, want none", ms, peer)
			}
		}
		if got.Uint64() != want.Uint64() {
			t.Error("periodicTarget drew a value without a candidate")
		}
	})

	t.Run("sim", func(t *testing.T) {
		sim.Simulate(t, simOpts(5, 13*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			names := []string{"n1", "n2", "n3", "n4"}
			for _, n := range names {
				c.node(n, simConfig("n1"))
			}
			w.RunFor(12 * time.Second)
			for _, n := range names {
				joined := c.recs(n, "joined")
				if len(joined) != 1 {
					tt.Fatalf("%s's joined records: %s", n, describe(joined))
				}
				var periodic []kernel.Record
				for _, r := range c.recs(n, "sync") {
					switch attr(r, "reason") {
					case syncPeriodic:
						periodic = append(periodic, r)
					case syncReconnect:
						tt.Errorf("%s sent a reconnect sync with every seed up: %s", n, describe([]kernel.Record{r}))
					}
				}
				if want := int(vt(12*time.Second).Sub(joined[0].At) / time.Second); len(periodic) != want {
					tt.Errorf("%s sent %d periodic syncs by 12s, want %d: one per second after it joined at %s", n, len(periodic), want, joined[0].At)
				}
				knownAt := map[string]kernel.Time{} // when n first had the member Alive; read by key
				for _, r := range c.recs(n, "member") {
					if _, ok := knownAt[attr(r, "node")]; !ok && attr(r, "to") == "alive" {
						knownAt[attr(r, "node")] = r.At
					}
				}
				targets := map[string]int{} // read by key
				for i, r := range periodic {
					peer := attr(r, "peer")
					if at := joined[0].At.Add(time.Duration(i+1) * time.Second); r.At != at {
						tt.Errorf("%s's periodic sync %d at %s, want %s", n, i+1, r.At, at)
					}
					if k, ok := knownAt[peer]; !ok || k > r.At || peer == n {
						tt.Errorf("%s synced with %s at %s, which it did not know as Alive then", n, peer, r.At)
					}
					if attrs(r) != "peer="+peer+" reason=periodic" {
						tt.Errorf("%s's periodic sync record %s", n, describe([]kernel.Record{r}))
					}
					targets[peer]++
				}
				if len(targets) < 2 {
					tt.Errorf("%s's periodic syncs all went to %v, want the draw to choose more than one member", n, targets)
				}
			}
		})
	})

	t.Run("alone", func(t *testing.T) {
		sim.Simulate(t, simOpts(2, 6*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			c.node("n1", simConfig())
			w.RunFor(5 * time.Second)
			if syncs := c.recs("n1", "sync"); len(syncs) != 0 {
				tt.Errorf("a cluster of one sent syncs: %s", describe(syncs))
			}
			if v := c.view("n1"); v.Version != 1 {
				tt.Errorf("a cluster of one published view %d", v.Version)
			}
		})
	})
}

// AT-CLU-27, CLU-450. "targets": the reconnect syncs go to every Dead member other than self in
// name order, then to every seed without an up entry in name order, leaving out self, the periodic
// target and the peers already listed. "sim": a partition that only the reconnect syncs can heal.
// n1 to n4 converge (seeds: n1 and n2 {n1, n3, n4}, n3 and n4 {n3}). At 4s a partition splits
// A = {n1, n2} from B = {n3, n4}; at 4.1s B's nodes crash, and A is told that their IDs are Dead
// (as probing would, from 3.3-05), so A sends B no periodic sync; at 4.5s B restarts with new IDs
// and no memory of A, so B sends A nothing. Each round of A is then its periodic sync to the other
// A node and reconnect syncs to n3 and n4, in name order (both also seeds, listed once). After the
// heal at 6s, what A queued for B's old incarnations is dropped (peer-restarted), the next
// reconnect syncs reach B, B's first news of A comes from such a syncReq, the answers bring B's new
// IDs to A, and every view holds the four Alive; the reconnect syncs then stop. Without reconnect
// syncs the halves stay apart.
func TestATCLU27ReconnectSync(t *testing.T) {
	t.Run("targets", func(t *testing.T) {
		members := []Member{
			{Node: "a", ID: 1, State: Dead}, alive("b", 2), {Node: "c", ID: 3, State: Left}, {Node: "d", ID: 4, State: Dead},
			{Node: "e", ID: 5, State: Suspect}, alive("self", 6), {Node: "z", ID: 7, State: Dead},
		}
		for _, c := range []struct {
			seeds    []string
			periodic string
			want     []string
		}{
			{nil, "", []string{"a", "d", "z"}},
			{[]string{"a", "b", "c", "e", "x", "y"}, "b", []string{"a", "d", "z", "c", "x", "y"}},
			{[]string{"c", "x"}, "x", []string{"a", "d", "z", "c"}},
		} {
			if got := reconnectTargets(members, c.seeds, "self", c.periodic); !slices.Equal(got, c.want) {
				t.Errorf("reconnectTargets(seeds %v, periodic %q) = %v, want %v", c.seeds, c.periodic, got, c.want)
			}
		}
		if got := reconnectTargets([]Member{alive("self", 6)}, []string{"b"}, "self", ""); !slices.Equal(got, []string{"b"}) {
			t.Errorf("reconnectTargets of an unknown seed = %v, want [b]", got)
		}
		// The actor sorts its seeds by name and leaves itself out.
		a := newMemberActor(&Membership{self: alive("self", 6), cfg: Config{Seeds: []string{"y", "self", "c", "x"}}})
		if !slices.Equal(a.seeds, []string{"c", "x", "y"}) {
			t.Errorf("the actor's reconnect seeds are %v, want [c x y]", a.seeds)
		}
	})

	t.Run("sim", func(t *testing.T) {
		const (
			partitionAt = 4 * time.Second
			crashAt     = 4100 * time.Millisecond
			restartAt   = 4500 * time.Millisecond
			healAt      = 6 * time.Second
			end         = 10 * time.Second
		)
		sim.Simulate(t, simOpts(5, end+time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			// A's seeds name both B nodes, so A's join attempts open a session to each of them at
			// the start: A's remoting then knows B's old incarnations and drops what it queued for
			// them once B restarts. A frame queued for a peer never reached has no incarnation and
			// would reach B's new incarnation: a stale Alive about B's old IDs would then supersede
			// B's new IDs at a node of B that never knew the old ones.
			c.node("n1", simConfig("n1", "n3", "n4"))
			c.node("n2", simConfig("n1", "n3", "n4"))
			c.node("n3", simConfig("n3"))
			c.node("n4", simConfig("n3"))
			names, halfA, halfB := []string{"n1", "n2", "n3", "n4"}, []string{"n1", "n2"}, []string{"n3", "n4"}
			current := func() []Member {
				var ms []Member
				for _, n := range names {
					ms = append(ms, alive(n, c.id(n)))
				}
				return ms
			}
			w.RunFor(partitionAt)
			for _, n := range names {
				if v := c.view(n); !slices.Equal(v.Members, current()) {
					tt.Fatalf("%s's view before the partition: %v, want %v", n, v.Members, current())
				}
			}

			// Half A loses half B: B's nodes crash and come back with new IDs and no memory of A,
			// and A is told that B's old IDs are Dead, so A sends B no periodic sync. Whatever A
			// queued for B's old incarnations is dropped when the sessions see the restart.
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
			for _, n := range halfB {
				if v := c.view(n); len(v.Members) != 2 {
					tt.Errorf("%s's view before the heal: %v, want only half B", n, v.Members)
				}
			}
			w.Faults.Inject(fault.Event{Kind: fault.KindHeal})
			w.RunFor(end - healAt)

			// Only the reconnect syncs crossed: every view holds the four, B with its new IDs.
			for _, n := range names {
				expectView(tt, n, c.view(n), current())
			}
			for _, n := range halfA {
				other := halfA[1-slices.Index(halfA, n)]
				rounds := map[kernel.Time][]string{} // the syncs of each round; read by key
				var times []kernel.Time
				for _, r := range c.recs(n, "sync") {
					if r.At <= vt(crashAt) {
						continue
					}
					if _, ok := rounds[r.At]; !ok {
						times = append(times, r.At)
					}
					rounds[r.At] = append(rounds[r.At], attrs(r))
				}
				want := []string{"peer=" + other + " reason=periodic", "peer=n3 reason=reconnect", "peer=n4 reason=reconnect"}
				reconnecting := 0
				for _, at := range times {
					got := rounds[at]
					switch {
					case slices.Equal(got, want):
						reconnecting++
						// The sessions come back about a second after the heal; the next round heals.
						if at > vt(healAt+3*time.Second) {
							tt.Errorf("%s still reconnects at %s, 3s after the heal", n, at)
						}
					case at > vt(healAt) && len(got) == 1 && strings.HasSuffix(got[0], " reason=periodic"):
						// A periodic sync once B is up again.
					default:
						tt.Errorf("%s's syncs at %s: %q, want %q, or one periodic sync once healed", n, at, got, want)
					}
				}
				if reconnecting < 2 {
					tt.Errorf("%s sent %d reconnect rounds, want one per round from the crash to the heal", n, reconnecting)
				}
				// What n queued for B's old incarnations was dropped, not delivered.
				id := c.n[n].ID()
				if !slices.ContainsFunc(w.Sim.Records(), func(r kernel.Record) bool {
					return r.Node == id && r.Kind == "lockstep.deadletter" && attr(r, "reason") == "peer-restarted"
				}) {
					tt.Errorf("%s dropped no frame queued for B's old incarnations", n)
				}
			}
			// B learns of A from A's reconnect syncReqs only, after the heal, and sends A nothing
			// before it has heard of it.
			for _, n := range halfB {
				learnt := map[string]kernel.Time{} // read by key
				for _, r := range c.recs(n, "member") {
					peer := attr(r, "node")
					if _, ok := learnt[peer]; r.At > vt(restartAt) && slices.Contains(halfA, peer) && !ok {
						learnt[peer] = r.At
						if r.At < vt(healAt) {
							tt.Errorf("%s learnt of %s at %s, before the heal", n, peer, r.At)
						}
					}
				}
				for _, r := range c.recs(n, "sync") {
					peer := attr(r, "peer")
					if at, ok := learnt[peer]; r.At > vt(restartAt) && slices.Contains(halfA, peer) && (!ok || r.At < at) {
						tt.Errorf("%s sent %s a sync at %s before it knew of it", n, peer, r.At)
					}
				}
			}
			// The first news of A in B, on either node, is a syncReq: one of A's reconnect syncs.
			var first *kernel.Record
			for _, n := range halfB {
				for _, r := range c.recs(n, "member") {
					if r.At > vt(restartAt) && slices.Contains(halfA, attr(r, "node")) && (first == nil || r.At < first.At) {
						first = &r
					}
				}
			}
			if first == nil || attr(*first, "source") != "syncreq" {
				tt.Errorf("half B's first news of half A is not a syncReq: %v", first)
			}
		})
	})
}

// vt returns the virtual time d after the start.
func vt(d time.Duration) kernel.Time { return kernel.Time(0).Add(d) }

// The helpers of the simulated membership tests.

// simRemote are remoting settings for short runs: a lost HELLO costs DialTimeout, which is 5s by
// default.
var simRemote = remote.Config{DialTimeout: 100 * time.Millisecond, RedialMin: 10 * time.Millisecond, RedialMax: 100 * time.Millisecond}

// simOpts returns Options that run seeds seeds of d each, keep every record and use simRemote.
func simOpts(seeds int, d time.Duration) sim.Options {
	return sim.Options{
		Options: faultline.Options{Seeds: seeds, Duration: d, Trace: kernel.TraceConfig{Level: kernel.TraceFull}},
		Remote:  simRemote,
	}
}

// simConfig returns a Config with seeds, a SyncInterval of 1s and a JoinRetryInterval of 500ms;
// every other field is its default.
func simConfig(seeds ...string) Config {
	return Config{Seeds: seeds, SyncInterval: time.Second, JoinRetryInterval: 500 * time.Millisecond}
}

// simCluster keeps the Membership of every node of a simulation.
type simCluster struct {
	s  *sim.Sim
	ms map[string]*Membership // the current boot's Membership, by node name; read by key
	n  map[string]*kernel.Node
}

func newSimCluster(s *sim.Sim) *simCluster {
	return &simCluster{s: s, ms: map[string]*Membership{}, n: map[string]*kernel.Node{}}
}

// node adds node name, which joins with cfg at every boot. Join's error fails the seed.
func (c *simCluster) node(name string, cfg Config) {
	c.n[name] = c.s.Node(name, func(sys *lockstep.System) {
		m, err := Join(sys, cfg)
		if err != nil {
			panic(err)
		}
		c.ms[name] = m
	})
}

// view returns node name's current view.
func (c *simCluster) view(name string) *View { return c.ms[name].View() }

// id returns the node ID (System.Incarnation) of node name's current boot.
func (c *simCluster) id(name string) uint32 { return c.s.System(name).Incarnation() }

// recs returns node name's records of kind (without the lockstep.cluster. prefix), in order.
func (c *simCluster) recs(name, kind string) []kernel.Record {
	id := c.n[name].ID()
	var out []kernel.Record
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && r.Kind == recordPrefix+kind {
			out = append(out, r)
		}
	}
	return out
}

// attrs returns r's attributes as "k=v k=v ...", in order.
func attrs(r kernel.Record) string {
	parts := make([]string, len(r.Attrs))
	for i, a := range r.Attrs {
		parts[i] = a.Key + "=" + a.Value
	}
	return strings.Join(parts, " ")
}

// attr returns the value of r's attribute key, or "".
func attr(r kernel.Record, key string) string {
	i := slices.IndexFunc(r.Attrs, func(a kernel.Attr) bool { return a.Key == key })
	if i < 0 {
		return ""
	}
	return r.Attrs[i].Value
}

// describe returns recs as lines "<at> <kind> <attrs>" for failure messages.
func describe(recs []kernel.Record) string {
	var b strings.Builder
	for _, r := range recs {
		fmt.Fprintf(&b, "\n  %s %s %s", r.At, r.Kind, attrs(r))
	}
	return b.String()
}

// alive returns the Member of node name with node ID id, incarnation 0 and state Alive.
func alive(name string, id uint32) Member { return Member{Node: name, ID: id, State: Alive} }

// expectView fails tt unless v holds exactly want (sorted by node) and its Self is want's entry
// of self.
func expectView(tt testing.TB, name string, v *View, want []Member) {
	tt.Helper()
	if !slices.Equal(v.Members, want) {
		tt.Errorf("%s's view %d has members %v, want %v", name, v.Version, v.Members, want)
	}
	if i := slices.IndexFunc(want, func(m Member) bool { return m.Node == name }); i < 0 || v.Self != want[i] {
		tt.Errorf("%s's view %d has Self %v", name, v.Version, v.Self)
	}
}

// uitoa returns id in decimal.
func uitoa(id uint32) string { return strconv.FormatUint(uint64(id), 10) }
