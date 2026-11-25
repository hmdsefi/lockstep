// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// The grain properties of VAC-800 and VAC-810 (3.4-08). Each seed draws a cluster of minNodes to
// maxNodes nodes, all hosting the kind counter of grains_sim_test.go, and sends grain traffic to
// propKeys keys from several nodes throughout the run. AT-VAC-21 checks single activation under
// agreement after every event of a run with random faults; AT-VAC-22 checks from the trace that the
// activation spans of a key on different nodes never overlap while nodes join one at a time;
// AT-VAC-23 runs both workloads twice per seed and checks that the hosts draw nothing.

const (
	// propKeys is the number of keys the traffic goes to, "k0" to "k<propKeys-1>".
	propKeys = 40
	// grainSeeds is the default number of seeds of AT-VAC-21 and AT-VAC-22; FAULTLINE_SEEDS
	// overrides it.
	grainSeeds = 30
	// trafficStep is the virtual time between two rounds of traffic.
	trafficStep = 50 * time.Millisecond
	// propIdle and propHandoff are the counter kind's IdleTimeout and HandoffTimeout: short, so
	// that activations come and go and acquisitions time out within a run.
	propIdle    = 2 * time.Second
	propHandoff = time.Second
	// faultyPhase is the length of the fault phase of AT-VAC-21's workload; the recovery window
	// follows it.
	faultyPhase = 40 * time.Second
)

// propOpts are the options of counter in the property tests.
var propOpts = []KindOption{WithIdleTimeout(propIdle), WithHandoffTimeout(propHandoff)}

// traffic sends grain messages to counter keys from the live nodes of a simulation.
type traffic struct {
	s     *sim.Sim
	c     *simCluster
	names []string
	r     *rand.Rand
	next  int // the N of the last message sent
	sent  int // the messages Send accepted
}

// round sends n messages, each from a random live node to a random key, numbered in send order.
// Send's errors of a node that has not joined, of a key without an owner and of a full mailbox are
// expected under faults; any other fails the seed.
func (tf *traffic) round(n int) {
	tt := tf.s.World().T()
	for range n {
		node := tf.names[tf.r.IntN(len(tf.names))]
		key := "k" + strconv.Itoa(tf.r.IntN(propKeys))
		m := tf.c.live(node)
		if m == nil {
			continue
		}
		tf.next++
		err := GrainOf[cmsg](m, "counter", key).Send(m.sys, cmsg{N: tf.next})
		switch {
		case err == nil:
			tf.sent++
		case errors.Is(err, ErrNotJoined), errors.Is(err, ErrNoOwner), errors.Is(err, lockstep.ErrMailboxFull):
		default:
			tt.Fatalf("send to counter/%s from %s: %v", key, node, err)
		}
	}
}

// runFor runs the simulation for d, a round of n messages every trafficStep.
func (tf *traffic) runFor(d time.Duration, n int) {
	w := tf.s.World()
	end := w.Sim.Now().Add(d)
	for w.Sim.Now() < end {
		w.RunFor(min(trafficStep, end.Sub(w.Sim.Now())))
		tf.round(n)
	}
}

// agreement computes VAC §3's agreement from the ground truth of a simulation.
type agreement struct {
	tr *truth

	checks int // the checks made
	agreed int // the checks at which agreement held
	busy   int // the checks at which agreement held and some key was active
}

// upJoined returns the up joined nodes (VAC §3): the live processes whose membership has joined
// and has not left, with their Membership.
func (a *agreement) upJoined() ([]proc, []*Membership) {
	var ps []proc
	var ms []*Membership
	for _, p := range a.tr.live() {
		m := a.tr.c.live(p.node)
		if m == nil || m.sys != p.sys {
			panic(fmt.Sprintf("cluster test: %s's Membership is not the one of its live process %d", p.node, p.id))
		}
		if m.joined.Load() && !m.left.Load() {
			ps, ms = append(ps, p), append(ms, m)
		}
	}
	return ps, ms
}

// holds reports whether agreement holds among the up joined nodes ms: every one is settled, its
// host of counter has applied its node's latest view, and every one lists the same (Node, ID)
// pairs as up in its view. It returns "" when it holds, else why not.
func holds(ps []proc, ms []*Membership) string {
	for i, m := range ms {
		v := m.View()
		if hi, _ := m.Host("counter"); hi.Version != v.Version {
			return fmt.Sprintf("%s's host applied view %d of %d", ps[i].node, hi.Version, v.Version)
		}
		if i > 0 && !sameUp(v, ms[0].View()) {
			return fmt.Sprintf("%s has up %v, %s has up %v", ps[i].node, upIDs(v), ps[0].node, upIDs(ms[0].View()))
		}
	}
	return ""
}

// sameUp reports whether a and b have the same up members with the same IDs.
func sameUp(a, b *View) bool {
	i, j := 0, 0
	for {
		for i < len(a.Members) && !a.Members[i].State.Up() {
			i++
		}
		for j < len(b.Members) && !b.Members[j].State.Up() {
			j++
		}
		if i == len(a.Members) || j == len(b.Members) {
			return i == len(a.Members) && j == len(b.Members)
		}
		if a.Members[i].Node != b.Members[j].Node || a.Members[i].ID != b.Members[j].ID {
			return false
		}
		i, j = i+1, j+1
	}
}

// single checks VAC-800 now: when agreement holds, no key is in the HostInfo.Active of counter of
// two up joined nodes. A process that is live but not an up joined node has no activation: it has
// not joined, so its host holds every message (VAC-310 rule 1).
func (a *agreement) single() error {
	a.tr.observe()
	a.checks++
	ps, ms := a.upJoined()
	if len(ms) == 0 || holds(ps, ms) != "" {
		return nil
	}
	a.agreed++
	var owner map[string]int // the up joined node, by index, that has the key active
	for i, m := range ms {
		hi, _ := m.Host("counter")
		for _, key := range hi.Active {
			if owner == nil {
				owner = map[string]int{}
			}
			if j, ok := owner[key]; ok {
				return fmt.Errorf("under agreement on view versions %s, %s is active on %s:%d and %s:%d (up %v)",
					versions(ps, ms), key, ps[j].node, ps[j].id, ps[i].node, ps[i].id, upIDs(m.View()))
			}
			owner[key] = i
		}
	}
	if owner != nil {
		a.busy++
	}
	return nil
}

// versions describes the view version of each up joined node, for failure messages.
func versions(ps []proc, ms []*Membership) string {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = ps[i].node + "=" + strconv.FormatUint(m.View().Version, 10)
	}
	return strings.Join(parts, " ")
}

// faultyGrains is the workload of AT-VAC-21: the nodes of a drawn cluster seed each other and
// host counter; random partitions, isolations, pauses and crashes (each crashed node restarted
// with a new ID after a while) run until the recovery window, and from time to time the host of a
// random live node fails (it restarts empty, VAC-450). Grain traffic runs from start to end. The
// invariant checks VAC-800 after every event. It returns the processes of the run and the number
// of host failures injected; the run is over when it returns.
func faultyGrains(s *sim.Sim) ([]proc, int) {
	w, tt := s.World(), s.World().T()
	names := propCluster(w)
	c := newSimCluster(s)
	l := &counterLog{}
	for _, n := range names {
		c.counterNode(n, l, names, propOpts...)
	}
	w.Plan(&fault.Random{Quiet: w.End().Sub(kernel.Time(0).Add(faultyPhase)), Rules: []fault.Rule{
		{Kind: fault.KindPartition, Every: 6 * time.Second, MinFor: 500 * time.Millisecond, MaxFor: 12 * time.Second},
		{Kind: fault.KindIsolate, Every: 9 * time.Second, MinFor: 500 * time.Millisecond, MaxFor: 10 * time.Second},
		{Kind: fault.KindPause, Every: 9 * time.Second, MinFor: 200 * time.Millisecond, MaxFor: 10 * time.Second},
		{Kind: fault.KindCrash, Every: 7 * time.Second, MinFor: 200 * time.Millisecond, MaxFor: 15 * time.Second},
	}})
	tr := newTruth(c, names)
	a := &agreement{tr: tr}
	w.Invariant("grain/single-activation", a.single)

	tf := &traffic{s: s, c: c, names: names, r: w.Rand("grain/traffic")}
	r := w.Rand("grain/host-fail")
	fails := 0
	for w.Sim.Now() < w.End() {
		tf.runFor(min(trafficStep, w.End().Sub(w.Sim.Now())), 2)
		if r.IntN(200) != 0 || w.Sim.Now() >= kernel.Time(0).Add(faultyPhase) {
			continue
		}
		name := names[r.IntN(len(names))]
		if s.System(name) == nil {
			continue
		}
		err := s.InjectFail(hostAt(name, "counter").Address(), errors.New("injected"))
		switch {
		case err == nil:
			fails++
		case errors.Is(err, lockstep.ErrNotFound), errors.Is(err, lockstep.ErrStopped):
		default:
			tt.Fatal(err)
		}
	}

	// The recovery window outlasts R(N): every live node has joined and agreement holds, with
	// the traffic's keys active somewhere.
	tr.observe()
	if err := tr.converged(); err != nil {
		tt.Errorf("not converged at the end: %v", err)
	}
	ps, ms := a.upJoined()
	if len(ps) != len(tr.live()) {
		tt.Errorf("%d of the %d live nodes are up joined nodes at the end", len(ps), len(tr.live()))
	} else if why := holds(ps, ms); why != "" {
		tt.Errorf("no agreement at the end: %s", why)
	}
	if a.busy == 0 || tf.sent == 0 {
		tt.Errorf("agreement held with active keys at %d of %d checks (%d in all), after %d sends: want some", a.busy, a.checks, a.agreed, tf.sent)
	}
	return tr.procs, fails
}

// joiningGrains is the workload of AT-VAC-22: the nodes of a drawn cluster start one at a time,
// each seeding one random node started before it, with no fault. Each waits until every node started so
// far has joined, the views have exactly them up and every host has applied its node's view, and
// up to 2s longer, before the next starts. Grain traffic runs from the first node's start until 5s
// after the last node joined. It returns the cluster and the deliveries; the run is over when it
// returns.
func joiningGrains(s *sim.Sim) (*simCluster, *counterLog) {
	w, tt := s.World(), s.World().T()
	names := propCluster(w)
	c := newSimCluster(s)
	l := &counterLog{}
	tf := &traffic{s: s, c: c, r: w.Rand("grain/traffic")}
	r := w.Rand("grain/joins")
	for i, n := range names {
		// One seed started before: a join syncs with every seed at once, so with more seeds every
		// node would know the joiner before it asks them for a key.
		seeds := []string{n}
		if i > 0 {
			seeds = append(seeds, names[r.IntN(i)])
		}
		c.counterNode(n, l, seeds, propOpts...)
		tf.names = names[:i+1]
		deadline := w.Sim.Now().Add(20 * time.Second)
		for {
			err := c.agreed(tf.names)
			if err == nil {
				break
			}
			if w.Sim.Now() >= deadline {
				tt.Fatalf("no agreement within 20s of %s's start: %v", n, err)
			}
			tf.runFor(trafficStep, 3)
		}
		tf.runFor(time.Duration(r.Int64N(int64(2*time.Second))), 3)
	}
	tf.runFor(5*time.Second, 3)
	w.RunFor(w.End().Sub(w.Sim.Now()))
	if len(l.got) == 0 || tf.sent == 0 {
		tt.Errorf("%d deliveries of %d messages sent, want some", len(l.got), tf.sent)
	}
	return c, l
}

// joiningDuration bounds joiningGrains: per node at most 20s to agree and 2s more, then 5s.
const joiningDuration = maxNodes*22*time.Second + 10*time.Second

// span is the activation of a key on a node, from its lockstep.grain.activate record to its
// lockstep.grain.stopped record, as indexes in the trace.
type span struct {
	node       string
	start, end int // end is -1 while the activation runs
}

// spans returns the activation spans of counter's keys in c's trace, by escaped key. It fails
// tt when a node activates a key that is already active there, or records stopped for a key that
// is not.
func spans(tt testing.TB, c *simCluster) map[string][]span {
	nodeOf := map[kernel.NodeID]string{}
	for name, n := range c.n { //lockstep:maporder builds a lookup table; order does not matter
		nodeOf[n.ID()] = name
	}
	out := map[string][]span{}
	for i, rec := range c.s.World().Sim.Records() {
		if rec.Kind != grainPrefix+"activate" && rec.Kind != grainPrefix+"stopped" || attr(rec, "kind") != "counter" {
			continue
		}
		node, key := nodeOf[rec.Node], attr(rec, "key")
		ss := out[key]
		open := slices.IndexFunc(ss, func(sp span) bool { return sp.node == node && sp.end < 0 })
		switch {
		case rec.Kind == grainPrefix+"activate" && open >= 0:
			tt.Fatalf("%s activated %s at %s while its activation from record %d runs", node, key, rec.At, ss[open].start)
		case rec.Kind == grainPrefix+"activate":
			out[key] = append(ss, span{node: node, start: i, end: -1})
		case open < 0:
			tt.Fatalf("%s recorded stopped for %s at %s without an activation", node, key, rec.At)
		default:
			ss[open].end = i
		}
	}
	return out
}

// hostNode returns the node of addr when addr names the host of counter, from its string form
// "lockstep://<node>/system/grains/counter#<uid>".
func hostNode(addr string) (string, bool) {
	rest, ok := strings.CutPrefix(addr, "lockstep://")
	node, path, _ := strings.Cut(rest, "/")
	path, _, _ = strings.Cut(path, "#")
	return node, ok && "/"+path == grainsPath+"/counter"
}

// handoffReqs are the handoff requests one host got and has not handled yet, in mailbox order.
type handoffReqs []struct{ key, from string }

// ackOrder checks from the trace that no host acks a handoff request while its activation of the
// requested key runs (VAC-510): a request for a key whose activation runs on the peer is acked
// only in the turn that records the key's lockstep.grain.stopped. An activation stops in no
// virtual time, so an ack sent at the request instead would still reach the requester after the
// stop, and the spans would not overlap; this check catches it.
//
// The trace names no key in an ack, so the check follows the requests: the requester's
// lockstep.grain.handoff record names the key and the peers it sent requests to, a peer's
// lockstep.deliver of a cluster.handoffReq from the requester's host takes the next request on
// that link (links and mailboxes are FIFO), and a turn of the peer's host that consumed a
// cluster.handoffReq (simulation turns take one user message) handles the next request its
// mailbox took. In each turn of a host, the acks to a node must not outnumber the requests from
// it, handled in the turn, for keys with no running activation there, plus the acks the turn's
// stopped records released to it. It returns the number of requests that found the key's
// activation running. It needs a run without faults: a lost request would misalign the queues.
func ackOrder(c *simCluster) (running int, err error) {
	nodeOf := map[kernel.NodeID]string{}
	for name, n := range c.n { //lockstep:maporder builds a lookup table; order does not matter
		nodeOf[n.ID()] = name
	}
	type pair struct{ a, b string }
	sent := map[pair][]string{}        // by (requester, peer): the keys requested, not yet delivered
	queued := map[string]handoffReqs{} // by peer: the requests its host's mailbox took
	open := map[pair]bool{}            // by (node, escaped key): the activation runs
	owed := map[pair][]string{}        // by (node, escaped key): the requesters owed an ack at the stop
	acks := map[string][]string{}      // by node: the nodes its host acked in the current turn
	allowed := map[string][]string{}   // by node: the nodes the current turn may ack, once each
	for _, rec := range c.s.World().Sim.Records() {
		node := nodeOf[rec.Node]
		switch rec.Kind {
		case grainPrefix + "activate":
			open[pair{node, attr(rec, "key")}] = true
		case grainPrefix + "stopped":
			k := pair{node, attr(rec, "key")}
			delete(open, k)
			allowed[node] = append(allowed[node], owed[k]...)
			delete(owed, k)
		case grainPrefix + "handoff":
			for _, p := range strings.Split(attr(rec, "peers"), ",") {
				sent[pair{node, p}] = append(sent[pair{node, p}], attr(rec, "key"))
			}
		case "lockstep.deliver":
			from, ok1 := hostNode(attr(rec, "from"))
			to, ok2 := hostNode(attr(rec, "to"))
			if !ok1 || !ok2 || attr(rec, "msg") != "cluster.handoffReq" {
				continue
			}
			q := sent[pair{from, to}]
			if len(q) == 0 {
				return running, fmt.Errorf("%s's host took a handoff request from %s at %s that no handoff record announced", to, from, rec.At)
			}
			sent[pair{from, to}] = q[1:]
			queued[to] = append(queued[to], struct{ key, from string }{q[0], from})
		case "lockstep.send":
			from, ok1 := hostNode(attr(rec, "from"))
			to, ok2 := hostNode(attr(rec, "to"))
			if ok1 && ok2 && attr(rec, "msg") == "cluster.handoffAck" {
				acks[from] = append(acks[from], to)
			}
		case "lockstep.turn":
			if h, ok := hostNode(attr(rec, "actor")); ok && h == node && attr(rec, "msg") == "cluster.handoffReq" {
				q := queued[node]
				if len(q) == 0 {
					return running, fmt.Errorf("%s's host handled a handoff request at %s that its mailbox never took", node, rec.At)
				}
				r := q[0]
				queued[node] = q[1:]
				if k := (pair{node, r.key}); open[k] {
					running++
					owed[k] = append(owed[k], r.from)
				} else {
					allowed[node] = append(allowed[node], r.from)
				}
			}
			for _, to := range acks[node] {
				i := slices.Index(allowed[node], to)
				if i < 0 {
					return running, fmt.Errorf("%s's host acked %s at %s while the requested activation still ran there (%s)",
						node, to, rec.At, attrs(rec))
				}
				allowed[node] = slices.Delete(allowed[node], i, i+1)
			}
			acks[node], allowed[node] = acks[node][:0], allowed[node][:0]
		}
	}
	for k, q := range sent { //lockstep:maporder any leftover is an error
		if len(q) > 0 {
			return running, fmt.Errorf("%d handoff requests from %s to %s never reached its host", len(q), k.a, k.b)
		}
	}
	return running, nil
}

// overlap returns the first pair of spans of one key on different nodes that overlap, as an
// error, or nil. A span that is still running at the end lasts forever.
func overlap(c *simCluster, all map[string][]span) error {
	recs := c.s.World().Sim.Records()
	keys := make([]string, 0, len(all))
	for k := range all { //lockstep:maporder sorted below
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, key := range keys {
		ss := all[key] // in start order
		for i, b := range ss {
			for _, a := range ss[:i] {
				if a.node != b.node && (a.end < 0 || a.end > b.start) {
					end := "still running"
					if a.end >= 0 {
						end = "stopped at " + recs[a.end].At.String()
					}
					return fmt.Errorf("%s: %s activated it at %s (%s) while %s's activation from %s runs (%s)",
						key, b.node, recs[b.start].At, attrs(recs[b.start]), a.node, recs[a.start].At, end)
				}
			}
		}
	}
	return nil
}

// AT-VAC-21, VAC-800: over grainSeeds seeds of random partitions, isolations, pauses, crashes
// with restarts under new IDs and host failures, with grain traffic to propKeys keys from every
// node, an invariant checks after every event: whenever agreement holds (VAC §3: every up joined
// node is settled, and all of them list the same (Node, ID) pairs as up), no key is in the
// HostInfo.Active of counter of two up joined nodes. An up joined node is a live process, by the
// simulator's ground truth, whose membership has joined and has not left. At the end, after a
// recovery window longer than R(N), agreement holds, and it held with active keys during the run.
func TestATVAC21SingleActivation(t *testing.T) {
	worst := boundsOf(simConfig(), maxNodes)
	sim.Simulate(t, simOpts(grainSeeds, faultyPhase+worst.converge+5*time.Second), func(s *sim.Sim) {
		faultyGrains(s)
	})
}

// AT-VAC-22, VAC-810: nodes join one at a time with no fault, each after the views agreed on the
// ones before and every host applied them, with grain traffic throughout. From the trace's
// lockstep.grain.activate and lockstep.grain.stopped records of counter, the activation spans of
// a key on different nodes never overlap: a node activates a key only after the activation on any
// other node stopped. An activation stops in no virtual time, so the spans would not overlap even
// if the old owner acked before its activation stopped; ackOrder checks the acks' order on the old
// owner as well. Keys change owners in every seed, some acquisitions end acked, no message reaches
// its grain twice, and across the seeds some requests find the key's activation running.
func TestATVAC22OrderedHandover(t *testing.T) {
	seeds, running := 0, 0
	sim.Simulate(t, simOpts(grainSeeds, joiningDuration), func(s *sim.Sim) {
		tt := s.World().T()
		c, l := joiningGrains(s)
		seeds++
		l.atMostOnce(tt)
		all := spans(tt, c)
		if err := overlap(c, all); err != nil {
			tt.Error(err)
		}
		n, err := ackOrder(c)
		if err != nil {
			tt.Error(err)
		}
		running += n
		moved := 0
		for _, ss := range all { //lockstep:maporder counts only
			if slices.ContainsFunc(ss, func(sp span) bool { return sp.node != ss[0].node }) {
				moved++
			}
		}
		acked := 0
		for _, rec := range s.World().Sim.Records() {
			if rec.Kind == grainPrefix+"activate" && attr(rec, "kind") == "counter" && attr(rec, "handoff") == handoffAcked {
				acked++
			}
		}
		if moved == 0 || acked == 0 {
			tt.Errorf("%d keys moved between nodes and %d activations were acked, want some of each", moved, acked)
		}
	})
	if seeds >= 10 && running == 0 {
		t.Errorf("no handoff request of %d runs found the key's activation running, want some", seeds)
	}
}

// drawSeeds is the number of seeds of each workload of AT-VAC-23.
const drawSeeds = 8

// AT-VAC-23, §6: both workloads above, run twice per seed with faultline's determinism check
// whatever LOCKSTEP_CHECK_DETERMINISM says, give equal trace hashes. At the end of each run, no
// incarnation of /system/grains or of a host, on any process of any node, took a value from its
// random stream: each stream's next value is the first value of a fresh stream with that label.
// As a control, the first incarnation of the membership actor of the first process seen, which
// draws its probe order (CLU §6), has drawn from its stream.
func TestATVAC23Determinism(t *testing.T) {
	worst := boundsOf(simConfig(), maxNodes)
	run := func(t *testing.T, d time.Duration, body func(s *sim.Sim) ([]proc, int)) {
		opts := simOpts(drawSeeds, d)
		opts.CheckDeterminism = true
		sim.Simulate(t, opts, func(s *sim.Sim) {
			procs, fails := body(s)
			if err := noDraws(s, procs, fails+1); err != nil {
				s.World().T().Error(err)
			}
		})
	}
	t.Run("faults", func(t *testing.T) {
		run(t, faultyPhase+worst.converge+5*time.Second, faultyGrains)
	})
	t.Run("joins", func(t *testing.T) {
		run(t, joiningDuration, func(s *sim.Sim) ([]proc, int) {
			c, _ := joiningGrains(s)
			names := slices.Sorted(maps.Keys(c.n))
			procs := make([]proc, len(names))
			for i, n := range names {
				procs[i] = proc{node: n, id: c.id(n)}
			}
			return procs, 0
		})
	})
}

// noDraws returns an error when an incarnation of /system/grains or /system/grains/counter of one
// of the processes procs, numbered 1 to incs, took a value from its random stream, or when the
// control stream of the membership actor's first incarnation of the first process did not.
func noDraws(s *sim.Sim, procs []proc, incs int) error {
	w := s.World()
	fresh := kernel.New(kernel.Config{Seed: w.Sim.Seed()})
	drawn := func(label string) bool { return w.Sim.Rand(label).Uint64() != fresh.Rand(label).Uint64() }
	label := func(p proc, path string, inc int) string {
		return "lockstep/" + p.node + "/" + uitoa(p.id) + path + "/" + strconv.Itoa(inc)
	}
	if len(procs) == 0 {
		return errors.New("no process seen")
	}
	if l := label(procs[0], actorPath, 1); !drawn(l) {
		return fmt.Errorf("the control stream %s shows no draw: the labels or the seed are wrong", l)
	}
	for _, p := range procs {
		for _, path := range []string{grainsPath, grainsPath + "/counter"} {
			for inc := 1; inc <= incs; inc++ {
				if l := label(p, path, inc); drawn(l) {
					return fmt.Errorf("%s drew from its random stream", l)
				}
			}
		}
	}
	return nil
}
