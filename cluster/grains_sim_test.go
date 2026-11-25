// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/sim"
)

// The simulation tests of grains across nodes (3.4-07): remote owners, client-only nodes, crashes,
// restarts, unreachable peers, joins, yields, disagreeing views, asks, leaves, HostInfo, host
// failures and the order of the views the hosts apply.
//
// Every node hosts the kind counter (except client-only ones), whose activation counts its
// messages and answers an ask with "<node>:<count>": the node it runs on and the number of
// messages it got, so a test sees which node answered. Every delivery is logged with its node, so
// a test checks that each message reaches the grain exactly once. The keys come from VAC §10's
// rankings over n1 ... n4 for counter:
//
//	k0 → n4, n1, n2, n3
//	k3 → n1, n3, n4, n2
//	k8 → n4, n3, n1, n2
//
// and a ranking over fewer nodes is the same order without the others.

// wireCmsg is the wire name of cmsg in counterRegistry.
const wireCmsg = "cluster.test.cmsg"

// cmsg is the message type of counter: N names the message (unique in a test), and a non-zero
// Reply is answered with "<node>:<count>".
type cmsg struct {
	N     int
	Reply lockstep.ReplyTo[string]
}

// delivery is one message an activation of counter got.
type delivery struct {
	at   kernel.Time
	node string
	key  string
	n    int
}

// counterLog is what the activations of counter of one simulation run got, in turn order. The
// simulation runs one turn at a time, so the activations share it without a lock.
type counterLog struct{ got []delivery }

// of returns the N of the messages key got on node, in order; every node when node is "".
func (l *counterLog) of(node, key string) []int {
	var out []int
	for _, d := range l.got {
		if d.key == key && (node == "" || d.node == node) {
			out = append(out, d.n)
		}
	}
	return out
}

// nodes returns the nodes on which key got a message, sorted, without duplicates.
func (l *counterLog) nodes(key string) []string {
	var out []string
	for _, d := range l.got {
		if d.key == key {
			out = append(out, d.node)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// atMostOnce fails tt when a message reached its grain twice.
func (l *counterLog) atMostOnce(tt testing.TB) {
	tt.Helper()
	seen := map[string]bool{}
	for _, d := range l.got {
		id := d.key + "/" + strconv.Itoa(d.n)
		if seen[id] {
			tt.Errorf("%s got message %d twice (again on %s at %s)", d.key, d.n, d.node, d.at)
		}
		seen[id] = true
	}
}

// counterKind returns the kind counter of node, whose activations log to l with the time now
// returns.
func counterKind(node string, now func() kernel.Time, l *counterLog, opts ...KindOption) Kind {
	return NewKind("counter", func(key string) lockstep.Actor[cmsg] {
		count := 0
		return actorFunc[cmsg](func(ctx *lockstep.Context[cmsg], m cmsg) {
			count++
			l.got = append(l.got, delivery{at: now(), node: node, key: key, n: m.N})
			if m.Reply != (lockstep.ReplyTo[string]{}) {
				m.Reply.Send(ctx, node+":"+strconv.Itoa(count))
			}
		})
	}, opts...)
}

// counterRegistry returns a registry that holds cmsg.
func counterRegistry() *codec.Registry {
	r := codec.NewRegistry()
	codec.RegisterIn[cmsg](r, wireCmsg)
	return r
}

// counterNode adds node name, which joins through seeds and hosts counter, logging to l, with
// opts and counterRegistry.
func (c *simCluster) counterNode(name string, l *counterLog, seeds []string, opts ...KindOption) {
	cfg := simConfig(seeds...)
	cfg.Kinds = []Kind{counterKind(name, c.s.World().Sim.Now, l, opts...)}
	cfg.Registry = counterRegistry()
	c.node(name, cfg)
}

// counterCluster adds the nodes names, which seed each other and host counter, and runs until
// they agree.
func counterCluster(s *sim.Sim, l *counterLog, names []string, opts ...KindOption) *simCluster {
	c := newSimCluster(s)
	for _, n := range names {
		c.counterNode(n, l, names, opts...)
	}
	c.settle(s.World().T(), names...)
	return c
}

// live returns node name's Membership when it is the one of the node's live process, else nil.
func (c *simCluster) live(name string) *Membership {
	m := c.ms[name]
	if m == nil || m.sys != c.s.System(name) {
		return nil
	}
	return m
}

// agreed returns nil when the nodes names agree (VAC §3): each has a live process that has joined
// and whose hosts are settled, and each view has as up members exactly names, with the IDs of their
// live processes. Otherwise it says what differs.
func (c *simCluster) agreed(names []string) error {
	names = slices.Sorted(slices.Values(names))
	want := make([]string, len(names))
	for i, n := range names {
		if c.live(n) == nil {
			return fmt.Errorf("%s has no live process", n)
		}
		want[i] = n + ":" + uitoa(c.id(n))
	}
	for _, n := range names {
		m := c.live(n)
		v := m.View()
		if !m.joined.Load() {
			return fmt.Errorf("%s has not joined", n)
		}
		if got := upIDs(v); !slices.Equal(got, want) {
			return fmt.Errorf("%s's view %d has up %v, want %v", n, v.Version, got, want)
		}
		if info, ok := m.Host("counter"); ok && info.Version != v.Version {
			return fmt.Errorf("%s's host applied view %d, not %d", n, info.Version, v.Version)
		}
	}
	return nil
}

// settle runs until the nodes names agree, and fails tt when they do not before the end of the
// run.
func (c *simCluster) settle(tt testing.TB, names ...string) {
	tt.Helper()
	w := c.s.World()
	for {
		err := c.agreed(names)
		if err == nil {
			return
		}
		if w.Sim.Now().Add(10*time.Millisecond) > w.End() {
			tt.Fatalf("no agreement by the end of the run: %v", err)
		}
		w.RunFor(10 * time.Millisecond)
	}
}

// await runs until cond holds, in steps of step, and fails tt with what when it does not hold
// within limit.
func await(tt testing.TB, w interface{ RunFor(time.Duration) }, step, limit time.Duration, what string, cond func() bool) {
	tt.Helper()
	for waited := time.Duration(0); !cond(); waited += step {
		if waited >= limit {
			tt.Fatalf("%s: not within %s", what, limit)
		}
		w.RunFor(step)
	}
}

// joined reports whether node name has a live process that has joined.
func (c *simCluster) joined(name string) bool {
	m := c.live(name)
	return m != nil && m.joined.Load()
}

// send sends message n to the grain key from node name, failing tt on an error.
func (c *simCluster) send(tt testing.TB, name, key string, n int) {
	tt.Helper()
	if err := GrainOf[cmsg](c.live(name), "counter", key).Send(c.s.System(name), cmsg{N: n}); err != nil {
		tt.Fatalf("send %d to %s from %s: %v", n, key, name, err)
	}
}

// ask asks the grain key from node name with message n, runs until the reply resolves and returns
// the answer, failing tt on an error.
func (c *simCluster) ask(tt testing.TB, name, key string, n int) string {
	tt.Helper()
	r := Ask(c.s.System(name), GrainOf[cmsg](c.live(name), "counter", key),
		func(rt lockstep.ReplyTo[string]) cmsg { return cmsg{N: n, Reply: rt} }, lockstep.WithTimeout(5*time.Second))
	await(tt, c.s.World(), 10*time.Millisecond, 6*time.Second, "the ask of "+key+" from "+name, func() bool { return askResolved(r) })
	v, err := awaitResolved(tt, r)
	if err != nil {
		tt.Fatalf("ask %s from %s: %v", key, name, err)
	}
	return v
}

// expectAnswer fails tt unless the answer of an ask is "<node>:<count>".
func expectAnswer(tt testing.TB, what, got, node string, count int) {
	tt.Helper()
	if want := node + ":" + strconv.Itoa(count); got != want {
		tt.Errorf("%s answered %q, want %q", what, got, want)
	}
}

// timedRec is a lockstep.grain record of counter as "<kind> <attrs>", with its time and its
// sequence number in the trace.
type timedRec struct {
	at  kernel.Time
	seq uint64
	rec string
}

// keyLog returns node name's lockstep.grain records of counter for key from since on, in order.
func (c *simCluster) keyLog(name, key string, since kernel.Time) []timedRec {
	id := c.n[name].ID()
	var out []timedRec
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && r.At >= since && strings.HasPrefix(r.Kind, grainPrefix) && attr(r, "kind") == "counter" &&
			attr(r, "key") == key {
			out = append(out, timedRec{r.At, r.Seq, strings.TrimPrefix(r.Kind, grainPrefix) + " " + attrs(r)})
		}
	}
	return out
}

// recs returns the records of l.
func recsOf(l []timedRec) []string {
	out := make([]string, len(l))
	for i, r := range l {
		out[i] = r.rec
	}
	return out
}

// timeOf returns the time of the first record of l that is rec, and fails tt when there is none.
func timeOf(tt testing.TB, l []timedRec, rec string) kernel.Time {
	tt.Helper()
	return find(tt, l, rec).at
}

// find returns the first record of l that is rec, and fails tt when there is none.
func find(tt testing.TB, l []timedRec, rec string) timedRec {
	tt.Helper()
	for _, r := range l {
		if r.rec == rec {
			return r
		}
	}
	tt.Fatalf("no record %q in %q", rec, recsOf(l))
	return timedRec{}
}

// ackSends returns the lockstep.send records of the handoffAcks that the host of counter on node
// from sent to the one on node to, from since on, in order. The record does not name the key.
func (c *simCluster) ackSends(from, to string, since kernel.Time) []kernel.Record {
	id := c.n[from].ID()
	src, dst := "lockstep://"+from+grainsPath+"/counter", "lockstep://"+to+grainsPath+"/counter"
	var out []kernel.Record
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && r.At >= since && r.Kind == "lockstep.send" && attr(r, "msg") == "cluster.handoffAck" &&
			pathOf(attr(r, "from")) == src && pathOf(attr(r, "to")) == dst {
			out = append(out, r)
		}
	}
	return out
}

// expectAckAfter fails tt unless the i-th handoffAck (from 0) that from's host of counter sent to
// to's host, from since on, comes after the record stopped in the trace. A stop and an ack can
// happen at the same virtual time, and the ack crosses the network while the stop is local, so
// only the order of the records shows that the ack waited for the stop.
func (c *simCluster) expectAckAfter(tt testing.TB, from, to string, since kernel.Time, i int, stopped timedRec) {
	tt.Helper()
	acks := c.ackSends(from, to, since)
	if len(acks) <= i {
		tt.Errorf("%s sent %d handoff acks to %s, want at least %d", from, len(acks), to, i+1)
		return
	}
	if acks[i].Seq < stopped.seq {
		tt.Errorf("%s sent its handoff ack %d to %s at %s, before %q at %s", from, i, to, acks[i].At, stopped.rec, stopped.at)
	}
}

// grainKinds returns the kinds (without the prefix) of node name's lockstep.grain records.
func (c *simCluster) grainKinds(name string) []string {
	id := c.n[name].ID()
	var out []string
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && strings.HasPrefix(r.Kind, grainPrefix) {
			out = append(out, strings.TrimPrefix(r.Kind, grainPrefix))
		}
	}
	return out
}

// memberAddr returns the address of node's membership actor.
func memberAddr(node string) lockstep.Address {
	return lockstep.PIDAt[memberMsg](node, actorPath).Address()
}

// rec returns the record "<kind> kind=counter key=<key> <rest>", or without rest when it is "".
func rec(kind, key, rest string) string {
	if rest == "" {
		return kind + " kind=counter key=" + key
	}
	return kind + " kind=counter key=" + key + " " + rest
}

// AT-VAC-07, VAC-110, VAC-200, VAC-330: n2 sends three messages and an ask to k3, which n1 owns
// (k3 → n1, n3, n2 over these nodes). n1 activates k3, gets the messages in send order and
// answers. n2, the sender, keeps nothing: no activation, buffer, acquisition or forward, and its
// only grain records are its views. n1 may ask n3 (the runner-up) to hand k3 over, depending on the
// views it went through at the start (VAC-400), so the activation is handoff=none or acked.
func TestATVAC07RemoteOwner(t *testing.T) {
	sim.Simulate(t, simOpts(3, 20*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		names := []string{"n1", "n2", "n3"}
		c := counterCluster(s, l, names)
		for n := 1; n <= 3; n++ {
			c.send(tt, "n2", "k3", n)
		}
		expectAnswer(tt, "k3", c.ask(tt, "n2", "k3", 4), "n1", 4)
		w.RunFor(100 * time.Millisecond)

		expectStrings(tt, "k3's messages", intStrings(l.of("", "k3")), intStrings([]int{1, 2, 3, 4}))
		expectStrings(tt, "k3's nodes", l.nodes("k3"), []string{"n1"})
		acts := slices.DeleteFunc(recsOf(c.keyLog("n1", "k3", 0)), func(r string) bool { return !strings.HasPrefix(r, "activate ") })
		if len(acts) != 1 || (acts[0] != rec("activate", "k3", "handoff=none") && acts[0] != rec("activate", "k3", "handoff=acked")) {
			tt.Errorf("n1's activations of k3 are %q, want one with handoff=none or acked", acts)
		}
		for _, n := range []string{"n2", "n3"} {
			expectStrings(tt, n+"'s activations", slices.DeleteFunc(c.grainKinds(n), func(k string) bool { return k != "activate" }), nil)
		}
		expectStrings(tt, "n2's grain records", slices.Compact(c.grainKinds("n2")), []string{"view"})
		expectHost(tt, c, "n2", HostInfo{Kind: "counter", Version: c.view("n2").Version, Joined: true})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: c.view("n1").Version, Joined: true, Active: []string{"k3"}})
	})
}

// intStrings returns ns as decimal strings, for expectStrings.
func intStrings(ns []int) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = strconv.Itoa(n)
	}
	return out
}

// AT-VAC-08, VAC-200, VAC-300: c1 is a client-only node (no kinds) of a cluster of n1, n2 and n3.
// It joins, every view lists it without kinds, and Host is false there. It sends to and asks k0,
// k3 and k8, which n1, n1 and n3 own over the hosting nodes, and the owners answer. Nothing
// activates on c1, which has no host and makes no grain record.
func TestATVAC08ClientOnly(t *testing.T) {
	sim.Simulate(t, simOpts(3, 20*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		names := []string{"n1", "n2", "n3", "c1"}
		c := newSimCluster(s)
		for _, n := range names[:3] {
			c.counterNode(n, l, names)
		}
		cfg := simConfig(names...)
		cfg.Registry = counterRegistry()
		c.node("c1", cfg)
		c.settle(tt, names...)

		if info, ok := c.ms["c1"].Host("counter"); ok {
			tt.Errorf("c1's Host(counter) = %+v, true, want false", info)
		}
		for _, n := range names {
			if m, ok := c.view(n).Member("c1"); !ok || m.Kinds != nil {
				tt.Errorf("%s's view has c1 %+v, %v, want it without kinds", n, m, ok)
			}
		}
		owners := map[string]string{"k0": "n1", "k3": "n1", "k8": "n3"}
		for i, key := range []string{"k0", "k3", "k8"} {
			c.send(tt, "c1", key, 10*i+1)
			expectAnswer(tt, key, c.ask(tt, "c1", key, 10*i+2), owners[key], 2)
		}
		w.RunFor(100 * time.Millisecond)
		for _, key := range []string{"k0", "k3", "k8"} {
			expectStrings(tt, key+"'s nodes", l.nodes(key), []string{owners[key]})
		}
		l.atMostOnce(tt)
		expectStrings(tt, "c1's grain records", c.grainKinds("c1"), nil)
	})
}

// AT-VAC-12, VAC-400, VAC-500, VAC-520: k0's owner n4 crashes. n2 keeps sending to k0, and once
// n1, the next owner, has n4 Dead it acquires k0 from n2, the runner-up of its view (n4 is down),
// and activates it (handoff=acked) within D(N) + HandoffTimeout of the crash, D(N) being the
// detection bound of the membership properties (CLU-800). Then n1 answers asks for k0, and no
// message reaches k0 twice.
func TestATVAC12OwnerCrash(t *testing.T) {
	sim.Simulate(t, simOpts(3, 40*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		names := []string{"n1", "n2", "n3", "n4"}
		c := counterCluster(s, l, names)
		expectAnswer(tt, "k0", c.ask(tt, "n2", "k0", 1), "n4", 1)

		bound := boundsOf(simConfig(names...), len(names)).detect + DefaultHandoffTimeout
		crash := w.Sim.Now()
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n4"})
		n := 1
		await(tt, w, 100*time.Millisecond, bound+time.Second, "n1 activates k0", func() bool {
			n++
			// A message whose owner in n2's view is still n4 is lost with n4: Send does not see it.
			_ = GrainOf[cmsg](c.live("n2"), "counter", "k0").Send(s.System("n2"), cmsg{N: n})
			info, _ := c.ms["n1"].Host("counter")
			return slices.Contains(info.Active, "k0")
		})
		act := timeOf(tt, c.keyLog("n1", "k0", crash), rec("activate", "k0", "handoff=acked"))
		if d := act.Sub(crash); d > bound {
			tt.Errorf("n1 activated k0 %s after n4's crash, more than D(N) + HandoffTimeout = %s", d, bound)
		}
		expectStrings(tt, "n1's records of k0 after the crash", recsOf(c.keyLog("n1", "k0", crash)), []string{
			rec("handoff", "k0", "peers=n2"),
			rec("handoff.ack", "k0", "from=n2 remaining=0"),
			rec("activate", "k0", "handoff=acked"),
		})
		c.settle(tt, "n1", "n2", "n3")
		w.RunFor(100 * time.Millisecond) // the last messages of n2 arrive
		got := len(l.of("n1", "k0"))
		expectAnswer(tt, "k0", c.ask(tt, "n3", "k0", 1000), "n1", got+1)
		l.atMostOnce(tt)
	})
}

// AT-VAC-13, VAC-400, VAC-450, VAC-510, VAC-520: k0's owner n4 crashes and n1 takes k0 over. Then
// n4 restarts with a new ID and gets k0 back through handoff: its host starts with no previous
// view, so it asks n1, the runner-up, which stops its activation (passivated for ownership when it
// saw n4 first, or for the handover when the request came first) and acks once it has stopped. n4
// activates k0 (handoff=acked) no earlier than that, and answers. No message reaches k0 twice. The
// wait for n4 is bounded by R(N) + HandoffTimeout: n4 joins and the views converge within R(N)
// (CLU-830), and the acquisition ends within HandoffTimeout after that.
func TestATVAC13RestartedOwner(t *testing.T) {
	sim.Simulate(t, simOpts(3, 90*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		names := []string{"n1", "n2", "n3", "n4"}
		c := counterCluster(s, l, names)
		expectAnswer(tt, "k0", c.ask(tt, "n2", "k0", 1), "n4", 1)
		limit := boundsOf(simConfig(names...), len(names)).converge + DefaultHandoffTimeout
		oldID := c.id("n4")

		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n4"})
		c.settle(tt, "n1", "n2", "n3")
		expectAnswer(tt, "k0", c.ask(tt, "n2", "k0", 2), "n1", 1)

		restart := w.Sim.Now()
		w.Faults.Inject(fault.Event{Kind: fault.KindRestart, Node: "n4"})
		n := 2
		await(tt, w, 100*time.Millisecond, limit, "n4 activates k0", func() bool {
			if c.joined("n2") {
				n++
				c.send(tt, "n2", "k0", n)
			}
			m := c.live("n4")
			if m == nil {
				return false
			}
			info, _ := m.Host("counter")
			return slices.Contains(info.Active, "k0")
		})
		c.settle(tt, names...)
		w.RunFor(100 * time.Millisecond) // the last messages of n2 arrive
		if c.id("n4") == oldID {
			tt.Fatalf("n4 restarted with its old ID %d", oldID)
		}
		n4 := c.keyLog("n4", "k0", restart)
		expectStrings(tt, "n4's records of k0 after its restart", recsOf(n4), []string{
			rec("handoff", "k0", "peers=n1"),
			rec("handoff.ack", "k0", "from=n1 remaining=0"),
			rec("activate", "k0", "handoff=acked"),
		})
		n1 := slices.DeleteFunc(c.keyLog("n1", "k0", restart), func(r timedRec) bool { return strings.HasPrefix(r.rec, "yield ") })
		if got := recsOf(n1); len(got) != 2 || (got[0] != rec("passivate", "k0", "reason=ownership") &&
			got[0] != rec("passivate", "k0", "reason=handover")) || got[1] != rec("stopped", "k0", "") {
			tt.Errorf("n1's records of k0 after n4's restart are %q, want a passivation (ownership or handover) and stopped", got)
		}
		stopped := find(tt, n1, rec("stopped", "k0", ""))
		c.expectAckAfter(tt, "n1", "n4", restart, 0, stopped)
		if act := timeOf(tt, n4, rec("activate", "k0", "handoff=acked")); act < stopped.at {
			tt.Errorf("n4 activated k0 at %s, before n1's activation stopped at %s", act, stopped.at)
		}
		got := 0
		for _, d := range l.got {
			if d.key == "k0" && d.node == "n4" && d.at >= restart {
				got++
			}
		}
		expectAnswer(tt, "k0", c.ask(tt, "n3", "k0", 1000), "n4", got+1)
		l.atMostOnce(tt)
	})
}

// AT-VAC-14, VAC-400, VAC-520, VAC-540: n4 joins n1, n2 and n3 while the links between n4 and n3
// are cut; the others relay the probes, so n3 stays up in n4's view. n4 owns k8 and has no
// previous joined view, so it asks n3, the runner-up, whose request is lost: exactly HandoffTimeout
// after lockstep.grain.handoff, n4 records lockstep.grain.handoff.timeout with missing=n3 and
// activates k8 (handoff=timeout), delivering the held messages in order.
func TestATVAC14UnreachablePeer(t *testing.T) {
	const handoff = 2 * time.Second
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		c := counterCluster(s, l, []string{"n1", "n2", "n3"}, WithHandoffTimeout(handoff))
		c.counterNode("n4", l, []string{"n1", "n2"}, WithHandoffTimeout(handoff))
		w.Faults.Inject(fault.Event{Kind: fault.KindCut, Node: "n4", Peer: "n3"})
		w.Faults.Inject(fault.Event{Kind: fault.KindCut, Node: "n3", Peer: "n4"})
		await(tt, w, time.Millisecond, 5*time.Second, "n4 joins", func() bool { return c.joined("n4") })
		w.RunFor(100 * time.Millisecond)
		if m, ok := c.view("n4").Member("n3"); !ok || !m.State.Up() {
			tt.Fatalf("n4's view has n3 %v, %v: the test needs it up", m, ok)
		}
		start := w.Sim.Now()
		c.send(tt, "n4", "k8", 1)
		c.send(tt, "n4", "k8", 2)
		w.RunFor(handoff + 100*time.Millisecond)
		recs := c.keyLog("n4", "k8", start)
		expectStrings(tt, "n4's records of k8", recsOf(recs), []string{
			rec("handoff", "k8", "peers=n3"),
			rec("handoff.timeout", "k8", "missing=n3"),
			rec("activate", "k8", "handoff=timeout"),
		})
		if len(recs) == 3 {
			if d := recs[1].at.Sub(recs[0].at); d != handoff {
				tt.Errorf("the handoff timed out %s after it started, want HandoffTimeout = %s", d, handoff)
			}
		}
		expectStrings(tt, "k8's messages on n4", intStrings(l.of("n4", "k8")), intStrings([]int{1, 2}))
		expectAnswer(tt, "k8", c.ask(tt, "n4", "k8", 3), "n4", 3)
		w.Faults.Inject(fault.Event{Kind: fault.KindHeal})
		c.settle(tt, "n1", "n2", "n3", "n4")
		expectAnswer(tt, "k8", c.ask(tt, "n2", "k8", 4), "n4", 4)
		l.atMostOnce(tt)
	})
}

// joinRun is the run of joinTakeover.
type joinRun struct {
	c    *simCluster
	l    *counterLog
	join kernel.Time // when n4 had joined
}

// joinTakeover runs the join of AT-VAC-15 and AT-VAC-16. n1, n2 and n3 form a cluster, and k0 is
// active on n1 and k8 on n3, their owners over these nodes. The membership messages from n2 and n4
// to n1 and n3 are delayed by 300ms, so that n1 and n3 learn of n4 at least that late. n4 joins
// through n2 and at once sends message 3 to k0 and k8, which it owns: it asks the runner-ups, n1
// and n3. 50ms later, when n1 and n3 have yielded but do not know n4, n1 sends 4 to k0, n3 sends 5
// to k0 (through n1, its owner in n3's view) and 6 to k8. Then the delays end, the four nodes
// agree, and n1 and n3 send 7 and 8 to k0 and k8, and ask them.
func joinTakeover(s *sim.Sim) joinRun {
	w, tt := s.World(), s.World().T()
	l := &counterLog{}
	c := counterCluster(s, l, []string{"n1", "n2", "n3"})
	expectAnswer(tt, "k0", c.ask(tt, "n2", "k0", 1), "n1", 1)
	expectAnswer(tt, "k8", c.ask(tt, "n2", "k8", 1), "n3", 1)
	const delay = 300 * time.Millisecond
	pairs := [][2]string{{"n2", "n1"}, {"n2", "n3"}, {"n4", "n1"}, {"n4", "n3"}}
	for _, p := range pairs {
		s.DelayBetween(memberAddr(p[0]), memberAddr(p[1]), delay, delay)
	}
	c.counterNode("n4", l, []string{"n2"})
	await(tt, w, time.Millisecond, 5*time.Second, "n4 joins", func() bool { return c.joined("n4") })
	join := w.Sim.Now()
	c.send(tt, "n4", "k0", 3)
	c.send(tt, "n4", "k8", 3)
	w.RunFor(50 * time.Millisecond)
	for _, n := range []string{"n1", "n3"} {
		if _, ok := c.view(n).Member("n4"); ok {
			tt.Fatalf("%s knows n4 %s after its join: the test needs the requests first", n, w.Sim.Now().Sub(join))
		}
	}
	c.send(tt, "n1", "k0", 4)
	c.send(tt, "n3", "k0", 5)
	c.send(tt, "n3", "k8", 6)
	w.RunFor(50 * time.Millisecond)
	for _, p := range pairs {
		s.DelayBetween(memberAddr(p[0]), memberAddr(p[1]), 0, 0)
	}
	c.settle(tt, "n1", "n2", "n3", "n4")
	c.send(tt, "n1", "k0", 7)
	c.send(tt, "n3", "k8", 7)
	expectAnswer(tt, "k0", c.ask(tt, "n1", "k0", 8), "n4", 5)
	expectAnswer(tt, "k8", c.ask(tt, "n3", "k8", 8), "n4", 4)
	return joinRun{c: c, l: l, join: join}
}

// AT-VAC-15, VAC-400, VAC-510, VAC-520: n4 joins and takes k0 and k8 over (see joinTakeover). It
// asks n1 for k0 and n3 for k8, the runner-ups, which have not heard of it yet: each yields the
// key, passivates its activation with reason handover and acks once it has stopped (its ack's send
// record comes after its stopped record), and n4 activates the key (handoff=acked) no earlier
// than that. Every message
// reaches its grain once: those before the join on the old owner, the others on n4.
func TestATVAC15JoinTakesKeys(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		tt := s.World().T()
		r := joinTakeover(s)
		c, l := r.c, r.l
		for _, k := range []struct{ key, old string }{{"k0", "n1"}, {"k8", "n3"}} {
			n4 := c.keyLog("n4", k.key, 0)
			expectStrings(tt, "n4's records of "+k.key, recsOf(n4), []string{
				rec("handoff", k.key, "peers="+k.old),
				rec("handoff.ack", k.key, "from="+k.old+" remaining=0"),
				rec("activate", k.key, "handoff=acked"),
			})
			old := c.keyLog(k.old, k.key, r.join)
			expectStrings(tt, k.old+"'s records of "+k.key+" from the join on", recsOf(old)[:min(3, len(old))], []string{
				rec("yield", k.key, "to=n4"),
				rec("passivate", k.key, "reason=handover"),
				rec("stopped", k.key, ""),
			})
			stopped := find(tt, old, rec("stopped", k.key, ""))
			c.expectAckAfter(tt, k.old, "n4", r.join, 0, stopped)
			if act := timeOf(tt, n4, rec("activate", k.key, "handoff=acked")); act < stopped.at {
				tt.Errorf("n4 activated %s at %s, before %s's activation stopped at %s", k.key, act, k.old, stopped.at)
			}
			expectStrings(tt, k.key+"'s messages on "+k.old, intStrings(l.of(k.old, k.key)), []string{"1"})
		}
		expectStrings(tt, "k0's messages on n4", intStrings(slices.Sorted(slices.Values(l.of("n4", "k0")))), intStrings([]int{3, 4, 5, 7, 8}))
		expectStrings(tt, "k8's messages on n4", intStrings(slices.Sorted(slices.Values(l.of("n4", "k8")))), intStrings([]int{3, 6, 7, 8}))
		l.atMostOnce(tt)
	})
}

// AT-VAC-16, VAC-310 rule 2, VAC-320, VAC-530: after the yields of joinTakeover, n1 and n3 do not
// know n4 yet, and still own k0 and k8 in their views. The messages they get for these keys, from
// their own nodes or from n3's Send of k0 through n1, are forwarded to n4 (forward hops=1), and
// nothing activates again on them. Once their views have n4, the yields end and their nodes send
// to n4 directly: nothing more is forwarded.
func TestATVAC16YieldForwards(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		tt := s.World().T()
		r := joinTakeover(s)
		c, l := r.c, r.l
		after := func(node, key string) []string {
			recs := recsOf(c.keyLog(node, key, r.join))
			if i := slices.Index(recs, rec("stopped", key, "")); i >= 0 {
				return recs[i+1:]
			}
			return recs
		}
		expectStrings(tt, "n1's records of k0 after its activation stopped", after("n1", "k0"), []string{
			rec("forward", "k0", "to=n4 hops=1"),
			rec("forward", "k0", "to=n4 hops=1"),
		})
		expectStrings(tt, "n3's records of k8 after its activation stopped", after("n3", "k8"), []string{
			rec("forward", "k8", "to=n4 hops=1"),
		})
		expectStrings(tt, "n3's records of k0", recsOf(c.keyLog("n3", "k0", r.join)), nil)
		for _, key := range []string{"k0", "k8"} {
			for _, d := range l.got {
				if d.key == key && d.node != "n4" && d.at >= r.join {
					tt.Errorf("%s got message %d on %s at %s, after n4's join", key, d.n, d.node, d.at)
				}
			}
		}
		l.atMostOnce(tt)
	})
}

// AT-VAC-17, VAC-220, VAC-310 rule 4, VAC-320: n4 joins n1, n2 and n3 through n1, and the
// membership messages to n3 are delayed by 300ms, so n3 learns of n4 late. Meanwhile n3 sends
// five messages to k0: in n3's view n1 owns k0, in n1's view n4 does, so n1 forwards each one
// to n4 (forward hops=1). n4 activates k0 and gets the five messages once each, in send order, and
// nothing reaches k0 elsewhere.
func TestATVAC17DisagreeingViews(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		c := counterCluster(s, l, []string{"n1", "n2", "n3"})
		const delay = 300 * time.Millisecond
		for _, from := range []string{"n1", "n2", "n4"} {
			s.DelayBetween(memberAddr(from), memberAddr("n3"), delay, delay)
		}
		c.counterNode("n4", l, []string{"n1"})
		await(tt, w, time.Millisecond, 5*time.Second, "n4 joins", func() bool { return c.joined("n4") })
		if _, ok := c.view("n3").Member("n4"); ok {
			tt.Fatal("n3 knows n4 already: the test needs views that disagree")
		}
		if o, _ := Owner(c.view("n1"), "counter", "k0"); o != "n4" {
			tt.Fatalf("n1's owner of k0 is %s, want n4", o)
		}
		for n := 1; n <= 5; n++ {
			c.send(tt, "n3", "k0", n)
		}
		w.RunFor(100 * time.Millisecond)
		for _, from := range []string{"n1", "n2", "n4"} {
			s.DelayBetween(memberAddr(from), memberAddr("n3"), 0, 0)
		}
		c.settle(tt, "n1", "n2", "n3", "n4")
		expectAnswer(tt, "k0", c.ask(tt, "n3", "k0", 6), "n4", 6)

		expectStrings(tt, "n1's records of k0", slices.DeleteFunc(recsOf(c.keyLog("n1", "k0", 0)), func(r string) bool {
			return strings.HasPrefix(r, "yield ")
		}), slices.Repeat([]string{rec("forward", "k0", "to=n4 hops=1")}, 5))
		expectStrings(tt, "n3's records of k0", recsOf(c.keyLog("n3", "k0", 0)), nil)
		expectStrings(tt, "k0's messages", intStrings(l.of("", "k0")), intStrings([]int{1, 2, 3, 4, 5, 6}))
		expectStrings(tt, "k0's nodes", l.nodes("k0"), []string{"n4"})
	})
}

// AT-VAC-19, VAC-210: asks across nodes. From each of n1, n2 and n3, an ask to a key of each node
// is answered by its owner. An ask to a grain of a kind that no member hosts fails at once, before
// Ask returns, with ErrNoOwner, and build is not called.
func TestATVAC19AskAcrossNodes(t *testing.T) {
	sim.Simulate(t, simOpts(3, 20*time.Second), func(s *sim.Sim) {
		tt := s.World().T()
		l := &counterLog{}
		names := []string{"n1", "n2", "n3"}
		c := counterCluster(s, l, names)
		n := 0
		for _, owner := range names {
			key := c.keyOf("n1", "counter", owner)
			for i, from := range names {
				n++
				expectAnswer(tt, key+" from "+from, c.ask(tt, from, key, n), owner, i+1)
			}
		}
		l.atMostOnce(tt)

		built := false
		r := Ask(s.System("n2"), GrainOf[cmsg](c.ms["n2"], "ghost", "k0"), func(rt lockstep.ReplyTo[string]) cmsg {
			built = true
			return cmsg{Reply: rt}
		})
		if !askResolved(r) {
			tt.Fatal("the ask to a grain without an owner has not failed at once")
		}
		_, err := awaitResolved(tt, r)
		expectErr(tt, "Ask without an owner", err, ErrNoOwner, "cluster: send ghost/k0: cluster: no owner")
		if built {
			tt.Error("Ask called build for a grain without an owner")
		}
	})
}

// AT-VAC-20, CLU-600, VAC-200, VAC-500 step 3: n1, which owns k3, leaves. Its host passivates k3
// with reason leave and the activation stops; Send and Ask on n1 return ErrLeft from then on. The
// others see n1 Left, and n3, the next owner, takes k3 over: its previous view had n1 as the owner,
// which has left, so it asks n4, the runner-up of its view (k3 → n1, n3, n4, n2), and activates
// k3 (handoff=acked) and answers.
func TestATVAC20Leave(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		c := counterCluster(s, l, []string{"n1", "n2", "n3", "n4"})
		expectAnswer(tt, "k3", c.ask(tt, "n2", "k3", 1), "n1", 1)

		m1 := c.ms["n1"]
		leave := w.Sim.Now()
		if err := m1.Leave(); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, "n1's records of k3 from Leave on", recsOf(c.keyLog("n1", "k3", leave)), []string{
			rec("passivate", "k3", "reason=leave"),
			rec("stopped", "k3", ""),
		})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: m1.View().Version, Joined: true})
		g := GrainOf[cmsg](m1, "counter", "k3")
		expectErr(tt, "Send after Leave", g.Send(s.System("n1"), cmsg{N: 99}), ErrLeft, "cluster: send counter/k3: cluster: left")
		r := Ask(s.System("n1"), g, func(rt lockstep.ReplyTo[string]) cmsg { return cmsg{N: 99, Reply: rt} })
		if !askResolved(r) {
			tt.Fatal("an ask after Leave has not failed at once")
		}
		_, err := awaitResolved(tt, r)
		expectErr(tt, "Ask after Leave", err, ErrLeft, "cluster: send counter/k3: cluster: left")
		if err := m1.Leave(); !errors.Is(err, ErrLeft) {
			tt.Errorf("a second Leave returned %v, want ErrLeft", err)
		}

		c.settle(tt, "n2", "n3", "n4")
		for _, n := range []string{"n2", "n3", "n4"} {
			if m, ok := c.view(n).Member("n1"); !ok || m.State != Left {
				tt.Errorf("%s's view has n1 %v, %v, want Left", n, m, ok)
			}
		}
		c.send(tt, "n2", "k3", 2)
		expectAnswer(tt, "k3", c.ask(tt, "n2", "k3", 3), "n3", 2)
		expectStrings(tt, "n3's records of k3 from Leave on", recsOf(c.keyLog("n3", "k3", leave)), []string{
			rec("handoff", "k3", "peers=n4"),
			rec("handoff.ack", "k3", "from=n4 remaining=0"),
			rec("activate", "k3", "handoff=acked"),
		})
		expectStrings(tt, "k3's messages on n3", intStrings(l.of("n3", "k3")), intStrings([]int{2, 3}))
		l.atMostOnce(tt)
	})
}

// hostInfoInvariant registers an invariant that checks the HostInfo of counter of every live
// process of names after every simulation event (VAC-600): the key lists are sorted, without
// duplicates, and disjoint; Buffered is not negative; Version is at most the node's View().Version
// and never goes down for a process.
func hostInfoInvariant(c *simCluster, names []string) {
	last := map[*Membership]uint64{}
	c.s.World().Invariant("cluster/host-info", func() error {
		for _, n := range names {
			m := c.live(n)
			if m == nil {
				continue
			}
			info, ok := m.Host("counter")
			if !ok {
				continue
			}
			var all []string
			for _, list := range [][]string{info.Active, info.Stopping, info.Acquiring} {
				if !slices.IsSorted(list) || len(slices.Compact(slices.Clone(list))) != len(list) {
					return fmt.Errorf("%s's HostInfo has a list that is not sorted or has duplicates: %+v", n, info)
				}
				all = append(all, list...)
			}
			slices.Sort(all)
			if len(slices.Compact(all)) != len(info.Active)+len(info.Stopping)+len(info.Acquiring) {
				return fmt.Errorf("%s's HostInfo has a key in two lists: %+v", n, info)
			}
			if info.Buffered < 0 {
				return fmt.Errorf("%s's HostInfo has Buffered %d", n, info.Buffered)
			}
			if v := m.View().Version; info.Version > v {
				return fmt.Errorf("%s's host applied view %d, after the latest view %d", n, info.Version, v)
			}
			if info.Version < last[m] {
				return fmt.Errorf("%s's host went back from view %d to %d", n, last[m], info.Version)
			}
			last[m] = info.Version
		}
		return nil
	})
}

// AT-VAC-25 (across nodes and views), VAC-600: n1, n2 and n3 activate twelve keys, then n4 joins
// and the four nodes route them again. An invariant checks every node's HostInfo after every event
// (see hostInfoInvariant). Whenever the nodes agree and every key has been routed, each node's
// HostInfo has the version of its view, Joined, no stopping, acquiring or buffered key, and as
// Active exactly the keys it owns in its view: so after the join n1, n2 and n3 hold no key n4
// owns, and every key is active on one node.
func TestATVAC25HostInfoAcrossNodes(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		names := []string{"n1", "n2", "n3", "n4"}
		c := counterCluster(s, l, names[:3])
		hostInfoInvariant(c, names)
		keys := make([]string, 12)
		for i := range keys {
			keys[i] = "k" + strconv.Itoa(i)
		}
		check := func(phase string, nodes []string) {
			tt.Helper()
			for _, n := range nodes {
				v := c.view(n)
				var mine []string
				for _, key := range keys {
					if o, _ := Owner(v, "counter", key); o == n {
						mine = append(mine, key)
					}
				}
				expectHost(tt, c, n, HostInfo{Kind: "counter", Version: v.Version, Joined: true, Active: sorted(mine...)})
			}
			for _, key := range keys {
				var on []string
				for _, n := range nodes {
					if info, _ := c.ms[n].Host("counter"); slices.Contains(info.Active, key) {
						on = append(on, n)
					}
				}
				if len(on) != 1 {
					tt.Errorf("%s: %s is active on %v, want one node", phase, key, on)
				}
			}
		}
		for i, key := range keys {
			c.send(tt, "n1", key, i+1)
		}
		w.RunFor(time.Second)
		check("before the join", names[:3])

		c.counterNode("n4", l, []string{"n1"})
		c.settle(tt, names...)
		for i, key := range keys {
			c.send(tt, "n2", key, 100+i)
		}
		w.RunFor(time.Second)
		check("after the join", names)
		for _, key := range []string{"k0", "k8"} {
			if info, _ := c.ms["n4"].Host("counter"); !slices.Contains(info.Active, key) {
				tt.Errorf("%s is not active on n4 after the join: %+v", key, info)
			}
		}
		l.atMostOnce(tt)
	})
}

// AT-VAC-26 (host failure), VAC-450, VAC-520, VAC-540: n4's host restarts, so it has no previous
// view, and sends of k0 and k8 make it acquire them from n1 and n3, the runner-ups. n1's acks to
// it take one second and n3's are lost. While both keys are acquiring, with two messages each,
// the host fails: it records one drop reason=host per held message, in key order, and restarts
// empty with m.View() as its first view (lockstep.grain.view). New messages make it acquire the
// keys again. n1's ack of the first acquisition of k0 arrives first and counts for nothing; the
// ack of the second one ends it (handoff=acked). The second acquisition of k8 ends at
// HandoffTimeout (handoff=timeout, missing=n3). Only the messages sent after the failure reach
// the grains.
func TestATVAC26HostFailureAcquiring(t *testing.T) {
	const handoff = 3 * time.Second
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		names := []string{"n1", "n2", "n3", "n4"}
		c := counterCluster(s, l, names, WithHandoffTimeout(handoff))
		host := hostAt("n4", "counter").Address()
		if err := s.InjectFail(host, errors.New("restart")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(100 * time.Millisecond)
		s.DelayBetween(hostAt("n1", "counter").Address(), host, time.Second, time.Second)
		s.DropBetween(hostAt("n3", "counter").Address(), host, 1_000_000)

		first := w.Sim.Now()
		// k0 first in both phases: if acquisitions were numbered from 1 per host incarnation, the
		// stale ack of the first acquisition of k0 would match the second one.
		for _, key := range []string{"k0", "k8"} {
			c.send(tt, "n4", key, 1)
			c.send(tt, "n4", key, 2)
		}
		w.RunFor(100 * time.Millisecond)
		v := c.view("n4").Version
		expectHost(tt, c, "n4", HostInfo{Kind: "counter", Version: v, Joined: true, Acquiring: []string{"k0", "k8"}, Buffered: 4})

		fail := w.Sim.Now()
		if err := s.InjectFail(host, errors.New("host failure")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(100 * time.Millisecond)
		id := c.n["n4"].ID()
		var turn []string
		for _, r := range w.Sim.Records() {
			if r.Node == id && r.At >= fail && strings.HasPrefix(r.Kind, grainPrefix) {
				turn = append(turn, strings.TrimPrefix(r.Kind, grainPrefix)+" "+attrs(r))
			}
		}
		expectStrings(tt, "n4's grain records at the failure", turn, []string{
			rec("drop", "k0", "reason=host hops=0"),
			rec("drop", "k0", "reason=host hops=0"),
			rec("drop", "k8", "reason=host hops=0"),
			rec("drop", "k8", "reason=host hops=0"),
			"view kind=counter version=" + strconv.FormatUint(v, 10) + " joined=true active=0",
		})
		expectHost(tt, c, "n4", HostInfo{Kind: "counter", Version: v, Joined: true})

		second := w.Sim.Now()
		c.send(tt, "n4", "k0", 3)
		c.send(tt, "n4", "k8", 3)
		w.RunFor(handoff + 100*time.Millisecond)
		k0 := c.keyLog("n4", "k0", second)
		expectStrings(tt, "n4's records of k0 after the failure", recsOf(k0), []string{
			rec("handoff", "k0", "peers=n1"),
			rec("handoff.ack", "k0", "from=n1 remaining=0"),
			rec("activate", "k0", "handoff=acked"),
		})
		if len(k0) == 3 && k0[1].at.Sub(second) < time.Second {
			tt.Errorf("the second acquisition of k0 counted an ack at %s, before its own could arrive", k0[1].at)
		}
		k8 := c.keyLog("n4", "k8", second)
		expectStrings(tt, "n4's records of k8 after the failure", recsOf(k8), []string{
			rec("handoff", "k8", "peers=n3"),
			rec("handoff.timeout", "k8", "missing=n3"),
			rec("activate", "k8", "handoff=timeout"),
		})
		if len(k8) == 3 && k8[1].at.Sub(k8[0].at) != handoff {
			tt.Errorf("the second acquisition of k8 timed out %s after it started, want %s", k8[1].at.Sub(k8[0].at), handoff)
		}
		before := recsOf(c.keyLog("n4", "k0", first))
		expectStrings(tt, "n4's records of k0 before the failure", before[:min(2, len(before))], []string{
			rec("handoff", "k0", "peers=n1"),
			rec("drop", "k0", "reason=host hops=0"),
		})
		for _, key := range []string{"k0", "k8"} {
			expectStrings(tt, key+"'s messages", intStrings(l.of("", key)), []string{"3"})
			expectStrings(tt, key+"'s nodes", l.nodes(key), []string{"n4"})
		}
	})
}

// viewSeg is the views of one process of a node: the versions of its lockstep.cluster.view and
// lockstep.grain.view records of counter, in order, with their times.
type viewSeg struct {
	inc     uint32
	cluster []timedVersion
	grain   []timedVersion
}

// timedVersion is a view version, with the time of its record and, for a host's view, whether the
// node had joined.
type timedVersion struct {
	at     kernel.Time
	v      uint64
	joined bool
}

// viewSegs returns the views of node name's processes, in boot order.
func (c *simCluster) viewSegs(tt testing.TB, name string) []viewSeg {
	tt.Helper()
	id := c.n[name].ID()
	var out []viewSeg
	version := func(r kernel.Record) timedVersion {
		v, err := strconv.ParseUint(attr(r, "version"), 10, 64)
		if err != nil {
			tt.Fatalf("%s's record %s has version %q", name, r.Kind, attr(r, "version"))
		}
		return timedVersion{at: r.At, v: v, joined: attr(r, "joined") == "true"}
	}
	for _, r := range c.s.World().Sim.Records() {
		cluster := r.Kind == recordPrefix+"view"
		grain := r.Kind == grainPrefix+"view" && attr(r, "kind") == "counter"
		if r.Node != id || !cluster && !grain {
			continue
		}
		i := slices.IndexFunc(out, func(seg viewSeg) bool { return seg.inc == r.Inc })
		if i < 0 {
			out = append(out, viewSeg{inc: r.Inc})
			i = len(out) - 1
		}
		if cluster {
			out[i].cluster = append(out[i].cluster, version(r))
		} else {
			out[i].grain = append(out[i].grain, version(r))
		}
	}
	return out
}

// AT-VAC-28, VAC-300, VAC-500, VAC-600: hosts apply the views in order. n1, n2 and n3 form a
// cluster, n4 joins, n2 crashes and restarts, and n3 leaves. For every process of every node, the
// versions of its host's lockstep.grain.view records increase, and each one (but Join's view 1,
// which has no record) is a version the node's membership actor recorded in lockstep.cluster.view
// no later; a process that did not crash ends with the host on the last view. An invariant checks
// that HostInfo.Version never passes View().Version nor goes back, and at the end every live
// node's HostInfo.Version is its View().Version.
func TestATVAC28ViewsInOrder(t *testing.T) {
	sim.Simulate(t, simOpts(3, 60*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &counterLog{}
		names := []string{"n1", "n2", "n3", "n4"}
		c := counterCluster(s, l, names[:3])
		hostInfoInvariant(c, names)
		c.counterNode("n4", l, []string{"n1"})
		c.settle(tt, names...)
		for i, key := range []string{"k0", "k3", "k8"} {
			c.send(tt, "n4", key, i)
		}
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n2"})
		c.settle(tt, "n1", "n3", "n4")
		w.Faults.Inject(fault.Event{Kind: fault.KindRestart, Node: "n2"})
		c.settle(tt, names...)
		if err := c.ms["n3"].Leave(); err != nil {
			tt.Fatal(err)
		}
		c.settle(tt, "n1", "n2", "n4")
		w.RunFor(time.Second)

		for _, n := range names {
			segs := c.viewSegs(tt, n)
			for i, seg := range segs {
				crashed := n == "n2" && i == 0
				var last timedVersion
				for _, g := range seg.grain {
					// The same version again only when the node has joined since (VAC-300).
					if g.v < last.v || g.v == last.v && (last.joined || !g.joined) {
						tt.Errorf("%s:%d's host applied view %d (joined %v) after view %d (joined %v)", n, seg.inc, g.v, g.joined, last.v, last.joined)
					}
					last = g
					if g.v == 1 {
						continue
					}
					j := slices.IndexFunc(seg.cluster, func(cv timedVersion) bool { return cv.v == g.v })
					if j < 0 || seg.cluster[j].at > g.at {
						tt.Errorf("%s:%d's host applied view %d at %s, which its membership did not publish before", n, seg.inc, g.v, g.at)
					}
				}
				if !crashed && len(seg.cluster) > 0 && last.v != seg.cluster[len(seg.cluster)-1].v {
					tt.Errorf("%s:%d's host ended on view %d, not on the last view %d", n, seg.inc, last.v, seg.cluster[len(seg.cluster)-1].v)
				}
			}
			want := 1
			if n == "n2" {
				want = 2
			}
			if len(segs) != want {
				tt.Errorf("%s had %d processes, want %d", n, len(segs), want)
			}
			m := c.live(n)
			if info, _ := m.Host("counter"); info.Version != m.View().Version {
				tt.Errorf("%s's host is on view %d, its view is %d", n, info.Version, m.View().Version)
			}
		}
		l.atMostOnce(tt)
	})
}
