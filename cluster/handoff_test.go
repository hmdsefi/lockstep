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
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/sim"
)

// The helpers of the handoff tests.

// peerView returns the published view, with its placement, of members "node:state:kind,kind" as
// hostView reads them.
func peerView(version uint64, specs ...string) *View {
	v := hostView(specs...)
	return newView(version, v.Self, v.Members)
}

// unitHost returns a host of counter on node self that is not running, for the view functions.
func unitHost(self string) *host {
	return &host{kind: &Kind{name: "counter"}, self: self, keys: map[string]*grain{}}
}

// fakeGot is a message a fake host received, with the time.
type fakeGot struct {
	at  kernel.Time
	msg hostMsg
}

// fakeHost is a stand-in for the host of counter on a node that never joins: an actor at
// /system/grains/counter that keeps every message it receives and, when ack is set, answers every
// handoffReq with its ack at once. Its node also runs a fake membership actor that answers pings
// with an entry that hosts counter, so a node that knows it keeps it up.
type fakeHost struct {
	node string
	now  func() kernel.Time
	ack  bool
	got  []fakeGot
}

// fakeGrains is the actor at /system/grains of a fake host's node.
type fakeGrains struct{ f *fakeHost }

func (a *fakeGrains) Started(ctx *lockstep.Context[grainsMsg]) {
	if _, err := lockstep.Spawn(ctx, "counter", func() lockstep.Actor[hostMsg] { return &fakeHostActor{a.f} }); err != nil {
		panic(err)
	}
}

func (*fakeGrains) Receive(*lockstep.Context[grainsMsg], grainsMsg) {}

// fakeHostActor is the actor of a fakeHost.
type fakeHostActor struct{ f *fakeHost }

// Receive keeps msg and acks a handoffReq when the fake host acks.
func (a *fakeHostActor) Receive(ctx *lockstep.Context[hostMsg], msg hostMsg) {
	a.f.got = append(a.f.got, fakeGot{a.f.now(), msg})
	if req, ok := msg.(handoffReq); ok && a.f.ack {
		ack := handoffAck{Kind: req.Kind, Key: req.Key, From: a.f.node, Seq: req.Seq}
		if err := lockstep.Send(ctx, hostAt(req.From, req.Kind), hostMsg(ack)); err != nil {
			panic(err)
		}
	}
}

// fakeHostNode adds node name with a fake host and a fake membership actor (see fakeHost).
func (c *simCluster) fakeHostNode(name string, ack bool) *fakeHost {
	f := &fakeHost{node: name, now: c.s.World().Sim.Now, ack: ack}
	fm := &fakeMember{now: c.s.World().Sim.Now}
	c.n[name] = c.s.Node(name, func(sys *lockstep.System) {
		sp, _ := hooks.SystemSpawner(sys).(lockstep.Spawner)
		fm.self = update{Node: name, ID: sys.Incarnation(), State: Alive, Kinds: []string{"counter"}}
		if _, err := lockstep.Spawn(sp, actorName, func() lockstep.Actor[memberMsg] { return &fakeActor{fm} }); err != nil {
			panic(err)
		}
		if _, err := lockstep.Spawn(sp, grainsName, func() lockstep.Actor[grainsMsg] { return &fakeGrains{f} }); err != nil {
			panic(err)
		}
	})
	return f
}

// msgs returns the messages the fake host received, as text: "req <key> <from> <seq>",
// "ack <key> <from> <seq>" or "env <key> <hops> <N>" (the N of the gmsg inside).
func (f *fakeHost) msgs(tt testing.TB) []string {
	tt.Helper()
	var out []string
	for _, g := range f.got {
		switch m := g.msg.(type) {
		case handoffReq:
			out = append(out, fmt.Sprintf("req %s %s %d", m.Key, m.From, m.Seq))
		case handoffAck:
			out = append(out, fmt.Sprintf("ack %s %s %d", m.Key, m.From, m.Seq))
		case grainEnvelope:
			v, err := grainRegistry().Decode(simCodec, m.Name, m.Data)
			if err != nil {
				tt.Fatalf("the fake host got an envelope it cannot decode: %v", err)
			}
			out = append(out, fmt.Sprintf("env %s %d %d", m.Key, m.Hops, v.(gmsg).N))
		default:
			out = append(out, fmt.Sprintf("%T", m))
		}
	}
	return out
}

// lastReq returns the last handoffReq the fake host received for key.
func (f *fakeHost) lastReq(tt testing.TB, key string) handoffReq {
	tt.Helper()
	for i := len(f.got) - 1; i >= 0; i-- {
		if r, ok := f.got[i].msg.(handoffReq); ok && r.Key == escapeKey(key) {
			return r
		}
	}
	tt.Fatalf("%s got no handoffReq for %s", f.node, key)
	return handoffReq{}
}

// addMember makes node's membership actor merge an Alive entry of member, with the ID of its
// current process and hosting kinds, as a syncReq from it would.
func (c *simCluster) addMember(tt testing.TB, node, member string, kinds ...string) {
	tt.Helper()
	u := update{Node: member, ID: c.id(member), State: Alive, Kinds: kinds}
	if err := sim.Send(c.s, node, lockstep.PIDAt[memberMsg](node, actorPath), memberMsg(syncReq{From: u})); err != nil {
		tt.Fatal(err)
	}
}

// sendHost sends msg to node's host of counter from the node from.
func sendHost(tt testing.TB, s *sim.Sim, from, node string, msg hostMsg) {
	tt.Helper()
	if err := sim.Send(s, from, hostAt(node, "counter"), msg); err != nil {
		tt.Fatalf("send %T to %s's host: %v", msg, node, err)
	}
}

// ownedKeys returns n keys "k<i>" that owner outranks every node of others for, in the order of i.
func ownedKeys(owner string, others []string, n int) []string {
	var out []string
	for i := 0; len(out) < n; i++ {
		key := "k" + strconv.Itoa(i)
		if !slices.ContainsFunc(others, func(o string) bool { return Score(o, "counter", key) > Score(owner, "counter", key) }) {
			out = append(out, key)
		}
	}
	return out
}

// keyRecs returns node's lockstep.grain records of counter whose key is key, in order, as
// "<kind> <attrs>"; the view records, which have no key, are left out.
func (c *simCluster) keyRecs(node, key string) []string {
	var out []string
	for _, r := range c.grainTrace(node) {
		if strings.Contains(r, " key="+escapeKey(key)+" ") || strings.HasSuffix(r, " key="+escapeKey(key)) {
			out = append(out, r)
		}
	}
	return out
}

// recAt returns the time of node's first lockstep.grain.<kind> record with attributes attrs.
func (c *simCluster) recAt(tt testing.TB, node, kind, attrsWant string) kernel.Time {
	tt.Helper()
	id := c.n[node].ID()
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && r.Kind == grainPrefix+kind && attrs(r) == attrsWant {
			return r.At
		}
	}
	tt.Fatalf("%s has no record %s %s", node, kind, attrsWant)
	return 0
}

// simCodec is the codec of the simulated nodes' grain messages: Config.Codec's default.
var simCodec = DefaultConfig().Codec

// AT-VAC-29 (peers), VAC-400: the handoff peers of a key this node owns in C. With P none or not
// joined, the runner-up in C; none when this node owns the key in P; else the owner in P and the
// runner-up in C. This node, nodes that are down or absent in C and nodes that do not host the
// kind in C are left out; the result is sorted, without duplicates. §10's rankings: k0 → n4, n1,
// n2, n3; k3 → n1, n3, n4, n2; k8 → n4, n3, n1, n2.
func TestATVAC29Peers(t *testing.T) {
	all := []string{"n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter"}
	tests := []struct {
		name    string
		self    string
		prev    []string // nil: P is none
		joined  bool     // P's joined flag
		cur     []string
		key     string
		want    []string
		wantNil bool
	}{
		{name: "P none: the runner-up", self: "n4", cur: all, key: "k0", want: []string{"n1"}},
		{name: "P not joined: the runner-up", self: "n4", prev: []string{"n4:a:counter"}, cur: all, key: "k0", want: []string{"n1"}},
		{name: "P not joined: never its owner", self: "n4", prev: []string{"n2:a:counter", "n4:a:counter"}, cur: all, key: "k0", want: []string{"n1"}},
		{name: "this node owns the key in P", self: "n4", prev: all, joined: true,
			cur: []string{"n1:a:counter", "n2:d:counter", "n3:a:counter", "n4:a:counter"}, key: "k0"},
		{name: "owner in P and runner-up in C", self: "n4", prev: []string{"n1:a:counter", "n2:a:counter"}, joined: true,
			cur: all, key: "k8", want: []string{"n1", "n3"}},
		{name: "sorted", self: "n1", prev: []string{"n2:a:counter", "n4:a:counter"}, joined: true,
			cur: all, key: "k3", want: []string{"n3", "n4"}},
		{name: "owner in P is the runner-up: once", self: "n4", prev: []string{"n1:a:counter", "n2:a:counter", "n3:a:counter"},
			joined: true, cur: all, key: "k0", want: []string{"n1"}},
		{name: "owner in P down in C", self: "n3", prev: []string{"n1:a:counter", "n2:a:counter", "n3:a:counter"}, joined: true,
			cur: []string{"n1:d:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter"}, key: "k3", want: []string{"n4"}},
		{name: "owner in P left in C", self: "n3", prev: []string{"n1:a:counter", "n2:a:counter", "n3:a:counter"}, joined: true,
			cur: []string{"n1:l:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter"}, key: "k3", want: []string{"n4"}},
		{name: "owner in P absent in C", self: "n3", prev: []string{"n1:a:counter", "n2:a:counter", "n3:a:counter"}, joined: true,
			cur: []string{"n2:a:counter", "n3:a:counter", "n4:a:counter"}, key: "k3", want: []string{"n4"}},
		{name: "owner in P does not host the kind in C", self: "n3", prev: []string{"n1:a:counter", "n2:a:counter", "n3:a:counter"},
			joined: true, cur: []string{"n1:a:other", "n2:a:counter", "n3:a:counter", "n4:a:counter"}, key: "k3", want: []string{"n4"}},
		{name: "a suspect runner-up counts", self: "n4", cur: []string{"n1:s:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter"},
			key: "k0", want: []string{"n1"}},
		{name: "no runner-up", self: "n4", cur: []string{"n4:a:counter"}, key: "k0"},
		{name: "the runner-up does not host the kind", self: "n4", cur: []string{"n1:a:other", "n4:a:counter"}, key: "k0"},
		{name: "no owner in P", self: "n4", prev: []string{"n1:a:other", "n4:d:counter"}, joined: true, cur: all, key: "k0",
			want: []string{"n1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := unitHost(tc.self)
			if tc.prev != nil {
				h.shift(peerView(1, tc.prev...), tc.joined)
			}
			h.shift(peerView(2, tc.cur...), true)
			if owner, _ := Owner(peerView(2, tc.cur...), "counter", tc.key); owner != tc.self {
				t.Fatalf("the test needs %s to own %s in C, the owner is %s", tc.self, tc.key, owner)
			}
			got := h.peers(tc.key)
			if !slices.Equal(got, tc.want) {
				t.Errorf("peers(%s) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

// VAC-400, VAC-500 step 1: a view that is equal to the current one for the kind (same joined flag
// and hosting members) replaces it but does not become P, so an unrelated view change after the
// join does not drop the handoff peers of a key this node took over at the join. A view that
// changes the hosting members, or the joined flag, shifts the views.
func TestHandoffEqualViewsKeepPrevious(t *testing.T) {
	h := unitHost("n4")
	h.shift(peerView(1, "n4:a:counter"), false)
	all := []string{"n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter"}
	h.shift(peerView(2, all...), false) // the members came, not joined yet
	h.shift(peerView(2, all...), true)  // the same version, now joined
	expectStrings(t, "peers of k0 after the join", h.peers("k0"), []string{"n1"})

	// An unrelated change: another kind's member, a suspicion, an incarnation.
	h.shift(peerView(3, slices.Concat(all, []string{"x:a:other"})...), true)
	h.shift(peerView(4, "n1:s:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter", "x:d:other"), true)
	if h.version != 4 {
		t.Fatalf("version %d, want 4", h.version)
	}
	expectStrings(t, "peers of k0 after views equal for the kind", h.peers("k0"), []string{"n1"})

	// A change of the hosting members shifts the views: this node owned k0 in P.
	h.shift(peerView(5, "n1:a:counter", "n2:d:counter", "n3:a:counter", "n4:a:counter"), true)
	if got := h.peers("k0"); got != nil {
		t.Errorf("peers of k0 after a change of the hosting members = %q, want none", got)
	}
}

// AT-VAC-29 (acquisitions, acks and timeout), VAC-310 rule 6, VAC-410, VAC-520, VAC-540: n1 knows
// n9, a fake host, and its host restarted, so P is none and n9, the runner-up, is the peer of
// every key n1 owns. The first message acquires the key: a handoffReq to n9, the handoff record,
// the key's messages held (HostInfo.Acquiring); the ack activates it (handoff.ack remaining=0,
// activate handoff=acked) and delivers the held messages in order. Without an ack, the timeout
// activates it at HandoffTimeout (handoff.timeout missing=n9, handoff=timeout). Stale acks (another
// Seq, a peer the acquisition does not wait for, a duplicate, an ack after the timeout) do
// nothing. A host restart while acquiring drops the held messages with reason host; the next
// acquisition has a new Seq, so the ack of the lost one does not count for it.
func TestATVAC29Acquire(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		fake := c.fakeHostNode("n9", true)
		cfg := simConfig()
		cfg.Kinds = []Kind{logKind("counter", l, WithHandoffTimeout(2*time.Second))}
		cfg.Registry = grainRegistry()
		c.node("n1", cfg)
		w.RunFor(10 * time.Millisecond)
		c.addMember(tt, "n1", "n9", "counter")
		w.RunFor(10 * time.Millisecond)
		if err := s.InjectFail(hostPID("n1", "counter").Address(), errors.New("restart")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(100 * time.Millisecond)
		states := hostAcquiring(s, c, "n1")
		keys := ownedKeys("n1", []string{"n9"}, 4)
		ka, kt, ks, kr := keys[0], keys[1], keys[2], keys[3]
		id := uint64(c.id("n1")) << 32

		// Acked.
		sendLocal(tt, s, "n1", ka, gmsg{N: 1})
		sendLocal(tt, s, "n1", ka, gmsg{N: 2})
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, "n9 got", fake.msgs(tt), []string{"req " + ka + " n1 " + strconv.FormatUint(id|1, 10)})
		expectStrings(tt, ka+"'s records", c.keyRecs("n1", ka), []string{
			"handoff kind=counter key=" + ka + " peers=n9",
			"handoff.ack kind=counter key=" + ka + " from=n9 remaining=0",
			"activate kind=counter key=" + ka + " handoff=acked",
		})
		if got := l.of(ka); !slices.Equal(got, []int{1, 2}) {
			tt.Errorf("%s got %v, want [1 2]", ka, got)
		}
		expectStrings(tt, "HostInfo while acquiring", (*states)[:min(3, len(*states))], []string{
			"acquiring=[" + ka + "] active=[] buffered=1",
			"acquiring=[" + ka + "] active=[] buffered=2",
			"acquiring=[] active=[" + ka + "] buffered=0",
		})

		// Timeout; then an ack of it after the timeout, and a duplicate ack of ka.
		fake.ack = false
		sendLocal(tt, s, "n1", kt, gmsg{N: 3})
		w.RunFor(3 * time.Second)
		started := c.recAt(tt, "n1", "handoff", "kind=counter key="+kt+" peers=n9")
		if at := c.recAt(tt, "n1", "handoff.timeout", "kind=counter key="+kt+" missing=n9"); at != started.Add(2*time.Second) {
			tt.Errorf("timeout at %v, want HandoffTimeout after the handoff at %v", at, started)
		}
		sendHost(tt, s, "n9", "n1", handoffAck{Kind: "counter", Key: kt, From: "n9", Seq: fake.lastReq(tt, kt).Seq})
		sendHost(tt, s, "n9", "n1", handoffAck{Kind: "counter", Key: ka, From: "n9", Seq: fake.lastReq(tt, ka).Seq})
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, kt+"'s records", c.keyRecs("n1", kt), []string{
			"handoff kind=counter key=" + kt + " peers=n9",
			"handoff.timeout kind=counter key=" + kt + " missing=n9",
			"activate kind=counter key=" + kt + " handoff=timeout",
		})
		if got := l.of(kt); !slices.Equal(got, []int{3}) {
			tt.Errorf("%s got %v, want [3]", kt, got)
		}

		// Stale acks while acquiring: another Seq, a node that is no peer, a key that is not
		// escaped, another kind; then the right one.
		sendLocal(tt, s, "n1", ks, gmsg{N: 4})
		w.RunFor(100 * time.Millisecond)
		seq := fake.lastReq(tt, ks).Seq
		for _, a := range []handoffAck{
			{Kind: "counter", Key: ks, From: "n9", Seq: seq + 1},
			{Kind: "counter", Key: ks, From: "n7", Seq: seq},
			{Kind: "counter", Key: ks + "%", From: "n9", Seq: seq},
			{Kind: "other", Key: ks, From: "n9", Seq: seq},
		} {
			sendHost(tt, s, "n9", "n1", a)
		}
		w.RunFor(100 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: c.view("n1").Version, Joined: true,
			Active: sorted(ka, kt), Acquiring: []string{ks}, Buffered: 1})
		sendHost(tt, s, "n9", "n1", handoffAck{Kind: "counter", Key: ks, From: "n9", Seq: seq})
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, ks+"'s records", c.keyRecs("n1", ks), []string{
			"handoff kind=counter key=" + ks + " peers=n9",
			"handoff.ack kind=counter key=" + ks + " from=n9 remaining=0",
			"activate kind=counter key=" + ks + " handoff=acked",
		})

		// A restart while acquiring: the held message is dropped, the acquisition is lost, and
		// the ack of it does not count for the next one.
		sendLocal(tt, s, "n1", kr, gmsg{N: 5})
		w.RunFor(100 * time.Millisecond)
		lost := fake.lastReq(tt, kr).Seq
		if err := s.InjectFail(hostPID("n1", "counter").Address(), errors.New("restart")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(100 * time.Millisecond)
		sendLocal(tt, s, "n1", kr, gmsg{N: 6})
		w.RunFor(100 * time.Millisecond)
		next := fake.lastReq(tt, kr).Seq
		if lost != id|4 || next != id|5 {
			tt.Errorf("Seq of %s's acquisitions %d and %d, want %d and %d: the counter survives the restart", kr, lost, next, id|4, id|5)
		}
		sendHost(tt, s, "n9", "n1", handoffAck{Kind: "counter", Key: kr, From: "n9", Seq: lost})
		w.RunFor(100 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: c.view("n1").Version, Joined: true,
			Acquiring: []string{kr}, Buffered: 1})
		sendHost(tt, s, "n9", "n1", handoffAck{Kind: "counter", Key: kr, From: "n9", Seq: next})
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, kr+"'s records", c.keyRecs("n1", kr), []string{
			"handoff kind=counter key=" + kr + " peers=n9",
			"drop kind=counter key=" + kr + " reason=host hops=0",
			"handoff kind=counter key=" + kr + " peers=n9",
			"handoff.ack kind=counter key=" + kr + " from=n9 remaining=0",
			"activate kind=counter key=" + kr + " handoff=acked",
		})
		if got := l.of(kr); !slices.Equal(got, []int{6}) {
			tt.Errorf("%s got %v, want [6]", kr, got)
		}
		expectStrings(tt, "n1's drops", c.grainRecs("n1", "drop"), []string{"kind=counter key=" + kr + " reason=host hops=0"})
	})
}

// hostAcquiring registers an invariant that keeps node's HostInfo of counter after every
// simulation event, as "acquiring=<keys> active=<keys> buffered=<n>", whenever it differs from the
// last one kept; it starts with the first state that has a key acquiring.
func hostAcquiring(s *sim.Sim, c *simCluster, node string) *[]string {
	var states []string
	s.World().Invariant("cluster/host-acquiring", func() error {
		m := c.ms[node]
		if m == nil {
			return nil
		}
		info, _ := m.Host("counter")
		st := fmt.Sprintf("acquiring=%v active=%v buffered=%d", info.Acquiring, info.Active, info.Buffered)
		if len(states) == 0 && len(info.Acquiring) == 0 {
			return nil
		}
		if len(states) == 0 || states[len(states)-1] != st {
			states = append(states, st)
		}
		return nil
	})
	return &states
}

// grainOnly returns node's lockstep.grain records of counter, in order, as "<kind> <attrs>".
func (c *simCluster) grainOnly(node string) []string {
	return slices.DeleteFunc(c.grainTrace(node), func(r string) bool { return strings.HasPrefix(r, "send ") })
}

// from returns the records of recs from the first one of kind on, or nil.
func from(recs []string, kind string) []string {
	i := slices.IndexFunc(recs, func(r string) bool { return strings.HasPrefix(r, kind+" ") })
	if i < 0 {
		return nil
	}
	return recs[i:]
}

// lastN returns the last n records of recs, or all of them when there are fewer.
func lastN(recs []string, n int) []string { return recs[max(0, len(recs)-n):] }

// AT-VAC-29 (requests and yields), VAC-310 rule 2, VAC-510, VAC-530: n8, a fake host that n1 does
// not know, asks n1 to hand four keys over. Each request records lockstep.grain.yield and yields
// the key to n8; the key's held messages go to n8 at once, then: the active key is passivated
// (handover) and acked once it has stopped; the stopping key is acked once it has stopped; the
// acquisition is abandoned and acked, and its timer and a late ack of it do nothing; the key
// without state is acked at once. Every later message for the four keys goes to n8, and nothing
// activates on n1, until n1 applies a view with an entry for n8: a view without one keeps the
// yields. n1 owns the keys in every view of the test.
func TestATVAC29Requests(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		peer := c.fakeHostNode("n9", true)
		req := c.fakeHostNode("n8", false)
		c.fake("n7", false)
		cfg := simConfig()
		cfg.Kinds = []Kind{logKind("counter", l, WithHandoffTimeout(2*time.Second))}
		cfg.Registry = grainRegistry()
		c.node("n1", cfg)
		w.RunFor(10 * time.Millisecond)
		c.addMember(tt, "n1", "n9", "counter")
		w.RunFor(10 * time.Millisecond)
		if err := s.InjectFail(hostPID("n1", "counter").Address(), errors.New("restart")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(100 * time.Millisecond)
		keys := ownedKeys("n1", []string{"n8", "n9"}, 4)
		ka, ks, kq, kz := keys[0], keys[1], keys[2], keys[3]

		// ka and ks become active (n9 acks); kq acquires (n9 no longer acks).
		sendLocal(tt, s, "n1", ka, gmsg{N: 1})
		sendLocal(tt, s, "n1", ks, gmsg{N: 1})
		w.RunFor(100 * time.Millisecond)
		peer.ack = false
		sendLocal(tt, s, "n1", kq, gmsg{N: 1})
		sendLocal(tt, s, "n1", kq, gmsg{N: 2})
		w.RunFor(100 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: c.view("n1").Version, Joined: true,
			Active: sorted(ka, ks), Acquiring: []string{kq}, Buffered: 2})
		qseq := peer.lastReq(tt, kq).Seq

		sendHost(tt, s, "n8", "n1", handoffReq{Kind: "counter", Key: ka, From: "n8", Seq: 11})
		w.RunFor(100 * time.Millisecond)
		// ks stops and holds a message when the request comes, in one turn.
		callHost(tt, s, func(ctx *lockstep.Context[hostMsg], h *host) {
			h.passivate(ctx, ks, h.keys[ks], reasonIdle)
			h.route(ctx, ks, gmsg{N: 2})
			h.handoffReq(ctx, handoffReq{Kind: "counter", Key: ks, From: "n8", Seq: 12})
		})
		sendHost(tt, s, "n8", "n1", handoffReq{Kind: "counter", Key: kq, From: "n8", Seq: 13})
		sendHost(tt, s, "n8", "n1", handoffReq{Kind: "counter", Key: kz, From: "n8", Seq: 14})
		w.RunFor(3 * time.Second) // past kq's HandoffTimeout
		sendHost(tt, s, "n9", "n1", handoffAck{Kind: "counter", Key: kq, From: "n9", Seq: qseq})
		w.RunFor(100 * time.Millisecond)

		expectStrings(tt, ka+"'s records", from(c.keyRecs("n1", ka), "yield"), []string{
			"yield kind=counter key=" + ka + " to=n8",
			"passivate kind=counter key=" + ka + " reason=handover",
			"stopped kind=counter key=" + ka,
		})
		expectStrings(tt, ks+"'s records", from(c.keyRecs("n1", ks), "passivate"), []string{
			"passivate kind=counter key=" + ks + " reason=idle",
			"yield kind=counter key=" + ks + " to=n8",
			"forward kind=counter key=" + ks + " to=n8 hops=1",
			"stopped kind=counter key=" + ks,
		})
		expectStrings(tt, kq+"'s records", c.keyRecs("n1", kq), []string{
			"handoff kind=counter key=" + kq + " peers=n9",
			"yield kind=counter key=" + kq + " to=n8",
			"forward kind=counter key=" + kq + " to=n8 hops=1",
			"forward kind=counter key=" + kq + " to=n8 hops=1",
		})
		expectStrings(tt, kz+"'s records", c.keyRecs("n1", kz), []string{"yield kind=counter key=" + kz + " to=n8"})
		byKey := func(key string) []string {
			return slices.DeleteFunc(req.msgs(tt), func(m string) bool { return strings.Fields(m)[1] != key })
		}
		expectStrings(tt, "n8 got for "+ka, byKey(ka), []string{"ack " + ka + " n1 11"})
		expectStrings(tt, "n8 got for "+ks, byKey(ks), []string{"env " + ks + " 1 2", "ack " + ks + " n1 12"})
		expectStrings(tt, "n8 got for "+kq, byKey(kq), []string{"env " + kq + " 1 1", "env " + kq + " 1 2", "ack " + kq + " n1 13"})
		expectStrings(tt, "n8 got for "+kz, byKey(kz), []string{"ack " + kz + " n1 14"})
		for _, key := range []string{ka, ks} {
			stopped := c.recAt(tt, "n1", "stopped", "kind=counter key="+key)
			i := slices.IndexFunc(req.got, func(g fakeGot) bool { a, ok := g.msg.(handoffAck); return ok && a.Key == key })
			if i < 0 || req.got[i].at < stopped {
				tt.Errorf("the ack of %s reached n8 before the activation stopped at %v", key, stopped)
			}
		}
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: c.view("n1").Version, Joined: true})

		// Rule 2: every message for the keys goes to n8, also after a view without n8.
		for _, key := range keys {
			sendLocal(tt, s, "n1", key, gmsg{N: 3})
		}
		w.RunFor(100 * time.Millisecond)
		version := c.view("n1").Version
		c.addMember(tt, "n1", "n7")
		w.RunFor(100 * time.Millisecond)
		if c.view("n1").Version == version {
			tt.Fatal("n1 applied no view without n8")
		}
		for _, key := range keys {
			sendLocal(tt, s, "n1", key, gmsg{N: 4})
		}
		w.RunFor(100 * time.Millisecond)
		for _, key := range keys {
			want := []string{"env " + key + " 1 3", "env " + key + " 1 4"}
			expectStrings(tt, "n8's last messages for "+key, lastN(byKey(key), 2), want)
			expectStrings(tt, key+"'s last records", lastN(c.keyRecs("n1", key), 2), []string{
				"forward kind=counter key=" + key + " to=n8 hops=1",
				"forward kind=counter key=" + key + " to=n8 hops=1",
			})
		}

		// A view with an entry for n8 ends the yields: n1 owns the keys and activates them.
		c.addMember(tt, "n1", "n8", "counter")
		w.RunFor(100 * time.Millisecond)
		for _, key := range keys {
			sendLocal(tt, s, "n1", key, gmsg{N: 5})
		}
		w.RunFor(100 * time.Millisecond)
		for _, key := range keys {
			expectStrings(tt, key+"'s last record", lastN(c.keyRecs("n1", key), 1),
				[]string{"activate kind=counter key=" + key + " handoff=none"})
		}
		if got := l.of(ka); !slices.Equal(got, []int{1, 5}) {
			tt.Errorf("%s got %v on n1, want [1 5]", ka, got)
		}
		if got := l.of(kq); !slices.Equal(got, []int{5}) {
			tt.Errorf("%s got %v on n1, want [5]: its acquisition was abandoned", kq, got)
		}
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: c.view("n1").Version, Joined: true, Active: sorted(keys...)})
	})
}

// VAC-500: the order of a view application. n1 knows n9 (a fake host) and its host restarted, so
// n9 is the peer of n1's keys; ka is active, kq is acquiring with two held messages, ky is yielded
// to n8 and kz to n6, two fake hosts n1 does not know. Then n1 applies the view in which n8 comes
// up: n8 owns ka and kq. In that turn the host, in order, ends the yield to n8 (step 2),
// passivates ka with reason ownership (step 3), abandons kq and forwards its held messages to n8
// (step 4), and records lockstep.grain.view (step 6). Afterwards ky, which n1 owns, activates on
// n1, and kz still goes to n6. On a node that joins, the joining buffer is routed (step 5) before
// the view record.
func TestApplyViewOrder(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		peer := c.fakeHostNode("n9", true)
		n6 := c.fakeHostNode("n6", false)
		c.fakeHostNode("n8", false)
		cfg := simConfig()
		cfg.Kinds = []Kind{logKind("counter", l, WithHandoffTimeout(time.Minute))}
		cfg.Registry = grainRegistry()
		c.node("n1", cfg)
		cfg2 := simConfig("n2", "n5") // n5 does not exist: n2 joins only when told
		cfg2.JoinRetryInterval = time.Hour
		cfg2.Kinds = []Kind{logKind("counter", l)}
		c.node("n2", cfg2)
		w.RunFor(10 * time.Millisecond)
		c.addMember(tt, "n1", "n9", "counter")
		w.RunFor(10 * time.Millisecond)
		if err := s.InjectFail(hostPID("n1", "counter").Address(), errors.New("restart")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(100 * time.Millisecond)

		// n8 outranks n1, and n1 outranks n9: n1 owns them until n8 comes up.
		taken := slices.DeleteFunc(ownedKeys("n8", []string{"n1"}, 40), func(k string) bool {
			return Score("n9", "counter", k) > Score("n1", "counter", k)
		})
		kept := ownedKeys("n1", []string{"n8", "n9"}, 2)
		ka, kq, ky, kz := taken[0], taken[1], kept[0], kept[1]

		sendLocal(tt, s, "n1", ka, gmsg{N: 1})
		w.RunFor(100 * time.Millisecond)
		peer.ack = false
		sendLocal(tt, s, "n1", kq, gmsg{N: 1})
		sendLocal(tt, s, "n1", kq, gmsg{N: 2})
		sendHost(tt, s, "n8", "n1", handoffReq{Kind: "counter", Key: ky, From: "n8", Seq: 1})
		sendHost(tt, s, "n6", "n1", handoffReq{Kind: "counter", Key: kz, From: "n6", Seq: 1})
		w.RunFor(100 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: c.view("n1").Version, Joined: true,
			Active: []string{ka}, Acquiring: []string{kq}, Buffered: 2})

		c.addMember(tt, "n1", "n8", "counter")
		w.RunFor(100 * time.Millisecond)
		v := c.view("n1")
		recs := from(c.grainOnly("n1"), "passivate")
		expectStrings(tt, "the view's turn", recs[:min(4, len(recs))], []string{
			"passivate kind=counter key=" + ka + " reason=ownership",
			"forward kind=counter key=" + kq + " to=n8 hops=1",
			"forward kind=counter key=" + kq + " to=n8 hops=1",
			"view kind=counter version=" + strconv.FormatUint(v.Version, 10) + " joined=true active=0",
		})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: v.Version, Joined: true})

		sendLocal(tt, s, "n1", ky, gmsg{N: 3})
		sendLocal(tt, s, "n1", kz, gmsg{N: 4})
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, ky+"'s records", c.keyRecs("n1", ky), []string{
			"yield kind=counter key=" + ky + " to=n8",
			"activate kind=counter key=" + ky + " handoff=none",
		})
		expectStrings(tt, kz+"'s records", c.keyRecs("n1", kz), []string{
			"yield kind=counter key=" + kz + " to=n6",
			"forward kind=counter key=" + kz + " to=n6 hops=1",
		})
		expectStrings(tt, "n6 got", n6.msgs(tt), []string{"ack " + kz + " n1 1", "env " + kz + " 1 4"})

		// Step 5 before step 6.
		sendLocal(tt, s, "n2", "x", gmsg{N: 1})
		w.RunFor(10 * time.Millisecond)
		joinAs(tt, s, "n2", "n5")
		w.RunFor(10 * time.Millisecond)
		v2 := c.view("n2")
		expectStrings(tt, "n2's records from the join on", lastN(c.grainOnly("n2"), 2), []string{
			"activate kind=counter key=x handoff=none",
			"view kind=counter version=" + strconv.FormatUint(v2.Version, 10) + " joined=true active=1",
		})
	})
}

// VAC-400, VAC-510, VAC-520, VAC-810 (one join, no faults): n1 and n3 form a cluster and k is
// active on n1. n2 joins through n3 and takes k over before n1 has heard of it: n2's previous
// view was not joined, so it asks n1, the runner-up; n1 yields k to n2, passivates it (handover)
// and acks once it has stopped, and only then does n2 activate it (handoff=acked). The message n1
// sends to k meanwhile reaches k once. In the end k is active only on n2.
func TestHandoffJoin(t *testing.T) {
	sim.Simulate(t, simOpts(3, 30*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		for _, n := range []string{"n1", "n3"} {
			cfg := simConfig("n1", "n3")
			cfg.Kinds = []Kind{logKind("counter", l)}
			cfg.Registry = grainRegistry()
			c.node(n, cfg)
		}
		w.RunFor(2 * time.Second)
		var k string
		for i := 0; k == ""; i++ {
			key := "k" + strconv.Itoa(i)
			if r := refRank([]string{"n1", "n2", "n3"}, "counter", key); r[0] == "n2" && r[1] == "n1" {
				k = key
			}
		}
		send := func(node string, n int) {
			tt.Helper()
			if err := GrainOf[gmsg](c.ms[node], "counter", k).Send(s.System(node), gmsg{N: n}); err != nil {
				tt.Fatal(err)
			}
		}
		send("n1", 1)
		w.RunFor(100 * time.Millisecond)

		cfg := simConfig("n3")
		cfg.Kinds = []Kind{logKind("counter", l)}
		cfg.Registry = grainRegistry()
		c.node("n2", cfg)
		for c.ms["n2"] == nil || !c.ms["n2"].joined.Load() {
			w.RunFor(time.Millisecond)
		}
		if _, ok := c.view("n1").Member("n2"); ok {
			tt.Fatal("n1 knows n2 already: the test needs n2's request to come first")
		}
		send("n2", 2)
		send("n1", 3)
		w.RunFor(time.Second)

		expectStrings(tt, "n2's records of "+k, c.keyRecs("n2", k), []string{
			"handoff kind=counter key=" + k + " peers=n1",
			"handoff.ack kind=counter key=" + k + " from=n1 remaining=0",
			"activate kind=counter key=" + k + " handoff=acked",
		})
		expectStrings(tt, "n1's records of "+k+" from the yield on", from(c.keyRecs("n1", k), "yield"), []string{
			"yield kind=counter key=" + k + " to=n2",
			"passivate kind=counter key=" + k + " reason=handover",
			"stopped kind=counter key=" + k,
		})
		stopped := c.recAt(tt, "n1", "stopped", "kind=counter key="+k)
		if act := c.recAt(tt, "n2", "activate", "kind=counter key="+k+" handoff=acked"); act < stopped {
			tt.Errorf("n2 activated %s at %v, before n1's activation stopped at %v", k, act, stopped)
		}
		if got := slices.Sorted(slices.Values(l.of(k))); !slices.Equal(got, []int{1, 2, 3}) {
			tt.Errorf("%s got %v, want 1, 2 and 3 once each", k, l.of(k))
		}
		w.RunFor(5 * time.Second)
		for _, n := range []string{"n1", "n2", "n3"} {
			info, _ := c.ms[n].Host("counter")
			if want := n == "n2"; slices.Contains(info.Active, k) != want {
				tt.Errorf("%s's Active is %q: want %s active there %v", n, info.Active, k, want)
			}
		}
	})
}
