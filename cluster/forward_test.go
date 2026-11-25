// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"context"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
	"github.com/hmdsefi/tenure/core"
)

// tmsg is the message type of the test kind "text": S is logged, and F lets a test make a value
// that codec.JSON cannot encode (NaN).
type tmsg struct {
	S string
	F float64
}

// Wire names of the test messages in grainRegistry.
const (
	wireGmsg = "cluster.test.gmsg"
	wireTmsg = "cluster.test.tmsg"
)

// grainRegistry returns a registry that holds the test message types gmsg and tmsg.
func grainRegistry() *codec.Registry {
	r := codec.NewRegistry()
	codec.RegisterIn[gmsg](r, wireGmsg)
	codec.RegisterIn[tmsg](r, wireTmsg)
	return r
}

// textKind returns the kind "text", whose activations log "<key>:<S>" to got.
func textKind(got *[]string, opts ...KindOption) Kind {
	return NewKind("text", func(key string) lockstep.Actor[tmsg] {
		return actorFunc[tmsg](func(_ *lockstep.Context[tmsg], m tmsg) {
			*got = append(*got, key+":"+m.S)
		})
	}, opts...)
}

// twoNodes starts n1 and n2, which seed each other and host counter (logging to l) and text
// (logging to texts) with grainRegistry, after edit changed each node's Config, and runs until both
// views hold both nodes with their kinds.
func twoNodes(s *sim.Sim, l *grainLog, texts *[]string, edit func(node string, cfg *Config)) *simCluster {
	w, tt := s.World(), s.World().T()
	c := newSimCluster(s)
	for _, n := range []string{"n1", "n2"} {
		cfg := simConfig("n1", "n2")
		cfg.Kinds = []Kind{logKind("counter", l), textKind(texts)}
		cfg.Registry = grainRegistry()
		if edit != nil {
			edit(n, &cfg)
		}
		c.node(n, cfg)
	}
	w.RunFor(2 * time.Second)
	want := []Member{
		withKinds(alive("n1", c.id("n1")), "counter", "text"),
		withKinds(alive("n2", c.id("n2")), "counter", "text"),
	}
	for _, n := range []string{"n1", "n2"} {
		expectView(tt, n, c.view(n), want)
	}
	return c
}

// keyOf returns the first key "k<i>" of kind that owner owns in node's view.
func (c *simCluster) keyOf(node, kind, owner string) string {
	for i := 0; ; i++ {
		key := "k" + strconv.Itoa(i)
		if o, _ := Owner(c.view(node), kind, key); o == owner {
			return key
		}
	}
}

// envOf returns the envelope of v for key of kind with hops, encoded as Grain.Send does.
func envOf(tt testing.TB, kind, key string, hops int, v any) grainEnvelope {
	tt.Helper()
	name, data, err := grainRegistry().Encode(codec.JSON{}, v)
	if err != nil {
		tt.Fatal(err)
	}
	return grainEnvelope{Kind: kind, Key: escapeKey(key), Hops: hops, Name: name, Data: data}
}

// sendEnv sends env from outside the cluster to node's host of env.Kind, as another node's
// Grain.Send or host does.
func sendEnv(tt testing.TB, s *sim.Sim, node string, env grainEnvelope) {
	tt.Helper()
	if err := sim.Send(s, node, hostAt(node, env.Kind), hostMsg(env)); err != nil {
		tt.Fatalf("send an envelope to %s: %v", node, err)
	}
}

// AT-VAC-18 (Send), VAC-200, VAC-210, VAC-220, VAC-330: Send and Ask to a grain that another node
// owns encode the message and send it to the owner's host, which activates the grain and delivers
// the messages decoded, in send order; Ask's reply comes back across the nodes. The sending node
// keeps nothing and forwards nothing.
func TestATVAC18RemoteSend(t *testing.T) {
	sim.Simulate(t, simOpts(3, 3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		var texts []string
		c := twoNodes(s, l, &texts, nil)
		m1, s1 := c.ms["n1"], s.System("n1")
		key := c.keyOf("n1", "counter", "n2")

		g := GrainOf[gmsg](m1, "counter", key)
		for n := 1; n <= 5; n++ {
			if err := g.Send(s1, gmsg{N: n}); err != nil {
				tt.Fatal(err)
			}
		}
		r := Ask(s1, g, func(rt lockstep.ReplyTo[int]) gmsg { return gmsg{N: 10, Reply: rt} }, lockstep.WithTimeout(time.Second))
		w.RunFor(100 * time.Millisecond)
		if v, err := awaitResolved(tt, r); v != 25 || err != nil {
			tt.Errorf("ask across nodes = %d, %v, want 25", v, err)
		}
		if got := l.of(key); !slices.Equal(got, []int{1, 2, 3, 4, 5, 10}) {
			tt.Errorf("%s got %v, want [1 2 3 4 5 10] in send order", key, got)
		}
		expectStrings(tt, "n2's activations", c.grainRecs("n2", "activate"),
			[]string{"kind=counter key=" + key + " handoff=none"})
		for _, kind := range []string{"activate", "forward", "drop"} {
			expectStrings(tt, "n1's "+kind+" records", c.grainRecs("n1", kind), nil)
		}
		expectStrings(tt, "n2's forwards and drops", slices.Concat(c.grainRecs("n2", "forward"), c.grainRecs("n2", "drop")), nil)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: c.view("n1").Version, Joined: true})
		expectHost(tt, c, "n2", HostInfo{Kind: "counter", Version: c.view("n2").Version, Joined: true, Active: []string{key}})
	})
}

// AT-VAC-18 (forwarding), VAC-310 rule 4, VAC-320: a host forwards a message for a key that
// another node owns to that node's host with one hop more and records lockstep.grain.forward with
// the hops after the increment; an envelope that already made MaxHops hops is dropped with reason
// hops. An envelope stays encoded while it is forwarded: n2's registry does not hold the message
// types, and it forwards them all the same. A message held by reference is encoded first and
// forwarded with one hop; one that cannot be encoded is dropped with reason encode, and Send
// returns the encode error, which also fails Ask's reply. MaxHops is n2's, set to 2 here.
func TestATVAC18Forward(t *testing.T) {
	sim.Simulate(t, simOpts(3, 3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		var texts []string
		c := twoNodes(s, l, &texts, func(node string, cfg *Config) {
			if node == "n2" {
				cfg.Registry = codec.NewRegistry()
				cfg.MaxHops = 2
			}
		})
		key := c.keyOf("n2", "counter", "n1")

		sendEnv(tt, s, "n2", envOf(tt, "counter", key, 0, gmsg{N: 1}))
		sendEnv(tt, s, "n2", envOf(tt, "counter", key, 1, gmsg{N: 2}))
		sendEnv(tt, s, "n2", envOf(tt, "counter", key, 2, gmsg{N: 3})) // MaxHops
		sendEnv(tt, s, "n2", envOf(tt, "counter", key, 7, gmsg{N: 4})) // beyond MaxHops
		// By reference: n2 cannot encode it, since its registry does not hold gmsg.
		sendLocalKind(tt, s, "n2", "counter", key, gmsg{N: 5})
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, "n2's forwards", c.grainRecs("n2", "forward"), []string{
			"kind=counter key=" + key + " to=n1 hops=1",
			"kind=counter key=" + key + " to=n1 hops=2",
		})
		expectStrings(tt, "n2's drops", c.grainRecs("n2", "drop"), []string{
			"kind=counter key=" + key + " reason=hops hops=2",
			"kind=counter key=" + key + " reason=hops hops=7",
			"kind=counter key=" + key + " reason=encode hops=0",
		})
		if got := l.of(key); !slices.Equal(got, []int{1, 2}) {
			tt.Errorf("%s got %v on n1, want [1 2]", key, got)
		}
		expectStrings(tt, "n1's forwards and drops", slices.Concat(c.grainRecs("n1", "forward"), c.grainRecs("n1", "drop")), nil)

		// n1 encodes by reference: a message it holds for a key that n2 owns, and one it cannot
		// encode (NaN), which Send reports too.
		mine := c.keyOf("n1", "text", "n2")
		sendLocalKind(tt, s, "n1", "text", mine, tmsg{S: "held"})
		sendLocalKind(tt, s, "n1", "text", mine, tmsg{S: "nan", F: math.NaN()})
		m1, s1 := c.ms["n1"], s.System("n1")
		err := GrainOf[tmsg](m1, "text", mine).Send(s1, tmsg{F: math.NaN()})
		wantErr := "cluster: encode text/" + mine + ": codec: encode " + wireTmsg + ": json: unsupported value: NaN"
		if err == nil || err.Error() != wantErr {
			tt.Errorf("Send of NaN = %v, want %q", err, wantErr)
		}
		built := 0
		r := Ask(s1, GrainOf[tmsg](m1, "text", mine), func(lockstep.ReplyTo[int]) tmsg {
			built++
			return tmsg{F: math.Inf(1)}
		})
		if !askResolved(r) || built != 1 {
			tt.Errorf("Ask of +Inf: resolved %v with %d build calls when it returned, want true, 1", askResolved(r), built)
		} else if _, err := r.Await(context.Background()); err == nil || !strings.HasPrefix(err.Error(), "cluster: encode text/"+mine+": ") {
			tt.Errorf("Ask of +Inf = %v, want the encode error", err)
		}
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, "n1's forwards", c.grainRecs("n1", "forward"), []string{"kind=text key=" + mine + " to=n2 hops=1"})
		expectStrings(tt, "n1's drops", c.grainRecs("n1", "drop"), []string{"kind=text key=" + mine + " reason=encode hops=0"})
		// n2 got the held message, but cannot decode it: its registry does not hold tmsg.
		expectStrings(tt, "n2's drops of text", grepRecs(c.grainRecs("n2", "drop"), "kind=text "),
			[]string{"kind=text key=" + mine + " reason=decode hops=1"})
		if len(texts) != 0 {
			tt.Errorf("text activations got %q, want nothing", texts)
		}
	})
}

// sendLocalKind sends a localGrain for key straight to the host of kind on node.
func sendLocalKind(tt testing.TB, s *sim.Sim, node, kind, key string, msg any) {
	tt.Helper()
	if err := sim.Send(s, node, hostAt(node, kind), hostMsg(localGrain{Key: key, Msg: msg})); err != nil {
		tt.Fatalf("send %s/%s to the host: %v", kind, key, err)
	}
}

// callHostOn runs f in a turn of node's host of counter.
func callHostOn(tt testing.TB, s *sim.Sim, node string, f hostCall) {
	tt.Helper()
	if err := sim.Send(s, node, hostAt(node, "counter"), hostMsg(f)); err != nil {
		tt.Fatalf("send a call to %s's host: %v", node, err)
	}
}

// grepRecs returns the records of recs that start with prefix.
func grepRecs(recs []string, prefix string) []string {
	var out []string
	for _, r := range recs {
		if strings.HasPrefix(r, prefix) {
			out = append(out, r)
		}
	}
	return out
}

// AT-VAC-18 (no loops), VAC-320: while two hosts each think the other owns a key, a message for it
// bounces between them, one hop more at every forward, until the host that gets it with MaxHops
// hops drops it. A test call makes each host's view of the kind hold only the other node.
func TestATVAC18NoLoop(t *testing.T) {
	sim.Simulate(t, simOpts(3, 3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		var texts []string
		c := twoNodes(s, l, &texts, nil)
		for _, p := range [][2]string{{"n1", "n2"}, {"n2", "n1"}} {
			callHostOn(tt, s, p[0], func(_ *lockstep.Context[hostMsg], h *host) {
				h.members = []core.Member{{ID: p[1]}}
			})
		}
		w.RunFor(10 * time.Millisecond)
		sendEnv(tt, s, "n1", envOf(tt, "counter", "k", 0, gmsg{N: 1}))
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, "n1's forwards", c.grainRecs("n1", "forward"), []string{
			"kind=counter key=k to=n2 hops=1",
			"kind=counter key=k to=n2 hops=3",
		})
		expectStrings(tt, "n2's forwards", c.grainRecs("n2", "forward"), []string{"kind=counter key=k to=n1 hops=2"})
		expectStrings(tt, "n1's drops", c.grainRecs("n1", "drop"), nil)
		expectStrings(tt, "n2's drops", c.grainRecs("n2", "drop"), []string{"kind=counter key=k reason=hops hops=3"})
		if len(l.got) != 0 {
			tt.Errorf("grains got %q, want nothing", l.got)
		}
	})
}

// AT-VAC-18 (send errors), VAC-200, VAC-320: a message whose envelope does not fit in a frame
// (MaxFrame is 4096 here) cannot be sent: Send returns the send's error, wrapped, and a host that
// forwards it drops it with reason mailbox and the hops it had made.
func TestATVAC18ForwardSendError(t *testing.T) {
	opts := simOpts(2, 3*time.Second)
	opts.Remote.MaxFrame = 4096
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		var texts []string
		c := twoNodes(s, l, &texts, nil)
		key := c.keyOf("n1", "text", "n2")
		big := tmsg{S: strings.Repeat("x", 5000)}
		m1, s1 := c.ms["n1"], s.System("n1")
		err := GrainOf[tmsg](m1, "text", key).Send(s1, big)
		if !errors.Is(err, remote.ErrFrameTooLarge) || !strings.HasPrefix(err.Error(), "cluster: send text/"+key+": ") {
			tt.Errorf("Send of 5000 bytes = %v, want ErrFrameTooLarge wrapped", err)
		}
		sendLocalKind(tt, s, "n1", "text", key, big)
		sendEnv(tt, s, "n1", envOf(tt, "text", key, 1, big))
		sendLocalKind(tt, s, "n1", "text", key, tmsg{S: "small"})
		w.RunFor(100 * time.Millisecond)
		expectStrings(tt, "n1's drops", c.grainRecs("n1", "drop"), []string{
			"kind=text key=" + key + " reason=mailbox hops=0",
			"kind=text key=" + key + " reason=mailbox hops=1",
		})
		expectStrings(tt, "n1's forwards", c.grainRecs("n1", "forward"), []string{"kind=text key=" + key + " to=n2 hops=1"})
		expectStrings(tt, "texts", texts, []string{key + ":small"})
	})
}

// AT-VAC-24, VAC-330, VAC-350: the owner's host decodes an envelope only to deliver it: a wire name
// its registry does not hold, or bytes that do not decode, drop it with reason decode; a value of
// another type than the kind's drops it with reason wrong-type; a refused send to the activation
// drops it with reason mailbox. Each drop records the hops the message made. Neither a decode nor
// a wrong-type drop activates the key.
func TestATVAC24EnvelopeDelivery(t *testing.T) {
	sim.Simulate(t, simOpts(3, time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		cfg := simConfig()
		cfg.Kinds = []Kind{logKind("counter", l, WithActivationOptions(lockstep.WithMailbox(1, lockstep.FailSend)))}
		cfg.Registry = grainRegistry()
		c.node("n1", cfg)
		w.RunFor(10 * time.Millisecond)

		unknown := envOf(tt, "counter", "a", 1, gmsg{N: 1})
		unknown.Name = "cluster.test.unknown"
		bad := envOf(tt, "counter", "a", 2, gmsg{N: 1})
		bad.Data = []byte("{")
		wrong := envOf(tt, "counter", "a", 0, 7) // lockstep.int
		for _, env := range []grainEnvelope{unknown, bad, wrong} {
			sendEnv(tt, s, "n1", env)
		}
		w.RunFor(10 * time.Millisecond)
		expectStrings(tt, "drops before the activation", c.grainRecs("n1", "drop"), []string{
			"kind=counter key=a reason=decode hops=1",
			"kind=counter key=a reason=decode hops=2",
			"kind=counter key=a reason=wrong-type hops=0",
		})
		expectStrings(tt, "activations", c.grainRecs("n1", "activate"), nil)

		// An activation with a mailbox of 1: in one host turn, the first message activates it and
		// fills its mailbox, so the second is refused. The same holds for an active key (rule 5).
		callHost(tt, s, func(ctx *lockstep.Context[hostMsg], h *host) {
			for i, n := range []int{1, 2} {
				e := envOf(tt, "counter", "a", i+1, gmsg{N: n})
				h.route(ctx, "a", encoded{hops: e.Hops, name: e.Name, data: e.Data})
			}
		})
		w.RunFor(10 * time.Millisecond)
		callHost(tt, s, func(ctx *lockstep.Context[hostMsg], h *host) {
			h.route(ctx, "a", gmsg{N: 3})
			e := envOf(tt, "counter", "a", 3, gmsg{N: 4})
			h.route(ctx, "a", encoded{hops: e.Hops, name: e.Name, data: e.Data})
		})
		w.RunFor(10 * time.Millisecond)
		expectStrings(tt, "drops", c.grainRecs("n1", "drop")[3:], []string{
			"kind=counter key=a reason=mailbox hops=2",
			"kind=counter key=a reason=mailbox hops=3",
		})
		if got := l.of("a"); !slices.Equal(got, []int{1, 3}) {
			tt.Errorf("a got %v, want [1 3]", got)
		}
		expectStrings(tt, "activations", c.grainRecs("n1", "activate"), []string{"kind=counter key=a handoff=none"})
	})
}

// VAC-310, VAC-330, VAC-700: a host routes an envelope by its unescaped key, also one that made
// more than MaxHops hops when this node owns the key: hops matter only to forwarding. An envelope
// whose key escapeKey cannot have made, whose kind is not the host's or whose Hops is negative is
// dropped with reason decode; its record carries the received key escaped again.
func TestEnvelopeDecode(t *testing.T) {
	sim.Simulate(t, simOpts(2, time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		cfg := simConfig()
		cfg.Kinds = []Kind{logKind("counter", l)}
		cfg.Registry = grainRegistry()
		c.node("n1", cfg)
		w.RunFor(10 * time.Millisecond)

		good := []struct {
			key  string
			hops int
		}{{"a/b", 0}, {".", 1}, {"\xff", 9}, {strings.Repeat("%", 256), 0}}
		for i, g := range good {
			sendEnv(tt, s, "n1", envOf(tt, "counter", g.key, g.hops, gmsg{N: i + 1}))
		}
		bad := []struct {
			key, kind string
			hops      int
		}{
			{"", "counter", 0},
			{"%2f", "counter", 0},                    // lowercase hex
			{"%61", "counter", 0},                    // a kept byte escaped
			{"a b", "counter", 0},                    // a byte escapeKey escapes
			{"%2", "counter", 0},                     // cut short
			{"..", "counter", 0},                     // the dots of ".." are escaped
			{strings.Repeat("a", 257), "counter", 0}, // too long
			{"a", "other", 0},                        // another kind
			{"a", "counter", -1},                     // negative hops
		}
		for _, b := range bad {
			env := envOf(tt, "counter", "x", b.hops, gmsg{N: 100})
			env.Key, env.Kind = b.key, b.kind
			if err := sim.Send(s, "n1", hostAt("n1", "counter"), hostMsg(env)); err != nil {
				tt.Fatal(err)
			}
		}
		w.RunFor(10 * time.Millisecond)

		var got []string
		for i, g := range good {
			got = append(got, g.key+":"+strconv.Itoa(i+1))
		}
		// Activations of different keys run in an order of the seed's.
		expectStrings(tt, "deliveries", sorted(l.got...), sorted(got...))
		var want []string
		for _, b := range bad {
			want = append(want, "kind=counter key="+escapeKey(b.key)+" reason=decode hops="+strconv.Itoa(b.hops))
		}
		expectStrings(tt, "drops", c.grainRecs("n1", "drop"), want)
	})
}
