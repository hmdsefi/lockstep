// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/sim"
)

// gmsg is the message type of the test grains: N is added to the grain's sum, a non-zero Reply is
// answered with the sum, Stop makes the activation stop after it and Fail makes it fail after it.
type gmsg struct {
	N     int
	Stop  bool
	Fail  bool
	Reply lockstep.ReplyTo[int]
}

// grainLog is what the test grains of one simulation run saw, in turn order. The simulation runs
// one turn at a time, so the grains share it without a lock.
type grainLog struct {
	made []string // the keys the factory was called with
	got  []string // "<key>:<N>" per message
}

// of returns the N of the messages key got, in order.
func (l *grainLog) of(key string) []int {
	var out []int
	for _, e := range l.got {
		k, n, _ := strings.Cut(e[len(key):], ":")
		if strings.HasPrefix(e, key+":") && k == "" {
			v, err := strconv.Atoi(n)
			if err != nil {
				panic(err)
			}
			out = append(out, v)
		}
	}
	return out
}

// logKind returns the kind name whose activations log to l (see gmsg).
func logKind(name string, l *grainLog, opts ...KindOption) Kind {
	return NewKind(name, func(key string) lockstep.Actor[gmsg] {
		l.made = append(l.made, key)
		sum := 0
		return actorFunc[gmsg](func(ctx *lockstep.Context[gmsg], m gmsg) {
			l.got = append(l.got, key+":"+strconv.Itoa(m.N))
			sum += m.N
			if m.Reply != (lockstep.ReplyTo[int]{}) {
				m.Reply.Send(ctx, sum)
			}
			if m.Stop {
				ctx.Stop()
			}
			if m.Fail {
				ctx.Fail(errors.New("grain failed"))
			}
		})
	}, opts...)
}

// grainRecs returns node name's lockstep.grain.<kind> records, in order, as their attributes.
func (c *simCluster) grainRecs(name, kind string) []string {
	id := c.n[name].ID()
	var out []string
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && r.Kind == grainPrefix+kind {
			out = append(out, attrs(r))
		}
	}
	return out
}

// hostPID returns the path PID of the host of kind on node.
func hostPID(node, kind string) lockstep.PID[hostMsg] {
	return lockstep.PIDAt[hostMsg](node, grainsPath+"/"+kind)
}

// sendLocal sends a localGrain for key straight to the host of counter on node, as Grain.Send does
// without its checks.
func sendLocal(tt testing.TB, s *sim.Sim, node, key string, msg any) {
	tt.Helper()
	if err := sim.Send(s, node, hostPID(node, "counter"), hostMsg(localGrain{Key: key, Msg: msg})); err != nil {
		tt.Fatalf("send counter/%s to the host: %v", key, err)
	}
}

// expectHost fails tt unless node's HostInfo of kind is want.
func expectHost(tt testing.TB, c *simCluster, node string, want HostInfo) {
	tt.Helper()
	got, ok := c.ms[node].Host(want.Kind)
	if !ok || got.Kind != want.Kind || got.Version != want.Version || got.Joined != want.Joined ||
		!slices.Equal(got.Active, want.Active) || !slices.Equal(got.Stopping, want.Stopping) || !slices.Equal(got.Acquiring, want.Acquiring) ||
		got.Buffered != want.Buffered {
		tt.Errorf("%s's Host(%q) = %+v, %v, want %+v", node, want.Kind, got, ok, want)
	}
}

// expectStrings fails tt unless got equals want.
func expectStrings(tt testing.TB, what string, got, want []string) {
	tt.Helper()
	if !slices.Equal(got, want) {
		tt.Errorf("%s:\n got %q\nwant %q", what, got, want)
	}
}

// joinAs makes node, whose seeds name only node and a node that does not exist, join by sending
// its membership actor a syncResp from that node (CLU-030).
func joinAs(tt testing.TB, s *sim.Sim, node, from string) {
	tt.Helper()
	msg := memberMsg(syncResp{From: update{Node: from, ID: 99, State: Alive}})
	if err := sim.Send(s, node, lockstep.PIDAt[memberMsg](node, actorPath), msg); err != nil {
		tt.Fatal(err)
	}
}

// AT-VAC-09, VAC-310 rule 1, VAC-340, VAC-350, VAC-500 step 5: until the node has joined, its host
// holds every message in the joining buffer, bounded by the kind's Buffer across keys; a message
// that does not fit is dropped with reason buffer. When the node joins, the host routes the
// buffer in arrival order: each key activates once and gets its messages in order.
func TestATVAC09Buffer(t *testing.T) {
	sim.Simulate(t, simOpts(2, 3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		cfg := simConfig("n1", "n9") // n9 does not exist: n1 joins only when told
		cfg.JoinRetryInterval = time.Hour
		cfg.Kinds = []Kind{logKind("counter", l, WithBuffer(3))}
		c.node("n1", cfg)
		w.RunFor(10 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1})

		for i, key := range []string{"k1", "k2", "k1", "k3", "k1"} {
			sendLocal(tt, s, "n1", key, gmsg{N: i})
		}
		w.RunFor(10 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Buffered: 3})
		expectStrings(tt, "drops", c.grainRecs("n1", "drop"), []string{
			"kind=counter key=k3 reason=buffer hops=0",
			"kind=counter key=k1 reason=buffer hops=0",
		})
		if len(l.made) != 0 || len(l.got) != 0 {
			tt.Errorf("activations before the node joined: made %q, got %q", l.made, l.got)
		}

		joinAs(tt, s, "n1", "n9")
		w.RunFor(10 * time.Millisecond)
		v := c.view("n1")
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: v.Version, Joined: true, Active: []string{"k1", "k2"}})
		expectStrings(tt, "activations", c.grainRecs("n1", "activate"), []string{
			"kind=counter key=k1 handoff=none",
			"kind=counter key=k2 handoff=none",
		})
		expectStrings(tt, "factory calls (sorted: each activation calls it in its own first turn)", sorted(l.made...), []string{"k1", "k2"})
		if got := l.of("k1"); !slices.Equal(got, []int{0, 2}) {
			tt.Errorf("k1 got %v, want [0 2]", got)
		}
		if got := l.of("k2"); !slices.Equal(got, []int{1}) {
			tt.Errorf("k2 got %v, want [1]", got)
		}
		views := c.grainRecs("n1", "view")
		want := "kind=counter version=" + strconv.FormatUint(v.Version, 10) + " joined=true active=2"
		if len(views) < 2 || views[0] != "kind=counter version=1 joined=false active=0" || views[len(views)-1] != want {
			tt.Errorf("view records %q, want version=1 joined=false first and %q last", views, want)
		}
		if n := len(c.grainRecs("n1", "drop")); n != 2 {
			tt.Errorf("%d drops after the join, want the 2 of before", n)
		}
	})
}

// VAC-300: Join spawns /system/grains and a host per kind, in Config.Kinds order, also for a node
// without kinds; before its first turn a host's HostInfo is only its Kind. Host is false for a
// kind the node does not host.
func TestGrainsTree(t *testing.T) {
	sim.Simulate(t, simOpts(1, time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		var early []HostInfo
		boot := func(name string, kinds ...Kind) {
			cfg := simConfig()
			cfg.Kinds = kinds
			c.n[name] = s.Node(name, func(sys *lockstep.System) {
				m, err := Join(sys, cfg)
				if err != nil {
					panic(err)
				}
				c.ms[name] = m
				for _, k := range kinds {
					info, _ := m.Host(k.Name())
					early = append(early, info)
				}
			})
		}
		boot("n1", validKind("zeta"), validKind("alpha"))
		boot("n2")
		w.RunFor(10 * time.Millisecond)

		if want := []HostInfo{{Kind: "zeta"}, {Kind: "alpha"}}; len(early) != 2 || early[0].Kind != "zeta" ||
			early[0].Version != 0 || early[1].Kind != "alpha" || early[1].Version != 0 {
			tt.Errorf("HostInfo before the first turn: %+v, want %+v", early, want)
		}
		sys := s.System("n1")
		if _, err := lockstep.Lookup[grainsMsg](sys, grainsPath); err != nil {
			tt.Errorf("n1's grains actor: %v", err)
		}
		if _, err := lockstep.Lookup[grainsMsg](s.System("n2"), grainsPath); err != nil {
			tt.Errorf("n2's grains actor: %v", err)
		}
		var spawned []string
		for _, r := range s.World().Sim.Records() {
			if r.Kind == "lockstep.spawn" && strings.HasPrefix(attr(r, "addr"), "lockstep://n1"+grainsPath+"/") {
				spawned = append(spawned, attr(r, "name"))
			}
		}
		expectStrings(tt, "n1's hosts in spawn order", spawned, []string{"zeta", "alpha"})
		for _, k := range []string{"zeta", "alpha"} {
			if _, err := lockstep.Lookup[hostMsg](sys, grainsPath+"/"+k); err != nil {
				tt.Errorf("n1's host of %s: %v", k, err)
			}
			expectHost(tt, c, "n1", HostInfo{Kind: k, Version: 1, Joined: true})
		}
		for _, n := range []string{"n1", "n2"} {
			if info, ok := c.ms[n].Host("other"); ok || info.Kind != "" {
				tt.Errorf("%s's Host(other) = %+v, %v, want false", n, info, ok)
			}
		}
		if info, ok := new(Membership).Host("zeta"); ok {
			tt.Errorf("the zero Membership's Host = %+v, true", info)
		}
	})
}

// VAC-410, VAC-440 (the part 3.4-03 needs): an activation that stops by itself loses its state at
// its Terminated (lockstep.grain.stopped), its name is free by then, and the next message
// activates the key again with a new incarnation of the grain.
func TestHostReactivatesAfterStop(t *testing.T) {
	sim.Simulate(t, simOpts(2, time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		cfg := simConfig()
		cfg.Kinds = []Kind{logKind("counter", l)}
		c.node("n1", cfg)
		w.RunFor(10 * time.Millisecond)
		m, sys := c.ms["n1"], s.System("n1")
		g := GrainOf[gmsg](m, "counter", "k")

		for _, msg := range []gmsg{{N: 1}, {N: 2, Stop: true}} {
			if err := g.Send(sys, msg); err != nil {
				tt.Fatal(err)
			}
		}
		w.RunFor(10 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true})
		expectStrings(tt, "stopped", c.grainRecs("n1", "stopped"), []string{"kind=counter key=k"})

		r := Ask(sys, g, func(rt lockstep.ReplyTo[int]) gmsg { return gmsg{N: 5, Reply: rt} })
		w.RunFor(10 * time.Millisecond)
		if v, err := awaitResolved(tt, r); err != nil || v != 5 {
			tt.Errorf("the new activation answered %d, %v, want 5 (a fresh sum)", v, err)
		}
		expectStrings(tt, "factory calls", l.made, []string{"k", "k"})
		expectStrings(tt, "activations", c.grainRecs("n1", "activate"), []string{
			"kind=counter key=k handoff=none",
			"kind=counter key=k handoff=none",
		})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true, Active: []string{"k"}})
	})
}

// VAC-450 (the part 3.4-03 needs), SUP-190: a host that fails restarts empty: its activations,
// its children, were stopped by the restart, its held messages are dropped with reason host, and
// it applies the latest view again; the next message activates the key again. The grains actor
// restarting respawns the hosts, which the path PIDs reach again.
func TestHostRestartStartsEmpty(t *testing.T) {
	sim.Simulate(t, simOpts(2, 5*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		cfg := simConfig()
		cfg.Kinds = []Kind{logKind("counter", l)}
		c.node("n1", cfg)
		cfg2 := simConfig("n2", "n9")
		cfg2.JoinRetryInterval = time.Hour
		cfg2.Kinds = []Kind{logKind("counter", l)}
		c.node("n2", cfg2)
		w.RunFor(10 * time.Millisecond)
		m, sys := c.ms["n1"], s.System("n1")

		send := func(key string, n int) {
			tt.Helper()
			if err := GrainOf[gmsg](m, "counter", key).Send(sys, gmsg{N: n}); err != nil {
				tt.Fatal(err)
			}
		}
		send("a", 1)
		send("b", 2)
		w.RunFor(10 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true, Active: []string{"a", "b"}})

		if err := s.InjectFail(hostPID("n1", "counter").Address(), errors.New("boom")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true})
		for _, key := range []string{"a", "b"} {
			if _, err := lockstep.Lookup[gmsg](sys, grainsPath+"/counter/"+key); !errors.Is(err, lockstep.ErrNotFound) {
				tt.Errorf("activation %s after the host's restart: %v, want ErrNotFound", key, err)
			}
		}
		send("a", 3)
		w.RunFor(10 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true, Active: []string{"a"}})
		if got := l.of("a"); !slices.Equal(got, []int{1, 3}) {
			tt.Errorf("a got %v, want [1 3]", got)
		}
		if n := len(c.grainRecs("n1", "activate")); n != 3 {
			tt.Errorf("%d activations on n1, want 3 (a, b, then a again)", n)
		}

		// The grains actor restarts: it stops the hosts and spawns them again.
		if err := s.InjectFail(lockstep.PIDAt[grainsMsg]("n1", grainsPath).Address(), errors.New("boom")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true})
		send("a", 4)
		w.RunFor(10 * time.Millisecond)
		if got := l.of("a"); !slices.Equal(got, []int{1, 3, 4}) {
			tt.Errorf("a got %v after the grains actor's restart, want [1 3 4]", got)
		}

		// n2 has not joined: its host holds; a restart drops what it held with reason host.
		sendLocal(tt, s, "n2", "x", gmsg{N: 1})
		sendLocal(tt, s, "n2", "y", gmsg{N: 2})
		w.RunFor(10 * time.Millisecond)
		expectHost(tt, c, "n2", HostInfo{Kind: "counter", Version: 1, Buffered: 2})
		if err := s.InjectFail(hostPID("n2", "counter").Address(), errors.New("boom")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)
		expectHost(tt, c, "n2", HostInfo{Kind: "counter", Version: 1})
		expectStrings(tt, "n2's drops", c.grainRecs("n2", "drop"), []string{
			"kind=counter key=x reason=host hops=0",
			"kind=counter key=y reason=host hops=0",
		})
	})
}

// VAC-310 rules 4 to 7, VAC-500 step 3: a message for a key that another node owns is forwarded to
// it; an active key that another node owns in a new view is passivated with reason ownership, so
// its next message is forwarded too; a key this node owns activates; once this node has left, its
// active keys are passivated with reason leave and every key is dropped with reason no-owner (no
// up member hosts the kind). The host applies the views the membership actor publishes (CLU-500):
// HostInfo.Version follows View().Version. n1 and n2 do not know each other.
func TestHostRoutingRules(t *testing.T) {
	sim.Simulate(t, simOpts(2, 3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		cfg := simConfig()
		cfg.Kinds = []Kind{logKind("counter", l)}
		cfg.Registry = grainRegistry()
		c.node("n1", cfg)
		c.node("n2", cfg)
		w.RunFor(10 * time.Millisecond)
		m, sys := c.ms["n1"], s.System("n1")

		// a will host counter too: split some keys by their owner once a is up.
		var mine, theirs []string
		for i := range 20 {
			key := "k" + strconv.Itoa(i)
			if Score("n1", "counter", key) > Score("a", "counter", key) {
				mine = append(mine, key)
			} else {
				theirs = append(theirs, key)
			}
		}
		if len(mine) < 2 || len(theirs) < 2 {
			tt.Fatalf("keys of n1 %q and of a %q: the test needs two of each", mine, theirs)
		}
		// Before a is up, n1 owns every key: theirs[0] becomes active on n1.
		active := theirs[0]
		if err := GrainOf[gmsg](m, "counter", active).Send(sys, gmsg{N: 1}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)

		if err := sim.Send(s, "n1", lockstep.PIDAt[memberMsg]("n1", actorPath),
			memberMsg(syncReq{From: update{Node: "a", ID: 7, State: Alive, Kinds: []string{"counter"}}})); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		v := m.View()
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: v.Version, Joined: true})
		expectStrings(tt, "passivations", c.grainRecs("n1", "passivate"), []string{"kind=counter key=" + active + " reason=ownership"})
		expectStrings(tt, "stopped", c.grainRecs("n1", "stopped"), []string{"kind=counter key=" + active})
		for _, key := range theirs {
			if o, _ := Owner(v, "counter", key); o != "a" {
				tt.Fatalf("Owner(%s) = %s, want a", key, o)
			}
		}
		sendLocal(tt, s, "n1", theirs[1], gmsg{N: 2})
		sendLocal(tt, s, "n1", active, gmsg{N: 3})
		sendLocal(tt, s, "n1", mine[0], gmsg{N: 4})
		w.RunFor(10 * time.Millisecond)
		expectStrings(tt, "forwards", c.grainRecs("n1", "forward"), []string{
			"kind=counter key=" + theirs[1] + " to=a hops=1",
			"kind=counter key=" + active + " to=a hops=1",
		})
		expectStrings(tt, "drops", c.grainRecs("n1", "drop"), nil)
		if got := l.of(mine[0]); !slices.Equal(got, []int{4}) {
			tt.Errorf("%s got %v, want [4]", mine[0], got)
		}

		// n2 has no other member. After it leaves, it is not up in its view: its active key is
		// passivated with reason leave, and no up member hosts counter, so every key has no owner
		// (rule 7).
		m2, sys2 := c.ms["n2"], s.System("n2")
		if err := GrainOf[gmsg](m2, "counter", "x").Send(sys2, gmsg{N: 7}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		if err := m2.Leave(); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		v = m2.View()
		if self, _ := v.Member("n2"); self.State != Left {
			tt.Fatalf("n2 is %v after Leave", self.State)
		}
		expectHost(tt, c, "n2", HostInfo{Kind: "counter", Version: v.Version, Joined: true})
		expectStrings(tt, "n2's passivations", c.grainRecs("n2", "passivate"), []string{"kind=counter key=x reason=leave"})
		sendLocal(tt, s, "n2", "x", gmsg{N: 8})
		sendLocal(tt, s, "n2", "y", gmsg{N: 9})
		w.RunFor(10 * time.Millisecond)
		if got := l.of("x"); !slices.Equal(got, []int{7}) {
			tt.Errorf("x got %v, want [7]", got)
		}
		expectStrings(tt, "n2's drops", c.grainRecs("n2", "drop"), []string{
			"kind=counter key=x reason=no-owner hops=0",
			"kind=counter key=y reason=no-owner hops=0",
		})
	})
}

// awaitResolved returns the value and error of r, failing tt if r has not resolved.
func awaitResolved[R any](tt testing.TB, r *lockstep.Reply[R]) (R, error) {
	tt.Helper()
	if !askResolved(r) {
		tt.Fatal("the reply has not resolved")
	}
	return r.Await(context.Background())
}

// askResolved reports, without waiting, whether r has resolved.
func askResolved[R any](r *lockstep.Reply[R]) bool { return hooks.AskResolved(r) }

// sorted returns a sorted copy of keys.
func sorted(keys ...string) []string {
	return slices.Sorted(slices.Values(keys))
}
