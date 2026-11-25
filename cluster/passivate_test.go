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
	"github.com/hmdsefi/lockstep/sim"
)

// hostStates registers an invariant that keeps node's HostInfo of kind after every simulation
// event, as a summary, whenever it differs from the last one kept; it starts with the first state
// that has a key.
func hostStates(s *sim.Sim, c *simCluster, node, kind string) *[]string {
	var states []string
	s.World().Invariant("cluster/host-states", func() error {
		m := c.ms[node]
		if m == nil {
			return nil
		}
		info, _ := m.Host(kind)
		st := fmt.Sprintf("active=%v stopping=%v buffered=%d", info.Active, info.Stopping, info.Buffered)
		if len(states) == 0 && len(info.Active)+len(info.Stopping) == 0 {
			return nil
		}
		if len(states) == 0 || states[len(states)-1] != st {
			states = append(states, st)
		}
		return nil
	})
	return &states
}

// callHost runs f in a turn of n1's host of counter.
func callHost(tt testing.TB, s *sim.Sim, f hostCall) {
	tt.Helper()
	if err := sim.Send(s, "n1", lockstep.PIDAt[hostMsg]("n1", grainsPath+"/counter"), hostMsg(f)); err != nil {
		tt.Fatalf("send a call to the host: %v", err)
	}
}

// grainTrace returns node's lockstep.grain records of kind counter and the lockstep.send records
// whose sender is the host of counter, in order, as "<kind> <attrs>" for a grain record and
// "send <to> <msg type>" for a send, where to is an activation's key, or host for a host of
// counter.
func (c *simCluster) grainTrace(node string) []string {
	id := c.n[node].ID()
	host := "lockstep://" + node + grainsPath + "/counter"
	var out []string
	for _, r := range c.s.World().Sim.Records() {
		switch {
		case r.Node != id:
		case strings.HasPrefix(r.Kind, grainPrefix):
			out = append(out, strings.TrimPrefix(r.Kind, grainPrefix)+" "+attrs(r))
		case r.Kind == "lockstep.send" && pathOf(attr(r, "from")) == host:
			to := strings.TrimPrefix(strings.TrimPrefix(pathOf(attr(r, "to")), host), "/")
			if to == "" {
				to = "host"
			}
			out = append(out, "send "+to+" "+attr(r, "msg"))
		}
	}
	return out
}

// pathOf returns the address text a without its UID suffix.
func pathOf(a string) string {
	if i := strings.LastIndexByte(a, '#'); i >= 0 {
		return a[:i]
	}
	return a
}

// deliveries returns the times at which node's host of counter sent a message to the activation
// of key.
func (c *simCluster) deliveries(node, key string) []kernel.Time {
	id := c.n[node].ID()
	host := "lockstep://" + node + grainsPath + "/counter"
	var out []kernel.Time
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && r.Kind == "lockstep.send" && pathOf(attr(r, "from")) == host && pathOf(attr(r, "to")) == host+"/"+key {
			out = append(out, r.At)
		}
	}
	return out
}

// recTimes returns the times of n1's lockstep.grain.<kind> records.
func (c *simCluster) recTimes(kind string) []kernel.Time {
	id := c.n["n1"].ID()
	var out []kernel.Time
	for _, r := range c.s.World().Sim.Records() {
		if r.Node == id && r.Kind == grainPrefix+kind {
			out = append(out, r.At)
		}
	}
	return out
}

// AT-VAC-10, AT-VAC-25 (the Stopping part), VAC-420, VAC-430, VAC-600: an activation is passivated
// IdleTimeout after the host's last delivery to it, not after its activation: the timer that fires
// earlier is armed again for the rest. A message sent to the activation directly, not by the host,
// does not count. While it stops, the key is in HostInfo.Stopping and not in Active; at its
// Terminated it leaves both (lockstep.grain.stopped). The next message activates the key again,
// with an idle timer of its own.
func TestATVAC10IdlePassivation(t *testing.T) {
	sim.Simulate(t, simOpts(3, 6*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c, m, sys := oneNode(s, logKind("counter", l, WithIdleTimeout(time.Second)))
		states := hostStates(s, c, "n1", "counter")
		g := GrainOf[gmsg](m, "counter", "k")
		send := func(n int) {
			tt.Helper()
			if err := g.Send(sys, gmsg{N: n}); err != nil {
				tt.Fatal(err)
			}
		}

		send(1)
		w.RunFor(400 * time.Millisecond)
		send(2)
		w.RunFor(400 * time.Millisecond)
		send(3)
		w.RunFor(700 * time.Millisecond)
		pid, err := lockstep.Lookup[gmsg](sys, grainsPath+"/counter/k")
		if err != nil {
			tt.Fatalf("the activation: %v", err)
		}
		if err := sim.Send(s, "n1", pid, gmsg{N: 4}); err != nil { // not through the host
			tt.Fatal(err)
		}
		w.RunFor(time.Second)

		if got := l.of("k"); !slices.Equal(got, []int{1, 2, 3, 4}) {
			tt.Errorf("k got %v, want [1 2 3 4]", got)
		}
		del, act := c.deliveries("n1", "k"), c.recTimes("activate")
		pass, stop := c.recTimes("passivate"), c.recTimes("stopped")
		if len(del) != 3 || len(act) != 1 || len(pass) != 1 || len(stop) != 1 {
			tt.Fatalf("deliveries %v, activations %v, passivations %v, stops %v: want 3, 1, 1, 1", del, act, pass, stop)
		}
		if want := del[2].Add(time.Second); pass[0] != want {
			tt.Errorf("passivated at %v, want %v: IdleTimeout after the last delivery at %v (activated at %v)", pass[0], want, del[2], act[0])
		}
		if stop[0] < pass[0] {
			tt.Errorf("stopped at %v, before the passivation at %v", stop[0], pass[0])
		}
		expectStrings(tt, "passivate", c.grainRecs("n1", "passivate"), []string{"kind=counter key=k reason=idle"})
		expectStrings(tt, "stopped", c.grainRecs("n1", "stopped"), []string{"kind=counter key=k"})
		expectStrings(tt, "HostInfo states", *states, []string{
			"active=[k] stopping=[] buffered=0",
			"active=[] stopping=[k] buffered=0",
			"active=[] stopping=[] buffered=0",
		})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true})

		// The next message activates k again; its own timer passivates it IdleTimeout later.
		send(5)
		w.RunFor(1500 * time.Millisecond)
		act, pass = c.recTimes("activate"), c.recTimes("passivate")
		del = c.deliveries("n1", "k")
		if len(act) != 2 || len(pass) != 2 || len(del) != 4 || pass[1] != del[3].Add(time.Second) {
			tt.Errorf("second activation: activations %v, passivations %v, deliveries %v: want a passivation IdleTimeout after the 4th delivery", act, pass, del)
		}
		expectStrings(tt, "factory calls", l.made, []string{"k", "k"})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true})
	})
}

// AT-VAC-11, VAC-310 rule 3, VAC-430, VAC-510 (the order): while the activation of k stops, the
// host holds k's messages; at the activation's Terminated it records lockstep.grain.stopped, then
// routes the held messages in order, the first of which activates k again, and only then sends the
// pending handoff acks, in the order they were added. A test call adds the acks, as a handoffReq
// for a stopping key will (3.4-06).
func TestATVAC11TerminatedOrder(t *testing.T) {
	sim.Simulate(t, simOpts(3, 2*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c, m, sys := oneNode(s, logKind("counter", l))
		states := hostStates(s, c, "n1", "counter")
		if err := GrainOf[gmsg](m, "counter", "k").Send(sys, gmsg{N: 1}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)

		// One host turn: passivate k, add two pending acks and route two messages for k, which
		// rule 3 holds.
		callHost(tt, s, func(ctx *lockstep.Context[hostMsg], h *host) {
			g := h.keys["k"]
			h.passivate(ctx, "k", g, reasonIdle)
			g.acks = append(g.acks, pendingAck{to: "n1", seq: 7}, pendingAck{to: "n1", seq: 8})
			h.route(ctx, "k", gmsg{N: 2})
			h.route(ctx, "k", gmsg{N: 3})
		})
		w.RunFor(10 * time.Millisecond)

		expectStrings(tt, "HostInfo states", *states, []string{
			"active=[k] stopping=[] buffered=0",
			"active=[] stopping=[k] buffered=2",
			"active=[k] stopping=[] buffered=0",
		})
		got := c.grainTrace("n1")
		i := slices.Index(got, "passivate kind=counter key=k reason=idle")
		if i < 0 {
			tt.Fatalf("no passivation in %q", got)
		}
		expectStrings(tt, "the host's records and sends from the passivation on", got[i:], []string{
			"passivate kind=counter key=k reason=idle",
			"stopped kind=counter key=k",
			"activate kind=counter key=k handoff=none",
			"send k cluster.gmsg",
			"send k cluster.gmsg",
			"send host cluster.handoffAck",
			"send host cluster.handoffAck",
		})
		if got := l.of("k"); !slices.Equal(got, []int{1, 2, 3}) {
			tt.Errorf("k got %v, want [1 2 3]", got)
		}
		expectStrings(tt, "factory calls", l.made, []string{"k", "k"})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true, Active: []string{"k"}})
	})
}

// AT-VAC-26 (the activation-restart part), VAC-440: an activation that fails is restarted by the
// host's strategy and stays active: no Terminated reaches the host, nothing is recorded, the key
// stays in HostInfo.Active, and the next message goes to the restarted activation, which the
// factory built again. An activation that stops by itself leaves the host at its Terminated with
// lockstep.grain.stopped and no passivation.
func TestATVAC26ActivationRestart(t *testing.T) {
	sim.Simulate(t, simOpts(3, 2*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c, m, sys := oneNode(s, logKind("counter", l))
		states := hostStates(s, c, "n1", "counter")
		g := GrainOf[gmsg](m, "counter", "k")
		for _, msg := range []gmsg{{N: 1}, {N: 2, Fail: true}} {
			if err := g.Send(sys, msg); err != nil {
				tt.Fatal(err)
			}
		}
		w.RunFor(500 * time.Millisecond)
		r := Ask(sys, g, func(rt lockstep.ReplyTo[int]) gmsg { return gmsg{N: 5, Reply: rt} })
		w.RunFor(10 * time.Millisecond)
		if v, err := awaitResolved(tt, r); err != nil || v != 5 {
			tt.Errorf("the restarted activation answered %d, %v, want 5 (a fresh sum)", v, err)
		}
		expectStrings(tt, "factory calls", l.made, []string{"k", "k"})
		if got := l.of("k"); !slices.Equal(got, []int{1, 2, 5}) {
			tt.Errorf("k got %v, want [1 2 5]", got)
		}
		restarts := 0
		for _, rec := range s.World().Sim.Records() {
			if rec.Kind == "lockstep.restart" && strings.Contains(attrs(rec), grainsPath+"/counter/k") {
				restarts++
			}
		}
		if restarts != 1 {
			tt.Errorf("%d restarts of the activation, want 1", restarts)
		}
		expectStrings(tt, "activations", c.grainRecs("n1", "activate"), []string{"kind=counter key=k handoff=none"})
		for _, kind := range []string{"passivate", "stopped", "drop"} {
			if got := c.grainRecs("n1", kind); len(got) != 0 {
				tt.Errorf("%s records %q, want none", kind, got)
			}
		}
		expectStrings(tt, "HostInfo states", *states, []string{"active=[k] stopping=[] buffered=0"})

		// Stopping by itself: stopped without a passivation.
		if err := g.Send(sys, gmsg{N: 6, Stop: true}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		expectStrings(tt, "stopped", c.grainRecs("n1", "stopped"), []string{"kind=counter key=k"})
		if got := c.grainRecs("n1", "passivate"); len(got) != 0 {
			tt.Errorf("passivate records %q, want none", got)
		}
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true})
	})
}

// VAC-450: a host that fails while a key is stopping records lockstep.grain.stopped for each of
// its activations, which stopped with it, in key order, and one drop with reason host per held
// message; pending acks are not sent. The restarted host starts empty, applies the latest view as
// its first and activates a key again on its next message.
func TestHostStopDropsHeld(t *testing.T) {
	sim.Simulate(t, simOpts(3, 3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c, m, sys := oneNode(s, logKind("counter", l))
		for _, key := range []string{"z", "a"} {
			if err := GrainOf[gmsg](m, "counter", key).Send(sys, gmsg{N: 1}); err != nil {
				tt.Fatal(err)
			}
		}
		w.RunFor(10 * time.Millisecond)

		callHost(tt, s, func(ctx *lockstep.Context[hostMsg], h *host) {
			g := h.keys["z"]
			h.passivate(ctx, "z", g, reasonIdle)
			g.acks = append(g.acks, pendingAck{to: "n1", seq: 1})
			h.route(ctx, "z", gmsg{N: 2})
			h.route(ctx, "z", gmsg{N: 3})
			ctx.Fail(errors.New("boom"))
		})
		w.RunFor(time.Second)

		got := c.grainTrace("n1")
		i := slices.Index(got, "passivate kind=counter key=z reason=idle")
		if i < 0 {
			tt.Fatalf("no passivation in %q", got)
		}
		expectStrings(tt, "the host's records and sends from the passivation on", got[i:], []string{
			"passivate kind=counter key=z reason=idle",
			"stopped kind=counter key=a",
			"stopped kind=counter key=z",
			"drop kind=counter key=z reason=host hops=0",
			"drop kind=counter key=z reason=host hops=0",
			"view kind=counter version=1 joined=true active=0",
		})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true})
		for _, key := range []string{"a", "z"} {
			if _, err := lockstep.Lookup[gmsg](sys, grainsPath+"/counter/"+key); !errors.Is(err, lockstep.ErrNotFound) {
				tt.Errorf("activation %s after the host's restart: %v, want ErrNotFound", key, err)
			}
		}

		if err := GrainOf[gmsg](m, "counter", "z").Send(sys, gmsg{N: 4}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true, Active: []string{"z"}})
		if got := l.of("z"); !slices.Equal(got, []int{1, 4}) {
			tt.Errorf("z got %v, want [1 4]", got)
		}
	})
}

// VAC-430, MBX-160: passivating calls lockstep.Stop, whose stop runs before the messages already
// in the activation's mailbox (system messages come first in a turn), so the stop drains them as
// lockstep dead letters with reason stopped; the host records no grain drop for them.
func TestPassivateDeadLettersQueued(t *testing.T) {
	sim.Simulate(t, simOpts(3, 2*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c, m, sys := oneNode(s, logKind("counter", l))
		if err := GrainOf[gmsg](m, "counter", "k").Send(sys, gmsg{N: 1}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)

		// One host turn: deliver two messages to k's mailbox (rule 5), then passivate k.
		callHost(tt, s, func(ctx *lockstep.Context[hostMsg], h *host) {
			h.route(ctx, "k", gmsg{N: 2})
			h.route(ctx, "k", gmsg{N: 3})
			h.passivate(ctx, "k", h.keys["k"], reasonIdle)
		})
		w.RunFor(10 * time.Millisecond)

		if got := l.of("k"); !slices.Equal(got, []int{1}) {
			tt.Errorf("k got %v, want [1]: the delivered messages were not processed", got)
		}
		var dead []string
		for _, r := range s.World().Sim.Records() {
			if r.Kind == "lockstep.deadletter" && pathOf(attr(r, "to")) == "lockstep://n1"+grainsPath+"/counter/k" {
				dead = append(dead, attr(r, "reason")+" "+attr(r, "msg"))
			}
		}
		expectStrings(tt, "dead letters to k", dead, []string{"stopped cluster.gmsg", "stopped cluster.gmsg"})
		if got := c.grainRecs("n1", "drop"); len(got) != 0 {
			tt.Errorf("grain drops %q, want none", got)
		}
		expectStrings(tt, "passivate", c.grainRecs("n1", "passivate"), []string{"kind=counter key=k reason=idle"})
		expectStrings(tt, "stopped", c.grainRecs("n1", "stopped"), []string{"kind=counter key=k"})
	})
}

// VAC-440, VAC-500 step 3: an activation that stopped by itself is no longer alive when the host,
// before it handles the Terminated, applies a view in which another node owns the key. Stop fails,
// so the key moves to stopping without a passivate record, and the Terminated ends it with
// lockstep.grain.stopped. The test publishes that view itself during the turn that delivers the
// message that stops the activation, so the host applies it at the start of its Terminated turn.
func TestPassivateAfterSelfStop(t *testing.T) {
	sim.Simulate(t, simOpts(3, 2*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c, m, sys := oneNode(s, logKind("counter", l))
		key := ""
		for i := 0; key == ""; i++ {
			if k := "k" + strconv.Itoa(i); Score("a", "counter", k) > Score("n1", "counter", k) {
				key = k
			}
		}
		if err := GrainOf[gmsg](m, "counter", key).Send(sys, gmsg{N: 1}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)

		callHost(tt, s, func(ctx *lockstep.Context[hostMsg], h *host) {
			h.route(ctx, key, gmsg{N: 2, Stop: true})
			v := m.View()
			members := slices.Clone(v.Members)
			i, _ := slices.BinarySearchFunc(members, "a", compareNode)
			members = slices.Insert(members, i, Member{Node: "a", ID: 7, State: Alive, Kinds: []string{"counter"}})
			m.view.Store(newView(v.Version+1, v.Self, members))
		})
		w.RunFor(10 * time.Millisecond)

		if got := c.grainRecs("n1", "passivate"); len(got) != 0 {
			tt.Errorf("passivate records %q, want none: the activation had stopped by itself", got)
		}
		expectStrings(tt, "stopped", c.grainRecs("n1", "stopped"), []string{"kind=counter key=" + key})
		if got := l.of(key); !slices.Equal(got, []int{1, 2}) {
			tt.Errorf("%s got %v, want [1 2]", key, got)
		}
		info, _ := m.Host("counter")
		if len(info.Active) != 0 || len(info.Stopping) != 0 || info.Version != m.View().Version {
			tt.Errorf("HostInfo %+v, want no keys at version %d", info, m.View().Version)
		}
	})
}
