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
	"github.com/hmdsefi/lockstep/sim"
)

// oneNode starts the simulated node n1, which joins at once, with kinds, and runs until its hosts
// have started.
func oneNode(s *sim.Sim, kinds ...Kind) (*simCluster, *Membership, *lockstep.System) {
	c := newSimCluster(s)
	cfg := simConfig()
	cfg.Kinds = kinds
	c.node("n1", cfg)
	s.World().RunFor(10 * time.Millisecond)
	return c, c.ms["n1"], s.System("n1")
}

// expectErr fails tt unless err wraps target and has the text want.
func expectErr(tt testing.TB, what string, err, target error, want string) {
	tt.Helper()
	if !errors.Is(err, target) || err.Error() != want {
		tt.Errorf("%s: %v, want %q (wrapping %v)", what, err, want, target)
	}
}

// AT-VAC-03 (the Send part), VAC-020, VAC-030: Send returns ErrInvalidKey for a key of 0 or 257
// bytes; keys of 1 and 256 bytes of any bytes are valid, and their activations are named by the
// escaped key, up to 768 bytes.
func TestATVAC03SendKeys(t *testing.T) {
	sim.Simulate(t, simOpts(1, time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c, m, sys := oneNode(s, logKind("counter", l))
		long := strings.Repeat("a", 257)
		for _, key := range []string{"", long} {
			err := GrainOf[gmsg](m, "counter", key).Send(sys, gmsg{})
			expectErr(tt, "Send to a key of "+strconv.Itoa(len(key))+" bytes", err, ErrInvalidKey,
				"cluster: send counter/"+key+": cluster: invalid key")
		}
		wide := strings.Repeat("\xff", 256)
		for _, key := range []string{"x", wide} {
			if err := GrainOf[gmsg](m, "counter", key).Send(sys, gmsg{N: len(key)}); err != nil {
				tt.Errorf("Send to a key of %d bytes: %v", len(key), err)
			}
		}
		w.RunFor(10 * time.Millisecond)
		name := strings.Repeat("%FF", 256)
		if _, err := lockstep.Lookup[gmsg](sys, grainsPath+"/counter/"+name); err != nil {
			tt.Errorf("the activation of 256 bytes 0xFF: %v", err)
		}
		expectStrings(tt, "activations", c.grainRecs("n1", "activate"), []string{
			"kind=counter key=x handoff=none",
			"kind=counter key=" + name + " handoff=none",
		})
		expectStrings(tt, "factory calls (sorted: each activation calls it in its own first turn)", sorted(l.made...), []string{"x", wide})
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true, Active: []string{"x", wide}})
	})
}

// AT-VAC-05, VAC-300, VAC-310 rules 5 and 6, VAC-330, VAC-410, VAC-600: on one node, the first
// message to a key activates /system/grains/<kind>/<escaped key> by calling the factory with the
// key (lockstep.grain.activate handoff=none), and every message reaches the activation, in order
// per key; HostInfo lists the active keys sorted bytewise; another kind's host is not involved.
func TestATVAC05LocalActivation(t *testing.T) {
	sim.Simulate(t, simOpts(2, time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l, other := &grainLog{}, &grainLog{}
		c, m, sys := oneNode(s, logKind("counter", l), logKind("other", other))
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true})

		keys := []string{"k1", "a/b", "k1", ".", "k1", "a/b"}
		for i, key := range keys {
			g := GrainOf[gmsg](m, "counter", key)
			if g.Kind() != "counter" || g.Key() != key {
				tt.Fatalf("GrainOf = %q/%q", g.Kind(), g.Key())
			}
			if err := g.Send(sys, gmsg{N: i}); err != nil {
				tt.Fatal(err)
			}
		}
		w.RunFor(10 * time.Millisecond)

		for _, name := range []string{"k1", "a%2Fb", "%2E"} {
			if _, err := lockstep.Lookup[gmsg](sys, grainsPath+"/counter/"+name); err != nil {
				tt.Errorf("activation %s: %v", name, err)
			}
		}
		expectStrings(tt, "activations", c.grainRecs("n1", "activate"), []string{
			"kind=counter key=k1 handoff=none",
			"kind=counter key=a%2Fb handoff=none",
			"kind=counter key=%2E handoff=none",
		})
		expectStrings(tt, "factory calls (sorted: each activation calls it in its own first turn)", sorted(l.made...), []string{".", "a/b", "k1"})
		for key, want := range map[string][]int{"k1": {0, 2, 4}, "a/b": {1, 5}, ".": {3}} {
			if got := l.of(key); !slices.Equal(got, want) {
				tt.Errorf("%q got %v, want %v", key, got, want)
			}
		}
		expectHost(tt, c, "n1", HostInfo{Kind: "counter", Version: 1, Joined: true, Active: []string{".", "a/b", "k1"}})
		expectHost(tt, c, "n1", HostInfo{Kind: "other", Version: 1, Joined: true})
		if len(other.made) != 0 || len(c.grainRecs("n1", "drop")) != 0 {
			tt.Errorf("other kind made %q; drops %q", other.made, c.grainRecs("n1", "drop"))
		}
	})
}

// AT-VAC-06, VAC-200: Send returns the first error in the order zero Grain, invalid key, left, not
// joined, wrong type, no owner, then the send's error; all but the first are wrapped with the kind
// and the escaped key. An owner on another node is reported as no owner until forwarding comes.
func TestATVAC06SendErrors(t *testing.T) {
	sim.Simulate(t, simOpts(1, 3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		c := newSimCluster(s)
		kinds := []Kind{logKind("counter", l), logKind("tiny", l, WithHostOptions(lockstep.WithMailbox(1, lockstep.FailSend)))}
		joined := simConfig()
		joined.Kinds = kinds
		c.node("n1", joined)
		lone := simConfig("n2", "n9") // n9 does not exist: n2 never joins
		lone.Kinds = kinds
		c.node("n2", lone)
		c.node("n3", joined)
		w.RunFor(10 * time.Millisecond)
		m1, m2, m3 := c.ms["n1"], c.ms["n2"], c.ms["n3"]
		s1, s2, s3 := s.System("n1"), s.System("n2"), s.System("n3")

		// 1. The zero Grain, and a Grain of no Membership, before every other check.
		for _, g := range []Grain[gmsg]{{}, GrainOf[gmsg](nil, "counter", "")} {
			if err := g.Send(s1, gmsg{}); err != ErrNotJoined { //nolint:errorlint // returned bare
				tt.Errorf("Send on %+v = %v, want ErrNotJoined itself", g, err)
			}
		}
		// 2. An invalid key comes before left and not joined.
		expectErr(tt, "invalid key, not joined", GrainOf[gmsg](m2, "counter", "").Send(s2, gmsg{}),
			ErrInvalidKey, "cluster: send counter/: cluster: invalid key")
		// 4. Not joined comes before the type check.
		expectErr(tt, "not joined", GrainOf[gmsg](m2, "counter", "k").Send(s2, gmsg{}),
			ErrNotJoined, "cluster: send counter/k: cluster: not joined")
		expectErr(tt, "not joined, wrong type", GrainOf[string](m2, "counter", "k").Send(s2, ""),
			ErrNotJoined, "cluster: send counter/k: cluster: not joined")
		// 5. The wrong type comes before the owner.
		expectErr(tt, "wrong type", GrainOf[string](m1, "counter", "k/1").Send(s1, "x"), lockstep.ErrWrongType,
			`cluster: send counter/k%2F1: lockstep: wrong message type: kind "counter" takes cluster.gmsg, not string`)
		// 6. No owner: no up member hosts the kind.
		expectErr(tt, "no owner", GrainOf[int](m1, "nothere", "k").Send(s1, 1),
			ErrNoOwner, "cluster: send nothere/k: cluster: no owner")
		// Then the send's error: tiny's host has a mailbox of 1.
		tiny := GrainOf[gmsg](m1, "tiny", "k")
		if err := tiny.Send(s1, gmsg{}); err != nil {
			tt.Fatal(err)
		}
		if err := tiny.Send(s1, gmsg{}); !errors.Is(err, lockstep.ErrMailboxFull) ||
			!strings.HasPrefix(err.Error(), "cluster: send tiny/k: ") {
			tt.Errorf("Send to a full host = %v, want ErrMailboxFull wrapped", err)
		}
		w.RunFor(10 * time.Millisecond)

		// 3. Left comes before not joined, and after an invalid key.
		if err := m2.Leave(); err != nil {
			tt.Fatal(err)
		}
		expectErr(tt, "left, not joined", GrainOf[gmsg](m2, "counter", "k").Send(s2, gmsg{}),
			ErrLeft, "cluster: send counter/k: cluster: left")
		if err := m3.Leave(); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		expectErr(tt, "left", GrainOf[string](m3, "counter", "k").Send(s3, ""),
			ErrLeft, "cluster: send counter/k: cluster: left")
		expectErr(tt, "left, invalid key", GrainOf[gmsg](m3, "counter", long257()).Send(s3, gmsg{}),
			ErrInvalidKey, "cluster: send counter/"+long257()+": cluster: invalid key")

		// An owner on another node: a hosts counter too.
		if err := sim.Send(s, "n1", lockstep.PIDAt[memberMsg]("n1", actorPath),
			memberMsg(syncReq{From: update{Node: "a", ID: 7, State: Alive, Kinds: []string{"counter"}}})); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		key := ""
		for _, k := range []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"} {
			if o, _ := Owner(m1.View(), "counter", k); o == "a" {
				key = k
				break
			}
		}
		if key == "" {
			tt.Fatal("a owns none of k0 to k9")
		}
		expectErr(tt, "remote owner", GrainOf[gmsg](m1, "counter", key).Send(s1, gmsg{}), ErrNoOwner,
			"cluster: send counter/"+key+": cluster: no owner: the owner a is another node, which is not supported yet")
		if n := len(c.grainRecs("n1", "activate")); n != 1 {
			tt.Errorf("%d activations on n1, want only tiny/k's", n)
		}
	})
}

// long257 returns a key of 257 bytes.
func long257() string { return strings.Repeat("b", 257) }

// AT-VAC-30, VAC-210, MSG-380, MSG-390: Ask creates the reply with NewReply, calls build once and
// sends the request as Send does; the activation answers. An invalid option returns NewReply's
// failed reply, and every error of Send fails the reply before Ask returns; build is not called
// when a check fails, and is called once when the send itself fails. A nil build panics.
func TestATVAC30Ask(t *testing.T) {
	sim.Simulate(t, simOpts(1, time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		l := &grainLog{}
		_, m, sys := oneNode(s, logKind("counter", l), logKind("tiny", l, WithHostOptions(lockstep.WithMailbox(1, lockstep.FailSend))))
		calls := 0
		build := func(n int) func(lockstep.ReplyTo[int]) gmsg {
			return func(rt lockstep.ReplyTo[int]) gmsg {
				calls++
				return gmsg{N: n, Reply: rt}
			}
		}
		g := GrainOf[gmsg](m, "counter", "k")
		r := Ask(sys, g, build(5))
		if calls != 1 || askResolved(r) {
			tt.Fatalf("after Ask: %d build calls, resolved %v; want 1, false", calls, askResolved(r))
		}
		w.RunFor(10 * time.Millisecond)
		if v, err := awaitResolved(tt, r); v != 5 || err != nil {
			tt.Errorf("first ask = %d, %v, want 5", v, err)
		}
		r = Ask(sys, g, build(7), lockstep.WithTimeout(time.Second))
		w.RunFor(10 * time.Millisecond)
		if v, err := awaitResolved(tt, r); v != 12 || err != nil {
			tt.Errorf("second ask = %d, %v, want 12", v, err)
		}

		// Failed before Ask returns, without calling build.
		calls = 0
		for _, c := range []struct {
			what   string
			r      *lockstep.Reply[int]
			target error
		}{
			{"invalid option", Ask(sys, g, build(1), lockstep.WithTimeout(0)), lockstep.ErrInvalidOption},
			{"zero grain", Ask(sys, Grain[gmsg]{}, build(1)), ErrNotJoined},
			{"invalid key", Ask(sys, GrainOf[gmsg](m, "counter", ""), build(1)), ErrInvalidKey},
			{"no owner", Ask(sys, GrainOf[gmsg](m, "nothere", "k"), build(1)), ErrNoOwner},
		} {
			if !askResolved(c.r) {
				tt.Errorf("%s: the reply has not resolved when Ask returned", c.what)
				continue
			}
			if _, err := c.r.Await(context.Background()); !errors.Is(err, c.target) {
				tt.Errorf("%s: %v, want %v", c.what, err, c.target)
			}
		}
		if calls != 0 {
			tt.Errorf("build called %d times by failed asks", calls)
		}

		// The send's error fails the reply too; build was called.
		tiny := GrainOf[gmsg](m, "tiny", "k")
		if err := tiny.Send(sys, gmsg{}); err != nil {
			tt.Fatal(err)
		}
		r = Ask(sys, tiny, build(1))
		if calls != 1 || !askResolved(r) {
			tt.Fatalf("ask to a full host: %d build calls, resolved %v; want 1, true", calls, askResolved(r))
		}
		if _, err := r.Await(context.Background()); !errors.Is(err, lockstep.ErrMailboxFull) || !strings.HasPrefix(err.Error(), "cluster: send tiny/k: ") {
			tt.Errorf("ask to a full host: %v, want ErrMailboxFull wrapped", err)
		}

		expectPanic(tt, "cluster: Ask with nil build function", func() { Ask[int](sys, g, nil) })
	})
}

// Send's owner lookup allocates nothing (VAC-110): the placement of the published view ranks with
// core.AppendRank.
func TestSendLookupAllocs(t *testing.T) {
	sim.Simulate(t, simOpts(1, time.Second), func(s *sim.Sim) {
		tt := s.World().T()
		_, m, _ := oneNode(s, validKind("counter"))
		g := GrainOf[int](m, "counter", "some-key")
		if n := testing.AllocsPerRun(100, func() {
			if _, err := g.target(); err != nil {
				panic(err)
			}
		}); n != 0 {
			tt.Errorf("Send's checks and owner lookup allocate %v times, want 0", n)
		}
	})
}

// placement holds the hosting members of every kind an up member names, sorted by node, and
// answers as Owner does.
func TestPlacement(t *testing.T) {
	v := hostView("n1:a:counter,x", "n2:s:x", "n3:d:counter,z", "n4:l:y", "n5:a:counter")
	p := newPlacement(v.Members)
	if !slices.Equal(p.kinds, []string{"counter", "x"}) {
		t.Errorf("kinds %q, want [counter x]: only up members count", p.kinds)
	}
	for _, kind := range []string{"counter", "x", "y", "z", "none"} {
		for _, key := range []string{"k0", "k3", "k8", "a/b"} {
			want, wok := Owner(v, kind, key)
			if got, ok := p.owner(kind, key); got != want || ok != wok {
				t.Errorf("owner(%s, %s) = %s, %v, want %s, %v", kind, key, got, ok, want, wok)
			}
		}
	}
	var none *placement
	if _, ok := none.owner("counter", "k"); ok || none.hosts("counter") != nil {
		t.Error("a nil placement must host nothing")
	}
}
