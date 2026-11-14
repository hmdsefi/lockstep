// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-REM-31 (defined here: the spec gives it one line), REM-110, REM-160, REM-345, MSG-030:
// simulated delivery is encoded. The sender's slice is encoded when it is sent, so a change the
// sender makes afterwards does not reach the receiver, which gets an equal decoded copy; each data
// frame has its lockstep.remote.send record and leaves as one simnet datagram (net.send_raw),
// never as a by-reference simnet message (net.send); an ask is answered across nodes.
func TestATREM31EncodedDelivery(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got [][]int
		n1 := s.Node("n1", noSetup)
		n2 := s.Node("n2", func(sys *lockstep.System) {
			spawn(sys, "r", &actor[[]int]{receive: func(_ *lockstep.Context[[]int], m []int) { got = append(got, m) }})
			spawn(sys, "echo", echo())
		})
		w.RunFor(time.Millisecond)

		r := lockstep.PIDAt[[]int]("n2", "/user/r")
		sent := []int{1, 2, 3}
		if err := sim.Send(s, "n1", r, sent); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		sent[0] = 99
		if v, err := sim.Ask(s, "n1", lockstep.PIDAt[Ping]("n2", "/user/echo"), buildPing(7)); v != 7 || err != nil {
			tt.Errorf("Ask across nodes = %d, %v; want 7, nil", v, err)
		}
		w.RunFor(time.Second)

		if len(got) != 1 || !slices.Equal(got[0], []int{1, 2, 3}) {
			tt.Fatalf("r got %v, want [[1 2 3]]: the value at the send", got)
		}
		if &got[0][0] == &sent[0] {
			tt.Error("r got the sender's array, want a decoded copy")
		}
		recs := w.Sim.Records()
		if n := len(ofKind(recs, "net.send")); n != 0 {
			tt.Errorf("%d net.send records, want 0: nodes exchange only datagrams", n)
		}
		var sends []string
		for _, rec := range ofKind(recs, "lockstep.remote.send") {
			if keys := attrKeys(rec); !slices.Equal(keys, []string{"kind", "seq", "from", "to", "type", "bytes"}) {
				tt.Errorf("lockstep.remote.send attributes %v, want kind, seq, from, to, type, bytes", keys)
			}
			node := map[kernel.NodeID]string{n1.ID(): "n1", n2.ID(): "n2"}[rec.Node]
			sends = append(sends, node+" "+attr(rec, "kind")+" "+attr(rec, "seq")+" "+attr(rec, "type"))
			// The frame left in one datagram of the same size, from the same node.
			want := " " + attr(rec, "kind") + " seq=" + attr(rec, "seq") + " bytes=" + attr(rec, "bytes")
			if !slices.ContainsFunc(recs, func(d kernel.Record) bool {
				return d.Kind == "net.send_raw" && d.Node == rec.Node && strings.HasSuffix(attr(d, "payload"), want)
			}) {
				tt.Errorf("no net.send_raw of a datagram ending in %q on node %d", want, rec.Node)
			}
		}
		want := []string{"n1 MSG 1 simtest.IntSlice", "n1 ASK 2 simtest.Ping", "n2 REPLY 1 lockstep.int"}
		if !slices.Equal(sends, want) {
			tt.Errorf("lockstep.remote.send records %q, want %q", sends, want)
		}
	})
}

// SIM-040, REM-335: every system's remoting core is registered for the system while the run
// lasts, and none is left in the table once Simulate returns, whether its node crashed, shut down
// or was still up at the end, so no run's world outlives it.
func TestSimulateUnregistersCores(t *testing.T) {
	var systems []*lockstep.System
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("n1", noSetup)
		n2 := s.Node("n2", noSetup)
		s.Node("n3", noSetup)
		w.RunFor(time.Millisecond)
		first := s.System("n2")
		systems = append(systems, s.System("n1"), first, s.System("n3"))
		n2.Crash()
		n2.Restart()
		systems = append(systems, s.System("n2"))
		if err := s.System("n3").Shutdown(context.Background()); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Millisecond)
		if remoting.CoreOf(s.System("n1")) == nil || remoting.CoreOf(s.System("n2")) == nil {
			tt.Error("a live system has no remoting core during the run")
		}
		if remoting.CoreOf(first) != nil {
			tt.Error("the crashed system still has a remoting core")
		}
		if remoting.CoreOf(s.System("n3")) != nil {
			tt.Error("the shut-down system still has a remoting core")
		}
	})
	if len(systems) == 0 {
		t.Fatal("the body never ran")
	}
	for i, sys := range systems {
		if c := remoting.CoreOf(sys); c != nil {
			t.Errorf("system %d (%s, incarnation %d) still has its remoting core after Simulate", i, sys.Node(), sys.Incarnation())
		}
	}
}

// The scenarios of TestATSIM33RemoteOptions: each prints bodyRan from body.
var (
	_ = scenario("remote-max-frame", func(t *testing.T) {
		sim.Simulate(t, sim.Options{Remote: remote.Config{MaxFrame: 100}}, printBody)
	})
	_ = scenario("remote-idle", func(t *testing.T) {
		sim.SimulateSeeds(t, sim.Options{Remote: remote.Config{Heartbeat: time.Second, IdleTimeout: time.Second}}, printBody, 1)
	})
	_ = scenario("remote-codec", func(t *testing.T) {
		r := codec.NewRegistry()
		codec.RegisterIn[chan int](r, "simtest.Chan")
		sim.Simulate(t, sim.Options{Remote: remote.Config{Registry: r}}, printBody)
	})
)

// AT-SIM-33 (defined here: the spec gives it no text), REM-015, REM-030, REM-300: Options.Remote
// is checked once per Simulate, before any seed. A protocol field that remote.Config.Validate
// refuses, or a registered type the codec does not support, fails the test with
// "lockstep: sim: Options.Remote: <error>" and runs no seed.
func TestATSIM33RemoteOptions(t *testing.T) {
	unsupported := codec.JSON{}.Supports(reflect.TypeFor[chan int]())
	if unsupported == nil {
		t.Fatal("JSON supports chan int; the scenario needs a type it does not support")
	}
	for _, tc := range []struct {
		scenario string
		want     string
	}{
		{"remote-max-frame", "lockstep: sim: Options.Remote: remote: invalid config: MaxFrame 100 out of range [4096, 67108864]"},
		{"remote-idle", "lockstep: sim: Options.Remote: remote: invalid config: IdleTimeout 1s is less than twice Heartbeat 1s"},
		{"remote-codec", "lockstep: sim: Options.Remote: " + unsupported.Error()},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			out, code := runScenario(t, tc.scenario)
			if code == 0 {
				t.Fatalf("child exited 0, want a failure\n%s", out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("output lacks %q:\n%s", tc.want, out)
			}
			if strings.Contains(out, bodyRan) || strings.Contains(out, "seed=0x") {
				t.Errorf("a seed ran:\n%s", out)
			}
		})
	}
}

// AT-SIM-33, REM-300, REM-040: the network fields of Options.Remote are ignored, even ones Validate
// would refuse, a zero protocol field gets remote's default and a set one is used: every node's
// core starts with the default codec and frame limit, without a listen address, and ticks at the
// configured Heartbeat.
func TestATSIM33RemoteDefaults(t *testing.T) {
	opts := sim.Options{
		Options: faultline.Options{Seeds: 2, Trace: kernel.TraceConfig{Level: kernel.TraceFull}},
		Remote: remote.Config{
			Listen: "127.0.0.1:1", Advertise: "x:1", Peers: map[string]string{"not a node": ""},
			Heartbeat: 3 * time.Second,
		},
	}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		n1 := s.Node("n1", noSetup)
		w.RunFor(7 * time.Second)
		recs := w.Sim.Records()
		starts := ofKind(recs, "lockstep.remote.start")
		if len(starts) != 1 || starts[0].Node != n1.ID() {
			tt.Fatalf("%d lockstep.remote.start records, want one of n1", len(starts))
		}
		if keys := attrKeys(starts[0]); !slices.Equal(keys, []string{"node", "inc", "codec", "max-frame"}) {
			tt.Errorf("start attributes %v, want node, inc, codec, max-frame: no listen", keys)
		}
		if attr(starts[0], "codec") != "json" || attr(starts[0], "max-frame") != "1048576" {
			tt.Errorf("start record %v, want codec json and max-frame 1048576", starts[0].Attrs)
		}
		var ticks []kernel.Time
		for _, r := range ofKind(recs, "kernel.event") {
			if r.Text == "lockstep/remote/tick" {
				ticks = append(ticks, r.At)
			}
		}
		if want := []kernel.Time{vt(3 * time.Second), vt(6 * time.Second)}; !slices.Equal(ticks, want) {
			tt.Errorf("ticks at %v, want %v", ticks, want)
		}
	})
}

// connect has every node of nodes dial every other one and runs the world for d, so that the
// remoting sessions between them are up afterwards when d covers a handshake (two trips). Tests
// whose timing assumes that a message takes one trip use it before they measure.
func connect(s *sim.Sim, d time.Duration, nodes ...string) {
	tt := s.World().T()
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				if err := s.Connect(a, b); err != nil {
					tt.Fatalf("Connect(%s, %s): %v", a, b, err)
				}
			}
		}
	}
	s.World().RunFor(d)
}

// withoutTicks returns a copy of recs without the kernel.event records of the remoting cores'
// heartbeat ticks (REM-090), which every live system has once a second, for which drop reports
// true given the tick's node and incarnation.
func withoutTicks(recs []kernel.Record, drop func(node kernel.NodeID, inc uint32) bool) []kernel.Record {
	return slices.DeleteFunc(slices.Clone(recs), func(r kernel.Record) bool {
		return r.Kind == "kernel.event" && r.Text == "lockstep/remote/tick" && drop(r.Node, r.Inc)
	})
}

// heldBackByFIFO counts the MSG datagrams from node from to node to, among recs, whose drawn
// delay (net.send_raw's delay_ns) would have delivered them before an earlier one: the
// reorderings that the datagrams' FIFO order prevented (REM-320). A test of an unordered link uses
// it to show that the link did draw such delays.
func heldBackByFIFO(recs []kernel.Record, from, to string) int {
	n := 0
	var latest kernel.Time
	seen := false
	for _, r := range recs {
		if r.Kind != "net.send_raw" || attr(r, "from") != from || attr(r, "to") != to || !strings.Contains(attr(r, "payload"), " MSG ") {
			continue
		}
		d, err := strconv.ParseInt(attr(r, "delay_ns"), 10, 64)
		if err != nil {
			panic(err)
		}
		due := r.At.Add(time.Duration(d))
		if seen && due < latest {
			n++
		}
		if !seen || due > latest {
			latest, seen = due, true
		}
	}
	return n
}

// ticksOf returns a drop function for withoutTicks that drops the ticks of incarnation inc of
// node only.
func ticksOf(node kernel.NodeID, inc uint32) func(kernel.NodeID, uint32) bool {
	return func(n kernel.NodeID, i uint32) bool { return n == node && i == inc }
}

// ticksBut returns a drop function for withoutTicks that drops every tick but those of
// incarnation inc of node.
func ticksBut(node kernel.NodeID, inc uint32) func(kernel.NodeID, uint32) bool {
	return func(n kernel.NodeID, i uint32) bool { return n != node || i != inc }
}

// askRetry is sim.Ask with a 100 ms timeout, retried up to five times while the ask times out or
// fails because the target's node restarted. After a restart of the target's node, the asking
// node's session to its old incarnation loses the first request, and the next one, queued for the
// old incarnation, fails with remote.ErrPeerRestarted when the new session shows the restart
// (REM-294); a later one reaches the new incarnation. A test that sends only once has the
// restarted node connect first instead: the other node's core then closes its session to the old
// incarnation.
func askRetry[M, R any](s *sim.Sim, node string, to lockstep.PID[M], build func(lockstep.ReplyTo[R]) M) (R, error) {
	var (
		v   R
		err error
	)
	for range 5 {
		v, err = sim.Ask(s, node, to, build, lockstep.WithTimeout(100*time.Millisecond))
		if !errors.Is(err, lockstep.ErrTimeout) && !errors.Is(err, remote.ErrPeerRestarted) {
			return v, err
		}
	}
	return v, err
}
