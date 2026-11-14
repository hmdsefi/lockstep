// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-REM-30, REM-040, REM-300: remote.Enable on a simulated system replaces the node's protocol
// settings. A config that Validate refuses returns Validate's error and changes nothing; the next
// call succeeds although its network fields would fail in production (an address it cannot listen
// on, a peer name Validate refuses), and a third call returns ErrAlreadyEnabled. A registry whose
// type the codec does not support is accepted: Enable does not run REM-030 again. The new frame
// limit is the one the node announces in its handshakes, and the heartbeat tick follows the new
// Heartbeat from the call on. A restarted node's setup enables its new boot again.
func TestATREM30EnableSimulated(t *testing.T) {
	unsupported := codec.NewRegistry()
	codec.RegisterIn[chan int](unsupported, "simtest.Chan")
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var errs []error
		n1 := s.Node("n1", func(sys *lockstep.System) {
			errs = append(errs[:0],
				remote.Enable(sys, remote.Config{MaxFrame: 100}),
				remote.Enable(sys, remote.Config{
					Listen: "not an address", Advertise: "x:1", Peers: map[string]string{"not a node": ""},
					MaxFrame: 8192, Heartbeat: 2 * time.Second, IdleTimeout: 10 * time.Second,
				}),
				remote.Enable(sys, remote.Config{}),
			)
		})
		s.Node("n2", func(sys *lockstep.System) { spawn(sys, "echo", echo()) })
		var n3err error
		s.Node("n3", func(sys *lockstep.System) { n3err = remote.Enable(sys, remote.Config{Registry: unsupported}) })

		checkErrs := func() {
			tt.Helper()
			const invalid = "remote: invalid config: MaxFrame 100 out of range [4096, 67108864]"
			if len(errs) != 3 || errs[0] == nil || errs[0].Error() != invalid || errs[1] != nil ||
				!errors.Is(errs[2], remote.ErrAlreadyEnabled) {
				tt.Fatalf("Enable errors %v, want %q, nil, ErrAlreadyEnabled", errs, invalid)
			}
		}
		w.RunFor(time.Millisecond)
		checkErrs()
		if n3err != nil {
			tt.Errorf("Enable with a registry the codec does not support: %v, want nil", n3err)
		}
		if v, err := sim.Ask(s, "n1", lockstep.PIDAt[Ping]("n2", "/user/echo"), buildPing(7)); v != 7 || err != nil {
			tt.Fatalf("Ask from n1 = %d, %v; want 7, nil", v, err)
		}
		w.RunFor(7 * time.Second)

		recs := w.Sim.Records()
		ups := map[string]string{}
		for _, r := range ofKind(recs, "lockstep.remote.up") {
			ups[nodeName(s, r.Node)+" "+attr(r, "dir")+" "+attr(r, "peer")] = attr(r, "max-frame")
		}
		if got := ups["n2 in n1"]; got != "8192" {
			tt.Errorf("n2's up record of n1's session has max-frame %q, want 8192: n1 announces its new limit", got)
		}
		if got := ups["n1 out n2"]; got != "1048576" {
			tt.Errorf("n1's up record of its session to n2 has max-frame %q, want n2's 1048576", got)
		}
		var starts int
		for _, r := range ofKind(recs, "lockstep.remote.start") {
			if r.Node == n1.ID() {
				starts++
			}
		}
		if starts != 1 {
			tt.Errorf("%d lockstep.remote.start records of n1, want 1: Enable records no start", starts)
		}
		var ticks []kernel.Time
		for _, r := range ofKind(recs, "kernel.event") {
			if r.Node == n1.ID() && r.Text == "lockstep/remote/tick" && r.At <= vt(7*time.Second) {
				ticks = append(ticks, r.At)
			}
		}
		if want := []kernel.Time{vt(2 * time.Second), vt(4 * time.Second), vt(6 * time.Second)}; !slices.Equal(ticks, want) {
			tt.Errorf("n1 ticks at %v, want %v", ticks, want)
		}

		errs = nil
		n1.Crash()
		n1.Restart()
		w.RunFor(time.Millisecond)
		checkErrs()
	})
}

// AT-REM-30, REM-040, SIM-120: Enable on a simulated system that is shutting down, has shut down
// or was discarded with its node's crash returns an error wrapping lockstep.ErrStopped, and never
// tries to listen: in production, Listen "not an address" would fail first with the listen error.
func TestATREM30EnableStopped(t *testing.T) {
	sim.Simulate(t, fullTrace(2), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("n1", func(sys *lockstep.System) { spawn(sys, "a", &actor[Deposit]{}) })
		n2 := s.Node("n2", noSetup)
		w.RunFor(time.Millisecond)
		cfg := remote.Config{Listen: "not an address"}
		check := func(what string, sys *lockstep.System) {
			tt.Helper()
			err := remote.Enable(sys, cfg)
			want := `remote: lockstep: stopped: node "` + sys.Node() + `"`
			if !errors.Is(err, lockstep.ErrStopped) || err.Error() != want {
				tt.Errorf("Enable on a %s system: %v, want %q", what, err, want)
			}
		}

		sys1 := s.System("n1")
		executed := w.Sim.Executed()
		if err := sys1.Shutdown(context.Background()); err != nil {
			tt.Fatalf("Shutdown: %v", err)
		}
		if w.Sim.Executed() != executed {
			tt.Fatal("the shutdown ran events before Shutdown returned")
		}
		check("shutting-down", sys1) // no actor has stopped yet
		w.RunFor(time.Second)
		check("shut-down", sys1)

		sys2 := s.System("n2")
		n2.Crash()
		check("crashed", sys2)
		n2.Restart()
		w.RunFor(time.Millisecond)
		check("crashed and restarted", sys2)
		if err := remote.Enable(s.System("n2"), remote.Config{}); err != nil {
			tt.Errorf("Enable on n2's new boot: %v", err)
		}
	})
}

// renamedJSON is codec.JSON under another name.
type renamedJSON struct{ codec.JSON }

func (renamedJSON) Name() string { return "json2" }

// AT-REM-30, REM-040: sessions already up keep the frame limit and the codec agreed at their
// handshake. n1 enables a 4 MiB limit at boot, and its session to n2 comes up with n2's 1 MiB.
// While n2 has sessions, Enable on n2 with another codec or another registry returns an error and
// changes nothing, so a later Enable succeeds. n2 then enables 4 MiB as well, but a 2 MiB message
// from n1 still exceeds the limit of n1's session and becomes a dead letter "too-large-for-peer",
// while a small one arrives.
func TestATREM30SessionsKeepHandshake(t *testing.T) {
	sim.Simulate(t, fullTrace(2), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var enableErr error
		s.Node("n1", func(sys *lockstep.System) { enableErr = remote.Enable(sys, remote.Config{MaxFrame: 4 << 20}) })
		var got []int
		n2 := s.Node("n2", func(sys *lockstep.System) {
			spawn(sys, "r", &actor[string]{receive: func(_ *lockstep.Context[string], m string) { got = append(got, len(m)) }})
		})
		w.RunFor(time.Millisecond)
		if enableErr != nil {
			tt.Fatalf("Enable on n1: %v", enableErr)
		}
		connect(s, 300*time.Millisecond, "n1", "n2")
		const inUse = "remote: the codec and registry cannot change while connections are open or messages wait for one"
		for _, c := range []remote.Config{{Codec: renamedJSON{}}, {Registry: codec.NewRegistry()}} {
			if err := remote.Enable(s.System("n2"), c); err == nil || err.Error() != inUse {
				tt.Errorf("Enable on n2 with %+v while connected: %v, want %q", c, err, inUse)
			}
		}
		if err := remote.Enable(s.System("n2"), remote.Config{MaxFrame: 4 << 20}); err != nil {
			tt.Fatalf("Enable on n2: %v", err)
		}
		r := lockstep.PIDAt[string]("n2", "/user/r")
		if err := sim.Send(s, "n1", r, strings.Repeat("x", 2<<20)); err != nil {
			tt.Fatalf("Send of 2 MiB: %v, want nil: the limit of n1's own frames is 4 MiB", err)
		}
		if err := sim.Send(s, "n1", r, "small"); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(time.Second)
		if !slices.Equal(got, []int{5}) {
			tt.Errorf("r got messages of lengths %v, want [5]", got)
		}
		var reasons []string
		for _, rec := range ofKind(w.Sim.Records(), "lockstep.deadletter") {
			reasons = append(reasons, nodeName(s, rec.Node)+" "+attr(rec, "reason"))
		}
		if want := []string{"n1 too-large-for-peer"}; !slices.Equal(reasons, want) {
			tt.Errorf("dead letters %q, want %q", reasons, want)
		}
		for _, rec := range ofKind(w.Sim.Records(), "lockstep.remote.down") {
			if rec.Node == n2.ID() || attr(rec, "peer") == "n2" {
				tt.Errorf("session down: %s %v; want every session up", rec.Text, rec.Attrs)
			}
		}
	})
}

// AT-SIM-20 (Phase 3 part; defined here, as simulation.md gives it no text), SIM-195, SIM-120,
// REM-296: a simulated system's Shutdown shuts its remoting core down before the engine is marked
// dead. After the shutdown's done record, n1 writes CLOSE shutdown on each session, records each
// session down with reason shutdown (by peer name, outgoing before incoming, the session still
// dialing n3 included), then makes the frame queued for the unreachable n3 a dead letter
// "shutdown"; every one of these records exists, so the engine was still alive. n2 sees both of
// its sessions with n1 go down with reason closed. Afterwards n1's engine records nothing.
func TestATSIM20ShutdownClosesSessions(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		n1 := s.Node("n1", noSetup)
		n2 := s.Node("n2", noSetup)
		s.Node("n3", func(sys *lockstep.System) { spawn(sys, "x", &actor[Deposit]{}) })
		w.RunFor(time.Millisecond)
		connect(s, 300*time.Millisecond, "n1", "n2")
		w.Faults.Inject(fault.Event{Kind: fault.KindPartition, Groups: [][]string{{"n1", "n2"}, {"n3"}}})
		if err := sim.Send(s, "n1", lockstep.PIDAt[Deposit]("n3", "/user/x"), Deposit{1}); err != nil {
			tt.Fatalf("Send to n3: %v", err)
		}
		w.RunFor(10 * time.Millisecond)
		if err := s.System("n1").Shutdown(context.Background()); err != nil {
			tt.Fatalf("Shutdown: %v", err)
		}
		w.RunFor(time.Second)

		recs := w.Sim.Records()
		done := slices.IndexFunc(recs, func(r kernel.Record) bool {
			return r.Node == n1.ID() && r.Kind == "lockstep.shutdown" && attr(r, "phase") == "done"
		})
		if done < 0 {
			tt.Fatal("no lockstep.shutdown phase=done record of n1")
		}
		var n1Recs []string
		for _, r := range recs[done+1:] {
			if r.Node != n1.ID() {
				continue
			}
			switch r.Kind {
			case "lockstep.remote.down":
				n1Recs = append(n1Recs, "down "+attr(r, "peer")+" "+attr(r, "dir")+" "+attr(r, "reason"))
			case "lockstep.deadletter":
				n1Recs = append(n1Recs, "deadletter "+attr(r, "to")+" "+attr(r, "reason"))
			case "net.send_raw":
				if strings.Contains(attr(r, "payload"), " CLOSE seq=") { // the datagram's Describe
					n1Recs = append(n1Recs, "close")
				}
			default:
				if strings.HasPrefix(r.Kind, "lockstep.") {
					n1Recs = append(n1Recs, r.Kind)
				}
			}
		}
		want := []string{
			"close", "close", "close", // one per session, in the down records' order; n3's is lost in the partition
			"down n2 out shutdown", "down n2 in shutdown", "down n3 out shutdown",
			"deadletter " + lockstep.PIDAt[Deposit]("n3", "/user/x").String() + " shutdown",
		}
		if !slices.Equal(n1Recs, want) {
			tt.Errorf("n1 after the shutdown completed: %q, want %q", n1Recs, want)
		}
		var n2Downs []string
		for _, r := range ofKind(recs[done+1:], "lockstep.remote.down") {
			if r.Node == n2.ID() {
				n2Downs = append(n2Downs, attr(r, "peer")+" "+attr(r, "dir")+" "+attr(r, "reason"))
			}
		}
		slices.Sort(n2Downs)
		if want := []string{"n1 in closed", "n1 out closed"}; !slices.Equal(n2Downs, want) {
			tt.Errorf("n2's down records %q, want %q", n2Downs, want)
		}

		// The dead letter is n1's last record of any kind: the engine is dead, so nothing of n1
		// runs or records afterwards, not even a kernel event.
		last := slices.IndexFunc(recs[done+1:], func(r kernel.Record) bool {
			return r.Node == n1.ID() && r.Kind == "lockstep.deadletter"
		})
		if last < 0 {
			tt.Fatal("no dead letter of n1 after the shutdown")
		}
		for _, r := range recs[done+1+last+1:] {
			if r.Node == n1.ID() {
				tt.Errorf("n1 recorded %s %q %v after its last dead letter, want nothing", r.Kind, r.Text, r.Attrs)
			}
		}
	})
}

// nodeName returns the name of s's node id.
func nodeName(s *sim.Sim, id kernel.NodeID) string { return s.World().Sim.Node(id).Name() }
