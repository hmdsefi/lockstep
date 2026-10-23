// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/overload"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-SIM-02, SIM-020, SIM-070: invalid Options fail the test before body runs, and
// lockstep.WithNodeName in Options.System leaves a node system named after its kernel node.
func TestATSIM02OptionsAndNodeName(t *testing.T) {
	for _, tc := range []struct{ scenario, want string }{
		{"mode-goroutine", "lockstep: sim: Options.Mode must be faultline.ModeEvent"},
		{"nil-option", "lockstep: sim: Options.System[0] is nil"},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			out, code := runScenario(t, tc.scenario)
			if code == 0 {
				t.Fatalf("child exited 0, want a failure\n%s", out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("output lacks %q:\n%s", tc.want, out)
			}
			if strings.Contains(out, bodyRan) {
				t.Errorf("body ran:\n%s", out)
			}
		})
	}

	t.Run("node name", func(t *testing.T) {
		opts := sim.Options{Options: faultline.Options{Seeds: 2}, System: []lockstep.Option{lockstep.WithNodeName("x")}}
		sim.Simulate(t, opts, func(s *sim.Sim) {
			s.Node("n1", noSetup)
			s.World().RunFor(time.Millisecond)
			if got := s.System("n1").Node(); got != "n1" {
				s.World().T().Errorf("System(n1).Node() = %q, want n1", got)
			}
		})
	})
}

// AT-SIM-05, SIM-050, SIM-060, SIM-100: every link between lockstep nodes is the default made
// FIFO, nodes have their tag, and the reserved names panic.
func TestATSIM05NodesAndLinks(t *testing.T) {
	def := simnet.Link{Latency: time.Millisecond, Jitter: 4 * time.Millisecond}
	opts := sim.Options{Options: faultline.Options{Seeds: 2, Net: simnet.Config{Default: def}}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		nodes := []*kernel.Node{s.Node("n1", noSetup), s.Node("n2", noSetup), s.Client("c1", noSetup)}
		want := def
		want.FIFO = true
		for _, a := range nodes {
			for _, b := range nodes {
				if a == b {
					continue
				}
				if got := w.Net.Link(a.ID(), b.ID()); got != want {
					tt.Errorf("Link(%s, %s) = %+v, want %+v", a.Name(), b.Name(), got, want)
				}
			}
		}
		if !nodes[2].HasTag("client") || nodes[2].HasTag("server") {
			tt.Errorf("c1 tags %v, want [client]", nodes[2].Tags())
		}
		if !nodes[0].HasTag("server") || nodes[0].HasTag("client") {
			tt.Errorf("n1 tags %v, want [server]", nodes[0].Tags())
		}
		expectPanic(tt, `lockstep: sim: Node: name "effect" is reserved`, func() { s.Node("effect", noSetup) })
		expectPanic(tt, `lockstep: sim: Client: name "pressure" is reserved`, func() { s.Client("pressure", noSetup) })
		expectPanic(tt, `lockstep: sim: Node: name "pressure" is reserved`, func() { s.Node("pressure", noSetup) })
		expectPanic(tt, `lockstep: sim: Client: name "effect" is reserved`, func() { s.Client("effect", noSetup) })
		expectPanic(tt, "lockstep: sim: Client: nil setup", func() { s.Client("c2", nil) })
	})
}

// SIM-060: a node added after the run started gets FIFO links too, and a link a test sets later
// is kept as given.
func TestSIM060LaterNodeAndLinkChanges(t *testing.T) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		n1 := s.Node("n1", noSetup)
		n2 := s.Node("n2", noSetup)
		w.RunFor(time.Millisecond)
		plain := simnet.Link{Jitter: 50 * time.Millisecond}
		w.Net.SetLink(n1.ID(), n2.ID(), plain)
		other := w.AddServer("other", func(*kernel.Node) {}) // not a lockstep node
		n3 := s.Node("n3", noSetup)
		for _, p := range [][2]*kernel.Node{{n1, n3}, {n3, n1}, {n2, n3}, {n3, n2}, {n2, n1}} {
			if !w.Net.Link(p[0].ID(), p[1].ID()).FIFO {
				tt.Errorf("link %s -> %s is not FIFO", p[0].Name(), p[1].Name())
			}
		}
		for _, p := range [][2]*kernel.Node{{n3, other}, {other, n3}} {
			if w.Net.Link(p[0].ID(), p[1].ID()).FIFO {
				tt.Errorf("link %s -> %s to a node that is not a lockstep node is FIFO", p[0].Name(), p[1].Name())
			}
		}
		if got := w.Net.Link(n1.ID(), n2.ID()); got != plain {
			tt.Errorf("Link(n1, n2) = %+v after adding n3, want %+v", got, plain)
		}
	})
}

// AT-SIM-15, SIM-205: Send to a node that has not booted returns the node-down error, and Send to
// an unknown node panics.
func TestATSIM15SendBeforeBoot(t *testing.T) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1}}, func(s *sim.Sim) {
		tt := s.World().T()
		s.Node("n1", noSetup)
		pid := lockstep.PIDAt[Deposit]("n1", "/user/x")
		err := sim.Send(s, "n1", pid, Deposit{1})
		if !errors.Is(err, lockstep.ErrStopped) {
			tt.Errorf("Send before boot: %v, want an error wrapping ErrStopped", err)
		}
		if got, want := fmt.Sprint(err), "lockstep: sim: node n1 is down: lockstep: stopped"; got != want {
			tt.Errorf("Send before boot: %q, want %q", got, want)
		}
		if sys := s.System("n1"); sys != nil {
			tt.Errorf("System(n1) before boot = %p, want nil", sys)
		}
		expectPanic(tt, `lockstep: sim: unknown node "nope"`, func() { _ = sim.Send(s, "nope", pid, Deposit{1}) })
	})
}

// AT-SIM-29, SIM-040, SIM-050, SIM-090: a Sim from an earlier run panics in every method, a nil
// setup panics, and System panics for an unknown node.
func TestATSIM29Misuse(t *testing.T) {
	var saved *sim.Sim
	runs := 0
	sim.SimulateSeeds(t, sim.Options{}, func(s *sim.Sim) {
		runs++
		tt := s.World().T()
		// Every run of seed 1 (one, or two with run-twice checks) behaves the same.
		if s.World().Seed() == 0x1 {
			saved = s
			s.Node("n1", noSetup)
			expectPanic(tt, "lockstep: sim: Node: nil setup", func() { s.Node("n2", nil) })
			expectPanic(tt, "lockstep: sim: Client: nil setup", func() { s.Client("c1", nil) })
			expectPanic(tt, `lockstep: sim: unknown node "zz"`, func() { s.System("zz") })
			return
		}
		pid := lockstep.PIDAt[Deposit]("n1", "/user/x")
		expectEnded(tt, "Node", func() { saved.Node("n3", noSetup) })
		expectEnded(tt, "Client", func() { saved.Client("c3", noSetup) })
		expectEnded(tt, "System", func() { saved.System("n1") })
		expectEnded(tt, "Counts", func() { saved.Counts() })
		expectEnded(tt, "Send", func() { _ = sim.Send(saved, "n1", pid, Deposit{1}) })
	}, 0x1, 0x2)
	if runs < 2 {
		t.Fatalf("body ran %d times, want at least 2", runs)
	}
}

var _ = scenario("invalid-system-option", func(t *testing.T) {
	opts := fullTrace(1)
	opts.System = []lockstep.Option{lockstep.WithDefaultMailbox(0, lockstep.FailSend)}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		s.Node("n1", func(*lockstep.System) { panic("setup ran") })
		s.World().RunFor(time.Second)
	})
})

// AT-SIM-30, SIM-070: an option that makes hooks.NewSystem fail fails the seed with check
// lockstep/misuse and the option error's text, and setup never runs.
func TestATSIM30InvalidSystemOption(t *testing.T) {
	want := func() (msg string) {
		defer func() { msg = fmt.Sprint(recover()) }()
		lockstep.New(lockstep.WithDefaultMailbox(0, lockstep.FailSend))
		return ""
	}()
	if !strings.Contains(want, "lockstep: invalid option") {
		t.Fatalf("lockstep.New panicked with %q, want an invalid option error", want)
	}
	got := failingSeed(t, "invalid-system-option")
	exp := failure{Kind: "invariant", Check: "lockstep/misuse", Signature: "invariant:lockstep/misuse", Message: want}
	if got != exp {
		t.Errorf("failure %+v, want %+v", got, exp)
	}
}

// SIM-210: Wait returns nil at once for a closed channel.
func TestSIM210WaitClosed(t *testing.T) {
	sim.Simulate(t, sim.Options{}, func(s *sim.Sim) {
		s.Node("n1", noSetup)
		s.World().RunFor(time.Millisecond)
		done := make(chan struct{})
		close(done)
		if err := s.Engine("n1").Waiter.Wait(context.Background(), done); err != nil {
			s.World().T().Errorf("Wait on a closed channel: %v, want nil", err)
		}
	})
}

// SIM-070, SIM-220, SFT-150: every boot builds the system on a new bundle bound to the node and
// its incarnation, with Blocking false and a new admission gate, and setup runs with it.
func TestSIM070BootBundle(t *testing.T) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 2}}, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var setups []*lockstep.System
		n1 := s.Node("n1", func(sys *lockstep.System) { setups = append(setups, sys) })
		w.RunFor(time.Millisecond)
		first := s.Engine("n1")
		n1.Crash()
		if sys := s.System("n1"); sys != nil {
			tt.Errorf("System(n1) while down = %p, want nil", sys)
		}
		n1.Restart()
		second := s.Engine("n1")
		if len(setups) != 2 || setups[0] == setups[1] {
			tt.Fatalf("setup ran with %v, want two different systems", setups)
		}
		if s.System("n1") != setups[1] {
			tt.Errorf("System(n1) is not the system setup ran with")
		}
		for i, b := range []struct {
			sys *lockstep.System
			inc uint32
		}{{setups[0], 1}, {setups[1], 2}} {
			if b.sys.Node() != "n1" || b.sys.Incarnation() != b.inc {
				tt.Errorf("boot %d: system %s incarnation %d, want n1 incarnation %d", i+1, b.sys.Node(), b.sys.Incarnation(), b.inc)
			}
		}
		for i, b := range []struct {
			eng sim.EngineBundle
			inc uint32
		}{{first, 1}, {second, 2}} {
			e := b.eng
			if e.Node != "n1" || e.Epoch != b.inc || e.Blocking {
				tt.Errorf("boot %d: bundle Node %q Epoch %d Blocking %t, want n1 %d false", i+1, e.Node, e.Epoch, e.Blocking, b.inc)
			}
			if _, ok := e.Admission.(*overload.Gate); !ok {
				tt.Errorf("boot %d: Admission is %T, want *overload.Gate", i+1, e.Admission)
			}
			if e.Transport == nil {
				tt.Errorf("boot %d: Transport nil, want the simnet transport", i+1)
			}
			if e.Interposer != nil || e.Stop != nil {
				tt.Errorf("boot %d: Interposer %v, Stop set %t; want none", i+1, e.Interposer, e.Stop != nil)
			}
		}
		if first.Admission == second.Admission {
			tt.Error("the restart reused the first boot's admission gate")
		}
	})
}
