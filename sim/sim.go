// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// Sim is the lockstep view of one simulated world. Call its methods on the simulation
// goroutine: from body, kernel callbacks (actor code included) and checks.
type Sim struct {
	w      *faultline.World
	ended  bool
	system []lockstep.Option // Options.System, applied at every boot (SIM-070)
	budget Budget            // Options.Budget, for every node system (SFT-140)

	// slots holds the first failure stored for each named check, indexed like checkNames.
	slots [len(checkNames)]error

	// nodes are the lockstep nodes in NodeID order; byName and byID find one by name and by
	// kernel node ID, and are only read by key (SIM-330).
	nodes  []*node
	byName map[string]*node
	byID   map[kernel.NodeID]*node

	// handlers is the effect handler table, shared by every node: Start reads it by the exact
	// effect name and nothing ranges over it (EFF-410, SIM-330).
	handlers map[string]func(c EffectCall) EffectResult

	counts Counts

	// pairs holds the pair rules of DelayBetween and DropBetween by pair; it is read by key only
	// (SFT-060, SIM-330).
	pairs map[pairKey]*pairRule

	// events counts the pending node events of every engine by label and class, and world the
	// pending world events sim scheduled (SIM-250). askGen changes whenever the number of pending
	// ask timeouts does, so whenever an ask starts or resolves; graphGen is the askGen of the last
	// time lockstep/deadlock read the pending asks (SIM-260).
	events, world    eventCounts
	askGen, graphGen uint64
}

// node is one lockstep node and the state of its latest boot.
type node struct {
	n     *kernel.Node
	setup func(sys *lockstep.System)

	// Set at each boot. sys is nil before the first boot, after a boot whose system could not
	// be built and after a crash.
	eng *simEngine
	sys *lockstep.System

	// pressure counts the node's active pressure episodes. It belongs to the kernel node, so it
	// survives crashes and restarts (SFT-125).
	pressure int
}

// World returns the faultline World of this run.
func (s *Sim) World() *faultline.World {
	s.check()
	return s.w
}

// check panics if the run of s has ended (SIM-040).
func (s *Sim) check() {
	if s.ended {
		panic("lockstep: sim: Sim used after its run ended")
	}
}

// Node adds a server node named name and returns it. At every boot the node gets a new System
// on the simulation engine, and setup runs with it to spawn the node's root actors. A crash
// discards the System; a restart boots a new one and runs setup again. The names "effect" and
// "pressure" are reserved.
func (s *Sim) Node(name string, setup func(sys *lockstep.System), opts ...kernel.NodeOption) *kernel.Node {
	s.check()
	return s.add("Node", name, setup, s.w.AddServer, opts)
}

// Client is Node for a client node (tag "client"), for workload actors. Fault planners target
// servers by default, so clients usually stay up.
func (s *Sim) Client(name string, setup func(sys *lockstep.System), opts ...kernel.NodeOption) *kernel.Node {
	s.check()
	return s.add("Client", name, setup, s.w.AddClient, opts)
}

// add checks name and setup as SIM-050 says, adds the kernel node with addNode, makes its links
// to the other lockstep nodes FIFO (SIM-060) and enters it in the node table. method names the
// caller in panic messages.
func (s *Sim) add(method, name string, setup func(*lockstep.System),
	addNode func(string, kernel.BootFunc, ...kernel.NodeOption) *kernel.Node, opts []kernel.NodeOption,
) *kernel.Node {
	if setup == nil {
		panic("lockstep: sim: " + method + ": nil setup")
	}
	if name == "effect" || name == "pressure" { // R7: the effect and pressure stream families
		panic(fmt.Sprintf("lockstep: sim: %s: name %q is reserved", method, name))
	}
	ln := &node{setup: setup}
	n := addNode(name, func(n *kernel.Node) { s.boot(ln, n) }, opts...)
	ln.n = n
	for _, y := range s.nodes {
		s.fifo(n.ID(), y.n.ID())
		s.fifo(y.n.ID(), n.ID())
	}
	s.nodes = append(s.nodes, ln)
	if s.byName == nil {
		s.byName = make(map[string]*node)
	}
	s.byName[name] = ln
	if s.byID == nil {
		s.byID = make(map[kernel.NodeID]*node)
	}
	s.byID[n.ID()] = ln
	return n
}

// fifo makes the link from a to b FIFO and keeps the rest of its configuration (SIM-060).
func (s *Sim) fifo(a, b kernel.NodeID) {
	l := s.w.Net.Link(a, b)
	l.FIFO = true
	s.w.Net.SetLink(a, b, l)
}

// boot is the boot function of lockstep node ln, run by the kernel at every boot of n (SIM-070).
func (s *Sim) boot(ln *node, n *kernel.Node) {
	e := newEngine(s, n, n.Incarnation())
	ln.eng = e
	b, err := hooks.NewSystem(e.bundle, s.systemOptions())
	if err != nil {
		ln.sys = nil
		e.misuse(err)
		return
	}
	sys, ok := b.System.(*lockstep.System)
	if !ok {
		panic(fmt.Sprintf("lockstep: sim: hooks.NewSystem returned a %T", b.System)) // unreachable (ENG-200)
	}

	// Install the node's simnet handler (SIM-170).
	e.host = b.Host
	s.w.Net.Handle(n, e.arrive)

	// Apply the node's pressure state to e's admission gate (SFT-125).
	ln.bootPressure(e)

	ln.sys = sys
	ln.setup(sys)
}

// crashed is the crash hook of s, registered once per run with w.Sim.OnCrash (SIM-110). The
// kernel runs it after node n is down and kernel.crash is recorded, and not for a crash before
// the first boot. For a lockstep node it marks the engine dead, discards the node's system,
// leaves the node without one and subtracts the node events the kernel drops with the crash from
// the counts (SIM-250), in that order: the engine is dead before Discard resolves the pending
// asks, so none of that is recorded (SIM-120), and the ask timeouts that Discard cancels are
// subtracted by their Cancel before the rest is dropped. Discarding runs no Stopped hook, closes
// nothing and records no lockstep.stop (SUP-360), and the discarded actors are not checked for
// leaks. Other nodes are not sim's.
func (s *Sim) crashed(n *kernel.Node) {
	ln, ok := s.byID[n.ID()]
	if !ok {
		return
	}
	if ln.eng != nil {
		ln.eng.kill()
	}
	if ln.sys != nil {
		hooks.Discard(ln.sys)
		ln.sys = nil
	}
	if ln.eng != nil {
		ln.eng.dropPending()
	}
}

// System returns the System of node name's current incarnation, or nil while the node is down
// or before its first boot. It panics if name is not a lockstep node of this Sim.
func (s *Sim) System(name string) *lockstep.System {
	s.check()
	ln := s.lookup(name)
	if ln.n.State() == kernel.NodeDown {
		return nil
	}
	return ln.sys
}

// lookup returns the lockstep node named name. It panics if there is none (SIM-090).
func (s *Sim) lookup(name string) *node {
	ln, ok := s.byName[name]
	if !ok {
		panic(fmt.Sprintf("lockstep: sim: unknown node %q", name))
	}
	return ln
}
