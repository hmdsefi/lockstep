// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"
	"slices"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// Sim is the lockstep view of one simulated world. Call its methods on the simulation
// goroutine: from body, kernel callbacks (actor code included) and checks.
type Sim struct {
	w      *faultline.World
	ended  bool
	system []lockstep.Option // Options.System, applied at every boot (SIM-070)

	// slots holds the first failure stored for each named check, indexed like checkNames.
	slots [len(checkNames)]error

	// nodes are the lockstep nodes in NodeID order; byName finds one by name and is only read by
	// key (SIM-330).
	nodes  []*node
	byName map[string]*node

	counts Counts
}

// node is one lockstep node and the state of its latest boot.
type node struct {
	n     *kernel.Node
	setup func(sys *lockstep.System)

	// Set at each boot. sys is nil before the first boot and after a boot whose system could
	// not be built.
	eng  *simEngine
	sys  *lockstep.System
	host engine.Host // the system's inbound side, for the transport (SIM-170)
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
	b, err := hooks.NewSystem(e.bundle, slices.Clip(s.system))
	if err != nil {
		ln.sys, ln.host = nil, nil
		e.misuse(err)
		return
	}
	sys, ok := b.System.(*lockstep.System)
	if !ok {
		panic(fmt.Sprintf("lockstep: sim: hooks.NewSystem returned a %T", b.System)) // unreachable (ENG-200)
	}

	// Install the node's simnet handler (SIM-170).

	// Apply the node's pressure state to e's admission gate (SFT-125).

	ln.sys, ln.host = sys, b.Host
	ln.setup(sys)
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
