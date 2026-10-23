// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// sim registers no fault planner and no role, and never calls World.Faults: kernel faults reach
// lockstep nodes only through faultline (SFT-010). lockstep's own faults are Sim methods, each
// recorded as lockstep.fault (SFT-030).

// InjectFail makes the actor at addr fail with err, as if its handler had called ctx.Fail(err):
// the failure reaches its supervisor with source "injected". It takes effect at the actor's next
// turn, and no user message is consumed. It returns an error wrapping lockstep.ErrNotFound if
// addr names no live actor, and one wrapping lockstep.ErrStopped if addr's node is down. It
// panics if err is nil, or if addr names a node that is not a lockstep node of this Sim. Every
// call records lockstep.fault with op=inject-fail.
func (s *Sim) InjectFail(addr lockstep.Address, err error) error {
	s.check()
	if err == nil {
		panic("lockstep: sim: InjectFail: nil error")
	}
	id, sys, rerr := s.faultTarget(addr)
	if rerr == nil {
		rerr = hooks.InjectFail(sys, engine.Address{Node: addr.Node(), Path: addr.Path(), UID: addr.UID()}, err)
	}
	s.recordFault(id, "inject-fail",
		kernel.Attr{Key: "actor", Value: addr.String()},
		kernel.Attr{Key: "err", Value: err.Error()},
		kernel.Attr{Key: "result", Value: faultResult(rerr)},
	)
	return rerr
}

// StopActor stops the actor at addr for good, as lockstep.Stop does with the node's System as
// sender, and returns that call's error, or one wrapping lockstep.ErrStopped if addr's node is
// down. It panics if addr names a node that is not a lockstep node of this Sim. Every call
// records lockstep.fault with op=stop.
func (s *Sim) StopActor(addr lockstep.Address) error {
	s.check()
	id, sys, rerr := s.faultTarget(addr)
	if rerr == nil {
		rerr = lockstep.Stop(sys, addr)
	}
	s.recordFault(id, "stop",
		kernel.Attr{Key: "actor", Value: addr.String()},
		kernel.Attr{Key: "result", Value: faultResult(rerr)},
	)
	return rerr
}

// faultTarget resolves the node of addr for a fault: the kernel node ID the fault record carries
// and the node's current System. The zero Address names no node and no actor: it returns node 0
// and an error wrapping lockstep.ErrNotFound. A node that is down returns an error wrapping
// lockstep.ErrStopped (SFT-030). It panics for a node that is not a lockstep node (SIM-090).
func (s *Sim) faultTarget(addr lockstep.Address) (kernel.NodeID, *lockstep.System, error) {
	if addr.IsZero() {
		return 0, nil, fmt.Errorf("%w: %s", lockstep.ErrNotFound, addr)
	}
	ln := s.lookup(addr.Node())
	sys := s.System(addr.Node())
	if sys == nil {
		return ln.n.ID(), nil, errNodeDown(addr.Node())
	}
	return ln.n.ID(), sys, nil
}

// recordFault emits lockstep.fault on kernel node node (0 for none) with op=<op> first, then
// attrs (SFT §8). The delay and drop faults of pair rules record through it too, on node 0.
func (s *Sim) recordFault(node kernel.NodeID, op string, attrs ...kernel.Attr) {
	all := make([]kernel.Attr, 0, len(attrs)+1)
	all = append(all, kernel.Attr{Key: "op", Value: op})
	all = append(all, attrs...)
	s.w.Sim.Emit(kernel.Record{Node: node, Kind: "lockstep.fault", Text: "fault " + op, Attrs: all})
}

// faultResult is the result attribute of lockstep.fault: ok, or the text of err.
func faultResult(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}
