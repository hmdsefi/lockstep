// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"context"
	"errors"
	"fmt"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// Send sends msg to to with the System of node as the sender, as lockstep.Send does. It returns
// an error wrapping lockstep.ErrStopped if the node is down or not booted yet. It panics if node
// is not a lockstep node of this Sim.
func Send[M any](s *Sim, node string, to lockstep.PID[M], msg M) error {
	sys := s.System(node)
	if sys == nil {
		return errNodeDown(node)
	}
	return lockstep.Send(sys, to, msg)
}

// Ask sends a request built by build from the System of node, runs the simulation until the
// reply resolves, and returns the reply's value and error. Call it from body only. It returns an
// error wrapping lockstep.ErrStopped if the node is down, or crashes before the reply resolves,
// and ErrRunEnded if the run ends first.
func Ask[M, R any](s *Sim, node string, to lockstep.PID[M], build func(lockstep.ReplyTo[R]) M, opts ...lockstep.AskOption) (R, error) {
	if kernel.Active() != nil {
		panic("lockstep: sim: Ask called inside the simulation; use lockstep.Ask with lockstep.Pipe in actors")
	}
	var zero R
	sys := s.System(node)
	if sys == nil {
		return zero, errNodeDown(node)
	}
	r := lockstep.Ask(sys, to, build, opts...)
	w := s.w
	// SIM-200: run event by event, so that Ask returns at the time the reply resolves.
	for !hooks.AskResolved(r) {
		if s.System(node) != sys {
			return zero, errNodeDown(node)
		}
		next, ok := w.Sim.NextAt()
		if !ok || next > w.End() {
			// No event is left before the end: run to the end, which RunFor allows exactly.
			w.RunFor(w.End().Sub(w.Sim.Now()))
			if !hooks.AskResolved(r) {
				return zero, ErrRunEnded
			}
			break
		}
		// RunFor runs the events due at the target time too, so RunFor(0) makes progress.
		w.RunFor(max(0, next.Sub(w.Sim.Now())))
	}
	return r.Await(context.Background()) // resolved: returns at once (MSG-450)
}

// ErrRunEnded means that the run reached its end (faultline World.End) before a reply that the
// body waited for resolved.
var ErrRunEnded = errors.New("lockstep: sim: run ended before the reply resolved")

// errNodeDown is the error of Send and Ask for a node without a current system (SIM-200,
// SIM-205).
func errNodeDown(node string) error {
	return fmt.Errorf("lockstep: sim: node %s is down: %w", node, lockstep.ErrStopped)
}
