// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"

	"github.com/hmdsefi/lockstep"
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

// errNodeDown is the error of Send and Ask for a node without a current system (SIM-200,
// SIM-205).
func errNodeDown(node string) error {
	return fmt.Errorf("lockstep: sim: node %s is down: %w", node, lockstep.ErrStopped)
}
