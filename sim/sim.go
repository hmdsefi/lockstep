// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import "github.com/hmdsefi/faultline"

// Sim is the lockstep view of one simulated world. Call its methods on the simulation
// goroutine: from body, kernel callbacks (actor code included) and checks.
type Sim struct {
	w     *faultline.World
	ended bool

	// slots holds the first failure stored for each named check, indexed like checkNames.
	slots [len(checkNames)]error
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
