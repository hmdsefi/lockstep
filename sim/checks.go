// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

// The named checks of SIM-300. Each is registered as an invariant and as a final check, and a
// lockstep failure of a running seed is raised through it, so faultline reports the failure with
// kind invariant (or final), this check name and signature invariant:<name> (or final:<name>).
const (
	checkMisuse          = "lockstep/misuse"
	checkNoEffectHandler = "lockstep/no-effect-handler"
	checkLeak            = "lockstep/leak"
	checkDeadlock        = "lockstep/deadlock"
)

// checkNames lists the named checks in SIM-300's registration order; the index of a name is its
// slot in Sim.slots.
var checkNames = [...]string{checkMisuse, checkNoEffectHandler, checkLeak, checkDeadlock}

// slot returns the index of check in checkNames. It panics for a name that is not a named check.
func slot(check string) int {
	for i, name := range checkNames {
		if name == check {
			return i
		}
	}
	panic("lockstep: sim: unknown check " + check)
}

// store keeps err in the slot of check unless the slot already holds a failure: a slot keeps the
// first failure stored in it (SIM-300). A nil err leaves the slot empty. It never fails the test itself;
// the check registered for the slot returns the failure to faultline (SIM-305).
func (s *Sim) store(check string, err error) {
	i := slot(check)
	if s.slots[i] == nil {
		s.slots[i] = err
	}
}

// registerChecks registers the named checks on s's World before body runs (SIM-300): the four
// invariants in order, then the four final checks in the same order. A final check returns its
// slot's failure when no invariant returned it, which is a failure stored by body code after the
// last event. lockstep/deadlock returns only its stored failure until deadlock detection exists.
func (s *Sim) registerChecks() {
	for i, name := range checkNames {
		s.w.Invariant(name, func() error { return s.slots[i] })
	}
	for i, name := range checkNames {
		s.w.Final(name, func() error { return s.slots[i] })
	}
}
