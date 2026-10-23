// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import "github.com/hmdsefi/lockstep/internal/engine"

// ApplyEnv exposes applyEnv to the external tests.
var ApplyEnv = applyEnv

// The named checks, for the external tests.
const (
	CheckMisuse          = checkMisuse
	CheckNoEffectHandler = checkNoEffectHandler
	CheckLeak            = checkLeak
	CheckDeadlock        = checkDeadlock
)

// Store exposes store to the external tests.
func (s *Sim) Store(check string, err error) { s.store(check, err) }

// EngineBundle is the engine bundle type, for the external tests.
type EngineBundle = engine.Engine

// Stored returns the failure stored in the slot of check, or nil.
func (s *Sim) Stored(check string) error { return s.slots[slot(check)] }

// Engine returns the engine bundle of node name's latest boot, also when that boot built no
// system, or the zero bundle before the first boot.
func (s *Sim) Engine(name string) engine.Engine {
	if e := s.lookup(name).eng; e != nil {
		return e.bundle
	}
	return engine.Engine{}
}

// MarkDead marks the engine of node name's latest boot dead, as a Stop does (SIM-120, SIM-195).
func (s *Sim) MarkDead(name string) { s.lookup(name).eng.dead = true }
