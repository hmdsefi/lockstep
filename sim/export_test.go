// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

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
