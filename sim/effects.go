// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
)

// Start is the engine's Effects. It never calls req.Run (EFF-400). With no handler for
// req.Name, which is every name while the Sim's handler table is empty, it schedules nothing,
// stores the failure of EFF-450 in the lockstep/no-effect-handler slot, which records no
// lockstep.misuse, and returns a Cancel that reports true the first time it is called; done is
// never called. A dead engine does nothing and returns a Cancel that reports false (SIM-120).
func (e *simEngine) Start(req engine.EffectRequest, _ func(v any, err error)) engine.Cancel {
	if e.dead {
		return func() bool { return false }
	}
	e.s.store(checkNoEffectHandler, fmt.Errorf("%w: effect %q called by %s", lockstep.ErrNoEffectHandler, req.Name, req.Actor))
	cancelled := false
	return func() bool {
		if cancelled {
			return false
		}
		cancelled = true
		return true
	}
}
