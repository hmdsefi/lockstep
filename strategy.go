// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hmdsefi/lockstep/internal/supervise"
)

// Directive is a supervisor's decision about a failed child.
type Directive uint8

const (
	DirectiveRestart  Directive = iota + 1 // end the incarnation and start a new one in place
	DirectiveStop                          // stop the child for good with Failed(err)
	DirectiveEscalate                      // fail the supervisor with the child's error
	DirectiveResume                        // keep the incarnation; the failing message is lost
)

// String returns "restart", "stop", "escalate", "resume", or "Directive(<n>)".
func (d Directive) String() string {
	switch d {
	case DirectiveRestart:
		return "restart"
	case DirectiveStop:
		return "stop"
	case DirectiveEscalate:
		return "escalate"
	case DirectiveResume:
		return "resume"
	}
	return "Directive(" + strconv.Itoa(int(d)) + ")"
}

// Strategy is a supervision strategy for the children of one actor. Build it with OneForOne or
// AllForOne and adjust the exported fields; a zero Strategy is invalid.
type Strategy struct {
	scope uint8 // a supervise.Scope; 0 for a Strategy not built by OneForOne or AllForOne

	// Decider maps a child's failure to a directive. nil means DirectiveRestart for every error.
	Decider func(err error) Directive
	// MaxRestarts is how many restarts are allowed within Window; one more becomes
	// DirectiveEscalate. 0 allows none.
	MaxRestarts int
	// Window is the sliding period over which restarts are counted, on the engine clock.
	Window time.Duration
	// MinBackoff is the delay before the first restart in the window; it doubles for each further
	// restart in the window, up to MaxBackoff. 0 restarts without delay.
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

// Defaults of OneForOne and AllForOne.
const (
	defaultMaxRestarts = 10
	defaultWindow      = time.Minute
	defaultMinBackoff  = 10 * time.Millisecond
	defaultMaxBackoff  = 10 * time.Second
)

// OneForOne returns a strategy that applies a directive to the failed child only, with
// MaxRestarts 10, Window 1 minute, MinBackoff 10 ms and MaxBackoff 10 s.
func OneForOne(decider func(err error) Directive) Strategy {
	return newStrategy(supervise.OneForOne, decider)
}

// AllForOne returns a strategy that restarts or stops every child of the supervisor when one
// fails, with the same limits as OneForOne. Restarts are counted for the group.
func AllForOne(decider func(err error) Directive) Strategy {
	return newStrategy(supervise.AllForOne, decider)
}

func newStrategy(scope supervise.Scope, decider func(err error) Directive) Strategy {
	return Strategy{
		scope:       uint8(scope),
		Decider:     decider,
		MaxRestarts: defaultMaxRestarts,
		Window:      defaultWindow,
		MinBackoff:  defaultMinBackoff,
		MaxBackoff:  defaultMaxBackoff,
	}
}

// WithStrategy sets the strategy for the children of the spawned actor. Without it, the strategy
// is OneForOne(nil). Spawn returns an error wrapping ErrInvalidOption if s is invalid (SUP-370).
func WithStrategy(s Strategy) SpawnOption {
	return func(c *spawnConfig) {
		if err := validateStrategy(s); err != nil {
			c.fail(err)
			return
		}
		c.strategy = s
	}
}

// WithGuardianStrategy sets the user guardian's strategy for top-level actors. Without it, the
// strategy is OneForOne(nil). New panics with an error wrapping ErrInvalidOption if s is invalid
// (SUP-370).
func WithGuardianStrategy(s Strategy) Option {
	return func(c *systemConfig) {
		if err := validateStrategy(s); err != nil {
			c.fail(err)
			return
		}
		c.guardian = s
	}
}

// limits returns s's restart limits.
func (s Strategy) limits() supervise.Limits {
	return supervise.Limits{MaxRestarts: s.MaxRestarts, Window: s.Window, MinBackoff: s.MinBackoff, MaxBackoff: s.MaxBackoff}
}

// validateStrategy runs the checks of SUP-370 in order and returns the first failure as
// fmt.Errorf("%w: %s", ErrInvalidOption, text), or nil.
func validateStrategy(s Strategy) error {
	if s.scope != uint8(supervise.OneForOne) && s.scope != uint8(supervise.AllForOne) {
		return fmt.Errorf("%w: Strategy must be built with OneForOne or AllForOne", ErrInvalidOption)
	}
	if err := s.limits().Validate(); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidOption, err.Error())
	}
	return nil
}
