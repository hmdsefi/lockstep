// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// ErrActorCap is the error of a spawn refused because the System has reached its actor cap
// (WithMaxActors). The refusal's text names the parent, the live actor count, the cap and its
// reserve, and errors.Is matches ErrActorCap.
var ErrActorCap = errors.New("lockstep: actor cap reached")

// DefaultMaxActors is the actor cap of a System built without WithMaxActors.
const DefaultMaxActors = 1_000_000

// WithMaxActors sets the maximum number of live actors of the System to n. n/100 of them (at least
// 1 when n >= 2) are reserved for restart rebuilds and system actors. An n < 1 makes New panic
// with an error wrapping ErrInvalidOption.
func WithMaxActors(n int) Option {
	return func(c *systemConfig) {
		if n < 1 {
			c.fail(fmt.Errorf("%w: WithMaxActors: n must be at least 1 (got %d)", ErrInvalidOption, n))
			return
		}
		c.maxActors = n
	}
}

// WithAdmission sets absolute admission thresholds in bytes: admission stops when the live heap
// reaches high and resumes when it falls to low. WithAdmission(0, 0) turns admission off even
// when a Go memory limit is set. Any other pair needs 0 < low < high, or New panics with an error
// wrapping ErrInvalidOption. Simulation ignores it; its byte budgets are sim.Options.Budget.
func WithAdmission(high, low uint64) Option {
	return func(c *systemConfig) {
		if (high != 0 || low != 0) && (low == 0 || low >= high) {
			c.fail(fmt.Errorf("%w: WithAdmission: need 0 < low < high (got low=%d high=%d)", ErrInvalidOption, low, high))
			return
		}
		c.admission = admissionConfig{set: true, high: high, low: low}
	}
}

// WithMemoryEstimates turns on per-actor memory estimates (Sizer, MemSizer, System.Estimates).
func WithMemoryEstimates() Option {
	return func(c *systemConfig) { c.estimates = true }
}

// DefaultWatchdogTimeout is the stuck-turn timeout of a production System without WithWatchdog.
const DefaultWatchdogTimeout = 5 * time.Second

// WithWatchdog sets the stuck-turn timeout. A turn that runs at least d is logged and its actor is
// stuck until the turn returns: messages sent to it meanwhile meet its mailbox's policy as if the
// mailbox were full, with an error wrapping ErrMailboxFull. When the turn returns, the actor fails
// with an error wrapping ErrTimeout, which its supervisor handles. 0 turns the watchdog off. A
// d < 0 makes New panic with an error wrapping ErrInvalidOption. Simulation ignores it.
func WithWatchdog(d time.Duration) Option {
	return func(c *systemConfig) {
		if d < 0 {
			c.fail(fmt.Errorf("%w: WithWatchdog: d must be >= 0 (got %s)", ErrInvalidOption, d))
			return
		}
		c.watchdog = d
	}
}

// DefaultGoroutineWarning is the goroutine count above which a production System logs a warning.
const DefaultGoroutineWarning = 10_000

// WithGoroutineWarning sets the goroutine count that triggers a warning. 0 turns the warning off
// (the count is still exported). A bound < 0 makes New panic with an error wrapping
// ErrInvalidOption. Simulation ignores it.
func WithGoroutineWarning(bound int) Option {
	return func(c *systemConfig) {
		if bound < 0 {
			c.fail(fmt.Errorf("%w: WithGoroutineWarning: bound must be >= 0 (got %d)", ErrInvalidOption, bound))
			return
		}
		c.goroutineWarning = bound
	}
}

// admissionConfig is WithAdmission's pair (OVL-350). Without the option, set is false and the
// thresholds follow the Go memory limit (OVL-310); with WithAdmission(0, 0) admission is off.
type admissionConfig struct {
	set       bool
	high, low uint64
}

// capError is the error of a spawn refused by the actor cap (OVL-050). It matches ErrActorCap.
type capError struct {
	parent               string // the parent's path
	live, limit, reserve int
}

func (e *capError) Error() string {
	return "lockstep: spawn in " + e.parent + ": actor cap reached (live " + strconv.Itoa(e.live) +
		", limit " + strconv.Itoa(e.limit) + ", reserve " + strconv.Itoa(e.reserve) + ")"
}

func (*capError) Unwrap() error { return ErrActorCap }

// liveActors returns the system's live-actor count (OVL-020): the actors whose spawn was admitted
// and that have not been removed from the registry. The guardians are not counted.
func (s *System) liveActors() int { return s.actors.Live() }

// spawnsSystemActor reports whether a child of the parent at parentPath is a system actor, an actor
// under the system guardian (ACT-030). Spawns of system actors are reserve spawns (OVL §3).
func spawnsSystemActor(parentPath string) bool {
	return parentPath == "/system" || strings.HasPrefix(parentPath, "/system/")
}

// recordCap emits lockstep.overload with event=cap (OVL §8) for the spawn refused with e;
// reserved reports whether it was a reserve spawn.
func (s *System) recordCap(e *capError, reserved bool) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.overload",
		engine.Attr{Key: "event", Value: "cap"},
		engine.Attr{Key: "parent", Value: e.parent},
		engine.Attr{Key: "live", Value: strconv.Itoa(e.live)},
		engine.Attr{Key: "limit", Value: strconv.Itoa(e.limit)},
		engine.Attr{Key: "reserve", Value: strconv.Itoa(e.reserve)},
		engine.Attr{Key: "reserved", Value: strconv.FormatBool(reserved)},
	)
}
