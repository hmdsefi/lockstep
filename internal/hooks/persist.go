// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package hooks

import (
	"context"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// The declarations below are PER §4.3. Package persist calls Control; tests in sim and persist
// call Control and Signal. Package lockstep sets both in its init function (ENG-210); they are nil
// until package lockstep is initialized.

// Control returns the controls of an actor context. ctx is a *lockstep.Context[M] for some M,
// used during one of its turns. Implemented by ACT (contexts) with the parts below.
var Control func(ctx any) ActorControl

// ActorControl gives runtime-internal packages access to one actor incarnation.
type ActorControl interface {
	// Address returns the PID's String() (ACT-410): lockstep://<node><path>#<uid in lowercase hex>.
	Address() string
	// StartInternal starts e for this incarnation and returns a cancel function (PER-240).
	// Implemented by EFF's effect path.
	StartInternal(e InternalEffect) (cancel func())
	// PauseUser stops (true) or resumes (false) the delivery of user messages (PER-250).
	// Implemented by MBX's turn loop.
	PauseUser(paused bool)
	// TraceEnabled reports the engine Tracer's Enabled (ENG-110).
	TraceEnabled() bool
	// Record emits Tracer.Record(kind, attrs...) with the full kind (R8); kv holds attribute
	// keys and values, alternating.
	Record(kind string, kv ...string)
}

// InternalEffect is an effect whose completion runs a callback instead of building a message.
type InternalEffect struct {
	Name   string                                 // effect name for traces, for example "lockstep.persist.write"
	Inline bool                                   // simulation calls Run in a node event (PER-520)
	Run    func(ctx context.Context) (any, error) // the operation
	Done   func(ctx any, v any, err error)        // runs in a later turn; ctx is that turn's *lockstep.Context[M]
}

// Signal enqueues sig, a lockstep.Signal, on the system queue of the live local actor at to on
// sys (a *lockstep.System), as SUP delivers signals, and reports true; it reports false and
// does nothing if no actor at to is alive. The receiver's watches and subscriptions do not
// filter sig (a lockstep.Terminated for an address the actor never watched still reaches
// Signal), and sig does not consume a watch. Outside Running, sig is kept or dropped as SUP does
// for unfiltered signals. Implemented by ACT (registry) and SUP (signal delivery).
var Signal func(sys any, to engine.Address, sig any) bool
