// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist

import "github.com/hmdsefi/lockstep"

// EventSourced defines a persistent actor with command type C, event type E and state type S.
// The factory passed to Spawn returns a fresh value at every start and restart.
type EventSourced[C, E, S any] interface {
	// Initial returns the state before the first event.
	Initial() S
	// Handle decides what cmd does and returns the events to persist, or nil. Replies and other
	// side effects that must wait for persistence are registered with ctx.Then.
	Handle(ctx *Context[C, E, S], state S, cmd C) []E
	// Apply folds one event into the state. It must be a pure function of its arguments.
	Apply(state S, event E) S
}

// Recoverer is implemented by an EventSourced that wants to know when recovery finished.
type Recoverer[C, E, S any] interface {
	Recovered(ctx *Context[C, E, S], state S, r Recovery)
}

// Stopper is implemented by an EventSourced that wants the lockstep Stopped hook.
type Stopper[C, E, S any] interface {
	Stopped(ctx *Context[C, E, S], state S, reason lockstep.StopReason)
}

// SignalReceiver is implemented by an EventSourced that wants lockstep signals.
type SignalReceiver[C, E, S any] interface {
	Signal(ctx *Context[C, E, S], state S, sig lockstep.Signal)
}

// Config configures a persistent actor.
type Config[E, S any] struct {
	Journal       Journal       // required
	Events        Codec[E]      // required
	Snapshots     SnapshotStore // nil: no snapshots
	State         Codec[S]      // required when Snapshots is set
	SnapshotEvery uint64        // snapshot after this many events since the last one; 0: only on request
	PruneEvents   bool          // delete events covered by the previous snapshot; needs Snapshots
	ReplayBatch   int           // events per Read during recovery; 0 means DefaultReplayBatch
}
