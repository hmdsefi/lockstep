// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist

import (
	"fmt"

	"github.com/hmdsefi/lockstep"
)

// Spawn spawns a persistent actor named name under parent, with persistence ID id. The actor's
// message type is C, so it is addressed and asked like any lockstep actor. opts are passed to
// lockstep.Spawn unchanged.
//
// Spawn checks its arguments first and spawns nothing if one is wrong. The error wraps
// ErrInvalidID for an id that is empty or longer than MaxIDLen bytes, and ErrInvalidConfig for a
// nil factory, a Config without Journal or Events, Snapshots without State, SnapshotEvery or
// PruneEvents without Snapshots, or a negative ReplayBatch. Otherwise it returns the PID and
// error of lockstep.Spawn.
//
// factory is called at every start and restart. Each incarnation pauses its user messages,
// recovers its state from the newest snapshot and the events after it, and only then handles
// commands.
func Spawn[C, E, S any](parent lockstep.Spawner, name, id string, factory func() EventSourced[C, E, S],
	cfg Config[E, S], opts ...lockstep.SpawnOption) (lockstep.PID[C], error) {
	if err := check(id, factory == nil, cfg); err != nil {
		return lockstep.PID[C]{}, err
	}
	return lockstep.Spawn(parent, name, func() lockstep.Actor[C] { return newWrapper(id, cfg, factory) }, opts...)
}

// check returns the first defect of Spawn's arguments (PER-010), in the order of PER-010's table,
// or nil.
func check[E, S any](id string, noFactory bool, cfg Config[E, S]) error {
	switch {
	case id == "" || len(id) > MaxIDLen:
		return fmt.Errorf("persist: Spawn: id %q: %w", id, ErrInvalidID)
	case noFactory:
		return fmt.Errorf("persist: Spawn %q: nil factory: %w", id, ErrInvalidConfig)
	case cfg.Journal == nil:
		return fmt.Errorf("persist: Spawn %q: nil Journal: %w", id, ErrInvalidConfig)
	case cfg.Events == nil:
		return fmt.Errorf("persist: Spawn %q: nil Events codec: %w", id, ErrInvalidConfig)
	case cfg.Snapshots != nil && cfg.State == nil:
		return fmt.Errorf("persist: Spawn %q: Snapshots without State codec: %w", id, ErrInvalidConfig)
	case cfg.Snapshots == nil && (cfg.SnapshotEvery > 0 || cfg.PruneEvents):
		return fmt.Errorf("persist: Spawn %q: SnapshotEvery or PruneEvents without Snapshots: %w", id, ErrInvalidConfig)
	case cfg.ReplayBatch < 0:
		return fmt.Errorf("persist: Spawn %q: negative ReplayBatch %d: %w", id, cfg.ReplayBatch, ErrInvalidConfig)
	}
	return nil
}
