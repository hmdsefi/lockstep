// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package hooks

import "github.com/hmdsefi/lockstep/internal/engine"

// The declarations below are CLU §4.1: they let package cluster, which may import only lockstep,
// codec and internal/hooks (DET-080), spawn under the system guardian, end the death watches of a
// down node and convert addresses for Signal. Package lockstep sets the four hooks in its init
// function (ENG-210); each is nil until package lockstep is initialized. Called by cluster;
// implemented by ACT. Every sys is a *lockstep.System, and a hook panics if it has another type.

// SystemSpawner returns a lockstep.Spawner (as any) whose children live under /system of sys: they
// are spawned as ACT-030's internal spawnSystem spawns them (parent the system guardian, a reserve
// spawn) and the system guardian supervises them.
var SystemSpawner func(sys any) any

// NodeDown ends every watch a local actor of sys holds on an actor of node, with
// Terminated{watched, Failed(reason)}, as WatchesDown does, then marks node down: until NodeUp, a
// Watch of an address on node delivers Terminated{addr, Failed(reason)} at once and hands nothing
// to the transport. Marking a marked node again replaces the reason. A node equal to sys's own is
// ignored. Message routing does not change: sends and asks to node go to the transport as before.
// It records nothing, and panics if reason is nil (CLU-530).
var NodeDown func(sys any, node string, reason error)

// NodeUp clears node's down mark; a node without a mark is a no-op (CLU-530).
var NodeUp func(sys any, node string)

// EngineAddress returns addr, a lockstep.Address, as an engine.Address, so that packages that may
// not import internal/engine can call Signal. It panics if addr has another type.
var EngineAddress func(addr any) engine.Address

// ValidNodeName is engine.ValidNodeName, for packages that may not import internal/engine: it
// reports whether name is a valid node name (ACT-020).
func ValidNodeName(name string) bool { return engine.ValidNodeName(name) }
