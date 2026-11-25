// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package hooks

import "github.com/hmdsefi/lockstep/internal/engine"

// The declarations below are CLU §4.1: they let package cluster, which may import only lockstep,
// codec and internal/hooks (DET-080), spawn under the system guardian, end the death watches of a
// down node, convert addresses for Signal, mark a system joined and check spawn options. Package
// lockstep sets the seven hooks in its init function (ENG-210); each is nil until package lockstep
// is initialized. Called by cluster; implemented by ACT. Every sys is a *lockstep.System, and a
// hook that takes one panics if it has another type.

// SystemSpawner returns a lockstep.Spawner (as any) whose children live under /system of sys: they
// are spawned as ACT-030's internal spawnSystem spawns them (parent the system guardian, a reserve
// spawn) and the system guardian supervises them.
var SystemSpawner func(sys any) any

// NodeDown ends every watch a local actor of sys holds on an actor of node, with
// Terminated{watched, Failed(reason)}, as WatchesDown does, and hands the transport a RemoteUnwatch
// for each, so that the transport and the peer drop them too. Then it marks node down: until
// NodeUp, a Watch of an address on node delivers Terminated{addr, Failed(reason)} at once and hands
// nothing to the transport. Marking a marked node again replaces the reason. A node equal to
// sys's own is ignored. Message routing does not change: sends and asks to node go to the
// transport as before. It records nothing, and panics if reason is nil (CLU-530). Call it in engine
// context, inside a turn or a callback, as package cluster does from the membership actor's turn:
// the transport's Deliver gets the RemoteUnwatch envelopes, and a call that races a Watch of an
// address on node from another goroutine could reach the transport ahead of that Watch's
// RemoteWatch, which would then stay registered.
var NodeDown func(sys any, node string, reason error)

// NodeUp clears node's down mark; a node without a mark is a no-op (CLU-530).
var NodeUp func(sys any, node string)

// EngineAddress returns addr, a lockstep.Address, as an engine.Address, so that packages that may
// not import internal/engine can call Signal. It panics if addr has another type.
var EngineAddress func(addr any) engine.Address

// ClaimCluster marks sys as joined to a cluster and reports true the first time it is called for
// sys, and false on every later call: the mark is never cleared except by ReleaseCluster, so a
// second Join fails also after the node left or the membership actor stopped (CLU-020). It is
// atomic, so of two concurrent calls exactly one reports true.
var ClaimCluster func(sys any) bool

// ReleaseCluster clears the mark ClaimCluster set on sys. Package cluster calls it only when the
// Join that claimed sys failed to spawn the membership actor (CLU-020).
var ReleaseCluster func(sys any)

// ValidNodeName is engine.ValidNodeName, for packages that may not import internal/engine: it
// reports whether name is a valid node name (ACT-020).
func ValidNodeName(name string) bool { return engine.ValidNodeName(name) }

// CheckSpawnOptions checks opts, each a lockstep.SpawnOption (as any), as lockstep.Spawn would
// check them, and returns the error a Spawn with these options returns: the error of the first
// invalid option, in argument order, which wraps lockstep.ErrInvalidOption, or nil. A nil element,
// or one of another type, is an invalid option too (Spawn itself panics on a nil option). It
// spawns nothing, so package cluster can check the options of a grain kind when a node joins
// (VAC-010). Set by package lockstep; nil until it is initialized.
var CheckSpawnOptions func(opts []any) error
