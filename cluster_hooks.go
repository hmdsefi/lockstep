// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// This file is the cluster hooks (CLU §4.1): package cluster spawns its actors under the system
// guardian through systemSpawner, and ends and short-cuts the death watches of the nodes it
// declares down through the outgoing watches' down marks (watch.go). Neither NodeDown nor NodeUp
// records anything: the membership actor records the transitions.

// systemSpawner is the Spawner hooks.SystemSpawner returns: Spawn with it spawns a child of the
// system guardian, as spawnSystem does (ACT-030). The child's path is under /system, the spawn is a
// reserve spawn (OVL §3), and the system guardian supervises the child (SUP-300).
type systemSpawner struct{ sys *System }

func (systemSpawner) spawner() {}

func (p systemSpawner) nilSpawner() bool { return p.sys == nil }

// spawnParent returns the system guardian as the parent; like a System, a systemSpawner spawns
// outside any turn.
//
//nolint:unparam // implements spawnParent, whose Context implementation returns the spawning cell
func (p systemSpawner) spawnParent() (*System, Address, engine.ActorRef) {
	return p.sys, p.sys.system, nil
}

func (systemSpawner) parentStopping() bool { return false }

func (systemSpawner) rebuilding() bool { return false }

// systemSpawnerHook is hooks.SystemSpawner: the Spawner of sys's system actors.
func systemSpawnerHook(sys any) any {
	return systemSpawner{sys: hookSystem("SystemSpawner", sys)}
}

// nodeDownHook is hooks.NodeDown (CLU §4.1): every watch a local actor of sys holds on an actor on
// node ends with Failed(reason), as hooks.WatchesDown ends it, and node is marked down with reason
// in the same operation (nodeDown). Until hooks.NodeUp clears the mark, Watch of an address on node
// ends at once with Failed(reason) and hands the transport nothing (watchRemote). Sends and asks
// are not affected. A node equal to sys's own is ignored. It panics if reason is nil.
func nodeDownHook(sys any, node string, reason error) {
	s := hookSystem("NodeDown", sys)
	if reason == nil {
		panic("lockstep: hooks.NodeDown: nil error")
	}
	if node == s.node {
		return
	}
	s.nodeDown(node, reason)
}

// nodeUpHook is hooks.NodeUp (CLU §4.1): it clears node's down mark; a node without one is a no-op.
func nodeUpHook(sys any, node string) {
	hookSystem("NodeUp", sys).rwatch.markUp(node)
}

// engineAddressHook is hooks.EngineAddress: addr, an Address, as the engine's address type. It
// panics if addr has another type.
func engineAddressHook(addr any) engine.Address {
	a, ok := addr.(Address)
	if !ok {
		panic(fmt.Sprintf("lockstep: hooks.EngineAddress: addr has type %T, want a lockstep.Address", addr))
	}
	return a.engine()
}

// claimClusterHook is hooks.ClaimCluster (CLU-020): it marks sys joined and reports whether this
// call set the mark.
func claimClusterHook(sys any) bool {
	return hookSystem("ClaimCluster", sys).clusterJoined.CompareAndSwap(false, true)
}

// releaseClusterHook is hooks.ReleaseCluster (CLU-020): it clears the mark of a failed Join.
func releaseClusterHook(sys any) {
	hookSystem("ReleaseCluster", sys).clusterJoined.Store(false)
}

// checkSpawnOptionsHook is hooks.CheckSpawnOptions (VAC-010): it applies opts, in argument order, to
// a spawn configuration that nothing uses and returns the error Spawn would return for the first
// invalid one. An element that is not a non-nil SpawnOption is reported as an invalid option
// instead of panicking as Spawn does.
func checkSpawnOptionsHook(opts []any) error {
	cfg := &spawnConfig{strategy: OneForOne(nil)}
	for i, o := range opts {
		so, ok := o.(SpawnOption)
		if !ok || so == nil {
			return fmt.Errorf("%w: option %d is %T, want a non-nil SpawnOption", ErrInvalidOption, i, o)
		}
		so(cfg)
	}
	return cfg.err
}
