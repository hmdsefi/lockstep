// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// Membership is this node's membership in a cluster. A Membership comes only from Join; the zero
// Membership is not usable. Its methods are safe for concurrent use.
type Membership struct {
	cfg  Config // validated; never changed after Join
	self Member // the own entry at Join

	// view is the last published view; never nil (CLU-500).
	view atomic.Pointer[View]

	// joined is set when the node has joined (CLU-030). Only the membership actor's turns read and
	// write it; it lives here so that a restarted actor knows it (CLU-620).
	joined bool
}

// Join makes sys a member of the cluster that c describes and returns at once. It copies c, fills
// in its defaults and checks it as Config.Validate does, then starts the system actor
// /system/cluster, which joins through the seeds and keeps the member table. Until the node has
// joined, the actor asks every other seed for its table at once and then every JoinRetryInterval;
// a node whose seeds name no other node has joined at once.
//
// The first View, published before Join returns, holds only this node: Alive with incarnation 0
// and the System's incarnation as its ID. A System joins at most once: a second Join returns
// ErrAlreadyJoined, also after the node left or the membership actor stopped. Join panics if sys
// is nil.
func Join(sys *lockstep.System, c Config) (*Membership, error) {
	if sys == nil {
		panic("cluster: Join with nil System")
	}
	c.Seeds = slices.Clone(c.Seeds)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	// The joined mark lives on the System (CLU-020): it is set once and cleared only when this
	// Join fails to spawn the actor.
	if !hooks.ClaimCluster(sys) {
		return nil, ErrAlreadyJoined
	}
	self := Member{Node: sys.Node(), ID: sys.Incarnation(), State: Alive}
	m := &Membership{cfg: c, self: self}
	m.view.Store(&View{Version: 1, Self: self, Members: []Member{self}})
	sp, ok := hooks.SystemSpawner(sys).(lockstep.Spawner)
	if !ok {
		panic("cluster: hooks.SystemSpawner returned no lockstep.Spawner") // unreachable
	}
	if _, err := lockstep.Spawn(sp, actorName, func() lockstep.Actor[memberMsg] { return newMemberActor(m) }); err != nil {
		hooks.ReleaseCluster(sys)
		return nil, fmt.Errorf("cluster: join: %w", err)
	}
	return m, nil
}

// View returns the last published view of the member table. For a Membership that Join returned it
// is never nil (the zero Membership returns nil), and it is never changed after it was published:
// a new View, with the next Version, replaces it whenever the table changes.
func (m *Membership) View() *View { return m.view.Load() }
