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
	sys  *lockstep.System
	cfg  Config                  // validated; never changed after Join
	self Member                  // the own entry at Join
	pid  lockstep.PID[memberMsg] // the membership actor; set by Join before it returns

	// view is the last published view; never nil (CLU-500).
	view atomic.Pointer[View]

	// leaving is set by a Leave whose request reached the actor's mailbox; left is set by the
	// actor once the own entry is Left (CLU-600, CLU-610). Either makes Leave and Subscribe return
	// ErrLeft.
	leaving atomic.Bool
	left    atomic.Bool

	// The fields below are read and written only in the membership actor's turns; they live here
	// so that a restarted actor keeps them (CLU-520, CLU-620). subs are the subscribers in
	// subscription order; since only the actor publishes views, signals and subscribes, every
	// subscriber gets the signals of the views in their order (CLU-530). joined is set when the
	// node has joined (CLU-030), and started by the first Started, so that a later Started is a
	// restart.
	subs    []lockstep.Address
	joined  bool
	started bool
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
	m := &Membership{sys: sys, cfg: c, self: self}
	m.view.Store(&View{Version: 1, Self: self, Members: []Member{self}})
	sp, ok := hooks.SystemSpawner(sys).(lockstep.Spawner)
	if !ok {
		panic("cluster: hooks.SystemSpawner returned no lockstep.Spawner") // unreachable
	}
	pid, err := lockstep.Spawn(sp, actorName, func() lockstep.Actor[memberMsg] { return newMemberActor(m) })
	if err != nil {
		hooks.ReleaseCluster(sys)
		return nil, fmt.Errorf("cluster: join: %w", err)
	}
	m.pid = pid
	return m, nil
}

// View returns the last published view of the member table. For a Membership that Join returned it
// is never nil (the zero Membership returns nil), and it is never changed after it was published:
// a new View, with the next Version, replaces it whenever the table changes.
func (m *Membership) View() *View { return m.view.Load() }

// Subscribe makes the local actor at addr a subscriber of membership signals, which reach its
// Signal method (lockstep.SignalReceiver). The membership actor adds it in a later turn and then
// sends it one lockstep.NodeUp for every member other than this node that is up in the latest
// View, in node name order. After that, whenever a published View shows a member other than this
// node come up, the subscriber receives NodeUp, and whenever one goes down, NodeDown, with Left
// set when the member left gracefully; a member that restarted under a new ID gives NodeDown of
// the old ID, then NodeUp of the new one. Every subscriber receives the signals in the order of the
// Views, so for a subscriber that stays running the signals about each node name alternate,
// starting with NodeUp. A signal that arrives while the subscriber is not running (suspended, or
// between the incarnations of a restart) is dropped, as the runtime drops other signals then, and
// the alternation may break for that subscriber.
//
// Subscribing a subscriber again does nothing. A subscriber is removed, or not added, when a
// signal cannot be delivered to it because no actor is alive at addr. Subscribe returns an error
// wrapping lockstep.ErrNotFound for the zero Address, lockstep.ErrNotLocal for an address on
// another node, ErrLeft once Leave was called or the node left, and an error wrapping the send's
// error when the request cannot reach the membership actor. Subscriptions survive a restart of
// the membership actor.
func (m *Membership) Subscribe(addr lockstep.Address) error {
	switch {
	case addr.IsZero():
		return fmt.Errorf("cluster: subscribe %v: %w", addr, lockstep.ErrNotFound)
	case addr.Node() != m.self.Node:
		return fmt.Errorf("cluster: subscribe %v: %w", addr, lockstep.ErrNotLocal)
	case m.leaving.Load() || m.left.Load():
		return fmt.Errorf("cluster: subscribe %v: %w", addr, ErrLeft)
	}
	if err := lockstep.Send(m.sys, m.pid, memberMsg(subscribeCmd{Addr: addr})); err != nil {
		return fmt.Errorf("cluster: subscribe %v: %w", addr, err)
	}
	return nil
}

// Unsubscribe removes the subscriber at addr. The membership actor removes it in a later turn; the
// subscriber receives no signal of a View published after that. An address that is not a
// subscriber is ignored.
func (m *Membership) Unsubscribe(addr lockstep.Address) {
	if err := lockstep.Send(m.sys, m.pid, memberMsg(unsubscribeCmd{Addr: addr})); err != nil {
		return // the actor has stopped, so it sends no more signals
	}
}

// Leave makes this node leave the cluster gracefully and returns at once: the membership actor
// marks the own entry Left, tells every other up member, publishes the View that shows it and
// stops, and the other members see this node as Left, not Dead. Shutting the System down leaves in
// the same way. Leave returns ErrLeft when it was called before or the node has left, and an error
// wrapping the send's error when the request cannot reach the membership actor. A System that left
// cannot join again: Join returns ErrAlreadyJoined.
func (m *Membership) Leave() error {
	if m.left.Load() || !m.leaving.CompareAndSwap(false, true) {
		return ErrLeft
	}
	if err := lockstep.Send(m.sys, m.pid, memberMsg(leaveCmd{})); err != nil {
		m.leaving.Store(false)
		return fmt.Errorf("cluster: leave: %w", err)
	}
	return nil
}
