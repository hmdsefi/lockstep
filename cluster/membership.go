// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
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

	// joined is set by the membership actor once the node has joined (CLU-030), after it published
	// the view of the turn that joined, so that whoever reads joined and then the view sees that
	// view or a later one. Grain.Send and the grain hosts read it (VAC-200, VAC-300).
	joined atomic.Bool

	// The grain hosts, one per Config.Kinds entry in that order (VAC-300, VAC-600): the path PID
	// of each kind's host, and the HostInfo each host publishes. They live here so that they
	// survive restarts of the hosts.
	hostPIDs []lockstep.PID[hostMsg]
	infos    []atomic.Pointer[HostInfo]

	// The fields below are read and written only in the membership actor's turns; they live here
	// so that a restarted actor keeps them (CLU-520, CLU-620). subs are the subscribers in
	// subscription order; since only the actor publishes views, signals and subscribes, every
	// subscriber gets the signals of the views in their order (CLU-530). started is set by the
	// first Started, so that a later Started is a restart.
	subs    []lockstep.Address
	started bool
}

// Join makes sys a member of the cluster that c describes and returns at once. It copies c, fills
// in its defaults and checks it as Config.Validate does, then starts the system actor
// /system/cluster, which joins through the seeds and keeps the member table. Until the node has
// joined, the actor asks every other seed for its table at once and then every JoinRetryInterval;
// a node whose seeds name no other node has joined at once.
//
// Join then starts the system actor /system/grains and under it one host per kind of c.Kinds, at
// /system/grains/<kind>, which runs the activations of the grains of that kind that this node
// owns (see Grain). If they cannot be started, Join stops the membership actor again and returns
// the error.
//
// The first View, published before Join returns, holds only this node: Alive with incarnation 0,
// the System's incarnation as its ID and the sorted names of c.Kinds as its Kinds (nil for none).
// The other members learn the kinds from the membership protocol. A System joins at most once: a
// second Join returns ErrAlreadyJoined, also after the node left or the membership actor stopped,
// and after a Join that could not start the grain hosts. Join panics if sys is nil.
func Join(sys *lockstep.System, c Config) (*Membership, error) {
	if sys == nil {
		panic("cluster: Join with nil System")
	}
	c.Seeds = slices.Clone(c.Seeds)
	c.Kinds = slices.Clone(c.Kinds)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	// The joined mark lives on the System (CLU-020): it is set once and cleared only when this
	// Join fails to spawn the actor.
	if !hooks.ClaimCluster(sys) {
		return nil, ErrAlreadyJoined
	}
	self := Member{Node: sys.Node(), ID: sys.Incarnation(), State: Alive, Kinds: kindNames(c.Kinds)}
	m := &Membership{sys: sys, cfg: c, self: self}
	m.hostPIDs = make([]lockstep.PID[hostMsg], len(c.Kinds))
	m.infos = make([]atomic.Pointer[HostInfo], len(c.Kinds))
	for i, k := range c.Kinds {
		m.hostPIDs[i] = lockstep.PIDAt[hostMsg](self.Node, grainsPath+"/"+k.name)
		m.infos[i].Store(&HostInfo{Kind: k.name})
	}
	m.view.Store(newView(1, self, []Member{self}))
	sp, ok := hooks.SystemSpawner(sys).(lockstep.Spawner)
	if !ok {
		panic("cluster: hooks.SystemSpawner returned no lockstep.Spawner") // unreachable
	}
	// An explicit mailbox, so that a system default that drops or blocks never applies: a command
	// that does not fit is refused, and Subscribe and Leave return the error.
	pid, err := lockstep.Spawn(sp, actorName, func() lockstep.Actor[memberMsg] { return newMemberActor(m) },
		lockstep.WithMailbox(mailboxCapacity, lockstep.FailSend))
	if err != nil {
		hooks.ReleaseCluster(sys)
		return nil, fmt.Errorf("cluster: join: %w", err)
	}
	m.pid = pid
	// The grain hosts come after the membership actor (VAC-300). If they cannot start, the
	// membership actor is stopped again and the System stays marked joined, as after a Leave.
	if err := spawnGrains(sp, m); err != nil {
		if serr := lockstep.Stop(sys, pid.Address()); serr != nil {
			err = errors.Join(err, serr)
		}
		return nil, fmt.Errorf("cluster: join: %w", err)
	}
	return m, nil
}

// View returns the last published view of the member table. For a Membership that Join returned it
// is never nil (the zero Membership returns nil), and it is never changed after it was published:
// a new View, with the next Version, replaces it whenever the table changes.
func (m *Membership) View() *View { return m.view.Load() }

// Subscribe makes the local actor at addr a subscriber of membership signals, which reach its
// Signal method (lockstep.SignalReceiver). addr must name one actor, with its UID, as the Address
// of a PID that Spawn returned does. Subscribe returns before the subscription takes effect: the
// membership actor adds the subscriber in its next turn and then sends it one lockstep.NodeUp for
// every member other than this node that is up in the latest View, in node name order. After
// that, whenever a published View shows a member other than this node come up, the subscriber
// receives NodeUp, and whenever one goes down, NodeDown, with Left set when the member left
// gracefully; a member that restarted under a new ID gives NodeDown of the old ID, then NodeUp of
// the new one. Every subscriber receives the signals in the order of the Views, so for a
// subscriber that stays running the signals about each node name alternate, starting with NodeUp.
// A signal that arrives while the subscriber is not running (suspended, or between the
// incarnations of a restart) is dropped, as the runtime drops other signals then, and the
// alternation may break for that subscriber. A subscriber that restarts keeps its UID and stays
// subscribed, and subscribing it again does nothing, so after a restart it reads View for the
// current members. An actor that does not implement SignalReceiver stays subscribed and ignores
// the signals.
//
// A subscriber is removed, or not added, when a signal cannot be delivered to it because no actor
// is alive at addr. Subscribe returns an error wrapping lockstep.ErrNotFound for the zero Address
// or an Address without a UID, lockstep.ErrNotLocal for an address on another node, ErrLeft once
// Leave was called or the node left, and an error wrapping the send's error when the request
// cannot reach the membership actor, for example lockstep.ErrMailboxFull. Subscriptions survive a
// restart of the membership actor.
func (m *Membership) Subscribe(addr lockstep.Address) error {
	switch {
	case addr.IsZero():
		return fmt.Errorf("cluster: subscribe %v: %w", addr, lockstep.ErrNotFound)
	case addr.Node() != m.self.Node:
		return fmt.Errorf("cluster: subscribe %v: %w", addr, lockstep.ErrNotLocal)
	case addr.UID() == 0:
		return fmt.Errorf("cluster: subscribe %v: %w", addr, lockstep.ErrNotFound)
	case m.leaving.Load() || m.left.Load():
		return fmt.Errorf("cluster: subscribe %v: %w", addr, ErrLeft)
	}
	if err := lockstep.Send(m.sys, m.pid, memberMsg(subscribeCmd{Addr: addr})); err != nil {
		if m.left.Load() {
			return fmt.Errorf("cluster: subscribe %v: %w", addr, ErrLeft)
		}
		return fmt.Errorf("cluster: subscribe %v: %w", addr, err)
	}
	return nil
}

// Unsubscribe removes the subscriber at addr. It returns before the removal takes effect: the
// membership actor removes the subscriber in its next turn, and the subscriber receives no signal
// of a View published after that. An address that is not a subscriber is ignored, and so is the
// request when it cannot reach the membership actor (the actor stopped, or its mailbox is full).
func (m *Membership) Unsubscribe(addr lockstep.Address) {
	if err := lockstep.Send(m.sys, m.pid, memberMsg(unsubscribeCmd{Addr: addr})); err != nil {
		return // the actor has stopped, so it sends no more signals, or the request did not fit
	}
}

// Leave makes this node leave the cluster gracefully and returns at once: the membership actor
// marks the own entry Left, tells every other up member, publishes the View that shows it and
// stops, and the other members see this node as Left, not Dead. Shutting the System down leaves in
// the same way. Leave returns ErrLeft when it was called before or the node has left, and an error
// wrapping the send's error when the request cannot reach the membership actor, for example
// lockstep.ErrMailboxFull; Leave may then be called again. A System that left cannot join again:
// Join returns ErrAlreadyJoined.
func (m *Membership) Leave() error {
	if m.left.Load() || !m.leaving.CompareAndSwap(false, true) {
		return ErrLeft
	}
	if err := lockstep.Send(m.sys, m.pid, memberMsg(leaveCmd{})); err != nil {
		m.leaving.Store(false)
		if m.left.Load() {
			return ErrLeft // the actor left meanwhile, as a shutdown makes it leave
		}
		return fmt.Errorf("cluster: leave: %w", err)
	}
	return nil
}

// Host returns the latest HostInfo of the host of kind on this node, and false when this node
// does not host kind (it is not in Config.Kinds). Before the host's first turn, the HostInfo has
// only its Kind set. The HostInfo is a copy, but its slices are shared: callers must not modify
// them.
func (m *Membership) Host(kind string) (HostInfo, bool) {
	i := m.kindIndex(kind)
	if i < 0 {
		return HostInfo{}, false
	}
	return *m.infos[i].Load(), true
}

// kindIndex returns the index in Config.Kinds of the kind named name, or -1.
func (m *Membership) kindIndex(name string) int {
	for i := range m.cfg.Kinds {
		if m.cfg.Kinds[i].name == name {
			return i
		}
	}
	return -1
}
