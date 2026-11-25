// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/tenure/core"
)

// The grain hosts (VAC-300 to VAC-350, VAC-410, VAC-500, VAC-600). Join spawns the system actor
// /system/grains, which spawns one host per configured kind at /system/grains/<kind>. A host
// routes the messages for the grains of its kind (VAC-310), activates the keys this node owns as
// its children, named by their escaped keys (VAC-410), delivers in arrival order (VAC-330), holds
// messages it cannot route yet in buffers bounded across keys (VAC-340) and publishes HostInfo
// (VAC-600). Hosts are actor code: no wall clock, randomness, goroutines, select, map ranges or
// locks (§6).
//
// This file implements the local path (3.4-03): routing rules 1, 3, 5, 6 and 7; rule 4 (another
// node owns the key) drops with no-owner until 3.4-05 forwards, and rule 2 (yields) comes with
// 3.4-06, as do handoff peers: every activation is handoff=none. Of VAC-500, steps 1 (without the
// previous view), 5 and 6.

// The paths of the grains actor and of the hosts under it (VAC-300).
const (
	grainsName = "grains"
	grainsPath = "/system/" + grainsName
)

// hostMailbox is the capacity of a host's mailbox, whose policy is FailSend; the kind's host
// options come after it and may change it (VAC-300).
const hostMailbox = 1024

// grainPrefix starts the kind of every grain record (§6).
const grainPrefix = "lockstep.grain."

// The drop reasons of this file (VAC-350).
const (
	dropBuffer   = "buffer"
	dropHost     = "host"
	dropMailbox  = "mailbox"
	dropNoOwner  = "no-owner"
	dropActivate = "activate"
)

// handoffNone is the handoff attribute of an activation without handoff peers (VAC-410).
const handoffNone = "none"

// HostInfo describes the host of one grain kind on this node, as Membership.Host returns it. The
// host publishes a new HostInfo after every turn that changed it; key lists are sorted bytewise and
// shared, so callers must not modify them.
type HostInfo struct {
	// Kind is the kind the host hosts.
	Kind string
	// Version is the Version of the last View the host applied; 0 before the first.
	Version uint64
	// Joined reports whether the node had joined the cluster when that View was applied.
	Joined bool
	// Active lists the keys with a running activation.
	Active []string
	// Stopping lists the keys whose activation is stopping.
	Stopping []string
	// Acquiring lists the keys waiting for the previous owners to hand them over.
	Acquiring []string
	// Buffered is the number of messages the host holds until it can route them.
	Buffered int
}

// hostMsg is the sealed message type of a grain host (VAC-300).
type hostMsg interface{ hostMsg() }

// localGrain is a message for the grain Key, sent by reference from this node (Grain.Send). Msg is
// a value of the kind's message type.
type localGrain struct {
	Key string
	Msg any
}

// viewChanged tells a host that the membership actor published a view or the node joined. The
// host reads the view and the joined flag at every turn, so the message only wakes it up.
type viewChanged struct{}

func (localGrain) hostMsg()  {}
func (viewChanged) hostMsg() {}

// grainsMsg is the message type of the grains actor, which receives none.
type grainsMsg struct{}

// spawnGrains spawns the grains actor of m under sp, the system spawner (VAC-300). It supervises
// the hosts OneForOne with the default limits.
func spawnGrains(sp lockstep.Spawner, m *Membership) error {
	_, err := lockstep.Spawn(sp, grainsName, func() lockstep.Actor[grainsMsg] { return &grainsActor{m: m} },
		lockstep.WithMailbox(1, lockstep.FailSend), lockstep.WithStrategy(lockstep.OneForOne(nil)))
	return err
}

// grainsActor is /system/grains: the parent of the hosts.
type grainsActor struct{ m *Membership }

// Started spawns the host of every configured kind, in the order of Config.Kinds, with a FailSend
// mailbox of hostMailbox and then the kind's host options (VAC-300). A host that cannot be spawned
// fails the actor, so that its supervisor restarts it.
func (a *grainsActor) Started(ctx *lockstep.Context[grainsMsg]) {
	for i := range a.m.cfg.Kinds {
		k := &a.m.cfg.Kinds[i]
		opts := append([]lockstep.SpawnOption{lockstep.WithMailbox(hostMailbox, lockstep.FailSend)}, k.host...)
		if _, err := lockstep.Spawn(ctx, k.name, func() lockstep.Actor[hostMsg] { return newHost(a.m, i) }, opts...); err != nil {
			ctx.Fail(fmt.Errorf("cluster: start the host of kind %q: %w", k.name, err))
			return
		}
	}
}

// Receive ignores msg: nothing sends to the grains actor.
func (*grainsActor) Receive(*lockstep.Context[grainsMsg], grainsMsg) {}

// grainPhase is the phase of a key that has state in a host.
type grainPhase uint8

// The phases of a key. 3.4-04 adds stopping and 3.4-06 acquiring; routing rule 3 holds the
// messages of a key in any phase but active.
const (
	phaseActive grainPhase = iota + 1 // a running activation: messages are delivered
)

// grain is the state of one key in a host.
type grain struct {
	phase grainPhase
	addr  lockstep.Address // the activation
	pid   any              // the activation's lockstep.PID[M], for Kind.send
	held  []any            // the messages held for the key, in arrival order (rule 3)
}

// heldMsg is a message held in the joining buffer (rule 1).
type heldMsg struct {
	key string
	msg any
}

// host is the host of one kind. A restarted host is a new value: it starts empty (VAC-450).
type host struct {
	m    *Membership
	kind *Kind
	info *atomic.Pointer[HostInfo]
	self string

	// The applied view (VAC-500): its version and joined flag, and the hosting members of the
	// kind in it, shared with the view's placement. applied is false before the first.
	applied bool
	version uint64
	joined  bool
	members []core.Member

	keys     map[string]*grain // by raw key; never ranged
	active   []string          // the keys in phaseActive, sorted
	joining  []heldMsg         // held until the node has joined, in arrival order (rule 1)
	buffered int               // the messages held in joining and in every key's held (VAC-340)
	changed  bool              // HostInfo changed in this turn (VAC-600)
}

// newHost returns the host of Config.Kinds[i] of m.
func newHost(m *Membership, i int) *host {
	return &host{m: m, kind: &m.cfg.Kinds[i], info: &m.infos[i], self: m.self.Node, keys: map[string]*grain{}}
}

// Started applies the latest view, the first of this incarnation (VAC-450).
func (h *host) Started(ctx *lockstep.Context[hostMsg]) {
	h.sync(ctx)
	h.endTurn()
}

// Receive routes a message for a grain; first it applies a newer view, which is all viewChanged
// asks for.
func (h *host) Receive(ctx *lockstep.Context[hostMsg], msg hostMsg) {
	h.sync(ctx)
	switch msg := msg.(type) {
	case localGrain:
		h.route(ctx, msg.Key, msg.Msg)
	case viewChanged:
	}
	h.endTurn()
}

// Signal handles the Terminated of an activation; first it applies a newer view.
func (h *host) Signal(ctx *lockstep.Context[hostMsg], sig lockstep.Signal) {
	h.sync(ctx)
	if t, ok := sig.(lockstep.Terminated); ok {
		h.terminated(ctx, t)
	}
	h.endTurn()
}

// Stopped drops every held message with reason host and publishes a HostInfo without keys or
// buffered messages: the activations, its children, have stopped before (VAC-450). It runs for a
// restart too; the next incarnation starts empty.
func (h *host) Stopped(ctx *lockstep.Context[hostMsg], _ lockstep.StopReason) {
	for _, x := range h.joining {
		h.drop(ctx, x.key, dropHost)
	}
	h.joining, h.buffered, h.active = nil, 0, nil
	h.keys = map[string]*grain{}
	h.publish()
}

// endTurn publishes HostInfo if the turn changed it (VAC-600).
func (h *host) endTurn() {
	if h.changed {
		h.publish()
	}
}

// publish stores a new HostInfo from the host's state (VAC-600).
func (h *host) publish() {
	h.changed = false
	info := &HostInfo{Kind: h.kind.name, Version: h.version, Joined: h.joined, Buffered: h.buffered}
	if len(h.active) > 0 {
		info.Active = slices.Clone(h.active)
	}
	h.info.Store(info)
}

// sync applies the latest view when it is newer than the applied one, or when the node has joined
// since (VAC-300, VAC-500): it reads the joined flag first, so the view is at least the one of the
// turn that joined. A newly joined node routes the joining buffer in arrival order (step 5); then
// the host publishes HostInfo and records lockstep.grain.view (step 6).
func (h *host) sync(ctx *lockstep.Context[hostMsg]) {
	joined := h.m.joined.Load()
	v := h.m.view.Load()
	if h.applied && v.Version <= h.version && (h.joined || !joined) {
		return
	}
	newly := joined && !h.joined
	h.applied, h.version, h.joined = true, v.Version, joined
	h.members = v.place.hosts(h.kind.name)
	if newly {
		held := h.joining
		h.joining = nil
		h.buffered -= len(held)
		for _, x := range held {
			h.route(ctx, x.key, x.msg)
		}
	}
	h.publish()
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"view", "kind", h.kind.name, "version", strconv.FormatUint(h.version, 10),
			"joined", strconv.FormatBool(h.joined), "active", strconv.Itoa(len(h.active)))
	}
}

// route applies the first routing rule that fits the message msg for key (VAC-310).
func (h *host) route(ctx *lockstep.Context[hostMsg], key string, msg any) {
	if !h.joined { // rule 1
		if h.hold(ctx, key) {
			h.joining = append(h.joining, heldMsg{key: key, msg: msg})
		}
		return
	}
	g := h.keys[key]
	if g != nil && g.phase != phaseActive { // rule 3
		if h.hold(ctx, key) {
			g.held = append(g.held, msg)
		}
		return
	}
	owner, _ := lookup(h.members, h.kind.name, key) // -1 when no up member hosts the kind
	switch {
	case owner >= 0 && h.members[owner].ID != h.self: // rule 4, until forwarding comes
		h.drop(ctx, key, dropNoOwner)
	case g != nil: // rule 5
		h.deliver(ctx, key, g, msg)
	case owner >= 0: // rule 6: this node owns key
		h.activate(ctx, key, msg)
	default: // rule 7
		h.drop(ctx, key, dropNoOwner)
	}
}

// hold makes room for one more held message (VAC-340): it reports false, after dropping the
// message for key with reason buffer, when the host's buffers are full.
func (h *host) hold(ctx *lockstep.Context[hostMsg], key string) bool {
	if h.buffered >= h.kind.buffer {
		h.drop(ctx, key, dropBuffer)
		return false
	}
	h.buffered++
	h.changed = true
	return true
}

// deliver sends msg to the activation of key (VAC-330); a refused send drops it with reason
// mailbox.
func (h *host) deliver(ctx *lockstep.Context[hostMsg], key string, g *grain, msg any) {
	if err := h.kind.send(ctx, g.pid, msg); err != nil {
		h.drop(ctx, key, dropMailbox)
	}
}

// activate activates key with no handoff peers and delivers msg to it (VAC-410): it spawns the
// activation under the host, named by the escaped key, with the kind's activation options and the
// kind's factory called with key, watches it, records lockstep.grain.activate and delivers. A
// spawn that fails drops msg with reason activate and leaves the key without state.
func (h *host) activate(ctx *lockstep.Context[hostMsg], key string, msg any) {
	addr, pid, err := h.kind.spawn(ctx, escapeKey(key), key, h.kind.activation)
	if err == nil {
		if err = lockstep.Watch(ctx, addr); err != nil {
			// Unreachable for a child spawned in this turn; without the watch the key could not
			// be activated again, so the activation goes.
			if serr := lockstep.Stop(ctx, addr); serr != nil {
				err = serr
			}
		}
	}
	if err != nil {
		h.drop(ctx, key, dropActivate)
		return
	}
	g := &grain{phase: phaseActive, addr: addr, pid: pid}
	h.keys[key] = g
	h.addActive(key)
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"activate", "kind", h.kind.name, "key", escapeKey(key), "handoff", handoffNone)
	}
	h.deliver(ctx, key, g, msg)
}

// terminated handles the Terminated of an activation: the key of the current activation at t.Addr
// loses its state and lockstep.grain.stopped is recorded, so that its next message activates it
// again. A Terminated of an earlier activation is ignored. The activation's name is free by then.
func (h *host) terminated(ctx *lockstep.Context[hostMsg], t lockstep.Terminated) {
	p := t.Addr.Path()
	key, ok := unescapeKey(p[strings.LastIndexByte(p, '/')+1:])
	if !ok {
		return
	}
	g := h.keys[key]
	if g == nil || g.addr != t.Addr {
		return
	}
	delete(h.keys, key)
	h.removeActive(key)
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"stopped", "kind", h.kind.name, "key", escapeKey(key))
	}
}

// addActive inserts key into the sorted active list.
func (h *host) addActive(key string) {
	if i, found := slices.BinarySearch(h.active, key); !found {
		h.active = slices.Insert(h.active, i, key)
		h.changed = true
	}
}

// removeActive removes key from the sorted active list.
func (h *host) removeActive(key string) {
	if i, found := slices.BinarySearch(h.active, key); found {
		h.active = slices.Delete(h.active, i, i+1)
		h.changed = true
	}
}

// drop records the drop of a message for key with reason (VAC-350). A message sent by reference
// has made no hops.
func (h *host) drop(ctx *lockstep.Context[hostMsg], key, reason string) {
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"drop", "kind", h.kind.name, "key", escapeKey(key), "reason", reason, "hops", "0")
	}
}
