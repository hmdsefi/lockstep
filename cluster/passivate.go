// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"slices"
	"strings"

	"github.com/hmdsefi/lockstep"
)

// Passivation and the stops of activations and hosts (VAC-420 to VAC-450, 3.4-04).
//
// An activation is passivated IdleTimeout after its last delivery (VAC-420), when this node no
// longer owns its key in an applied view (VAC-500 step 3) and, from 3.4-06, when another node takes
// the key over (VAC-510). Passivating asks the activation to stop and moves the key to stopping,
// where routing rule 3 holds its messages. The key leaves the host at the activation's Terminated:
// the host records lockstep.grain.stopped, routes the held messages again and then sends the
// pending handoff acks (VAC-430). An activation that stops by itself is handled the same way at
// its Terminated (VAC-440).
//
// What lockstep guarantees, and the host relies on:
//   - The activations are the host's children and the host watches each one. A supervised restart
//     of an activation keeps its cell, address and mailbox and sends no Terminated, so the key stays
//     active (VAC-440). Terminated comes only when the activation stops for good: by Stop, by
//     itself, or by a Stop directive of the host's strategy.
//   - The stopping actor's name is free before its watchers get Terminated (SUP-060 steps 4 and 7),
//     so re-routing the held messages on Terminated can spawn the key's next activation at once.
//   - A host that restarts or stops first ends its timers and watches, then stops every child, and
//     runs Stopped after the last child stopped (SUP-060, SUP-190). No Terminated of its
//     activations reaches the host, the ending incarnation or the next one; Stopped closes the
//     books instead, and the next incarnation is a new host value that starts empty (VAC-450).

// The reasons of lockstep.grain.passivate (§6); 3.4-06 adds handover (VAC-510).
const (
	reasonIdle      = "idle"
	reasonOwnership = "ownership"
	reasonLeave     = "leave"
)

// idleTick is the idle timer of the activation of Key whose generation is Gen (VAC-420). A tick of
// another generation, or of a key that is not active, is stale and does nothing.
type idleTick struct {
	Key string
	Gen uint64
}

// hostCall runs in a turn of the host it is sent to, with the host's context and state. Nothing in
// production sends it; tests use it to set up state that other tasks' messages create, such as
// pending acks.
type hostCall func(ctx *lockstep.Context[hostMsg], h *host)

// pendingAck is a handoff ack the host owes a node once the activation of a key has stopped: To is
// the node whose host gets handoffAck{Seq} (VAC-510).
type pendingAck struct {
	to  string
	seq uint64
}

// idle handles the idle timer t (VAC-420): with e the time since the activation's last delivery,
// the activation is passivated when e reaches the kind's IdleTimeout, else the timer is armed again
// for the rest. A stale tick does nothing.
func (h *host) idle(ctx *lockstep.Context[hostMsg], t idleTick) {
	g := h.keys[t.Key]
	if g == nil || g.gen != t.Gen || g.phase != phaseActive {
		return
	}
	if e := ctx.Now().Sub(g.last); e < h.kind.idle {
		g.timer = ctx.After(h.kind.idle-e, t)
		return
	}
	h.passivate(ctx, t.Key, g, reasonIdle)
}

// passivate asks the activation of the active key to stop and moves key to stopping (VAC-430),
// recording lockstep.grain.passivate with reason, and cancels its idle timer. The key leaves the
// host at the activation's Terminated. Stop fails only when the activation has already stopped by
// itself, and its Terminated is then on its way, since the host watches it: the key moves to
// stopping without a passivate record (VAC-440).
//
// Stop runs before the messages already in the activation's mailbox (system messages come first),
// and the stop drains them as lockstep dead letters (reason stopped), not as grain drops.
func (h *host) passivate(ctx *lockstep.Context[hostMsg], key string, g *grain, reason string) {
	stopped := lockstep.Stop(ctx, g.addr) == nil
	ctx.CancelTimer(g.timer)
	g.phase = phaseStopping
	h.active.remove(key)
	h.stopping.add(key)
	h.changed = true
	if ctl := tracer(ctx); ctl != nil && stopped {
		ctl.Record(grainPrefix+"passivate", "kind", h.kind.name, "key", escapeKey(key), "reason", reason)
	}
}

// passivateNotOwned passivates every active key that this node does not own in the applied view,
// in key order (VAC-500 step 3): with reason leave when this node is not up in it, else ownership.
func (h *host) passivateNotOwned(ctx *lockstep.Context[hostMsg], up bool) {
	reason := reasonOwnership
	if !up {
		reason = reasonLeave
	}
	for _, key := range h.active.snapshot() { // passivate changes the set, never this list
		if owner, _ := lookup(h.members, h.kind.name, key); owner < 0 || h.members[owner].ID != h.self {
			h.passivate(ctx, key, h.keys[key], reason)
		}
	}
}

// terminated handles the Terminated of an activation (VAC-430, VAC-440): the key of the current
// activation at t.Addr, stopping or active (it stopped by itself), leaves the host and
// lockstep.grain.stopped is recorded; then the messages held for the key are routed again in
// arrival order, so that the first one may activate the key again, and then the key's pending acks
// are sent. A Terminated of an earlier activation is ignored.
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
	if g.phase == phaseActive {
		ctx.CancelTimer(g.timer)
		h.active.remove(key)
	} else {
		h.stopping.remove(key)
	}
	h.changed = true
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"stopped", "kind", h.kind.name, "key", escapeKey(key))
	}
	if len(g.held) > 0 {
		h.buffered -= len(g.held)
		h.changed = true
		for _, msg := range g.held {
			h.route(ctx, key, msg)
		}
	}
	for _, a := range g.acks {
		h.sendAck(ctx, key, a)
	}
}

// sendAck sends the handoff ack a for key (VAC-510). Acks are best effort (VAC-540): a send error
// is ignored, and the requester falls back on its HandoffTimeout.
func (h *host) sendAck(ctx *lockstep.Context[hostMsg], key string, a pendingAck) {
	to := lockstep.PIDAt[hostMsg](a.to, grainsPath+"/"+h.kind.name)
	if err := lockstep.Send(ctx, to, hostMsg(handoffAck{Kind: h.kind.name, Key: escapeKey(key), From: h.self, Seq: a.seq})); err != nil {
		return // the requester activates at its HandoffTimeout
	}
}

// Stopped closes the host's books when it stops or restarts (VAC-450). Its activations, its
// children, have stopped before, and their Terminated never comes: the host records
// lockstep.grain.stopped for each of them in key order, then drops every held message with reason
// host: the joining buffer in arrival order, then each key's buffer in key order. Pending acks are
// lost. It publishes a HostInfo without keys or buffered messages; the next incarnation starts
// empty and applies the latest view as its first.
func (h *host) Stopped(ctx *lockstep.Context[hostMsg], _ lockstep.StopReason) {
	for _, x := range h.joining {
		h.drop(ctx, x.key, dropHost)
	}
	keys := slices.Concat(h.active.snapshot(), h.stopping.snapshot())
	slices.Sort(keys)
	ctl := tracer(ctx)
	for _, key := range keys {
		if ctl != nil {
			ctl.Record(grainPrefix+"stopped", "kind", h.kind.name, "key", escapeKey(key))
		}
		for range h.keys[key].held {
			h.drop(ctx, key, dropHost)
		}
	}
	h.joining, h.buffered, h.active, h.stopping = nil, 0, keyList{}, keyList{}
	h.keys = map[string]*grain{}
	h.publish()
}
