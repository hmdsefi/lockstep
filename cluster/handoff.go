// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/tenure/core"
)

// The handover of grain ownership (VAC-310 rule 2, VAC-400, VAC-500, VAC-510 to VAC-540; 3.4-06).
//
// When a key's owner changes, the new owner may find the key still active on the node that owned
// it before. So a host that owns a key with no state does not activate it at once when another
// node may hold it (its handoff peers, VAC-400): it acquires the key instead. It sends every peer
// a handoffReq, holds the key's messages, and activates once every peer acknowledged
// (handoff=acked) or HandoffTimeout passed (handoff=timeout). A peer that gets a handoffReq yields
// the key to the requester: it forwards the key's held messages and every later message for the
// key to the requester, stops its activation if it has one, and acknowledges once the activation
// has stopped. The yield lasts until the peer applies a view that has an entry for the requester:
// from then on its own view routes the key. A peer whose view already has an entry for the
// requester sets no yield, and one whose view shows the requester down only acknowledges: the
// requester is gone, and a yield to it would hold the key away from its owner for good.
//
// Handoff messages are best effort (VAC-540): a lost request or ack ends at the timeout, and a
// stale or duplicate ack, or a stale timer, does nothing. An acquisition is numbered with a
// sequence number unique on the node (handoffSeq); its acks and its timer carry it.
//
// The views (VAC-400, VAC-500 step 1). Handoff peers come from the host's current view C and the
// view P it applied before C. Views that are equal for the kind, with the same joined flag and
// the same hosting members (by node name), count as one: applying such a view replaces C but
// keeps P. Otherwise any later view that changes nothing for the kind (another kind's member, an
// incarnation, a suspicion) would make P equal C, and a key this node took over at its join
// would be activated without asking the node that held it, as long as that node has not seen the
// join yet. P is none after a start or restart of the host.

// The handoff attribute of lockstep.grain.activate after an acquisition (VAC-410, VAC-520).
const (
	handoffAcked   = "acked"
	handoffTimeout = "timeout"
)

// reasonHandover is the reason of lockstep.grain.passivate for an activation stopped by a
// handoffReq (VAC-510).
const reasonHandover = "handover"

// handoffTimer is the HandoffTimeout timer of the acquisition of Key numbered Seq (VAC-520). A
// timer of another acquisition, or of a key that is not acquiring, is stale and does nothing.
type handoffTimer struct {
	Key string
	Seq uint64
}

func (handoffReq) hostMsg()   {}
func (handoffTimer) hostMsg() {}

// yield is the yield of a key to the node to (VAC-530): the host forwards the key's messages
// there (rule 2). When the view in which the host set it had an entry for to of another process
// (an earlier incarnation of the node), stale is that entry's ID: that entry does not end the
// yield, only an entry of another process does.
type yield struct {
	key      string
	to       string
	stale    uint32
	hasStale bool
}

// shift applies the views part of v, the view to apply, with the node's joined flag (VAC-500 step
// 1): the current view becomes the previous one, unless v is equal to it for the kind (see the
// comment at the top of the file), and v becomes the current one.
func (h *host) shift(v *View, joined bool) {
	members := v.place.hosts(h.kind.name)
	if h.applied && (joined != h.joined || !slices.EqualFunc(members, h.members, sameNode)) {
		h.prevApplied, h.prevJoined, h.prevMembers = true, h.joined, h.members
	}
	h.applied, h.version, h.joined, h.members, h.view = true, v.Version, joined, members, v
}

// sameNode reports whether a and b name the same node.
func sameNode(a, b core.Member) bool { return a.ID == b.ID }

// peers returns the handoff peers of key, a key this node owns in the current view C (VAC-400):
// with P the previous view, the runner-up in C when P is none or was not joined; none when this
// node owns key in P; else the owner in P and the runner-up in C. Nodes that do not host the kind
// in C (down, absent or without the kind) and this node are left out; the result is sorted,
// without duplicates.
func (h *host) peers(key string) []string {
	var out []string
	if h.prevApplied && h.prevJoined {
		if o, _ := lookup(h.prevMembers, h.kind.name, key); o >= 0 {
			if h.prevMembers[o].ID == h.self {
				return nil
			}
			out = append(out, h.prevMembers[o].ID)
		}
	}
	if _, r := lookup(h.members, h.kind.name, key); r >= 0 {
		out = append(out, h.members[r].ID)
	}
	out = slices.DeleteFunc(out, func(n string) bool { return n == h.self || !h.hosts(n) })
	slices.Sort(out)
	return slices.Compact(out)
}

// hosts reports whether node hosts the kind in the current view.
func (h *host) hosts(node string) bool {
	_, ok := slices.BinarySearchFunc(h.members, node, func(m core.Member, n string) int { return strings.Compare(m.ID, n) })
	return ok
}

// acquire handles the first message for key, which this node owns and has no state for (VAC-310
// rule 6): msg is the message as routed, and v and hops its value and hops, which open returned.
// Without handoff peers the key is activated at once and gets v. Otherwise the host holds msg in
// the key's buffer and acquires the key: a new sequence number, a handoffReq to the host of every
// peer, in peer order, a lockstep.grain.handoff record and the HandoffTimeout timer. A message
// that does not fit in the buffers is dropped and acquires nothing.
func (h *host) acquire(ctx *lockstep.Context[hostMsg], key string, msg, v any, hops int) {
	peers := h.peers(key)
	if len(peers) == 0 {
		h.activate(ctx, key, v, hops)
		return
	}
	if !h.hold(ctx, key, msg) {
		return
	}
	seq := h.nextSeq()
	g := &grain{phase: phaseAcquiring, held: []any{msg}, seq: seq, pending: peers}
	h.keys[key] = g
	h.acquiring.add(key)
	esc := escapeKey(key)
	for _, p := range peers {
		h.sendReq(ctx, p, handoffReq{Kind: h.kind.name, Key: esc, From: h.self, Seq: seq})
	}
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"handoff", "kind", h.kind.name, "key", esc, "peers", strings.Join(peers, ","))
	}
	g.timer = ctx.After(h.kind.handoff, handoffTimer{Key: key, Seq: seq})
}

// sendReq sends req to the host of the kind on peer. Requests are best effort (VAC-540): a send
// error is ignored, and the acquisition ends at its timeout.
func (h *host) sendReq(ctx *lockstep.Context[hostMsg], peer string, req handoffReq) {
	if err := lockstep.Send(ctx, hostAt(peer, h.kind.name), hostMsg(req)); err != nil {
		return // the acquisition activates at its HandoffTimeout
	}
}

// nextSeq returns the sequence number of a new acquisition: the node ID in the high 32 bits and,
// in the low ones, a counter of the kind's acquisitions on this node that survives restarts of the
// host. So an ack that a previous incarnation of the host, or a previous process of the node, was
// waiting for never matches an acquisition of this one.
func (h *host) nextSeq() uint64 {
	*h.seqs++
	return uint64(h.m.self.ID)<<32 | *h.seqs&math.MaxUint32
}

// handoffReq handles a request from req.From to hand the key over (VAC-510). What it does depends
// on req.From's entry in the applied view, if that entry is of the requesting process: the one
// whose ID is in the high 32 bits of req.Seq (nextSeq). An entry of another process, an earlier
// incarnation of the node, counts as no entry:
//   - no entry: the host records lockstep.grain.yield and yields the key to req.From until a view
//     has an entry for it other than the stale one (VAC-530);
//   - an entry that is down (Dead or Left): the requester is gone, so a yield to it would never
//     end while the entry stays, and it would take the key from this node for good. The host only
//     acks at once, which is harmless, and changes nothing else;
//   - an entry that is up: a yield would end at the next view anyway and would override the
//     routing of this node's view until then, so the host sets none.
//
// Unless the requester is down, the host then forwards the key's held messages to req.From in
// order and, by the key's phase: passivates an active key (reason handover) and acks once it has
// stopped; acks a stopping key once it has stopped; abandons an acquisition and acks; acks at once
// a key with no state. A request that no host of this version sends (a key that is not escaped,
// another kind, no sender or this node as the sender) is ignored.
func (h *host) handoffReq(ctx *lockstep.Context[hostMsg], req handoffReq) {
	key, ok := unescapeKey(req.Key)
	if !ok || req.Kind != h.kind.name || req.From == "" || req.From == h.self {
		return
	}
	ack := pendingAck{to: req.From, seq: req.Seq}
	m, known := h.view.Member(req.From)
	same := known && m.ID == uint32(req.Seq>>32)
	switch {
	case same && !m.State.Up():
		h.sendAck(ctx, key, ack)
		return
	case !same:
		if ctl := tracer(ctx); ctl != nil {
			ctl.Record(grainPrefix+"yield", "kind", h.kind.name, "key", req.Key, "to", req.From)
		}
		h.setYield(yield{key: key, to: req.From, stale: m.ID, hasStale: known})
	}
	g := h.keys[key]
	if g == nil {
		h.sendAck(ctx, key, ack)
		return
	}
	held := g.held
	g.held = nil
	h.unhold(len(held))
	for _, msg := range held {
		h.forward(ctx, key, msg, req.From)
	}
	switch g.phase {
	case phaseActive:
		h.passivate(ctx, key, g, reasonHandover)
		g.acks = append(g.acks, ack)
	case phaseStopping:
		g.acks = append(g.acks, ack)
	case phaseAcquiring:
		h.dropAcquisition(ctx, key, g)
		h.sendAck(ctx, key, ack)
	}
}

// handoffAck handles an ack (VAC-520): one for the current acquisition of the key, from a peer
// the acquisition still waits for, removes the peer and records lockstep.grain.handoff.ack; the
// last one activates the key (handoff=acked). Any other ack is stale or a duplicate and does
// nothing.
func (h *host) handoffAck(ctx *lockstep.Context[hostMsg], ack handoffAck) {
	key, ok := unescapeKey(ack.Key)
	if !ok || ack.Kind != h.kind.name {
		return
	}
	g := h.keys[key]
	if g == nil || g.phase != phaseAcquiring || g.seq != ack.Seq {
		return
	}
	i := slices.Index(g.pending, ack.From)
	if i < 0 {
		return
	}
	g.pending = slices.Delete(g.pending, i, i+1)
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"handoff.ack", "kind", h.kind.name, "key", ack.Key, "from", ack.From,
			"remaining", strconv.Itoa(len(g.pending)))
	}
	if len(g.pending) == 0 {
		h.activateAcquired(ctx, key, g, handoffAcked)
	}
}

// handoffTimeout handles the timer t (VAC-520): at the timer of the current acquisition of its key
// the host records lockstep.grain.handoff.timeout with the peers that did not ack and activates
// the key (handoff=timeout). A stale timer does nothing.
func (h *host) handoffTimeout(ctx *lockstep.Context[hostMsg], t handoffTimer) {
	g := h.keys[t.Key]
	if g == nil || g.phase != phaseAcquiring || g.seq != t.Seq {
		return
	}
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"handoff.timeout", "kind", h.kind.name, "key", escapeKey(t.Key),
			"missing", strings.Join(g.pending, ","))
	}
	h.activateAcquired(ctx, t.Key, g, handoffTimeout)
}

// activateAcquired ends the acquisition g of key and activates the key (VAC-410) with the handoff
// attribute how, then delivers the held messages in order and arms the idle timer. When the
// activation cannot be spawned, every held message is dropped with reason activate and the key is
// left without state.
func (h *host) activateAcquired(ctx *lockstep.Context[hostMsg], key string, g *grain, how string) {
	held := g.held
	h.dropAcquisition(ctx, key, g)
	h.unhold(len(held))
	a, ok := h.start(ctx, key, how)
	if !ok {
		for _, msg := range held {
			h.drop(ctx, key, hopsOf(msg), dropActivate)
		}
		return
	}
	for _, msg := range held {
		if v, hops, ok := h.open(ctx, key, msg); ok {
			h.deliver(ctx, key, a, v, hops)
		}
	}
	a.timer = ctx.After(h.kind.idle, idleTick{Key: key, Gen: a.gen})
}

// dropAcquisition removes the state g of key, which is acquiring, and cancels its timer, leaving
// its held messages to the caller. Its acks become stale.
func (h *host) dropAcquisition(ctx *lockstep.Context[hostMsg], key string, g *grain) {
	ctx.CancelTimer(g.timer)
	delete(h.keys, key)
	h.acquiring.remove(key)
	h.changed = true
}

// abandonNotOwned abandons every acquisition of a key this node does not own in the current view,
// in key order, and routes its held messages again, in order (VAC-500 step 4).
func (h *host) abandonNotOwned(ctx *lockstep.Context[hostMsg]) {
	for _, key := range h.acquiring.snapshot() { // dropAcquisition changes the set, never this list
		if owner, _ := lookup(h.members, h.kind.name, key); owner >= 0 && h.members[owner].ID == h.self {
			continue
		}
		g := h.keys[key]
		held := g.held
		h.dropAcquisition(ctx, key, g)
		h.unhold(len(held))
		for _, msg := range held {
			h.route(ctx, key, msg)
		}
	}
}

// unhold takes n messages out of the count of held messages (VAC-340).
func (h *host) unhold(n int) {
	if n > 0 {
		h.buffered -= n
		h.changed = true
	}
}

// yieldOf returns the node key is yielded to (VAC-530), and whether it is yielded.
func (h *host) yieldOf(key string) (string, bool) {
	i, ok := slices.BinarySearchFunc(h.yields, key, compareYield)
	if !ok {
		return "", false
	}
	return h.yields[i].to, true
}

// setYield sets the yield y, replacing an earlier yield of its key.
func (h *host) setYield(y yield) {
	i, ok := slices.BinarySearchFunc(h.yields, y.key, compareYield)
	if ok {
		h.yields[i] = y
		return
	}
	h.yields = slices.Insert(h.yields, i, y)
}

// clearYields ends the yields to the nodes that have an entry, in any state, in v, other than the
// stale entry a yield was set against (VAC-500 step 2, VAC-530).
func (h *host) clearYields(v *View) {
	h.yields = slices.DeleteFunc(h.yields, func(y yield) bool {
		m, ok := v.Member(y.to)
		return ok && (!y.hasStale || m.ID != y.stale)
	})
}

// compareYield orders a yield by its key against key, for binary searches.
func compareYield(y yield, key string) int { return strings.Compare(y.key, key) }
