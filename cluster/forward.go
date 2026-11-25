// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"strconv"

	"github.com/hmdsefi/lockstep"
)

// Grain messages between nodes (VAC-200's remote branch, VAC-310 rule 4, VAC-320, VAC-330; 3.4-05).
//
// A message for a grain whose owner is another node travels as a grainEnvelope to the host of the
// kind on the owner's node, at the path /system/grains/<kind> (a PIDAt PID, resolved there).
// Grain.Send sends it with Hops 0; a host that receives it routes it as it routes a local message,
// and when its own view names yet another node as the owner (rule 4), it forwards the envelope
// there with Hops + 1. Every forward raises Hops and a host drops an envelope that already made
// MaxHops hops, so a message never loops, even while views disagree for good.
//
// The message itself stays encoded, as the wire name and bytes Config.Registry and Config.Codec
// made of it, until a host delivers it to an activation: only then is it decoded and checked to
// be a value of the kind's message type (VAC-330). The membership wire types and the envelope go
// through remoting's own registry and codec; Config.Registry is used only for the grain messages
// inside envelopes, by the node that encodes them (a sender, or a host forwarding a message it
// holds by reference) and by the node that delivers them, so both must register the message type
// under the same name.
//
// Order: remoting keeps the order of the messages from one node to another, and a host routes in
// arrival order, so messages from one sender to one grain keep their order while the owner does
// not change and nothing is dropped (VAC-220).

// The drop reasons of forwarding and delivery (VAC-350).
const (
	dropHops      = "hops"
	dropEncode    = "encode"
	dropDecode    = "decode"
	dropWrongType = "wrong-type"
)

func (grainEnvelope) hostMsg() {}

// encoded is a message for a grain that came from another node in a grainEnvelope: its wire name,
// its bytes and the hops it made so far. Hosts hold and route it as it is, and decode it only to
// deliver it (VAC-330). The type is unexported, so no kind's message type is an encoded.
type encoded struct {
	hops int
	name string
	data []byte
}

// hopsOf returns the hops that msg, a message a host routes, made so far: those of an encoded
// message, and 0 for a message sent by reference on this node.
func hopsOf(msg any) int {
	if e, ok := msg.(encoded); ok {
		return e.hops
	}
	return 0
}

// hostAt returns the path PID of the host of kind on node.
func hostAt(node, kind string) lockstep.PID[hostMsg] {
	return lockstep.PIDAt[hostMsg](node, grainsPath+"/"+kind)
}

// envelope routes the message of env, which came from another node (VAC-310). An envelope that no
// sender or host of this version makes is dropped with reason decode: one whose key escapeKey
// cannot have made, whose kind is not the host's or whose Hops is negative. The drop record then
// carries the received key escaped once more, so that it stays ASCII.
func (h *host) envelope(ctx *lockstep.Context[hostMsg], env grainEnvelope) {
	key, ok := unescapeKey(env.Key)
	if !ok || env.Kind != h.kind.name || env.Hops < 0 {
		h.dropEscaped(ctx, escapeKey(env.Key), env.Hops, dropDecode)
		return
	}
	h.route(ctx, key, encoded{hops: env.Hops, name: env.Name, data: env.Data})
}

// forward sends msg for key to the host of the kind on node, the key's owner in the host's view
// (VAC-320), as a grainEnvelope with one hop more than msg made, and records
// lockstep.grain.forward with the hops after the increment. A message that already made MaxHops
// hops is dropped with reason hops instead. A message sent by reference on this node is encoded
// first, and dropped with reason encode when that fails. A send that fails drops the message with
// reason mailbox.
func (h *host) forward(ctx *lockstep.Context[hostMsg], key string, msg any, node string) {
	env := grainEnvelope{Kind: h.kind.name, Key: escapeKey(key)}
	if e, ok := msg.(encoded); ok {
		if e.hops >= h.m.cfg.MaxHops {
			h.drop(ctx, key, e.hops, dropHops)
			return
		}
		env.Hops, env.Name, env.Data = e.hops+1, e.name, e.data
	} else {
		// No hops yet, and MaxHops is at least 1.
		name, data, err := h.m.cfg.Registry.Encode(h.m.cfg.Codec, msg)
		if err != nil {
			h.drop(ctx, key, 0, dropEncode)
			return
		}
		env.Hops, env.Name, env.Data = 1, name, data
	}
	if err := lockstep.Send(ctx, hostAt(node, h.kind.name), hostMsg(env)); err != nil {
		h.drop(ctx, key, env.Hops-1, dropMailbox)
		return
	}
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(grainPrefix+"forward", "kind", h.kind.name, "key", env.Key, "to", node,
			"hops", strconv.Itoa(env.Hops))
	}
}

// open returns the value of msg, a message for key that the host is about to deliver, and the hops
// it made (VAC-330). A message sent by reference on this node is the value itself. An encoded
// message is decoded with Config.Registry and Config.Codec and must be a value of the kind's
// message type: it is dropped with reason decode or wrong-type, and open reports false.
func (h *host) open(ctx *lockstep.Context[hostMsg], key string, msg any) (any, int, bool) {
	e, ok := msg.(encoded)
	if !ok {
		return msg, 0, true
	}
	v, err := h.m.cfg.Registry.Decode(h.m.cfg.Codec, e.name, e.data)
	if err != nil {
		h.drop(ctx, key, e.hops, dropDecode)
		return nil, 0, false
	}
	if !h.kind.accepts(v) {
		h.drop(ctx, key, e.hops, dropWrongType)
		return nil, 0, false
	}
	return v, e.hops, true
}
