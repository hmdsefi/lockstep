// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import "github.com/hmdsefi/lockstep/codec"

// memberMsg is the message type of the membership actor. It is sealed: only this package's
// types implement it.
type memberMsg interface{ memberMsg() }

// update is one entry of a node's member table as it travels on the wire. It is also a memberMsg
// only so that it can be registered on its own; the actor ignores a bare update. Nothing modifies
// the Kinds of an update: the table, the views and the messages share one slice.
//
//lockstep:immutable
type update struct {
	Node  string
	ID    uint32
	Inc   uint64
	State MemberState
	Kinds []string // the kinds the member hosts: sorted, unique, valid names; nil for none (VAC-700)
}

// ping probes a node. From is the sender's own entry.
type ping struct {
	From     update
	Seq      uint64
	Target   string
	TargetID uint32
	Updates  []update
}

// pingReq asks a node to probe Target for the sender.
type pingReq struct {
	From     update
	Seq      uint64
	Target   string
	TargetID uint32
	Updates  []update
}

// ack answers a ping, or relays the answer of a pingReq.
type ack struct {
	From    update
	Seq     uint64
	Target  string
	Updates []update
}

// syncReq pushes the sender's entries and asks for the receiver's.
type syncReq struct {
	From    update
	Members []update
}

// syncResp answers a syncReq with every entry of the receiver's table.
type syncResp struct {
	From    update
	Members []update
}

// leave announces that the sender leaves the cluster; From has state Left.
type leave struct {
	From update
}

func (update) memberMsg()   {}
func (ping) memberMsg()     {}
func (pingReq) memberMsg()  {}
func (ack) memberMsg()      {}
func (syncReq) memberMsg()  {}
func (syncResp) memberMsg() {}
func (leave) memberMsg()    {}

// Wire names (CLU-700).
const (
	wireUpdate   = "lockstep.cluster.update"
	wirePing     = "lockstep.cluster.ping"
	wirePingReq  = "lockstep.cluster.pingreq"
	wireAck      = "lockstep.cluster.ack"
	wireSyncReq  = "lockstep.cluster.syncreq"
	wireSyncResp = "lockstep.cluster.syncresp"
	wireLeave    = "lockstep.cluster.leave"
)

func init() { //nolint:gochecknoinits // the wire types must be registered before any message is decoded
	codec.Register[update](wireUpdate)
	codec.Register[ping](wirePing)
	codec.Register[pingReq](wirePingReq)
	codec.Register[ack](wireAck)
	codec.Register[syncReq](wireSyncReq)
	codec.Register[syncResp](wireSyncResp)
	codec.Register[leave](wireLeave)
}

// Reasons validUpdate gives for rejecting an update; they are the reason of the invalid record.
const (
	reasonEmptyNode = "empty-node"
	reasonBadState  = "bad-state"
	reasonBadKinds  = "bad-kinds"
)

// validUpdate checks an update received from a peer (CLU-710). It returns an empty reason when
// the update is valid, otherwise reasonEmptyNode, reasonBadState or reasonBadKinds (Kinds that are
// not sorted, unique, valid kind names, VAC-700).
func validUpdate(u update) (reason string) {
	switch {
	case u.Node == "":
		return reasonEmptyNode
	case u.State < Alive || u.State > Left:
		return reasonBadState
	case !validKindList(u.Kinds):
		return reasonBadKinds
	}
	return ""
}

// toUpdate returns the wire form of the entry m.
func toUpdate(m Member) update {
	return update{Node: m.Node, ID: m.ID, Inc: m.Incarnation, State: m.State, Kinds: m.Kinds}
}

// member returns the entry the update describes; an empty Kinds is nil, as in every Member.
func (u update) member() Member {
	m := Member{Node: u.Node, ID: u.ID, Incarnation: u.Inc, State: u.State}
	if len(u.Kinds) > 0 {
		m.Kinds = u.Kinds
	}
	return m
}
