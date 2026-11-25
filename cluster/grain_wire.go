// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import "github.com/hmdsefi/lockstep/codec"

// The grain wire types (VAC-700): what a node sends to the grain host of another node. They are
// unexported structs with exported fields (for codec.JSON), registered in init with
// codec.Register, like the membership wire types. Every Key is the escaped key (escapeKey), so
// that traces and JSON stay ASCII.

// grainEnvelope carries one message for a grain to the host of the node that owns it. Name and
// Data are the message as the codec registry encoded it; Hops counts the nodes that forwarded the
// envelope so far.
type grainEnvelope struct {
	Kind string
	Key  string
	Hops int
	Name string
	Data []byte
}

// handoffReq asks a node to stop its activation of the grain and acknowledge: the sender takes the
// grain over. From is the sender's node name and Seq numbers the acquisition.
type handoffReq struct {
	Kind string
	Key  string
	From string
	Seq  uint64
}

// handoffAck answers a handoffReq once the answering node holds no activation of the grain. From
// is the answering node and Seq is the Seq of the request.
type handoffAck struct {
	Kind string
	Key  string
	From string
	Seq  uint64
}

// Wire names (VAC-700); they use the prefix that membership's wire names use.
const (
	wireGrain      = "lockstep.cluster.grain"
	wireHandoff    = "lockstep.cluster.handoff"
	wireHandoffAck = "lockstep.cluster.handoffack"
)

func init() { //nolint:gochecknoinits // the wire types must be registered before any message is decoded
	codec.Register[grainEnvelope](wireGrain)
	codec.Register[handoffReq](wireHandoff)
	codec.Register[handoffAck](wireHandoffAck)
}
