// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import "github.com/hmdsefi/lockstep/codec"

// init registers the messages that cross nodes, so a simulation or a remoting transport can
// encode them. Self-messages and effect values never leave their node and stay unregistered.
//
//nolint:gochecknoinits // wire types register in init so any binary that imports raft has them
func init() {
	codec.Register[RequestVote]("raft.RequestVote")
	codec.Register[VoteReply]("raft.VoteReply")
	codec.Register[AppendEntries]("raft.AppendEntries")
	codec.Register[AppendReply]("raft.AppendReply")
	codec.Register[Propose]("raft.Propose")
	codec.Register[Result]("raft.Result")
}
