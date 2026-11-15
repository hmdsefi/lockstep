// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

// NodeDown tells a subscriber of cluster membership that a member node went down.
type NodeDown struct {
	Node string // the member's node name
	ID   uint32 // the member's node ID: the incarnation of its system
	Left bool   // true: it left gracefully; false: it was declared dead or replaced by a new ID
}

func (NodeDown) signal() {}

// NodeUp tells a subscriber of cluster membership that a member node came up.
type NodeUp struct {
	Node string // the member's node name
	ID   uint32 // the member's node ID: the incarnation of its system
}

func (NodeUp) signal() {}
