// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package remote lets actors on different nodes exchange messages.
//
// Remoting connects a System to its peers over TCP, optionally with TLS. Once it is enabled, an
// address whose node differs from the local node is delivered through the connection to that node:
// the sender writes the message into a bounded per-peer queue, the message is encoded with the
// configured codec, and the receiving node decodes it and delivers it to the local actor. Message
// types that cross nodes are registered in a codec.Registry on both sides. Failures that happen on
// the other node come back as an *Error whose Code unwraps to the matching lockstep sentinel.
//
// A node is configured with a Config. DefaultConfig returns every default filled in, and
// Config.Validate fills in the defaults of a partly set Config and checks it. Peers are named by
// node name and dialed lazily; a node that is not in Config.Peers is looked up with Config.Resolve.
// Connections are kept alive with heartbeats and redialed with a backoff between RedialMin and
// RedialMax. Frames larger than MaxFrame are refused with ErrFrameTooLarge.
//
// Remoting is switched on for a System in a later version of this package. This version declares
// the configuration, the sentinel errors and the failure codes that the rest of the package builds
// on.
package remote
