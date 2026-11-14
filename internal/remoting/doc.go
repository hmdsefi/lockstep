// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package remoting is the protocol core that production TCP connections and simulated datagrams
// share: the frame format, its payloads and the sessions built on them.
//
// A frame is a 4-byte big-endian length L followed by L bytes: version, kind, flags and sequence
// number, a headers section, the sender, recipient and type fields, and the payload. AppendFrame
// encodes a Frame, ParseFrame decodes and checks one, and SplitFrame cuts frames out of a byte
// stream. The payloads of HELLO and WELCOME, ASK, FAIL and TERMINATED frames have their own
// encoders and parsers. Every rule a received frame breaks is reported as a *ProtocolError.
//
// Core runs the sessions of one node over the links of a Driver: it dials peers on demand,
// handshakes with HELLO and WELCOME (or REJECT), keeps one outgoing session per peer for this
// node's data frames, accepts incoming sessions for the peers' frames, pings and closes idle
// sessions on a heartbeat tick, and redials with seeded, bounded backoff. The core guards its
// state with a mutex, reads time only from the engine's clock and randomness only from the
// engine's entropy, and starts no goroutine; drivers call it in engine context.
package remoting
