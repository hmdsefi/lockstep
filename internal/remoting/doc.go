// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package remoting is the protocol core that production TCP connections and simulated datagrams
// share: the frame format, its payloads and, in later versions, the sessions built on them.
//
// A frame is a 4-byte big-endian length L followed by L bytes: version, kind, flags and sequence
// number, a headers section, the sender, recipient and type fields, and the payload. AppendFrame
// encodes a Frame, ParseFrame decodes and checks one, and SplitFrame cuts frames out of a byte
// stream. The payloads of HELLO and WELCOME, ASK, FAIL and TERMINATED frames have their own
// encoders and parsers. Every rule a received frame breaks is reported as a *ProtocolError.
//
// The package keeps no state, reads no clock and starts no goroutine.
package remoting
