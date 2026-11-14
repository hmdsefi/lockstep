// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// hello returns this node's HELLO or WELCOME payload addressed to receiver (REM-065).
func (c *Core) hello(receiver string) []byte {
	return AppendHello(nil, &Hello{
		Sender:      c.node,
		Receiver:    receiver,
		Incarnation: c.inc,
		FrameLimit:  uint32(c.set.MaxFrame), //nolint:gosec // G115: New checked MaxFrame <= MaxFrameLimit
		Codec:       c.codecName,
		Advertise:   c.set.Advertise,
	})
}

// rejectReason returns the REJECT reason for a first frame that ParseFrame refused (REM-070), by
// ProtocolError's documented mapping: a wrong version byte is "version"; a broken length prefix,
// found before the version byte is read, is "payload"; a kind byte that is unknown or not HELLO
// is "kind"; any other broken rule of a HELLO is "payload".
func rejectReason(err error) string {
	var pe *ProtocolError
	switch {
	case !errors.As(err, &pe), pe.Field == FieldLength:
		return RejectPayload
	case pe.Field == FieldVersion:
		return RejectVersion
	case pe.Kind != KindHello:
		return RejectKind
	}
	return RejectPayload
}

// acceptHello answers the first frame of an incoming session (REM-070). It checks the version,
// the kind (HELLO), the payload, then that the sender is not this node ("self"), that the
// receiver is this node ("wrong-node") and that the codecs match ("codec"), and answers REJECT
// with the first reason that fails. An accepted HELLO replaces the peer's incoming session, if it
// has one, with CLOSE replaced (REM-085), and is answered with WELCOME.
func (c *Core) acceptHello(s *session, b []byte) {
	var h Hello
	reason := ""
	f, err := ParseFrame(b, c.set.MaxFrame)
	switch {
	case err != nil:
		reason = rejectReason(err)
	case f.Kind != KindHello:
		reason = RejectKind
	default:
		h, _ = ParseHello(f.Payload) // ParseFrame checked the payload
		switch {
		case h.Sender == c.node:
			reason = RejectSelf
		case h.Receiver != c.node:
			reason = RejectWrongNode
		case h.Codec != c.codecName:
			reason = RejectCodec
		}
	}
	if reason != "" {
		c.write(s, &Frame{Kind: KindReject, Payload: []byte(reason)})
		c.recordReject(s, reason)
		c.down(s, DownRejected)
		return
	}

	p := c.peer(h.Sender)
	if old := p.in; old != nil {
		c.closeSession(old, CloseReplaced, DownReplaced)
	}
	s.peer = p
	p.in = s
	if h.Advertise != "" {
		p.advertise = h.Advertise
	}
	c.write(s, &Frame{Kind: KindWelcome, Payload: c.hello(h.Sender)})
	c.sessionUp(s, &h)
}

// handshakeReply applies the first frame received on an outgoing session: WELCOME brings it up,
// unless it names other nodes than this dial's (REM-075: down wrong-node) or another codec
// (protocol); REJECT takes it down rejected, CLOSE closed. Any other frame is a protocol error.
func (c *Core) handshakeReply(s *session, f *Frame) {
	switch f.Kind {
	case KindWelcome:
		h, _ := ParseHello(f.Payload) // ParseFrame checked the payload
		switch {
		case h.Sender != s.peer.name || h.Receiver != c.node:
			c.warn("remoting: welcome from the wrong node", "node", c.node, "peer", s.peer.name, "conn", s.conn,
				"sender", h.Sender, "receiver", h.Receiver)
			c.down(s, DownWrongNode)
		case h.Codec != c.codecName:
			c.protocolError(s, &ProtocolError{Field: FieldPayload, Detail: "welcome codec " + strconv.Quote(h.Codec) +
				" is not " + strconv.Quote(c.codecName)})
		default:
			s.peer.failures = 0
			c.sessionUp(s, &h)
			// The frames queued while the session was dialing go out after the up record.
			s.peer.want = false
			c.flush(s)
		}
	case KindReject:
		c.recordReject(s, string(f.Payload))
		c.down(s, DownRejected)
	case KindClose:
		c.down(s, DownClosed)
	default:
		c.protocolError(s, &ProtocolError{Field: FieldKind, Detail: f.Kind.String() + " frame before WELCOME"})
	}
}

// sessionUp marks s up after its handshake with h and records it. Peer restarts are detected
// where the known incarnation is replaced (REM-294).
func (c *Core) sessionUp(s *session, h *Hello) {
	if s.dialTimeout != nil {
		s.dialTimeout()
		s.dialTimeout = nil
	}
	s.up = true
	s.peerInc = h.Incarnation
	s.peerLimit = h.FrameLimit
	s.peer.inc = h.Incarnation
	if c.tracer != nil {
		c.tracer.Record(recordUp,
			engine.Attr{Key: "peer", Value: s.peer.name},
			engine.Attr{Key: "conn", Value: s.connText()},
			engine.Attr{Key: "dir", Value: s.dir.String()},
			engine.Attr{Key: "peer-inc", Value: strconv.FormatUint(uint64(h.Incarnation), 10)},
			engine.Attr{Key: "max-frame", Value: strconv.FormatUint(uint64(h.FrameLimit), 10)},
		)
	}
}

// recordReject records a REJECT that s sent or received.
func (c *Core) recordReject(s *session, reason string) {
	if c.tracer != nil {
		c.tracer.Record(recordReject,
			engine.Attr{Key: "conn", Value: s.connText()},
			engine.Attr{Key: "reason", Value: reason},
			engine.Attr{Key: "dir", Value: s.dir.String()},
		)
	}
}
