// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"strconv"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// dir is a session's direction: outgoing sessions are dialed by this node and carry its data
// frames; incoming sessions are dialed by the peer and carry the peer's (REM-080).
type dir uint8

const (
	dirOut dir = iota + 1
	dirIn
)

func (d dir) String() string {
	if d == dirOut {
		return "out"
	}
	return "in"
}

// word returns "outgoing" or "incoming".
func (d dir) word() string {
	if d == dirOut {
		return "outgoing"
	}
	return "incoming"
}

// session is one link and its protocol state. A session lives in Core.links from its dial or
// accept until it goes down; then it is removed and its link closed.
type session struct {
	serial uint64 // creation order, for ticks
	link   Link
	conn   uint64 // link.ID()
	dir    dir
	// peer is the session's peer: set at the dial for an outgoing session, and when its HELLO is
	// accepted for an incoming one. Nil for an incoming session still waiting for its HELLO.
	peer *peer
	up   bool // the handshake completed

	lastRecv time.Time // the last frame received, or the session's start
	lastSend time.Time // the last frame written, or the session's start

	dialTimeout engine.Cancel // outgoing, until WELCOME: the dial-timeout timer

	peerInc   uint32 // from the peer's HELLO or WELCOME
	peerLimit uint32 // the peer's frame limit, from its HELLO or WELCOME

	// The send path adds the sequence number of the last data frame written on an outgoing
	// session here, and the receive path the next sequence number expected on an incoming one.
}

func (s *session) connText() string { return strconv.FormatUint(s.conn, 10) }

// peerName returns the session's peer name, or "" when it has none yet.
func (s *session) peerName() string {
	if s.peer == nil {
		return ""
	}
	return s.peer.name
}

// peer is what the core knows about one other node.
type peer struct {
	name string
	out  *session // the one outgoing session, dialing or up; nil when none
	in   *session // the up incoming session; nil when none

	failures int           // consecutive failed dials since a session last came up (REM-100)
	redial   engine.Cancel // the armed redial timer; nil when none
	// want is set by Connect and cleared when an outgoing session comes up. The send path adds
	// its queue of unwritten frames to waiting, and watches add the outgoing watches.
	want bool

	advertise string // the address from the peer's last HELLO, or ""
	inc       uint32 // the peer's last known incarnation, 0 if unknown
}

// waiting reports whether p has work that needs an outgoing session, so that a redial is armed
// when the session goes down or a dial fails (REM-100: only while frames are queued or outgoing
// watches exist).
func (c *Core) waiting(p *peer) bool {
	return p.want
}

// writable reports whether a session of direction d may write a frame of kind k (REM-080,
// REM-085): data frames, HELLO and PING go only on outgoing sessions; WELCOME, REJECT and PONG
// only on incoming ones; CLOSE on either.
func writable(d dir, k Kind) bool {
	switch k {
	case KindClose:
		return true
	case KindHello, KindPing:
		return d == dirOut
	case KindWelcome, KindReject, KindPong:
		return d == dirIn
	}
	return d == dirOut && k.Data()
}

// write writes f on s. Writing a frame the session's direction forbids is a bug in the core.
func (c *Core) write(s *session, f *Frame) {
	if !writable(s.dir, f.Kind) {
		panic("remoting: " + f.Kind.String() + " frame written on an " + s.dir.word() + " session")
	}
	s.link.Write(AppendFrame(nil, f))
	s.lastSend = c.clock.Now()
}

// closeSession writes CLOSE with reason on s, then takes it down with downReason (REM-095).
func (c *Core) closeSession(s *session, reason, downReason string) {
	c.write(s, &Frame{Kind: KindClose, Payload: []byte(reason)})
	c.down(s, downReason)
}

// protocolError closes s after a received frame broke the protocol.
func (c *Core) protocolError(s *session, err error) {
	c.log.Warn("remoting: protocol error", "node", c.node, "peer", s.peerName(), "conn", s.conn, "dir", s.dir.String(), "err", err)
	c.closeSession(s, CloseProtocol, DownProtocol)
}

// down ends s: it closes the link, forgets the session, records the down and, for an outgoing
// session, counts a failure if it never came up and redials while the peer has work waiting.
func (c *Core) down(s *session, reason string) {
	s.link.Close()
	delete(c.links, s.link)
	if s.dialTimeout != nil {
		s.dialTimeout()
		s.dialTimeout = nil
	}
	if c.tracer != nil {
		c.tracer.Record(recordDown,
			engine.Attr{Key: "peer", Value: s.peerName()},
			engine.Attr{Key: "conn", Value: s.connText()},
			engine.Attr{Key: "dir", Value: s.dir.String()},
			engine.Attr{Key: "reason", Value: reason},
		)
	}
	p := s.peer
	switch {
	case p == nil:
	case p.in == s:
		p.in = nil
	case p.out == s:
		p.out = nil
		if !s.up {
			p.failures++
		}
		if !c.waiting(p) {
			return
		}
		if p.failures > 0 {
			c.armRedial(p)
			return
		}
		// The session was up: dial again at once.
		if err := c.dial(p); err != nil {
			c.dialFailed(p, err)
		}
	}
}

// sessionFrame applies a frame received on an up session (REM-080 to REM-095).
func (c *Core) sessionFrame(s *session, f *Frame) {
	switch {
	case f.Kind == KindClose:
		c.down(s, DownClosed)
	case s.dir == dirIn && f.Kind == KindPing:
		c.write(s, &Frame{Kind: KindPong})
	case s.dir == dirOut && f.Kind == KindPong:
	case s.dir == dirIn && f.Kind.Data():
		c.receive(s, f)
	default:
		c.protocolError(s, &ProtocolError{Field: FieldKind, Detail: f.Kind.String() + " frame on an up " + s.dir.word() + " session"})
	}
}

// receive applies a data frame received on an up incoming session. The receive path implements
// it: sequencing, decoding and delivery to the host. Until then data frames are dropped.
func (c *Core) receive(*session, *Frame) {}

// dial starts an outgoing session to p: it asks the driver for a link, records the dial, writes
// HELLO and arms the dial timeout. It returns the driver's error, and then changes nothing.
func (c *Core) dial(p *peer) error {
	link, err := c.driver.Dial(p.name, p.advertise)
	if err != nil {
		return err
	}
	s := c.newSession(link, dirOut, p)
	p.out = s
	if c.tracer != nil {
		c.tracer.Record(recordDial,
			engine.Attr{Key: "peer", Value: p.name},
			engine.Attr{Key: "conn", Value: s.connText()},
			engine.Attr{Key: "attempt", Value: strconv.Itoa(p.failures + 1)},
		)
	}
	c.write(s, &Frame{Kind: KindHello, Payload: c.hello(p.name)})
	s.dialTimeout = c.clock.AfterFunc(c.set.DialTimeout, labelDialTimeout, func() { c.dialTimedOut(s) })
	return nil
}

// dialFailed counts a dial the driver could not start as a failure and redials while p has work
// waiting.
func (c *Core) dialFailed(p *peer, err error) {
	c.log.Info("remoting: dial failed", "node", c.node, "peer", p.name, "err", err)
	p.failures++
	if c.waiting(p) {
		c.armRedial(p)
	}
}

// dialTimedOut takes an outgoing session down that got no WELCOME within DialTimeout.
func (c *Core) dialTimedOut(s *session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.links[s.link] != s || s.up {
		return
	}
	s.dialTimeout = nil
	c.down(s, DownDialTimeout)
}

// armRedial arms p's redial after p.failures consecutive failures, with REM-100's delay drawn from
// the core's stream, and records it.
func (c *Core) armRedial(p *peer) {
	d := redialDelay(p.failures, c.set.RedialMin, c.set.RedialMax, c.rand)
	if c.tracer != nil {
		c.tracer.Record(recordRedial,
			engine.Attr{Key: "peer", Value: p.name},
			engine.Attr{Key: "failures", Value: strconv.Itoa(p.failures)},
			engine.Attr{Key: "delay", Value: d.String()},
		)
	}
	p.redial = c.clock.AfterFunc(d, labelRedial, func() { c.redialFired(p) })
}

// redialFired dials p again if it still has work waiting.
func (c *Core) redialFired(p *peer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p.redial = nil
	if p.out != nil || !c.waiting(p) {
		return
	}
	if err := c.dial(p); err != nil {
		c.dialFailed(p, err)
	}
}
