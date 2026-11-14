// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"fmt"
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

	// dialTimeout is the dial-timeout timer, armed until the handshake completes: until WELCOME
	// on an outgoing session, until the HELLO on an incoming one.
	dialTimeout engine.Cancel

	peerInc   uint32 // from the peer's HELLO or WELCOME
	peerLimit uint32 // the peer's frame limit, from its HELLO or WELCOME

	// blocked holds the frames the link did not take (Write returned false), in order; they are
	// written first when the driver calls Writable. Control frames go here; the send path keeps
	// data frames in the peer's queue, counted against OutboundQueue, and writes them after these.
	blocked [][]byte
	// stalled is set when the link refused a data frame, which stays in the peer's queue, and
	// cleared by Writable.
	stalled bool

	seq           uint64 // outgoing: the sequence number of the last data frame written (REM-130)
	headersWarned bool   // outgoing: the dropped-headers warning was logged (REM-055)
	counted       bool   // the session is counted in Core.up
	recvSeq       uint64 // incoming: the sequence number of the last data frame accepted, 0 before any (REM-200)
}

// held reports whether s's link must not be written to until the driver calls Writable: it
// refused a frame, control or data, that the core still holds.
func (s *session) held() bool { return len(s.blocked) > 0 || s.stalled }

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

	failures int           // consecutive failed dials since an outgoing session last came up (REM-100)
	redial   engine.Cancel // the armed redial timer; nil when none
	// want is set by Connect while no outgoing session is up, and cleared when one comes up.
	want bool
	// queue holds the data frames Deliver accepted and no session has written yet, in order, at
	// most OutboundQueue of them (REM-140), plus the WATCH frames a reconnect puts in front.
	queue []*outFrame

	// watches are the outgoing watches on the peer's actors: the watches of local actors whose
	// RemoteWatch Deliver took, with their watch numbers, until a RemoteUnwatch or a TERMINATED of
	// the same number, the peer's restart or the shutdown ends them (REM-282, REM-284).
	watches map[watchPair]uint64
	// watchers are the watches of the peer's actors on local actors, from its WATCH frames, until
	// UNWATCH, the watched actor's end, the peer's restart or the shutdown ends them (REM-280).
	watchers map[watchPair]*inWatch
	// refusedWarned is set when the core logged that it refused a WATCH of the peer at the bound on
	// its watches; it is cleared once the peer holds fewer watches than the bound, so the warning
	// is logged once each time the peer reaches the bound.
	refusedWarned bool
	// registered are the watches that the WATCH frames written to the peer's current incarnation
	// may have left there: for each pair, the number of the last WATCH written, until an UNWATCH of
	// that number is written or a TERMINATED of it arrives. A session that comes up writes an
	// UNWATCH for every one of them whose pair has no outgoing watch any more, because its UNWATCH
	// became a dead letter or was dropped, so that the peer does not keep the registration.
	registered map[watchPair]uint64
	// lookups are the lookups sent to the peer that have no answer yet, in the order they were
	// sent (REM-290).
	lookups []lookup

	advertise string // the address from the peer's last HELLO, or ""
	inc       uint32 // the peer's last known incarnation, 0 if unknown

	// recvCount is the number of envelopes handed to the host from the peer's incarnation
	// recvInc: the Seq of the last of them (REM-160).
	recvCount uint64
	recvInc   uint32
	// closedIn is the peer's last up incoming session that went down, while Core.closed holds it.
	closedIn *session
	// upOut and upIn are set once a session of that direction has been up to the peer, so that
	// the next one counts as a reconnect.
	upOut, upIn bool
}

// waiting reports whether p has work that needs an outgoing session, so that a redial is armed
// when the session goes down or a dial fails (REM-100: only while frames are queued or outgoing
// watches exist).
func (c *Core) waiting(p *peer) bool {
	return p.want || len(p.queue) > 0 || len(p.watches) > 0
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

// write writes f on s, or keeps it in s.blocked when the link holds frames it did not take or does
// not take this one; s.blocked holds at most one PONG, and a PONG beyond it is dropped. Writing a
// frame the session's direction forbids is a bug in the core.
func (c *Core) write(s *session, f *Frame) {
	if !writable(s.dir, f.Kind) {
		panic("remoting: " + f.Kind.String() + " frame written on an " + s.dir.word() + " session")
	}
	if f.Kind == KindPong && s.holdsPong() {
		// One held PONG answers every PING that arrives while the link is blocked, so a peer
		// that pings without reading cannot grow s.blocked.
		return
	}
	b := AppendFrame(nil, f)
	if s.held() || !s.link.Write(b) {
		s.blocked = append(s.blocked, b)
		return
	}
	s.lastSend = c.clock.Now()
	c.sent(len(b))
}

// holdsPong reports whether s.blocked holds a PONG.
func (s *session) holdsPong() bool {
	for _, b := range s.blocked {
		if Kind(b[prefixLen+1]) == KindPong {
			return true
		}
	}
	return false
}

// flushBlocked writes the frames s's link did not take, in order, until the link refuses one
// again. The send path then writes the peer's queued data frames.
func (c *Core) flushBlocked(s *session) {
	for len(s.blocked) > 0 {
		if !s.link.Write(s.blocked[0]) {
			return
		}
		c.sent(len(s.blocked[0]))
		s.blocked[0] = nil
		s.blocked = s.blocked[1:]
		s.lastSend = c.clock.Now()
	}
	s.blocked = nil
}

// closeSession writes CLOSE with reason on s, then takes it down with downReason (REM-095). The
// CLOSE is best effort: on a link that holds frames it did not take, or that does not take the
// CLOSE, the link is closed without it.
func (c *Core) closeSession(s *session, reason, downReason string) {
	if !s.held() {
		b := AppendFrame(nil, &Frame{Kind: KindClose, Payload: []byte(reason)})
		if s.link.Write(b) {
			s.lastSend = c.clock.Now()
			c.sent(len(b))
		}
	}
	c.down(s, downReason)
}

// protocolError closes s after a received frame broke the protocol.
func (c *Core) protocolError(s *session, err error) {
	c.warn("remoting: protocol error", "node", c.node, "peer", s.peerName(), "conn", s.conn, "dir", s.dir.String(), "err", err)
	c.closeSession(s, CloseProtocol, DownProtocol)
	c.failProtocol(s, err)
}

// failProtocol reports err, a frame received on s that broke the protocol, to the Failer once the
// lock is released, after the frame was handled as in production (REM-340).
func (c *Core) failProtocol(s *session, err error) {
	if c.failer != nil {
		err = fmt.Errorf("lockstep: remote: %s: protocol error on %s conn %d from %s: %w",
			c.node, s.dir.word(), s.conn, quoted(s.peerName()), err)
		c.deferred = append(c.deferred, func() { c.failer.Fail(err) })
	}
}

// down ends s: it closes the link, forgets the session, records the down and, for an outgoing
// session, counts a failure if it never came up and redials while the peer has work waiting. After
// Shutdown it only forgets the session.
func (c *Core) down(s *session, reason string) {
	s.link.Close()
	delete(c.links, s.link)
	if s.counted {
		s.counted = false
		c.up.Add(-1)
	}
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
	case c.shut:
		if p.in == s {
			p.in = nil
		}
		if p.out == s {
			p.out = nil
		}
	case p.in == s:
		p.in = nil
		c.keepClosed(s)
	case p.out == s:
		p.out = nil
		if !s.up {
			p.failures++
		}
		if !c.waiting(p) {
			c.tidy(p)
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

// sessionFrame applies a frame received on an up session (REM-080 to REM-095); in is the frame's
// decoded form, or nil.
func (c *Core) sessionFrame(s *session, f *Frame, in *inbound) {
	switch {
	case f.Kind == KindClose:
		c.down(s, DownClosed)
	case s.dir == dirIn && f.Kind == KindPing:
		c.write(s, &Frame{Kind: KindPong})
	case s.dir == dirOut && f.Kind == KindPong:
	case s.dir == dirIn && f.Kind.Data():
		c.receive(s, f, in)
	default:
		c.protocolError(s, &ProtocolError{Field: FieldKind, Detail: f.Kind.String() + " frame on an up " + s.dir.word() + " session"})
	}
}

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
	c.armDialTimeout(s)
	return nil
}

// armDialTimeout arms s's dial timeout: an outgoing session must get its WELCOME, and an
// incoming one its HELLO, within DialTimeout.
func (c *Core) armDialTimeout(s *session) {
	s.dialTimeout = c.clock.AfterFunc(c.set.DialTimeout, labelDialTimeout, func() { c.dialTimedOut(s) })
}

// dialFailed counts a dial the driver could not start as a failure and redials while p has work
// waiting. When the driver knows no address for p, the frames queued for p are dead letters
// (REM-135).
func (c *Core) dialFailed(p *peer, err error) {
	c.info("remoting: dial failed", "node", c.node, "peer", p.name, "err", err)
	if errors.Is(err, ErrUnknownNode) {
		c.dropQueue(p, DeadUnknownNode)
	}
	p.failures++
	if c.waiting(p) {
		c.armRedial(p)
		return
	}
	c.tidy(p)
}

// dialTimedOut takes a session down whose handshake did not complete within DialTimeout: an
// outgoing session without WELCOME, or an incoming one without HELLO. No frame is written.
func (c *Core) dialTimedOut(s *session) {
	c.mu.Lock()
	defer c.unlock()
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
	defer c.unlock()
	p.redial = nil
	if p.out != nil || c.shut {
		return
	}
	if !c.waiting(p) {
		c.tidy(p)
		return
	}
	if err := c.dial(p); err != nil {
		c.dialFailed(p, err)
	}
}
