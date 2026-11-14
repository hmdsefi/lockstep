// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// Record kinds of the receive path (§8).
const (
	recordGap     = "lockstep.remote.gap"
	recordDiscard = "lockstep.remote.discard"
)

// The reasons a received frame is discarded, as the discard record names them.
const (
	DiscardLate          = "late"           // its sequence number is below the next one expected on its session
	DiscardMisrouted     = "misrouted"      // its recipient is not on this node, or an ASK's ask address is
	DiscardDecode        = "decode"         // its sender, ask address or codec bytes do not decode
	DiscardWrongType     = "wrong-type"     // its value is not of a type its recipient takes
	DiscardClosedSession = "closed-session" // it arrived on a link after the core closed the session
)

// DeadWrongType is the dead-letter reason of a message or request whose value is not of a type
// its live recipient takes.
const DeadWrongType = "wrong-type"

// codeOther is the failure code byte of a failure that matches no lockstep error (remote.CodeOther).
const codeOther uint8 = 0

// inbound is what the receive path reads from a MSG, ASK, REPLY or FAIL frame before it takes the
// lock. Decoding reads only settings that do not change, so a slow codec does not hold the lock.
type inbound struct {
	from, to engine.Address
	toOK     bool           // the recipient is an address
	ask      engine.Address // ASK: the ask address
	value    any            // MSG, ASK, REPLY: the decoded value
	failure  Failure        // FAIL: the failure
	err      error          // the first of the sender, the ask address and the codec bytes that does not decode
}

// decodeFrame decodes f's addresses and value (REM-230, COD-070). It returns nil for a frame of
// another kind: the control frames, and the watch and lookup frames, which the receive path does
// not decode.
func (c *Core) decodeFrame(f *Frame) *inbound {
	switch f.Kind {
	case KindMsg, KindAsk, KindReply, KindFail:
	default:
		return nil
	}
	in := &inbound{}
	var err error
	in.to, err = parseAddress(f.Recipient)
	in.toOK = err == nil
	if in.from, err = parseAddress(f.Sender); err != nil {
		in.err = fmt.Errorf("sender: %w", err)
	}
	body := f.Payload
	switch f.Kind {
	case KindAsk:
		var text string
		text, body, _ = ParseAsk(f.Payload) // ParseFrame checked the layout
		switch in.ask, err = parseAddress(text); {
		case in.err != nil:
		case err != nil:
			in.err = fmt.Errorf("ask address: %w", err)
		case text == "":
			in.err = errors.New("ask address is empty")
		}
	case KindFail:
		in.failure, _ = ParseFailure(f.Payload) // ParseFrame checked the layout
		return in
	}
	switch {
	case in.err != nil:
	case f.Kind == KindReply && f.Type == "" && len(body) == 0:
		// A nil answer, for an R of interface type.
	default:
		in.value, in.err = c.set.Registry.Decode(c.set.Codec, f.Type, body)
	}
	return in
}

// parseAddress parses an address in the text form of Address.MarshalText; empty text is the zero
// address.
func parseAddress(text string) (engine.Address, error) {
	var a lockstep.Address
	if err := a.UnmarshalText([]byte(text)); err != nil {
		return engine.Address{}, err
	}
	return engine.Address{Node: a.Node(), Path: a.Path(), UID: a.UID()}, nil
}

// receive applies a data frame received on s, an up incoming session, in its decoded form in
// (nil for the watch and lookup kinds). It applies the sequence rule (REM-200): a frame numbered
// below the next one expected is discarded late; one above it records the gap and is accepted.
// An accepted frame whose recipient, or ASK's ask address, is not right for this node is
// discarded misrouted (REM-210, REM-220), one that does not decode is discarded decode (REM-230),
// and the rest are checked against their recipients and handed to the host (REM-160, REM-260 to
// REM-268). The host, the dead-letter function and the Failer run after the lock is released, in
// frame order.
func (c *Core) receive(s *session, f *Frame, in *inbound) {
	want := s.recvSeq + 1
	switch {
	case f.Seq < want:
		c.discard(s, f, DiscardLate)
		return
	case f.Seq > want:
		c.recordGap(s, want, f.Seq)
	}
	s.recvSeq = f.Seq
	if in == nil {
		// WATCH, UNWATCH, TERMINATED and LOOKUP are applied by the watch and lookup path of the
		// core; until it exists they are dropped here, once the sequence rule has counted them.
		return
	}
	switch {
	case !in.toOK || in.to.Node != c.node, f.Kind == KindAsk && in.ask.Node == c.node:
		// A uid-0 recipient is a path address: it passes as it is, and the runtime resolves it.
		// Only the asker's node allocates ask addresses, so a request whose answer would come back
		// here was not sent by the peer's runtime.
		c.discard(s, f, DiscardMisrouted)
		return
	case in.err != nil:
		c.discard(s, f, DiscardDecode)
		c.fail(s, f, c.recvError(s, f, in.err))
		return
	}
	switch f.Kind {
	case KindMsg, KindAsk:
		c.receiveMessage(s, f, in)
	case KindReply:
		c.receiveReply(s, f, in)
	case KindFail:
		err := c.errs.Remote(s.peer.name, in.failure.Code, in.failure.Message)
		c.inbound(s, f, in, hooks.RemotePayload{Kind: hooks.RemoteFail, Err: err})
	}
}

// receiveMessage hands a MSG or ASK to the host, unless its recipient is a live actor whose
// message type does not take the value: then it is discarded wrong-type, becomes a dead letter
// wrong-type and fails the seed (REM-230, REM-240). A recipient that is not a live actor gets the
// frame all the same, so that the runtime makes it a dead letter and fails an ask request back
// with ErrTerminated (MSG-350).
func (c *Core) receiveMessage(s *session, f *Frame, in *inbound) {
	if !hooks.Assignable(c.sys, in.to, in.value) && c.live(in.to) {
		c.discard(s, f, DiscardWrongType)
		from, to, v := in.from, in.to, in.value
		c.deferred = append(c.deferred, func() { c.dead(from, to, DeadWrongType, v) })
		c.fail(s, f, c.recvError(s, f, fmt.Errorf("%w: the actor at %s does not take it", lockstep.ErrWrongType, to)))
		return
	}
	p := hooks.RemotePayload{Kind: hooks.RemoteMsg, Value: in.value}
	if f.Kind == KindAsk {
		p.Kind, p.Ask = hooks.RemoteAsk, in.ask
	}
	c.inbound(s, f, in, p)
}

// live reports whether to names a live actor of this node: one registered at its path, with its
// uid unless the uid is 0.
func (c *Core) live(to engine.Address) bool {
	a, err := hooks.LookupPath(c.sys, to.Path, "")
	return err == nil && (to.UID == 0 || a.UID == to.UID)
}

// receiveReply hands a REPLY to the host. For a pending ask whose R does not take the answer the
// host gets a failure wrapping lockstep.ErrWrongType instead, the frame is discarded wrong-type
// and the seed fails (REM-264, REM-268). An answer for an ask that is not pending is not checked:
// it stays a late reply.
func (c *Core) receiveReply(s *session, f *Frame, in *inbound) {
	if hooks.Pending(c.sys, in.to) && !hooks.Assignable(c.sys, in.to, in.value) {
		c.discard(s, f, DiscardWrongType)
		err := c.recvError(s, f, fmt.Errorf("%w: the ask %s does not take it", lockstep.ErrWrongType, in.to))
		c.inbound(s, f, in, hooks.RemotePayload{Kind: hooks.RemoteFail, Err: err})
		c.fail(s, f, err)
		return
	}
	c.inbound(s, f, in, hooks.RemotePayload{Kind: hooks.RemoteReply, Value: in.value})
}

// inbound hands p to the host, from the frame's sender to its recipient, once the lock is
// released. Its Seq is the count of envelopes handed to the host from the peer's incarnation
// that s belongs to, from 1 (REM-160).
func (c *Core) inbound(s *session, f *Frame, in *inbound, p hooks.RemotePayload) {
	pr := s.peer
	if pr.recvInc != s.peerInc {
		pr.recvInc, pr.recvCount = s.peerInc, 0
	}
	pr.recvCount++
	env := engine.Envelope{From: in.from, To: in.to, Seq: pr.recvCount, Headers: f.Headers, Payload: hooks.NewPayload(p)}
	c.deferred = append(c.deferred, func() { c.host.Inbound(env) })
}

// recvError returns the error of a frame that s received and the core discarded or failed:
// "lockstep: remote: <node>: <kind> from <peer>: <wire name>: <cause>", without the wire name
// when the frame has none.
func (c *Core) recvError(s *session, f *Frame, cause error) error {
	typ := ""
	if f.Type != "" {
		typ = f.Type + ": "
	}
	return fmt.Errorf("lockstep: remote: %s: %s from %s: %s%w", c.node, f.Kind, s.peer.name, typ, cause)
}

// fail logs err, a frame that s received and the core could not apply as it was, and reports it
// to the Failer, both once the lock is released (REM-340).
func (c *Core) fail(s *session, f *Frame, err error) {
	c.warn("remoting: frame not applied", "node", c.node, "peer", s.peer.name, "conn", s.conn,
		"kind", f.Kind.String(), "seq", f.Seq, "err", err)
	if c.failer != nil {
		c.deferred = append(c.deferred, func() { c.failer.Fail(err) })
	}
}

// replyFailed returns the FAIL frame that goes in place of the REPLY of env, whose answer v did
// not encode or whose frame exceeded MaxFrame with err: code other and err's text. It reports err
// to the Failer (REM-266). It runs without the lock, as encode does.
func (c *Core) replyFailed(env engine.Envelope, v any, err error) *outFrame {
	name, nerr := c.set.Registry.NameOf(v)
	if nerr != nil {
		name = fmt.Sprintf("%T", v)
	}
	if c.failer != nil {
		c.failer.Fail(fmt.Errorf("lockstep: remote: %s: REPLY to %s: %s: %w", c.node, env.To.Node, name, err))
	}
	f := Frame{
		Kind:      KindFail,
		Sender:    addrText(env.From),
		Recipient: addrText(env.To),
		Payload:   AppendFailure(nil, &Failure{Code: codeOther, Message: err.Error()}),
	}
	return &outFrame{b: AppendFrame(nil, &f), kind: KindFail, from: env.From, to: env.To, msg: err}
}

// discard records that s discarded f with reason.
func (c *Core) discard(s *session, f *Frame, reason string) {
	if c.tracer != nil {
		c.tracer.Record(recordDiscard,
			engine.Attr{Key: "peer", Value: s.peerName()},
			engine.Attr{Key: "conn", Value: s.connText()},
			engine.Attr{Key: "kind", Value: f.Kind.String()},
			engine.Attr{Key: "seq", Value: strconv.FormatUint(f.Seq, 10)},
			engine.Attr{Key: "reason", Value: reason},
		)
	}
}

// recordGap records that s expected the frame numbered want and received the one numbered got.
func (c *Core) recordGap(s *session, want, got uint64) {
	if c.tracer != nil {
		c.tracer.Record(recordGap,
			engine.Attr{Key: "peer", Value: s.peerName()},
			engine.Attr{Key: "conn", Value: s.connText()},
			engine.Attr{Key: "want", Value: strconv.FormatUint(want, 10)},
			engine.Attr{Key: "got", Value: strconv.FormatUint(got, 10)},
		)
	}
}

// keepClosed remembers s, an up incoming session that went down, so that frames the driver still
// hands over for its link are discarded closed-session (REM-250). The core remembers one such
// session per peer, the last, and forgets it when the driver reports its link closed.
func (c *Core) keepClosed(s *session) {
	p := s.peer
	if old := p.closedIn; old != nil {
		delete(c.closed, old.link)
	}
	p.closedIn = s
	c.closed[s.link] = s
}

// forgetClosed forgets the closed session on link, if the core remembers one.
func (c *Core) forgetClosed(link Link) {
	s := c.closed[link]
	if s == nil {
		return
	}
	delete(c.closed, link)
	if s.peer.closedIn == s {
		s.peer.closedIn = nil
	}
}
