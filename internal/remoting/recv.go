// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
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

// inbound is what the receive path reads from a data frame before it takes the lock. Decoding
// reads only settings that do not change, so a slow codec does not hold the lock.
type inbound struct {
	from, to engine.Address
	toOK     bool           // the recipient is an address
	ask      engine.Address // ASK: the ask address
	value    any            // MSG, ASK, REPLY: the decoded value
	failure  Failure        // FAIL: the failure
	watch    uint64         // WATCH, UNWATCH: the watch number
	term     Terminated     // TERMINATED: the payload
	err      error          // the first of the sender, the ask address and the codec bytes that does not decode
}

// decodeFrame decodes the addresses and the payload of f, a data frame (REM-230, COD-070). It
// returns nil for a control frame.
func (c *Core) decodeFrame(f *Frame) *inbound {
	if !f.Kind.Data() {
		return nil
	}
	in := &inbound{}
	var ok bool
	in.to, in.toOK = parseAddress(f.Recipient)
	if in.from, ok = parseAddress(f.Sender); !ok {
		in.err = fmt.Errorf("sender %s is not an address", quoted(f.Sender))
	}
	body := f.Payload
	switch f.Kind {
	case KindAsk:
		var text string
		text, body, _ = ParseAsk(f.Payload) // ParseFrame checked the layout
		switch in.ask, ok = parseAddress(text); {
		case in.err != nil:
		case !ok:
			in.err = fmt.Errorf("ask address %s is not an address", quoted(text))
		case text == "":
			in.err = errors.New("ask address is empty")
		}
	case KindFail:
		in.failure, _ = ParseFailure(f.Payload) // ParseFrame checked the layout
		return in
	case KindWatch, KindUnwatch:
		in.watch, _ = ParseWatch(f.Payload) // ParseFrame checked the layout
		return in
	case KindTerminated:
		in.term, _ = ParseTerminated(f.Payload) // ParseFrame checked the layout
		return in
	case KindLookup:
		return in
	}
	switch {
	case in.err != nil:
	case f.Kind == KindReply && f.Type == "" && len(body) == 0:
		// A nil answer, for an R of interface type.
	default:
		// An unknown wire name is reported here, quoted and cut, rather than by Decode, whose
		// error would quote all of it. The decode errors of known names name the local type.
		if _, err := c.set.Registry.TypeOf(f.Type); err != nil {
			in.err = fmt.Errorf("%w: wire name %s", codec.ErrNotRegistered, quoted(f.Type))
			break
		}
		in.value, in.err = c.set.Registry.Decode(c.set.Codec, f.Type, body)
	}
	return in
}

// parseAddress parses an address in the text form of Address.MarshalText, and reports false for
// text in another form; empty text is the zero address.
func parseAddress(text string) (engine.Address, bool) {
	var a lockstep.Address
	if err := a.UnmarshalText([]byte(text)); err != nil {
		return engine.Address{}, false
	}
	return engine.Address{Node: a.Node(), Path: a.Path(), UID: a.UID()}, true
}

// maxQuoted is the most bytes of text from a peer, a wire name or an address, that the core's
// logs and errors quote.
const maxQuoted = 256

// quoted returns s quoted with strconv.Quote, cut to maxQuoted bytes at a rune boundary and
// followed by "..." when it is longer.
func quoted(s string) string {
	if len(s) <= maxQuoted {
		return strconv.Quote(s)
	}
	return strconv.Quote(cutUTF8(s, maxQuoted)) + "..."
}

// cutUTF8 returns the longest prefix of s of at most n bytes that does not end inside a UTF-8
// sequence.
func cutUTF8(s string, n int) string {
	if n >= len(s) {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// receive applies a data frame received on s, an up incoming session, in its decoded form in. It
// applies the sequence rule (REM-200): a frame numbered below the next one expected is discarded
// late; one above it records the gap and is accepted. An accepted frame whose recipient, or ASK's
// ask address, is not right for this node is discarded misrouted (REM-210, REM-220), one that does
// not decode is discarded decode (REM-230), and the rest are checked against their recipients and
// handed to the host (REM-160, REM-260 to REM-268). WATCH, UNWATCH, TERMINATED and LOOKUP go to
// the watch and lookup path (REM-280 to REM-290). The host, the hooks, the dead-letter function
// and the Failer run after the lock is released, in frame order. A frame numbered math.MaxUint64
// is the last one a session takes: any data frame after it closes the session as a protocol error.
func (c *Core) receive(s *session, f *Frame, in *inbound) {
	if s.recvSeq == math.MaxUint64 {
		c.protocolError(s, protoErr(FieldSeq, "%s frame after sequence number %d, the last", f.Kind, s.recvSeq))
		return
	}
	want := s.recvSeq + 1
	switch {
	case f.Seq < want:
		c.discard(s, f, DiscardLate)
		return
	case f.Seq > want:
		c.recordGap(s, want, f.Seq)
	}
	s.recvSeq = f.Seq
	switch f.Kind {
	case KindWatch, KindUnwatch, KindTerminated, KindLookup:
		c.receivePeerFrame(s, f, in)
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
		err := c.recvError(s, f, in.err)
		if (f.Kind == KindReply || f.Kind == KindFail) && hooks.Pending(c.sys, in.to) {
			// The ask would otherwise wait for its timeout: it fails with the decode error.
			c.forgetLookup(s.peer, in.to)
			c.inbound(s, f, in, hooks.RemotePayload{Kind: hooks.RemoteFail, Err: err})
		}
		c.fail(s, f, err)
		return
	}
	switch f.Kind {
	case KindMsg, KindAsk:
		c.receiveMessage(s, f, in)
	case KindReply:
		c.forgetLookup(s.peer, in.to)
		c.receiveReply(s, f, in)
	case KindFail:
		c.forgetLookup(s.peer, in.to)
		err := c.errs.Remote(s.peer.name, in.failure.Code, in.failure.Message)
		c.inbound(s, f, in, hooks.RemotePayload{Kind: hooks.RemoteFail, Err: err})
	}
}

// receivePeerFrame applies a WATCH, UNWATCH, TERMINATED or LOOKUP frame that s accepted. Its
// recipient must be on this node and its sender on the peer's: a watcher or a lookup's reply
// address of the peer for WATCH, UNWATCH and LOOKUP, the watched actor for TERMINATED. Otherwise
// it is discarded misrouted, or decode when its sender is not an address.
func (c *Core) receivePeerFrame(s *session, f *Frame, in *inbound) {
	switch {
	case !in.toOK || in.to.Node != c.node:
		c.discard(s, f, DiscardMisrouted)
		return
	case in.err != nil:
		c.discard(s, f, DiscardDecode)
		c.fail(s, f, c.recvError(s, f, in.err))
		return
	case in.from.Node != s.peer.name:
		c.discard(s, f, DiscardMisrouted)
		return
	}
	switch f.Kind {
	case KindWatch:
		c.receiveWatch(s.peer, watchPair{watcher: in.from, watched: in.to}, in.watch)
	case KindUnwatch:
		c.receiveUnwatch(s.peer, watchPair{watcher: in.from, watched: in.to}, in.watch)
	case KindTerminated:
		c.receiveTerminated(s, f, in)
	case KindLookup:
		c.receiveLookup(s.peer, in.from, in.to, f.Type)
	}
}

// receiveMessage hands a MSG or ASK to the host, unless its recipient is a live actor whose
// message type does not take the value: then it is discarded wrong-type, becomes a dead letter
// wrong-type and fails the seed (REM-230, REM-240), and an ASK fails back to its asker with an
// error wrapping lockstep.ErrWrongType, as the runtime fails back a request it dead-letters
// (MSG-350). A recipient that is not a live actor gets the frame all the same, so that the
// runtime makes it a dead letter and fails an ask request back with ErrTerminated.
//
// Assignable and LookupPath are two reads, so in production an actor that starts or stops at the
// path between them can make a frame wrong-type that the runtime would have dead-lettered, or the
// other way round. In simulation nothing runs between them.
func (c *Core) receiveMessage(s *session, f *Frame, in *inbound) {
	if !hooks.Assignable(c.sys, in.to, in.value) && c.live(in.to) {
		c.discard(s, f, DiscardWrongType)
		from, to, ask, v := in.from, in.to, in.ask, in.value
		cause := fmt.Errorf("%w: the actor at %s does not take it", lockstep.ErrWrongType, quoted(to.String()))
		c.deferred = append(c.deferred, func() { c.dead(from, to, DeadWrongType, v) })
		if f.Kind == KindAsk {
			fail := hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteFail, Err: cause})
			// Like the runtime's reply attempts, the failure has no caller to report an error to.
			c.deferred = append(c.deferred, func() { _ = c.Deliver(engine.Envelope{From: to, To: ask, Payload: fail}) })
		}
		c.fail(s, f, c.recvError(s, f, cause))
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
//
// Assignable is read first and Pending only when it refuses. Assignable is false for an ask that
// is no longer pending, and an ask never becomes pending again, so an ask that resolves (a
// timeout, say) at any point of the check gives a late reply, never a wrong-type failure; reading
// Pending first would leave that window open.
func (c *Core) receiveReply(s *session, f *Frame, in *inbound) {
	if !hooks.Assignable(c.sys, in.to, in.value) && hooks.Pending(c.sys, in.to) {
		c.discard(s, f, DiscardWrongType)
		err := c.recvError(s, f, fmt.Errorf("%w: the ask %s does not take it", lockstep.ErrWrongType, quoted(in.to.String())))
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
// "lockstep: remote: <node>: <kind> from <peer>: <wire name>: <cause>", with the wire name
// quoted, and without it when the frame has none.
func (c *Core) recvError(s *session, f *Frame, cause error) error {
	typ := ""
	if f.Type != "" {
		typ = quoted(f.Type) + ": "
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
// not encode or whose frame exceeded MaxFrame with err: code other and err's text, cut to fit
// MaxFrame. It logs a warning and reports err to the Failer (REM-266). It runs without the lock,
// as encode does. It returns the frame-size error when even a FAIL with an empty message does not
// fit.
func (c *Core) replyFailed(env engine.Envelope, v any, err error) (*outFrame, error) {
	name, nerr := c.set.Registry.NameOf(v)
	if nerr != nil {
		name = fmt.Sprintf("%T", v)
	}
	c.log.Warn("remoting: reply sent as a failure", "node", c.node, "peer", env.To.Node, "type", name,
		"err", quoted(err.Error()))
	if c.failer != nil {
		c.failer.Fail(fmt.Errorf("lockstep: remote: %s: REPLY to %s: %s: %w", c.node, env.To.Node, name, err))
	}
	f := Frame{Kind: KindFail, Sender: addrText(env.From), Recipient: addrText(env.To)}
	out := &outFrame{b: c.failFrame(&f, Failure{Code: codeOther, Message: err.Error()}), kind: KindFail, from: env.From, to: env.To, msg: err}
	if n := out.size(); n > c.set.MaxFrame {
		return nil, c.errs.FrameTooLarge(n, c.set.MaxFrame)
	}
	return out, nil
}

// failFrame encodes f, a FAIL frame, with the failure fl, cutting fl's message at a rune
// boundary so that the frame fits MaxFrame, if an empty message fits.
func (c *Core) failFrame(f *Frame, fl Failure) []byte {
	fl.Message = strings.ToValidUTF8(fl.Message, "�")
	f.Payload = AppendFailure(nil, &fl)
	b := AppendFrame(nil, f)
	// The failure is the rest of the frame, so each byte cut from the message is one byte less.
	over := len(b) - prefixLen - c.set.MaxFrame
	if over <= 0 {
		return b
	}
	fl.Message = cutUTF8(fl.Message, max(len(fl.Message)-over, 0))
	f.Payload = AppendFailure(nil, &fl)
	return AppendFrame(nil, f)
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
	c.tally.gaps++
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
