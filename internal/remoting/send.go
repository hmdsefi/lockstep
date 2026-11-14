// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// Errors builds the errors of package remote's types for the core, which cannot import that
// package. The package that creates the core supplies it.
type Errors interface {
	// Remote returns the failure that node reported with a FAIL frame of code and msg.
	Remote(node string, code uint8, msg string) error
	// Unreachable returns the failure of an ask whose request the core could not send to node.
	Unreachable(node string) error
	// PeerRestarted returns the failure of an ask that node lost by restarting.
	PeerRestarted(node string) error
	// FrameTooLarge returns the error of a frame of size bytes, its length without the prefix,
	// that exceeds limit; it reads "remote: frame too large: <size> bytes > max <limit>".
	FrameTooLarge(size, limit int) error
	// Code returns the failure code that a FAIL frame carries for err.
	Code(err error) uint8
}

// The dead-letter reasons of the send path.
const (
	DeadQueueFull       = "queue-full"         // the peer's queue held OutboundQueue frames already
	DeadUnknownNode     = "unknown-node"       // the driver knows no address for the peer
	DeadTooLargeForPeer = "too-large-for-peer" // the frame exceeds the frame limit the peer announced
)

// labelFail is the timer that fails asks whose requests were dead letters (REM-145).
const labelFail = "lockstep/remote/fail"

// recordSend is written for every data frame the core writes.
const recordSend = "lockstep.remote.send"

// seqOffset is where a frame's sequence number starts, length prefix included.
const seqOffset = prefixLen + 4

// remoteKinds maps the kinds of remote envelope one to one to frame kinds (REM-120).
var remoteKinds = [...]Kind{
	hooks.RemoteMsg:        KindMsg,
	hooks.RemoteAsk:        KindAsk,
	hooks.RemoteReply:      KindReply,
	hooks.RemoteFail:       KindFail,
	hooks.RemoteWatch:      KindWatch,
	hooks.RemoteUnwatch:    KindUnwatch,
	hooks.RemoteTerminated: KindTerminated,
}

// frameKind returns the frame kind of k, or 0 when k is not a kind of remote envelope.
func frameKind(k hooks.RemoteKind) Kind {
	if int(k) < len(remoteKinds) {
		return remoteKinds[k]
	}
	return 0
}

// outFrame is one data frame Deliver accepted and no session has written yet.
type outFrame struct {
	b        []byte // the frame with its length prefix; the sequence number is set when written
	kind     Kind
	from, to engine.Address
	typ      string         // the wire name, or empty
	msg      any            // what a dead letter carries: the message, the answer or the failure
	ask      engine.Address // ASK: the ask address, failed when the frame is a dead letter
	inc      uint32         // the peer's known incarnation when the frame was queued, 0 if unknown (REM-130)
	dropped  int            // the headers dropped because they break the header limits (REM-055)
}

// size returns the frame's length L, without the length prefix.
func (f *outFrame) size() int { return len(f.b) - prefixLen }

// askFailure is an ask to fail because its request was a dead letter.
type askFailure struct {
	target, ask engine.Address
	err         error
}

// Deliver sends env to the node env.To.Node. It encodes the message with the configured registry
// and codec and returns an error, sending nothing, when env cannot be sent at all: its payload is
// not one the runtime builds, its message type is not registered or does not encode, or the frame
// exceeds MaxFrame, which returns the error of Errors.FrameTooLarge.
//
// Otherwise Deliver queues the frame for the peer, dials the peer if no session is up or on its
// way, and writes the queued frames in order once the session is up and its link takes them. It
// never blocks and reports no loss: a frame that cannot be sent becomes a dead letter, with reason
// "unknown-node" when the driver knows no address for the peer, "queue-full" when OutboundQueue
// frames are queued for the peer already, and "too-large-for-peer" when it exceeds the frame limit
// the peer announced. An ask whose request becomes a dead letter fails in a later callback, with
// Errors.Unreachable, or with Errors.FrameTooLarge for a frame too large for the peer. Headers
// that break the header limits are dropped and the frame is sent without them, with one warning
// per session.
//
// Call it in engine context, inside a turn or a callback, holding no lock of the runtime.
func (c *Core) Deliver(env engine.Envelope) error {
	node := env.To.Node
	if node == c.node || !engine.ValidNodeName(node) {
		return fmt.Errorf("remoting: cannot send to node %q", node)
	}
	f, err := c.encode(env)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.unlock()
	c.enqueue(node, f)
	return nil
}

// encode builds env's data frame (REM-110, REM-115, REM-120). It runs without the lock: it reads
// only settings that do not change. A reply attempt always becomes a frame: a nil answer is a
// REPLY with an empty type and payload, and an answer that does not encode, or whose frame exceeds
// MaxFrame, is sent as a FAIL in its place (REM-266).
func (c *Core) encode(env engine.Envelope) (*outFrame, error) {
	p, ok := hooks.Payload(env.Payload)
	if !ok {
		return nil, fmt.Errorf("remoting: payload of type %T is not a runtime payload", env.Payload)
	}
	k := frameKind(p.Kind)
	out := &outFrame{kind: k, from: env.From, to: env.To}
	f := Frame{Kind: k, Sender: addrText(env.From), Recipient: addrText(env.To)}
	switch k {
	case KindMsg, KindAsk, KindReply:
		var name string
		var data []byte
		var err error
		if k != KindReply || p.Value != nil {
			name, data, err = c.set.Registry.Encode(c.set.Codec, p.Value)
		}
		if err != nil {
			if k == KindReply {
				return c.replyFailed(env, p.Value, err), nil
			}
			return nil, err
		}
		f.Type, f.Payload = name, data
		if k == KindAsk {
			f.Payload = AppendAsk(nil, addrText(p.Ask), data)
			out.ask = p.Ask
		}
		if ValidHeaders(env.Headers) {
			f.Headers = env.Headers
		} else {
			out.dropped = len(env.Headers)
		}
		out.typ, out.msg = name, p.Value
	case KindFail:
		if p.Err == nil {
			return nil, errors.New("remoting: FAIL envelope without an error")
		}
		f.Payload = AppendFailure(nil, &Failure{Code: c.errs.Code(p.Err), Message: p.Err.Error()})
		out.msg = p.Err
	case 0:
		return nil, fmt.Errorf("remoting: unknown remote envelope kind %d", p.Kind)
	default:
		// WATCH, UNWATCH and TERMINATED carry a watch number that the watch path adds.
		return nil, fmt.Errorf("remoting: %s envelopes are not supported", k)
	}
	// The sequence number stays 0 until flush writes the frame.
	out.b = AppendFrame(nil, &f)
	if n := out.size(); n > c.set.MaxFrame {
		err := c.errs.FrameTooLarge(n, c.set.MaxFrame)
		if k == KindReply {
			return c.replyFailed(env, p.Value, err), nil
		}
		return nil, err
	}
	return out, nil
}

// addrText returns a's text form, as Address.MarshalText writes it: empty for the zero address.
func addrText(a engine.Address) string {
	if a == (engine.Address{}) {
		return ""
	}
	return a.String()
}

// enqueue queues f for node and writes or dials (REM-130 to REM-140). A full queue makes f a dead
// letter "queue-full"; a driver that knows no address for node makes the queue, f included, dead
// letters "unknown-node".
func (c *Core) enqueue(node string, f *outFrame) {
	p := c.peer(node)
	if len(p.queue) >= c.set.OutboundQueue {
		c.drop([]*outFrame{f}, DeadQueueFull, func() error { return c.errs.Unreachable(node) })
		return
	}
	f.inc = p.inc
	p.queue = append(p.queue, f)
	switch {
	case p.out != nil && p.out.up:
		c.flush(p.out)
	case p.out != nil || p.redial != nil:
		// A dial is in progress or armed; the frame goes out once a session is up.
	default:
		err := c.dial(p)
		switch {
		case err == nil:
		case errors.Is(err, ErrUnknownNode):
			// No failure is counted, as Connect counts none: nothing is retried.
			c.dropQueue(p, DeadUnknownNode)
			c.tidy(p)
		default:
			c.dialFailed(p, err)
		}
	}
}

// flush writes the frames queued for s's peer on s, an up outgoing session, in order, until the
// queue is empty or the link refuses a frame, which then stays queued. Each written frame gets the
// session's next sequence number, from 1 (REM-130). A frame larger than the peer's frame limit
// becomes a dead letter "too-large-for-peer" and takes no sequence number.
func (c *Core) flush(s *session) {
	p := s.peer
	for len(p.queue) > 0 && !s.held() {
		f := p.queue[0]
		if limit := int(s.peerLimit); f.size() > limit {
			c.popQueue(p)
			c.drop([]*outFrame{f}, DeadTooLargeForPeer, func() error { return c.errs.FrameTooLarge(f.size(), limit) })
			continue
		}
		seq := s.seq + 1
		binary.BigEndian.PutUint64(f.b[seqOffset:], seq)
		if !s.link.Write(f.b) {
			s.stalled = true
			return
		}
		c.popQueue(p)
		s.seq = seq
		s.lastSend = c.clock.Now()
		if f.dropped > 0 && !s.headersWarned {
			s.headersWarned = true
			c.warn("remoting: headers dropped: they break the header limits", "node", c.node, "peer", p.name,
				"conn", s.conn, "kind", f.kind.String(), "from", addrText(f.from), "to", addrText(f.to), "headers", f.dropped)
		}
		if c.tracer != nil {
			c.tracer.Record(recordSend,
				engine.Attr{Key: "kind", Value: f.kind.String()},
				engine.Attr{Key: "seq", Value: strconv.FormatUint(seq, 10)},
				engine.Attr{Key: "from", Value: addrText(f.from)},
				engine.Attr{Key: "to", Value: addrText(f.to)},
				engine.Attr{Key: "type", Value: f.typ},
				engine.Attr{Key: "bytes", Value: strconv.Itoa(len(f.b))},
			)
		}
	}
}

// popQueue removes the first frame of p's queue.
func (c *Core) popQueue(p *peer) {
	p.queue[0] = nil
	p.queue = p.queue[1:]
	if len(p.queue) == 0 {
		p.queue = nil
	}
}

// dropQueue makes every frame queued for p a dead letter with reason, and fails the asks among
// them with the unreachable error.
func (c *Core) dropQueue(p *peer, reason string) {
	if len(p.queue) == 0 {
		return
	}
	q := p.queue
	p.queue = nil
	c.drop(q, reason, func() error { return c.errs.Unreachable(p.name) })
}

// failTimer is one armed lockstep/remote/fail timer and the asks it fails.
type failTimer struct {
	cancel engine.Cancel
	fails  []askFailure
}

// drop makes frames dead letters with reason, in order, once the lock is released, and fails the
// asks among them with the error askErr builds, called once and only when there is an ask, in one
// later lockstep/remote/fail callback (REM-145). None of the frames took a sequence number.
func (c *Core) drop(frames []*outFrame, reason string, askErr func() error) {
	var fails []askFailure
	var err error
	for _, f := range frames {
		c.deferred = append(c.deferred, func() { c.dead(f.from, f.to, reason, f.msg) })
		if f.kind == KindAsk {
			if err == nil {
				err = askErr()
			}
			fails = append(fails, askFailure{target: f.to, ask: f.ask, err: err})
		}
	}
	if len(fails) == 0 {
		return
	}
	t := &failTimer{fails: fails}
	// The callback cannot run before the lock is released, so t.cancel is set by then.
	t.cancel = c.clock.AfterFunc(0, labelFail, func() { c.failFired(t) })
	c.failTimers = append(c.failTimers, t)
}

// failFired fails t's asks through the host, in order, without the lock.
func (c *Core) failFired(t *failTimer) {
	c.mu.Lock()
	i := slices.Index(c.failTimers, t)
	if i >= 0 {
		c.failTimers = slices.Delete(c.failTimers, i, i+1)
	}
	c.unlock()
	if i < 0 {
		return // stopped
	}
	for _, a := range t.fails {
		c.host.Inbound(engine.Envelope{
			From:    a.target,
			To:      a.ask,
			Payload: hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteFail, Err: a.err}),
		})
	}
}

// unlock releases the lock, then runs the calls deferred while it was held, in order.
func (c *Core) unlock() {
	calls := c.deferred
	c.deferred = nil
	c.mu.Unlock()
	for _, call := range calls {
		call()
	}
}
