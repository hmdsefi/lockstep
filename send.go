// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/mailbox"
)

// ErrMailboxFull is the error of a message that a full mailbox refuses (MBX-090). A refusal
// returns fmt.Errorf("%w: %s", ErrMailboxFull, address), so errors.Is matches it and the text
// names the target.
var ErrMailboxFull = errors.New("lockstep: mailbox full")

// Send delivers msg to the actor to. It returns nil when msg was enqueued, became a dead letter,
// or was accepted by the transport for another node. Errors: ErrNotFound for the zero PID,
// ErrStopped when the sender's system is stopped, the target mailbox's policy error
// (ErrMailboxFull, MBX), the admission error (ErrOverloaded, OVL), and a synchronous transport
// error, wrapped (MSG-110). Send panics if from is nil or is a Context outside its turn.
func Send[M any](from Sender, to PID[M], msg M) error {
	sys, fromAddr, fromRef := mustSender(from).sendOrigin() // panics for a Context outside its turn (ACT-120)
	if to.IsZero() {
		return fmt.Errorf("%w: %s", ErrNotFound, to.addr)
	}
	// MSG-100's last check. A system that is only shutting down still sends, so Stopped hooks can
	// send.
	if sys.stopped.Load() {
		return sys.errStopped()
	}
	sys.recordSend(fromAddr, to.addr, msg)
	return sys.route(routeRequest{
		from:    fromAddr,
		fromRef: fromRef,
		to:      to.addr,
		typ:     reflect.TypeFor[M](),
		msg:     msg,
	}).err
}

// mustSender returns from's senderInfo. It panics with ACT's nil Sender message if from is nil or
// a nil pointer (ACT-200).
func mustSender(from Sender) senderInfo {
	var s senderInfo
	if from != nil {
		s, _ = from.(senderInfo)
	}
	if s == nil || s.nilSender() {
		panic("lockstep: nil Sender")
	}
	return s
}

// senderInfo is what Send needs from a Sender; exactly *System and *Context[X] implement it.
type senderInfo interface {
	Sender
	// nilSender reports whether the receiver is a nil pointer (ACT-200).
	nilSender() bool
	// sendOrigin returns the sender's system, the sender's address (ACT-210), and the cell whose
	// turn sends, nil for a System. It panics for a Context outside its turn (ACT-120).
	sendOrigin() (sys *System, from Address, ref engine.ActorRef)
}

func (s *System) nilSender() bool { return s == nil }

// sendOrigin returns the user guardian as the sender's address; a System sends outside any turn.
//
//nolint:unparam // implements senderInfo, whose Context implementation returns the sending cell
func (s *System) sendOrigin() (*System, Address, engine.ActorRef) { return s, s.user, nil }

func (c *Context[M]) nilSender() bool { return c == nil }

func (c *Context[M]) sendOrigin() (*System, Address, engine.ActorRef) {
	c.mustInTurn()
	return c.cell.sys, c.cell.addr, c.cell
}

// recordSend emits lockstep.send (MSG §8) before a message is routed.
func (s *System) recordSend(from, to Address, msg any) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.send",
		engine.Attr{Key: "from", Value: from.String()},
		engine.Attr{Key: "to", Value: to.String()},
		engine.Attr{Key: "msg", Value: tr.Describe(msg)},
	)
}

// The dead-letter reasons of MBX-150, in its order. REM adds its own in Phase 3.
const (
	reasonStopped     = "stopped"     // a local address that is not alive, or an inbound one that names no live actor
	reasonNotFound    = "not-found"   // a path address that resolves to no actor of the sender's message type
	reasonUnreachable = "unreachable" // another node, and the engine has no Transport
)

// routeRequest is one user message on its way to a mailbox: what Send routes, and what Ask and
// Host.Inbound route the same way.
type routeRequest struct {
	from    Address         // the sender
	fromRef engine.ActorRef // the cell whose turn sends, or nil outside any turn
	to      Address         // the target, as the sender named it
	typ     reflect.Type    // the sender's message type M: a path address needs an actor of exactly this type; nil: any
	msg     any             // the M value
	ask     Address         // MBX-092's ask address of an Ask request, zero for every other message
	seq     uint64          // the inbound envelope's Seq (MBX-120), 0 for a local send
}

// routeKind is the kind of a routing outcome.
type routeKind uint8

const (
	routeEnqueued routeKind = iota + 1 // enqueued in a live local actor's user mailbox
	routeDead                          // a dead letter; Send returns nil
	routeHanded                        // handed to Transport.Deliver, which returned nil
	routeFailed                        // Transport.Deliver returned an error
	routeHeld                          // the engine's interposer took the message and offers it later (ENG-150)
	routeRefused                       // the mailbox's policy refused or dropped the message (MBX-090)
)

// outcome is the result of routing one message. Send maps it to MSG-110's result. For an Ask
// request the router itself applies MSG-310's dead-letter and enqueue outcomes (deadRequest,
// offer), also for a request the interposer held, and for routeRefused askRefused has resolved the
// ask already; Ask applies routeFailed.
type outcome struct {
	kind   routeKind
	cell   actorCell // routeEnqueued: the target's cell
	reason string    // routeDead: one of the reason constants
	err    error     // routeFailed: the wrapped transport error; routeRefused: ErrMailboxFull wrapped, nil for a drop
}

// route routes r by its target address (MSG-110): to the local actor, a dead letter, or the
// transport. A local message is enqueued inside the call (MBX-095), after the interposer let it
// through.
func (s *System) route(r routeRequest) outcome {
	if r.to.node != s.node {
		return s.sendRemote(r)
	}
	return s.deliverLocal(r)
}

// deliverLocal offers r to its local target, first through the engine's interposer when it has
// one (ENG-150). A message the interposer holds is offered, without a second interposition, when
// the engine calls offer, in engine context and outside any turn of the sender. Host.Inbound
// uses it for the envelopes that arrive (MBX-120).
func (s *System) deliverLocal(r routeRequest) outcome {
	if ip := s.eng.Interposer; ip != nil {
		held := r
		held.fromRef = nil
		if ip.Interpose(r.from.engine(), r.to.engine(), r.msg, func() { s.offer(held) }) {
			return outcome{kind: routeHeld}
		}
	}
	return s.offer(r)
}

// offer resolves r's target and offers the message to its user mailbox, or makes it a dead letter
// (MBX-150). The message goes to the user mailbox of the actor now registered at the address, so a
// held message whose target stopped meanwhile is a dead letter. The queue's outcome becomes
// MBX-090's result and records: Send returns nil for an enqueued message, a drop and a dead
// letter, and an error wrapping ErrMailboxFull for a refusal. BlockSender acts as FailSend here:
// the offer never waits.
//
// An ask request becomes tracked by its target before it is offered (MSG-310, MSG-340), so the
// reply attempt that removes the entry (MSG-410) cannot come first. A request that the mailbox
// refuses or drops, and the request DropOldest evicts, is reported to askRefused once, after the
// queue's decision and before offer returns (MBX-092, MSG-345), which removes the tracked entry
// again.
//
// A target can complete its final stop between the lookup and the enqueue (an offer from another
// goroutine on the local engine). Its closed mailbox refuses the entry, and the message is a dead
// letter with reason stopped (MBX-150 (b)). The ask of such a request fails with ErrTerminated
// through whoever removes its tracked entry: here, or the stop's sweep of its tracked asks
// (MSG-340), so it fails once and leaves no tracked entry behind.
func (s *System) offer(r routeRequest) outcome {
	c, reason := s.resolve(r.to, r.typ)
	if c == nil {
		return s.deadRequest(r, reason)
	}
	owner := addressOf(c.Address())
	tracked := !r.ask.IsZero() && s.asks.track(r.ask, c, r.ask.node == s.node)
	out, evicted := c.offerUser(entry{msg: r.msg, from: r.from, ask: r.ask, seq: r.seq, kind: entrySent}, r.fromRef)
	switch out {
	case mailbox.Enqueued:
		s.recordDeliver(c, r)
		return outcome{kind: routeEnqueued, cell: c}
	case mailbox.Rejected:
		err := mailboxFullError(owner)
		s.recordMailboxFull(c, r, "rejected", nil)
		s.askRefused(r.ask, owner, err)
		return outcome{kind: routeRefused, err: err}
	case mailbox.DroppedNewest:
		s.recordMailboxFull(c, r, "dropped-newest", nil)
		s.askRefused(r.ask, owner, mailboxFullError(owner))
		return outcome{kind: routeRefused}
	case mailbox.DroppedOldest:
		s.recordMailboxFull(c, r, "dropped-oldest", &evicted)
		s.askRefused(evicted.ask, owner, mailboxFullError(owner))
		s.recordDeliver(c, r)
		return outcome{kind: routeEnqueued, cell: c}
	}
	// The mailbox is closed.
	if r.ask.IsZero() || tracked && s.asks.untrack(r.ask) {
		return s.deadRequest(r, reasonStopped)
	}
	// The stop's sweep took the tracked entry and fails the ask; a request that was not tracked
	// here (a duplicate, or an ask that already resolved) has nothing to fail.
	s.deadLetter(r.from, r.to, reasonStopped, r.msg)
	return outcome{kind: routeDead, reason: reasonStopped}
}

// resolve returns the cell the local address to names, or nil and the dead-letter reason. A full
// address needs an actor registered with exactly that path and uid (reason stopped). A path
// address (uid 0) names whichever actor is registered at its path, provided it was spawned with
// message type typ (reason not-found; ACT-440).
func (s *System) resolve(to Address, typ reflect.Type) (actorCell, string) {
	uid, a, ok := s.reg.Lookup(to.path)
	c, _ := a.(actorCell)
	if to.uid == 0 {
		if !ok || c == nil || typ != nil && c.msgType() != typ {
			return nil, reasonNotFound
		}
		return c, ""
	}
	if !ok || c == nil || uid != to.uid {
		return nil, reasonStopped
	}
	return c, ""
}

// sendRemote routes r to another node: to the transport, or to a dead letter when the engine has
// none (ENG-080). A transport error comes back wrapped (MSG-110), with nothing sent.
func (s *System) sendRemote(r routeRequest) outcome {
	tr := s.eng.Transport
	if tr == nil {
		return s.deadRequest(r, reasonUnreachable)
	}
	err := tr.Deliver(engine.Envelope{
		From:    r.from.engine(),
		To:      r.to.engine(),
		Payload: userPayload{msg: r.msg, ask: r.ask, typ: r.typ},
	})
	if err != nil {
		return outcome{kind: routeFailed, err: fmt.Errorf("lockstep: send to %s: %w", r.to, err)}
	}
	return outcome{kind: routeHanded}
}

// deadRequest makes r a dead letter with reason (MBX-150). The request of an ask whose asker is
// this system resolves that ask with ErrTerminated, for every reason (MSG-310): before
// Ask returns, or when the engine offers a request its interposer held. For an ask request that
// arrived from another node, the system issues Fail(ErrTerminated) from the target address,
// which travels back to the asker like any reply attempt (MSG-350).
func (s *System) deadRequest(r routeRequest, reason string) outcome {
	s.deadLetter(r.from, r.to, reason, r.msg)
	switch {
	case r.ask.IsZero():
	case r.ask.node == s.node:
		s.settle(r.ask, resolution{how: byRouting, from: r.to, err: fmt.Errorf("%w: %s", ErrTerminated, r.to)})
	default:
		s.replyAttempt(r.ask, r.to, nil, fmt.Errorf("%w: %s", ErrTerminated, r.to))
	}
	return outcome{kind: routeDead, reason: reason}
}

// mailboxFullError returns the error of a refusal by the full mailbox of the actor at owner
// (MBX §4.1); a message dropped by a policy is reported with the same error.
func mailboxFullError(owner Address) error { return fmt.Errorf("%w: %s", ErrMailboxFull, owner) }

// askRefused is called for each sent entry that carries the ask address ask, which a mailbox
// policy refused or dropped, and for the entry DropOldest evicted (MBX-092): target is the address
// of the mailbox's actor and err the error of the refusal. It removes ask from the tracked asks, if
// it is there, and issues the reply attempt Fail(err) for ask from target (MSG-345). For an asker
// on this system the attempt is applied before askRefused returns; for an asker on another node it
// goes through the transport. A zero ask is no request, and nothing happens. The caller holds no
// cell lock.
func (s *System) askRefused(ask, target Address, err error) {
	if ask.IsZero() {
		return
	}
	s.asks.untrack(ask)
	s.replyAttempt(ask, target, nil, err)
}

// recordDeliver emits lockstep.deliver (MBX §8) for the entry of r that c's mailbox took.
func (s *System) recordDeliver(c actorCell, r routeRequest) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	length, _, _ := c.mailboxState()
	tr.Record("lockstep.deliver",
		engine.Attr{Key: "from", Value: r.from.String()},
		engine.Attr{Key: "to", Value: c.Address().String()},
		engine.Attr{Key: "seq", Value: strconv.FormatUint(r.seq, 10)},
		engine.Attr{Key: "len", Value: strconv.Itoa(length)},
		engine.Attr{Key: "msg", Value: tr.Describe(r.msg)},
	)
}

// recordMailboxFull emits lockstep.mailbox.full (MBX §8) for the message of r that met the full
// mailbox of c and was not shed. action is rejected, dropped-newest or dropped-oldest; for
// dropped-oldest, dropped is the entry the mailbox evicted.
func (s *System) recordMailboxFull(c actorCell, r routeRequest, action string, dropped *entry) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	length, capacity, policy := c.mailboxState()
	attrs := []engine.Attr{
		{Key: "from", Value: r.from.String()},
		{Key: "to", Value: c.Address().String()},
		{Key: "policy", Value: policy.String()},
		{Key: "action", Value: action},
		{Key: "cause", Value: "capacity"},
		{Key: "cap", Value: strconv.Itoa(capacity)},
		{Key: "len", Value: strconv.Itoa(length)},
		{Key: "msg", Value: tr.Describe(r.msg)},
	}
	if dropped != nil {
		attrs = append(attrs, engine.Attr{Key: "dropped", Value: tr.Describe(dropped.msg)})
	}
	tr.Record("lockstep.mailbox.full", attrs...)
}

// deadLetter is the one place a message that cannot be delivered goes (MBX-150) and emits
// lockstep.deadletter (MBX §8): from is the sender, to the address the message was sent to, reason
// one of the reason constants, msg the M value. For a reply attempt that cannot reach another node
// it is the answer, or the error of a Fail.
func (s *System) deadLetter(from, to Address, reason string, msg any) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.deadletter",
		engine.Attr{Key: "from", Value: from.String()},
		engine.Attr{Key: "to", Value: to.String()},
		engine.Attr{Key: "reason", Value: reason},
		engine.Attr{Key: "msg", Value: tr.Describe(msg)},
	)
}
