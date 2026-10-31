// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
)

// defaultAskTimeout is an ask's timeout without WithTimeout (MSG-320).
const defaultAskTimeout = 5 * time.Second

// askTimeoutLabel is the label of the ask timeout timers (ENG-070).
const askTimeoutLabel = "lockstep/ask-timeout"

// askConfig is what the options of one Ask configure. Options only record values; Ask validates
// them (MSG-700).
type askConfig struct {
	timeout time.Duration
}

// AskOption configures one Ask.
type AskOption func(*askConfig)

// WithTimeout sets the ask's timeout. The default is 5 s. It never panics: Ask and NewReply
// resolve the reply with an error wrapping ErrInvalidOption when d <= 0.
func WithTimeout(d time.Duration) AskOption {
	return func(c *askConfig) { c.timeout = d }
}

// Ask sends the message build returns to the actor to and returns the future of its answer.
// build receives the ask's ReplyTo and is called once, synchronously, before Ask returns, unless
// the ask fails before sending. With a *System sender outside any actor's turn, a full
// BlockSender mailbox in a System built by New makes Ask wait until the request is enqueued or
// the ask resolves, at its timeout at the latest. An invalid option value resolves the reply with
// an error wrapping ErrInvalidOption. Ask panics if from or build is nil, an option is nil, or
// from is a Context outside its turn.
func Ask[M, R any](from Sender, to PID[M], build func(ReplyTo[R]) M, opts ...AskOption) *Reply[R] {
	// 1. Misuse panics.
	s := mustSender(from)
	if build == nil {
		panic("lockstep: Ask with nil build function")
	}
	for _, o := range opts {
		if o == nil {
			panic("lockstep: nil AskOption")
		}
	}
	sys, fromAddr, fromRef := s.sendOrigin() // panics for a Context outside its turn (ACT-120)

	// 2. Options, in argument order: the last WithTimeout wins.
	cfg, err := applyAskOptions(opts)

	// 3. Failures before sending: nothing is built, allocated or recorded.
	if err != nil {
		return resolvedReply[R](sys, err)
	}
	if to.IsZero() {
		return resolvedReply[R](sys, fmt.Errorf("%w: %s", ErrNotFound, to.addr))
	}
	if sys.stopped.Load() {
		return resolvedReply[R](sys, sys.errStopped())
	}

	// 4. The ask address and the table entry. newAsk checks the stopped mark again under its
	// shard's lock, for a system that stopped since the check above (MSG-360).
	r := newReply[R](sys)
	p := &r.entry
	if !sys.newAsk(p, r, fromAddr, to.addr, cfg.timeout) {
		return resolvedReply[R](sys, sys.errStopped())
	}
	r.ask = p.addr // before Ask returns, so before any Pipe reads it

	// 5. The request; a panic of build removes the entry and propagates.
	msg, size := buildRequest(sys, p, to.addr, build)

	// 6. Timer, record, routing exactly as Send's, with the ask address in the mailbox entry.
	sys.armTimeout(p)
	sys.recordAsk(fromAddr, p, msg)
	req := routeRequest{
		from:    fromAddr,
		fromRef: fromRef,
		to:      to.addr,
		typ:     reflect.TypeFor[M](),
		msg:     msg,
		ask:     p.addr,
		size:    size,
	}
	if _, outside := from.(*System); outside {
		// A wait for room in a BlockSender mailbox starts after the timer and ends when the ask
		// resolves, at its timeout at the latest (MSG-115, MSG-330); an actor never waits, and
		// neither does a call in engine context (offer).
		req.wait = askWait{done: r.done, resolved: &r.resolved}
	}
	o := sys.route(req)

	// 7. The routing outcome (MSG-310). The router has already resolved the ask for a dead letter
	// on this system (ErrTerminated) and for a request the target's mailbox refused or dropped
	// (askRefused), and made an enqueued request tracked by its target; a wait for room ended only
	// because the ask resolved (MSG-330). A request handed to the transport or held by the
	// interposer leaves the ask pending.
	if o.kind == routeFailed {
		sys.settle(p.addr, resolution{how: byRouting, from: to.addr, err: o.err})
	}
	return r
}

// askWait is the context of a *System sender's wait inside Ask for room in a BlockSender mailbox
// (MSG-330): it ends when the ask resolves, so its Done is the reply's done channel. It needs no
// goroutine and no timer of its own, since the ask's timeout resolves the reply. Its Err is
// context.Canceled once the ask has resolved; the wait then leaves the line without enqueueing the
// request and Ask returns the resolved reply. A slot handed to the waiter after the ask resolved but
// before the waiter saw it still enqueues the request (MBX-070): the hand-off came first in the
// queue's order, and the target's answer is then recorded as late. The request is tracked by its
// target for the whole wait, so a target that completes its final stop while a timed-out waiter
// has not yet left the line fails the ask once more, recorded as late.
type askWait struct {
	done     <-chan struct{} // the reply's done channel, closed at resolution
	resolved *atomic.Bool    // the reply's resolved mark, set before done is closed
}

func (askWait) Deadline() (time.Time, bool) { return time.Time{}, false }

func (w askWait) Done() <-chan struct{} { return w.done }

func (w askWait) Err() error {
	if w.resolved.Load() {
		return context.Canceled
	}
	return nil
}

func (askWait) Value(any) any { return nil }

// buildRequest calls build with p's ReplyTo and returns the request and its size for the mailbox
// estimate of the target at to (OVL-410). If build or Size panics, p leaves the table before the
// panic propagates (MSG-300 step 5).
func buildRequest[M, R any](sys *System, p *pendingAsk, to Address, build func(ReplyTo[R]) M) (msg M, size int64) {
	built := false
	defer func() {
		if !built {
			sys.asks.drop(p)
		}
	}()
	msg = build(ReplyTo[R]{addr: p.addr})
	size = sys.sizeFor(to, msg)
	built = true
	return msg, size
}

// NewReply creates an ask without a target: a pending reply on from's system with Ask's timeout
// and exactly-once resolution, and the ReplyTo that answers it. The caller builds and sends the
// request itself with Send. An error from that Send (for example ErrMailboxFull) does not
// resolve the reply; the caller resolves it, for example with rt.Fail(from, err). A request that
// a DropNewest or DropOldest mailbox drops is not reported, so the reply resolves at its timeout.
// An invalid option value returns the zero ReplyTo and a reply resolved with an error wrapping
// ErrInvalidOption. NewReply panics if from is nil, an option is nil, or from is a Context
// outside its turn.
func NewReply[R any](from Sender, opts ...AskOption) (ReplyTo[R], *Reply[R]) {
	// MSG-380's checks, in its order: from nil, a Context outside its turn (ACT-120), a nil option.
	sys, fromAddr, _ := mustSender(from).sendOrigin()
	for _, o := range opts {
		if o == nil {
			panic("lockstep: nil AskOption")
		}
	}
	// An invalid option value, then a stopped system: the zero ReplyTo and a resolved reply, with
	// nothing allocated or recorded.
	cfg, err := applyAskOptions(opts)
	if err != nil {
		return ReplyTo[R]{}, resolvedReply[R](sys, err)
	}
	if sys.stopped.Load() {
		return ReplyTo[R]{}, resolvedReply[R](sys, sys.errStopped())
	}
	// MSG-300 step 4 with the zero target, then the timer and the record. No actor tracks the ask
	// (MSG-390): it resolves by a reply attempt, its timeout or its system stopping.
	r := newReply[R](sys)
	p := &r.entry
	if !sys.newAsk(p, r, fromAddr, Address{}, cfg.timeout) {
		return ReplyTo[R]{}, resolvedReply[R](sys, sys.errStopped())
	}
	r.ask = p.addr
	sys.armTimeout(p)
	sys.recordAsk(fromAddr, p, nil)
	return ReplyTo[R]{addr: p.addr}, r
}

// errStopped is the ErrStopped error of a send, an ask or a pending ask of the stopped system s
// (MSG §7): it names the system address.
func (s *System) errStopped() error { return fmt.Errorf("%w: %s", ErrStopped, s.user) }

// applyAskOptions applies opts, which hold no nil option, in argument order, so the last
// WithTimeout wins, and validates the result: a timeout <= 0 is MSG-700's ErrInvalidOption error.
func applyAskOptions(opts []AskOption) (askConfig, error) {
	cfg := askConfig{timeout: defaultAskTimeout}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.timeout <= 0 {
		return cfg, fmt.Errorf("%w: WithTimeout(%s): timeout must be positive", ErrInvalidOption, cfg.timeout)
	}
	return cfg, nil
}

// armTimeout starts p's timeout timer on the engine clock (MSG-320): a deadline of the engine's
// Deadlines, keyed by p's uid, so that concurrent asks spread over its structure, or else a
// Clock.AfterFunc timer (ENG-070). If p resolved already (a reply attempt can arrive through a
// ReplyTo that build leaked to another goroutine), it stops the timer again.
func (s *System) armTimeout(p *pendingAsk) {
	if dl := s.eng.Deadlines; dl != nil {
		p.deadline = dl.Arm(p.addr.uid, p.timeout, p)
	} else {
		p.cancel = s.eng.Clock.AfterFunc(p.timeout, askTimeoutLabel, func() { s.askTimedOut(p) })
	}
	if !p.timer.CompareAndSwap(timerUnarmed, timerArmed) {
		s.stopTimeout(p)
	}
}
