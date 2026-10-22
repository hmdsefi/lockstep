// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"reflect"
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
// resolve the reply with an error wrapping ErrInvalidOption when d <= 0 (MSG-700).
func WithTimeout(d time.Duration) AskOption {
	return func(c *askConfig) { c.timeout = d }
}

// Ask sends the message build returns to the actor to and returns the future of its answer.
// build receives the ask's ReplyTo and is called once, synchronously, before Ask returns, unless
// the ask fails before sending (MSG-300). An invalid option value resolves the reply with an error
// wrapping ErrInvalidOption. Ask panics if from or build is nil, an option is nil, or from is a
// Context outside its turn.
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
	cfg := askConfig{timeout: defaultAskTimeout}
	for _, o := range opts {
		o(&cfg)
	}

	// 3. Failures before sending: nothing is built, allocated or recorded. The stopped-system check
	// (ErrStopped) arrives with Shutdown (1.2-12).
	if cfg.timeout <= 0 {
		return resolvedReply[R](sys, fmt.Errorf("%w: WithTimeout(%s): timeout must be positive", ErrInvalidOption, cfg.timeout))
	}
	if to.IsZero() {
		return resolvedReply[R](sys, fmt.Errorf("%w: %s", ErrNotFound, to.addr))
	}

	// 4. The ask address and the table entry.
	r := newReply[R](sys)
	p := sys.newAsk(fromAddr, to.addr, cfg.timeout, r.resolve)

	// 5. The request; a panic of build removes the entry and propagates.
	msg := buildRequest(sys, p, build)

	// 6. Timer, record, routing exactly as Send's, with the ask address in the mailbox entry.
	sys.asks.arm(p, sys.eng.Clock.AfterFunc(cfg.timeout, askTimeoutLabel, func() { sys.askTimedOut(p) }))
	sys.recordAsk(fromAddr, p, msg)
	o := sys.route(routeRequest{
		from:    fromAddr,
		fromRef: fromRef,
		to:      to.addr,
		typ:     reflect.TypeFor[M](),
		msg:     msg,
		ask:     p.addr,
	})

	// 7. The routing outcome (MSG-310). The router has already resolved the ask for a dead letter
	// on this system (ErrTerminated) and made an enqueued request tracked by its target; a request
	// handed to the transport or held by the interposer leaves the ask pending.
	if o.kind == routeFailed {
		sys.settle(p.addr, resolution{how: byRouting, from: to.addr, err: o.err})
	}
	return r
}

// buildRequest calls build with p's ReplyTo and returns the request. If build panics, p leaves the
// table before the panic propagates (MSG-300 step 5).
func buildRequest[M, R any](sys *System, p *pendingAsk, build func(ReplyTo[R]) M) M {
	built := false
	defer func() {
		if !built {
			sys.asks.drop(p)
		}
	}()
	msg := build(ReplyTo[R]{addr: p.addr})
	built = true
	return msg
}
