// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// Pipe delivers the resolution of r to the actor of ctx as one of its own messages: when r
// resolves, to(value, err) is called during a turn of the same incarnation and its result is
// passed to Receive in that turn. If the incarnation has ended by then, nothing is delivered.
// Pipe panics if ctx, r or to is nil, if ctx is outside its turn, or if r was created on another
// system.
func Pipe[R, M any](ctx *Context[M], r *Reply[R], to func(R, error) M) {
	// MSG-600's checks, in its order.
	if ctx == nil {
		panic("lockstep: Pipe with nil Context")
	}
	if r == nil {
		panic("lockstep: nil Reply")
	}
	if to == nil {
		panic("lockstep: Pipe with nil function")
	}
	ctx.mustInTurn()
	c := ctx.cell
	if r.sys != c.sys {
		panic("lockstep: Pipe of a Reply from another system")
	}

	// The continuation, bound to ctx's incarnation (MSG-600). It runs when r resolves, during the
	// call of the reply attempt, or right here if r has resolved already. It only enqueues: to
	// runs in a later turn of the actor (MSG-620). A local reply attempt enqueues before
	// ReplyTo.Send or Fail returns, so the entry follows the earlier messages of the same pair
	// (MSG-610); the mailbox always admits a Self entry (MBX-030).
	//
	// If the incarnation has ended by then, or the actor has completed its final stop, the
	// continuation enqueues nothing and records lockstep.pipe.drop (MSG-610); offerSelf decides
	// under the cell's lock. An entry enqueued before the incarnation ended is dropped when it is
	// dequeued (MSG-620) or drained at the final stop (MBX-160).
	inc := ctx.inc
	r.then(func() {
		ok := c.offerSelf(entry{
			msg:  pipedReply[M]{ask: r.ask, reply: r.resolution(), call: func() M { return to(r.val, r.err) }},
			kind: entrySelf,
			inc:  inc,
		})
		if !ok {
			c.dropPiped(r.ask)
		}
	})
}

// pipedReply is the msg of a Self entry that Pipe enqueues: the ask it answers, for
// lockstep.pipe.drop, the reply's resolution, which the drain's lockstep.drop describes (MBX §8),
// and the call of to with the reply's value and error.
type pipedReply[M any] struct {
	ask   Address
	reply any // the reply's error when it is non-nil, otherwise its value
	call  func() M
}

// resolution returns r's error when it is non-nil, otherwise r's value. r has resolved.
func (r *Reply[R]) resolution() any {
	if r.err != nil {
		return r.err
	}
	return r.val
}

// processSelf handles one Self entry of the user mailbox: a timer message goes to processTimer and
// an effect result to processEffectResult. It returns the message that the turn's lockstep.turn
// record describes (ENG-340): the M that to returned for a piped reply, none when the entry was
// discarded or to panicked.
//
// An entry of an incarnation that has ended is not delivered (MBX-170): a piped reply is recorded
// only as lockstep.pipe.drop (MSG-620), any other Self entry as lockstep.drop reason
// incarnation-ended.
func (c *cell[M]) processSelf(e entry) (msg any, ok bool) {
	if e.timer != 0 {
		return c.processTimer(e)
	}
	if r, isEffect := e.msg.(effectResult[M]); isEffect {
		return c.processEffectResult(e, r)
	}
	p, isPipe := e.msg.(pipedReply[M])
	if e.inc != c.inc {
		if isPipe {
			c.dropPiped(p.ask)
		} else {
			c.sys.recordDropEnded(c.addr, e.inc, e.msg)
		}
		return nil, false
	}
	if !isPipe {
		return nil, false
	}
	// to and Receive are both calls of this turn (MSG-620): one guarded handler call with source
	// receive, so a panic or a Fail in either is a failure of Receive (SUP-140, SUP-150).
	c.callHandler(supervise.InReceive, func() {
		m := p.call()
		msg, ok = m, true
		c.actor.Receive(c.ctx, m)
	})
	return msg, ok
}

// dropPiped discards a piped reply of an incarnation that has ended: to and Receive are not
// called, and lockstep.pipe.drop records it (MSG-610, MSG-620, MSG §8).
func (c *cell[M]) dropPiped(ask Address) {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.pipe.drop",
		engine.Attr{Key: "addr", Value: c.addr.String()},
		engine.Attr{Key: "ask", Value: ask.String()},
		engine.Attr{Key: "reason", Value: "incarnation"},
	)
}
