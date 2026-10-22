// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "github.com/hmdsefi/lockstep/internal/engine"

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
	// An incarnation that has ended by then is dropped when the actor dequeues the entry
	// (dropPiped), which is all plan 1.2's one incarnation needs. 1.2-11 makes this check whether
	// the cell has stopped too and emit lockstep.pipe.drop instead of enqueueing (MSG-610).
	inc := ctx.inc
	r.then(func() {
		c.offerUser(entry{
			msg:  pipedReply[M]{ask: r.ask, call: func() M { return to(r.val, r.err) }},
			kind: entrySelf,
			inc:  inc,
		}, nil)
	})
}

// pipedReply is the msg of a Self entry that Pipe enqueues: the ask it answers, for
// lockstep.pipe.drop, and the call of to with the reply's value and error.
type pipedReply[M any] struct {
	ask  Address
	call func() M
}

// processSelf handles one Self entry of the user mailbox. It returns the message that the turn's
// lockstep.turn record describes (ENG-340): the M that to returned for a piped reply, none when
// the entry was discarded or to panicked (m20).
func (c *cell[M]) processSelf(e entry) (msg any, ok bool) {
	p, isPipe := e.msg.(pipedReply[M])
	if !isPipe {
		return nil, false
	}
	if e.inc != c.inc { // MSG-620: another incarnation dequeues it
		c.dropPiped(p.ask)
		return nil, false
	}
	// to and Receive are both calls of this turn (MSG-620): one in-turn window, so a panic of
	// either goes to handlerFailed through leave.
	c.enter()
	defer c.leave()
	m := p.call()
	msg, ok = m, true
	c.actor.Receive(c.ctx, m)
	return msg, ok
}

// dropPiped discards a piped reply of an incarnation that has ended: to and Receive are not
// called, and lockstep.pipe.drop records it (MSG-620, MSG §8).
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
