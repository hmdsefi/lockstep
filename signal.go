// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "github.com/hmdsefi/lockstep/internal/supervise"

// Signal is a notification from lockstep itself, delivered through SignalReceiver[M].Signal and
// never as an M. It is implemented only by Terminated and ChildFailed (SUP), Overloaded (OVL) and,
// from Phase 3, NodeDown and NodeUp (CLU).
type Signal interface {
	signal() // unexported: no other implementations
}

// SignalReceiver is implemented by actors that receive lockstep's notifications (MSG-810). An actor
// without it ignores signals; its supervisor strategy still handles its children's failures
// (SUP-150 to SUP-230).
type SignalReceiver[M any] interface {
	Signal(ctx *Context[M], sig Signal)
}

// signalMsg is the system message that carries a Signal to a cell (MSG-810, SUP-330). Signals
// travel only through the system queue: they never count against the user mailbox's capacity and
// never go through Send. For a Terminated, seq is the sequence number of the Watch call it answers
// (ACT-630); the public Terminated does not carry it.
type signalMsg struct {
	sig Signal
	seq uint64
}

func (signalMsg) isSysMsg() {}

// deliverSignal handles the signal message m at the head of the system queue (MSG-810). An
// incarnation that implements SignalReceiver gets the signal in Signal, during a turn; any other
// ignores it without a record. Only a running incarnation delivers: the other lifecycle states keep
// or drop signals by SUP-320's rules, which supervision applies.
//
// A Terminated whose Addr is not in the incarnation's watched set, or is there under another Watch
// call's sequence number, is skipped; the address leaves the set when the Terminated is delivered
// (ACT-630). An actor without SignalReceiver consumes the watch the same way, so a later Watch of
// the address registers again.
func (c *cell[M]) deliverSignal(m signalMsg) {
	if c.state != supervise.Running {
		return
	}
	sig := m.sig
	if t, ok := sig.(Terminated); ok {
		if seq, in := c.ctx.watched.seq(t.Addr); !in || seq != m.seq {
			return
		}
		c.ctx.watched.remove(t.Addr)
	}
	if c.signals == nil {
		return
	}
	c.callHandler(supervise.InSignal, func() { c.signals.Signal(c.ctx, sig) })
}
