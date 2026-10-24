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
// (ACT-630); the public Terminated has no such field. For an Overloaded, inc is the incarnation
// whose subscription it answers and seq the subscription's number in it (OVL-170).
type signalMsg struct {
	sig Signal
	seq uint64
	inc uint32
}

func (signalMsg) isSysMsg() {}

// receiveSignal handles the signal message m at the head of the system queue by the cell's
// lifecycle state (SUP-320): a Running cell delivers it (deliverSignal); a Suspended cell keeps it,
// in arrival order, until a resume delivers it or the end of the incarnation drops it (SUP-160);
// in Starting, Restarting and Stopping it is recorded as lockstep.drop reason not-running, except a
// Terminated that a delivery to the incarnation that ended last would skip (ACT-630), which is
// discarded without a record. A Stopped cell has closed its system queue, whose drain records the
// signals left in it (SUP-060).
func (c *cell[M]) receiveSignal(m signalMsg) {
	switch c.state {
	case supervise.Running:
		c.deliverSignal(m)
	case supervise.Suspended:
		c.keep(m)
	default:
		if !c.endedWatched.skips(m) {
			c.sys.recordDropNotRunning(c.addr, m.sig)
		}
	}
}

// skips reports whether m is a Terminated that a delivery against the watched set w would skip:
// its Addr is not in w, or is there under another Watch call's sequence number (ACT-630). Such a
// signal answers no watch of the incarnation, so the not-running paths discard it without a
// record, as a delivery would.
func (w *watchedSet) skips(m signalMsg) bool {
	t, ok := m.sig.(Terminated)
	if !ok {
		return false
	}
	seq, in := w.seq(t.Addr)
	return !in || seq != m.seq
}

// deliverSignal delivers the signal message m to the Running incarnation (MSG-810): an
// incarnation that implements SignalReceiver gets the signal in Signal, during the turn; any other
// ignores it without a record.
//
// A Terminated whose Addr is not in the incarnation's watched set, or is there under another Watch
// call's sequence number, is skipped; the address leaves the set when the Terminated is delivered
// (ACT-630). An actor without SignalReceiver consumes the watch the same way, so a later Watch of
// the address registers again. An Overloaded is skipped unless it answers the live subscription:
// the incarnation that made it is the live one, it is still subscribed, and it has not
// unsubscribed and subscribed again since (OVL-170).
func (c *cell[M]) deliverSignal(m signalMsg) {
	sig := m.sig
	switch s := sig.(type) {
	case Terminated:
		if c.ctx.watched.skips(m) {
			return
		}
		c.ctx.watched.remove(s.Addr)
	case Overloaded:
		if m.inc != c.inc || !c.ctx.overloadSub || m.seq != c.ctx.overloadGen {
			return
		}
	}
	c.signal(sig)
}

// signal calls the Running incarnation's Signal with sig as a guarded handler with source signal,
// if the incarnation implements SignalReceiver (MSG-810, SUP-230); otherwise sig is ignored.
func (c *cell[M]) signal(sig Signal) {
	if c.signals == nil {
		return
	}
	c.callHandler(supervise.InSignal, func() { c.signals.Signal(c.ctx, sig) })
}
