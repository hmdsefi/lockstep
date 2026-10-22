// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

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
// never go through Send.
type signalMsg struct{ sig Signal }

func (signalMsg) isSysMsg() {}

// pushSignal appends sig to the system queue and schedules the cell (ENG-350). Signals run before
// every user message (ENG-310 step 1). Plan 1.3's supervision and 1.2-10's Watch push signals
// here; neither applies SUP-320's per-state rules before the push.
func (c *cell[M]) pushSignal(sig Signal) { c.pushSystem(signalMsg{sig: sig}, nil) }

// deliverSignal handles a signal at the head of the system queue (MSG-810). An incarnation that
// implements SignalReceiver gets it in Signal, during a turn; any other ignores it without a
// record. Only a running incarnation delivers: the other lifecycle states keep or drop signals by
// SUP-320's rules, which arrive with 1.2-13.
//
// A Terminated whose Addr is not in the incarnation's watched set is skipped, and the address
// leaves the set when the Terminated is delivered (ACT-630). An actor without SignalReceiver
// consumes the watch the same way, so a later Watch of the address registers again.
func (c *cell[M]) deliverSignal(sig Signal) {
	if c.state != cellRunning {
		return
	}
	if t, ok := sig.(Terminated); ok && !c.ctx.watched.remove(t.Addr) {
		return
	}
	if c.signals == nil {
		return
	}
	c.enter()
	defer c.leave()
	c.signals.Signal(c.ctx, sig)
}
