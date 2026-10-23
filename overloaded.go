// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"strconv"
	"sync"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// OverloadReason is the source of an admission change. Its values equal those of
// engine.AdmissionSource.
type OverloadReason uint8

const (
	OverloadMemory   OverloadReason = iota + 1 // production live heap
	OverloadBudget                             // simulation count budget (sim.Options.Budget)
	OverloadPressure                           // simulation memory-pressure fault
)

// String returns "memory", "budget", "pressure", or "OverloadReason(<n>)".
func (r OverloadReason) String() string {
	switch r {
	case OverloadMemory, OverloadBudget, OverloadPressure:
		return engine.AdmissionSource(r).String()
	}
	return "OverloadReason(" + strconv.FormatUint(uint64(r), 10) + ")"
}

// Overloaded is the signal sent to subscribed actors when their System starts or stops shedding.
// It implements Signal.
type Overloaded struct {
	Active bool           // true: admission stopped; false: admission resumed
	Reason OverloadReason // the source whose change caused this transition
	Live   uint64         // measured bytes at the change (OverloadBudget: the estimate); 0 for OverloadPressure
	High   uint64         // thresholds in force (OverloadBudget: the byte budget and its resume mark); 0 for OverloadPressure
	Low    uint64
}

func (Overloaded) signal() {}

// SubscribeOverload subscribes the current incarnation to Overloaded signals. Subscribing again
// has no effect. If the System is shedding, an Overloaded{Active: true} signal is queued at once.
func (c *Context[M]) SubscribeOverload() {
	c.mustInTurn()
	cl := c.cell
	// Called from Stopped, the incarnation's subscription has ended already (OVL-170).
	if cl.ending() || c.overloadSub {
		return
	}
	c.overloadSub = true
	c.overloadGen++
	cl.sys.subscribeOverload(cl, c.inc, c.overloadGen)
}

// UnsubscribeOverload cancels the subscription of the current incarnation.
func (c *Context[M]) UnsubscribeOverload() {
	c.mustInTurn()
	if !c.overloadSub {
		return
	}
	c.overloadSub = false
	c.cell.sys.unsubscribeOverload(c.cell, c.inc)
}

// overloadSub is one subscription: the incarnation inc of cell (OVL-170). gen numbers the
// incarnation's subscriptions (Context.overloadGen); the signals queued for it are tagged with it, so a
// signal of an earlier subscription is dropped on delivery after the incarnation subscribes again.
type overloadSub struct {
	cell actorCell
	inc  uint32
	gen  uint64
}

// overloadSubs are the System's subscriptions, in subscription order (OVL-180).
//
// Lock order: deliver, then mu or the admission state's lock (both leaves), then, still under
// deliver, the queue locks that pushSystem takes. Nothing that holds a cell's lock or a queue's
// takes deliver, and the end of an incarnation takes only mu.
type overloadSubs struct {
	// deliver serializes whatever decides who gets which signal with the pushes of those signals:
	// a change of the combined state (admissionChanged) and a subscription. Without it a
	// subscription made while a change is handled could get the signal twice, or its Active: true
	// after the Active: false of the end of the period (OVL-190).
	deliver sync.Mutex

	mu   sync.Mutex // guards list
	list []overloadSub
}

// snapshot returns the subscriptions at this moment, in order.
func (o *overloadSubs) snapshot() []overloadSub {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]overloadSub(nil), o.list...)
}

// remove deletes the subscription of incarnation inc of c, if there is one.
func (o *overloadSubs) remove(c actorCell, inc uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i, sub := range o.list {
		if sub.cell == c && sub.inc == inc {
			o.list = append(o.list[:i], o.list[i+1:]...)
			return
		}
	}
}

// subscribeOverload adds incarnation inc of c to the subscribers and, while the system sheds,
// queues Overloaded{Active: true} with the values of the change that started the period, ahead of
// any later signal (OVL-190). It queues the signal of a change to a subscriber that the change
// finds in the list, so the subscription and the change are ordered by deliver.
func (s *System) subscribeOverload(c actorCell, inc uint32, gen uint64) {
	s.ovl.deliver.Lock()
	defer s.ovl.deliver.Unlock()
	s.ovl.mu.Lock()
	s.ovl.list = append(s.ovl.list, overloadSub{cell: c, inc: inc, gen: gen})
	s.ovl.mu.Unlock()
	if start, shedding := s.adm.shedPeriod(); shedding {
		c.pushSystem(signalMsg{sig: overloadedBy(start, true), seq: gen, inc: inc}, c)
	}
}

// unsubscribeOverload removes the subscription of incarnation inc of c. A signal already queued
// for it is not delivered (deliverSignal).
func (s *System) unsubscribeOverload(c actorCell, inc uint32) { s.ovl.remove(c, inc) }

// signalOverload queues Overloaded for change ch, which changed the combined state to
// ch.Shedding, to every subscriber, in subscription order (OVL-180). The signal carries the
// subscription's incarnation and generation, so one that outlives its subscription is dropped on
// delivery (OVL-170). The caller holds s.ovl.deliver.
func (s *System) signalOverload(ch engine.AdmissionChange) {
	sig := overloadedBy(ch, ch.Shedding)
	for _, sub := range s.ovl.snapshot() {
		sub.cell.pushSystem(signalMsg{sig: sig, seq: sub.gen, inc: sub.inc}, nil)
	}
}

// overloadedBy returns the signal for the change ch of source with the new combined state active.
func overloadedBy(ch engine.AdmissionChange, active bool) Overloaded {
	return Overloaded{
		Active: active,
		Reason: OverloadReason(ch.Source),
		Live:   ch.Live,
		High:   ch.High,
		Low:    ch.Low,
	}
}
