// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package orders is an order service written as lockstep actors. Orders creates, charges and
// cancels orders; Checkout screens an order and asks Orders to create it, and asks again when
// the ask times out.
//
// lockstep delivers each message at most once and never reorders the messages of one sender to
// one receiver, but a retried ask delivers the same Create twice, and a Cancel that travels a
// shorter route than its Create arrives first. Orders handles both: the order ID is an
// idempotency key, and a Cancel for an unknown order leaves a tombstone. The Bugs switches turn
// each defense off, so the simulation tests can show the bug it prevents.
package orders

import (
	"fmt"
	"strings"

	"github.com/hmdsefi/lockstep"
)

// Status is the state of an order.
type Status uint8

const (
	Pending   Status = iota + 1 // the charge is in flight
	Created                     // charged
	Cancelled                   // cancelled, and refunded if it was charged
	Failed                      // the charge failed
)

// String returns "pending", "created", "cancelled", "failed", or "status(<n>)".
func (s Status) String() string {
	switch s {
	case Pending:
		return "pending"
	case Created:
		return "created"
	case Cancelled:
		return "cancelled"
	case Failed:
		return "failed"
	}
	return fmt.Sprintf("status(%d)", uint8(s))
}

// Result answers a Create.
type Result struct {
	ID     string
	Status Status // Created, Cancelled or Failed
}

// Msg is the message type of Orders.
type Msg interface{ isOrdersMsg() }

// Create creates order ID and charges Amount for it. ID is the idempotency key: a Create for an
// ID that Orders has seen charges nothing and answers with the order's status.
type Create struct {
	ID     string
	Amount int
	Reply  lockstep.ReplyTo[Result]
}

// Cancel cancels order ID and refunds it if it was charged. A Cancel for an ID that Orders has
// not seen leaves a tombstone, so a Create that arrives later is cancelled, not charged.
type Cancel struct {
	ID string
}

// charged is the result of the Charge effect.
type charged struct {
	id  string
	err error
}

// refunded is the result of the Refund effect.
type refunded struct {
	id  string
	err error
}

func (Create) isOrdersMsg()   {}
func (Cancel) isOrdersMsg()   {}
func (charged) isOrdersMsg()  {}
func (refunded) isOrdersMsg() {}

// Bugs is a bitmask of planted bugs. The zero value plants none.
type Bugs uint8

const (
	// BugNoDedup charges every Create, even a retry of an order that Orders has seen.
	BugNoDedup Bugs = 1 << iota
	// BugDropEarlyCancel ignores a Cancel for an order that Orders has not seen.
	BugDropEarlyCancel
)

// allBugs is the union of the defined bits.
const allBugs = BugNoDedup | BugDropEarlyCancel

// String returns the names of the set bits, such as "no-dedup", joined by "|", or "none".
func (b Bugs) String() string {
	var names []string
	if b&BugNoDedup != 0 {
		names = append(names, "no-dedup")
	}
	if b&BugDropEarlyCancel != 0 {
		names = append(names, "drop-early-cancel")
	}
	if undefined := b &^ allBugs; undefined != 0 {
		names = append(names, fmt.Sprintf("0x%x", uint8(undefined)))
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, "|")
}

// ParseBugs returns the Bugs that String names. It accepts "" and "none" as zero.
func ParseBugs(s string) (Bugs, error) {
	var b Bugs
	if s == "" || s == "none" {
		return 0, nil
	}
	for name := range strings.SplitSeq(s, "|") {
		switch name {
		case "no-dedup":
			b |= BugNoDedup
		case "drop-early-cancel":
			b |= BugDropEarlyCancel
		default:
			return 0, fmt.Errorf("orders: unknown bug %q: want no-dedup or drop-early-cancel", name)
		}
	}
	return b, nil
}

// order is what Orders keeps per order ID. A record with status Cancelled and no amount is a
// tombstone.
type order struct {
	amount  int
	status  Status
	cancel  bool                       // a Cancel arrived while the charge was pending
	waiters []lockstep.ReplyTo[Result] // Creates to answer when the charge completes
}

// Orders is the order service actor. It keeps every order ID it has seen, so its state grows
// with the number of orders; a long-running service prunes old records or keeps them in a
// persistent actor.
type Orders struct {
	api  Payments
	bugs Bugs
	// orders holds the state of both defenses: its keys are the idempotency keys seen so far,
	// and a Cancelled record with no amount is a tombstone.
	orders map[string]*order
}

// New returns a factory of Orders that charges through api (nil in simulation) with bugs
// planted.
func New(api Payments, bugs Bugs) func() lockstep.Actor[Msg] {
	return func() lockstep.Actor[Msg] {
		return &Orders{api: api, bugs: bugs, orders: map[string]*order{}}
	}
}

// Receive handles one message.
func (s *Orders) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	switch m := msg.(type) {
	case Create:
		s.create(ctx, m)
	case charged:
		s.charged(ctx, m)
	case Cancel:
		s.cancel(ctx, m)
	case refunded:
		// A failed refund needs an operator or a retry of its own; this example stops here.
	}
}

// create charges a new order, or answers a Create for a known ID from its record.
func (s *Orders) create(ctx *lockstep.Context[Msg], m Create) {
	if o, seen := s.orders[m.ID]; seen && s.bugs&BugNoDedup == 0 {
		// A retry, or a Create after its Cancel: answer from the record, never charge again.
		if o.status == Pending {
			o.waiters = append(o.waiters, m.Reply)
			return
		}
		m.Reply.Send(ctx, Result{ID: m.ID, Status: o.status})
		return
	}
	s.orders[m.ID] = &order{amount: m.Amount, status: Pending, waiters: []lockstep.ReplyTo[Result]{m.Reply}}
	id := m.ID
	lockstep.Do(ctx, Charge{API: s.api, Order: id, Amount: m.Amount}, func(_ struct{}, err error) Msg {
		return charged{id: id, err: err}
	})
}

// charged settles a charge and answers every Create that waits for it.
func (s *Orders) charged(ctx *lockstep.Context[Msg], m charged) {
	o := s.orders[m.id]
	switch {
	case m.err != nil:
		o.status = Failed
	case o.cancel:
		o.status = Cancelled
		s.refund(ctx, m.id, o.amount)
	default:
		o.status = Created
	}
	for _, r := range o.waiters {
		r.Send(ctx, Result{ID: m.id, Status: o.status})
	}
	o.waiters = nil
}

// cancel cancels an order, or leaves a tombstone for one it has not seen.
func (s *Orders) cancel(ctx *lockstep.Context[Msg], m Cancel) {
	o, seen := s.orders[m.ID]
	switch {
	case !seen && s.bugs&BugDropEarlyCancel != 0:
		// The Create has not arrived yet, and this Cancel is lost.
	case !seen:
		s.orders[m.ID] = &order{status: Cancelled} // tombstone
	case o.status == Pending:
		o.cancel = true // refund once the charge completes
	case o.status == Created:
		o.status = Cancelled
		s.refund(ctx, m.ID, o.amount)
	}
}

// refund starts the Refund effect for an order.
func (s *Orders) refund(ctx *lockstep.Context[Msg], id string, amount int) {
	lockstep.Do(ctx, Refund{API: s.api, Order: id, Amount: amount}, func(_ struct{}, err error) Msg {
		return refunded{id: id, err: err}
	})
}
