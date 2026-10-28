// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package orders is an order service written as lockstep actors. Orders creates, charges and
// cancels orders; Checkout screens an order and asks Orders to create it, and asks again when
// the ask times out.
//
// lockstep delivers each message at most once and never reorders the messages of one sender to
// one receiver, but a retried ask delivers the same Create twice, and a Cancel that travels a
// shorter route than its Create arrives first. Orders handles both with package dedup. The order
// ID is an idempotency key: a retry is answered from the order's record while its charge is in
// flight, and from a dedup.Replies of first results after it ended. A Cancel for an unknown order
// leaves a tombstone in a dedup.Window. The Bugs switches turn each defense off, so the
// simulation tests can show the bug it prevents.
package orders

import (
	"fmt"
	"strings"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/dedup"
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
// ID that Orders is charging, or that it settled recently (see New), charges nothing and answers
// with the order's status.
type Create struct {
	ID     string
	Amount int
	Reply  lockstep.ReplyTo[Result]
}

// Cancel cancels order ID and refunds it if it was charged. A Cancel for an ID that Orders has no
// record of leaves a tombstone for a while (see New), so a Create that arrives later is cancelled,
// not charged.
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

// order is the entity state of a live order: Pending while the charge is in flight, Created once
// it succeeded. Orders deletes the record when the order ends Failed or Cancelled.
type order struct {
	amount  int
	status  Status
	cancel  bool                       // a Cancel arrived while the charge was pending
	waiters []lockstep.ReplyTo[Result] // Creates to answer when the charge completes
}

// Orders is the order service actor. It keeps its state in three places:
//
//   - orders holds the entity state of live orders, one record per order from its Create until it
//     ends Failed or Cancelled. A Created order keeps its record until a Cancel refunds it,
//     because the refund needs its amount.
//   - settled, a dedup.Replies, holds the first Result of each order whose charge has ended, so a
//     retried Create gets that Result for as long as settled keeps it.
//   - tombs, a dedup.Window, holds the ID of each order cancelled before its Create arrived.
//
// settled and tombs forget old entries, so they stay bounded. orders grows with the orders that
// are created and not cancelled: a service of real size keeps them in a database or a persistent
// actor.
type Orders struct {
	api     Payments
	bugs    Bugs
	orders  map[string]*order
	settled *dedup.Replies[string, Result]
	tombs   *dedup.Window[string]
}

// New returns a factory of Orders that charges through api (nil in simulation) with bugs
// planted.
//
// settled and tombs share one dedup.Config. Its TTL must be longer than the time in which a copy
// of a request can still arrive. Checkout, as the tests configure it, makes three attempts with
// a timeout of 100 ms and no wait between them, so its last Create is sent 2 x 100 ms after the
// first and takes about 5 ms to arrive: a retry arrives within about 205 ms of the first Create.
// A Cancel can reach Orders before the first Create is even sent, by at most the fraud check
// (50 ms), so a tombstone must last about 260 ms. A TTL of one second covers both. Capacity must
// hold every ID written within one TTL: here, 1024 orders per second.
func New(api Payments, bugs Bugs) func() lockstep.Actor[Msg] {
	cfg := dedup.Config{Capacity: 1024, TTL: time.Second}
	return func() lockstep.Actor[Msg] {
		return &Orders{
			api:     api,
			bugs:    bugs,
			orders:  map[string]*order{},
			settled: dedup.NewReplies[string, Result](cfg),
			tombs:   dedup.NewWindow[string](cfg),
		}
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

// create charges a new order. It answers a retry, and a Create that follows its Cancel, without
// charging.
func (s *Orders) create(ctx *lockstep.Context[Msg], m Create) {
	if r, ok := s.settled.Get(ctx.Now(), m.ID); ok && s.bugs&BugNoDedup == 0 {
		m.Reply.Send(ctx, r) // a retry of an order that has ended: the first Result
		return
	}
	if o, ok := s.orders[m.ID]; ok && s.bugs&BugNoDedup == 0 {
		// A retry of a live order: wait for its charge, or answer Created.
		if o.status == Pending {
			o.waiters = append(o.waiters, m.Reply)
			return
		}
		m.Reply.Send(ctx, Result{ID: m.ID, Status: o.status})
		return
	}
	if s.tombs.Contains(ctx.Now(), m.ID) {
		// The Cancel came first: refuse the Create and charge nothing.
		m.Reply.Send(ctx, Result{ID: m.ID, Status: Cancelled})
		return
	}
	s.orders[m.ID] = &order{amount: m.Amount, status: Pending, waiters: []lockstep.ReplyTo[Result]{m.Reply}}
	id := m.ID
	lockstep.Do(ctx, Charge{API: s.api, Order: id, Amount: m.Amount}, func(_ struct{}, err error) Msg {
		return charged{id: id, err: err}
	})
}

// charged settles a charge, keeps the Result for retries and answers every Create that waits for
// it. A Failed or Cancelled order has nothing left to refund, so its record goes.
func (s *Orders) charged(ctx *lockstep.Context[Msg], m charged) {
	o, ok := s.orders[m.id]
	if !ok {
		return // a second charge of an order that already ended: only BugNoDedup starts one
	}
	switch {
	case m.err != nil:
		o.status = Failed
	case o.cancel:
		o.status = Cancelled
		s.refund(ctx, m.id, o.amount)
	default:
		o.status = Created
	}
	res := Result{ID: m.id, Status: o.status}
	s.settled.Put(ctx.Now(), m.id, res)
	if o.status != Created {
		delete(s.orders, m.id)
	}
	for _, r := range o.waiters {
		r.Send(ctx, res)
	}
	o.waiters = nil
}

// cancel cancels an order, or leaves a tombstone for one it has no record of.
func (s *Orders) cancel(ctx *lockstep.Context[Msg], m Cancel) {
	o, seen := s.orders[m.ID]
	switch {
	case !seen && s.bugs&BugDropEarlyCancel != 0:
		// The Create has not arrived yet, and this Cancel is lost.
	case !seen:
		s.tombs.Add(ctx.Now(), m.ID)
	case o.status == Pending:
		o.cancel = true // refund once the charge completes
	case o.status == Created:
		delete(s.orders, m.ID)
		s.refund(ctx, m.ID, o.amount)
	}
}

// refund starts the Refund effect for an order.
func (s *Orders) refund(ctx *lockstep.Context[Msg], id string, amount int) {
	lockstep.Do(ctx, Refund{API: s.api, Order: id, Amount: amount}, func(_ struct{}, err error) Msg {
		return refunded{id: id, err: err}
	})
}
