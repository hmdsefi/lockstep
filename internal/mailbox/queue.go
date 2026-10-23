// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package mailbox holds the two queues of an actor: the bounded user Queue with its full-mailbox
// policy, and the unbounded SystemQueue for lifecycle commands and signals (MBX-030 to MBX-080).
//
// The package starts no goroutine and uses no channel or select statement, so it passes the
// determinism lint (MBX-080).
package mailbox

import (
	"strconv"
	"sync"
)

// DefaultCapacity is the user mailbox capacity when neither WithMailbox nor WithDefaultMailbox
// is given.
const DefaultCapacity = 1024

// Policy has the same values, in the same order, as lockstep.MailboxPolicy.
type Policy uint8

// The full-mailbox policies.
const (
	FailSend Policy = iota
	DropNewest
	DropOldest
	BlockSender
)

// Kind classifies an entry for capacity accounting.
type Kind uint8

// The entry kinds.
const (
	Sent Kind = iota + 1 // counts toward capacity; subject to the policy
	Self                 // runtime self delivery; never refused, never counted
)

// Outcome is the result of offering an entry.
type Outcome uint8

// The offer outcomes.
const (
	Enqueued      Outcome = iota + 1 // the entry is in the queue
	Rejected                         // full; FailSend, or BlockSender without waiting
	DroppedNewest                    // full; the offered entry was dropped (DropNewest)
	DroppedOldest                    // full; the oldest Sent entry was dropped, the offered one enqueued
	Closed                           // the queue is closed; the caller dead-letters the entry
)

// String returns "enqueued", "rejected", "dropped-newest", "dropped-oldest", "closed", or
// "Outcome(<n>)".
func (o Outcome) String() string {
	switch o {
	case Enqueued:
		return "enqueued"
	case Rejected:
		return "rejected"
	case DroppedNewest:
		return "dropped-newest"
	case DroppedOldest:
		return "dropped-oldest"
	case Closed:
		return "closed"
	}
	return "Outcome(" + strconv.Itoa(int(o)) + ")"
}

// Entry is one queued value and its kind.
type Entry[T any] struct {
	V    T
	Kind Kind
}

// minRing is the length of a queue's first ring. The ring is allocated by the first entry, so an
// idle actor's queue costs nothing.
const minRing = 4

// slot is one position of the ring. A removed slot was dropped by DropOldest and is skipped by
// Pop; it keeps its position until it reaches the head or the ring compacts.
type slot[T any] struct {
	v       T
	kind    Kind
	removed bool
}

// Queue is a user mailbox: FIFO, many producers, one consumer. Offer, OfferWait, Recheck, Len,
// SentLen and Close are safe for concurrent use; Pop is called only by the owning actor's turn.
type Queue[T any] struct {
	mu       sync.Mutex
	capacity int
	policy   Policy

	cond    sync.Cond    // signalled when a waiter's state changes or its context ends; L is &mu
	waiters []*waiter[T] // senders waiting in OfferWait, in the order they started waiting

	ring    []slot[T] // growable ring; the used slots are ring[head : head+used) modulo len(ring)
	head    int
	used    int // slots in use, removed ones included
	removed int // removed slots among the used ones
	sent    int // live Sent entries
	closed  bool
}

// New returns an open, empty queue. It panics if capacity < 1 or policy is unknown.
func New[T any](capacity int, policy Policy) *Queue[T] {
	if capacity < 1 {
		panic("mailbox: invalid capacity " + strconv.Itoa(capacity))
	}
	if policy > BlockSender {
		panic("mailbox: unknown policy " + strconv.Itoa(int(policy)))
	}
	q := &Queue[T]{capacity: capacity, policy: policy}
	q.cond.L = &q.mu
	return q
}

// Capacity returns the capacity passed to New.
func (q *Queue[T]) Capacity() int { return q.capacity }

// Policy returns the policy passed to New.
func (q *Queue[T]) Policy() Policy { return q.policy }

// Offer adds v without waiting (MBX-030 to MBX-060). forceFull makes a Sent entry take the full
// branch even when there is room (shedding, OVL). For BlockSender, Offer behaves as FailSend.
// When the outcome is DroppedOldest, dropped is the removed entry; otherwise it is the zero T.
func (q *Queue[T]) Offer(v T, kind Kind, forceFull bool) (out Outcome, dropped T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.offer(v, kind, forceFull)
}

// offer is Offer with q.mu held.
func (q *Queue[T]) offer(v T, kind Kind, forceFull bool) (out Outcome, dropped T) {
	if q.closed {
		return Closed, dropped
	}
	if kind == Self {
		q.push(v, Self)
		return Enqueued, dropped
	}
	if !forceFull && q.sent < q.capacity {
		q.push(v, Sent)
		q.sent++
		return Enqueued, dropped
	}
	switch q.policy {
	case DropNewest:
		return DroppedNewest, dropped
	case DropOldest:
		if q.sent == 0 {
			return DroppedNewest, dropped
		}
		dropped = q.dropOldest()
		q.push(v, Sent)
		q.sent++
		return DroppedOldest, dropped
	}
	return Rejected, dropped
}

// Pop removes and returns the oldest entry. ok is false if the queue is empty. It takes the queue
// mutex because producers run concurrently. When it removes a Sent entry it hands the freed slot to
// the first waiting sender if that sender is admitted (MBX-070).
func (q *Queue[T]) Pop() (v T, kind Kind, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.skipRemoved()
	if q.used == 0 {
		return v, 0, false
	}
	s := &q.ring[q.head]
	v, kind = s.v, s.kind
	*s = slot[T]{}
	q.head = (q.head + 1) % len(q.ring)
	q.used--
	q.skipRemoved()
	if kind == Sent {
		q.sent--
		q.handOff()
	}
	return v, kind, true
}

// Len returns the number of entries.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used - q.removed
}

// SentLen returns the number of Sent entries.
func (q *Queue[T]) SentLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.sent
}

// Close closes the queue and returns the remaining entries in queue order with their kinds.
// Waiting senders return Closed. Later offers return Closed. Close on a closed queue returns nil.
func (q *Queue[T]) Close() []Entry[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	rest := make([]Entry[T], 0, q.used-q.removed)
	for i := 0; i < q.used; i++ {
		s := &q.ring[(q.head+i)%len(q.ring)]
		if !s.removed {
			rest = append(rest, Entry[T]{V: s.v, Kind: s.kind})
		}
	}
	q.ring, q.head, q.used, q.removed, q.sent = nil, 0, 0, 0, 0
	q.closeWaiters()
	return rest
}

// push appends an entry at the tail, growing the ring when it is full. The caller holds q.mu.
func (q *Queue[T]) push(v T, kind Kind) {
	if q.used == len(q.ring) {
		q.grow()
	}
	q.ring[(q.head+q.used)%len(q.ring)] = slot[T]{v: v, kind: kind}
	q.used++
}

// grow doubles the ring, laying the used slots out from index 0.
func (q *Queue[T]) grow() {
	n := len(q.ring) * 2
	if n < minRing {
		n = minRing
	}
	ring := make([]slot[T], n)
	for i := 0; i < q.used; i++ {
		ring[i] = q.ring[(q.head+i)%len(q.ring)]
	}
	q.ring, q.head = ring, 0
}

// dropOldest marks the first live Sent slot removed and returns its value. The caller holds q.mu
// and has checked that a Sent entry exists. Self entries before it keep their positions. The
// caller adds the entry that replaces it, so sent stays as it is.
func (q *Queue[T]) dropOldest() T {
	for i := 0; i < q.used; i++ {
		s := &q.ring[(q.head+i)%len(q.ring)]
		if s.removed || s.kind != Sent {
			continue
		}
		v := s.v
		*s = slot[T]{removed: true}
		q.removed++
		q.sent--
		q.skipRemoved()
		if q.removed*2 > q.used {
			q.compact()
		}
		return v
	}
	panic("mailbox: Sent entry count out of step with the ring")
}

// skipRemoved releases the removed slots at the head.
func (q *Queue[T]) skipRemoved() {
	for q.removed > 0 && q.ring[q.head].removed {
		q.ring[q.head] = slot[T]{}
		q.head = (q.head + 1) % len(q.ring)
		q.used--
		q.removed--
	}
}

// compact squeezes the removed slots out of the ring in place, keeping the order of the live ones.
func (q *Queue[T]) compact() {
	n := len(q.ring)
	w := 0
	for r := 0; r < q.used; r++ {
		s := q.ring[(q.head+r)%n]
		if s.removed {
			continue
		}
		q.ring[(q.head+w)%n] = s
		w++
	}
	for i := w; i < q.used; i++ {
		q.ring[(q.head+i)%n] = slot[T]{}
	}
	q.used, q.removed = w, 0
}
