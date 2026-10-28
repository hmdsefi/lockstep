// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"errors"
	"math"
	"time"
)

// kind names one of the three value types in panics, errors and the encoding.
type kind struct {
	name string // Window
	ctor string // NewWindow
	tag  string // window, the encoding's "type"
}

// unusable is the error of a method called on a zero value (DLV-100).
func (k kind) unusable() error {
	return errors.New("dedup: " + k.name + " used before " + k.ctor + " or UnmarshalJSON")
}

// The range of time.Time.UnixNano (DLV-030).
var (
	minNow = time.Unix(0, math.MinInt64)
	maxNow = time.Unix(0, math.MaxInt64)
)

// node is one entry, linked in write order.
type node[K comparable, V any] struct {
	k          K
	v          V
	at         int64 // write time, nanoseconds since the Unix epoch
	prev, next *node[K, V]
}

// store is the state Window, Replies and Versions share: bounds, last and the entries, indexed by
// key and linked in write order (head oldest). A store with cfg.Capacity == 0 is a zero value
// (DLV-100). No method ranges over index, so map order reaches no result (DLV-300).
type store[K comparable, V any] struct {
	cfg        Config
	last       int64
	index      map[K]*node[K, V]
	head, tail *node[K, V]
}

// newStore returns an empty store bounded by cfg, or panics with cfg's Validate error (DLV-020).
func newStore[K comparable, V any](cfg Config) store[K, V] {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return store[K, V]{cfg: cfg, last: math.MinInt64, index: make(map[K]*node[K, V])}
}

// usable panics if s is a zero value (DLV-100).
func (s *store[K, V]) usable(k kind) {
	if s.cfg.Capacity == 0 {
		panic(k.unusable())
	}
}

// nanos returns now.UnixNano(), or panics if now lies outside its range (DLV-030).
func nanos(now time.Time) int64 {
	if now.Before(minNow) || now.After(maxNow) {
		panic(errors.New("dedup: now " + now.UTC().Format(time.RFC3339Nano) + " is outside the range of UnixNano"))
	}
	return now.UnixNano()
}

// eff returns the effective now for a call given now (DLV-040 steps 1 to 3). It changes nothing.
func (s *store[K, V]) eff(k kind, now time.Time) int64 {
	s.usable(k)
	return max(nanos(now), s.last)
}

// begin starts a write call at now: it computes the effective now, sets last to it and removes
// every entry that is no longer live (DLV-040, DLV-060). It returns the effective now.
func (s *store[K, V]) begin(k kind, now time.Time) int64 {
	e := s.eff(k, now)
	s.last = e
	for s.head != nil && !s.live(e, s.head.at) {
		s.unlink(s.head)
	}
	return e
}

// live reports whether an entry written at at is live at effective now e (DLV-050). Since
// at <= e, the uint64 difference is the true age, which an int64 difference could overflow.
func (s *store[K, V]) live(e, at int64) bool {
	return s.cfg.TTL == 0 || uint64(e)-uint64(at) < uint64(s.cfg.TTL) //nolint:gosec // G115: at <= e, so the uint64 difference is the true age (DLV-050)
}

// get returns k's entry if it is live at e.
func (s *store[K, V]) get(e int64, k K) (*node[K, V], bool) {
	n, ok := s.index[k]
	if !ok || !s.live(e, n.at) {
		return nil, false
	}
	return n, true
}

// add appends a new entry for k, which has none, at e, removing the oldest entry first if the
// store is full (DLV-070). Call it after begin.
func (s *store[K, V]) add(e int64, k K, v V) {
	if len(s.index) >= s.cfg.Capacity {
		s.unlink(s.head)
	}
	n := &node[K, V]{k: k, v: v, at: e}
	s.index[k] = n
	s.append(n)
}

// moveLast sets n's write time to e and places it last (DLV-060).
func (s *store[K, V]) moveLast(e int64, n *node[K, V]) {
	s.detach(n)
	n.at = e
	s.append(n)
}

// remove removes k's entry, live or not, if there is one (DLV-090).
func (s *store[K, V]) remove(k K) {
	if n, ok := s.index[k]; ok {
		s.unlink(n)
	}
}

// count returns the number of entries live at e without removing any (DLV-160). Entries are in
// at order, so the expired ones come first.
func (s *store[K, V]) count(e int64) int {
	expired := 0
	for n := s.head; n != nil && !s.live(e, n.at); n = n.next {
		expired++
	}
	return len(s.index) - expired
}

// unlink removes n from the list and the index.
func (s *store[K, V]) unlink(n *node[K, V]) {
	s.detach(n)
	delete(s.index, n.k)
}

// detach takes n out of the list.
func (s *store[K, V]) detach(n *node[K, V]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		s.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		s.tail = n.prev
	}
	n.prev, n.next = nil, nil
}

// append places n last in the list.
func (s *store[K, V]) append(n *node[K, V]) {
	n.prev = s.tail
	if s.tail != nil {
		s.tail.next = n
	} else {
		s.head = n
	}
	s.tail = n
}
