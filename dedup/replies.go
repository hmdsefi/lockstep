// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"encoding/json"
	"fmt"
	"time"
)

var repliesKind = kind{name: "Replies", ctor: "NewReplies", tag: "replies"}

// Replies is a bounded cache of the reply to each handled request key, so a retried request gets
// the first reply. Create it with NewReplies and keep it in actor state as a *Replies; it must not
// be copied. K is as for Window; R must encode with encoding/json and decode back to an equal
// reply.
type Replies[K comparable, R any] struct {
	s store[K, R]
}

// NewReplies returns an empty cache bounded by cfg. It panics with the error of cfg.Validate if
// cfg is invalid.
func NewReplies[K comparable, R any](cfg Config) *Replies[K, R] {
	return &Replies[K, R]{s: newStore[K, R](cfg)}
}

// Put stores r as the reply to k at now and returns true if the cache held no reply to k. If it
// already holds one it returns false and keeps the first reply and its age.
func (c *Replies[K, R]) Put(now time.Time, k K, r R) bool {
	e := c.s.begin(repliesKind, now)
	if _, ok := c.s.index[k]; ok { // live: begin removed the expired entries (DLV-130)
		return false
	}
	c.s.add(e, k, r)
	return true
}

// Get returns the reply to k at now and true, or the zero R and false if the cache holds none.
func (c *Replies[K, R]) Get(now time.Time, k K) (R, bool) {
	n, ok := c.s.get(c.s.eff(repliesKind, now), k)
	if !ok {
		var zero R
		return zero, false
	}
	return n.v, true
}

// Forget removes the reply to k, if there is one, so the next request with key k is handled
// again.
func (c *Replies[K, R]) Forget(k K) {
	c.s.usable(repliesKind)
	c.s.remove(k)
}

// Len returns the number of replies in the cache at now.
func (c *Replies[K, R]) Len(now time.Time) int {
	return c.s.count(c.s.eff(repliesKind, now))
}

// Config returns the cache's bounds.
func (c *Replies[K, R]) Config() Config {
	c.s.usable(repliesKind)
	return c.s.cfg
}

// MarshalJSON encodes the cache, its bounds and its entries in write order. It returns an error
// if a key or a reply does not encode.
func (c *Replies[K, R]) MarshalJSON() ([]byte, error) {
	return c.s.marshal(repliesKind, replyField[R])
}

// UnmarshalJSON replaces c with the cache that data encodes, as Window.UnmarshalJSON does.
func (c *Replies[K, R]) UnmarshalJSON(data []byte) error {
	return c.s.unmarshal(repliesKind, data, replyValue[R])
}

// replyField appends `,"r":<reply>`, the encoding/json encoding of r, to an entry (DLV-200).
func replyField[R any](b []byte, r R) ([]byte, error) {
	enc, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	b = append(b, `,"r":`...)
	return append(b, enc...), nil
}

// replyValue decodes the "r" of an entry into an R (DLV-210).
func replyValue[R any](e *rawEntry) (R, error) {
	var r R
	if err := json.Unmarshal(e.R, &r); err != nil {
		return r, fmt.Errorf("reply: %w", err)
	}
	return r, nil
}
