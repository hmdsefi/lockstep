// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package overload holds the parts of overload protection every engine shares: the actor cap, the
// admission gate and the hysteresis switch (OVL §4.2). It uses the standard library and
// internal/engine only, starts no goroutine and reads no clock.
package overload

import "sync/atomic"

// Cap counts live actors against a limit with a reserve (OVL-010, OVL-030). It is safe for
// concurrent use and holds one atomic word.
type Cap struct {
	limit   int
	reserve int
	live    atomic.Int64
}

// reserveOf returns the reserve of a limit (OVL §3): limit/100, at least 1 when limit >= 2, and 0
// otherwise.
func reserveOf(limit int) int {
	if limit < 2 {
		return 0
	}
	return max(limit/100, 1)
}

// NewCap returns a Cap with no live actor. A limit below 1 admits nothing; the caller validates
// the limit (OVL-010).
func NewCap(limit int) *Cap {
	return &Cap{limit: limit, reserve: reserveOf(limit)}
}

// Acquire takes one slot. A normal spawn needs live < limit-reserve; a reserve spawn needs
// live < limit (OVL-030). It reports the counts it decided on: live is the count it compared,
// before taking the slot.
func (c *Cap) Acquire(reserve bool) (ok bool, live, limit, rsv int) {
	room := c.limit - c.reserve
	if reserve {
		room = c.limit
	}
	for {
		n := c.live.Load()
		if n >= int64(room) {
			return false, int(n), c.limit, c.reserve
		}
		if c.live.CompareAndSwap(n, n+1) {
			return true, int(n), c.limit, c.reserve
		}
	}
}

// Release returns one slot. The count never goes below zero.
func (c *Cap) Release() {
	for {
		n := c.live.Load()
		if n <= 0 || c.live.CompareAndSwap(n, n-1) {
			return
		}
	}
}
