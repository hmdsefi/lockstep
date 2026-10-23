// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"cmp"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/registry"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// Sizer is implemented by messages that report their size in bytes for memory estimates.
type Sizer interface{ Size() int }

// MemSizer is implemented by actors that report the size of their state in bytes.
type MemSizer interface{ MemSize() int }

// ActorEstimate is the memory estimate of one live actor.
type ActorEstimate struct {
	Addr         Address
	MailboxBytes int64
	StateBytes   int64
}

// total is the estimate's size: its mailbox and state bytes.
func (e ActorEstimate) total() int64 { return e.MailboxBytes + e.StateBytes }

// memCount is one actor's estimate (OVL §11): the bytes of the sized messages in its user mailbox
// and the bytes its actor last reported. Every change also changes sum, the system's total, so
// the usage function reads one counter instead of walking the actors. Both counters are atomic:
// senders add to the mailbox's from any goroutine while the turn subtracts, and Estimates reads
// them from any goroutine.
type memCount struct {
	mailbox atomic.Int64
	state   atomic.Int64
	sum     *atomic.Int64 // the system's total
}

// addMailbox adds n, which may be negative, to the mailbox bytes.
func (m *memCount) addMailbox(n int64) {
	if n != 0 {
		m.mailbox.Add(n)
		m.sum.Add(n)
	}
}

// setState makes n the state bytes.
func (m *memCount) setState(n int64) {
	if old := m.state.Swap(n); old != n {
		m.sum.Add(n - old)
	}
}

// sizeOf returns the size to store with msg when it enters a mailbox of this system (OVL-410):
// max(0, Size()) for a message implementing Sizer, and 0 for any other message and while
// estimates are off (OVL-400). It calls user code, so callers hold no runtime lock: a panic in
// Size propagates to the caller, and a lock held then would stay locked.
func (s *System) sizeOf(msg any) int64 {
	if !s.estimates {
		return 0
	}
	if z, ok := msg.(Sizer); ok {
		return int64(max(0, z.Size()))
	}
	return 0
}

// sizeFor is sizeOf for a message to the address to: a message for another node enters no mailbox
// of this system, so it is never sized.
func (s *System) sizeFor(to Address, msg any) int64 {
	if to.node != s.node {
		return 0
	}
	return s.sizeOf(msg)
}

// measure makes the actor's MemSize the incarnation's state bytes (OVL-420), as a guarded call in
// the in-turn window. A failure in MemSize fails the incarnation as a failure of Receive would
// (source receive). Only a Running incarnation is measured, and only while estimates are on.
func (c *cell[M]) measure() {
	c.measured = c.handled
	if c.memSizer == nil || c.state != supervise.Running {
		return
	}
	var n int
	if failed, err := c.callGuarded(func() { n = c.memSizer.MemSize() }); failed {
		c.handlerFailed(supervise.InReceive, err)
		return
	}
	c.mem.setState(int64(max(0, n)))
}

// measureIfRan measures the incarnation when one of its handlers ran since the last measurement:
// after every turn that called actor code (OVL-420).
func (c *cell[M]) measureIfRan() {
	if c.handled != c.measured {
		c.measure()
	}
}

// estimate returns the actor's mailbox and state bytes (actorCell).
func (c *cell[M]) estimate() (mailbox, state int64) { return c.mem.mailbox.Load(), c.mem.state.Load() }

// Estimates returns up to n live actors with the largest estimates, largest first, ties in byte
// order of Addr.String(). It returns nil when n <= 0 or estimates are off.
func (s *System) Estimates(n int) []ActorEstimate {
	if n <= 0 || !s.estimates {
		return nil
	}
	var all []ActorEstimate
	for _, guardian := range []Address{s.user, s.system} {
		all = s.collectEstimates(all, guardian.path)
	}
	slices.SortFunc(all, func(a, b ActorEstimate) int {
		if d := cmp.Compare(b.total(), a.total()); d != 0 {
			return d
		}
		return strings.Compare(a.Addr.String(), b.Addr.String())
	})
	if len(all) > n {
		all = all[:n]
	}
	return all
}

// collectEstimates appends the estimates of the live descendants of parent, in registry order:
// each child, then its own descendants. The registry lists children in spawn order, so the walk is
// the same for the same run (OVL §6); the caller sorts the result. Each address is the registry's,
// not read from the cell, which Spawn may still be setting up.
func (s *System) collectEstimates(all []ActorEstimate, parent string) []ActorEstimate {
	for _, k := range s.reg.Children(parent) {
		if c, ok := s.estimatedCell(k); ok {
			mailbox, state := c.estimate()
			addr := Address{node: k.Node, path: k.Path, uid: k.UID}
			all = append(all, ActorEstimate{Addr: addr, MailboxBytes: mailbox, StateBytes: state})
			all = s.collectEstimates(all, k.Path)
		}
	}
	return all
}

// estimatedCell returns the cell registered as k, and false when it has been removed meanwhile.
func (s *System) estimatedCell(k registry.Addr) (actorCell, bool) {
	uid, a, ok := s.reg.Lookup(k.Path)
	if !ok || uid != k.UID {
		return nil, false
	}
	c, ok := a.(actorCell)
	return c, ok
}
