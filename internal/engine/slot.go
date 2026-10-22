// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package engine

import (
	"strconv"
	"sync/atomic"
)

// SlotState is the scheduling state of one actor cell.
type SlotState uint32

const (
	SlotIdle         SlotState = iota // no turn queued or running
	SlotQueued                        // a turn is queued
	SlotRunning                       // a turn is running
	SlotRunningReady                  // a turn is running and Ready was called during it
)

// String returns "idle", "queued", "running", "running-ready", or "SlotState(<n>)".
func (s SlotState) String() string {
	switch s {
	case SlotIdle:
		return "idle"
	case SlotQueued:
		return "queued"
	case SlotRunning:
		return "running"
	case SlotRunningReady:
		return "running-ready"
	}
	return "SlotState(" + strconv.FormatUint(uint64(s), 10) + ")"
}

// Slot is the scheduling state an engine keeps for one actor cell (§5.1.2). The runtime embeds
// one in every cell and returns it from ActorRef.Slot; only engines call its methods or touch
// Hint. The zero value is SlotIdle. All methods are safe for concurrent use.
type Slot struct {
	state atomic.Uint32

	// Hint is engine-private placement data (SCH: the worker that last ran the actor, plus 1).
	Hint atomic.Uint32
}

// State returns the current state.
func (s *Slot) State() SlotState {
	return SlotState(s.state.Load())
}

// Ready moves idle to queued and returns true; the caller must then queue one turn. In every
// other state it returns false; running becomes running-ready.
func (s *Slot) Ready() bool {
	for {
		old := s.state.Load()
		switch SlotState(old) {
		case SlotIdle:
			if s.state.CompareAndSwap(old, uint32(SlotQueued)) {
				return true
			}
		case SlotRunning:
			// End may finish the turn first: then the retry finds idle and queues a turn.
			if s.state.CompareAndSwap(old, uint32(SlotRunningReady)) {
				return false
			}
		default:
			// Queued or running-ready: a turn that starts after this call is already promised.
			return false
		}
	}
}

// Begin moves queued to running. It panics in any other state.
func (s *Slot) Begin() {
	for {
		old := s.state.Load()
		if SlotState(old) != SlotQueued {
			panic("engine: Slot.Begin in state " + SlotState(old).String())
		}
		if s.state.CompareAndSwap(old, uint32(SlotRunning)) {
			return
		}
	}
}

// End finishes a turn. If more is true or the state is running-ready, it moves to queued and
// returns true; the caller must then queue one more turn. Otherwise it moves running to idle and
// returns false. It panics unless the state is running or running-ready.
func (s *Slot) End(more bool) bool {
	for {
		old := s.state.Load()
		next := SlotQueued
		switch SlotState(old) {
		case SlotRunning:
			if !more {
				next = SlotIdle
			}
		case SlotRunningReady:
		default:
			panic("engine: Slot.End in state " + SlotState(old).String())
		}
		// A Ready that moves running to running-ready between the Load and here makes the swap
		// fail; the retry then queues the turn that Ready asked for.
		if s.state.CompareAndSwap(old, uint32(next)) {
			return next == SlotQueued
		}
	}
}
