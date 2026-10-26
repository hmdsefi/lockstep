// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"sync"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// RunningTurn describes a turn in progress, for the watchdog (OVL).
type RunningTurn struct {
	Worker int             // worker index
	GoID   uint64          // goroutine ID of the worker, as printed by runtime.Stack
	Actor  engine.ActorRef // actor whose turn is running
	Start  time.Time       // when the turn started (monotonic reading included)
}

// RunningTurns appends one RunningTurn per worker that is inside RunTurn and returns the
// extended slice. The snapshot is per worker; it may include a turn that ends while
// RunningTurns runs.
func (e *Engine) RunningTurns(dst []RunningTurn) []RunningTurn {
	s := &e.s
	for i := range s.workers {
		if a, start, ok := s.workers[i].turn.get(); ok {
			dst = append(dst, RunningTurn{Worker: i, GoID: s.goids[i], Actor: a, Start: start})
		}
	}
	return dst
}

// publishedTurn is one worker's turn in progress (SCH-270, SCH §11): set by the worker around
// RunTurn, read by RunningTurns. The mutex is uncontended except while RunningTurns reads, and
// setting and clearing allocate nothing (SCH-110).
type publishedTurn struct {
	mu    sync.Mutex
	actor engine.ActorRef // nil when no turn runs
	start time.Time
}

// set publishes a turn of a starting now.
func (p *publishedTurn) set(a engine.ActorRef) {
	now := time.Now()
	p.mu.Lock()
	p.actor, p.start = a, now
	p.mu.Unlock()
}

// clear withdraws the published turn and drops its reference to the actor.
func (p *publishedTurn) clear() {
	p.mu.Lock()
	p.actor, p.start = nil, time.Time{}
	p.mu.Unlock()
}

// get returns the published turn, or false if none is published.
func (p *publishedTurn) get() (engine.ActorRef, time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.actor, p.start, p.actor != nil
}
