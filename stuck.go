// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"time"
)

// A stuck actor (OVL-520 to OVL-540). The watchdog's adapter marks a cell stuck when it reports a
// turn of it (overload_prod.go); while the mark names the turn in progress, user messages to the
// actor take the full branch of its mailbox's policy with errStuck as the reason. The turn clears
// the mark itself: right after the handler during which it was marked, failing the incarnation,
// or, for code that is not a handler of the live incarnation, without a failure.
//
// The cell's turn counter tells the turns apart (OVL §11): RunTurn adds one when it starts and one
// when it ends, so the counter is odd while a turn runs and names that turn. A mark carries the
// counter of the turn the watchdog reported and counts only while the counter still has that
// value, so a mark that arrives after its turn has ended makes nothing stuck, and the next turn
// never inherits it. Everything here is atomics: senders read the mark from any goroutine, and a
// waiting sender's admit reads it with a mailbox's lock held.

// stuckMark marks the turn whose counter is seq, which started at start, stuck (OVL-520).
type stuckMark struct {
	seq   uint64
	start time.Time
}

// errStuck stands where admission's error goes for a message to a stuck actor (OVL-520), which no
// source refused: refusalError turns it into the stuck error, and its records name reason stuck
// (OVL §8).
var errStuck = errors.New("stuck")

// sendStuck returns the error of a message to the stuck actor at to that the mailbox's policy
// refused or dropped as if full (OVL-520). It matches ErrMailboxFull, not ErrOverloaded.
func sendStuck(to Address) error {
	return &overloadError{
		text:   "lockstep: send to " + to.String() + ": actor stuck, mailbox treated as full",
		reason: "stuck",
		errs:   []error{ErrMailboxFull},
	}
}

// turnSeq returns the cell's turn counter (actorCell): odd while a turn runs.
func (c *cell[M]) turnSeq() uint64 { return c.turns.Load() }

// markStuck marks the turn whose counter is seq, which started at start, stuck (actorCell,
// OVL-520), unless that turn is no longer in progress. It runs on the watchdog's goroutine and
// only stores the mark: a turn that ends meanwhile leaves a mark that counts for nothing.
func (c *cell[M]) markStuck(seq uint64, start time.Time) {
	if seq%2 == 1 && c.turns.Load() == seq {
		c.stuckAt.Store(&stuckMark{seq: seq, start: start})
	}
}

// stuck reports whether the actor is stuck (actorCell): it is marked, and the mark names the turn
// in progress (OVL-520). Any goroutine may call it; it takes no lock.
func (c *cell[M]) stuck() bool {
	m := c.stuckAt.Load()
	return m != nil && m.seq == c.turns.Load()
}

// beginTurn starts the turn's count: the counter becomes odd and names this turn.
func (c *cell[M]) beginTurn() { c.turn = c.turns.Add(1) }

// endTurn ends the turn's count and clears a mark of the turn that is left (OVL-530). The counter
// moves on first, so no sender sees the actor stuck once the mark has been cleared.
func (c *cell[M]) endTurn() {
	c.turns.Add(1)
	c.unmark()
}

// unmark clears the mark of the turn in progress, if there is one, and returns it; then it calls
// Recheck on the user mailbox, so that senders waiting while the actor was stuck are admitted
// (OVL-530, MBX-075). It returns nil when there was no mark of this turn. Only the turn calls it,
// holding no lock.
func (c *cell[M]) unmark() *stuckMark {
	m := c.stuckAt.Load()
	if m == nil || m.seq != c.turn || !c.stuckAt.CompareAndSwap(m, nil) {
		return nil
	}
	c.user.Recheck()
	return m
}

// stuckFailure returns the failure of the incarnation whose handler ran in the turn that m marked
// stuck (OVL-530): the running time is the turn's, from its start to now.
func (c *cell[M]) stuckFailure(m *stuckMark) error {
	running := c.sys.eng.Clock.Now().Sub(m.start)
	return fmt.Errorf("lockstep: actor %s stuck for %s: %w", c.addr, running, ErrTimeout)
}
