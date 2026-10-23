// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep/internal/engine"
)

// delayLabel is the label of the events that release held messages (SFT-090).
const delayLabel = "lockstep/delay"

// Interpose applies the pair rules to a sent message the receiver's runtime is about to offer
// (SFT-070, ENG-150). It returns true when it dropped the message or took it into the pair's
// hold queue, and false to let the runtime offer it now. With no rule set it costs one map length
// check.
func (e *simEngine) Interpose(from, to engine.Address, msg any, offer func()) bool {
	s := e.s
	if e.dead || len(s.pairs) == 0 {
		return false
	}
	r := s.pairs[pairKey{from.Node, from.Path, to.Node, to.Path}]
	if r == nil {
		return false
	}
	if len(r.held) > 0 && r.eng != e {
		// The held messages are for an earlier incarnation of the receiver, whose release event
		// has not run yet: they are gone (SFT-090).
		s.discardHeld(r)
		s.prunePair(r)
		if s.pairs[r.key] != r { // the rule had no part left
			return false
		}
	}
	if r.dropPPM > 0 { // SFT-080 step 1
		if r.dropRand == nil {
			r.dropRand = s.w.Sim.Rand("lockstep/fault/drop/" + r.key.label())
		}
		if kernel.Chance(r.dropRand, r.dropPPM) {
			e.emit("lockstep.drop", "drop to "+to.String(), []engine.Attr{
				{Key: "to", Value: to.String()},
				{Key: "reason", Value: "injected"},
				{Key: "from", Value: from.String()},
				{Key: "msg", Value: kernel.Describe(msg)},
			})
			return true
		}
	}
	if len(r.held) == 0 && !r.delay { // step 3
		return false
	}
	var d time.Duration // step 2
	if r.delay {
		if r.delayRand == nil {
			r.delayRand = s.w.Sim.Rand("lockstep/fault/delay/" + r.key.label())
		}
		d = kernel.Uniform(r.delayRand, r.min, r.max)
	}
	release := s.w.Sim.Now().Add(d)
	if n := len(r.held); n > 0 {
		release = max(release, r.held[n-1].release)
	}
	r.held = append(r.held, heldMsg{offer: offer, release: release})
	r.eng = e
	e.emit("lockstep.delay", "delay to "+to.String(), []engine.Attr{
		{Key: "from", Value: from.String()},
		{Key: "to", Value: to.String()},
		{Key: "msg", Value: kernel.Describe(msg)},
		{Key: "delay", Value: d.String()},
		{Key: "release", Value: release.String()},
	})
	if r.ev == 0 {
		s.scheduleRelease(r)
	}
	return true
}

// scheduleRelease schedules the release event of r's head (SFT-090).
func (s *Sim) scheduleRelease(r *pairRule) {
	r.ev = s.atDelay(r.held[0].release, func() { s.release(r) })
}

// atDelay schedules fn as a world event labeled lockstep/delay at virtual time t.
func (s *Sim) atDelay(t kernel.Time, fn func()) kernel.EventID {
	return s.w.Sim.At(t, delayLabel, fn)
}

// postDelay posts fn as a node event of n labeled lockstep/delay: it runs when n resumes.
func (s *Sim) postDelay(n *kernel.Node, fn func()) kernel.EventID {
	return n.Post(delayLabel, fn)
}

// release is the release event of r (SFT-090). If the receiver's incarnation changed, its node is
// down or its engine is dead, the queue is discarded. If the node is paused, the event is posted
// again to the node and continues on resume. Otherwise every held message whose release time has
// come is offered, in order, without interposition, and the event is scheduled for the new head.
func (s *Sim) release(r *pairRule) {
	r.ev = 0
	e := r.eng
	if e.dead || e.inc != e.n.Incarnation() || e.n.State() == kernel.NodeDown {
		s.discardHeld(r)
		s.prunePair(r)
		return
	}
	if e.n.State() == kernel.NodePaused {
		r.ev = s.postDelay(e.n, func() { s.release(r) })
		return
	}
	now := s.w.Sim.Now()
	for len(r.held) > 0 && r.held[0].release <= now {
		h := r.held[0]
		r.held[0] = heldMsg{}
		r.held = r.held[1:]
		h.offer()
	}
	if len(r.held) > 0 {
		if r.ev == 0 {
			s.scheduleRelease(r)
		}
		return
	}
	r.held = nil
	s.prunePair(r)
}

// discardHeld drops r's held messages and cancels its pending release event.
func (s *Sim) discardHeld(r *pairRule) {
	if r.ev != 0 {
		s.w.Sim.Cancel(r.ev)
		r.ev = 0
	}
	clear(r.held)
	r.held = nil
}
