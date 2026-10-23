// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
)

// maxPPM is the probability scale of DropBetween: ppm of 1_000_000 drops every message.
const maxPPM = 1_000_000

// pairKey identifies a pair rule: the ordered pair of actor locations, a node name and an actor
// path each, without uids (SFT-060).
type pairKey struct {
	fromNode, fromPath, toNode, toPath string
}

// pairKeyOf returns the key of the pair from, to.
func pairKeyOf(from, to lockstep.Address) pairKey {
	return pairKey{from.Node(), from.Path(), to.Node(), to.Path()}
}

// label returns the pair's part of its stream labels (SFT §6).
func (k pairKey) label() string {
	return k.fromNode + k.fromPath + "->" + k.toNode + k.toPath
}

// pairRule is the delay part, the drop part and the hold queue of one pair (SFT-060, SFT-090).
// A rule exists while it has a part or held messages. The streams are the pair's: a stream is
// the kernel's for its label, so a rule created again for the same pair continues it.
type pairRule struct {
	key pairKey

	delay    bool          // the delay part is set
	min, max time.Duration // its range, when set
	dropPPM  uint32        // the drop part; 0: none

	held   []heldMsg     // the hold queue, in offer order, with non-decreasing release times
	eng    *simEngine    // the receiver's engine of the messages in held
	cancel engine.Cancel // Cancel of the pending release event, for held[0]; nil: none

	dropRand, delayRand *rand.Rand // the pair's streams, taken at the first draw
}

// heldMsg is a message the interposer took, with the function that offers it to the receiver.
type heldMsg struct {
	offer   func()
	release kernel.Time
}

// DelayBetween delays every message sent from the actor at from's location to the actor at to's
// location by a virtual duration drawn uniformly from [min, max], before it is offered to the
// receiver's mailbox. Order within the pair is kept. min = max = 0 removes the delay part of the
// pair's rule; the rule goes when it has no drop part either and no message is held. It panics if
// min < 0 or max < min. Replies, timer messages, effect results and system messages are never
// delayed. The call records lockstep.fault with op=delay.
func (s *Sim) DelayBetween(from, to lockstep.Address, min, max time.Duration) {
	s.check()
	if min < 0 || max < min {
		panic(fmt.Sprintf("lockstep: sim: DelayBetween: invalid range [%s, %s]", min, max))
	}
	delays := min != 0 || max != 0
	s.setPair(pairKeyOf(from, to), delays, func(r *pairRule) {
		r.delay = delays
		r.min, r.max = min, max
	})
	s.recordFault(0, "delay",
		kernel.Attr{Key: "from", Value: from.String()},
		kernel.Attr{Key: "to", Value: to.String()},
		kernel.Attr{Key: "min", Value: min.String()},
		kernel.Attr{Key: "max", Value: max.String()},
	)
}

// DropBetween drops each message sent from the actor at from's location to the actor at to's
// location with probability ppm/1_000_000, before it is offered. 0 removes the drop part of the
// pair's rule; the rule goes when it has no delay part either and no message is held. It panics if
// ppm > 1_000_000. The call records lockstep.fault with op=drop.
func (s *Sim) DropBetween(from, to lockstep.Address, ppm uint32) {
	s.check()
	if ppm > maxPPM {
		panic("lockstep: sim: DropBetween: ppm " + strconv.FormatUint(uint64(ppm), 10) + " > 1000000")
	}
	s.setPair(pairKeyOf(from, to), ppm != 0, func(r *pairRule) { r.dropPPM = ppm })
	s.recordFault(0, "drop",
		kernel.Attr{Key: "from", Value: from.String()},
		kernel.Attr{Key: "to", Value: to.String()},
		kernel.Attr{Key: "ppm", Value: strconv.FormatUint(uint64(ppm), 10)},
	)
}

// setPair applies update to the rule of k. A rule is created only when the call sets a part
// (sets), and a rule left with no part and no held message is removed (SFT-060, SFT-080).
func (s *Sim) setPair(k pairKey, sets bool, update func(r *pairRule)) {
	r := s.pairs[k]
	if r == nil {
		if !sets {
			return
		}
		r = &pairRule{key: k}
		if s.pairs == nil {
			s.pairs = make(map[pairKey]*pairRule)
		}
		s.pairs[k] = r
	}
	update(r)
	s.prunePair(r)
}

// prunePair removes r from the rule table once it has no part and no held message.
func (s *Sim) prunePair(r *pairRule) {
	if !r.delay && r.dropPPM == 0 && len(r.held) == 0 && s.pairs[r.key] == r {
		delete(s.pairs, r.key)
	}
}
