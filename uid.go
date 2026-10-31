// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"strconv"
)

// maxSeq is the largest seq of one epoch (ACT-040).
const maxSeq = 1<<32 - 1

// allocUID returns the next uid: uint64(epoch)<<32 | uint64(seq), with seq starting at 1 and rising
// by 1 per call. When seq would pass 2^32-1 the system takes a new epoch and seq starts again at 1
// (ACT-040). It is safe for concurrent use and never returns 0.
func (s *System) allocUID() uint64 {
	s.uidMu.Lock()
	defer s.uidMu.Unlock()
	return s.nextUID()
}

// allocAsk returns the next ask number (MSG-300 step 4) and the next uid, both allocated under the
// uid lock, so ask numbers and uids rise together. It reports false, allocating nothing, when s is
// stopped.
func (s *System) allocAsk() (n, uid uint64, ok bool) {
	s.uidMu.Lock()
	defer s.uidMu.Unlock()
	if s.stopped.Load() {
		return 0, 0, false
	}
	s.asks.counter++
	return s.asks.counter, s.nextUID(), true
}

// nextUID is allocUID with s.uidMu held.
func (s *System) nextUID() uint64 {
	if s.seq > maxSeq {
		s.nextEpoch()
	}
	uid := uint64(s.epoch)<<32 | s.seq
	s.seq++
	return uid
}

// nextEpoch moves allocation to a new epoch, drawn from the "$uid" stream until it is neither 0
// nor an epoch this system has used. The caller holds s.uidMu.
func (s *System) nextEpoch() {
	if s.uidRand == nil {
		label := "lockstep/" + s.node + "/" + strconv.FormatUint(uint64(s.incarnation), 10) + "/$uid"
		s.uidRand = s.eng.Entropy.Rand(label) // once per system (ENG-100)
	}
	e := s.uidRand.Uint32()
	for e == 0 || slices.Contains(s.epochs, e) {
		e = s.uidRand.Uint32()
	}
	s.epochs = append(s.epochs, e)
	s.epoch, s.seq = e, 1
}
