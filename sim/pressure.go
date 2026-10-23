// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep/internal/engine"
)

// pressureLabel labels the kernel events that start and end pressure episodes (SFT §6).
const pressureLabel = "lockstep/pressure"

// PressureRule describes random memory-pressure episodes on one node.
type PressureRule struct {
	Every  time.Duration // mean gap between episodes; each gap is uniform in [Every/2, Every+Every/2]
	MinFor time.Duration // each episode lasts a duration uniform in [MinFor, MaxFor]
	MaxFor time.Duration
}

// problem returns what makes r invalid (SFT-100), or "" if it is valid.
func (r PressureRule) problem() string {
	switch {
	case r.Every <= 0:
		return fmt.Sprintf("Every %s must be positive", r.Every)
	case r.MinFor <= 0:
		return fmt.Sprintf("MinFor %s must be positive", r.MinFor)
	case r.MaxFor < r.MinFor:
		return fmt.Sprintf("MaxFor %s is less than MinFor %s", r.MaxFor, r.MinFor)
	case r.Every > fault.MaxRuleDuration:
		return fmt.Sprintf("Every %s exceeds fault.MaxRuleDuration %s", r.Every, fault.MaxRuleDuration)
	case r.MaxFor > fault.MaxRuleDuration:
		return fmt.Sprintf("MaxFor %s exceeds fault.MaxRuleDuration %s", r.MaxFor, fault.MaxRuleDuration)
	}
	return ""
}

// Pressure starts random pressure episodes on node, drawn from the stream
// "lockstep/pressure/<node>". Episodes start only before the recovery start
// (faultline World.RecoveryStart), and an episode still active there ends there. While an episode
// is active the node's System sheds work: it refuses normal spawns and sheds sent messages. It
// panics if node is not a lockstep node or rule is invalid.
func (s *Sim) Pressure(node string, rule PressureRule) {
	s.check()
	ln := s.lookup(node)
	if p := rule.problem(); p != "" {
		panic("lockstep: sim: Pressure: " + p)
	}
	pr := &pressureRule{s: s, ln: ln, rule: rule, r: s.w.Sim.Rand(pressureLabel + "/" + node)}
	pr.next(s.w.Sim.Now())
}

// PressureFor puts node under memory pressure now, for d of virtual time. It panics if node is
// not a lockstep node or d <= 0.
func (s *Sim) PressureFor(node string, d time.Duration) {
	s.check()
	ln := s.lookup(node)
	if d <= 0 {
		panic(fmt.Sprintf("lockstep: sim: PressureFor: duration %s must be positive", d))
	}
	ln.beginEpisode()
	s.w.Sim.AtFront(s.w.Sim.Now().Add(d), pressureLabel, ln.endEpisode)
}

// pressureRule is one Pressure rule while it runs: its episodes never overlap (SFT-110).
type pressureRule struct {
	s    *Sim
	ln   *node
	rule PressureRule
	r    *rand.Rand // the node's lockstep/pressure/<node> stream
}

// next draws a gap and schedules the rule's next start that gap after from (SFT-100, SFT-110).
func (p *pressureRule) next(from kernel.Time) {
	gap := kernel.Uniform(p.r, p.rule.Every/2, p.rule.Every+p.rule.Every/2)
	p.s.w.Sim.AtFront(from.Add(gap), pressureLabel, p.start)
}

// start is a start event of the rule (SFT-110): before the recovery start it draws the episode's
// length, begins it and schedules its end, cut at the recovery start; from the recovery start on
// the rule ends.
func (p *pressureRule) start() {
	w := p.s.w
	t, rec := w.Sim.Now(), w.RecoveryStart()
	if t >= rec {
		return
	}
	end := min(t.Add(kernel.Uniform(p.r, p.rule.MinFor, p.rule.MaxFor)), rec)
	p.ln.beginEpisode()
	w.Sim.AtFront(end, pressureLabel, func() {
		p.ln.endEpisode()
		p.next(end)
	})
}

// beginEpisode begins a pressure episode on the node; the first active one turns the pressure source of
// the node's current system on (SFT-125).
func (ln *node) beginEpisode() {
	ln.pressure++
	if ln.pressure == 1 {
		ln.setPressure(true)
	}
}

// endEpisode ends a pressure episode on the node; the last active one turns the pressure source of the
// node's current system off (SFT-125).
func (ln *node) endEpisode() {
	ln.pressure--
	if ln.pressure == 0 {
		ln.setPressure(false)
	}
}

// setPressure turns the pressure source of ln's current system on or off. A node without a system
// (down, or its boot built none) has nothing to tell: its next system gets the state at boot.
func (ln *node) setPressure(on bool) {
	if ln.sys != nil {
		ln.eng.gate.Set(engine.SourcePressure, on, 0, 0, 0)
	}
}

// bootPressure applies ln's pressure state to the system just built on e, before setup runs, so
// that setup runs shedding under pressure (SFT-125, OVL-160).
func (ln *node) bootPressure(e *simEngine) {
	if ln.pressure > 0 {
		e.gate.Set(engine.SourcePressure, true, 0, 0, 0)
	}
}
