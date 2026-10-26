// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"github.com/hmdsefi/lockstep/internal/overload"
	"github.com/hmdsefi/lockstep/internal/prodengine"
)

// prodOverload is the overload machinery that only a system New builds runs (OVL §5.5–§5.9,
// OVL-230): a system built on a given bundle, simulation included, has none, and every field is
// nil. startOverload sets the fields before New returns; stopOverload stops what they hold.
type prodOverload struct {
	sampler *overload.Sampler // samples the live heap for the memory source on the engine clock (OVL-300)
}

// startOverload starts the production overload machinery of a system New built on pe, whose
// bundle's Admission is gate, configured by cfg. It runs once, before New returns, after the
// system has bound gate (ENG-125), so the sampler's first sample, taken here, can already turn
// the memory source on (OVL-300) and report it to the system on the caller's goroutine. Each
// feature starts on its own line, in the order the features depend on each other.
func (s *System) startOverload(pe *prodengine.Engine, gate *overload.Gate, cfg *systemConfig) {
	s.prod = pe
	log := s.log.With("node", s.node) // the system's events carry the node (OVL-340)

	// Memory admission (OVL-300, OVL-310): the sampler ticks whether or not admission is on, and
	// reads the metrics only when it is. The goroutine monitor (OVL-600) runs on every 10th tick.
	adm := cfg.admission
	mon := overload.NewMonitor(cfg.goroutineWarning, log)
	s.prodOvl.sampler = overload.NewSampler(gate, s.eng.Clock, adm.high, adm.low, adm.set, log, mon)
	s.prodOvl.sampler.Start()
}

// stopOverload stops the production overload machinery, if the system has any, before the
// engine stops (OVL-700): the sampler's pending tick is cancelled, and a tick running on a worker
// arms no further one. stopEngine calls it once, on every path that stops the engine.
func (s *System) stopOverload() {
	if s.prodOvl.sampler != nil {
		s.prodOvl.sampler.Stop()
	}
}
