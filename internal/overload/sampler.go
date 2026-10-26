// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload

import (
	"log/slog"
	"math"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

const (
	// sampleInterval is the period of the sampler tick (OVL-300).
	sampleInterval = 100 * time.Millisecond
	// sampleLabel names the sampler's timer (OVL-300, ENG-070).
	sampleLabel = "lockstep/overload-sample"

	metricHeapLive = "/gc/heap/live:bytes"
	metricMemLimit = "/gc/gomemlimit:bytes"
)

// readHook, when set, replaces metrics.Read (OVL §10).
var readHook atomic.Pointer[func([]metrics.Sample)]

// readMetrics calls the test hook if one is set and metrics.Read otherwise.
func readMetrics(s []metrics.Sample) {
	if h := readHook.Load(); h != nil {
		(*h)(s)
		return
	}
	metrics.Read(s)
}

// Sampler reads runtime/metrics on every tick of the engine clock and feeds SourceMemory
// (production). It starts no goroutine.
type Sampler struct {
	gate     *Gate
	clock    engine.Clock
	high     uint64 // explicit thresholds; meaningful only when explicit
	low      uint64
	explicit bool
	log      *slog.Logger
	mon      *Monitor

	// hyst, samples and ticks belong to the sampling callbacks, which the engine clock never
	// overlaps.
	hyst    Hysteresis
	on      bool
	samples [2]metrics.Sample
	ticks   int

	// enabled reports whether the last sample found thresholds in force (OVL-310). It is written by
	// the sampling callbacks and read from any goroutine.
	enabled atomic.Bool

	mu      sync.Mutex // guards cancel and stopped
	cancel  engine.Cancel
	stopped bool
}

// NewSampler returns a stopped Sampler that feeds SourceMemory of g. explicit is true when the
// user gave WithAdmission; with high and low zero it means admission is off, so the sampler never
// reads the metrics. Without explicit thresholds they derive from the Go memory limit at each
// sample (OVL-310). log receives the admission events of OVL-340 and may be nil. mon is the
// goroutine monitor the sampler calls on every 10th tick (OVL-600); it may be nil.
func NewSampler(g *Gate, clock engine.Clock, high, low uint64, explicit bool, log *slog.Logger, mon *Monitor) *Sampler {
	s := &Sampler{gate: g, clock: clock, high: high, low: low, explicit: explicit, log: log, mon: mon}
	s.samples[0].Name = metricHeapLive
	s.samples[1].Name = metricMemLimit
	return s
}

// Start takes the first sample and arms the first tick (OVL-300). Calling it after Stop does
// nothing.
func (s *Sampler) Start() {
	if s.isStopped() {
		return
	}
	s.sample()
	s.arm()
}

// Stop cancels the pending tick. A tick that is already running finishes but arms no further one.
func (s *Sampler) Stop() {
	s.mu.Lock()
	s.stopped = true
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Enabled reports whether memory admission has thresholds in force: the last sample found
// explicit thresholds or a Go memory limit, and both metrics were readable (OVL-310, OVL-330). It
// is false before the first sample and with admission off. Safe for concurrent use.
func (s *Sampler) Enabled() bool { return s.enabled.Load() }

func (s *Sampler) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

// arm schedules the next tick unless the sampler is stopped.
func (s *Sampler) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.cancel = s.clock.AfterFunc(sampleInterval, sampleLabel, s.tick)
}

// tick is the engine-context callback of OVL-300: it samples and re-arms itself.
func (s *Sampler) tick() {
	if s.isStopped() {
		return
	}
	s.sample()
	s.ticks++
	if s.mon != nil && s.ticks%monitorEvery == 0 {
		s.mon.Sample() // OVL-600: every 10th tick, whether or not admission is on
	}
	s.arm()
}

// admissionOff reports whether the user switched memory admission off with WithAdmission(0, 0).
func (s *Sampler) admissionOff() bool {
	return s.explicit && s.high == 0 && s.low == 0
}

// sample reads the metrics once and updates the memory source (OVL-310, OVL-320, OVL-330).
func (s *Sampler) sample() {
	if s.admissionOff() {
		return
	}
	readMetrics(s.samples[:])
	liveVal, limitVal := s.samples[0].Value, s.samples[1].Value
	if liveVal.Kind() != metrics.KindUint64 || limitVal.Kind() != metrics.KindUint64 {
		s.switchOff(0) // OVL-330: a metric the runtime cannot read keeps the source off
		return
	}
	live, limit := liveVal.Uint64(), limitVal.Uint64()

	high, low := s.high, s.low
	if !s.explicit {
		if limit >= math.MaxInt64 {
			s.switchOff(live) // OVL-310: no thresholds
			return
		}
		high, low = limit-limit/10, limit-limit/5
	}
	s.enabled.Store(true)
	s.hyst.High, s.hyst.Low = high, low
	changed, on := s.hyst.Update(live)
	if changed {
		s.change(on, live, high, low)
	}
}

// switchOff turns the memory source off, with no thresholds in force, when it is on.
func (s *Sampler) switchOff(live uint64) {
	s.enabled.Store(false)
	s.hyst = Hysteresis{}
	if s.on {
		s.change(false, live, 0, 0)
	}
}

// change feeds the new state to the Gate and logs it with OBS-060's admission events (OVL-340).
func (s *Sampler) change(on bool, live, high, low uint64) {
	s.on = on
	s.gate.Set(engine.SourceMemory, on, live, high, low)
	if s.log == nil {
		return
	}
	if on {
		s.log.Warn("admission refusing",
			slog.Uint64("heap_live_bytes", live), slog.Uint64("high_mark_bytes", high))
	} else {
		s.log.Info("admission resumed",
			slog.Uint64("heap_live_bytes", live), slog.Uint64("low_mark_bytes", low))
	}
}
