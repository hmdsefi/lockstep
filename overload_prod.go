// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/hmdsefi/lockstep/internal/overload"
	"github.com/hmdsefi/lockstep/internal/prodengine"
)

// prodOverload is the overload machinery that only a system New builds runs (OVL §5.5–§5.9,
// OVL-230): a system built on a given bundle, simulation included, has none, and every field is
// nil. startOverload sets the fields before New returns; stopOverload stops what they hold.
type prodOverload struct {
	sampler *overload.Sampler // samples the live heap for the memory source on the engine clock (OVL-300)
	// watchdog reports the turns running past the watchdog timeout (OVL-500); nil when the timeout
	// is 0.
	watchdog *overload.Watchdog
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

	// Stuck-turn watchdog (OVL-500): its one goroutine polls the engine's running turns.
	if d := cfg.watchdog; d > 0 {
		turns := &runningTurns{pe: s.prod}
		s.prodOvl.watchdog = overload.NewWatchdog(d, turns.running, func(t overload.Turn, elapsed time.Duration, stack string) {
			s.onStuck(t, elapsed, d, stack)
			turns.markStuck(t)
		})
		s.prodOvl.watchdog.Start()
	}
}

// stopOverload stops the production overload machinery, if the system has any, before the
// engine stops (OVL-700): the sampler's pending tick is cancelled, and a tick running on a worker
// arms no further one; the watchdog's goroutine has exited when it returns. stopEngine calls it
// once, on every path that stops the engine.
func (s *System) stopOverload() {
	if s.prodOvl.sampler != nil {
		s.prodOvl.sampler.Stop()
	}
	if s.prodOvl.watchdog != nil {
		s.prodOvl.watchdog.Stop()
	}
}

// runningTurns adapts (*prodengine.Engine).RunningTurns to the watchdog (OVL-500): the watchdog
// reaches the engine only through it, so internal/prodengine and internal/overload do not import
// each other (R38). It also marks the actors of the turns the watchdog reports (OVL-520). Only the
// watchdog's goroutine calls running and markStuck, so the buffers need no lock.
type runningTurns struct {
	pe      *prodengine.Engine
	scratch []prodengine.RunningTurn // reused by every poll
	polled  []polledTurn             // the turns of the last poll, with their actors' turn counters
}

// polledTurn is a turn of the last poll: its worker and start, which identify it (OVL-500), and
// the turn counter its actor had right after the snapshot, 0 for the guardians, which are not a
// cell.
type polledTurn struct {
	worker int
	start  time.Time
	seq    uint64
}

// running appends the engine's turns in progress to dst as watchdog turns and returns the
// extended slice. With each turn it keeps the actor's turn counter, read after the snapshot, for
// markStuck: the Turn of OVL §4.2 has no field for it.
func (r *runningTurns) running(dst []overload.Turn) []overload.Turn {
	r.scratch = r.pe.RunningTurns(r.scratch[:0])
	r.polled = r.polled[:0]
	for _, t := range r.scratch {
		dst = append(dst, overload.Turn(t)) // the same fields (SCH-270, OVL §4.2)
		var seq uint64
		if c, ok := t.Actor.(actorCell); ok {
			seq = c.turnSeq()
		}
		r.polled = append(r.polled, polledTurn{worker: t.Worker, start: t.Start, seq: seq})
	}
	clear(r.scratch) // keep no actor alive until the next poll
	return dst
}

// markStuck marks the actor of t, a turn of the last poll that the watchdog reports, stuck,
// unless the turn has ended (OVL-520). The counter read with the poll belongs to t only if t was
// still running when it was read, so a fresh snapshot must still hold t's (Worker, Start): t ran
// from the first snapshot to the second, and one actor's turns never overlap, so the counter read
// between them is t's. The cell then ignores the mark if t ends before it is stored. A turn of the
// guardians is never marked. It takes only the engine's per-worker locks and stores an atomic:
// it runs on the watchdog's goroutine and must not block.
func (r *runningTurns) markStuck(t overload.Turn) {
	c, ok := t.Actor.(actorCell)
	if !ok {
		return
	}
	same := func(worker int, start time.Time) bool { return worker == t.Worker && start.Equal(t.Start) }
	i := slices.IndexFunc(r.polled, func(p polledTurn) bool { return same(p.worker, p.start) })
	if i < 0 {
		return
	}
	r.scratch = r.pe.RunningTurns(r.scratch[:0])
	running := slices.ContainsFunc(r.scratch, func(p prodengine.RunningTurn) bool { return same(p.Worker, p.Start) })
	clear(r.scratch)
	if running {
		c.markStuck(r.polled[i].seq, t.Start)
	}
}

// onStuck reports a turn of t.Actor that has run for elapsed, at least the watchdog timeout, on
// the watchdog's goroutine (OVL-510), before the adapter marks the actor (OVL-520): it logs OBS-060's "Stuck turn" event through the system's
// logger with the actor's attributes in OBS-020's order and the stack of the turn's worker. A
// turn of the guardians, which are not a cell, carries the user guardian's address. The address
// is safe to read here: it is written before the actor's first turn, and the engine publishes the
// turn under a lock that RunningTurns takes.
func (s *System) onStuck(t overload.Turn, elapsed, timeout time.Duration, stack string) {
	a := t.Actor.Address()
	s.log.LogAttrs(context.Background(), slog.LevelError, "turn exceeds watchdog timeout",
		slog.String("node", a.Node), slog.String("path", a.Path), slog.Uint64("uid", a.UID),
		slog.Duration("elapsed", elapsed.Truncate(time.Millisecond)),
		slog.Duration("timeout", timeout),
		slog.String("stack", stack),
	)
}
