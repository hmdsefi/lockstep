// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/overload"
	"github.com/hmdsefi/lockstep/internal/prodengine"
	"github.com/hmdsefi/lockstep/internal/registry"
)

// System is one runtime instance. All of its exported functions and methods are safe for
// concurrent use when *System is the Spawner or Sender.
type System struct {
	eng engine.Engine
	// prod is the production engine whose bundle eng is, when New built the system (SCH-030);
	// nil for a system built on a given bundle (ACT-015). The watchdog reads its running turns
	// (OVL-500).
	prod        *prodengine.Engine
	node        string
	incarnation uint32        // Engine.Epoch; constant for the system's life (ACT-045)
	mbx         mailboxConfig // the mailbox of an actor spawned without WithMailbox (MBX-010)
	// log is the system logger (OBS-010): WithLogger's logger, or else slog's default logger when
	// the system was built on a blocking engine and a discarding logger on any other (obs.go). The
	// actor loggers build on it (OBS-030); the runtime log events go through events.
	log *slog.Logger
	// events is the logger of the runtime log events (obs_log.go): log for a system New built, nil
	// for one built on a given bundle, which logs none of them (OBS-155). logOff removes the
	// logging calls; only AT-OBS-06 sets it.
	events *slog.Logger
	logOff atomic.Bool
	// obs holds the counters New registered with WithMetrics's backend (OBS-120); nil when the
	// system has metrics off and for one built on a given bundle, which never counts (OBS-110,
	// OBS-155). Set before New returns and never changed.
	obs *obsCounters

	reg          *registry.Registry
	user, system Address // the guardians; neither is registered (ACT-030)
	// actors counts the live actors against the actor cap (OVL-010, OVL-020): Spawn takes a slot
	// before it registers the child, and the stop sequence returns it when it removes the child
	// from the registry. In-place restarts never touch it (OVL-160).
	actors  *overload.Cap
	closing atomic.Bool // shutting down or stopped: Spawn returns ErrStopped (ACT-050); set by Shutdown
	// stopped is set when both guardians' children have stopped (ACT-050): Send, Ask and NewReply
	// return ErrStopped and reply attempts are dropped (MSG-100, MSG-300, MSG-380, MSG-410). It is
	// set under the ask table's lock (askTable.stop).
	stopped atomic.Bool
	// discarded is set by Discard, when the simulation engine discards the system after a node
	// crash (SUP-360): Spawn returns ErrStopped and Shutdown does nothing, on top of what stopped
	// does. No actor runs again, so nothing else reads it.
	discarded atomic.Bool
	// spawnMu makes Spawn's closing check atomic with the child's registration: Spawn holds it for
	// reading from the check through Registry.Add, and Shutdown sets closing holding it for writing.
	// A child that registers is therefore one the shutdown's guardian sweep finds (ACT-050).
	spawnMu sync.RWMutex
	shut    shutdownState
	// guardians supervises the guardians' children and runs the shutdown's sweep (SUP-300,
	// ACT-050).
	guardians guardians

	// estimates reports whether memory estimates are on (OVL-400); memBytes is their sum over the
	// live actors (OVL-420), the Bytes of the usage reported to admission.
	estimates bool
	memBytes  atomic.Int64

	// effectSeq is the last EffectID assigned: Do numbers the effects of every actor of the system
	// from it (EFF-020).
	effectSeq atomic.Uint64

	// skipEffectCancel makes a stop leave the actor's pending effects uncancelled. Only the
	// tests set it, through SkipEffectCancelAtStop, to give the simulation's leak check an
	// effect to find (AT-SIM-26).
	skipEffectCancel atomic.Bool

	asks    askTable       // pending asks of this system's askers and tracked asks of its actors (MSG §11)
	waiting waitSet        // the user mailboxes that outside senders wait on (MBX-075)
	adm     admissionState // whether the engine's admission sheds (OVL-150)
	ovl     overloadSubs   // the incarnations subscribed to Overloaded, in order (OVL-170)
	prodOvl prodOverload   // the production overload machinery; empty unless New built the system

	// afterRegister, when set, is called by Spawn with the new cell between its registration and
	// the push of its begin. It is a test hook; production code never sets it.
	afterRegister func(c actorCell)
	// afterTurn, when set, is called at the end of every turn with the actor's address and the
	// numbers of system and user messages the turn processed (ENG-340). It is a test hook set
	// before the first Spawn; production code never sets it.
	afterTurn func(a Address, system, user int)

	uidMu   sync.Mutex
	epoch   uint32 // the epoch of the next uid
	seq     uint64 // the seq of the next uid, 1 to 2^32-1 (ACT-040)
	epochs  []uint32
	uidRand *rand.Rand // the "$uid" stream, fetched on the first rollover (ENG-100)
}

// Spawner is a parent for Spawn. It is implemented only by *System (the user guardian is the
// parent) and *Context[X] for any X (that actor is the parent).
type Spawner interface {
	spawner() // unexported: no other implementations
}

// Sender is the origin of a send, an ask, a reply or a stop. It is implemented only by *System and
// *Context[X] for any X.
type Sender interface {
	sender() // unexported: no other implementations
}

func (*System) spawner() {}
func (*System) sender()  {}

// New builds a system on a pool of worker goroutines that run actor turns, creates its two
// guardians and returns it. Turns of different actors can run in parallel on different workers;
// the turns of one actor never overlap. WithWorkers, WithThroughput and WithEffectPool configure
// the pools. The system also samples the live heap for memory admission (WithAdmission) and logs
// each turn that runs for at least the watchdog timeout at level Error, with the stack of the
// goroutine running it. The watchdog is on by default, with DefaultWatchdogTimeout; WithWatchdog
// sets the timeout or turns it off. New panics with an error wrapping ErrInvalidOption if an
// option is nil or invalid.
func New(opts ...Option) *System {
	// The engine needs the node name and the pool settings before it starts its workers, so
	// validate the options first: an invalid option panics before any goroutine exists.
	cfg, err := newConfig(opts, "")
	if err != nil {
		panic(err)
	}
	gate := overload.NewGate() // OVL §4.2; newSystem binds it once, startOverload feeds it
	pe := prodengine.New(prodengine.Config{
		Node:          cfg.node,
		Workers:       cfg.workers,
		Throughput:    cfg.throughput,
		EffectWorkers: cfg.effectWorkers,
		EffectQueue:   effectQueue(cfg.effectQueue),
		Admission:     gate,
	})
	sys, err := newSystem(pe.Engine(), opts)
	if err != nil {
		pe.Stop()
		panic(err) // unreachable: the options passed newConfig above
	}
	sys.events = sys.log
	if cfg.metrics != nil {
		// Counters register before the overload machinery starts, which can change admission and
		// count that change (OBS-080). A panic in the backend leaves the engine's workers behind
		// unless it stops them.
		registered := false
		defer func() {
			if !registered {
				pe.Stop()
			}
		}()
		sys.registerMetrics(cfg.metrics)
		registered = true
	}
	sys.startOverload(pe, gate, cfg)
	return sys
}

// effectQueue maps WithEffectPool's queue to prodengine.Config.EffectQueue, where 0 means the
// default and -1 means no queue (SCH-030).
func effectQueue(q int) int {
	if q == 0 {
		return -1
	}
	return q
}

// Node returns the system's node name: the node part of every address it allocates.
func (s *System) Node() string { return s.node }

// Incarnation returns the epoch the system started with: the faultline node incarnation in
// simulation, a random value otherwise. It is never 0 and does not change for the system's life,
// even when uid allocation moves to a new epoch.
func (s *System) Incarnation() uint32 { return s.incarnation }

// newSystem builds a system on eng, which must be valid (engine.Engine.Validate), exactly as New
// does with these differences: the node name is eng.Node and a WithNodeName option is ignored, the
// uid epoch is eng.Epoch, and an invalid option is returned instead of panicked with (ACT-015).
// It never panics for an option list; it calls eng.Admission.Bind once, after the options have
// passed, so a failed call leaves eng's admission unbound (ENG-125).
func newSystem(eng engine.Engine, opts []Option) (*System, error) {
	cfg, err := newConfig(opts, eng.Node)
	if err != nil {
		return nil, err
	}
	s := &System{
		eng:         eng,
		node:        cfg.node,
		log:         systemLogger(cfg, eng),
		mbx:         cfg.mbx,
		estimates:   cfg.estimates,
		incarnation: eng.Epoch,
		reg:         registry.New(),
		actors:      overload.NewCap(cfg.maxActors),
		epoch:       eng.Epoch,
		seq:         1,
		epochs:      []uint32{eng.Epoch},
		shut:        shutdownState{done: make(chan struct{})},
	}
	s.guardians.init(s, cfg.guardian)
	// ACT-030: the user guardian first, then the system guardian, with uids 1 and 2 of the epoch.
	s.user = Address{node: s.node, path: "/user", uid: s.allocUID()}
	s.system = Address{node: s.node, path: "/system", uid: s.allocUID()}
	s.reg.AddRoot(registry.Addr{Node: s.user.node, Path: s.user.path, UID: s.user.uid})
	s.reg.AddRoot(registry.Addr{Node: s.system.node, Path: s.system.path, UID: s.system.uid})
	s.bindAdmission()
	return s, nil
}

// shutdownState is the progress of a system's shutdown (ACT-050, ACT-060, ENG-140).
type shutdownState struct {
	start sync.Once     // the first Shutdown call starts the shutdown; later calls never start another
	done  chan struct{} // closed when the system is stopped and phase=done is recorded (ACT-060)
	over  atomic.Bool   // done is closed

	began time.Time // the engine clock's Now when the shutdown started; set only if events is set

	engOnce    sync.Once   // Engine.Stop is called at most once (ENG-140)
	engStopped atomic.Bool // Engine.Stop has returned (or the engine has none)
}

// Shutdown stops every actor, user actors first, then system actors, and waits until every
// Stopped hook has returned. It returns nil when the system is stopped, or ctx.Err() if ctx ends
// first; shutdown then continues and a later call waits again. Called during a turn (an actor
// may shut down its own system) or on an engine that cannot block, such as the simulation
// engine, it starts the shutdown and returns nil at once without waiting.
func (s *System) Shutdown(ctx context.Context) error {
	if s.discarded.Load() { // SUP-360: no Stopped hook runs, so there is no shutdown to start or wait for
		return nil
	}
	started := false
	s.shut.start.Do(func() {
		s.startShutdown()
		started = true
	})
	if started {
		// Logged after Do has returned, so a handler that calls Shutdown does not deadlock on
		// the Once (OBS-060), and before the sweep is scheduled, so it precedes shutdown finished.
		s.logShutdownStarted()
		s.guardians.push(sweepStart{}, nil)
	}
	// ACT-060: only a caller outside engine context on a blocking engine waits. Once the engine
	// has stopped, nothing calls it again (ENG-140).
	if s.shut.engStopped.Load() || !s.eng.Blocking || s.eng.Scheduler.InTurn() {
		return nil
	}
	if !s.shut.over.Load() {
		if err := s.eng.Waiter.Wait(ctx, s.shut.done); err != nil {
			s.logShutdownIncomplete(err)
			return err // unwrapped (ACT-060); the shutdown continues
		}
	}
	// ENG-140, ACT-070: the caller stops the engine before it returns nil, so the engine's
	// goroutines have exited. The shutdown completed in engine context, where the goroutine it
	// started may be stopping the engine already: then this call returns after that Stop.
	s.stopEngine()
	return nil
}

// startShutdown is the first Shutdown call's start (ACT-050): it marks the system shutting down,
// so every later Spawn returns ErrStopped, and records phase=begin. That call then schedules the
// guardians' sweep, which stops the actors in later engine callbacks (ACT-060).
func (s *System) startShutdown() {
	s.spawnMu.Lock()
	s.closing.Store(true)
	s.spawnMu.Unlock()
	if s.events != nil {
		s.shut.began = s.eng.Clock.Now()
	}
	s.recordShutdown("begin")
}

// completeShutdown runs once, in the guardians' turn after every actor has stopped (ACT-050): it
// marks the system stopped, resolves the pending asks with ErrStopped in ask-counter order
// (MSG-360), records phase=done, closes done and stops the engine (ENG-140).
func (s *System) completeShutdown() {
	err := s.errStopped()
	for _, ask := range s.asks.stop(&s.stopped) {
		s.settle(ask, resolution{how: byStop, from: s.user, err: err})
	}
	if s.events != nil {
		s.logShutdownFinished(s.eng.Clock.Now().Sub(s.shut.began))
	}
	s.recordShutdown("done")
	s.shut.over.Store(true)
	close(s.shut.done)

	// ENG-140: on a non-blocking engine directly, at the end of this engine callback; on a
	// blocking engine never from inside RunTurn (the engine's Stop panics in engine context,
	// ENG-460, SCH-200), so from a goroutine of its own.
	if !s.eng.Blocking {
		s.stopEngine()
		return
	}
	//lockstep:blocking the goroutine that calls Engine.Stop after a shutdown that completed in engine context on a blocking engine (ENG-140)
	go s.stopEngine()
}

// stopEngine stops the production overload machinery and then calls Engine.Stop, when the engine
// has one, at most once (ENG-140, OVL-700). A concurrent call returns after that Stop has
// returned.
func (s *System) stopEngine() {
	s.shut.engOnce.Do(func() {
		s.stopOverload()
		if s.eng.Stop != nil {
			s.eng.Stop()
		}
		s.shut.engStopped.Store(true)
	})
}

// recordShutdown emits lockstep.shutdown (ACT §8) with phase begin or done.
func (s *System) recordShutdown(phase string) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.shutdown",
		engine.Attr{Key: "node", Value: s.node},
		engine.Attr{Key: "phase", Value: phase},
	)
}
