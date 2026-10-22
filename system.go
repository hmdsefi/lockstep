// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/local"
	"github.com/hmdsefi/lockstep/internal/registry"
)

// System is one runtime instance. All of its exported functions and methods are safe for
// concurrent use when *System is the Spawner or Sender.
type System struct {
	eng         engine.Engine
	node        string
	incarnation uint32 // Engine.Epoch; constant for the system's life (ACT-045)

	reg          *registry.Registry
	user, system Address     // the guardians; neither is registered (ACT-030)
	closing      atomic.Bool // shutting down or stopped: Spawn returns ErrStopped (ACT-050); set by Shutdown
	// stopped is set when both guardians' children have stopped (ACT-050): Send, Ask and NewReply
	// return ErrStopped and reply attempts are dropped (MSG-100, MSG-300, MSG-380, MSG-410). It is
	// set under the ask table's lock (askTable.stop).
	stopped atomic.Bool
	// spawnMu makes Spawn's closing check atomic with the child's registration: Spawn holds it for
	// reading from the check through Registry.Add, and Shutdown sets closing holding it for writing.
	// A child that registers is therefore one the shutdown's guardian sweep finds (ACT-050).
	spawnMu sync.RWMutex
	shut    shutdownState

	asks askTable // pending asks of this system's askers and tracked asks of its actors (MSG §11)

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

// New builds a system on the local engine (Phase 1) or the production engine (Phase 2, SCH),
// creates its two guardians and returns it. It panics with an error wrapping ErrInvalidOption if
// an option is nil or invalid.
func New(opts ...Option) *System {
	// The local engine needs the node name before it starts, so validate the options first: an
	// invalid option panics before any goroutine exists.
	cfg, err := newConfig(opts, "")
	if err != nil {
		panic(err)
	}
	le, err := local.New(local.Config{Node: cfg.node})
	if err != nil {
		panic(err) // unreachable: the node name is valid and the throughput is the default
	}
	sys, err := newSystem(le.Engine(), opts)
	if err != nil {
		le.Stop()
		panic(err) // unreachable: the options passed newConfig above
	}
	return sys
}

// Node returns the system's node name: the node part of every address it allocates.
func (s *System) Node() string { return s.node }

// Incarnation returns the epoch the system started with: the faultline node incarnation in
// simulation, a random value otherwise. It is never 0 and does not change for the system's life,
// even when uid allocation moves to a new epoch (ACT-040).
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
		incarnation: eng.Epoch,
		reg:         registry.New(),
		epoch:       eng.Epoch,
		seq:         1,
		epochs:      []uint32{eng.Epoch},
		shut:        shutdownState{done: make(chan struct{})},
	}
	s.shut.sweep.sys = s
	// ACT-030: the user guardian first, then the system guardian, with uids 1 and 2 of the epoch.
	s.user = Address{node: s.node, path: "/user", uid: s.allocUID()}
	s.system = Address{node: s.node, path: "/system", uid: s.allocUID()}
	s.reg.AddRoot(registry.Addr{Node: s.user.node, Path: s.user.path, UID: s.user.uid})
	s.reg.AddRoot(registry.Addr{Node: s.system.node, Path: s.system.path, UID: s.system.uid})
	// Admission's change and usage callbacks are OVL's (plan 1.3); until then nothing changes and
	// nothing is used.
	eng.Admission.Bind(func(engine.AdmissionChange) {}, func() engine.Usage { return engine.Usage{} })
	return s, nil
}

// shutdownState is the progress of a system's shutdown (ACT-050, ACT-060, ENG-140).
type shutdownState struct {
	start sync.Once     // the first Shutdown call starts the shutdown; later calls never start another
	sweep guardianSweep // stops the guardians' children, in engine context
	done  chan struct{} // closed when the system is stopped and phase=done is recorded (ACT-060)
	over  atomic.Bool   // done is closed

	engOnce    sync.Once   // Engine.Stop is called at most once (ENG-140)
	engStopped atomic.Bool // Engine.Stop has returned (or the engine has none)
}

// Shutdown stops every actor, user actors first, then system actors, and waits until every
// Stopped hook has returned. It returns nil when the system is stopped, or ctx.Err() if ctx ends
// first; shutdown then continues and a later call waits again. Called during a turn (an actor
// may shut down its own system) or on an engine that cannot block, such as the simulation
// engine, it starts the shutdown and returns nil at once without waiting (ACT-060).
func (s *System) Shutdown(ctx context.Context) error {
	s.shut.start.Do(s.startShutdown)
	// ACT-060: only a caller outside engine context on a blocking engine waits. Once the engine
	// has stopped, nothing calls it again (ENG-140).
	if s.shut.engStopped.Load() || !s.eng.Blocking || s.eng.Scheduler.InTurn() {
		return nil
	}
	if !s.shut.over.Load() {
		if err := s.eng.Waiter.Wait(ctx, s.shut.done); err != nil {
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
// so every later Spawn returns ErrStopped, records phase=begin and schedules the guardian sweep,
// which stops the actors in later engine callbacks (ACT-060).
func (s *System) startShutdown() {
	s.spawnMu.Lock()
	s.closing.Store(true)
	s.spawnMu.Unlock()
	s.recordShutdown("begin")
	s.shut.sweep.push(sweepMsg{start: true}, nil)
}

// guardianChildStopped is a guardian's stopped notice (SUP-060 step 7): child, a child of the
// user or the system guardian, completed its final stop during the turn of from. During a
// shutdown the notice is queued for the guardian sweep, which handles it in a later turn of its
// own, as a parent cell handles its children's notices. Before a shutdown no guardian waits.
func (s *System) guardianChildStopped(child Address, from engine.ActorRef) {
	if s.closing.Load() {
		s.shut.sweep.push(sweepMsg{child: child}, from)
	}
}

// guardianSweep stops the guardians' children for Shutdown (ACT-050): the user guardian's children,
// then, after every one of them has completed its stop, the system guardian's, each guardian's one
// subtree at a time in reverse spawn order, with reason Shutdown. The guardians have no cell (ACT-030);
// the sweep is the engine.ActorRef that runs their part of SUP-060 step 2 in engine context. Its
// queue takes the start and the guardians' stopped notices from any goroutine; the engine
// schedules it like a cell (ENG-350), so a notice is handled after the turns already scheduled.
type guardianSweep struct {
	sys  *System
	slot engine.Slot

	mu sync.Mutex
	q  fifo[sweepMsg] // guarded by mu

	// turn-owned
	phase shutdownPhase
	wait  Address // the guardian's child whose final stop the sweep waits for, or zero
}

// sweepMsg is one message of the guardian sweep: the start, or a guardian child's stopped notice.
type sweepMsg struct {
	start bool
	child Address
}

// shutdownPhase is the guardian whose children the sweep stops (ACT-050).
type shutdownPhase uint8

const (
	sweepIdle   shutdownPhase = iota // not started
	sweepUser                        // stopping the user guardian's children
	sweepSystem                      // stopping the system guardian's children
	sweepDone                        // every guardian child has stopped
)

// Address returns the user guardian's address (engine.ActorRef).
func (g *guardianSweep) Address() engine.Address { return g.sys.user.engine() }

// Slot returns the sweep's scheduling state (engine.ActorRef).
func (g *guardianSweep) Slot() *engine.Slot { return &g.slot }

// push queues m and schedules the sweep (ENG-350). from is the cell whose turn pushes, or nil.
func (g *guardianSweep) push(m sweepMsg, from engine.ActorRef) {
	g.mu.Lock()
	g.q.push(m)
	g.mu.Unlock()
	g.sys.ready(g, from)
}

func (g *guardianSweep) pop() (sweepMsg, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.q.pop()
}

// RunTurn handles every queued message (engine.ActorRef). It runs no actor code, records no
// lockstep.turn and never blocks. After the last guardian child has stopped it completes the
// shutdown.
func (g *guardianSweep) RunTurn(int) bool {
	for {
		m, ok := g.pop()
		if !ok {
			return false
		}
		switch {
		case m.start && g.phase == sweepIdle:
			g.phase = sweepUser
		case !m.start && (g.phase == sweepUser || g.phase == sweepSystem) && m.child == g.wait:
			g.wait = Address{}
		default:
			continue // a notice of a child the sweep does not wait for: it has left the registry
		}
		if g.next() {
			g.phase = sweepDone
			g.sys.completeShutdown()
		}
	}
}

// next asks the most recently spawned child of the current phase's guardian to stop with reason
// Shutdown and waits for its notice; with no child left it moves from the user guardian to the
// system guardian, and after that reports true. A child whose stop has started already, for any
// reason, keeps its reason and still notifies (ACT-710).
func (g *guardianSweep) next() bool {
	s := g.sys
	for {
		parent := s.user
		if g.phase == sweepSystem {
			parent = s.system
		}
		kids := s.reg.Children(parent.path)
		if len(kids) == 0 {
			if g.phase == sweepUser {
				g.phase = sweepSystem
				continue
			}
			return true
		}
		k := kids[len(kids)-1]
		uid, a, ok := s.reg.Lookup(k.Path)
		child, _ := a.(actorCell)
		if !ok || child == nil || uid != k.UID {
			continue // it completed its stop after Children returned
		}
		g.wait = Address{node: k.Node, path: k.Path, uid: k.UID}
		child.requestStop(Shutdown, g)
		return false
	}
}

// completeShutdown runs once, in the sweep's turn after every actor has stopped (ACT-050): it
// marks the system stopped, resolves the pending asks with ErrStopped in ask-counter order
// (MSG-360), records phase=done, closes done and stops the engine (ENG-140).
func (s *System) completeShutdown() {
	err := s.errStopped()
	for _, ask := range s.asks.stop(&s.stopped) {
		s.settle(ask, resolution{how: byStop, from: s.user, err: err})
	}
	s.recordShutdown("done")
	s.shut.over.Store(true)
	close(s.shut.done)

	// ENG-140: on a non-blocking engine directly, at the end of this engine callback; on a
	// blocking engine never from inside RunTurn (the local engine's Stop panics there, ENG-460),
	// so from a goroutine of its own.
	if !s.eng.Blocking {
		s.stopEngine()
		return
	}
	//lockstep:blocking the goroutine that calls Engine.Stop after a shutdown that completed in engine context on a blocking engine (ENG-140)
	go s.stopEngine()
}

// stopEngine calls Engine.Stop, when the engine has one, at most once (ENG-140). A concurrent
// call returns after that Stop has returned.
func (s *System) stopEngine() {
	s.shut.engOnce.Do(func() {
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
