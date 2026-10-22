// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
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
	closing      atomic.Bool // shutting down or stopped: Spawn returns ErrStopped (ACT-050); set by Shutdown (1.2-12)

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
	}
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
