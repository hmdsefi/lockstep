// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// SpawnOption configures one Spawn. WithMailbox (MBX), WithStrategy (SUP) and the throughput hint
// (SCH) are SpawnOptions.
type SpawnOption func(*spawnConfig)

// spawnConfig is what the SpawnOptions of one Spawn configure. Option appliers only set fields and
// report a bad value through fail; Spawn validates (ACT-300 step 3a). The mailbox (MBX) and the
// children's strategy (SUP) are set here; the other settings arrive with their owner plans (SCH).
type spawnConfig struct {
	mbx      mailboxConfig // WithMailbox's value, meaningful when mbxSet
	mbxSet   bool
	strategy Strategy // the strategy for the children of the spawned actor (SUP-370)
	err      error    // the first invalid option's error, in argument order
}

// fail records err, which wraps ErrInvalidOption, for the option being applied. Only the first
// call counts.
func (c *spawnConfig) fail(err error) {
	if c.err == nil {
		c.err = err
	}
}

// newSpawnConfig applies opts in argument order. It panics on a nil option (ACT-300).
func newSpawnConfig(opts []SpawnOption) *spawnConfig {
	c := &spawnConfig{strategy: OneForOne(nil)}
	for _, o := range opts {
		if o == nil {
			panic("lockstep: nil SpawnOption")
		}
		o(c)
	}
	return c
}

// mailbox returns the capacity and policy of the new actor's mailbox: its WithMailbox if given
// (the last one), else def, the system's default (MBX-010).
func (c *spawnConfig) mailbox(def mailboxConfig) mailboxConfig {
	if c.mbxSet {
		return c.mbx
	}
	return def
}

// spawnParent is what Spawn needs from a Spawner; exactly *System and *Context[X] implement it.
type spawnParent interface {
	Spawner
	// nilSpawner reports whether the receiver is a nil pointer (ACT-200).
	nilSpawner() bool
	// spawnParent returns the system, the parent's address, and the cell whose turn spawns (nil
	// for a System). It panics for a Context outside its turn (ACT-120).
	spawnParent() (sys *System, parent Address, from engine.ActorRef)
	// parentStopping reports whether the parent's incarnation is stopping (SUP-090): an actor
	// whose stop has started, in its Stopped hook. A System reports false; Shutdown has its own
	// flag (closing). Spawn calls it after spawnParent, so a Context is in its turn.
	parentStopping() bool
	// rebuilding reports whether the spawn is made from a rebuild context (OVL §3): the parent's
	// Started hook runs for a restart, or is the first Started of an actor spawned from a rebuild
	// context. A System reports false. Spawn calls it after spawnParent.
	rebuilding() bool
}

func (s *System) nilSpawner() bool { return s == nil }

// spawnParent returns the user guardian as the parent; a System spawns outside any turn.
//
//nolint:unparam // implements spawnParent, whose Context implementation returns the spawning cell
func (s *System) spawnParent() (*System, Address, engine.ActorRef) { return s, s.user, nil }

func (*System) parentStopping() bool { return false }

func (*System) rebuilding() bool { return false }

// Spawn creates a child of parent named name and returns its PID. name "" asks for a generated
// name "$<n>". factory is called at every start and restart, never by Spawn itself.
//
// Errors: ErrInvalidName, ErrInvalidOption, ErrStopped, ErrNameTaken, ErrActorCap and
// ErrOverloaded (OVL), each wrapped as in §7. Spawn panics if parent, factory or an option is nil.
// A spawn refused for any of these reasons registers nothing and takes no generated name.
func Spawn[M any](parent Spawner, name string, factory func() Actor[M], opts ...SpawnOption) (PID[M], error) {
	cfg := newSpawnConfig(opts)
	var p spawnParent
	if parent != nil {
		p, _ = parent.(spawnParent)
	}
	if p == nil || p.nilSpawner() {
		panic("lockstep: nil Spawner")
	}
	if factory == nil {
		panic("lockstep: Spawn with nil factory")
	}
	sys, parentAddr, from := p.spawnParent()
	return spawn(sys, parentAddr, from, p.parentStopping(), p.rebuilding(), name, factory, cfg)
}

// spawnSystem spawns a child of the system guardian, under /system (ACT-030). Only lockstep's own
// packages spawn there; no exported API does. It is a reserve spawn (OVL §3).
func spawnSystem[M any](sys *System, name string, factory func() Actor[M], opts ...SpawnOption) (PID[M], error) {
	cfg := newSpawnConfig(opts)
	if factory == nil {
		panic("lockstep: Spawn with nil factory")
	}
	return spawn(sys, sys.system, nil, false, false, name, factory, cfg)
}

// spawn runs ACT-300 from step 3 on: it checks the name, the options, the system's and the
// parent's state, the name's availability, the actor cap and admission, in that order (OVL-040),
// then registers the child, records the spawn, pushes begin and schedules the child (ACT-340,
// ENG-350). from is the cell whose turn spawns, or nil; stopping reports that it is the parent and
// is stopping (SUP-090); rebuild that the spawn is made from a rebuild context (OVL §3). A spawn
// from a rebuild context and a spawn of a system actor are reserve spawns.
func spawn[M any](sys *System, parent Address, from engine.ActorRef, stopping, rebuild bool, name string,
	factory func() Actor[M], cfg *spawnConfig,
) (PID[M], error) {
	typ := reflect.TypeFor[M]().String()
	refuse := func(err error) (PID[M], error) {
		sys.recordSpawnRefused(parent, name, typ, err)
		return PID[M]{}, err
	}
	if name != "" && !validName(name) {
		return refuse(fmt.Errorf("%w: %q", ErrInvalidName, name))
	}
	if cfg.err != nil {
		return refuse(cfg.err)
	}
	c := newCell(sys, parent, factory, cfg.mailbox(sys.mbx), cfg.strategy)
	c.rebuildSpawn = rebuild
	reserve := rebuild || spawnsSystemActor(parent.path)
	if err := register(sys, c, parent, stopping, reserve, name); err != nil {
		var capErr *capError
		var overloaded *overloadError
		switch {
		case errors.As(err, &capErr):
			sys.recordCap(capErr, reserve)
		case errors.As(err, &overloaded):
			sys.recordRefuse(overloaded)
		}
		return refuse(err)
	}
	sys.recordSpawn(parent, c.addr, typ)
	if sys.afterRegister != nil {
		sys.afterRegister(c)
	}
	c.pushSystem(begin{}, from)
	return PID[M]{addr: c.addr}, nil
}

// register runs ACT-300 steps 4 to 6 and registers c as parent's child named name (ACT-340). It
// holds the system's spawnMu for reading from the step 4 check through Registry.Add, so a child
// either sees the shutdown's closing mark or is registered before the shutdown lists the
// guardians' children (ACT-050). stopping reports that the parent is stopping (SUP-090): its stop
// sequence's registry removal then never finds a child.
//
// Step 6 is the actor cap, then admission (OVL-040), both with reserve, which reports a reserve
// spawn (OVL-030). Admission refuses a normal spawn while the system sheds (OVL-100) and never a
// reserve spawn: the runtime ignores a refusal of one, so rebuilds and system actors start whatever
// the engine answers (OVL-160). The cap slot is taken before Registry.Add allocates a generated
// name, so a refused spawn advances no name counter (ACT-320), and given back if admission refuses
// or Add loses the name to a concurrent spawn: a refused spawn changes no count.
func register[M any](sys *System, c *cell[M], parent Address, stopping, reserve bool, name string) error {
	sys.spawnMu.RLock()
	defer sys.spawnMu.RUnlock()
	if sys.closing.Load() || sys.discarded.Load() || stopping {
		return fmt.Errorf("%w: %s", ErrStopped, parent)
	}
	nameTaken := func() error { return fmt.Errorf("%w: %q under %s", ErrNameTaken, name, parent) }
	if name != "" && sys.reg.Taken(parent.path, name) {
		return nameTaken()
	}
	if ok, live, limit, rsv := sys.actors.Acquire(reserve); !ok {
		return &capError{parent: parent.path, live: live, limit: limit, reserve: rsv}
	}
	if err := sys.eng.Admission.AdmitSpawn(reserve); err != nil && !reserve {
		sys.actors.Release()
		return spawnOverloaded(parent.path, err)
	}
	// Hold c.mu until addr is set: whoever finds c through the registry takes c.mu before reading
	// addr (see cell). Add allocates the uid only when it registers c (ACT-040).
	c.mu.Lock()
	defer c.mu.Unlock()
	var uid uint64
	path, ok := sys.reg.Add(parent.path, name, func() uint64 {
		uid = sys.allocUID()
		return uid
	}, actorCell(c))
	if !ok {
		sys.actors.Release()
		return nameTaken() // a concurrent Spawn took the name after Taken
	}
	c.addr = Address{node: sys.node, path: path, uid: uid}
	return nil
}

// recordSpawn emits lockstep.spawn for a successful spawn (ACT §8). name is the child's name,
// generated or not.
func (s *System) recordSpawn(parent, child Address, typ string) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.spawn",
		engine.Attr{Key: "parent", Value: parent.String()},
		engine.Attr{Key: "addr", Value: child.String()},
		engine.Attr{Key: "name", Value: child.path[len(parent.path)+1:]},
		engine.Attr{Key: "type", Value: typ},
		engine.Attr{Key: "result", Value: "ok"},
	)
}

// recordSpawnRefused emits lockstep.spawn for a spawn refused at ACT-300 steps 3 to 6 (ACT §8):
// name is the requested name, possibly empty, and result the error's message.
func (s *System) recordSpawnRefused(parent Address, name, typ string, err error) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.spawn",
		engine.Attr{Key: "parent", Value: parent.String()},
		engine.Attr{Key: "name", Value: name},
		engine.Attr{Key: "type", Value: typ},
		engine.Attr{Key: "result", Value: err.Error()},
	)
}
