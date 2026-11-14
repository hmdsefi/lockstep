// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/persist/filestore"
)

// ApplyEnv exposes applyEnv to the external tests.
var ApplyEnv = applyEnv

// The named checks, for the external tests.
const (
	CheckMisuse          = checkMisuse
	CheckNoEffectHandler = checkNoEffectHandler
	CheckLeak            = checkLeak
	CheckDeadlock        = checkDeadlock
)

// Store exposes store to the external tests.
func (s *Sim) Store(check string, err error) { s.store(check, err) }

// EngineBundle is the engine bundle type, for the external tests.
type EngineBundle = engine.Engine

// Stored returns the failure stored in the slot of check, or nil.
func (s *Sim) Stored(check string) error { return s.slots[slot(check)] }

// Engine returns the engine bundle of node name's latest boot, also when that boot built no
// system, or the zero bundle before the first boot.
func (s *Sim) Engine(name string) engine.Engine {
	if e := s.lookup(name).eng; e != nil {
		return e.bundle
	}
	return engine.Engine{}
}

// Transport returns the transport that the boot of the engine bundle b attached to its system:
// the remoting core, behind the engine's checks (SIM-120).
func (s *Sim) Transport(b engine.Engine) engine.Transport {
	e, ok := b.Scheduler.(*simEngine)
	if !ok {
		panic("Transport: not a simulation engine bundle")
	}
	return &transport{e: e, core: e.core}
}

// Connect has the remoting core of node from's current system dial node to, as a send would.
func (s *Sim) Connect(from, to string) error { return s.lookup(from).eng.core.Connect(to) }

// CloseUnderlying closes the simdisk handle behind f, a File of a Volume, without the Volume
// seeing it, so that f's own Close returns an error.
func CloseUnderlying(f filestore.File) error {
	vf, ok := f.(*volFile)
	if !ok {
		panic("CloseUnderlying: not a File of a Volume")
	}
	return vf.f.Close()
}
