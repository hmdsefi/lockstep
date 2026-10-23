// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

// UserGuardianUID returns the uid of sys's user guardian, for the external tests.
func UserGuardianUID(sys *System) uint64 { return sys.user.UID() }

// SystemGuardianUID returns the uid of sys's system guardian, for the external tests.
func SystemGuardianUID(sys *System) uint64 { return sys.system.UID() }

// Hold keeps the cell registered at addr on sys from running turns until release is called, so its
// mailbox fills without being consumed (the hold hook of MBX §10). It panics if no actor is
// registered at addr.
func Hold(sys *System, addr Address) (release func()) {
	release, ok := holdAt(sys, addr)
	if !ok {
		panic("lockstep: Hold: " + addr.String() + " is not a registered actor")
	}
	return release
}

// EngineBlocking reports the Blocking flag of the engine bundle sys was built on, for the
// simulation tests (SIM-220).
func EngineBlocking(sys *System) bool { return sys.eng.Blocking }

// SpawnSystem spawns a child of sys's system guardian, an actor under /system, which no exported
// API does (OVL §3), for the simulation tests of OVL-110.
func SpawnSystem[M any](sys *System, name string, factory func() Actor[M], opts ...SpawnOption) (PID[M], error) {
	return spawnSystem(sys, name, factory, opts...)
}
