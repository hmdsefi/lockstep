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

// SkipEffectCancelAtStop makes a stop of an actor of sys leave its pending effects uncancelled,
// so the simulation's leak check finds them (AT-SIM-26). A restart still cancels them.
func SkipEffectCancelAtStop(sys *System) { sys.skipEffectCancel.Store(true) }

// SpawnSystem spawns a child of sys's system guardian, an actor under /system, which no exported
// API does (OVL §3), for the simulation tests of OVL-110.
func SpawnSystem[M any](sys *System, name string, factory func() Actor[M], opts ...SpawnOption) (PID[M], error) {
	return spawnSystem(sys, name, factory, opts...)
}

// RaceEnabled reports whether the test binary was built with -race, for the external tests that
// skip allocation counts under the race detector.
const RaceEnabled = raceEnabled

// ActorLoggerBuilt reports whether the actor logger of the actor registered as pid on sys has been
// built (OBS-030: Spawn does not build it). It panics if pid is not a registered actor of type M.
func ActorLoggerBuilt[M any](sys *System, pid PID[M]) bool {
	uid, a, ok := sys.reg.Lookup(pid.addr.path)
	c, isCell := a.(*cell[M])
	if !ok || uid != pid.addr.uid || !isCell {
		panic("lockstep: ActorLoggerBuilt: " + pid.String() + " is not a registered actor")
	}
	return c.log.Load() != nil
}
