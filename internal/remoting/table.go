// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import "sync"

// table holds the core of each system that has one, by the system (Config.System), so that
// package remote's Enable and Lookup find a system's core. It also holds a stopped marker for each
// simulated system whose core shut down or whose node crashed, so that Enable on it reports the
// system stopped rather than enabling it anew (REM-040). Both maps are only read by key.
var table = struct {
	mu      sync.Mutex
	cores   map[any]*Core
	stopped map[any]bool
}{cores: make(map[any]*Core), stopped: make(map[any]bool)}

// Register makes c the core of its system, the Config.System it was built with, for CoreOf. It
// reports false, and changes nothing, when that system has a core or a stopped marker already.
// Core.Shutdown removes the entry again; for a simulated core it leaves a stopped marker.
func Register(c *Core) bool {
	table.mu.Lock()
	defer table.mu.Unlock()
	if _, ok := table.cores[c.sys]; ok || table.stopped[c.sys] {
		return false
	}
	table.cores[c.sys] = c
	return true
}

// CoreOf returns the core registered for sys, or nil when it has none. sys is a *lockstep.System.
func CoreOf(sys any) *Core {
	table.mu.Lock()
	defer table.mu.Unlock()
	return table.cores[sys]
}

// Stopped reports whether sys has a stopped marker: it is a simulated system whose core shut down
// or whose node crashed (MarkStopped).
func Stopped(sys any) bool {
	table.mu.Lock()
	defer table.mu.Unlock()
	return table.stopped[sys]
}

// MarkStopped replaces the core registered for sys, if any, with a stopped marker, without shutting
// the core down: for a simulated system that is discarded because its node crashed. Unregister
// removes the marker.
func MarkStopped(sys any) {
	table.mu.Lock()
	defer table.mu.Unlock()
	delete(table.cores, sys)
	table.stopped[sys] = true
}

// Unregister removes the core registered for sys, or its stopped marker, without shutting a core
// down: for a system that is discarded rather than shut down, and for every system of a
// simulation run when the run ends.
func Unregister(sys any) {
	table.mu.Lock()
	defer table.mu.Unlock()
	delete(table.cores, sys)
	delete(table.stopped, sys)
}

// unregister removes c from the table if it is still the core registered for sys, leaving a
// stopped marker when c is simulated.
func unregister(sys any, c *Core) {
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.cores[sys] == c {
		delete(table.cores, sys)
		if c.simulated {
			table.stopped[sys] = true
		}
	}
}
