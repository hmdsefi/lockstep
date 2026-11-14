// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import "sync"

// table holds the core of each system that has one, by the system (Config.System), so that
// package remote's Enable and Lookup find a system's core. Cores are only looked up by key.
var table = struct {
	mu    sync.Mutex
	cores map[any]*Core
}{cores: make(map[any]*Core)}

// Register makes c the core of its system, the Config.System it was built with, for CoreOf. It
// reports false, and changes nothing, when that system has a core already. Core.Shutdown removes
// the entry again.
func Register(c *Core) bool {
	table.mu.Lock()
	defer table.mu.Unlock()
	if _, ok := table.cores[c.sys]; ok {
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

// Unregister removes the core registered for sys, if it has one, without shutting it down: for a
// system that is discarded rather than shut down, such as a simulated node that crashed.
func Unregister(sys any) {
	table.mu.Lock()
	defer table.mu.Unlock()
	delete(table.cores, sys)
}

// unregister removes c from the table if it is still the core registered for sys.
func unregister(sys any, c *Core) {
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.cores[sys] == c {
		delete(table.cores, sys)
	}
}
