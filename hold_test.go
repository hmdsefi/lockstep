// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "testing"

// holdAt keeps the cell registered at addr on sys from running turns until the returned function
// is called (MBX §10, "hold"). It reports false if no actor is registered at addr.
func holdAt(sys *System, addr Address) (release func(), ok bool) {
	uid, a, found := sys.reg.Lookup(addr.Path())
	if !found || uid != addr.UID() {
		return nil, false
	}
	h, ok := a.(interface{ holdTurns() func() })
	if !ok {
		return nil, false
	}
	return h.holdTurns(), true
}

// hold is holdAt for package tests: it fails the test if no actor is registered at addr.
func hold(t *testing.T, sys *System, addr Address) (release func()) {
	t.Helper()
	release, ok := holdAt(sys, addr)
	if !ok {
		t.Fatalf("hold: %v is not registered", addr)
	}
	return release
}
