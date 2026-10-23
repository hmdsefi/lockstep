// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import "testing"

// SIM-290: release forgets each released effect, and an actor whose last held effect is released
// leaves the engine's map, so an actor that ran effects and never stops holds no entry.
func TestReleaseForgetsEmptyActors(t *testing.T) {
	e := &simEngine{}
	a1, a2, b := &heldEffect{id: 1}, &heldEffect{id: 2}, &heldEffect{id: 3}
	e.hold("a", a1)
	e.hold("a", a2)
	e.hold("b", b)

	e.release("a", a1)
	if got := e.held["a"]; len(got) != 1 || got[0] != a2 {
		t.Errorf("held for a after releasing effect 1: %v, want effect 2", got)
	}
	e.release("a", a2)
	e.release("b", b)
	if len(e.held) != 0 {
		t.Errorf("engine holds effects for %d actors after every release, want 0", len(e.held))
	}
}
