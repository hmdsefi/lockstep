// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/overload"
)

func TestHysteresis(t *testing.T) {
	h := overload.Hysteresis{High: 900, Low: 800}
	for i, step := range []struct {
		v       uint64
		changed bool
		on      bool
	}{
		{899, false, false},
		{900, true, true},
		{850, false, true},
		{800, true, false},
		{799, false, false},
	} {
		changed, on := h.Update(step.v)
		if changed != step.changed || on != step.on {
			t.Errorf("step %d: Update(%d) = (changed %v, on %v); want (%v, %v)",
				i, step.v, changed, on, step.changed, step.on)
		}
	}
}

// Above High while on, and below Low while off, report nothing.
func TestHysteresisStaysPut(t *testing.T) {
	h := overload.Hysteresis{High: 10, Low: 5}
	if changed, on := h.Update(0); changed || on {
		t.Fatalf("Update(0) = (%v, %v); want (false, false)", changed, on)
	}
	h.Update(10)
	if changed, on := h.Update(1000); changed || !on {
		t.Fatalf("Update(1000) while on = (%v, %v); want (false, true)", changed, on)
	}
	h.Update(5)
	if changed, on := h.Update(0); changed || on {
		t.Fatalf("Update(0) while off = (%v, %v); want (false, false)", changed, on)
	}
}
