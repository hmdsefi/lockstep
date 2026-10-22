// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package hooks_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/dettest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// TestMain runs the package's tests under goleak with the run-twice switch prepared (DET-155). The
// tests live in hooks_test so that importing dettest cannot form an import cycle (R39).
func TestMain(m *testing.M) { dettest.Main(m) }

// AT-ENG-10, ENG-200: nothing in this binary initializes package lockstep.
func TestATENG10HookUnset(t *testing.T) {
	if hooks.NewSystem != nil {
		t.Error("hooks.NewSystem != nil without package lockstep")
	}
}
