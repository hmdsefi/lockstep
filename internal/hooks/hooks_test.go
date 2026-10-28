// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package hooks_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/dettest"
	"github.com/hmdsefi/lockstep/internal/engine"
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

// AT-SIM-32 (the unset half), ENG-210: nothing in this binary initializes package lockstep, so
// none of the hooks it sets is set: SIM's, SFT's, PER's and OBS's.
func TestHooksUnset(t *testing.T) {
	if hooks.Discard != nil {
		t.Error("hooks.Discard != nil without package lockstep")
	}
	if hooks.AskResolved != nil {
		t.Error("hooks.AskResolved != nil without package lockstep")
	}
	if hooks.PendingAsks != nil {
		t.Error("hooks.PendingAsks != nil without package lockstep")
	}
	if hooks.Address != nil {
		t.Error("hooks.Address != nil without package lockstep")
	}
	if hooks.Control != nil {
		t.Error("hooks.Control != nil without package lockstep")
	}
	if hooks.Signal != nil {
		t.Error("hooks.Signal != nil without package lockstep")
	}
	if hooks.InjectFail != nil {
		t.Error("hooks.InjectFail != nil without package lockstep")
	}
	if hooks.MetricCounter != nil {
		t.Error("hooks.MetricCounter != nil without package lockstep")
	}
	if hooks.MetricGauge != nil {
		t.Error("hooks.MetricGauge != nil without package lockstep")
	}
	if got := (hooks.PendingAsk{}); got.ByActor || got.Ask != (engine.Address{}) {
		t.Errorf("zero PendingAsk = %+v", got)
	}
}
