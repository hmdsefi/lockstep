// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/engine/local"
)

// newManualSystem returns a system built through the ACT-015 constructor on a manual engine for
// node "local" with entropy seed 1 (ACT §10, "manual system").
func newManualSystem(t *testing.T) (*System, *enginetest.Engine) {
	t.Helper()
	eng := enginetest.New("local", 1)
	sys, err := newSystem(eng.Engine(), nil)
	if err != nil {
		t.Fatalf("newSystem on a manual engine: %v", err)
	}
	return sys, eng
}

// newRecordingSystem returns a system built through the ACT-015 constructor on a local engine
// whose tracer is a new recorder (ACT §10, "recording tracer"). The test stops the engine.
func newRecordingSystem(t *testing.T) (*System, *enginetest.Recorder) {
	t.Helper()
	rec := enginetest.NewRecorder()
	le, err := local.New(local.Config{Node: "local", Tracer: rec})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	sys, err := newSystem(le.Engine(), nil)
	if err != nil {
		le.Stop()
		t.Fatalf("newSystem on a local engine: %v", err)
	}
	stopEngineAtEnd(t, sys)
	return sys, rec
}

// newLocalSystem returns New(opts...) and stops its engine when the test ends.
func newLocalSystem(t *testing.T, opts ...Option) *System {
	t.Helper()
	sys := New(opts...)
	stopEngineAtEnd(t, sys)
	return sys
}

// stopEngineAtEnd stops sys's engine when the test ends, so goleak finds no engine goroutine. Until
// System has a Shutdown method this is the only way to stop a local system.
func stopEngineAtEnd(t *testing.T, sys *System) {
	t.Helper()
	t.Cleanup(func() {
		if sys.eng.Stop != nil {
			sys.eng.Stop()
		}
	})
}
