// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"testing"

	"golang.org/x/tools/go/analysis"
)

// TestAll checks AT-ANL-25 (Phase 1): All returns Directives, Determinism and AwaitInActor in
// check-ID order with the names and Doc strings of ANL §4.1, the set passes analysis.Validate, and
// every call returns a new slice holding the same analyzer values (ANL-390).
func TestAll(t *testing.T) {
	want := []struct {
		analyzer *analysis.Analyzer
		name     string
		doc      string
	}{
		{Directives, "directives", "report malformed and unknown //lockstep: directives (LS001, LS002)"},
		{Determinism, "determinism", "report nondeterminism and blocking in actor code (LS100)"},
		{AwaitInActor, "awaitinactor", "report Reply.Await in actor code (LS110)"},
	}

	got := All()
	if len(got) != len(want) {
		t.Fatalf("All() has %d analyzers, want %d", len(got), len(want))
	}
	for i, w := range want {
		a := got[i]
		if a != w.analyzer {
			t.Errorf("All()[%d] is %q, want %q", i, a.Name, w.name)
		}
		if a.Name != w.name {
			t.Errorf("All()[%d].Name = %q, want %q", i, a.Name, w.name)
		}
		if a.Doc != w.doc {
			t.Errorf("All()[%d].Doc = %q, want %q", i, a.Doc, w.doc)
		}
	}

	if err := analysis.Validate(got); err != nil {
		t.Errorf("analysis.Validate(All()) = %v, want nil", err)
	}

	again := All()
	if &again[0] == &got[0] {
		t.Fatal("two calls of All() return the same slice")
	}
	got[0] = nil // a shared backing array would show up in again
	for i, w := range want {
		if again[i] != w.analyzer {
			t.Errorf("second All()[%d] is %v, want %q", i, again[i], w.name)
		}
	}
}
