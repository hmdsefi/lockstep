// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "testing"

// TestMustUse checks the LS220 corpus (AT-ANL-19, AT-ANL-20): the replies that are never used and
// the discarded Send and Spawn errors of ls220/bad, and nothing in ls220/good.
func TestMustUse(t *testing.T) {
	runCorpus(t, MustUse, "ls220/bad", "ls220/good")
}

// TestMustUseRequires checks that MustUse requires the directive index and the inspector and
// nothing else: it walks the syntax tree with type information and needs no SSA.
func TestMustUseRequires(t *testing.T) {
	want := []string{indexAnalyzer.Name, "inspect"}
	if len(MustUse.Requires) != len(want) {
		t.Fatalf("MustUse.Requires has %d analyzers, want %d", len(MustUse.Requires), len(want))
	}
	for i, a := range MustUse.Requires {
		if a.Name != want[i] {
			t.Errorf("MustUse.Requires[%d] = %s, want %s", i, a.Name, want[i])
		}
	}
}
