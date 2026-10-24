// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"strings"
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// runCorpus runs analyzer a on the corpus packages pkgs of testdata/src and checks the diagnostics
// against their want comments. It also checks the diagnostic format (ANL-110, AT-ANL-08): every
// diagnostic's Category equals the text of its Message before the first colon.
func runCorpus(t *testing.T, a *analysis.Analyzer, pkgs ...string) {
	t.Helper()
	for _, res := range analysistest.Run(t, analysistest.TestData(), a, pkgs...) {
		for _, d := range res.Diagnostics {
			if prefix, _, _ := strings.Cut(d.Message, ":"); d.Category != prefix {
				t.Errorf("%s: category %q, want %q (message %q)",
					res.Pass.Fset.Position(d.Pos), d.Category, prefix, d.Message)
			}
		}
	}
}
