// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"regexp"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestExhaustive checks the LS230 corpus (AT-ANL-21, AT-ANL-22): sealed members and self-message
// types with no case in ls230/bad, none in ls230/good, and the exported members of an imported
// sealed interface, read from its fact, in ls230/remote. The facts list the exported members only.
func TestExhaustive(t *testing.T) {
	runCorpus(t, Exhaustive, "ls230/bad", "ls230/good")
	runCorpus(t, Exhaustive, "ls230/remote", "ls230/msgs")
}

// TestExhaustiveOrder checks ANL-330's order: the findings at one position are sorted by the
// type string of the uncovered type.
func TestExhaustiveOrder(t *testing.T) {
	typeOf := regexp.MustCompile(`message type (\S+?),? (of|which) `)
	for _, res := range analysistest.Run(t, analysistest.TestData(), Exhaustive, "ls230/bad", "ls230/remote") {
		ds := res.Diagnostics
		for i := 1; i < len(ds); i++ {
			if ds[i].Pos != ds[i-1].Pos {
				continue
			}
			prev := typeOf.FindStringSubmatch(ds[i-1].Message)
			cur := typeOf.FindStringSubmatch(ds[i].Message)
			if prev == nil || cur == nil || prev[1] >= cur[1] {
				t.Errorf("%s: %q reported before %q", res.Pass.Fset.Position(ds[i].Pos), ds[i-1].Message, ds[i].Message)
			}
		}
	}
}
