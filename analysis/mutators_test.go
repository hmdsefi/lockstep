// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// mutatorProbe reports "mutator <op>" at every call of an in-place mutator and "builtin <name>"
// at every call of a writing builtin.
var mutatorProbe = &analysis.Analyzer{
	Name: "mutatorprobe",
	Doc:  "report calls of in-place mutators and writing builtins",
	Run: func(pass *analysis.Pass) (any, error) {
		for _, f := range pass.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if op, ok := inPlaceMutator(pass.TypesInfo, call); ok {
					pass.Reportf(call.Pos(), "mutator %s", op)
				}
				if name, ok := builtinWrite(pass.TypesInfo, call); ok {
					pass.Reportf(call.Pos(), "builtin %s", name)
				}
				return true
			})
		}
		return nil, nil
	},
}

// TestInPlaceMutators checks the in-place mutators and writing builtins of ANL-260 with the
// mutators corpus: each one by its <pkg>.<Name> or builtin name, and nothing for functions that
// only share a name. The probe's texts have no LSnnn prefix, so it runs through analysistest.Run
// rather than runCorpus.
func TestInPlaceMutators(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), mutatorProbe, "mutators")
}
