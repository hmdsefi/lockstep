// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"cmp"
	"go/ast"
	"go/types"
	"slices"
	"strings"
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// TestDirectives checks AT-ANL-05 and AT-ANL-06: every ANL-100 message appears with its exact
// text in the bad packages, and the good packages get none.
func TestDirectives(t *testing.T) {
	runCorpus(t, Directives, "ls001/bad", "ls001/good", "ls002/bad", "ls002/good")
}

// TestDirectivesOrder checks that Directives reports in position order, and in list order for the
// IDs of one directive (ANL-120).
func TestDirectivesOrder(t *testing.T) {
	for _, res := range analysistest.Run(t, analysistest.TestData(), Directives, "ls001/bad", "ls002/bad") {
		if !slices.IsSortedFunc(res.Diagnostics, func(a, b analysis.Diagnostic) int { return cmp.Compare(a.Pos, b.Pos) }) {
			t.Errorf("%s: diagnostics are not in position order", res.Pass.Pkg.Path())
		}
	}
	res := analysistest.Run(t, analysistest.TestData(), Directives, "ls002/bad")
	ds := res[0].Diagnostics
	a := slices.IndexFunc(ds, func(d analysis.Diagnostic) bool { return strings.Contains(d.Message, "LS998") })
	if a < 0 || a+1 >= len(ds) || !strings.Contains(ds[a+1].Message, "LS999") || ds[a].Pos != ds[a+1].Pos {
		t.Errorf("the LS998 diagnostic is not followed by LS999 at the same position: %v", ds)
	}
}

// indexProbe reports what the directive index says about the index corpus: "suppressed" or
// "visible" at every use of the constant probe, and "marker <names>" at every marked type.
var indexProbe = &analysis.Analyzer{
	Name:     "indexprobe",
	Doc:      "report the allow scopes and markers of the directive index",
	Requires: []*analysis.Analyzer{indexAnalyzer},
	Run: func(pass *analysis.Pass) (any, error) {
		idx, ok := indexOf(pass)
		if !ok {
			return nil, nil
		}
		for _, f := range pass.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if !ok {
					return true
				}
				if id.Name == "probe" && pass.TypesInfo.Uses[id] != nil {
					msg := "visible"
					if idx.suppressed(id.Pos(), "LS100") {
						msg = "suppressed"
					}
					pass.Report(analysis.Diagnostic{Pos: id.Pos(), Message: msg})
				}
				if tn, ok := pass.TypesInfo.Defs[id].(*types.TypeName); ok && idx.markers[tn] != 0 {
					names := []string{"marker"}
					if idx.markers[tn]&markImmutable != 0 {
						names = append(names, "immutable")
					}
					if idx.markers[tn]&markResource != 0 {
						names = append(names, "resource")
					}
					pass.Report(analysis.Diagnostic{Pos: id.Pos(), Message: strings.Join(names, " ")})
				}
				return true
			})
		}
		return nil, nil
	},
}

// TestDirectiveIndex checks the three allow scopes of ANL-080, that malformed directives suppress
// nothing, and the marker bindings of ANL-090.
func TestDirectiveIndex(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), indexProbe, "dirindex")
}
