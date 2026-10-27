// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/token"
	"go/types"
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// markerProbe reports "marked <marker> <T>" at every use of a marked type, once per marker. It
// asks the directive index, which holds the markers of this package and, through markerFact, of
// imported ones.
var markerProbe = &analysis.Analyzer{
	Name:     "markerprobe",
	Doc:      "report the markers of the types a package uses",
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
				tn, ok := pass.TypesInfo.Uses[id].(*types.TypeName)
				if !ok || tn.Pkg() == nil {
					return true
				}
				name := tn.Name()
				if tn.Pkg() != pass.Pkg {
					name = tn.Pkg().Name() + "." + name
				}
				if idx.hasMarker(tn, markImmutable) {
					pass.Reportf(id.Pos(), "marked immutable %s", name)
				}
				if idx.hasMarker(tn, markResource) {
					pass.Reportf(id.Pos(), "marked resource %s", name)
				}
				return true
			})
		}
		return nil, nil
	},
}

// TestMarkerFacts checks the markerFact of ANL-090: a package sees the markers of the types it
// imports, and of its own types, through the directive index.
func TestMarkerFacts(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), markerProbe, "facts/importer")
}

// refsProbe reports "refs=<bool>" at every package-level type spec: containsRefs of its type.
var refsProbe = &analysis.Analyzer{
	Name:     "refsprobe",
	Doc:      "report containsRefs of every package-level type",
	Requires: []*analysis.Analyzer{indexAnalyzer},
	Run: func(pass *analysis.Pass) (any, error) {
		for _, f := range pass.Files {
			for _, decl := range f.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.TYPE {
					continue
				}
				for _, spec := range gd.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					if tn, ok := pass.TypesInfo.Defs[ts.Name].(*types.TypeName); ok {
						pass.Reportf(ts.Name.Pos(), "refs=%t", containsRefs(pass, tn.Type()))
					}
				}
			}
		}
		return nil, nil
	},
}

// TestContainsRefs checks ANL-210 with the refs corpus.
func TestContainsRefs(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), refsProbe, "refs")
}
