// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// apiEntries lists the entries of the API table (ANL-020) in the table's order.
var apiEntries = []string{
	"Actor", "Context", "Context.Own", "Context.After", "Context.Every",
	"Reply", "Reply.Await", "ReplyTo", "ReplyTo.Send", "PID", "Address",
	"Send", "Ask", "Spawn", "Pipe", "Do",
}

// apiprobe reports "api <entry>" at every identifier that refers to an object of the API table.
var apiprobe = &analysis.Analyzer{
	Name: "apiprobe",
	Doc:  "report identifiers that refer to lockstep API-table objects",
	Run: func(pass *analysis.Pass) (any, error) {
		for _, f := range pass.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if !ok {
					return true
				}
				obj := pass.TypesInfo.Uses[id]
				for _, entry := range apiEntries {
					if isAPI(obj, entry) {
						pass.Reportf(id.Pos(), "api %s", entry)
					}
				}
				return true
			})
		}
		return nil, nil
	},
}

// TestAPITable checks ANL-020 with the apitable corpus: every entry is found through calls,
// instantiations, method values and type names, and same-named objects of another package are not.
func TestAPITable(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), apiprobe, "apitable")
}
