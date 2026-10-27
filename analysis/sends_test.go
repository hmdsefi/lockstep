// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/types"
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// sendProbe reports "send <api>" at every send site of every function and function literal, and
// "leaf <expr> root=<v> shared=<bool>" at every leaf of the site's messages. A leaf without a root
// reports root=-.
var sendProbe = &analysis.Analyzer{
	Name: "sendprobe",
	Doc:  "report send sites and the leaves of their messages",
	Run: func(pass *analysis.Pass) (any, error) {
		for _, f := range pass.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				switch n.(type) {
				case *ast.FuncDecl, *ast.FuncLit:
				default:
					return true
				}
				for _, s := range sendSites(pass, n) {
					pass.Reportf(s.call.Pos(), "send %s", s.api)
					for _, msg := range s.msgs {
						for _, l := range placed(pass, n, msg) {
							root := "-"
							if l.root != nil {
								root = l.root.Name
							}
							pass.Reportf(l.expr.Pos(), "leaf %s root=%s shared=%t", types.ExprString(l.expr), root, l.shared)
						}
					}
				}
				return true
			})
		}
		return nil, nil
	},
}

// TestPlacedExpressions checks ANL-200 with the sendsites corpus: the send sites of lockstep.Send,
// ReplyTo.Send and lockstep.Ask (and not of ctx.After, ctx.Every, lockstep.Do or lockstep.Pipe),
// and the leaves, roots and shared marks of the expressions they place in messages.
func TestPlacedExpressions(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), sendProbe, "sendsites")
}
