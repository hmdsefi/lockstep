// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
)

// AwaitInActor reports Reply.Await in actor code (LS110).
var AwaitInActor = &analysis.Analyzer{
	Name:     "awaitinactor",
	Doc:      "report Reply.Await in actor code (LS110)",
	Requires: []*analysis.Analyzer{actorsAnalyzer, indexAnalyzer},
	Run:      runAwaitInActor,
}

// idAwait is the check ID of AwaitInActor.
const idAwait = "LS110"

// awaitText is the message of an LS110 finding, without its ID and chain.
const awaitText = "Reply.Await blocks the actor; use lockstep.Pipe to receive the reply as a message"

func runAwaitInActor(pass *analysis.Pass) (any, error) {
	code, ok := pass.ResultOf[actorsAnalyzer].(*actorCode)
	if !ok {
		return nil, nil
	}
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	r := newReporter(pass, idx)
	for _, f := range code.Funcs {
		f.inspect(func(n ast.Node) bool {
			// Every use of Await is a selector: a call, a deferred call, a go statement, a method
			// value and a method expression all name the method through one (ANL-170).
			if sel, ok := n.(*ast.SelectorExpr); ok && isAPI(pass.TypesInfo.Uses[sel.Sel], "Reply.Await") {
				r.reportActor(sel.Sel.Pos(), idAwait, awaitText, f.Chain)
			}
			return true
		})
	}
	r.flush()
	return nil, nil
}
