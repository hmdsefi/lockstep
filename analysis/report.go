// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"cmp"
	"go/ast"
	"go/token"
	"slices"

	"golang.org/x/tools/go/analysis"
)

// reporter collects the diagnostics of one analyzer run on one package and reports them in
// position order (ANL-110, ANL-120). A diagnostic is dropped when its position is in a generated
// file, when a //lockstep:allow directive suppresses it (ANL-080), or when the same position, ID
// and message was added before.
type reporter struct {
	pass      *analysis.Pass
	idx       *directiveIndex // nil: nothing is suppressed
	generated map[*token.File]bool
	seen      map[finding]bool
	findings  []finding
}

// finding is one diagnostic waiting to be reported.
type finding struct {
	pos     token.Pos
	id      string
	message string
}

// newReporter returns a reporter for pass. Directives of idx suppress the diagnostics of the IDs
// they list; with a nil idx nothing is suppressed.
func newReporter(pass *analysis.Pass, idx *directiveIndex) *reporter {
	r := &reporter{
		pass:      pass,
		idx:       idx,
		generated: map[*token.File]bool{},
		seen:      map[finding]bool{},
	}
	for _, f := range pass.Files {
		if ast.IsGenerated(f) {
			r.generated[pass.Fset.File(f.Pos())] = true
		}
	}
	return r
}

// report adds the diagnostic "<id>: <text>" at pos.
func (r *reporter) report(pos token.Pos, id, text string) {
	r.add(finding{pos: pos, id: id, message: id + ": " + text})
}

// reportActor adds the diagnostic "<id>: <text> (actor code: <chain>)" at pos, for a check on
// actor code; chain is the chain of the actor-code function holding pos (ANL-060).
func (r *reporter) reportActor(pos token.Pos, id, text, chain string) { //nolint:unparam // every actor-code check of ANL-110 (LS100, LS110) reports through it
	r.add(finding{pos: pos, id: id, message: id + ": " + text + " (actor code: " + chain + ")"})
}

func (r *reporter) add(f finding) {
	if r.seen[f] || r.generated[r.pass.Fset.File(f.pos)] {
		return
	}
	if r.idx != nil && isSuppressibleID(f.id) && r.idx.suppressed(f.pos, f.id) {
		return
	}
	r.seen[f] = true
	r.findings = append(r.findings, f)
}

// flush reports the collected diagnostics sorted by file name and offset. The sort is stable:
// diagnostics at one position keep the order they were added in.
func (r *reporter) flush() {
	slices.SortStableFunc(r.findings, func(a, b finding) int {
		pa := r.pass.Fset.PositionFor(a.pos, false)
		pb := r.pass.Fset.PositionFor(b.pos, false)
		return cmp.Or(cmp.Compare(pa.Filename, pb.Filename), cmp.Compare(pa.Offset, pb.Offset))
	})
	for _, f := range r.findings {
		r.pass.Report(analysis.Diagnostic{Pos: f.pos, Category: f.id, Message: f.message})
	}
	r.findings = nil
}
