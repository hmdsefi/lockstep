// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"fmt"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Directives reports malformed and unknown //lockstep: directives (LS001, LS002; ANL-100).
var Directives = &analysis.Analyzer{
	Name:     "directives",
	Doc:      "report malformed and unknown //lockstep: directives (LS001, LS002)",
	Requires: []*analysis.Analyzer{indexAnalyzer},
	Run:      runDirectives,
}

// problem is one finding about a directive: the check ID and the message text after "<ID>: ".
type problem struct {
	id, text string
}

func runDirectives(pass *analysis.Pass) (any, error) {
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	// The reporter drops directives in generated files and reports in position order (ANL-120).
	// Nothing is suppressed: LS001 and LS002 cannot be (ANL-080).
	r := newReporter(pass, nil)
	for _, d := range idx.directives {
		for _, p := range d.problems() {
			r.report(d.pos, p.id, p.text)
		}
	}
	r.flush()
	return nil, nil
}

// problems lists what is wrong with d, one entry per row of the ANL-100 table.
func (d *directive) problems() []problem {
	switch d.name {
	case "":
		return []problem{{idDirectiveSyntax, "missing directive name after //lockstep:"}}
	case "allow":
		return d.allowProblems()
	case "immutable", "resource":
		return d.markerProblems()
	default:
		return []problem{{idDirectiveName, "unknown directive //lockstep:" + d.name +
			"; known directives: allow, immutable, resource"}}
	}
}

func (d *directive) allowProblems() []problem {
	if d.args == "" {
		return []problem{{idDirectiveSyntax,
			"//lockstep:allow needs a check ID and a reason: //lockstep:allow LSnnn <reason>"}}
	}
	var ps []problem
	if d.ids == nil {
		ps = append(ps, problem{idDirectiveSyntax,
			fmt.Sprintf(`malformed check ID list "%s"; use LSnnn or LSnnn,LSnnn`, d.idField)})
	}
	if d.reason == "" {
		ps = append(ps, problem{idDirectiveSyntax,
			fmt.Sprintf("//lockstep:allow %s needs a reason", d.idField)})
	}
	for _, id := range d.ids {
		switch {
		case id == idDirectiveSyntax || id == idDirectiveName:
			ps = append(ps, problem{idDirectiveName, id + " cannot be suppressed"})
		case !isSuppressibleID(id):
			ps = append(ps, problem{idDirectiveName,
				"unknown check ID " + id + "; known: " + strings.Join(suppressibleIDs, ", ")})
		}
	}
	return ps
}

func (d *directive) markerProblems() []problem {
	var ps []problem
	if d.args != "" {
		ps = append(ps, problem{idDirectiveSyntax, "//lockstep:" + d.name + " takes no arguments"})
	}
	switch d.place {
	case placedOutside:
		ps = append(ps, problem{idDirectiveSyntax,
			"//lockstep:" + d.name + " must be in the doc comment of a type declaration"})
	case placedAlias:
		ps = append(ps, problem{idDirectiveSyntax, "//lockstep:" + d.name + " cannot mark alias " + d.typeName})
	case placedInterface:
		ps = append(ps, problem{idDirectiveSyntax, "//lockstep:immutable cannot mark interface type " + d.typeName})
	}
	return ps
}
