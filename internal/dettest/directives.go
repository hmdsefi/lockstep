// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/ast"
	"go/token"
	"regexp"
	"slices"
	"strings"
)

// directiveRE matches the text after "//" of a directive comment (DET-030).
var directiveRE = regexp.MustCompile(`^lockstep:([a-z]+)(?:[ \t]+(.*\S))?[ \t]*$`)

// directiveRules lists the rules each directive suppresses (DET-030).
var directiveRules = map[string][]Rule{
	"maporder": {RuleMapRange, RuleUnordered, RuleMapIter},
	"blocking": {RuleGo, RuleSelect},
}

// analyzerDirectives are the directives of the analyzer (ANL). The lint neither honors nor reports
// them (DET-030).
var analyzerDirectives = []string{"allow", "immutable", "resource"}

// directive is one //lockstep:<name> <reason> comment.
type directive struct {
	pos    token.Position
	line   int // the line the directive applies to (faultline DET-006)
	name   string
	reason string
	used   bool
}

// directives returns the directives of f in source order. A directive that follows code on its
// line applies to that line; any other directive applies to the line below it (faultline DET-006).
func (l *linter) directives(f *ast.File) []*directive {
	code := map[int]bool{} // lines where a syntax node starts or ends
	ast.Inspect(f, func(n ast.Node) bool {
		switch n.(type) {
		case nil, *ast.CommentGroup:
			return false
		}
		code[l.fset.Position(n.Pos()).Line] = true
		code[l.fset.Position(n.End()-1).Line] = true
		return true
	})
	var out []*directive
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if !strings.HasPrefix(c.Text, "//") {
				continue
			}
			if m := directiveRE.FindStringSubmatch(c.Text[2:]); m != nil && !slices.Contains(analyzerDirectives, m[1]) {
				pos := l.fset.Position(c.Pos())
				line := pos.Line + 1
				if code[pos.Line] {
					line = pos.Line
				}
				out = append(out, &directive{pos: pos, line: line, name: m[1], reason: m[2]})
			}
		}
	}
	return out
}

// honored reports whether the directive name is honored in class c (DET-030).
func honored(name string, c Class) bool {
	switch name {
	case "maporder":
		return c == ClassCore || c == ClassRuntime || c == ClassEngine
	case "blocking":
		return c == ClassRuntime
	}
	return false
}

// applyDirectives removes the findings suppressed by valid directives and appends a DL000 finding
// for every directive that is unknown, has no reason, is not honored in class c, or suppressed
// nothing (faultline DET-006, DET-007). classOf returns the class of the file a directive is in
// (DET-055). Finding files are absolute paths here.
func applyDirectives(findings []Finding, dirs []*directive, classOf func(file string) Class) []Finding {
	var valid []*directive
	var problems []Finding
	meta := func(d *directive, msg string) {
		problems = append(problems, Finding{File: d.pos.Filename, Line: d.pos.Line, Col: d.pos.Column, Rule: RuleMeta, Msg: msg})
	}
	for _, d := range dirs {
		switch {
		case directiveRules[d.name] == nil:
			meta(d, "unknown directive //lockstep:"+d.name)
		case d.reason == "":
			meta(d, fmt.Sprintf("//lockstep:%s needs a reason", d.name))
		case !honored(d.name, classOf(d.pos.Filename)):
			meta(d, fmt.Sprintf("//lockstep:%s is not allowed in %s packages", d.name, classOf(d.pos.Filename)))
		default:
			valid = append(valid, d)
		}
	}
	kept := findings[:0:0]
	for _, f := range findings {
		suppressed := false
		for _, d := range valid {
			if d.pos.Filename == f.File && f.Line == d.line &&
				slices.Contains(directiveRules[d.name], f.Rule) {
				d.used = true
				suppressed = true
			}
		}
		if !suppressed {
			kept = append(kept, f)
		}
	}
	for _, d := range valid {
		if !d.used {
			meta(d, fmt.Sprintf("//lockstep:%s suppresses no finding", d.name))
		}
	}
	return append(kept, problems...)
}
