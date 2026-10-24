// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"bytes"
	"go/ast"
	"go/token"
	"go/types"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// indexAnalyzer parses directives and markers (ANL-070 to ANL-090).
var indexAnalyzer = &analysis.Analyzer{
	Name:       "lsindex",
	Doc:        "index //lockstep: directives",
	Run:        runIndex,
	ResultType: reflect.TypeFor[*directiveIndex](),
}

// directivePrefix starts every directive comment (ANL-070).
const directivePrefix = "//lockstep:"

// idListPattern is the syntax of the ID list of //lockstep:allow (ANL-070).
var idListPattern = regexp.MustCompile(`^LS[0-9]{3}(,LS[0-9]{3})*$`)

// marker is the set of //lockstep: markers on a type.
type marker uint8

const (
	markImmutable marker = 1 << iota
	markResource
)

// placement says whether a marker directive sits where ANL-090 allows it.
type placement uint8

const (
	placedOK        placement = iota // on a type declaration
	placedOutside                    // not in the doc comment of a type declaration
	placedAlias                      // on an alias
	placedInterface                  // //lockstep:immutable on an interface type
)

// directive is one //lockstep: comment as parsed from the source (ANL-070).
type directive struct {
	pos       token.Pos // start of the comment
	name      string    // text after //lockstep: up to the first space or tab
	args      string    // the rest, without leading and trailing spaces and tabs
	generated bool      // in a generated file

	// allow only: field 1 of the arguments, the IDs when it is a well-formed list (nil
	// otherwise), and the reason.
	idField string
	ids     []string
	reason  string

	// immutable and resource only: where the directive sits, and the type's name for messages.
	place    placement
	typeName string
}

// effective reports whether an allow directive is well-formed and names only IDs it may suppress
// (ANL-080). A directive that is not effective suppresses nothing.
func (d *directive) effective() bool {
	return d.name == "allow" && d.ids != nil && d.reason != "" &&
		!slices.ContainsFunc(d.ids, func(id string) bool { return !isSuppressibleID(id) })
}

// span is a half-open source range.
type span struct{ pos, end token.Pos }

// allowScope is the part of one file an effective allow directive covers (ANL-080).
type allowScope struct {
	ids                 []string
	firstLine, lastLine int    // physical lines of scopes 1 and 2
	ranges              []span // scope 3: declarations holding the directive in their comments
}

// directiveIndex is the result of indexAnalyzer.
type directiveIndex struct {
	fset       *token.FileSet
	directives []*directive // every directive of the package, in file and source order
	markers    map[*types.TypeName]marker
	scopes     map[*token.File][]allowScope
}

// suppressed reports whether a //lockstep:allow directive covers the diagnostic of check id at pos
// (ANL-080).
func (x *directiveIndex) suppressed(pos token.Pos, id string) bool {
	if !pos.IsValid() {
		return false
	}
	tf := x.fset.File(pos)
	line := x.fset.PositionFor(pos, false).Line
	for _, s := range x.scopes[tf] {
		if !slices.Contains(s.ids, id) {
			continue
		}
		if line >= s.firstLine && line <= s.lastLine {
			return true
		}
		if slices.ContainsFunc(s.ranges, func(r span) bool { return r.pos <= pos && pos < r.end }) {
			return true
		}
	}
	return false
}

// indexOf returns the result of indexAnalyzer for pass.
func indexOf(pass *analysis.Pass) (*directiveIndex, bool) {
	idx, ok := pass.ResultOf[indexAnalyzer].(*directiveIndex)
	return idx, ok
}

func runIndex(pass *analysis.Pass) (any, error) {
	idx := &directiveIndex{
		fset:    pass.Fset,
		markers: make(map[*types.TypeName]marker),
		scopes:  make(map[*token.File][]allowScope),
	}
	for _, file := range pass.Files {
		idx.addFile(pass, file)
	}
	return idx, nil
}

// docs maps the comment groups of a file to the declarations that own them.
type docs struct {
	ranges map[*ast.CommentGroup][]span // declarations with the group as Doc or Comment
	types  map[*ast.CommentGroup]*ast.TypeSpec
}

// collectDocs finds the declarations of ANL-080 scope 3 and the type declarations a doc comment
// marks (ANL-090).
func collectDocs(file *ast.File) docs {
	d := docs{
		ranges: make(map[*ast.CommentGroup][]span),
		types:  make(map[*ast.CommentGroup]*ast.TypeSpec),
	}
	add := func(g *ast.CommentGroup, n ast.Node) {
		if g != nil {
			d.ranges[g] = append(d.ranges[g], span{n.Pos(), n.End()})
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncDecl:
			add(n.Doc, n)
		case *ast.GenDecl:
			add(n.Doc, n)
			if n.Tok == token.TYPE && n.Doc != nil && len(n.Specs) == 1 {
				if spec, ok := n.Specs[0].(*ast.TypeSpec); ok {
					d.types[n.Doc] = spec
				}
			}
		case *ast.TypeSpec:
			add(n.Doc, n)
			add(n.Comment, n)
			if n.Doc != nil {
				d.types[n.Doc] = n
			}
		case *ast.ValueSpec:
			add(n.Doc, n)
			add(n.Comment, n)
		case *ast.StructType:
			if n.Fields != nil {
				for _, f := range n.Fields.List {
					add(f.Doc, f)
					add(f.Comment, f)
				}
			}
		}
		return true
	})
	return d
}

// addFile indexes the directives of one file.
func (x *directiveIndex) addFile(pass *analysis.Pass, file *ast.File) {
	owners := collectDocs(file)
	generated := ast.IsGenerated(file)
	var src []byte // loaded when the first allow directive needs it
	for _, group := range file.Comments {
		for i, c := range group.List {
			rest, ok := strings.CutPrefix(c.Text, directivePrefix)
			if !ok {
				continue
			}
			d := parseDirective(c, rest)
			d.generated = generated
			x.directives = append(x.directives, d)
			switch d.name {
			case "allow":
				if d.effective() {
					if src == nil {
						src, _ = pass.ReadFile(x.fset.PositionFor(file.Pos(), false).Filename)
					}
					x.addAllow(file, d, owners.ranges[group], i == len(group.List)-1 && ownLine(src, x.fset, c))
				}
			case "immutable", "resource":
				x.addMarker(pass, d, owners.types[group])
			}
		}
	}
}

// parseDirective splits the text of a directive comment after //lockstep: (ANL-070).
func parseDirective(c *ast.Comment, rest string) *directive {
	d := &directive{pos: c.Slash, name: rest}
	if i := strings.IndexAny(rest, " \t"); i >= 0 {
		d.name, d.args = rest[:i], strings.Trim(rest[i:], " \t")
	}
	if d.name != "allow" {
		return d
	}
	fields := strings.FieldsFunc(d.args, func(r rune) bool { return r == ' ' || r == '\t' })
	if len(fields) == 0 {
		return d
	}
	d.idField = fields[0]
	if idListPattern.MatchString(d.idField) {
		d.ids = strings.Split(d.idField, ",")
	}
	d.reason = strings.Join(fields[1:], " ")
	return d
}

// ownLine reports whether only spaces and tabs precede c on its line. Without the file's source
// it reports false.
func ownLine(src []byte, fset *token.FileSet, c *ast.Comment) bool {
	if src == nil {
		return false
	}
	off := fset.File(c.Slash).Offset(c.Slash)
	if off > len(src) {
		return false
	}
	lineStart := bytes.LastIndexByte(src[:off], '\n') + 1
	return len(bytes.Trim(src[lineStart:off], " \t")) == 0
}

// addAllow records the three scopes of an effective allow directive (ANL-080): its own line, the
// next line when nextLine is set, and the declarations that hold it in their comments.
func (x *directiveIndex) addAllow(file *ast.File, d *directive, ranges []span, nextLine bool) {
	line := x.fset.PositionFor(d.pos, false).Line
	s := allowScope{ids: d.ids, firstLine: line, lastLine: line, ranges: ranges}
	if nextLine {
		s.lastLine++
	}
	tf := x.fset.File(file.Pos())
	x.scopes[tf] = append(x.scopes[tf], s)
}

// addMarker checks the placement of an immutable or resource directive and, when it is valid,
// binds it to its type (ANL-090). spec is the type declaration whose doc comment holds d, or nil.
func (x *directiveIndex) addMarker(pass *analysis.Pass, d *directive, spec *ast.TypeSpec) {
	if spec == nil {
		d.place = placedOutside
		return
	}
	obj, ok := pass.TypesInfo.Defs[spec.Name].(*types.TypeName)
	if !ok {
		d.place = placedOutside
		return
	}
	d.typeName = types.TypeString(obj.Type(), types.RelativeTo(pass.Pkg))
	switch {
	case spec.Assign.IsValid():
		d.place = placedAlias
	case d.name == "immutable" && types.IsInterface(obj.Type()):
		d.place = placedInterface
	}
	if d.place != placedOK || d.args != "" {
		return
	}
	if d.name == "immutable" {
		x.markers[obj] |= markImmutable
	} else {
		x.markers[obj] |= markResource
	}
}
