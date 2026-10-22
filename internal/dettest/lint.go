// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"cmp"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// linter holds the FileSet and the source importer shared by all packages of one run (faultline DET-015).
type linter struct {
	fset *token.FileSet
	imp  types.Importer
}

func newLinter() *linter {
	fset := token.NewFileSet()
	return &linter{fset: fset, imp: importer.ForCompiler(fset, "source", nil)}
}

// LintDir lints the single package in dir as importPath with class c. File paths in findings are
// relative to dir.
func LintDir(dir, importPath string, c Class) ([]Finding, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	fs, err := newLinter().lintPackage(abs, abs, importPath, c, nil)
	if err != nil {
		return nil, err
	}
	sortFindings(fs)
	return fs, nil
}

// lintPackage applies the rules of class c to the package in the absolute directory dir. Finding
// paths are made relative to the absolute directory base. A file whose path relative to base is a
// key of files is linted with that class instead of c, directives included (DET-055).
func (l *linter) lintPackage(base, dir, importPath string, c Class, files map[string]Class) ([]Finding, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	classOf := map[string]Class{} // absolute file name → class of the file
	has := func(file string, r Rule) bool { return slices.Contains(classOf[file].Rules(), r) }
	var findings []Finding
	var dirs []*directive
	var host []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		n, err := contextMatches(dir, name)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue // matched by no check context: ignored by every check
		}
		file := filepath.Join(dir, name)
		classOf[file] = c
		if rel, err := filepath.Rel(base, file); err == nil {
			if fc, ok := files[filepath.ToSlash(rel)]; ok {
				classOf[file] = fc
			}
		}
		if has(file, RulePlatform) && n < len(checkContexts()) {
			findings = append(findings, Finding{File: file, Line: 1, Col: 1, Rule: RulePlatform,
				Msg: fmt.Sprintf("platform-specific file %s in a simulation package", name)})
		}
		if ok, err := hostContext().MatchFile(dir, name); err != nil {
			return nil, err
		} else if !ok {
			continue
		}
		f, err := parser.ParseFile(l.fset, file, nil, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		host = append(host, f)
		if classOf[file] != ClassExempt { // an exempt file has no findings, directives included
			dirs = append(dirs, l.directives(f)...)
		}
	}
	if len(host) > 0 {
		info := &types.Info{
			Types: map[ast.Expr]types.TypeAndValue{},
			Uses:  map[*ast.Ident]types.Object{},
			Defs:  map[*ast.Ident]types.Object{},
		}
		conf := types.Config{Importer: l.imp}
		pkg, err := conf.Check(importPath, l.fset, host, info)
		if err != nil {
			return nil, fmt.Errorf("dettest: type-checking %s: %w", importPath, err)
		}
		qual := func(p *types.Package) string {
			if p == pkg {
				return ""
			}
			return p.Name()
		}
		for _, f := range host {
			file := l.fset.Position(f.Package).Filename
			findings = append(findings, l.lintFile(f, info, func(r Rule) bool { return has(file, r) }, qual)...)
		}
		findings = append(findings, l.floats(info, qual, func(file string) bool { return has(file, RuleFloat) })...)
	}
	findings = applyDirectives(findings, dirs, func(file string) Class { return classOf[file] })
	for i := range findings {
		rel, err := filepath.Rel(base, findings[i].File)
		if err != nil {
			return nil, err
		}
		findings[i].File = filepath.ToSlash(rel)
	}
	return findings, nil
}

var (
	// wallClockFuncs read the wall clock or arm a runtime timer, by import path (DL001).
	wallClockFuncs = map[string][]string{
		"time":    {"Now", "Since", "Until", "Sleep", "After", "AfterFunc", "NewTimer", "NewTicker", "Tick"},
		"context": {"WithDeadline", "WithDeadlineCause", "WithTimeout", "WithTimeoutCause"},
	}
	// goroutineFuncs run a function on another goroutine without a go statement, by
	// types.Func.FullName, with what each does (DL004). SetFinalizer and AddCleanup run it when the
	// garbage collector decides.
	goroutineFuncs = map[string]string{
		"context.AfterFunc":    "starts a goroutine",
		"(*sync.WaitGroup).Go": "starts a goroutine",
		"runtime.SetFinalizer": "runs a function on a runtime goroutine",
		"runtime.AddCleanup":   "runs a function on a runtime goroutine",
	}
	randCtors = []string{"New", "NewSource", "NewZipf", "NewPCG", "NewChaCha8"}
	// envFuncs read environment variables, by pkgpath.Name (DL008). TempDir and the User*Dir
	// functions read them to find a directory.
	envFuncs = []string{"os.Getenv", "os.LookupEnv", "os.Environ", "os.ExpandEnv", "syscall.Getenv", "syscall.Environ",
		"os.TempDir", "os.UserHomeDir", "os.UserCacheDir", "os.UserConfigDir"}
	// unorderedFuncs list a map in random order, or call a function in map order, by
	// types.Func.FullName (DL009). Seq and Seq2 do so only for a map value, but the lint cannot tell
	// the value's kind.
	unorderedFuncs = []string{
		"(reflect.Value).MapRange", "(reflect.Value).MapKeys", "(reflect.Value).Seq", "(reflect.Value).Seq2",
		"(*sync.Map).Range", "maps.DeleteFunc", "maps.EqualFunc",
	}
	mapIterFuncs = []string{"All", "Keys", "Values"}
	sortedFuncs  = []string{"Sorted", "SortedFunc", "SortedStableFunc"}
)

// lintFile applies the syntax and identifier rules (DL001 to DL006, DL008 to DL010) to f, for the
// rules that has accepts.
func (l *linter) lintFile(f *ast.File, info *types.Info, has func(Rule) bool, qual types.Qualifier) []Finding {
	var out []Finding
	add := func(pos token.Pos, r Rule, msg string) {
		if has(r) {
			p := l.fset.Position(pos)
			out = append(out, Finding{File: p.Filename, Line: p.Line, Col: p.Column, Rule: r, Msg: msg})
		}
	}
	for _, imp := range f.Imports {
		if path, _ := strconv.Unquote(imp.Path.Value); path == "crypto/rand" {
			add(imp.Path.Pos(), RuleCryptoRand, "crypto/rand is forbidden in simulation code")
		}
	}
	ast.PreorderStack(f, nil, func(n ast.Node, stack []ast.Node) bool {
		switch n := n.(type) {
		case *ast.GoStmt:
			add(n.Go, RuleGo, "go statement is forbidden in simulation code")
		case *ast.SelectStmt:
			add(n.Select, RuleSelect, "select statement is forbidden in simulation code")
		case *ast.RangeStmt:
			if tv, ok := info.Types[n.X]; ok && isMap(tv.Type) {
				add(n.For, RuleMapRange, fmt.Sprintf(
					"range over map %s has random order; range over sorted keys or annotate //lockstep:maporder <reason>",
					types.TypeString(tv.Type, qual)))
			}
		case *ast.Ident:
			fn, ok := info.Uses[n].(*types.Func)
			if !ok || fn.Pkg() == nil {
				return true
			}
			pkg, name, full := fn.Pkg().Path(), fn.Name(), fn.FullName()
			switch {
			case goroutineFuncs[full] != "":
				add(n.Pos(), RuleGo, fmt.Sprintf(
					"%s %s, which is forbidden in simulation code", full, goroutineFuncs[full]))
			case slices.Contains(unorderedFuncs, full):
				add(n.Pos(), RuleUnordered, fmt.Sprintf(
					"unordered iteration: %s; sort the keys or annotate //lockstep:maporder <reason>", full))
			case fn.Signature().Recv() != nil:
				// The remaining rules name package-level functions only.
			case slices.Contains(wallClockFuncs[pkg], name):
				add(n.Pos(), RuleWallClock, fmt.Sprintf(
					"wall-clock time: %s.%s is forbidden; use engine.Clock (ctx.Now, ctx.After in actors)", pkg, name))
			case (pkg == "math/rand" || pkg == "math/rand/v2") && !slices.Contains(randCtors, name):
				add(n.Pos(), RuleGlobalRand, fmt.Sprintf(
					"global randomness: %s.%s is forbidden; use engine.Entropy (ctx.Rand in actors)", pkg, name))
			case slices.Contains(envFuncs, pkg+"."+name):
				add(n.Pos(), RuleEnv, fmt.Sprintf(
					"environment access: %s.%s is forbidden in simulation code", pkg, name))
			case pkg == "maps" && slices.Contains(mapIterFuncs, name) && !sortedArgument(n, stack, info):
				add(n.Pos(), RuleMapIter, fmt.Sprintf(
					"maps.%s yields in random order; wrap it in slices.Sorted or annotate //lockstep:maporder <reason>", name))
			}
		}
		return true
	})
	return out
}

// isMap reports whether ranging over a value of type t ranges over a map (faultline DET-001, DL006): a map
// type, or a type parameter whose constraint allows a map type.
func isMap(t types.Type) bool {
	return allowsMap(t)
}

// allowsMap reports whether the constraint t allows a map type: t is one, or t is an interface that
// embeds one, directly, as a union term or through another interface (`interface{ MapLike }` with
// `type MapLike interface{ ~map[K]V }`), at any depth. The embedding cannot form a cycle: go/types
// rejects a recursive constraint.
func allowsMap(t types.Type) bool {
	switch u := t.Underlying().(type) {
	case *types.Map:
		return true
	case *types.Interface:
		for i := 0; i < u.NumEmbeddeds(); i++ {
			e := u.EmbeddedType(i)
			if union, ok := e.(*types.Union); ok {
				for j := 0; j < union.Len(); j++ {
					if allowsMap(union.Term(j).Type()) {
						return true
					}
				}
			} else if allowsMap(e) {
				return true
			}
		}
	}
	return false
}

// sortedArgument reports whether the identifier id (maps.All, maps.Keys or maps.Values) is called
// as the first argument of slices.Sorted, slices.SortedFunc or slices.SortedStableFunc.
func sortedArgument(id *ast.Ident, stack []ast.Node, info *types.Info) bool {
	i := len(stack) - 1
	var fun ast.Expr = id
	if i >= 0 {
		if sel, ok := stack[i].(*ast.SelectorExpr); ok && sel.Sel == id {
			fun = sel
			i--
		}
	}
	if i < 1 {
		return false
	}
	inner, ok := stack[i].(*ast.CallExpr)
	if !ok || inner.Fun != fun {
		return false
	}
	outer, ok := stack[i-1].(*ast.CallExpr)
	if !ok || len(outer.Args) == 0 || outer.Args[0] != inner {
		return false
	}
	f := outer.Fun
	for {
		if x, ok := f.(*ast.IndexExpr); ok {
			f = x.X
		} else if x, ok := f.(*ast.IndexListExpr); ok {
			f = x.X
		} else {
			break
		}
	}
	if sel, ok := f.(*ast.SelectorExpr); ok {
		f = sel.Sel
	}
	ident, ok := f.(*ast.Ident)
	if !ok {
		return false
	}
	fn, ok := info.Uses[ident].(*types.Func)
	return ok && fn.Pkg() != nil && fn.Pkg().Path() == "slices" && slices.Contains(sortedFuncs, fn.Name())
}

// floats returns the DL007 findings: at most one per file line, at the first floating-point or
// complex expression of the line (faultline DET-001), in the files for which include is true.
func (l *linter) floats(info *types.Info, qual types.Qualifier, include func(file string) bool) []Finding {
	type hit struct {
		pos token.Position
		typ string
	}
	var hits []hit
	for expr, tv := range info.Types {
		if tv.Type == nil {
			continue
		}
		b, ok := tv.Type.Underlying().(*types.Basic)
		if !ok || b.Info()&(types.IsFloat|types.IsComplex) == 0 || b.Info()&types.IsUntyped != 0 {
			continue
		}
		if pos := l.fset.Position(expr.Pos()); include(pos.Filename) {
			hits = append(hits, hit{pos, types.TypeString(tv.Type, qual)})
		}
	}
	slices.SortFunc(hits, func(a, b hit) int {
		return cmp.Or(cmp.Compare(a.pos.Filename, b.pos.Filename), cmp.Compare(a.pos.Line, b.pos.Line),
			cmp.Compare(a.pos.Column, b.pos.Column), cmp.Compare(a.typ, b.typ))
	})
	var out []Finding
	for i, h := range hits {
		if i > 0 && hits[i-1].pos.Filename == h.pos.Filename && hits[i-1].pos.Line == h.pos.Line {
			continue
		}
		out = append(out, Finding{File: h.pos.Filename, Line: h.pos.Line, Col: h.pos.Column, Rule: RuleFloat,
			Msg: fmt.Sprintf("floating-point type %s in decision code; use integers (ppm, nanoseconds)", h.typ)})
	}
	return out
}
