// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// CheckImports returns the DL012 and DL000 findings of DET-090 for m, exempt packages included,
// sorted like Lint.
func CheckImports(m Module) ([]Finding, error) {
	dirs, err := packageDirs(m.Root)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	var out []Finding
	for _, rel := range dirs {
		allow, ok := m.Imports[rel]
		if !ok {
			out = append(out, Finding{File: rel, Rule: RuleMeta, Msg: fmt.Sprintf(
				"package %s has no import rule; add it to DET §5.2 and internal/dettest", importPath(m, rel))})
			continue
		}
		fs, err := dirImports(m, fset, rel, allow)
		if err != nil {
			return nil, fmt.Errorf("dettest: imports of %s: %w", importPath(m, rel), err)
		}
		out = append(out, fs...)
	}
	sortFindings(out)
	return out, nil
}

// dirImports returns the DL012 findings of the package directory rel, whose rule is allow.
func dirImports(m Module, fset *token.FileSet, rel string, allow Allow) ([]Finding, error) {
	dir := filepath.Join(m.Root, filepath.FromSlash(rel))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Finding
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if n, err := contextMatches(dir, name); err != nil {
			return nil, err
		} else if n == 0 {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ImportsOnly)
		if err != nil {
			return nil, err
		}
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return nil, err
			}
			if allowed(m, allow, p) {
				continue
			}
			pos := fset.PositionFor(imp.Path.Pos(), false)
			out = append(out, Finding{File: path.Join(rel, name), Line: pos.Line, Col: pos.Column, Rule: RuleImport,
				Msg: fmt.Sprintf("import %q is not allowed in package %s", p, importPath(m, rel))})
		}
	}
	return out, nil
}

// allowed reports whether a package with rule a may import p (DET-090). A path whose first element
// has no dot is the standard library; every other path outside the module needs an External entry.
func allowed(m Module, a Allow, p string) bool {
	switch {
	case p == "C":
		return false
	case p == m.Path || strings.HasPrefix(p, m.Path+"/"):
		rel := strings.TrimPrefix(strings.TrimPrefix(p, m.Path), "/")
		return slices.Contains(a.Packages, rel)
	case slices.ContainsFunc(a.External, func(e string) bool { return externalMatch(e, p) }):
		return true
	case !strings.Contains(strings.SplitN(p, "/", 2)[0], "."):
		if p == "testing" || strings.HasPrefix(p, "testing/") {
			return slices.Contains(a.Testing, p)
		}
		return a.Stdlib
	}
	return false
}

// externalMatch reports whether the External entry e allows the import path p: p equals e, or e is
// "q/..." and p is q or lies below it.
func externalMatch(e, p string) bool {
	q, tree := strings.CutSuffix(e, "/...")
	if !tree {
		return p == e
	}
	return p == q || strings.HasPrefix(p, q+"/")
}
