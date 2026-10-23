// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// failingCalls returns one problem per call in src (the text of one Go file) that raises a failure
// other than through a named check (SIM-305): a call of a method named Fail (sim has no Fail of
// its own, so any such call is on a *kernel.Sim), a call of Error, Errorf, Fatal or Fatalf on a
// testing value, and a call of Fatalf on a testing value outside the function named allowFatalf.
//
// A testing value is a *testing.T, *testing.B, *testing.F or testing.TB parameter or variable,
// anything assigned the result of a call of a method named T (World.T), and such a call itself.
// fmt.Errorf and the Error method of an error are not calls on a testing value.
func failingCalls(t *testing.T, filename, src, allowFatalf string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	testing_ := ""
	for _, imp := range f.Imports {
		if path, _ := strconv.Unquote(imp.Path.Value); path == "testing" {
			testing_ = "testing"
			if imp.Name != nil {
				testing_ = imp.Name.Name
			}
		}
	}
	isT := func(e ast.Expr) bool { // the type of a testing value
		if star, ok := e.(*ast.StarExpr); ok {
			e = star.X
		}
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		x, ok := sel.X.(*ast.Ident)
		return ok && testing_ != "" && x.Name == testing_ && slices.Contains([]string{"T", "B", "F", "TB"}, sel.Sel.Name)
	}
	isWorldT := func(e ast.Expr) bool { // a call of a method named T
		call, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		return ok && sel.Sel.Name == "T" && len(call.Args) == 0
	}

	// Names of testing values, file-wide: a conservative approximation that keeps the check
	// simple and cannot miss a value that a function of this package declares under another name.
	values := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.Field:
			if isT(n.Type) {
				for _, name := range n.Names {
					values[name.Name] = true
				}
			}
		case *ast.AssignStmt:
			if len(n.Lhs) == len(n.Rhs) {
				for i, rhs := range n.Rhs {
					if id, ok := n.Lhs[i].(*ast.Ident); ok && isWorldT(rhs) {
						values[id.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			if n.Type != nil && isT(n.Type) {
				for _, name := range n.Names {
					values[name.Name] = true
				}
			}
			if len(n.Names) == len(n.Values) {
				for i, v := range n.Values {
					if isWorldT(v) {
						values[n.Names[i].Name] = true
					}
				}
			}
		}
		return true
	})

	var problems []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			at := fset.Position(call.Pos()).String() + ": " + fn.Name.Name + ": "
			if sel.Sel.Name == "Fail" {
				problems = append(problems, at+"call of Fail")
				return true
			}
			id, isIdent := sel.X.(*ast.Ident)
			onTesting := isWorldT(sel.X) || (isIdent && values[id.Name])
			switch {
			case !onTesting:
			case sel.Sel.Name == "Fatalf" && fn.Name.Name == allowFatalf:
			case slices.Contains([]string{"Error", "Errorf", "Fatal", "Fatalf"}, sel.Sel.Name):
				problems = append(problems, at+"call of "+sel.Sel.Name+" on a testing value")
			}
			return true
		})
	}
	return problems
}

// AT-SIM-31, SIM-305: sim raises failures only through the named checks. No non-test file calls
// Fail on a *kernel.Sim, or Error, Errorf, Fatal or Fatalf on World.T(), and the only call of
// Fatalf on the *testing.T that Simulate receives is in the SIM-020 validation function.
func TestATSIM31NoDirectFailures(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fatalf := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		for _, p := range failingCalls(t, name, src, "setup") {
			t.Error(p)
		}
		if name == "simulate.go" {
			fatalf = strings.Count(src, ".Fatalf(")
		}
	}
	// The allowance must not be vacuous: simulate.go's setup is where SIM-020's failure is raised.
	if fatalf != 1 {
		t.Errorf("simulate.go has %d calls of Fatalf, want 1 (in setup)", fatalf)
	}
}

// The checker itself: it finds each kind of direct failure and passes the allowed forms.
func TestATSIM31CheckerFindsViolations(t *testing.T) {
	const header = "package p\nimport (\"fmt\"; \"testing\"; \"github.com/hmdsefi/faultline\")\n"
	for _, tc := range []struct {
		name, body string
		want       int
	}{
		{"Fail on a kernel.Sim", "func f(w *faultline.World) { w.Sim.Fail(nil) }", 1},
		{"Error on World.T()", "func f(w *faultline.World) { w.T().Error(\"x\") }", 1},
		{"Errorf on World.T()", "func f(w *faultline.World) { w.T().Errorf(\"x\") }", 1},
		{"Fatal on World.T()", "func f(w *faultline.World) { w.T().Fatal(\"x\") }", 1},
		{"Fatalf on World.T()", "func f(w *faultline.World) { w.T().Fatalf(\"x\") }", 1},
		{"Fatalf on a variable from World.T()", "func f(w *faultline.World) { tb := w.T(); tb.Fatalf(\"x\") }", 1},
		{"Fatalf on a testing.TB var", "func f() { var tb testing.TB; tb.Fatalf(\"x\") }", 1},
		{"Fatalf on a *testing.T outside setup", "func f(t *testing.T) { t.Fatalf(\"x\") }", 1},
		{"Error on a *testing.T", "func f(t *testing.T) { t.Error(\"x\") }", 1},
		{"Fatalf on a *testing.T in setup", "func setup(t *testing.T) { t.Fatalf(\"x\") }", 0},
		{"Error on a *testing.T in setup", "func setup(t *testing.T) { t.Error(\"x\") }", 1},
		{"fmt.Errorf", "func f() error { return fmt.Errorf(\"x\") }", 0},
		{"Error method of an error", "func f(err error) string { return err.Error() }", 0},
		{"other World.T() uses", "func f(w *faultline.World) string { w.T().Helper(); return w.T().Name() }", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := failingCalls(t, "p.go", header+tc.body, "setup")
			if len(got) != tc.want {
				t.Errorf("problems %q, want %d", got, tc.want)
			}
		})
	}
}
