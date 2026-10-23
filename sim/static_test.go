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

// Kinds of testing value that failingCalls tells apart.
const (
	notTesting = iota
	simulateT  // a *testing.T, *testing.B, *testing.F or testing.TB parameter or variable, or an alias of one
	worldT     // the result of a call of a method named T (World.T), or an alias of it
	fieldT     // reached through a selector whose name is that of a testing value, such as x.t
)

// failingCalls returns one problem per use in src (the text of the Go file filename) that raises a
// failure other than through a named check (SIM-305): any selector named Fail (sim has no Fail of
// its own, so each is on a *kernel.Sim), and any selector Error, Errorf, Fatal or Fatalf on a
// testing value, whether called or taken as a method value. The one allowance is a call of Fatalf
// on a testing value declared as a parameter or variable of testing type (the Simulate *testing.T)
// in the function named allowFunc of the file named allowFile, never on World.T(). It also returns
// how many calls it allowed.
//
// A testing value is an identifier or field of testing type, anything assigned a testing value (an
// alias), the result of a call of a method named T, and a selector whose name is that of a testing
// value. The analysis is by name over the whole file, so it is conservative. fmt.Errorf and the
// Error method of an error are not on a testing value.
func failingCalls(t *testing.T, filename, src, allowFile, allowFunc string) (problems []string, allowed int) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	testingName := ""
	for _, imp := range f.Imports {
		if path, _ := strconv.Unquote(imp.Path.Value); path == "testing" {
			testingName = "testing"
			if imp.Name != nil {
				testingName = imp.Name.Name
			}
		}
	}
	isTestingType := func(e ast.Expr) bool {
		if star, ok := e.(*ast.StarExpr); ok {
			e = star.X
		}
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		x, ok := sel.X.(*ast.Ident)
		return ok && testingName != "" && x.Name == testingName && slices.Contains([]string{"T", "B", "F", "TB"}, sel.Sel.Name)
	}

	// Names of testing values, file-wide, until no alias adds one.
	values := map[string]int{}
	var kind func(e ast.Expr) int
	kind = func(e ast.Expr) int {
		switch e := e.(type) {
		case *ast.ParenExpr:
			return kind(e.X)
		case *ast.Ident:
			return values[e.Name]
		case *ast.SelectorExpr:
			if values[e.Sel.Name] != notTesting {
				return fieldT
			}
		case *ast.CallExpr:
			if sel, ok := e.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "T" && len(e.Args) == 0 {
				return worldT
			}
		}
		return notTesting
	}
	for changed := true; changed; {
		changed = false
		mark := func(name string, k int) {
			if k != notTesting && name != "_" && values[name] == notTesting {
				values[name] = k
				changed = true
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.Field: // parameters, results and struct fields
				if isTestingType(n.Type) {
					for _, name := range n.Names {
						mark(name.Name, simulateT)
					}
				}
			case *ast.AssignStmt:
				if len(n.Lhs) == len(n.Rhs) {
					for i, rhs := range n.Rhs {
						if id, ok := n.Lhs[i].(*ast.Ident); ok {
							mark(id.Name, kind(rhs))
						}
					}
				}
			case *ast.ValueSpec:
				for i, name := range n.Names {
					if n.Type != nil && isTestingType(n.Type) {
						mark(name.Name, simulateT)
					}
					if len(n.Names) == len(n.Values) {
						mark(name.Name, kind(n.Values[i]))
					}
				}
			}
			return true
		})
	}

	called := map[ast.Expr]bool{} // the Fun of every call
	ast.Inspect(f, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			called[call.Fun] = true
		}
		return true
	})
	// Every declaration is walked, so a function literal in a package-level variable is too; it is
	// in no function, which the empty name stands for.
	for _, decl := range f.Decls {
		fn := ""
		if d, ok := decl.(*ast.FuncDecl); ok {
			fn = d.Name.Name
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			at := fset.Position(sel.Pos()).String() + ": " + fn + ": "
			if sel.Sel.Name == "Fail" {
				problems = append(problems, at+"Fail")
				return true
			}
			if !slices.Contains([]string{"Error", "Errorf", "Fatal", "Fatalf"}, sel.Sel.Name) || kind(sel.X) == notTesting {
				return true
			}
			if sel.Sel.Name == "Fatalf" && called[sel] && kind(sel.X) == simulateT && filename == allowFile && fn == allowFunc {
				allowed++
				return true
			}
			problems = append(problems, at+sel.Sel.Name+" on a testing value")
			return true
		})
	}
	return problems, allowed
}

// AT-SIM-31, SIM-305: sim raises failures only through the named checks. No non-test file calls
// Fail on a *kernel.Sim, or Error, Errorf, Fatal or Fatalf on World.T(), and the only call of
// Fatalf on the *testing.T that Simulate receives is in the SIM-020 validation function.
func TestATSIM31NoDirectFailures(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	allowed := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		problems, n := failingCalls(t, name, string(b), "simulate.go", "setup")
		for _, p := range problems {
			t.Error(p)
		}
		allowed += n
	}
	// The allowance must not be vacuous: SIM-020's failure is raised by setup in simulate.go.
	if allowed != 1 {
		t.Errorf("found %d calls of Fatalf on the Simulate *testing.T in setup of simulate.go, want 1", allowed)
	}
}

// The checker itself: it finds each form of direct failure and passes the allowed ones.
func TestATSIM31CheckerFindsViolations(t *testing.T) {
	const header = "package p\nimport (\"fmt\"; \"testing\"; \"github.com/hmdsefi/faultline\")\n"
	for _, tc := range []struct {
		name, body string
		file       string // the file the source stands for; "" is simulate.go
		problems   int
		allowed    int
	}{
		{name: "Fail on a kernel.Sim", body: "func f(w *faultline.World) { w.Sim.Fail(nil) }", problems: 1},
		{name: "Fail as a method value", body: "func f(w *faultline.World) { g := w.Sim.Fail; g(nil) }", problems: 1},
		{name: "Error on World.T()", body: "func f(w *faultline.World) { w.T().Error(\"x\") }", problems: 1},
		{name: "Errorf on World.T()", body: "func f(w *faultline.World) { w.T().Errorf(\"x\") }", problems: 1},
		{name: "Fatal on World.T()", body: "func f(w *faultline.World) { w.T().Fatal(\"x\") }", problems: 1},
		{name: "Fatalf on World.T()", body: "func f(w *faultline.World) { w.T().Fatalf(\"x\") }", problems: 1},
		{name: "Fatalf on a variable from World.T()", body: "func f(w *faultline.World) { tb := w.T(); tb.Fatalf(\"x\") }", problems: 1},
		{name: "method value of World.T()", body: "func f(w *faultline.World) { g := w.T().Error; g(\"x\") }", problems: 1},
		{name: "Fatalf on a testing.TB var", body: "func f() { var tb testing.TB; tb.Fatalf(\"x\") }", problems: 1},
		{name: "Fatalf on a *testing.T outside setup", body: "func f(t *testing.T) { t.Fatalf(\"x\") }", problems: 1},
		{name: "Error on a *testing.T", body: "func f(t *testing.T) { t.Error(\"x\") }", problems: 1},
		{name: "function literal in a package variable", body: "var f = func(t *testing.T) { t.Fatalf(\"x\") }", problems: 1},
		{name: "function literal in a function", body: "func f(t *testing.T) { func() { t.Errorf(\"x\") }() }", problems: 1},
		{name: "field of testing type", body: "type h struct{ t *testing.T }\nfunc f(x h) { x.t.Fatalf(\"x\") }", problems: 1},
		{name: "alias of a *testing.T", body: "func f(t *testing.T) { tt := t; tt.Fatalf(\"x\") }", problems: 1},
		{name: "alias declared after use", body: "func f(t *testing.T) { g := func() { tt.Fatalf(\"x\") }; tt := t; g() }", problems: 1},
		{name: "method value of a *testing.T", body: "func f(t *testing.T) { fat := t.Fatalf; fat(\"x\") }", problems: 1},
		{name: "Fatalf on World.T() in setup", body: "func setup(w *faultline.World) { w.T().Fatalf(\"x\") }", problems: 1},
		{name: "Fatalf in setup of another file", body: "func setup(t *testing.T) { t.Fatalf(\"x\") }", file: "other.go", problems: 1},
		{name: "Error on a *testing.T in setup", body: "func setup(t *testing.T) { t.Error(\"x\") }", problems: 1},
		{name: "method value of Fatalf in setup", body: "func setup(t *testing.T) { _ = t.Fatalf }", problems: 1},
		{name: "Fatalf on the Simulate *testing.T in setup", body: "func setup(t *testing.T) { t.Fatalf(\"x\") }", allowed: 1},
		{name: "fmt.Errorf", body: "func f() error { return fmt.Errorf(\"x\") }"},
		{name: "Error method of an error", body: "func f(err error) string { return err.Error() }"},
		{name: "other World.T() uses", body: "func f(w *faultline.World) string { w.T().Helper(); return w.T().Name() }"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := tc.file
			if file == "" {
				file = "simulate.go"
			}
			got, allowed := failingCalls(t, file, header+tc.body, "simulate.go", "setup")
			if len(got) != tc.problems || allowed != tc.allowed {
				t.Errorf("problems %q and %d allowed, want %d and %d", got, allowed, tc.problems, tc.allowed)
			}
		})
	}
}
