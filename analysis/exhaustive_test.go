// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"regexp"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestExhaustive checks the LS230 corpus (AT-ANL-21, AT-ANL-22): sealed members and self-message
// types with no case in ls230/bad, none in ls230/good, and the exported members of an imported
// sealed interface, read from its fact, in ls230/remote. The facts list the exported members only.
func TestExhaustive(t *testing.T) {
	runCorpus(t, Exhaustive, "ls230/bad", "ls230/good")
	runCorpus(t, Exhaustive, "ls230/remote", "ls230/msgs")
}

// discard is an analysistest.Testing that drops the errors of a run whose want comments
// TestExhaustive checks already.
type discard struct{}

func (discard) Errorf(string, ...any) {}

// TestExhaustiveOrder checks ANL-330's order: the findings at one position are sorted by the
// type string of the uncovered type.
func TestExhaustiveOrder(t *testing.T) {
	typeOf := regexp.MustCompile(`message type (\S+?),? (of|which) `)
	for _, res := range analysistest.Run(discard{}, analysistest.TestData(), Exhaustive, "ls230/bad", "ls230/remote") {
		ds := res.Diagnostics
		for i := 1; i < len(ds); i++ {
			if ds[i].Pos != ds[i-1].Pos {
				continue
			}
			prev := typeOf.FindStringSubmatch(ds[i-1].Message)
			cur := typeOf.FindStringSubmatch(ds[i].Message)
			if prev == nil || cur == nil || prev[1] >= cur[1] {
				t.Errorf("%s: %q reported before %q", res.Pass.Fset.Position(ds[i].Pos), ds[i-1].Message, ds[i].Message)
			}
		}
	}
}

// TestExhaustiveTestFileMembers checks ANL-320 for types declared in _test.go files: they are not
// members of a sealed interface declared in another file, and they are members of one declared in
// a _test.go file. The corpus cannot hold this case, because analysistest also loads the test
// variant of every package.
func TestExhaustiveTestFileMembers(t *testing.T) {
	sources := []struct{ name, src string }{
		{"m.go", "package m\ntype I interface{ i() }\ntype A struct{}\nfunc (A) i() {}\n"},
		{"m_test.go", "package m\ntype J interface{ j() }\ntype B struct{}\nfunc (B) i() {}\nfunc (B) j() {}\n"},
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, s := range sources {
		f, err := parser.ParseFile(fset, s.name, s.src, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	pkg, err := new(types.Config).Check("m", fset, files, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ iface, want string }{{"I", "[m.A]"}, {"J", "[m.B]"}} {
		named, ok := pkg.Scope().Lookup(tt.iface).Type().(*types.Named)
		if !ok {
			t.Fatalf("%s is not a named type", tt.iface)
		}
		if got := fmt.Sprint(exhaustiveLocalMembers(fset, named)); got != tt.want {
			t.Errorf("members of %s = %s, want %s", tt.iface, got, tt.want)
		}
	}
}
