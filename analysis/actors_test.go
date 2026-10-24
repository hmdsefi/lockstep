// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"errors"
	"fmt"
	"go/ast"
	"go/types"
	"reflect"
	"slices"
	"strings"
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// probe reports "actor code: <chain>" at every actor-code function and returns the actors
// analyzer's result, so tests can also inspect the actor types and the search order.
var probe = &analysis.Analyzer{
	Name:       "probe",
	Doc:        "report every actor-code function with its chain",
	Requires:   []*analysis.Analyzer{actorsAnalyzer},
	ResultType: reflect.TypeFor[*actorCode](),
	Run: func(pass *analysis.Pass) (any, error) {
		code, ok := pass.ResultOf[actorsAnalyzer].(*actorCode)
		if !ok {
			return nil, errors.New("probe: no actor code result")
		}
		for _, f := range code.Funcs {
			pass.Reportf(f.Fn.Pos(), "actor code: %s", f.Chain)
		}
		return code, nil
	},
}

// TestActorCode checks ANL-030 to ANL-060 against the actorcode corpus (AT-ANL-03, AT-ANL-04):
// the reports of probe, the actor types with their roots, the search order and the body lookup.
func TestActorCode(t *testing.T) {
	results := analysistest.Run(t, analysistest.TestData(), probe, "actorcode", "actorcode/generic")
	byPath := map[string]*actorCode{}
	for _, r := range results {
		code, ok := r.Result.(*actorCode)
		if !ok {
			t.Fatalf("package %s: result %T, want *actorCode", r.Pass.Pkg.Path(), r.Result)
		}
		byPath[r.Pass.Pkg.Path()] = code
		checkFuncs(t, r.Pass.Pkg.Path(), code)
	}

	tests := []struct {
		path   string
		actors []string
		order  []string
	}{
		{
			path: "actorcode",
			actors: []string{
				"Account message Msg receive (*Account).Receive roots [(*Account).Started (*Account).Receive (*Account).Stopped (*Account).Signal]",
				"Value message string receive (Value).Receive roots [(Value).Receive]",
				"Router message M receive (*Router[M]).Receive roots [(*Router[M]).Receive]",
				"Embedder message Msg receive (*Account).Receive roots []",
			},
			order: []string{
				"(*Account).Started", "(*Account).Receive", "(*Account).Stopped", "(*Account).Signal",
				"(Value).Receive", "(*Router[M]).Receive",
				"shared", "(*Account).Receive$1", "(*Account).flush", "apply", "less", "generic",
				"viaArg", "late", "early", "audit",
			},
		},
		{
			path: "actorcode/generic",
			actors: []string{
				"Cell message M receive (*Cell[M]).Receive roots [(*Cell[M]).Started (*Cell[M]).Receive]",
			},
			order: []string{
				"(*Cell[M]).Started", "(*Cell[M]).Receive", "(*Cell[M]).reset", "(*Cell[M]).set",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			code := byPath[tt.path]
			if code == nil {
				t.Fatalf("no result for package %s", tt.path)
			}
			var actors []string
			for _, a := range code.Actors {
				actors = append(actors, describeActor(a))
			}
			if !slices.Equal(actors, tt.actors) {
				t.Errorf("actor types:\n got  %q\n want %q", actors, tt.actors)
			}
			var order []string
			for _, f := range code.Funcs {
				order = append(order, f.Fn.RelString(f.Fn.Pkg.Pkg))
			}
			if !slices.Equal(order, tt.order) {
				t.Errorf("search order:\n got  %q\n want %q", order, tt.order)
			}
		})
	}
}

// TestActorFuncInspect checks that inspect walks an actor-code function's body without the
// function literals nested in it (ANL-050), and walks a literal's own body.
func TestActorFuncInspect(t *testing.T) {
	results := analysistest.Run(t, analysistest.TestData(), probe, "actorcode")
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	code, ok := results[0].Result.(*actorCode)
	if !ok {
		t.Fatalf("result %T, want *actorCode", results[0].Result)
	}
	tests := []struct {
		fn    string
		calls []string
	}{
		{"(*Account).Receive", []string{"apply", "Slice", "f", "record", "spin", "run", "generic", "Run", "shared"}},
		{"(*Account).Receive$1", []string{"append"}},
	}
	for _, tt := range tests {
		t.Run(tt.fn, func(t *testing.T) {
			var calls []string
			for _, f := range code.Funcs {
				if f.Fn.RelString(f.Fn.Pkg.Pkg) != tt.fn {
					continue
				}
				f.inspect(func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						switch fun := ast.Unparen(call.Fun).(type) {
						case *ast.Ident:
							calls = append(calls, fun.Name)
						case *ast.SelectorExpr:
							calls = append(calls, fun.Sel.Name)
						case *ast.IndexExpr:
							if id, ok := fun.X.(*ast.Ident); ok {
								calls = append(calls, id.Name)
							}
						}
					}
					return true
				})
			}
			if !slices.Equal(calls, tt.calls) {
				t.Errorf("calls in the body of %s:\n got  %q\n want %q", tt.fn, calls, tt.calls)
			}
		})
	}
}

// TestActorsNoSSA checks that a missing SSA result is not an error (ANL §7): the search finds
// no actor code.
func TestActorsNoSSA(t *testing.T) {
	result, err := runActors(&analysis.Pass{})
	if err != nil {
		t.Fatalf("runActors: %v, want a nil error", err)
	}
	code, ok := result.(*actorCode)
	if !ok {
		t.Fatalf("result %T, want *actorCode", result)
	}
	if len(code.Actors) != 0 || len(code.Funcs) != 0 {
		t.Errorf("got %d actor types and %d functions, want none", len(code.Actors), len(code.Funcs))
	}
	if got := code.funcOf(&ast.FuncLit{}); got != nil {
		t.Errorf("funcOf = %v, want nil", got)
	}
}

// describeActor renders an actor type as "<name> message <M> receive <method> roots [<roots>]".
func describeActor(a *actorType) string {
	q := types.RelativeTo(a.Type.Obj().Pkg())
	receive := a.Receive.Origin()
	recv := "(" + types.TypeString(receive.Signature().Recv().Type(), q) + ")"
	var roots []string
	for _, r := range a.Roots {
		roots = append(roots, r.Fn.RelString(r.Fn.Pkg.Pkg))
	}
	return fmt.Sprintf("%s message %s receive %s.%s roots [%s]", a.Type.Obj().Name(),
		types.TypeString(a.Message, q), recv, receive.Name(), strings.Join(roots, " "))
}

// checkFuncs checks every actor-code function's body, lookup and owning actor type.
func checkFuncs(t *testing.T, path string, code *actorCode) {
	t.Helper()
	for _, f := range code.Funcs {
		name := f.Fn.RelString(f.Fn.Pkg.Pkg)
		if got := code.funcOf(f.Body); got != f {
			t.Errorf("%s: funcOf(body of %s) = %v, want the function", path, name, got)
		}
		switch body := f.Body.(type) {
		case *ast.FuncDecl:
			if body.Name.Pos() != f.Fn.Pos() {
				t.Errorf("%s: body of %s is the declaration of %s", path, name, body.Name.Name)
			}
		case *ast.FuncLit:
			if f.Fn.Parent() == nil {
				t.Errorf("%s: body of %s is a function literal", path, name)
			}
		default:
			t.Errorf("%s: body of %s is %T", path, name, f.Body)
		}
		var want string
		if recv := f.Fn.Signature.Recv(); recv != nil {
			want = receiverTypeName(recv.Type())
		}
		var got string
		if f.Actor != nil {
			got = f.Actor.Type.Obj().Name()
		}
		if got != want {
			t.Errorf("%s: actor type of %s = %q, want %q", path, name, got, want)
		}
	}
	if got := code.funcOf(&ast.FuncLit{}); got != nil {
		t.Errorf("%s: funcOf(unknown body) = %v, want nil", path, got)
	}
}
