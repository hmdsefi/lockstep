// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// SharedState reports package variables written from actor code and actor state placed in
// messages (LS210).
var SharedState = &analysis.Analyzer{
	Name:     "sharedstate",
	Doc:      "report state shared between actors (LS210)",
	Requires: []*analysis.Analyzer{actorsAnalyzer, indexAnalyzer},
	Run:      runSharedState,
}

// idSharedState is the check ID of SharedState.
const idSharedState = "LS210"

func runSharedState(pass *analysis.Pass) (any, error) {
	code, ok := pass.ResultOf[actorsAnalyzer].(*actorCode)
	if !ok {
		return nil, nil
	}
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	r := newReporter(pass, idx)
	for _, f := range code.Funcs {
		checkPackageVars(pass, r, f)
		if f.Actor != nil {
			checkActorState(pass, r, f)
		}
	}
	r.flush()
	return nil, nil
}

// checkPackageVars reports the writes to package variables in the body of the actor-code function
// f (ANL-270). The body leaves out the function literals nested in f: those that are actor code
// are checked as functions of their own.
func checkPackageVars(pass *analysis.Pass, r *reporter, f *actorFunc) {
	info := pass.TypesInfo
	write := func(e ast.Expr, op string) {
		v := packageVar(info, e)
		if v == nil {
			return
		}
		name := v.Name()
		if v.Pkg() != pass.Pkg {
			name = v.Pkg().Name() + "." + name
		}
		r.reportActor(e.Pos(), idSharedState,
			"actor code writes package variable "+name+" ("+op+"); keep state in the actor's fields", f.Chain)
	}
	f.inspect(func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			if n.Tok == token.DEFINE {
				return true // every name on the left is new, so none is a package variable
			}
			for _, lhs := range n.Lhs {
				write(lhs, "assignment")
			}
		case *ast.RangeStmt:
			if n.Tok == token.ASSIGN {
				for _, e := range []ast.Expr{n.Key, n.Value} {
					if e != nil {
						write(e, "assignment")
					}
				}
			}
		case *ast.IncDecStmt:
			write(n.X, "increment")
		case *ast.UnaryExpr:
			if n.Op == token.AND {
				write(n.X, "taking its address")
			}
		case *ast.CallExpr:
			if e, op, ok := writtenByCall(info, n); ok {
				write(e, op)
			}
		}
		return true
	})
}

// packageVar returns the package-level variable, of any package, at the root of the written
// expression e (ANL-200 root), or nil.
func packageVar(info *types.Info, e ast.Expr) *types.Var {
	root := leafRoot(info, e)
	if root == nil {
		return nil
	}
	v, ok := info.ObjectOf(root).(*types.Var)
	if !ok || v.IsField() || v.Pkg() == nil || v.Pkg().Scope().Lookup(v.Name()) != v {
		return nil
	}
	return v
}

// writtenByCall returns the expression that call writes and the op of the finding, when call is a
// call of delete, clear or copy, of an in-place mutator of ANL-260, or of a pointer-receiver
// method on a receiver expression that is not itself a pointer. append is no write form: the
// slice it returns is assigned, and that assignment is the write.
func writtenByCall(info *types.Info, call *ast.CallExpr) (written ast.Expr, op string, ok bool) {
	if name, ok := builtinWrite(info, call); ok && name != "append" && len(call.Args) > 0 {
		return call.Args[0], name, true
	}
	if op, ok := inPlaceMutator(info, call); ok && len(call.Args) > 0 {
		return call.Args[0], op, true
	}
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok && takesAddress(info, sel) {
		return sel.X, "pointer method " + sel.Sel.Name, true
	}
	return nil, "", false
}

// takesAddress reports whether the method call through sel passes the address of the receiver
// expression sel.X: the method has a pointer receiver and neither sel.X nor a field embedded on
// the way to the method holds a pointer, so Go takes the address of sel.X, or of a part of it.
func takesAddress(info *types.Info, sel *ast.SelectorExpr) bool {
	s := info.Selections[sel]
	if s == nil || s.Kind() != types.MethodVal {
		return false
	}
	fn, ok := s.Obj().(*types.Func)
	if !ok || !isPointer(fn.Signature().Recv().Type()) {
		return false
	}
	t := info.TypeOf(sel.X)
	path := s.Index()
	for _, i := range path[:len(path)-1] {
		if t == nil || isPointer(t) {
			return false
		}
		st, ok := t.Underlying().(*types.Struct)
		if !ok {
			return false
		}
		t = st.Field(i).Type()
	}
	return t != nil && !isPointer(t)
}

// isPointer reports whether the underlying type of t is a pointer.
func isPointer(t types.Type) bool {
	_, ok := t.Underlying().(*types.Pointer)
	return ok
}

// checkActorState reports the actor state placed in messages at the send sites of the actor-code
// method f (ANL-280). Function literals are not methods of an actor type, so none is checked.
func checkActorState(pass *analysis.Pass, r *reporter, f *actorFunc) {
	decl, ok := f.Body.(*ast.FuncDecl)
	if !ok {
		return
	}
	info := pass.TypesInfo
	var recv *types.Var
	var recvName string
	if decl.Recv != nil && len(decl.Recv.List) == 1 && len(decl.Recv.List[0].Names) == 1 {
		id := decl.Recv.List[0].Names[0]
		recv, _ = info.Defs[id].(*types.Var)
		recvName = id.Name
	}
	qual := func(p *types.Package) string {
		if p == pass.Pkg {
			return ""
		}
		return p.Name()
	}
	pl := newPlacer(pass, decl)
	for _, site := range sendSites(pass, decl) {
		for _, msg := range site.msgs {
			for _, l := range pl.placed(msg) {
				t := info.TypeOf(l.expr)
				if t == nil {
					continue
				}
				rooted := recv != nil && l.root != nil && info.ObjectOf(l.root) == recv
				switch {
				case isContextPtr(t):
					r.report(l.expr.Pos(), idSharedState, types.ExprString(l.expr)+
						" (*lockstep.Context) is placed in a message; it is valid only during the current turn")
				case !rooted:
				case isIdentOf(info, l.expr, recv):
					if l.shared || containsRefs(pass, t) {
						r.report(l.expr.Pos(), idSharedState, "the actor itself ("+recvName+
							") is placed in a message; send ctx.Self() instead")
					}
				case l.shared:
					r.report(l.expr.Pos(), idSharedState,
						"the address of actor field "+fieldPath(l.expr)+" is placed in a message")
				case isFieldSelector(info, l.expr) && containsRefs(pass, t):
					r.report(l.expr.Pos(), idSharedState, "actor field "+fieldPath(l.expr)+" ("+
						types.TypeString(t, qual)+") is placed in a message; send a copy (slices.Clone, maps.Clone) "+
						"or mark its type //lockstep:immutable")
				}
			}
		}
	}
}

// isContextPtr reports whether t is *lockstep.Context[X] for some X.
func isContextPtr(t types.Type) bool {
	p, ok := types.Unalias(t).(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := types.Unalias(p.Elem()).(*types.Named)
	return ok && isAPI(named.Origin().Obj(), "Context")
}

// isIdentOf reports whether e is an identifier that refers to v.
func isIdentOf(info *types.Info, e ast.Expr, v *types.Var) bool {
	id, ok := e.(*ast.Ident)
	return ok && info.ObjectOf(id) == v
}

// isFieldSelector reports whether e is a selector of a struct field.
func isFieldSelector(info *types.Info, e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	s := info.Selections[sel]
	return s != nil && s.Kind() == types.FieldVal
}

// fieldPath returns the dotted path of the selector chain e, such as a.cache.items, without the
// parentheses around its parts.
func fieldPath(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.ParenExpr:
		return fieldPath(e.X)
	case *ast.SelectorExpr:
		return fieldPath(e.X) + "." + e.Sel.Name
	}
	return types.ExprString(e)
}
