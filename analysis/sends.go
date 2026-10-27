// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/types/typeutil"
)

// sendSite is a call of lockstep.Send, ReplyTo.Send or lockstep.Ask (ANL-200).
type sendSite struct {
	call *ast.CallExpr
	api  string     // "lockstep.Send", "ReplyTo.Send" or "lockstep.Ask"
	msgs []ast.Expr // the message expressions: one, or the Ask build literal's return results
}

// leaf is one placed expression of a message (ANL-200). An identifier leaf need not name a
// variable: constants, nil, true, false and function names are identifiers too, so a consumer that
// tracks variables checks that the root's object is a *types.Var.
type leaf struct {
	expr   ast.Expr
	root   *ast.Ident // base of the selector, index and dereference chain
	shared bool       // reached through & or slicing
}

// sendSites returns the send sites in body, in source order (ANL-200). body is usually an
// *ast.FuncDecl or *ast.FuncLit; function literals nested in it are not entered, so that each
// function's sites are found once, when that function is the body (ANL-050). The function F of a
// site (ANL-220) is therefore the body passed here: the innermost FuncDecl or FuncLit holding the
// send call. The return results of an Ask build literal belong to the Ask site all the same: they
// are the messages of the literal's own return statements, without those of literals nested in
// it, so the leaves of an Ask lie inside the build literal while F is the function holding the
// Ask call. An Ask whose build function is not a literal has no message expressions.
func sendSites(pass *analysis.Pass, body ast.Node) []sendSite {
	var sites []sendSite
	ast.Inspect(body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncLit:
			return n == body
		case *ast.CallExpr:
			if s, ok := sendSiteOf(pass, n); ok {
				sites = append(sites, s)
			}
		}
		return true
	})
	return sites
}

// sendSiteOf returns the send site of call when call is one.
func sendSiteOf(pass *analysis.Pass, call *ast.CallExpr) (sendSite, bool) {
	callee := typeutil.Callee(pass.TypesInfo, call)
	args := call.Args
	// A method expression, lockstep.ReplyTo[R].Send(rt, from, v), takes the receiver first.
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok && len(args) > 0 {
		if s := pass.TypesInfo.Selections[sel]; s != nil && s.Kind() == types.MethodExpr {
			args = args[1:]
		}
	}
	switch {
	case isAPI(callee, "Send") && len(args) == 3:
		return sendSite{call: call, api: "lockstep.Send", msgs: []ast.Expr{args[2]}}, true
	case isAPI(callee, "ReplyTo.Send") && len(args) == 2:
		return sendSite{call: call, api: "ReplyTo.Send", msgs: []ast.Expr{args[1]}}, true
	case isAPI(callee, "Ask") && len(args) >= 3:
		return sendSite{call: call, api: "lockstep.Ask", msgs: returnResults(args[2])}, true
	}
	return sendSite{}, false
}

// returnResults returns the result expressions of the return statements of build when it is a
// function literal, without those of function literals nested in it.
func returnResults(build ast.Expr) []ast.Expr {
	lit, ok := ast.Unparen(build).(*ast.FuncLit)
	if !ok {
		return nil
	}
	var results []ast.Expr
	ast.Inspect(lit.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			results = append(results, n.Results...)
		}
		return true
	})
	return results
}

// placed returns the leaves of the placed expressions P(e) of message expression e, in the order
// of the walk of ANL-200. fn is the function F of the send site, as sendSites defines it: the
// innermost *ast.FuncDecl or *ast.FuncLit holding the send call, also for the leaves of an Ask,
// which lie in its build literal.
//
// A leaf that is a local variable declared in fn's syntax (in a function literal nested in it
// too, such as an Ask build literal) and assigned only by its declaration's initializer also
// yields the leaves of that initializer, each variable once per call. The shared mark passes
// through parentheses, & and slicing to the leaf they apply to; it does not pass through
// conversions, type assertions, composite literals or initializers. A composite literal whose
// type does not contain references (containsRefs false, for example a type marked
// //lockstep:immutable) places nothing: the receiver gets a safe value (ANL-200, AT-ANL-14).
//
// Each call scans fn for its assignments; a check with several sites in one function uses
// newPlacer once and calls its placed method per message instead.
func placed(pass *analysis.Pass, fn ast.Node, e ast.Expr) []leaf {
	return newPlacer(pass, fn).placed(e)
}

// placer computes P(e) for the send sites of one function F. It scans F's assignments once, on
// the first leaf that needs them.
type placer struct {
	pass     *analysis.Pass
	fn       ast.Node
	inits    map[*types.Var]ast.Expr // single-assignment locals of fn; built on first need
	expanded map[*types.Var]bool     // per placed call
	leaves   []leaf                  // per placed call
}

// newPlacer returns a placer for the send sites of fn, as placed defines fn.
func newPlacer(pass *analysis.Pass, fn ast.Node) *placer {
	return &placer{pass: pass, fn: fn}
}

// placed returns the leaves of P(e), as the function placed does.
func (p *placer) placed(e ast.Expr) []leaf {
	p.expanded = make(map[*types.Var]bool)
	p.leaves = nil
	p.walk(e, false)
	return p.leaves
}

// walk appends the leaves of P(e); shared marks e's own leaf as reached through & or slicing.
func (p *placer) walk(e ast.Expr, shared bool) {
	info := p.pass.TypesInfo
	switch e := e.(type) {
	case *ast.ParenExpr:
		p.walk(e.X, shared)
	case *ast.CompositeLit:
		t := info.TypeOf(e)
		if t == nil || !containsRefs(p.pass, t) {
			return
		}
		_, isMap := t.Underlying().(*types.Map)
		for _, elt := range e.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				p.walk(elt, false)
				continue
			}
			if isMap {
				p.walk(kv.Key, false)
			}
			p.walk(kv.Value, false)
		}
	case *ast.UnaryExpr:
		if e.Op == token.AND {
			p.walk(e.X, true)
		}
	case *ast.SliceExpr:
		p.walk(e.X, true)
	case *ast.TypeAssertExpr:
		p.walk(e.X, false)
	case *ast.CallExpr:
		if len(e.Args) == 1 && info.Types[e.Fun].IsType() {
			p.walk(e.Args[0], false) // a conversion
		}
	default:
		if !isLeaf(info, e) {
			return
		}
		p.leaves = append(p.leaves, leaf{expr: e, root: leafRoot(info, e), shared: shared})
		if id, ok := e.(*ast.Ident); ok {
			p.expand(id)
		}
	}
}

// expand walks the initializer of the local variable id refers to, when it has exactly one
// assignment, its declaration, and was not expanded before.
func (p *placer) expand(id *ast.Ident) {
	v, ok := p.pass.TypesInfo.Uses[id].(*types.Var)
	if !ok || p.expanded[v] {
		return
	}
	if p.inits == nil {
		p.inits = singleAssignments(p.pass.TypesInfo, p.fn)
	}
	init, ok := p.inits[v]
	if !ok {
		return
	}
	p.expanded[v] = true
	p.walk(init, false)
}

// isLeaf reports whether e is a leaf of ANL-200: an identifier (a qualified one included), a field
// selector, an index expression or a dereference.
func isLeaf(info *types.Info, e ast.Expr) bool {
	switch e := e.(type) {
	case *ast.Ident:
		return true
	case *ast.SelectorExpr:
		if s := info.Selections[e]; s != nil {
			return s.Kind() == types.FieldVal
		}
		return isQualified(info, e)
	case *ast.IndexExpr:
		return isElementIndex(info, e)
	case *ast.StarExpr:
		return !info.Types[e].IsType()
	}
	return false
}

// leafRoot returns the root of e as ANL-200 defines it: the identifier at the base of e's chain of
// field selectors, index expressions and dereferences (parentheses ignored), or nil when the
// chain starts at another expression, such as a call. For a qualified identifier such as os.Args
// it returns the selected name, which refers to the package variable. It applies to any
// expression, not only to leaves, so the checks that need the root of a written expression
// (ANL-270) use it too.
func leafRoot(info *types.Info, e ast.Expr) *ast.Ident {
	for {
		switch x := ast.Unparen(e).(type) {
		case *ast.Ident:
			return x
		case *ast.SelectorExpr:
			if isQualified(info, x) {
				return x.Sel
			}
			if s := info.Selections[x]; s == nil || s.Kind() != types.FieldVal {
				return nil
			}
			e = x.X
		case *ast.IndexExpr:
			if !isElementIndex(info, x) {
				return nil
			}
			e = x.X
		case *ast.StarExpr:
			e = x.X
		default:
			return nil
		}
	}
}

// isQualified reports whether sel is a qualified identifier, pkg.Name.
func isQualified(info *types.Info, sel *ast.SelectorExpr) bool {
	id, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	_, ok = info.Uses[id].(*types.PkgName)
	return ok
}

// isElementIndex reports whether ix indexes an array, slice, string, map or pointer to array, as
// opposed to instantiating a generic function or type.
func isElementIndex(info *types.Info, ix *ast.IndexExpr) bool {
	tv, ok := info.Types[ix.X]
	if !ok || tv.IsType() {
		return false
	}
	_, isFunc := tv.Type.Underlying().(*types.Signature)
	return !isFunc
}

// singleAssignments returns the local variables declared in fn's syntax that are assigned exactly
// once, by an initializer in their declaration, mapped to that initializer. Assignments anywhere
// in fn count, in nested function literals too.
func singleAssignments(info *types.Info, fn ast.Node) map[*types.Var]ast.Expr {
	count := make(map[*types.Var]int)
	inits := make(map[*types.Var]ast.Expr)
	assign := func(id *ast.Ident, init ast.Expr) {
		v, ok := info.ObjectOf(id).(*types.Var)
		if !ok {
			return
		}
		count[v]++
		if init != nil && info.Defs[id] == v {
			inits[v] = init
		}
	}
	ast.Inspect(fn, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				id, ok := ast.Unparen(lhs).(*ast.Ident)
				if !ok {
					continue
				}
				var init ast.Expr
				if n.Tok == token.DEFINE && len(n.Lhs) == len(n.Rhs) {
					init = n.Rhs[i]
				}
				assign(id, init)
			}
		case *ast.ValueSpec:
			for i, id := range n.Names {
				var init ast.Expr
				if len(n.Names) == len(n.Values) {
					init = n.Values[i]
				}
				assign(id, init)
			}
		case *ast.RangeStmt:
			for _, x := range []ast.Expr{n.Key, n.Value} {
				if id, ok := ast.Unparen(x).(*ast.Ident); ok {
					assign(id, nil)
				}
			}
		case *ast.IncDecStmt:
			if id, ok := ast.Unparen(n.X).(*ast.Ident); ok {
				assign(id, nil)
			}
		}
		return true
	})
	for v := range inits {
		if count[v] != 1 {
			delete(inits, v)
		}
	}
	return inits
}
