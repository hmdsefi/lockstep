// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/token"
	"go/types"
	"slices"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/types/typeutil"
)

// Cleanup reports actor fields holding resources that are neither passed to ctx.Own nor released
// in Stopped (LS240).
var Cleanup = &analysis.Analyzer{
	Name:     "cleanup",
	Doc:      "report actor resources that are never released (LS240)",
	Requires: []*analysis.Analyzer{actorsAnalyzer, indexAnalyzer},
	Run:      runCleanup,
}

// idCleanup is the check ID of Cleanup.
const idCleanup = "LS240"

// resourceReleaseNames are the release methods of a //lockstep:resource type (ANL-350).
var resourceReleaseNames = []string{"Close", "Stop", "Release"}

// cleanup holds the state of one run of Cleanup on one package.
type cleanup struct {
	pass *analysis.Pass
	code *actorCode
	idx  *directiveIndex
	rep  *reporter

	// closer is interface{ Close() error }, the structure of io.Closer (ANL-340). It is built
	// here so that the analyzed package need not import io.
	closer *types.Interface

	// locals records how often each local variable of the package is assigned and, for the last
	// assignment seen, the initializer, so that a variable assigned once from <recv>.<field> can
	// stand for the field (ANL-340, ANL-350). It is only looked up.
	locals map[*types.Var]*localAssign
}

// localAssign is what the package assigns to one local variable.
type localAssign struct {
	n    int      // assignments, declarations with an initializer, range heads, ++ and --, and &v
	init ast.Expr // the right-hand side of the last counted assignment, nil when it has none
}

func runCleanup(pass *analysis.Pass) (any, error) {
	code, ok := pass.ResultOf[actorsAnalyzer].(*actorCode)
	if !ok {
		return nil, nil
	}
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	c := &cleanup{
		pass:   pass,
		code:   code,
		idx:    idx,
		rep:    newReporter(pass, idx),
		closer: closerInterface(),
		locals: map[*types.Var]*localAssign{},
	}
	c.collectLocals()
	for _, a := range code.Actors {
		c.checkActor(a)
	}
	c.checkResourceTypes()
	c.rep.flush()
	return nil, nil
}

// closerInterface returns interface{ Close() error }.
func closerInterface() *types.Interface {
	errType := types.Universe.Lookup("error").Type()
	sig := types.NewSignatureType(nil, nil, nil, nil, types.NewTuple(types.NewVar(token.NoPos, nil, "", errType)), false)
	closeFn := types.NewFunc(token.NoPos, nil, "Close", sig)
	return types.NewInterfaceType([]*types.Func{closeFn}, nil).Complete()
}

// collectLocals fills c.locals from every function of the package. A variable whose address is
// taken counts as assigned once more, because it can then change out of sight.
func (c *cleanup) collectLocals() {
	note := func(e ast.Expr, init ast.Expr) {
		id, ok := e.(*ast.Ident)
		if !ok {
			return
		}
		v, ok := c.pass.TypesInfo.ObjectOf(id).(*types.Var)
		if !ok || v.IsField() {
			return
		}
		la := c.locals[v]
		if la == nil {
			la = &localAssign{}
			c.locals[v] = la
		}
		la.n++
		la.init = init
	}
	pairs := func(lhs []ast.Expr, rhs []ast.Expr) {
		for i, l := range lhs {
			var init ast.Expr
			if len(lhs) == len(rhs) {
				init = rhs[i]
			}
			note(l, init)
		}
	}
	for _, file := range c.pass.Files {
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				if n.Tok == token.ASSIGN || n.Tok == token.DEFINE {
					pairs(n.Lhs, n.Rhs)
				} else {
					for _, l := range n.Lhs {
						note(l, nil)
					}
				}
			case *ast.ValueSpec:
				idents := make([]ast.Expr, len(n.Names))
				for i, name := range n.Names {
					idents[i] = name
				}
				if len(n.Values) > 0 {
					pairs(idents, n.Values)
				}
			case *ast.IncDecStmt:
				note(ast.Unparen(n.X), nil)
			case *ast.RangeStmt:
				if n.Key != nil {
					note(n.Key, nil)
				}
				if n.Value != nil {
					note(n.Value, nil)
				}
			case *ast.UnaryExpr:
				if n.Op == token.AND {
					note(ast.Unparen(n.X), nil)
				}
			}
			return true
		})
	}
}

// releaseNames returns the names of the release methods of a resource field of type t (ANL-340,
// ANL-350), or nil when t is not a resource type: Close when t or *t implements io.Closer, Stop
// for *time.Timer and *time.Ticker, and Close, Stop and Release for a marked type or a pointer to
// one.
func (c *cleanup) releaseNames(t types.Type) []string {
	var names []string
	add := func(ns ...string) {
		for _, n := range ns {
			if !slices.Contains(names, n) {
				names = append(names, n)
			}
		}
	}
	if types.Implements(t, c.closer) || types.Implements(types.NewPointer(t), c.closer) {
		add("Close")
	}
	if isTimeTimerPtr(t) {
		add("Stop")
	}
	if c.hasResourceMarker(t) {
		add(resourceReleaseNames...)
	}
	return names
}

// hasResourceMarker reports whether t, or the type t points to, is marked //lockstep:resource.
func (c *cleanup) hasResourceMarker(t types.Type) bool {
	t = types.Unalias(t)
	if c.idx.typeHasMarker(t, markResource) {
		return true
	}
	p, ok := t.(*types.Pointer)
	return ok && c.idx.typeHasMarker(p.Elem(), markResource)
}

// isTimeTimerPtr reports whether t is *time.Timer or *time.Ticker.
func isTimeTimerPtr(t types.Type) bool {
	p, ok := types.Unalias(t).(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := types.Unalias(p.Elem()).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == "time" && (obj.Name() == "Timer" || obj.Name() == "Ticker")
}

// checkActor reports the resource fields of actor a that are neither owned nor released
// (ANL-340 to ANL-360).
func (c *cleanup) checkActor(a *actorType) {
	st, ok := a.Type.Underlying().(*types.Struct)
	if !ok {
		return
	}
	// release[i] holds the release method names of field i, nil for a field that is no resource.
	release := make([][]string, st.NumFields())
	resources := 0
	for i := range st.NumFields() {
		release[i] = c.releaseNames(st.Field(i).Type())
		if release[i] != nil {
			resources++
		}
	}
	if resources == 0 {
		return
	}
	done := make([]bool, st.NumFields())
	c.findOwned(a, done)
	c.findReleased(a, st, release, done)
	qual := func(p *types.Package) string {
		if p == c.pass.Pkg {
			return ""
		}
		return p.Name()
	}
	for i := range st.NumFields() {
		f := st.Field(i)
		if release[i] == nil || done[i] || f.Pkg() != c.pass.Pkg {
			continue
		}
		c.rep.report(f.Pos(), idCleanup, "field "+f.Name()+" ("+types.TypeString(f.Type(), qual)+") of actor "+
			a.Type.Obj().Name()+" is neither passed to ctx.Own nor released in Stopped")
	}
}

// fieldOf returns the index of the field of actor a that e selects as x.<field>, where x has the
// actor's type or a pointer to it: the method receiver, a parameter or a local (ANL-340,
// ANL-350).
func (c *cleanup) fieldOf(e ast.Expr, a *actorType) (int, bool) {
	sel, ok := ast.Unparen(e).(*ast.SelectorExpr)
	if !ok {
		return 0, false
	}
	s := c.pass.TypesInfo.Selections[sel]
	if s == nil || s.Kind() != types.FieldVal || len(s.Index()) != 1 || !c.isActor(sel.X, a) {
		return 0, false
	}
	return s.Index()[0], true
}

// fieldRef is fieldOf, or, for a local variable that the package assigns exactly once, from
// x.<field>, the field that variable stands for (ANL-340, ANL-350).
func (c *cleanup) fieldRef(e ast.Expr, a *actorType) (int, bool) {
	if field, ok := c.fieldOf(e, a); ok {
		return field, true
	}
	id, ok := ast.Unparen(e).(*ast.Ident)
	if !ok {
		return 0, false
	}
	v, ok := c.pass.TypesInfo.Uses[id].(*types.Var)
	if !ok {
		return 0, false
	}
	if la := c.locals[v]; la != nil && la.n == 1 && la.init != nil {
		return c.fieldOf(la.init, a)
	}
	return 0, false
}

// isActor reports whether e has the type of actor a or a pointer to it.
func (c *cleanup) isActor(e ast.Expr, a *actorType) bool {
	t := c.pass.TypesInfo.TypeOf(e)
	if t == nil {
		return false
	}
	t = types.Unalias(t)
	if p, ok := t.(*types.Pointer); ok {
		t = types.Unalias(p.Elem())
	}
	named, ok := t.(*types.Named)
	return ok && named.Origin().Obj() == a.Type.Obj()
}

// findOwned sets done for every field that ctx.Own receives in a's actor code (ANL-340): the
// selector x.<field>, a local variable that stands for it, or a local variable that the same
// function also assigns to it.
func (c *cleanup) findOwned(a *actorType, done []bool) {
	info := c.pass.TypesInfo
	for _, f := range c.code.reach(a.Roots...) {
		// stored[v] holds the fields that the function assigns the local variable v to.
		stored := map[*types.Var][]int{}
		f.inspect(func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || as.Tok != token.ASSIGN || len(as.Lhs) != len(as.Rhs) {
				return true
			}
			for i, lhs := range as.Lhs {
				field, ok := c.fieldOf(lhs, a)
				rhs, isIdent := ast.Unparen(as.Rhs[i]).(*ast.Ident)
				if !ok || !isIdent {
					continue
				}
				if v, ok := info.Uses[rhs].(*types.Var); ok {
					stored[v] = append(stored[v], field)
				}
			}
			return true
		})
		f.inspect(func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isAPI(typeutil.Callee(info, call), "Context.Own") {
				return true
			}
			arg := ownedArg(info, call)
			if arg == nil {
				return true
			}
			if field, ok := c.fieldRef(arg, a); ok {
				done[field] = true
			}
			if id, ok := ast.Unparen(arg).(*ast.Ident); ok {
				if v, ok := info.Uses[id].(*types.Var); ok {
					for _, field := range stored[v] {
						done[field] = true
					}
				}
			}
			return true
		})
	}
}

// ownedArg returns the resource argument of a call of Context.Own: argument 0 of ctx.Own(r), and
// argument 1 of the method expression (*lockstep.Context[M]).Own(ctx, r). It returns nil when
// the argument is missing.
func ownedArg(info *types.Info, call *ast.CallExpr) ast.Expr {
	i := 0
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok {
		if s := info.Selections[sel]; s != nil && s.Kind() == types.MethodExpr {
			i = 1
		}
	}
	if i >= len(call.Args) {
		return nil
	}
	return call.Args[i]
}

// findReleased sets done for every field that a's Stopped root, or a function reachable from it,
// releases (ANL-350): a release method of the field's type called on x.<field> or on a local
// variable that stands for it, promoted through an embedded field or not, deferred or not, or
// x.<field> or such a local passed to any call.
func (c *cleanup) findReleased(a *actorType, st *types.Struct, release [][]string, done []bool) {
	var stopped *actorFunc
	for _, root := range a.Roots {
		if root.Fn.Name() == "Stopped" {
			stopped = root
		}
	}
	if stopped == nil {
		return
	}
	info := c.pass.TypesInfo
	for _, f := range c.code.reach(stopped) {
		f.inspect(func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok {
				if field, ok := c.releasedField(sel, a, st); ok && slices.Contains(release[field], sel.Sel.Name) {
					done[field] = true
				}
			}
			if info.Types[call.Fun].IsType() {
				return true // a conversion is not a call
			}
			for _, arg := range call.Args {
				if field, ok := c.fieldRef(arg, a); ok {
					done[field] = true
				}
			}
			return true
		})
	}
}

// releasedField returns the field of a on which the method selector sel is called: x.<field> (or
// a local standing for it) as the operand, or, for a method promoted from an embedded field, x
// itself.
func (c *cleanup) releasedField(sel *ast.SelectorExpr, a *actorType, st *types.Struct) (int, bool) {
	s := c.pass.TypesInfo.Selections[sel]
	if s == nil || s.Kind() != types.MethodVal {
		return 0, false
	}
	if field, ok := c.fieldRef(sel.X, a); ok {
		return field, true
	}
	if path := s.Index(); len(path) > 1 && c.isActor(sel.X, a) && path[0] < st.NumFields() && st.Field(path[0]).Embedded() {
		return path[0], true
	}
	return 0, false
}

// checkResourceTypes reports the types of the package marked //lockstep:resource that have no
// Close, Stop or Release method (ANL-360).
func (c *cleanup) checkResourceTypes() {
	for _, tn := range c.idx.marked {
		if !c.idx.hasMarker(tn, markResource) {
			continue
		}
		named, ok := tn.Type().(*types.Named)
		if !ok || hasReleaseMethod(named) {
			continue
		}
		c.rep.report(tn.Pos(), idCleanup, "resource type "+tn.Name()+" has no Close, Stop or Release method")
	}
}

// hasReleaseMethod reports whether the method set of named or of *named has a method called
// Close, Stop or Release. The method set of a pointer to an interface is empty, so the type itself
// is looked at too.
func hasReleaseMethod(named *types.Named) bool {
	t := types.Type(named)
	if self := selfInstance(named); self != nil {
		t = self
	}
	for _, set := range []*types.MethodSet{types.NewMethodSet(t), types.NewMethodSet(types.NewPointer(t))} {
		for _, name := range resourceReleaseNames {
			if set.Lookup(nil, name) != nil {
				return true
			}
		}
	}
	return false
}
