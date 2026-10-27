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

	// recvs maps the receiver variable of every method declared in the package to the type name
	// of its receiver's base type, for the <recv> of ANL-340 and ANL-350. It is only looked up.
	recvs map[*types.Var]*types.TypeName
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
		recvs:  map[*types.Var]*types.TypeName{},
	}
	c.collectReceivers()
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

// collectReceivers fills c.recvs from the method declarations of the package.
func (c *cleanup) collectReceivers() {
	for _, file := range c.pass.Files {
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || len(fd.Recv.List) != 1 || len(fd.Recv.List[0].Names) != 1 {
				continue
			}
			v, ok := c.pass.TypesInfo.Defs[fd.Recv.List[0].Names[0]].(*types.Var)
			if !ok {
				continue
			}
			t := types.Unalias(v.Type())
			if p, ok := t.(*types.Pointer); ok {
				t = types.Unalias(p.Elem())
			}
			if named, ok := t.(*types.Named); ok {
				c.recvs[v] = named.Origin().Obj()
			}
		}
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

// fieldOf returns the index of the field of actor a that e selects as <recv>.<field>, where <recv>
// is the receiver of a method of a's type (ANL-340).
func (c *cleanup) fieldOf(e ast.Expr, a *actorType) (int, bool) {
	sel, ok := ast.Unparen(e).(*ast.SelectorExpr)
	if !ok {
		return 0, false
	}
	s := c.pass.TypesInfo.Selections[sel]
	if s == nil || s.Kind() != types.FieldVal || len(s.Index()) != 1 || !c.isReceiver(sel.X, a) {
		return 0, false
	}
	return s.Index()[0], true
}

// isReceiver reports whether e is the receiver variable of a method of a's type.
func (c *cleanup) isReceiver(e ast.Expr, a *actorType) bool {
	id, ok := ast.Unparen(e).(*ast.Ident)
	if !ok {
		return false
	}
	v, ok := c.pass.TypesInfo.Uses[id].(*types.Var)
	return ok && c.recvs[v] == a.Type.Obj()
}

// findOwned sets done for every field that ctx.Own receives in a's actor code (ANL-340): the
// selector <recv>.<field>, or a local variable that the same function also assigns to it.
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
			if field, ok := c.fieldOf(arg, a); ok {
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
// releases (ANL-350): a release method of the field's type called on <recv>.<field>, promoted
// through an embedded field or not, deferred or not, or <recv>.<field> passed to any call.
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
				if field, ok := c.fieldOf(arg, a); ok {
					done[field] = true
				}
			}
			return true
		})
	}
}

// releasedField returns the field of a on which the method selector sel is called: <recv>.<field>
// as the operand, or, for a method promoted from an embedded field, <recv> itself.
func (c *cleanup) releasedField(sel *ast.SelectorExpr, a *actorType, st *types.Struct) (int, bool) {
	s := c.pass.TypesInfo.Selections[sel]
	if s == nil || s.Kind() != types.MethodVal {
		return 0, false
	}
	if field, ok := c.fieldOf(sel.X, a); ok {
		return field, true
	}
	if path := s.Index(); len(path) > 1 && c.isReceiver(sel.X, a) && path[0] < st.NumFields() && st.Field(path[0]).Embedded() {
		return path[0], true
	}
	return 0, false
}

// checkResourceTypes reports the types of the package marked //lockstep:resource that have no
// Close, Stop or Release method (ANL-360).
func (c *cleanup) checkResourceTypes() {
	for _, tn := range c.idx.marked {
		if c.idx.markers[tn]&markResource == 0 {
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
