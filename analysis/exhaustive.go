// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/types/typeutil"
)

// Exhaustive reports message types that have no case in Receive (LS230).
var Exhaustive = &analysis.Analyzer{
	Name:      "exhaustive",
	Doc:       "report message types with no case in Receive (LS230)",
	Requires:  []*analysis.Analyzer{actorsAnalyzer, indexAnalyzer},
	FactTypes: []analysis.Fact{new(sealedFact)},
	Run:       runExhaustive,
}

// idExhaustive is the check ID of Exhaustive.
const idExhaustive = "LS230"

// sealedMember is one exported member of a sealed interface (ANL-320): the name of a type declared
// at package scope of the interface's package, and whether the member is the pointer to it.
type sealedMember struct {
	Name    string
	Pointer bool
}

// sealedFact is the fact Exhaustive exports for a sealed interface (ANL-320). It lists the
// interface's exported members sorted by Name; its fields are exported for gob encoding.
type sealedFact struct{ Members []sealedMember }

// AFact marks sealedFact as an analysis fact.
func (*sealedFact) AFact() {}

// String lists the members, pointer members with a leading *, for example "sealed(*Close, Open)".
func (f *sealedFact) String() string {
	names := make([]string, len(f.Members))
	for i, m := range f.Members {
		names[i] = m.Name
		if m.Pointer {
			names[i] = "*" + m.Name
		}
	}
	return "sealed(" + strings.Join(names, ", ") + ")"
}

// exhaustiveExpected is one expected message type of an actor (ANL-310).
type exhaustiveExpected struct {
	t      types.Type
	member bool // a member of the sealed message type, else a self-message type only
}

func runExhaustive(pass *analysis.Pass) (any, error) {
	exhaustiveExportFacts(pass)
	code, ok := pass.ResultOf[actorsAnalyzer].(*actorCode)
	if !ok {
		return nil, nil
	}
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	decls := map[*types.Func]*ast.FuncDecl{}
	for _, f := range pass.Files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				if fn, ok := pass.TypesInfo.Defs[fd.Name].(*types.Func); ok {
					decls[fn] = fd
				}
			}
		}
	}
	r := newReporter(pass, idx)
	for _, a := range code.Actors {
		exhaustiveCheck(pass, r, code, decls, a)
	}
	r.flush()
	return nil, nil
}

// exhaustiveExportFacts exports a sealedFact on every sealed interface declared at package scope
// of the analyzed package (ANL-320), in the sorted order of the scope's names.
func exhaustiveExportFacts(pass *analysis.Pass) {
	scope := pass.Pkg.Scope()
	for _, name := range scope.Names() {
		tn, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || tn.IsAlias() {
			continue
		}
		named, ok := tn.Type().(*types.Named)
		if !ok || named.TypeParams().Len() != 0 || !exhaustiveSealed(named) {
			continue
		}
		fact := &sealedFact{}
		for _, m := range exhaustiveLocalMembers(pass.Fset, named) {
			_, ptr := m.(*types.Pointer)
			obj := exhaustiveNamed(m).Obj()
			if obj.Exported() {
				fact.Members = append(fact.Members, sealedMember{Name: obj.Name(), Pointer: ptr})
			}
		}
		slices.SortFunc(fact.Members, func(x, y sealedMember) int { return strings.Compare(x.Name, y.Name) })
		pass.ExportObjectFact(tn, fact)
	}
}

// exhaustiveSealed reports whether named is an interface whose method set has an unexported
// method.
func exhaustiveSealed(named *types.Named) bool {
	iface, ok := named.Underlying().(*types.Interface)
	if !ok {
		return false
	}
	for i := range iface.NumMethods() {
		if !iface.Method(i).Exported() {
			return true
		}
	}
	return false
}

// exhaustiveNamed returns the named type of a member: t itself or the type t points to.
func exhaustiveNamed(t types.Type) *types.Named {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	named, _ := t.(*types.Named)
	return named
}

// exhaustiveLocalMembers returns the members of the sealed interface named, computed from the
// scope of its package (ANL-320): every non-interface, non-generic named type of that scope that
// implements it, or else whose pointer does, in the order of the scope's names. Two kinds of type
// are left out: a type embedded as a field in another member, such as a base type that only
// carries the sealing method, and, when the interface is declared outside a _test.go file, a type
// declared in one.
func exhaustiveLocalMembers(fset *token.FileSet, named *types.Named) []types.Type {
	iface, ok := named.Underlying().(*types.Interface)
	if !ok {
		return nil
	}
	isTest := func(obj types.Object) bool {
		return strings.HasSuffix(fset.PositionFor(obj.Pos(), false).Filename, "_test.go")
	}
	ifaceInTest := isTest(named.Obj())
	scope := named.Obj().Pkg().Scope()
	var candidates []types.Type
	for _, name := range scope.Names() {
		tn, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || tn.IsAlias() || !ifaceInTest && isTest(tn) {
			continue
		}
		t, ok := tn.Type().(*types.Named)
		if !ok || t.TypeParams().Len() != 0 || types.IsInterface(t) {
			continue
		}
		switch {
		case types.Implements(t, iface):
			candidates = append(candidates, t)
		case types.Implements(types.NewPointer(t), iface):
			candidates = append(candidates, types.NewPointer(t))
		}
	}
	var embedded []*types.Named
	for _, c := range candidates {
		st, ok := exhaustiveNamed(c).Underlying().(*types.Struct)
		if !ok {
			continue
		}
		for f := range st.Fields() {
			if f.Embedded() {
				embedded = append(embedded, exhaustiveNamed(types.Unalias(f.Type())))
			}
		}
	}
	return slices.DeleteFunc(candidates, func(c types.Type) bool {
		return slices.Contains(embedded, exhaustiveNamed(c))
	})
}

// exhaustiveMembers returns the members of message type m when it is a sealed interface
// (ANL-320). For an interface of another package they come from its sealedFact, so only its
// exported members are returned.
func exhaustiveMembers(pass *analysis.Pass, m types.Type) []types.Type {
	named, ok := types.Unalias(m).(*types.Named)
	if !ok || !exhaustiveSealed(named) {
		return nil
	}
	if named.Obj().Pkg() == pass.Pkg {
		return exhaustiveLocalMembers(pass.Fset, named)
	}
	var fact sealedFact
	if named.TypeArgs().Len() != 0 || !pass.ImportObjectFact(named.Obj(), &fact) {
		return nil
	}
	scope := named.Obj().Pkg().Scope()
	var members []types.Type
	for _, sm := range fact.Members {
		tn, ok := scope.Lookup(sm.Name).(*types.TypeName)
		if !ok {
			continue
		}
		if sm.Pointer {
			members = append(members, types.NewPointer(tn.Type()))
		} else {
			members = append(members, tn.Type())
		}
	}
	return members
}

// exhaustiveSelfTypes returns the self-message types found in the actor-code functions fns
// (ANL-310), in the order they are found: the static types of the message argument of ctx.After
// and ctx.Every, and of the results of the return statements of a function literal passed as
// done to lockstep.Do or as to to lockstep.Pipe. Interface types are left out.
func exhaustiveSelfTypes(pass *analysis.Pass, fns []*actorFunc) []types.Type {
	info := pass.TypesInfo
	var out []types.Type
	add := func(e ast.Expr) {
		t := info.TypeOf(e)
		if t == nil || types.IsInterface(t) {
			return
		}
		if b, ok := t.(*types.Basic); ok && b.Info()&types.IsUntyped != 0 {
			return
		}
		out = append(out, t)
	}
	for _, f := range fns {
		f.inspect(func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			callee := typeutil.Callee(info, call)
			args := exhaustiveArgs(info, call)
			switch {
			case (isAPI(callee, "Context.After") || isAPI(callee, "Context.Every")) && len(args) == 2:
				add(args[1])
			case (isAPI(callee, "Do") || isAPI(callee, "Pipe")) && len(args) >= 3:
				for _, res := range returnResults(args[2]) {
					add(res)
				}
			}
			return true
		})
	}
	return out
}

// exhaustiveCheck reports the expected message types of actor a that its Receive and delegates
// have no case for (ANL-330).
func exhaustiveCheck(pass *analysis.Pass, r *reporter, code *actorCode, decls map[*types.Func]*ast.FuncDecl, a *actorType) {
	decl := decls[a.Receive]
	if decl == nil {
		return // declared in another package: its cases are unknown
	}
	var expected []exhaustiveExpected
	for _, t := range exhaustiveMembers(pass, a.Message) {
		expected = append(expected, exhaustiveExpected{t: t, member: true})
	}
	for _, t := range exhaustiveSelfTypes(pass, code.reach(a.Roots...)) {
		if !slices.ContainsFunc(expected, func(e exhaustiveExpected) bool { return types.Identical(e.t, t) }) {
			expected = append(expected, exhaustiveExpected{t: t})
		}
	}
	if len(expected) == 0 {
		return
	}
	msg := a.Receive.Signature().Params().At(1)
	w := &exhaustiveWalk{pass: pass, decls: decls, visited: map[*ast.FuncDecl]bool{}}
	w.visit(decl, msg)
	if !w.inspected || w.forwarded {
		return // messages handled uniformly
	}
	qual := func(p *types.Package) string {
		if p == pass.Pkg {
			return ""
		}
		return p.Name()
	}
	var uncovered []exhaustiveExpected
	for _, e := range expected {
		if !w.covers(e.t) {
			uncovered = append(uncovered, e)
		}
	}
	slices.SortStableFunc(uncovered, func(x, y exhaustiveExpected) int {
		return strings.Compare(types.TypeString(x.t, qual), types.TypeString(y.t, qual))
	})
	pos := decl.Name.Pos()
	if i := slices.IndexFunc(w.switches, token.Pos.IsValid); i >= 0 {
		pos = w.switches[i]
	}
	recv := exhaustiveRecv(a.Receive)
	for _, e := range uncovered {
		text := recv + ".Receive has no case for message type " + types.TypeString(e.t, qual)
		if e.member {
			text += " of " + types.TypeString(a.Message, qual)
		} else {
			text += ", which the actor sends to itself"
		}
		r.report(pos, idExhaustive, text)
	}
}

// exhaustiveRecv returns the receiver of method fn as ANL-330 writes it: T or (*T).
func exhaustiveRecv(fn *types.Func) string {
	t := fn.Signature().Recv().Type()
	if p, ok := types.Unalias(t).(*types.Pointer); ok {
		return "(*" + receiverTypeName(p) + ")"
	}
	return receiverTypeName(t)
}

// exhaustiveWalk collects the case set of one actor from Receive and its delegates (ANL-330).
type exhaustiveWalk struct {
	pass    *analysis.Pass
	decls   map[*types.Func]*ast.FuncDecl
	visited map[*ast.FuncDecl]bool

	cases     []types.Type
	inspected bool // a type switch or comma-ok assertion on the message was found
	forwarded bool // the message was passed to a call with no static callee

	// switches holds, per visited function in visit order, the position of the switch keyword of
	// its first type switch on the message, or token.NoPos.
	switches []token.Pos
}

// visit walks decl, in which the parameter msg plays the role of Receive's message parameter, and
// the delegates it calls with msg, each when the call is reached in source order. In a type switch
// on msg that binds a variable, the variable holds msg unchanged in the clauses where its type is
// msg's type (default and clauses listing several types), so it plays msg's role there too
// (ANL-330).
func (w *exhaustiveWalk) visit(decl *ast.FuncDecl, msg *types.Var) {
	w.visited[decl] = true
	i := len(w.switches)
	w.switches = append(w.switches, token.NoPos)
	info := w.pass.TypesInfo
	roles := []types.Object{msg}
	isMsg := func(e ast.Expr) bool {
		id, ok := ast.Unparen(e).(*ast.Ident)
		return ok && slices.Contains(roles, info.Uses[id])
	}
	commaOK := func(e ast.Expr) {
		if ta, ok := ast.Unparen(e).(*ast.TypeAssertExpr); ok && ta.Type != nil && isMsg(ta.X) {
			w.inspected = true
			w.cases = append(w.cases, info.TypeOf(ta.Type))
		}
	}
	ast.Inspect(decl.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.TypeSwitchStmt:
			if !isMsg(exhaustiveSwitchSubject(n)) {
				return true
			}
			w.inspected = true
			if !w.switches[i].IsValid() {
				w.switches[i] = n.Switch
			}
			for _, s := range n.Body.List {
				clause, ok := s.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, e := range clause.List {
					if tv := info.Types[e]; tv.IsType() {
						w.cases = append(w.cases, tv.Type)
					}
				}
				if v := info.Implicits[clause]; v != nil && types.Identical(v.Type(), msg.Type()) {
					roles = append(roles, v)
				}
			}
		case *ast.AssignStmt:
			if len(n.Lhs) == 2 && len(n.Rhs) == 1 {
				commaOK(n.Rhs[0])
			}
		case *ast.ValueSpec:
			if len(n.Names) == 2 && len(n.Values) == 1 {
				commaOK(n.Values[0])
			}
		case *ast.CallExpr:
			w.call(n, msg.Type(), isMsg)
		}
		return true
	})
}

// exhaustiveSwitchSubject returns x of the type switch's x.(type), or nil.
func exhaustiveSwitchSubject(s *ast.TypeSwitchStmt) ast.Expr {
	var e ast.Expr
	switch a := s.Assign.(type) {
	case *ast.ExprStmt:
		e = a.X
	case *ast.AssignStmt:
		if len(a.Rhs) == 1 {
			e = a.Rhs[0]
		}
	}
	if ta, ok := ast.Unparen(e).(*ast.TypeAssertExpr); ok {
		return ta.X
	}
	return nil
}

// call handles a call that passes the message, unchanged, as an argument (ANL-330). A
// same-package function or method called statically, whose parameter in that position has the
// message's type msgType, is a delegate, visited unless it was before. A call with no static
// callee, through a function value of any form, may handle any message: the actor then handles
// messages uniformly. A call of an interface method, a builtin or a function of another package is
// neither.
func (w *exhaustiveWalk) call(call *ast.CallExpr, msgType types.Type, isMsg func(ast.Expr) bool) {
	if !slices.ContainsFunc(call.Args, isMsg) {
		return
	}
	info := w.pass.TypesInfo
	callee := typeutil.StaticCallee(info, call)
	if callee == nil {
		if exhaustiveFuncValue(info, call.Fun) {
			w.forwarded = true
		}
		return
	}
	callee = callee.Origin()
	decl := w.decls[callee]
	if decl == nil || w.visited[decl] {
		return
	}
	sig := callee.Signature()
	for i, arg := range exhaustiveArgs(info, call) {
		if i >= sig.Params().Len() || sig.Variadic() && i >= sig.Params().Len()-1 {
			break
		}
		if p := sig.Params().At(i); isMsg(arg) && types.Identical(p.Type(), msgType) {
			w.visit(decl, p)
			return
		}
	}
}

// exhaustiveArgs returns the arguments of call that match the callee's parameters: for a method
// expression, (*lockstep.Context[M]).After(ctx, d, msg), the receiver argument is left out.
func exhaustiveArgs(info *types.Info, call *ast.CallExpr) []ast.Expr {
	args := call.Args
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok && len(args) > 0 {
		if s := info.Selections[sel]; s != nil && s.Kind() == types.MethodExpr {
			args = args[1:]
		}
	}
	return args
}

// exhaustiveFuncValue reports whether fun, the operand of a call with no static callee, is a
// function value: not a type (a conversion), a builtin or an interface method.
func exhaustiveFuncValue(info *types.Info, fun ast.Expr) bool {
	fun = ast.Unparen(fun)
	if info.Types[fun].IsType() {
		return false
	}
	switch f := fun.(type) {
	case *ast.Ident:
		if _, ok := info.Uses[f].(*types.Builtin); ok {
			return false
		}
	case *ast.SelectorExpr:
		if _, ok := info.Uses[f.Sel].(*types.Builtin); ok {
			return false
		}
		if s := info.Selections[f]; s != nil && s.Kind() != types.FieldVal {
			if m, ok := s.Obj().(*types.Func); ok && types.IsInterface(m.Signature().Recv().Type()) {
				return false
			}
		}
	}
	t := info.TypeOf(fun)
	if t == nil {
		return false
	}
	_, ok := t.Underlying().(*types.Signature)
	return ok
}

// covers reports whether the case set contains t or an interface type that t implements.
func (w *exhaustiveWalk) covers(t types.Type) bool {
	return slices.ContainsFunc(w.cases, func(c types.Type) bool {
		if types.Identical(c, t) {
			return true
		}
		iface, ok := c.Underlying().(*types.Interface)
		return ok && types.Implements(t, iface)
	})
}
