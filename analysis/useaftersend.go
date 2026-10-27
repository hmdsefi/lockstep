// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
	"golang.org/x/tools/go/cfg"
)

// UseAfterSend reports uses of a message's reference-typed parts after it is sent (LS200).
var UseAfterSend = &analysis.Analyzer{
	Name:     "useaftersend",
	Doc:      "report use of a message's references after it is sent (LS200)",
	Requires: []*analysis.Analyzer{indexAnalyzer, inspect.Analyzer},
	Run:      runUseAfterSend,
}

// idUseAfterSend is the check ID of UseAfterSend.
const idUseAfterSend = "LS200"

// useAfterSendText is the format of an LS200 message without its ID: the variable, the API and
// the line of the send.
const useAfterSendText = "%s is used after it was sent with %s on line %d; " +
	"the receiver owns it now, so copy it before sending or stop using it"

func runUseAfterSend(pass *analysis.Pass) (any, error) {
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	ins, ok := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	if !ok {
		return nil, nil
	}
	r := newReporter(pass, idx)
	// Every function of the package, literals included, is checked on its own (ANL-200): a send
	// in a function literal belongs to the literal.
	ins.Preorder([]ast.Node{(*ast.FuncDecl)(nil), (*ast.FuncLit)(nil)}, func(n ast.Node) {
		if fd, ok := n.(*ast.FuncDecl); ok && fd.Body == nil {
			return
		}
		checkUseAfterSend(pass, r, n)
	})
	r.flush()
	return nil, nil
}

// checkUseAfterSend reports, for each send site of fn and each variable the site tracks, the
// first use the send reaches (ANL-220, ANL-230).
func checkUseAfterSend(pass *analysis.Pass, r *reporter, fn ast.Node) {
	sites := sendSites(pass, fn)
	if len(sites) == 0 {
		return
	}
	f := newUASFunc(pass, fn)
	for _, s := range sites {
		start, at := f.nodeOf(s.call)
		if start == nil {
			continue
		}
		// A deferred send happens when the function returns, so nothing in the function uses
		// its message after it.
		if d, ok := start.Nodes[at].(*ast.DeferStmt); ok && d.Call == s.call {
			continue
		}
		tracks, inits := f.tracked(s)
		for _, t := range tracks {
			if use := f.firstUse(s.call, start, at, t, inits); use != nil {
				line := pass.Fset.Position(s.call.Pos()).Line
				r.report(use.Pos(), idUseAfterSend, fmt.Sprintf(useAfterSendText, t.v.Name(), s.api, line))
			}
		}
	}
}

// uasFunc is one function holding send sites, with its control-flow graph.
type uasFunc struct {
	pass   *analysis.Pass
	fn     ast.Node
	recv   *types.Var // the method receiver, never tracked; nil for other functions
	placer *placer    // the placed expressions of fn's messages
	// lhs are the bare variables on the left of an assignment (any token) or of a range clause
	// assigning with =; notUses are the operands of == nil and != nil and the slice, array and
	// string arguments of len and cap (ANL-220).
	lhs, notUses map[*ast.Ident]bool
	// paths are the access paths of the identifiers of fn that are the root of a chain of field
	// selectors, index and slice expressions and dereferences; a bare identifier has none.
	paths map[*ast.Ident][]uasStep
	graph *cfg.CFG
}

func newUASFunc(pass *analysis.Pass, fn ast.Node) *uasFunc {
	f := &uasFunc{pass: pass, fn: fn, placer: newPlacer(pass, fn)}
	var body *ast.BlockStmt
	switch fn := fn.(type) {
	case *ast.FuncDecl:
		body = fn.Body
		if fn.Recv != nil {
			for _, field := range fn.Recv.List {
				for _, name := range field.Names {
					f.recv, _ = pass.TypesInfo.Defs[name].(*types.Var)
				}
			}
		}
	case *ast.FuncLit:
		body = fn.Body
	}
	f.scanIdents()
	// Only a call of panic ends a path besides return; telling other calls that never return
	// would take another analyzer.
	f.graph = cfg.New(body, func(call *ast.CallExpr) bool { return !uasIsBuiltin(pass.TypesInfo, call, "panic") })
	return f
}

// scanIdents fills lhs, notUses and paths.
func (f *uasFunc) scanIdents() {
	info := f.pass.TypesInfo
	f.lhs = make(map[*ast.Ident]bool)
	f.notUses = make(map[*ast.Ident]bool)
	f.paths = make(map[*ast.Ident][]uasStep)
	mark := func(set map[*ast.Ident]bool, e ast.Expr) {
		if id, ok := ast.Unparen(e).(*ast.Ident); ok {
			set[id] = true
		}
	}
	ast.Inspect(f.fn, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				mark(f.lhs, lhs)
			}
		case *ast.RangeStmt:
			if n.Tok == token.ASSIGN {
				mark(f.lhs, n.Key)
				mark(f.lhs, n.Value)
			}
		case *ast.BinaryExpr:
			if n.Op != token.EQL && n.Op != token.NEQ {
				break
			}
			if info.Types[n.Y].IsNil() {
				mark(f.notUses, n.X)
			}
			if info.Types[n.X].IsNil() {
				mark(f.notUses, n.Y)
			}
		case *ast.CallExpr:
			if (uasIsBuiltin(info, n, "len") || uasIsBuiltin(info, n, "cap")) && uasHasLength(info.TypeOf(n.Args[0])) {
				mark(f.notUses, n.Args[0])
			}
		case ast.Expr:
			// Inspect visits the outermost chain first; the identifier keeps its path.
			if root, path := uasPath(info, n); root != nil && len(path) > 0 {
				if _, ok := f.paths[root]; !ok {
					f.paths[root] = path
				}
			}
		}
		return true
	})
}

// uasStep is one step of an access path: a field (by its origin, so that the fields of generic
// instances compare equal) or an element. index is the constant index or key of an element, or
// empty when it is not constant or the step is a slice expression.
type uasStep struct {
	field   *types.Var
	element bool
	index   string
}

// uasPath returns the root identifier of e's chain of field selectors (embedded fields spelled
// out), index and slice expressions and dereferences, and the chain's steps from the root.
// Dereferences, explicit or implicit, are not steps: a path names memory reached from the root
// whatever pointers it follows. The root is nil when the chain starts at another expression.
func uasPath(info *types.Info, e ast.Expr) (*ast.Ident, []uasStep) {
	var rev []uasStep // the steps from e back to the root
	for {
		switch x := e.(type) {
		case *ast.Ident:
			path := make([]uasStep, 0, len(rev))
			for i := len(rev) - 1; i >= 0; i-- {
				path = append(path, rev[i])
			}
			return x, path
		case *ast.ParenExpr:
			e = x.X
		case *ast.StarExpr:
			if info.Types[x].IsType() {
				return nil, nil
			}
			e = x.X
		case *ast.SelectorExpr:
			sel := info.Selections[x]
			if sel == nil || sel.Kind() != types.FieldVal {
				return nil, nil
			}
			fields := uasFields(sel)
			for i := len(fields) - 1; i >= 0; i-- {
				rev = append(rev, uasStep{field: fields[i]})
			}
			e = x.X
		case *ast.IndexExpr:
			if !isElementIndex(info, x) {
				return nil, nil
			}
			step := uasStep{element: true}
			if tv := info.Types[x.Index]; tv.Value != nil {
				step.index = tv.Value.ExactString()
			}
			rev = append(rev, step)
			e = x.X
		case *ast.SliceExpr:
			rev = append(rev, uasStep{element: true})
			e = x.X
		default:
			return nil, nil
		}
	}
}

// uasFields returns the fields a field selection goes through, embedded ones included, by their
// origin.
func uasFields(sel *types.Selection) []*types.Var {
	var fields []*types.Var
	t := sel.Recv()
	for _, i := range sel.Index() {
		if p, ok := t.Underlying().(*types.Pointer); ok {
			t = p.Elem()
		}
		st, ok := t.Underlying().(*types.Struct)
		if !ok {
			break
		}
		fv := st.Field(i)
		fields = append(fields, fv.Origin())
		t = fv.Type()
	}
	return fields
}

// uasCompatible reports whether two access paths may reach the same memory: one is a prefix of
// the other, step by step the same field, or two elements whose constant indices, when both
// have one, are equal (ANL-220).
func uasCompatible(a, b []uasStep) bool {
	for i := range min(len(a), len(b)) {
		x, y := a[i], b[i]
		switch {
		case x.element != y.element:
			return false
		case !x.element && x.field != y.field:
			return false
		case x.element && x.index != "" && y.index != "" && x.index != y.index:
			return false
		}
	}
	return true
}

// uasIsBuiltin reports whether call calls the builtin function name with one argument.
func uasIsBuiltin(info *types.Info, call *ast.CallExpr, name string) bool {
	id, ok := ast.Unparen(call.Fun).(*ast.Ident)
	if !ok || len(call.Args) != 1 {
		return false
	}
	b, ok := info.Uses[id].(*types.Builtin)
	return ok && b.Name() == name
}

// uasHasLength reports whether t is a slice, an array or a string, whose length and capacity
// are part of the value itself.
func uasHasLength(t types.Type) bool {
	if t == nil {
		return false
	}
	switch t.Underlying().(type) {
	case *types.Slice, *types.Array:
		return true
	}
	return isStringType(t)
}

// uasTrack is a variable a send site tracks.
type uasTrack struct {
	v      *types.Var
	shared bool        // a leaf rooted at v is shared: assigning the bare v writes what was sent
	paths  [][]uasStep // the access paths of the leaves rooted at v
}

// tracked returns the variables site s tracks, in the order of their first leaf (ANL-220): the
// roots of the placed leaves that are declared in the function (parameters and results
// included, the receiver excluded), when the leaf's type contains references or the leaf is
// shared. It also returns the initializers of the local variables placed expanded for s.
func (f *uasFunc) tracked(s sendSite) ([]*uasTrack, []ast.Expr) {
	info := f.pass.TypesInfo
	var tracks []*uasTrack
	var inits []ast.Expr
	byVar := make(map[*types.Var]*uasTrack)
	for _, msg := range s.msgs {
		leaves := f.placer.placed(msg)
		for _, l := range leaves {
			if id, ok := l.expr.(*ast.Ident); ok {
				if v, ok := info.Uses[id].(*types.Var); ok && f.placer.expanded[v] {
					inits = append(inits, f.placer.inits[v])
				}
			}
		}
		for _, l := range leaves {
			if l.root == nil {
				continue
			}
			v, ok := info.Uses[l.root].(*types.Var)
			if !ok || v == f.recv || v.IsField() || !uasWithin(v.Pos(), f.fn) {
				continue
			}
			if !l.shared && !containsRefs(f.pass, info.TypeOf(l.expr)) {
				continue
			}
			t := byVar[v]
			if t == nil {
				t = &uasTrack{v: v}
				byVar[v] = t
				tracks = append(tracks, t)
			}
			_, path := uasPath(info, l.expr)
			t.shared = t.shared || l.shared
			t.paths = append(t.paths, path)
		}
	}
	return tracks, inits
}

// isUse reports whether id is a use of t's variable for the send call (ANL-220): an identifier
// referring to it outside the call and outside the initializers placed expanded for the send
// (evaluating one again reads its sources, not what was sent), not exempt, and on an access
// path compatible with a placed leaf. For a shared variable, the bare variable on the left of
// an assignment writes what was sent, so it is a use.
func (f *uasFunc) isUse(id *ast.Ident, call *ast.CallExpr, t *uasTrack, inits []ast.Expr) bool {
	if f.pass.TypesInfo.Uses[id] != t.v || uasWithin(id.Pos(), call) || f.notUses[id] {
		return false
	}
	if f.lhs[id] && !t.shared {
		return false
	}
	for _, init := range inits {
		if uasWithin(id.Pos(), init) {
			return false
		}
	}
	path := f.paths[id]
	for _, p := range t.paths {
		if uasCompatible(p, path) {
			return true
		}
	}
	return false
}

// firstUse returns the use of t's variable that comes first in the source among those a path of
// the function's control-flow graph leads to from the end of call, the node at of block start,
// without passing a kill (ANL-220, ANL-230), or nil. A use in a function literal counts where
// the literal is, since the literal may run later.
func (f *uasFunc) firstUse(call *ast.CallExpr, start *cfg.Block, at int, t *uasTrack, inits []ast.Expr) *ast.Ident {
	var first *ast.Ident
	// scan records the uses in node n positioned at or after from and reports whether n kills
	// the variable; the uses in a killing node come before the kill.
	scan := func(n ast.Node, from token.Pos) bool {
		ast.Inspect(n, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Pos() >= from && (first == nil || id.Pos() < first.Pos()) &&
				f.isUse(id, call, t, inits) {
				first = id
			}
			return true
		})
		return f.kills(n, t)
	}
	// walk scans the nodes of b from index i and reports whether the path goes on past b.
	walk := func(b *cfg.Block, i int, from token.Pos) bool {
		for ; i < len(b.Nodes); i++ {
			if scan(b.Nodes[i], from) {
				return false
			}
			from = token.NoPos
		}
		return true
	}
	visited := make(map[*cfg.Block]bool)
	var queue []*cfg.Block
	if walk(start, at, call.End()) {
		queue = append(queue, start.Succs...)
	}
	for len(queue) > 0 {
		b := queue[0]
		queue = queue[1:]
		if visited[b] {
			continue
		}
		visited[b] = true
		if f.killsOnEntry(b, t) || !walk(b, 0, token.NoPos) {
			continue
		}
		queue = append(queue, b.Succs...)
	}
	return first
}

// nodeOf returns the block holding call and the index of the innermost node holding it.
func (f *uasFunc) nodeOf(call *ast.CallExpr) (*cfg.Block, int) {
	var block *cfg.Block
	at := -1
	for _, b := range f.graph.Blocks {
		for i, n := range b.Nodes {
			if n.Pos() > call.Pos() || call.End() > n.End() {
				continue
			}
			if block == nil || n.End()-n.Pos() < block.Nodes[at].End()-block.Nodes[at].Pos() {
				block, at = b, i
			}
		}
	}
	return block, at
}

// kills reports whether node n of the graph kills t's variable (ANL-230): a declaration of it
// (by := or a var specification), or an assignment of the bare variable with = or := when it is
// not shared; for a shared variable such an assignment writes what was sent and is a use.
func (f *uasFunc) kills(n ast.Node, t *uasTrack) bool {
	info := f.pass.TypesInfo
	switch n := n.(type) {
	case *ast.AssignStmt:
		if n.Tok != token.ASSIGN && n.Tok != token.DEFINE {
			return false
		}
		for _, lhs := range n.Lhs {
			id, ok := ast.Unparen(lhs).(*ast.Ident)
			if !ok {
				continue
			}
			if info.Defs[id] == t.v || !t.shared && info.Uses[id] == t.v {
				return true
			}
		}
	case *ast.ValueSpec:
		for _, id := range n.Names {
			if info.Defs[id] == t.v {
				return true
			}
		}
	}
	return false
}

// killsOnEntry reports whether entering b kills t's variable through a header (ANL-230): every
// iteration of a range loop declares or assigns its key and value at the start of the body
// (an assignment kills only a variable that is not shared), every iteration of a for loop has
// its own copy of the variables its init statement declares, and every clause of a type switch
// declares its own symbolic variable.
func (f *uasFunc) killsOnEntry(b *cfg.Block, t *uasTrack) bool {
	info := f.pass.TypesInfo
	switch s := b.Stmt.(type) {
	case *ast.RangeStmt:
		if b.Kind != cfg.KindRangeBody || s.Tok == token.ASSIGN && t.shared {
			return false
		}
		for _, x := range []ast.Expr{s.Key, s.Value} {
			if id, ok := ast.Unparen(x).(*ast.Ident); ok && info.ObjectOf(id) == t.v {
				return true
			}
		}
	case *ast.ForStmt:
		init, ok := s.Init.(*ast.AssignStmt)
		if !ok || init.Tok != token.DEFINE {
			return false
		}
		switch b.Kind {
		case cfg.KindForBody, cfg.KindForLoop, cfg.KindForPost:
			for _, lhs := range init.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && info.Defs[id] == t.v {
					return true
				}
			}
		}
	case *ast.CaseClause:
		return b.Kind == cfg.KindSwitchCaseBody && info.Implicits[s] == t.v
	}
	return false
}

// uasWithin reports whether pos is inside n.
func uasWithin(pos token.Pos, n ast.Node) bool {
	return n.Pos() <= pos && pos < n.End()
}
