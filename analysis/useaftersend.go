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
	f := newSendFunc(pass, fn)
	for _, s := range sites {
		for _, v := range f.tracked(s) {
			if use := f.firstUse(s.call, v); use != nil {
				line := pass.Fset.Position(s.call.Pos()).Line
				r.report(use.Pos(), idUseAfterSend, fmt.Sprintf(useAfterSendText, v.Name(), s.api, line))
			}
		}
	}
}

// sendFunc is one function holding send sites, with its control-flow graph.
type sendFunc struct {
	pass   *analysis.Pass
	fn     ast.Node
	recv   *types.Var // the method receiver, never tracked; nil for other functions
	placer *placer    // the placed expressions of fn's messages
	exempt map[*ast.Ident]bool
	graph  *cfg.CFG
}

func newSendFunc(pass *analysis.Pass, fn ast.Node) *sendFunc {
	f := &sendFunc{
		pass:   pass,
		fn:     fn,
		placer: newPlacer(pass, fn),
		exempt: exemptIdents(pass.TypesInfo, fn),
	}
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
	// Only a call of panic ends a path besides return; telling other calls that never return
	// would take another analyzer.
	f.graph = cfg.New(body, func(call *ast.CallExpr) bool { return !isBuiltin(pass.TypesInfo, call, "panic") })
	return f
}

// exemptIdents returns the identifiers of fn that ANL-220 says are not uses: the bare variable on
// the left of an assignment (op= and a range clause assigning with = included), an operand of
// == nil or != nil, and the argument of len or cap when it is a slice, array or string.
func exemptIdents(info *types.Info, fn ast.Node) map[*ast.Ident]bool {
	exempt := make(map[*ast.Ident]bool)
	bare := func(e ast.Expr) {
		if id, ok := ast.Unparen(e).(*ast.Ident); ok {
			exempt[id] = true
		}
	}
	ast.Inspect(fn, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				bare(lhs)
			}
		case *ast.RangeStmt:
			if n.Tok == token.ASSIGN {
				bare(n.Key)
				bare(n.Value)
			}
		case *ast.BinaryExpr:
			if n.Op != token.EQL && n.Op != token.NEQ {
				break
			}
			if info.Types[n.Y].IsNil() {
				bare(n.X)
			}
			if info.Types[n.X].IsNil() {
				bare(n.Y)
			}
		case *ast.CallExpr:
			if (isBuiltin(info, n, "len") || isBuiltin(info, n, "cap")) && hasLength(info.TypeOf(n.Args[0])) {
				bare(n.Args[0])
			}
		}
		return true
	})
	return exempt
}

// isBuiltin reports whether call calls the builtin function name with one argument.
func isBuiltin(info *types.Info, call *ast.CallExpr, name string) bool {
	id, ok := ast.Unparen(call.Fun).(*ast.Ident)
	if !ok || len(call.Args) != 1 {
		return false
	}
	b, ok := info.Uses[id].(*types.Builtin)
	return ok && b.Name() == name
}

// hasLength reports whether t is a slice, an array or a string, whose length and capacity are
// part of the value itself.
func hasLength(t types.Type) bool {
	if t == nil {
		return false
	}
	switch u := t.Underlying().(type) {
	case *types.Slice, *types.Array:
		return true
	case *types.Basic:
		return u.Info()&types.IsString != 0
	}
	return false
}

// tracked returns the variables site s tracks, in the order of their first leaf (ANL-220): the
// roots of the placed leaves that are declared in the function (parameters and results
// included, the receiver excluded), when the leaf's type contains references or the leaf is
// shared.
func (f *sendFunc) tracked(s sendSite) []*types.Var {
	var vars []*types.Var
	seen := make(map[*types.Var]bool)
	for _, msg := range s.msgs {
		for _, l := range f.placer.placed(msg) {
			if l.root == nil {
				continue
			}
			v, ok := f.pass.TypesInfo.Uses[l.root].(*types.Var)
			if !ok || seen[v] || v == f.recv || v.IsField() || !within(v.Pos(), f.fn) {
				continue
			}
			if !l.shared && !containsRefs(f.pass, f.pass.TypesInfo.TypeOf(l.expr)) {
				continue
			}
			seen[v] = true
			vars = append(vars, v)
		}
	}
	return vars
}

// firstUse returns the use of v that comes first in the source among those a path of the
// function's control-flow graph leads to from the end of call without passing a kill of v
// (ANL-220, ANL-230), or nil. Identifiers inside call are not uses. A use in a function literal
// counts where the literal is, since the literal may run later. A deferred send happens when
// the function returns, so nothing in the function uses its message after it.
func (f *sendFunc) firstUse(call *ast.CallExpr, v *types.Var) *ast.Ident {
	start, at := f.nodeOf(call)
	if start == nil {
		return nil
	}
	if d, ok := start.Nodes[at].(*ast.DeferStmt); ok && d.Call == call {
		return nil
	}
	var first *ast.Ident
	// scan records the uses of v in node n positioned at or after from and reports whether n
	// kills v; the uses in a killing node come before the kill.
	scan := func(n ast.Node, from token.Pos) bool {
		ast.Inspect(n, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok || id.Pos() < from || within(id.Pos(), call) || f.exempt[id] {
				return true
			}
			if f.pass.TypesInfo.Uses[id] == v && (first == nil || id.Pos() < first.Pos()) {
				first = id
			}
			return true
		})
		return f.kills(n, v)
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
		if f.killsOnEntry(b, v) || !walk(b, 0, token.NoPos) {
			continue
		}
		queue = append(queue, b.Succs...)
	}
	return first
}

// nodeOf returns the block holding call and the index of the innermost node holding it.
func (f *sendFunc) nodeOf(call *ast.CallExpr) (*cfg.Block, int) {
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

// kills reports whether node n of the graph assigns the bare v (ANL-230): an assignment with =
// or := (declaring or redeclaring v) or a var specification declaring v.
func (f *sendFunc) kills(n ast.Node, v *types.Var) bool {
	info := f.pass.TypesInfo
	switch n := n.(type) {
	case *ast.AssignStmt:
		if n.Tok != token.ASSIGN && n.Tok != token.DEFINE {
			return false
		}
		for _, lhs := range n.Lhs {
			if id, ok := ast.Unparen(lhs).(*ast.Ident); ok && info.ObjectOf(id) == v {
				return true
			}
		}
	case *ast.ValueSpec:
		for _, id := range n.Names {
			if info.Defs[id] == v {
				return true
			}
		}
	}
	return false
}

// killsOnEntry reports whether entering b kills v through a loop header (ANL-230): every
// iteration of a range loop assigns its key and value at the start of the body, and every
// iteration of a for loop has its own copy of the variables its init statement declares.
func (f *sendFunc) killsOnEntry(b *cfg.Block, v *types.Var) bool {
	info := f.pass.TypesInfo
	switch s := b.Stmt.(type) {
	case *ast.RangeStmt:
		if b.Kind != cfg.KindRangeBody {
			return false
		}
		for _, x := range []ast.Expr{s.Key, s.Value} {
			if id, ok := ast.Unparen(x).(*ast.Ident); ok && info.ObjectOf(id) == v {
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
				if id, ok := lhs.(*ast.Ident); ok && info.Defs[id] == v {
					return true
				}
			}
		}
	}
	return false
}

// within reports whether pos is inside n.
func within(pos token.Pos, n ast.Node) bool {
	return n.Pos() <= pos && pos < n.End()
}
