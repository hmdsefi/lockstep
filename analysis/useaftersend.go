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

// checkUseAfterSend reports the first use after each send site of fn of every variable the site
// tracks (ANL-220, ANL-230).
func checkUseAfterSend(pass *analysis.Pass, r *reporter, fn ast.Node) {
	sites := sendSites(pass, fn)
	if len(sites) == 0 {
		return
	}
	fu := newSendFunc(pass, fn)
	for _, s := range sites {
		path := fu.pathTo(s.call)
		for _, v := range fu.tracked(s) {
			if use := fu.firstUse(s.call, path, v); use != nil {
				line := pass.Fset.Position(s.call.Pos()).Line
				r.report(use.Pos(), idUseAfterSend, fmt.Sprintf(useAfterSendText, v.Name(), s.api, line))
			}
		}
	}
}

// sendFunc is one function holding send sites, with the identifiers in it that use variables.
type sendFunc struct {
	pass *analysis.Pass
	fn   ast.Node
	recv *types.Var // the method receiver, never tracked; nil for other functions
	// uses are the identifiers of fn (function literals included) that use a variable, in source
	// order. The identifiers ANL-220 exempts are left out.
	uses []*ast.Ident
}

func newSendFunc(pass *analysis.Pass, fn ast.Node) *sendFunc {
	f := &sendFunc{pass: pass, fn: fn}
	if fd, ok := fn.(*ast.FuncDecl); ok && fd.Recv != nil {
		for _, field := range fd.Recv.List {
			for _, name := range field.Names {
				f.recv, _ = pass.TypesInfo.Defs[name].(*types.Var)
			}
		}
	}
	exempt := exemptIdents(pass.TypesInfo, fn)
	ast.Inspect(fn, func(n ast.Node) bool {
		id, ok := n.(*ast.Ident)
		if !ok || exempt[id] {
			return true
		}
		if _, ok := pass.TypesInfo.Uses[id].(*types.Var); ok {
			f.uses = append(f.uses, id)
		}
		return true
	})
	return f
}

// exemptIdents returns the identifiers of fn that ANL-220 says are not uses: the bare variable on
// the left of an assignment (a range clause assigning with = included), an operand of == nil or
// != nil, and the argument of len or cap when it is a slice, array or string.
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
			if isLenOrCap(info, n) && hasLength(info.TypeOf(n.Args[0])) {
				bare(n.Args[0])
			}
		}
		return true
	})
	return exempt
}

// isLenOrCap reports whether call calls the builtin len or cap with one argument.
func isLenOrCap(info *types.Info, call *ast.CallExpr) bool {
	id, ok := ast.Unparen(call.Fun).(*ast.Ident)
	if !ok || len(call.Args) != 1 {
		return false
	}
	b, ok := info.Uses[id].(*types.Builtin)
	return ok && (b.Name() == "len" || b.Name() == "cap")
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

// declaredIn reports whether v is declared in the function: a parameter, a result or a local
// variable, the receiver excluded.
func (f *sendFunc) declaredIn(v *types.Var) bool {
	return v != f.recv && !v.IsField() && within(v.Pos(), f.fn)
}

// tracked returns the variables site s tracks, in the order of their first leaf (ANL-220): the
// roots of the placed leaves that are declared in the function, when the leaf's type contains
// references or the leaf is shared.
func (f *sendFunc) tracked(s sendSite) []*types.Var {
	var vars []*types.Var
	seen := make(map[*types.Var]bool)
	for _, msg := range s.msgs {
		for _, l := range placed(f.pass, f.fn, msg) {
			if l.root == nil {
				continue
			}
			v, ok := f.pass.TypesInfo.Uses[l.root].(*types.Var)
			if !ok || seen[v] || !f.declaredIn(v) {
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

// sendPath is where a send call sits in its function.
type sendPath struct {
	// blocks are the statement lists holding the call, innermost last: the block holding the
	// send and the blocks enclosing it.
	blocks [][]ast.Stmt
	// loop is the innermost for or range statement enclosing the call, or nil.
	loop ast.Stmt
}

// pathTo returns the blocks and the innermost loop enclosing call in the function, without
// entering function literals other than the function itself.
func (f *sendFunc) pathTo(call *ast.CallExpr) sendPath {
	var p sendPath
	var stack []ast.Node
	found := false
	ast.Inspect(f.fn, func(n ast.Node) bool {
		if found {
			return false
		}
		if n == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		if _, ok := n.(*ast.FuncLit); ok && n != f.fn {
			return false
		}
		if n == call {
			found = true
			for _, a := range stack {
				switch a := a.(type) {
				case *ast.BlockStmt:
					p.blocks = append(p.blocks, a.List)
				case *ast.CaseClause:
					p.blocks = append(p.blocks, a.Body)
				case *ast.CommClause:
					p.blocks = append(p.blocks, a.Body)
				case *ast.ForStmt:
					p.loop = a
				case *ast.RangeStmt:
					p.loop = a
				}
			}
			return false
		}
		if !within(call.Pos(), n) {
			return false
		}
		stack = append(stack, n)
		return true
	})
	return p
}

// firstUse returns the first use of v that the send call gives (ANL-220), with the kills of
// ANL-230 applied, or nil.
func (f *sendFunc) firstUse(call *ast.CallExpr, p sendPath, v *types.Var) *ast.Ident {
	// The kills: assignments of the bare v that are direct statements of the block holding the
	// send or of an enclosing block. The first one after the call ends tracking for the uses
	// after it; any one inside the innermost loop ends the loop-carried uses but those of its own
	// right-hand side.
	var after *ast.AssignStmt
	var inLoop []*ast.AssignStmt
	for _, list := range p.blocks {
		for _, stmt := range list {
			as, ok := stmt.(*ast.AssignStmt)
			if !ok || !f.assigns(as, v) {
				continue
			}
			if as.Pos() >= call.End() && (after == nil || as.Pos() < after.Pos()) {
				after = as
			}
			if p.loop != nil && within(as.Pos(), p.loop) {
				inLoop = append(inLoop, as)
			}
		}
	}
	carried := p.loop != nil && !within(v.Pos(), p.loop)
	for _, id := range f.uses {
		if f.pass.TypesInfo.Uses[id] != v {
			continue
		}
		switch pos := id.Pos(); {
		case pos >= call.End():
			if after == nil || pos < after.End() {
				return id
			}
		case pos < call.Pos() && carried && within(pos, p.loop):
			if loopCarriedLive(pos, inLoop) {
				return id
			}
		}
	}
	return nil
}

// loopCarriedLive reports whether a loop-carried use at pos survives the kills inside the loop:
// there is none, or the only one is the assignment whose right-hand side holds the use.
func loopCarriedLive(pos token.Pos, kills []*ast.AssignStmt) bool {
	for _, k := range kills {
		if !inRHS(pos, k) {
			return false
		}
	}
	return true
}

// inRHS reports whether pos is in the right-hand side of as.
func inRHS(pos token.Pos, as *ast.AssignStmt) bool {
	return len(as.Rhs) > 0 && as.Rhs[0].Pos() <= pos && pos < as.Rhs[len(as.Rhs)-1].End()
}

// assigns reports whether as assigns the bare v with = or with := redeclaring it.
func (f *sendFunc) assigns(as *ast.AssignStmt, v *types.Var) bool {
	if as.Tok != token.ASSIGN && as.Tok != token.DEFINE {
		return false
	}
	for _, lhs := range as.Lhs {
		if id, ok := ast.Unparen(lhs).(*ast.Ident); ok && f.pass.TypesInfo.Uses[id] == v {
			return true
		}
	}
	return false
}

// within reports whether pos is inside n.
func within(pos token.Pos, n ast.Node) bool {
	return n.Pos() <= pos && pos < n.End()
}
