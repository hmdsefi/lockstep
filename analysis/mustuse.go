// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// MustUse reports replies that are neither awaited nor piped and ignored Send and Spawn errors
// (LS220).
var MustUse = &analysis.Analyzer{
	Name:     "mustuse",
	Doc:      "report ignored replies and errors (LS220)",
	Requires: []*analysis.Analyzer{indexAnalyzer, inspect.Analyzer},
	Run:      runMustUse,
}

// idMustUse is the check ID of MustUse.
const idMustUse = "LS220"

// mustUseReplyText is the message of an LS220 finding for a reply, without its ID.
const mustUseReplyText = "the reply of lockstep.Ask is never awaited or piped; " +
	"call Await outside actors or lockstep.Pipe inside them"

func runMustUse(pass *analysis.Pass) (any, error) {
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	insp, ok := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	if !ok {
		return nil, nil
	}
	m := &mustUse{pass: pass, info: pass.TypesInfo, r: newReporter(pass, idx)}
	insp.WithStack([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node, push bool, stack []ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && push {
			m.call(call, stack)
		}
		return true
	})
	m.r.flush()
	return nil, nil
}

// mustUse is the state of one MustUse run.
type mustUse struct {
	pass *analysis.Pass
	info *types.Info
	r    *reporter
}

// call checks one call: the reply of an Ask (ANL-290), the error of a Send or Spawn (ANL-300).
// stack holds the nodes from the file down to call.
func (m *mustUse) call(call *ast.CallExpr, stack []ast.Node) {
	switch callee := m.callee(call); {
	case isAPI(callee, "Ask"):
		if !m.replyUsed(stack) {
			m.r.report(call.Pos(), idMustUse, mustUseReplyText)
		}
	case isAPI(callee, "Send"):
		if errorDiscarded(stack) {
			m.r.report(call.Pos(), idMustUse, "the error returned by lockstep.Send is ignored")
		}
	case isAPI(callee, "Spawn"):
		if errorDiscarded(stack) {
			m.r.report(call.Pos(), idMustUse, "the error returned by lockstep.Spawn is ignored")
		}
	}
}

// callee returns the function a call names directly, with an explicit instantiation looked
// through, or nil.
func (m *mustUse) callee(call *ast.CallExpr) types.Object {
	fun := ast.Unparen(call.Fun)
	switch f := fun.(type) {
	case *ast.IndexExpr:
		fun = f.X
	case *ast.IndexListExpr:
		fun = f.X
	}
	switch f := ast.Unparen(fun).(type) {
	case *ast.Ident:
		return m.info.Uses[f]
	case *ast.SelectorExpr:
		return m.info.Uses[f.Sel]
	}
	return nil
}

// valueContext returns the node that consumes the value of the expression at the end of stack and
// the expression whose value it consumes: parentheses and the address operator are looked through,
// since the address of a reply holds the reply.
func valueContext(stack []ast.Node) (parent, child ast.Node) {
	child = stack[len(stack)-1]
	for i := len(stack) - 2; i >= 0; i-- {
		switch p := stack[i].(type) {
		case *ast.ParenExpr:
			child = p
			continue
		case *ast.UnaryExpr:
			if p.Op == token.AND {
				child = p
				continue
			}
		}
		return stack[i], child
	}
	return nil, child
}

// indexIn returns the index of e in list, or -1.
func indexIn(list []ast.Expr, e ast.Node) int {
	for i, x := range list {
		if x == e {
			return i
		}
	}
	return -1
}

// isBlank reports whether e is the blank identifier.
func isBlank(e ast.Expr) bool {
	id, ok := ast.Unparen(e).(*ast.Ident)
	return ok && id.Name == "_"
}

// replyUsed reports whether the reply of the Ask call at the end of stack is used (ANL-290).
func (m *mustUse) replyUsed(stack []ast.Node) bool {
	used, v := m.consumed(stack)
	if used || v == nil {
		return used
	}
	// The reply went into a local variable: follow it through the top-level declaration that
	// holds the call, whose nested literals can reach the variable too.
	return m.variableUsed(v, stack[1], map[*types.Var]bool{})
}

// consumed classifies the use of the expression at the end of stack: used is true when it is
// used; otherwise v is the local variable it is assigned to, if any.
func (m *mustUse) consumed(stack []ast.Node) (used bool, v *types.Var) {
	parent, child := valueContext(stack)
	switch p := parent.(type) {
	case *ast.SelectorExpr:
		return p.X == child && isAPI(m.info.Uses[p.Sel], "Reply.Await"), nil
	case *ast.CallExpr:
		return indexIn(p.Args, child) >= 0, nil
	case *ast.ReturnStmt:
		return true, nil
	case *ast.SendStmt:
		return p.Value == child, nil
	case *ast.CompositeLit, *ast.KeyValueExpr:
		return true, nil
	case *ast.AssignStmt:
		if i := indexIn(p.Rhs, child); i >= 0 && len(p.Lhs) == len(p.Rhs) {
			return m.stored(p.Lhs[i])
		}
	case *ast.ValueSpec:
		if i := indexIn(p.Values, child); i >= 0 && len(p.Names) == len(p.Values) {
			return m.stored(p.Names[i])
		}
	}
	return false, nil
}

// stored classifies an assignment target: a field, element, map entry, pointer target or package
// variable holds the value (used); a local variable is followed (v); the blank identifier drops it.
func (m *mustUse) stored(lhs ast.Expr) (used bool, v *types.Var) {
	switch l := ast.Unparen(lhs).(type) {
	case *ast.Ident:
		if l.Name == "_" {
			return false, nil
		}
		obj := m.info.Defs[l]
		if obj == nil {
			obj = m.info.Uses[l]
		}
		local, ok := obj.(*types.Var)
		if !ok {
			return false, nil
		}
		if local.Parent() == local.Pkg().Scope() {
			return true, nil
		}
		return false, local
	case *ast.SelectorExpr, *ast.IndexExpr, *ast.IndexListExpr, *ast.StarExpr:
		return true, nil
	}
	return false, nil
}

// variableUsed reports whether the local variable v, which holds a reply, is used anywhere in
// root: awaited, passed, returned, sent, stored or captured by a function literal, or copied into a
// variable that is (ANL-290). It looks at the whole declaring function including its literals.
func (m *mustUse) variableUsed(v *types.Var, root ast.Node, seen map[*types.Var]bool) bool {
	seen[v] = true
	var stack []ast.Node
	var copies []*types.Var
	used := false
	ast.Inspect(root, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		if used {
			return false
		}
		switch n := n.(type) {
		case *ast.FuncType:
			// A named result is returned when the function ends.
			if n.Results != nil && m.declares(n.Results, v) {
				used = true
			}
		case *ast.Ident:
			if m.info.Uses[n] != v {
				return true
			}
			if capturedAt(stack, v) {
				used = true
				return false
			}
			u, w := m.consumed(stack)
			used = u
			if w != nil && !seen[w] {
				copies = append(copies, w)
			}
		}
		return true
	})
	if used {
		return true
	}
	for _, w := range copies {
		if !seen[w] && m.variableUsed(w, root, seen) {
			return true
		}
	}
	return false
}

// declares reports whether a field list declares v.
func (m *mustUse) declares(fields *ast.FieldList, v *types.Var) bool {
	for _, f := range fields.List {
		for _, name := range f.Names {
			if m.info.Defs[name] == v {
				return true
			}
		}
	}
	return false
}

// capturedAt reports whether the identifier at the end of stack names v inside a function literal
// that does not declare v: the literal captures v, by reading or by assigning it.
func capturedAt(stack []ast.Node, v *types.Var) bool {
	for _, n := range stack {
		if lit, ok := n.(*ast.FuncLit); ok && (v.Pos() < lit.Pos() || v.Pos() >= lit.End()) {
			return true
		}
	}
	return false
}

// errorDiscarded reports whether the error result of the Send or Spawn call at the end of stack is
// thrown away: the call is a statement, is started with go or defer, or its error goes to the
// blank identifier (ANL-300). An error assigned to a named variable is not checked further.
func errorDiscarded(stack []ast.Node) bool {
	parent, child := valueContext(stack)
	var target ast.Expr
	switch p := parent.(type) {
	case *ast.ExprStmt, *ast.GoStmt, *ast.DeferStmt:
		return true
	case *ast.AssignStmt:
		target = errorTarget(p.Lhs, p.Rhs, child)
	case *ast.ValueSpec:
		names := make([]ast.Expr, len(p.Names))
		for i, name := range p.Names {
			names[i] = name
		}
		target = errorTarget(names, p.Values, child)
	}
	return target != nil && isBlank(target)
}

// errorTarget returns the assignment target that receives the error result of the call at rhs
// position child: the target at the same index, or the last one when a single call returns all
// the values.
func errorTarget(lhs, rhs []ast.Expr, child ast.Node) ast.Expr {
	i := indexIn(rhs, child)
	switch {
	case i < 0:
		return nil
	case len(lhs) == len(rhs):
		return lhs[i]
	case len(rhs) == 1 && len(lhs) > 1:
		return lhs[len(lhs)-1]
	}
	return nil
}
