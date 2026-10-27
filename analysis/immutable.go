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

// Immutable verifies types marked //lockstep:immutable (LS201).
var Immutable = &analysis.Analyzer{
	Name:     "immutable",
	Doc:      "verify types marked //lockstep:immutable (LS201)",
	Requires: []*analysis.Analyzer{indexAnalyzer, inspect.Analyzer},
	Run:      runImmutable,
}

// idImmutable is the check ID of Immutable.
const idImmutable = "LS201"

// The operations of ANL-260 that write to a field of an immutable type, as messages name them.
const (
	opElemAssign = "element assignment"
	opIncrement  = "increment"
	opElemAddr   = "taking the address of an element"
	opPtrAssign  = "assignment through a pointer"
)

// immutableCheck is the state of one run of Immutable.
type immutableCheck struct {
	pass *analysis.Pass
	idx  *directiveIndex
	r    *reporter
	qual types.Qualifier
}

func runImmutable(pass *analysis.Pass) (any, error) {
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	insp, ok := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	if !ok {
		return nil, nil
	}
	c := &immutableCheck{
		pass: pass,
		idx:  idx,
		r:    newReporter(pass, idx),
		qual: func(p *types.Package) string {
			if p == pass.Pkg {
				return ""
			}
			return p.Name()
		},
	}
	nodes := []ast.Node{
		(*ast.TypeSpec)(nil), (*ast.AssignStmt)(nil), (*ast.RangeStmt)(nil), (*ast.IncDecStmt)(nil),
		(*ast.UnaryExpr)(nil), (*ast.CallExpr)(nil),
	}
	insp.Preorder(nodes, func(n ast.Node) {
		switch n := n.(type) {
		case *ast.TypeSpec:
			c.checkFields(n)
		case *ast.AssignStmt:
			if n.Tok != token.DEFINE {
				for _, lhs := range n.Lhs {
					c.checkAssign(lhs, opElemAssign)
				}
			}
		case *ast.RangeStmt:
			if n.Tok == token.ASSIGN {
				for _, e := range []ast.Expr{n.Key, n.Value} {
					if e != nil {
						c.checkAssign(e, opElemAssign)
					}
				}
			}
		case *ast.IncDecStmt:
			c.checkAssign(n.X, opIncrement)
		case *ast.UnaryExpr:
			if n.Op == token.AND {
				if t, ok := c.writeTarget(n.X); ok && t.elem {
					c.report(n.Pos(), opElemAddr, t)
				}
			}
		case *ast.CallExpr:
			c.checkCall(n)
		}
	})
	c.r.flush()
	return nil, nil
}

// checkFields reports the first component of each field of a type marked immutable here that is
// not immutable, or of the type itself when it is not a struct (ANL-250).
func (c *immutableCheck) checkFields(spec *ast.TypeSpec) {
	tn, ok := c.pass.TypesInfo.Defs[spec.Name].(*types.TypeName)
	if !ok || !c.idx.hasMarker(tn, markImmutable) {
		return
	}
	name := types.TypeString(tn.Type(), c.qual)
	st, isStruct := tn.Type().Underlying().(*types.Struct)
	if _, literal := ast.Unparen(spec.Type).(*ast.StructType); isStruct && literal {
		for f := range st.Fields() {
			if desc := c.firstMutable(f.Type()); desc != "" {
				c.r.report(f.Pos(), idImmutable, "field "+f.Name()+" of immutable type "+name+" contains "+desc+
					", which is not immutable")
			}
		}
		return
	}
	if desc := c.firstMutable(tn.Type().Underlying()); desc != "" {
		c.r.report(spec.Name.Pos(), idImmutable, "immutable type "+name+" contains "+desc+", which is not immutable")
	}
}

// firstMutable walks t depth first and describes its first component that is not immutable, as
// ANL-250 spells it, or returns "" when every component is immutable.
func (c *immutableCheck) firstMutable(t types.Type) string {
	w := immutableWalk{c: c, seen: make(map[*types.Named]bool)}
	return w.component(t, t)
}

// immutableWalk is the state of one firstMutable call.
type immutableWalk struct {
	c    *immutableCheck
	seen map[*types.Named]bool // unmarked named types already visited, so recursive types terminate
}

// component describes the first component of t that is not immutable. shown is the type the
// description names: t itself, or the named type whose underlying type t is.
func (w *immutableWalk) component(t, shown types.Type) string {
	str := func(kind string) string { return kind + " " + types.TypeString(shown, w.c.qual) }
	switch u := types.Unalias(t).(type) {
	case *types.Basic:
		if u.Kind() == types.UnsafePointer {
			return "unsafe.Pointer"
		}
	case *types.TypeParam:
		return str("type parameter")
	case *types.Pointer:
		if !w.c.idx.typeHasMarker(u.Elem(), markImmutable) {
			return str("pointer type")
		}
	case *types.Chan:
		return str("channel type")
	case *types.Signature:
		return str("function type")
	case *types.Interface:
		return str("interface type")
	case *types.Slice:
		return w.component(u.Elem(), u.Elem())
	case *types.Array:
		return w.component(u.Elem(), u.Elem())
	case *types.Map:
		if desc := w.component(u.Key(), u.Key()); desc != "" {
			return desc
		}
		return w.component(u.Elem(), u.Elem())
	case *types.Struct:
		for f := range u.Fields() {
			if desc := w.component(f.Type(), f.Type()); desc != "" {
				return desc
			}
		}
	case *types.Named:
		if w.seen[u] || w.allowed(u) {
			return ""
		}
		w.seen[u] = true
		return w.component(u.Underlying(), shown)
	}
	return ""
}

// allowed reports whether the named type t is immutable by itself: error, time.Time,
// lockstep.PID, lockstep.Address, lockstep.ReplyTo and the types marked immutable (ANL-250).
func (w *immutableWalk) allowed(t *types.Named) bool {
	obj := t.Obj()
	switch {
	case obj.Pkg() == nil:
		return obj.Name() == "error"
	case obj.Pkg().Path() == "time" && obj.Name() == "Time":
		return true
	}
	return isAPI(obj, "PID") || isAPI(obj, "Address") || isAPI(obj, "ReplyTo") ||
		w.c.idx.hasMarker(obj, markImmutable)
}

// fieldWrite is a write to field field of the immutable type named.
type fieldWrite struct {
	field *types.Var
	named *types.Named
	elem  bool // the write is to an element of a slice or map field, not to the field itself
}

// writeTarget finds the field of an immutable type that a write to the location e changes
// (ANL-260): an element of a slice or map field x.f[k], with x of type T or *T, or any field p.f
// with p of type *T. Writes to a part of such a location (a field of the element, an element of an
// array field) count as writes to it. Field writes on a value of type T change a local copy and
// are not returned.
func (c *immutableCheck) writeTarget(e ast.Expr) (fieldWrite, bool) {
	for {
		switch x := ast.Unparen(e).(type) {
		case *ast.IndexExpr:
			if f, named, _, ok := c.immutableField(x.X); ok && isSliceOrMap(f.Type()) {
				return fieldWrite{field: f, named: named, elem: true}, true
			}
			t := c.pass.TypesInfo.TypeOf(x.X)
			if t == nil {
				return fieldWrite{}, false
			}
			if _, isArray := t.Underlying().(*types.Array); !isArray {
				return fieldWrite{}, false
			}
			e = x.X
		case *ast.SelectorExpr:
			if f, named, ptr, ok := c.immutableField(x); ok && ptr {
				return fieldWrite{field: f, named: named}, true
			}
			s := c.pass.TypesInfo.Selections[x]
			if s == nil || s.Kind() != types.FieldVal || s.Indirect() {
				return fieldWrite{}, false
			}
			e = x.X
		default:
			return fieldWrite{}, false
		}
	}
}

// immutableField reports whether e is a field selection x.f with x of type T or *T for a type T
// marked immutable, here or in an imported package. It returns the field, T and whether x is a
// pointer.
func (c *immutableCheck) immutableField(e ast.Expr) (field *types.Var, named *types.Named, ptr, ok bool) {
	sel, isSel := ast.Unparen(e).(*ast.SelectorExpr)
	if !isSel {
		return nil, nil, false, false
	}
	s := c.pass.TypesInfo.Selections[sel]
	if s == nil || s.Kind() != types.FieldVal {
		return nil, nil, false, false
	}
	recv := s.Recv()
	if p, isPtr := recv.Underlying().(*types.Pointer); isPtr {
		recv, ptr = p.Elem(), true
	} else if _, deref := ast.Unparen(sel.X).(*ast.StarExpr); deref {
		ptr = true
	}
	named, isNamed := types.Unalias(recv).(*types.Named)
	if !isNamed || !c.idx.hasMarker(named.Obj(), markImmutable) {
		return nil, nil, false, false
	}
	field, isVar := s.Obj().(*types.Var)
	return field, named, ptr, isVar
}

// isSliceOrMap reports whether t's underlying type is a slice or a map.
func isSliceOrMap(t types.Type) bool {
	switch t.Underlying().(type) {
	case *types.Slice, *types.Map:
		return true
	}
	return false
}

// checkAssign reports an assignment or increment of the location lhs; op names a write to an
// element.
func (c *immutableCheck) checkAssign(lhs ast.Expr, op string) {
	t, ok := c.writeTarget(lhs)
	if !ok {
		return
	}
	if !t.elem {
		op = opPtrAssign
	}
	c.report(lhs.Pos(), op, t)
}

// checkCall reports a builtin or an in-place mutator whose first argument is a slice or map field
// of an immutable type (ANL-260). Slicing the field still writes into its memory.
func (c *immutableCheck) checkCall(call *ast.CallExpr) {
	if len(call.Args) == 0 {
		return
	}
	op, ok := builtinWrite(c.pass.TypesInfo, call)
	if !ok {
		op, ok = inPlaceMutator(c.pass.TypesInfo, call)
	}
	if !ok {
		return
	}
	arg := ast.Unparen(call.Args[0])
	for {
		s, isSlice := arg.(*ast.SliceExpr)
		if !isSlice {
			break
		}
		arg = ast.Unparen(s.X)
	}
	if f, named, _, ok := c.immutableField(arg); ok && isSliceOrMap(f.Type()) {
		c.report(call.Pos(), op, fieldWrite{field: f, named: named, elem: true})
	}
}

// report adds the LS201 finding of the write t at pos.
func (c *immutableCheck) report(pos token.Pos, op string, t fieldWrite) {
	c.r.report(pos, idImmutable, op+" writes to field "+t.field.Name()+" of immutable type "+
		types.TypeString(t.named, c.qual))
}
