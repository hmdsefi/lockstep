// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"cmp"
	"go/ast"
	"go/token"
	"go/types"
	"reflect"
	"slices"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/buildssa"
	"golang.org/x/tools/go/ssa"
)

// actorsAnalyzer finds actor types, roots, actor code and chains (ANL-030 to ANL-060).
var actorsAnalyzer = &analysis.Analyzer{
	Name:       "lsactors",
	Doc:        "find lockstep actor code",
	Requires:   []*analysis.Analyzer{buildssa.Analyzer},
	Run:        runActors,
	ResultType: reflect.TypeFor[*actorCode](),
}

// actorCode is the result of actorsAnalyzer for one package. Its slices are in a fixed order that
// depends only on the source (ANL-120).
type actorCode struct {
	// Actors holds the actor types declared in the package (ANL-030), in source order of their
	// declarations (file name, then offset).
	Actors []*actorType

	// Funcs holds the actor-code functions (ANL-050) in the order the breadth-first search of
	// ANL-060 reached them: first every root in source order, then the functions they reach.
	Funcs []*actorFunc

	byBody map[ast.Node]*actorFunc
	succ   map[*actorFunc][]*actorFunc // the edges of ANL-050 between actor-code functions
	fset   *token.FileSet
}

// actorType is an actor type of the analyzed package.
type actorType struct {
	// Type is the declared type; for a generic type, the uninstantiated origin.
	Type *types.Named

	// Message is the message type M. For a generic actor type it is written in the type
	// parameters of Type's declaration.
	Message types.Type

	// Receive is the declared Receive method that makes Type an actor type. It may be promoted from
	// an embedded type, and then it is not one of Roots.
	Receive *types.Func

	// Roots holds the roots declared on Type (ANL-040) in source order.
	Roots []*actorFunc
}

// actorFunc is one actor-code function.
type actorFunc struct {
	// Fn is the SSA function. For a generic function it is the generic function, not an instance.
	Fn *ssa.Function

	// Body is Fn's syntax, an *ast.FuncDecl or an *ast.FuncLit. Function literals nested in it are
	// actor-code functions of their own when they are reached; a check that walks Body skips them
	// so that each literal is inspected once (ANL-050).
	Body ast.Node

	// Chain is the path by which the search first reached Fn, for example
	// "(*Account).Receive -> apply" (ANL-060).
	Chain string

	// Actor is the actor type when Fn is a method of one (a root or another method), else nil.
	Actor *actorType
}

// inspect walks Body like ast.Inspect, without the function literals nested in it: visit is
// never called for a nested *ast.FuncLit or anything inside one. This is the body of ANL-050.
func (f *actorFunc) inspect(visit func(ast.Node) bool) {
	ast.Inspect(f.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok && n != f.Body {
			return false
		}
		return visit(n)
	})
}

// reach returns the actor-code functions reachable from the given ones over the edges of ANL-050,
// the given ones included, each once. It searches breadth first like ANL-060: the given functions
// in source order (file name, then offset), then the successors of each in source order of their
// declarations. Unlike the chains, it is per call, so a helper that two actors share is in the
// result for each of them: "T's actor code" is reach(T.Roots...), and the functions reachable
// from a Stopped root are reach(stopped).
func (c *actorCode) reach(from ...*actorFunc) []*actorFunc {
	queue := slices.Clone(from)
	slices.SortStableFunc(queue, func(x, y *actorFunc) int { return comparePos(c.fset, x.Fn.Pos(), y.Fn.Pos()) })
	queue = slices.Compact(queue)
	seen := make(map[*actorFunc]bool, len(queue))
	for _, f := range queue {
		seen[f] = true
	}
	for i := 0; i < len(queue); i++ {
		for _, g := range c.succ[queue[i]] {
			if !seen[g] {
				seen[g] = true
				queue = append(queue, g)
			}
		}
	}
	return queue
}

// funcOf returns the actor-code function whose Body is body, or nil when body is not the body of
// an actor-code function.
func (c *actorCode) funcOf(body ast.Node) *actorFunc {
	return c.byBody[body]
}

// rootNames are the method names that make a method of an actor type a root (ANL-040).
var rootNames = []string{"Receive", "Started", "Stopped", "Signal"}

// actorSearch holds the state of runActors for one package.
type actorSearch struct {
	pass *analysis.Pass

	// declared maps the declared functions of the package to their SSA functions.
	declared map[*types.Func]*ssa.Function

	// own holds every source function of the package, function literals included.
	own map[*ssa.Function]bool

	// goLits holds the function literals that are the called function of a go statement.
	goLits map[*ast.FuncLit]bool

	code *actorCode
}

// runActors computes the package's actorCode. It never returns an error (ANL §7): without an
// SSA result, which the driver guarantees, it finds no actor code.
func runActors(pass *analysis.Pass) (any, error) {
	ssaResult, ok := pass.ResultOf[buildssa.Analyzer].(*buildssa.SSA)
	if !ok {
		return &actorCode{byBody: map[ast.Node]*actorFunc{}, fset: pass.Fset}, nil
	}
	s := &actorSearch{
		pass:     pass,
		declared: map[*types.Func]*ssa.Function{},
		own:      map[*ssa.Function]bool{},
		goLits:   map[*ast.FuncLit]bool{},
		code: &actorCode{
			byBody: map[ast.Node]*actorFunc{},
			succ:   map[*actorFunc][]*actorFunc{},
			fset:   pass.Fset,
		},
	}
	for _, fn := range ssaResult.SrcFuncs {
		s.own[fn] = true
		if obj, ok := fn.Object().(*types.Func); ok && fn.Parent() == nil {
			s.declared[obj] = fn
		}
	}
	for _, f := range pass.Files {
		ast.Inspect(f, func(n ast.Node) bool {
			if g, ok := n.(*ast.GoStmt); ok {
				if lit, ok := ast.Unparen(g.Call.Fun).(*ast.FuncLit); ok {
					s.goLits[lit] = true
				}
			}
			return true
		})
	}
	s.findActors()
	s.search()
	return s.code, nil
}

// findActors fills code.Actors with the package's actor types and their roots (ANL-030, ANL-040).
func (s *actorSearch) findActors() {
	scope := s.pass.Pkg.Scope()
	var names []*types.TypeName
	for _, name := range scope.Names() {
		if tn, ok := scope.Lookup(name).(*types.TypeName); ok && !tn.IsAlias() {
			names = append(names, tn)
		}
	}
	slices.SortFunc(names, func(a, b *types.TypeName) int { return s.comparePos(a.Pos(), b.Pos()) })
	for _, tn := range names {
		if a := s.actorTypeOf(tn); a != nil {
			s.code.Actors = append(s.code.Actors, a)
		}
	}
}

// actorTypeOf returns the actor type declared by tn, or nil when tn does not declare one.
func (s *actorSearch) actorTypeOf(tn *types.TypeName) *actorType {
	named, ok := tn.Type().(*types.Named)
	if !ok {
		return nil
	}
	self := selfInstance(named)
	if self == nil {
		return nil
	}
	sel := types.NewMethodSet(types.NewPointer(self)).Lookup(nil, "Receive")
	if sel == nil {
		return nil
	}
	receive, ok := sel.Obj().(*types.Func)
	if !ok {
		return nil
	}
	params := receive.Signature().Params()
	if params.Len() != 2 || receive.Signature().Results().Len() != 0 {
		return nil
	}
	msg := contextTypeArg(params.At(0).Type())
	if msg == nil || !types.Identical(params.At(1).Type(), msg) {
		return nil
	}
	a := &actorType{Type: named, Message: msg, Receive: receive.Origin()}
	for i := range self.NumMethods() {
		m := self.Method(i)
		if !slices.Contains(rootNames, m.Name()) {
			continue
		}
		params := m.Signature().Params()
		if params.Len() == 0 {
			continue
		}
		if x := contextTypeArg(params.At(0).Type()); x == nil || !types.Identical(x, msg) {
			continue
		}
		if fn := s.declared[m.Origin()]; fn != nil {
			a.Roots = append(a.Roots, &actorFunc{Fn: fn, Body: fn.Syntax(), Chain: s.name(fn), Actor: a})
		}
	}
	slices.SortFunc(a.Roots, func(x, y *actorFunc) int { return s.comparePos(x.Fn.Pos(), y.Fn.Pos()) })
	return a
}

// selfInstance returns named itself when it is not generic, and otherwise its instance with its
// own type parameters as type arguments. In that instance every method's signature is written in
// the same type parameters, those of the type declaration, so the message types of different
// methods compare as identical; the methods of the generic type itself each have their own
// receiver type parameters. It returns nil if the instance cannot be built.
func selfInstance(named *types.Named) *types.Named {
	tparams := named.TypeParams()
	if tparams.Len() == 0 {
		return named
	}
	targs := make([]types.Type, tparams.Len())
	for i := range targs {
		targs[i] = tparams.At(i)
	}
	inst, err := types.Instantiate(nil, named, targs, false)
	if err != nil {
		return nil
	}
	self, ok := inst.(*types.Named)
	if !ok {
		return nil
	}
	return self
}

// contextTypeArg returns X when t is *lockstep.Context[X], and nil otherwise.
func contextTypeArg(t types.Type) types.Type {
	ptr, ok := types.Unalias(t).(*types.Pointer)
	if !ok {
		return nil
	}
	named, ok := types.Unalias(ptr.Elem()).(*types.Named)
	if !ok || !isAPI(named.Obj(), "Context") || named.TypeArgs().Len() != 1 {
		return nil
	}
	return named.TypeArgs().At(0)
}

// search runs the breadth-first search of ANL-060 from all roots and fills code.Funcs and the
// edges between them, code.succ.
func (s *actorSearch) search() {
	owners := map[*types.TypeName]*actorType{}
	var queue []*actorFunc
	for _, a := range s.code.Actors {
		owners[a.Type.Obj()] = a
		queue = append(queue, a.Roots...)
	}
	slices.SortFunc(queue, func(x, y *actorFunc) int { return s.comparePos(x.Fn.Pos(), y.Fn.Pos()) })
	reached := map[*ssa.Function]*actorFunc{}
	for _, f := range queue {
		reached[f.Fn] = f
	}
	for len(queue) > 0 {
		f := queue[0]
		queue = queue[1:]
		s.code.Funcs = append(s.code.Funcs, f)
		s.code.byBody[f.Body] = f
		for _, g := range s.successors(f.Fn) {
			next := reached[g]
			if next == nil {
				next = &actorFunc{
					Fn:    g,
					Body:  g.Syntax(),
					Chain: f.Chain + " -> " + s.name(g),
					Actor: owners[receiverTypeNameObj(g)],
				}
				reached[g] = next
				queue = append(queue, next)
			}
			s.code.succ[f] = append(s.code.succ[f], next)
		}
	}
}

// receiverTypeNameObj returns the type name of fn's receiver's named type (pointer removed,
// origin taken), or nil when fn is not a method.
func receiverTypeNameObj(fn *ssa.Function) *types.TypeName {
	recv := fn.Signature.Recv()
	if recv == nil {
		return nil
	}
	t := types.Unalias(recv.Type())
	if ptr, ok := t.(*types.Pointer); ok {
		t = types.Unalias(ptr.Elem())
	}
	named, ok := t.(*types.Named)
	if !ok {
		return nil
	}
	return named.Origin().Obj()
}

// successors returns the functions fn has an edge to (ANL-050), each once, in source order of
// their declarations.
func (s *actorSearch) successors(fn *ssa.Function) []*ssa.Function {
	var out []*ssa.Function
	add := func(g *ssa.Function) {
		if g = s.resolve(g); g != nil && !slices.Contains(out, g) {
			out = append(out, g)
		}
	}
	var operands []*ssa.Value
	for _, b := range fn.Blocks {
		for _, instr := range b.Instrs {
			switch instr := instr.(type) {
			case *ssa.Go:
				// The called function of a go statement is never an edge; its arguments are
				// operands like any other.
				for _, arg := range instr.Call.Args {
					if g, ok := arg.(*ssa.Function); ok {
						add(g)
					}
				}
				continue
			case ssa.CallInstruction:
				if callee := instr.Common().StaticCallee(); callee != nil {
					add(callee)
				}
			}
			operands = instr.Operands(operands[:0])
			for _, op := range operands {
				if op == nil {
					continue
				}
				if g, ok := (*op).(*ssa.Function); ok {
					add(g)
				}
			}
		}
	}
	for _, lit := range fn.AnonFuncs {
		if syntax, ok := lit.Syntax().(*ast.FuncLit); ok && !s.goLits[syntax] {
			out = append(out, lit)
		}
	}
	slices.SortFunc(out, func(x, y *ssa.Function) int { return s.comparePos(x.Pos(), y.Pos()) })
	return out
}

// resolve maps a function referred to by an instruction to the source function of the package it
// stands for: an instance to its generic origin, a bound-method or thunk wrapper to the wrapped
// method. It returns nil for function literals, which are edges only through
// ssa.Function.AnonFuncs, and for functions of other packages.
func (s *actorSearch) resolve(g *ssa.Function) *ssa.Function {
	if origin := g.Origin(); origin != nil {
		g = origin
	}
	if g.Parent() != nil {
		return nil
	}
	if g.Synthetic != "" {
		obj, ok := g.Object().(*types.Func)
		if !ok {
			return nil
		}
		return s.declared[obj.Origin()]
	}
	if !s.own[g] {
		return nil
	}
	return g
}

// name returns fn's name in chains (ANL-060).
func (s *actorSearch) name(fn *ssa.Function) string {
	return fn.RelString(s.pass.Pkg)
}

// comparePos orders positions by file name, then offset, ignoring //line directives.
func (s *actorSearch) comparePos(x, y token.Pos) int {
	return comparePos(s.pass.Fset, x, y)
}

// comparePos orders positions of fset by file name, then offset, ignoring //line directives.
func comparePos(fset *token.FileSet, x, y token.Pos) int {
	px := fset.PositionFor(x, false)
	py := fset.PositionFor(y, false)
	return cmp.Or(cmp.Compare(px.Filename, py.Filename), cmp.Compare(px.Offset, py.Offset))
}
