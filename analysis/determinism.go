// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/token"
	"go/types"
	"slices"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Determinism reports nondeterminism and blocking in actor code (LS100). Phase 1.
var Determinism = &analysis.Analyzer{
	Name:     "determinism",
	Doc:      "report nondeterminism and blocking in actor code (LS100)",
	Requires: []*analysis.Analyzer{actorsAnalyzer, indexAnalyzer, inspect.Analyzer},
	Run:      runDeterminism,
}

// originFaultline is the origin of a sub-rule that ports one of faultline's determinism rules
// (ANL-150).
const originFaultline = "faultline"

// originLockstep is the origin of a sub-rule of lockstep's own (RFC-0003), which maps to no
// faultline rule (ANL-150).
const originLockstep = "lockstep"

// subRule is one LS100 sub-rule of ANL-130 and the origin of its rule.
type subRule struct {
	name   string
	origin string
}

// ls100SubRules lists the LS100 sub-rules in ANL-130 order. Every sub-rule of origin faultline
// maps to exactly one faultline rule (ANL-150, ANL-160).
var ls100SubRules = []subRule{
	{"wallclock", originFaultline},
	{"globalrand", originFaultline},
	{"cryptorand", originFaultline},
	{"goroutine", originFaultline},
	{"select", originFaultline},
	{"maprange", originFaultline},
	{"unordered", originFaultline},
	{"mapiter", originFaultline},
	{"env", originFaultline},
	{"blocking", originLockstep},
}

// The detection lists below are copied from faultline v0.1.2 (internal/detlint/lint.go); the
// module does not import faultline.
var (
	// wallClockFuncs read the wall clock or arm a runtime timer, by import path (wallclock).
	wallClockFuncs = map[string][]string{
		"time":    {"Now", "Since", "Until", "Sleep", "After", "AfterFunc", "NewTimer", "NewTicker", "Tick"},
		"context": {"WithDeadline", "WithDeadlineCause", "WithTimeout", "WithTimeoutCause"},
	}

	// goroutineFuncs run a function on another goroutine without a go statement, by
	// types.Func.FullName, with the rest of the message (goroutine).
	goroutineFuncs = map[string]string{
		"context.AfterFunc":    "starts a goroutine; use lockstep.Do for work outside the turn",
		"(*sync.WaitGroup).Go": "starts a goroutine; use lockstep.Do for work outside the turn",
		"runtime.SetFinalizer": "runs a function on a runtime goroutine; release resources in Stopped",
		"runtime.AddCleanup":   "runs a function on a runtime goroutine; release resources in Stopped",
	}

	// randCtors are the package-level functions of math/rand and math/rand/v2 that build a seeded
	// generator instead of using the global one (globalrand).
	randCtors = []string{"New", "NewSource", "NewZipf", "NewPCG", "NewChaCha8"}

	// envFuncs read environment variables, by import path and name (env). TempDir and the
	// User*Dir functions read them to find a directory.
	envFuncs = []string{
		"os.Getenv", "os.LookupEnv", "os.Environ", "os.ExpandEnv", "syscall.Getenv", "syscall.Environ",
		"os.TempDir", "os.UserHomeDir", "os.UserCacheDir", "os.UserConfigDir",
	}

	// unorderedFuncs list a map in random order, or call a function in map order, by
	// types.Func.FullName (unordered). Seq and Seq2 do so only for a map value, but the check
	// cannot tell the value's kind.
	unorderedFuncs = []string{
		"(reflect.Value).MapRange", "(reflect.Value).MapKeys", "(reflect.Value).Seq", "(reflect.Value).Seq2",
		"(*sync.Map).Range", "maps.DeleteFunc", "maps.EqualFunc",
	}

	// mapIterFuncs are the iterators of package maps that yield in map order (mapiter), and
	// sortedFuncs the functions of package slices that sort what such an iterator yields.
	mapIterFuncs = []string{"All", "Keys", "Values"}
	sortedFuncs  = []string{"Sorted", "SortedFunc", "SortedStableFunc"}
)

// idDeterminism is the check ID of Determinism.
const idDeterminism = "LS100"

func runDeterminism(pass *analysis.Pass) (any, error) {
	code, ok := pass.ResultOf[actorsAnalyzer].(*actorCode)
	if !ok {
		return nil, nil
	}
	idx, ok := indexOf(pass)
	if !ok {
		return nil, nil
	}
	insp, ok := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	if !ok {
		return nil, nil
	}
	r := newReporter(pass, idx)
	qual := func(p *types.Package) string {
		if p == pass.Pkg {
			return ""
		}
		return p.Name()
	}
	for cur := range insp.Root().Preorder((*ast.FuncDecl)(nil), (*ast.FuncLit)(nil)) {
		if f := code.funcOf(cur.Node()); f != nil {
			checkDeterminism(pass, r, f, cur, qual)
		}
	}
	r.flush()
	return nil, nil
}

// checkDeterminism reports the LS100 findings in the body of the actor-code function f, whose
// syntax is at cur. The body leaves out the function literals nested in f (ANL-050): those that
// are actor code are checked as functions of their own. Types in messages are qualified with qual
// (ANL-110).
func checkDeterminism(pass *analysis.Pass, r *reporter, f *actorFunc, cur inspector.Cursor, qual types.Qualifier) {
	info := pass.TypesInfo
	cur.Inspect(nil, func(c inspector.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.FuncLit:
			return n == f.Body
		case *ast.GoStmt:
			r.reportActor(n.Go, idDeterminism,
				"go statement starts a goroutine; use lockstep.Do for work outside the turn", f.Chain)
		case *ast.SelectStmt:
			r.reportActor(n.Select, idDeterminism,
				"select statement waits on channels; actors get input only as messages", f.Chain)
		case *ast.RangeStmt:
			if tv, ok := info.Types[n.X]; ok {
				switch {
				case allowsMap(tv.Type):
					r.reportActor(n.For, idDeterminism, "range over "+types.TypeString(tv.Type, qual)+
						" has random order; range over slices.Sorted(maps.Keys(m))", f.Chain)
				case allowsChan(tv.Type):
					r.reportActor(n.For, idDeterminism,
						"range over a channel blocks; actors get input only as messages", f.Chain)
				}
			}
		case *ast.SendStmt:
			r.reportActor(n.Arrow, idDeterminism, "channel send blocks; actors talk with lockstep.Send", f.Chain)
		case *ast.UnaryExpr:
			if n.Op == token.ARROW {
				r.reportActor(n.OpPos, idDeterminism,
					"channel receive blocks; actors get input only as messages", f.Chain)
			}
		case *ast.Ident:
			if text := useFinding(info.Uses[n], func() bool { return sortedArgument(c, info) }); text != "" {
				r.reportActor(n.Pos(), idDeterminism, text, f.Chain)
			}
		}
		return true
	})
}

// useFinding returns the LS100 message, without its ID and chain, for a use of obj in actor code,
// or "" when the use is not reported (ANL-130). It follows the identifier switch of faultline's
// lint at v0.1.2: the goroutine functions are matched before methods are set aside, so
// (*sync.WaitGroup).Go is reported and a method such as (*rand.Rand).IntN is not. Unlike that
// switch, which reports the import of crypto/rand, it reports every use of a crypto/rand object,
// the variable Reader included, and it adds the blocking calls of ANL-140. sorted reports whether
// the use is the first argument of a sorting function of package slices (mapiter).
func useFinding(obj types.Object, sorted func() bool) string {
	if obj == nil || obj.Pkg() == nil {
		return ""
	}
	pkg, name := obj.Pkg().Path(), obj.Name()
	if pkg == "crypto/rand" {
		return "crypto/rand." + name + " is nondeterministic; use ctx.Rand()"
	}
	fn, ok := obj.(*types.Func)
	if !ok {
		return ""
	}
	full := fn.FullName()
	switch {
	case goroutineFuncs[full] != "":
		return full + " " + goroutineFuncs[full]
	case slices.Contains(unorderedFuncs, full):
		return full + " iterates in random order; sort the keys first"
	case isBlocking(fn):
		return full + " blocks; run it as an effect with lockstep.Do"
	case fn.Signature().Recv() != nil:
		// The remaining sub-rules name package-level functions only.
	case slices.Contains(wallClockFuncs[pkg], name):
		return pkg + "." + name + " uses the wall clock; use ctx.Now, ctx.After or ctx.Every"
	case (pkg == "math/rand" || pkg == "math/rand/v2") && !slices.Contains(randCtors, name):
		return pkg + "." + name + " uses global randomness; use ctx.Rand()"
	case slices.Contains(envFuncs, pkg+"."+name):
		return pkg + "." + name +
			" reads the environment; read configuration before spawning and pass it through the factory"
	case pkg == "maps" && slices.Contains(mapIterFuncs, name) && !sorted():
		return "maps." + name + " yields in random order; wrap it in slices.Sorted"
	}
	return ""
}

// allowsMap reports whether the constraint t allows a map type: t is one, or t is an interface
// that embeds one, directly, as a union term or through another interface
// (`interface{ MapLike }` with `type MapLike interface{ ~map[K]V }`), at any depth (maprange). A
// type parameter's underlying type is its constraint. The embedding cannot form a cycle: go/types
// rejects a recursive constraint. Ported from faultline v0.1.2.
func allowsMap(t types.Type) bool {
	return allows(t, func(u types.Type) bool {
		_, ok := u.(*types.Map)
		return ok
	})
}

// allowsChan reports whether the constraint t allows a channel type, in the same way as allowsMap
// (blocking).
func allowsChan(t types.Type) bool {
	return allows(t, func(u types.Type) bool {
		_, ok := u.(*types.Chan)
		return ok
	})
}

// allows reports whether the constraint t allows a type whose underlying type matches: the
// walk of allowsMap.
func allows(t types.Type, match func(types.Type) bool) bool {
	u := t.Underlying()
	if match(u) {
		return true
	}
	iface, ok := u.(*types.Interface)
	if !ok {
		return false
	}
	for i := range iface.NumEmbeddeds() {
		e := iface.EmbeddedType(i)
		if union, ok := e.(*types.Union); ok {
			for j := range union.Len() {
				if allows(union.Term(j).Type(), match) {
					return true
				}
			}
		} else if allows(e, match) {
			return true
		}
	}
	return false
}

// sortedArgument reports whether the identifier at c (maps.All, maps.Keys or maps.Values) is
// called as the first argument of slices.Sorted, slices.SortedFunc or slices.SortedStableFunc
// (mapiter). Ported from faultline v0.1.2, with the cursor's ancestors in place of the stack.
func sortedArgument(c inspector.Cursor, info *types.Info) bool {
	id, ok := c.Node().(*ast.Ident)
	if !ok {
		return false
	}
	// Up to three ancestors of id, nearest first: the selector when id is qualified, the call of
	// id and the call taking that call as an argument.
	var stack []ast.Node
	for a := range c.Parent().Enclosing() {
		if len(stack) == 3 {
			break
		}
		stack = append(stack, a.Node())
	}
	i := 0
	var fun ast.Expr = id
	if i < len(stack) {
		if sel, ok := stack[i].(*ast.SelectorExpr); ok && sel.Sel == id {
			fun = sel
			i++
		}
	}
	if i+1 >= len(stack) {
		return false
	}
	inner, ok := stack[i].(*ast.CallExpr)
	if !ok || inner.Fun != fun {
		return false
	}
	outer, ok := stack[i+1].(*ast.CallExpr)
	if !ok || len(outer.Args) == 0 || outer.Args[0] != inner {
		return false
	}
	f := outer.Fun
	for {
		if x, ok := f.(*ast.IndexExpr); ok {
			f = x.X
		} else if x, ok := f.(*ast.IndexListExpr); ok {
			f = x.X
		} else {
			break
		}
	}
	if sel, ok := f.(*ast.SelectorExpr); ok {
		f = sel.Sel
	}
	ident, ok := f.(*ast.Ident)
	if !ok {
		return false
	}
	fn, ok := info.Uses[ident].(*types.Func)
	return ok && fn.Pkg() != nil && fn.Pkg().Path() == "slices" && slices.Contains(sortedFuncs, fn.Name())
}
