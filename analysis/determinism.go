// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
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
	{"env", originFaultline},
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
	for cur := range insp.Root().Preorder((*ast.FuncDecl)(nil), (*ast.FuncLit)(nil)) {
		if f := code.funcOf(cur.Node()); f != nil {
			checkDeterminism(pass, r, f, cur)
		}
	}
	r.flush()
	return nil, nil
}

// checkDeterminism reports the LS100 findings in the body of the actor-code function f, whose
// syntax is at cur. The body leaves out the function literals nested in f (ANL-050): those that
// are actor code are checked as functions of their own.
func checkDeterminism(pass *analysis.Pass, r *reporter, f *actorFunc, cur inspector.Cursor) {
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
		case *ast.Ident:
			if text := useFinding(pass.TypesInfo.Uses[n]); text != "" {
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
// the variable Reader included.
func useFinding(obj types.Object) string {
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
	case fn.Signature().Recv() != nil:
		// The remaining sub-rules name package-level functions only.
	case slices.Contains(wallClockFuncs[pkg], name):
		return pkg + "." + name + " uses the wall clock; use ctx.Now, ctx.After or ctx.Every"
	case (pkg == "math/rand" || pkg == "math/rand/v2") && !slices.Contains(randCtors, name):
		return pkg + "." + name + " uses global randomness; use ctx.Rand()"
	case slices.Contains(envFuncs, pkg+"."+name):
		return pkg + "." + name +
			" reads the environment; read configuration before spawning and pass it through the factory"
	}
	return ""
}
