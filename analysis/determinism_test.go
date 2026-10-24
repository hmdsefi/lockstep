// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"fmt"
	"go/types"
	"os"
	"slices"
	"strings"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
	"golang.org/x/tools/go/packages"
)

// TestDeterminism checks the LS100 corpus (AT-ANL-08, AT-ANL-09, AT-ANL-10): every finding of
// ls100/bad with its message and chain, none in ls100/good, and the diagnostic format.
func TestDeterminism(t *testing.T) {
	runCorpus(t, Determinism, "ls100/bad", "ls100/good")
}

// TestATANL07AllowScope checks the allow scopes on LS100 findings (AT-ANL-07), and that
// Directives reports exactly one problem in the same package: the allow without a reason.
func TestATANL07AllowScope(t *testing.T) {
	runCorpus(t, Determinism, "allow")

	// The want comments of the package are for Determinism, so analysistest's own checks fail
	// for Directives; the recorder keeps them away from t.
	var rec recorder
	results := analysistest.Run(&rec, analysistest.TestData(), Directives, "allow")
	if len(results) != 1 {
		t.Fatalf("Directives on allow: %d results, want 1 (errors: %q)", len(results), rec.errors)
	}
	res := results[0]
	if len(res.Diagnostics) != 1 {
		t.Fatalf("Directives on allow: %d diagnostics, want 1: %v", len(res.Diagnostics), res.Diagnostics)
	}
	d := res.Diagnostics[0]
	if want := "LS001: //lockstep:allow LS100 needs a reason"; d.Message != want {
		t.Errorf("Directives on allow: message %q, want %q", d.Message, want)
	}
	pos := res.Pass.Fset.Position(d.Pos)
	src, err := os.ReadFile(pos.Filename)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(src), "\n")
	if got := strings.TrimSpace(lines[pos.Line-1]); got != "//lockstep:allow LS100" {
		t.Errorf("Directives on allow: diagnostic on line %d %q, want the allow without a reason", pos.Line, got)
	}
}

// recorder is an analysistest.Testing that records errors instead of failing a test.
type recorder struct {
	errors []string
}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

// TestSubRules checks the LS100 sub-rule table against ANL-130: names in the table's order, each
// once, with the origin of its rule (ANL-150).
func TestSubRules(t *testing.T) {
	want := []subRule{
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
	if !slices.Equal(ls100SubRules, want) {
		t.Errorf("ls100SubRules = %v, want %v", ls100SubRules, want)
	}
}

// namePackages are the standard-library packages whose names ANL-130 and ANL-140 list.
var namePackages = []string{
	"context", "crypto/rand", "database/sql", "maps", "math/rand", "math/rand/v2", "net", "net/http",
	"net/rpc", "os", "os/exec", "reflect", "runtime", "sync", "syscall", "time",
}

// Messages of the use findings, before the chain (ANL-130).
const (
	wallClockText  = " uses the wall clock; use ctx.Now, ctx.After or ctx.Every"
	globalRandText = " uses global randomness; use ctx.Rand()"
	cryptoRandText = " is nondeterministic; use ctx.Rand()"
	goroutineText  = " starts a goroutine; use lockstep.Do for work outside the turn"
	runtimeText    = " runs a function on a runtime goroutine; release resources in Stopped"
	unorderedText  = " iterates in random order; sort the keys first"
	mapIterText    = " yields in random order; wrap it in slices.Sorted"
	envText        = " reads the environment; read configuration before spawning and pass it through the factory"
	blockingText   = " blocks; run it as an effect with lockstep.Do"
)

// TestUseFindingNames resolves every name of ANL-130 and ANL-140 in the standard library and checks
// the message useFinding gives for it, and that near misses give none. The rows that name every
// function or method with a prefix are checked against every exported function and method of the
// package.
func TestUseFindingNames(t *testing.T) {
	pkgs := loadStdlib(t)
	notSorted := func() bool { return false }

	var cases []useCase
	add := func(text string, names ...string) {
		for _, n := range names {
			want := ""
			if text != "" {
				want = n + text
			}
			cases = append(cases, useCase{lookupName(t, pkgs, n), n, want})
		}
	}
	add(wallClockText, "time.Now", "time.Since", "time.Until", "time.Sleep", "time.After", "time.AfterFunc",
		"time.NewTimer", "time.NewTicker", "time.Tick", "context.WithDeadline", "context.WithDeadlineCause",
		"context.WithTimeout", "context.WithTimeoutCause")
	add(cryptoRandText, "crypto/rand.Reader", "crypto/rand.Read", "crypto/rand.Int", "crypto/rand.Prime",
		"crypto/rand.Text")
	add(goroutineText, "context.AfterFunc", "(*sync.WaitGroup).Go")
	add(runtimeText, "runtime.SetFinalizer", "runtime.AddCleanup")
	add(unorderedText, "(reflect.Value).MapRange", "(reflect.Value).MapKeys", "(reflect.Value).Seq",
		"(reflect.Value).Seq2", "(*sync.Map).Range", "maps.DeleteFunc", "maps.EqualFunc")
	add(mapIterText, "maps.All", "maps.Keys", "maps.Values")
	add(envText, "os.Getenv", "os.LookupEnv", "os.Environ", "os.ExpandEnv", "os.TempDir", "os.UserHomeDir",
		"os.UserCacheDir", "os.UserConfigDir", "syscall.Getenv", "syscall.Environ")
	add(blockingText,
		"(*sync.WaitGroup).Wait", "(*sync.Cond).Wait", "(*sync.Mutex).Lock", "(*sync.RWMutex).Lock",
		"(*sync.RWMutex).RLock",
		"net.Dial", "net.DialTimeout", "net.Listen", "net.ListenPacket", "net.LookupHost", "net.ResolveTCPAddr",
		"(net.Conn).Read", "(net.Conn).Write", "(net.Listener).Accept", "(net.PacketConn).ReadFrom",
		"(*net.TCPListener).AcceptTCP", "(*net.UDPConn).WriteToUDP",
		"net/http.Get", "net/http.Head", "net/http.Post", "net/http.PostForm", "net/http.ListenAndServe",
		"net/http.ListenAndServeTLS", "net/http.Serve", "net/http.ServeTLS",
		"(*net/http.Client).Do", "(*net/http.Client).Get", "(*net/http.Client).Head", "(*net/http.Client).Post",
		"(*net/http.Client).PostForm", "(*net/http.Server).ListenAndServe", "(*net/http.Server).ListenAndServeTLS",
		"(*net/http.Server).Serve", "(*net/http.Server).ServeTLS", "(*net/http.Server).Shutdown",
		"net/rpc.Dial", "net/rpc.DialHTTP", "net/rpc.DialHTTPPath", "(*net/rpc.Client).Call",
		"database/sql.Open", "(*database/sql.DB).Conn", "(*database/sql.DB).QueryRow", "(*database/sql.Tx).ExecContext",
		"(*database/sql.Stmt).Query", "(*database/sql.Conn).PingContext", "(*database/sql.DB).BeginTx",
		"(*database/sql.Conn).PrepareContext", "(*database/sql.Rows).Next", "(*database/sql.Row).Scan",
		"(*os/exec.Cmd).Run", "(*os/exec.Cmd).Start", "(*os/exec.Cmd).Wait", "(*os/exec.Cmd).Output",
		"(*os/exec.Cmd).CombinedOutput",
		"os.Open", "os.OpenFile", "os.Create", "os.ReadFile", "os.WriteFile", "os.ReadDir", "os.Stat", "os.Lstat",
		"os.Mkdir", "os.MkdirAll", "os.Remove", "os.RemoveAll", "os.Rename", "(*os.File).Read", "(*os.File).Sync")
	// Near misses: names next to the listed ones that are not reported.
	add("", "database/sql.OpenDB", "(*sync.Mutex).TryLock", "(*sync.Mutex).Unlock", "(*sync.RWMutex).TryRLock",
		"(*sync.WaitGroup).Add", "(net.Conn).Close", "(net.Conn).LocalAddr", "net.ParseIP", "(*os.File).Name",
		"(*os.File).Fd", "(*os.File).Close", "(time.Time).After", "(time.Duration).String",
		"(*math/rand/v2.Rand).IntN", "(*math/rand.Rand).Intn", "math/rand.New", "math/rand.NewSource",
		"math/rand.NewZipf", "math/rand/v2.New", "math/rand/v2.NewPCG", "math/rand/v2.NewChaCha8",
		"math/rand/v2.NewZipf", "(*database/sql.Tx).Commit", "(*database/sql.Rows).Scan",
		"(*database/sql.DB).Close", "(*net/http.Client).CloseIdleConnections", "(*os/exec.Cmd).String",
		"os.Getpid", "maps.Clone", "maps.Copy")

	// globalrand: every package-level function but the constructors.
	for _, path := range []string{"math/rand", "math/rand/v2"} {
		for _, fn := range exportedFuncs(pkgs[path]) {
			want := ""
			if !slices.Contains(randCtorNames, fn.Name()) {
				want = fn.FullName() + globalRandText
			}
			cases = append(cases, useCase{fn, fn.FullName(), want})
		}
	}
	// net: every function with one of the function prefixes; every method of a type declared in
	// net with one of the method prefixes.
	for _, fn := range exportedFuncs(pkgs["net"]) {
		cases = append(cases, prefixCase(fn, netFuncPrefixes))
	}
	for _, name := range pkgs["net"].Scope().Names() {
		if tn, ok := pkgs["net"].Scope().Lookup(name).(*types.TypeName); ok && tn.Exported() {
			for _, fn := range methodsOf(tn) {
				cases = append(cases, prefixCase(fn, netMethodPrefixes))
			}
		}
	}
	// database/sql: methods of DB, Conn, Tx and Stmt by prefix, and Conn.
	for _, name := range []string{"DB", "Conn", "Tx", "Stmt"} {
		tn, ok := pkgs["database/sql"].Scope().Lookup(name).(*types.TypeName)
		if !ok {
			t.Fatalf("database/sql.%s is not a type", name)
		}
		for _, fn := range methodsOf(tn) {
			c := prefixCase(fn, []string{"Query", "Exec", "Ping", "Begin", "Prepare"})
			if fn.Name() == "Conn" {
				c.want = fn.FullName() + blockingText
			}
			cases = append(cases, c)
		}
	}
	// os: every method of *File but Close, Name and Fd.
	file, ok := pkgs["os"].Scope().Lookup("File").(*types.TypeName)
	if !ok {
		t.Fatal("os.File is not a type")
	}
	for _, fn := range methodsOf(file) {
		want := fn.FullName() + blockingText
		if slices.Contains([]string{"Close", "Name", "Fd"}, fn.Name()) {
			want = ""
		}
		cases = append(cases, useCase{fn, fn.FullName(), want})
	}

	for _, c := range cases {
		if got := useFinding(c.obj, notSorted); got != c.want {
			t.Errorf("useFinding(%s) = %q, want %q", c.name, got, c.want)
		}
	}
	// mapiter: no finding when the call is the argument of a sorting function.
	for _, n := range []string{"maps.All", "maps.Keys", "maps.Values"} {
		if got := useFinding(lookupName(t, pkgs, n), func() bool { return true }); got != "" {
			t.Errorf("useFinding(%s) sorted = %q, want none", n, got)
		}
	}
}

// useCase is a resolved name and the message useFinding must give for it.
type useCase struct {
	obj  types.Object
	name string
	want string
}

// The constructors of math/rand and math/rand/v2 (globalrand) and the prefixes of the net row of
// ANL-140, as the spec lists them.
var (
	randCtorNames     = []string{"New", "NewSource", "NewZipf", "NewPCG", "NewChaCha8"}
	netFuncPrefixes   = []string{"Dial", "Listen", "Lookup", "Resolve"}
	netMethodPrefixes = []string{"Read", "Write", "Accept"}
)

// prefixCase expects a blocking finding for fn when its name starts with one of prefixes and is
// not Close.
func prefixCase(fn *types.Func, prefixes []string) useCase {
	c := useCase{fn, fn.FullName(), ""}
	if fn.Name() != "Close" && slices.ContainsFunc(prefixes, func(p string) bool {
		return strings.HasPrefix(fn.Name(), p)
	}) {
		c.want = fn.FullName() + blockingText
	}
	return c
}

// loadStdlib loads the type information of namePackages, by import path.
func loadStdlib(t *testing.T) map[string]*types.Package {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{Mode: packages.NeedName | packages.NeedTypes}, namePackages...)
	if err != nil {
		t.Fatal(err)
	}
	pkgs := map[string]*types.Package{}
	for _, p := range loaded {
		for _, e := range p.Errors {
			t.Errorf("load %s: %v", p.PkgPath, e)
		}
		pkgs[p.PkgPath] = p.Types
	}
	if len(pkgs) != len(namePackages) {
		t.Fatalf("loaded %d packages, want %d", len(pkgs), len(namePackages))
	}
	return pkgs
}

// lookupName resolves "<path>.<Name>", "(<path>.<T>).<M>" or "(*<path>.<T>).<M>" and checks that
// the name is the object's own: its types.Func.FullName, or path and name for a variable.
func lookupName(t *testing.T, pkgs map[string]*types.Package, name string) types.Object {
	t.Helper()
	split := func(s string) (*types.Package, string) {
		i := strings.LastIndex(s, ".")
		pkg := pkgs[s[:i]]
		if pkg == nil {
			t.Fatalf("%s: package %s not loaded", name, s[:i])
		}
		return pkg, s[i+1:]
	}
	var obj types.Object
	if recv, method, ok := strings.Cut(strings.TrimPrefix(name, "("), ")."); ok && name[0] == '(' {
		pkg, typeName := split(strings.TrimPrefix(recv, "*"))
		tn := pkg.Scope().Lookup(typeName)
		if tn == nil {
			t.Fatalf("%s: no type %s", name, typeName)
		}
		typ := tn.Type()
		if strings.HasPrefix(recv, "*") {
			typ = types.NewPointer(typ)
		}
		obj, _, _ = types.LookupFieldOrMethod(typ, false, pkg, method)
	} else {
		pkg, n := split(name)
		obj = pkg.Scope().Lookup(n)
	}
	if obj == nil {
		t.Fatalf("%s: not declared", name)
	}
	full := obj.Pkg().Path() + "." + obj.Name()
	if fn, ok := obj.(*types.Func); ok {
		full = fn.FullName()
	}
	if full != name {
		t.Fatalf("%s resolves to %s", name, full)
	}
	return obj
}

// exportedFuncs returns the exported package-level functions of pkg, in name order.
func exportedFuncs(pkg *types.Package) []*types.Func {
	var out []*types.Func
	for _, name := range pkg.Scope().Names() {
		if fn, ok := pkg.Scope().Lookup(name).(*types.Func); ok && fn.Exported() {
			out = append(out, fn)
		}
	}
	return out
}

// methodsOf returns the exported methods of the type tn declares, of the pointer type for a
// non-interface type, promoted methods included, in method-set order.
func methodsOf(tn *types.TypeName) []*types.Func {
	typ := tn.Type()
	if !types.IsInterface(typ) {
		typ = types.NewPointer(typ)
	}
	var out []*types.Func
	ms := types.NewMethodSet(typ)
	for i := range ms.Len() {
		if fn, ok := ms.At(i).Obj().(*types.Func); ok && fn.Exported() {
			out = append(out, fn)
		}
	}
	return out
}
