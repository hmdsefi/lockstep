// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"bufio"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// missing is the object string of an entry that a package does not declare.
const missing = "(not declared)"

// compareStub returns one line per ANL-020 entry whose object strings differ between the real
// lockstep package and the stub, in the order of the API table:
//
//	stub entry <entry> differs:\n  lockstep: <object string>\n  stub:     <object string>
//
// An object string is types.ObjectString(obj, q), where q returns the package's name. A method
// entry is found with types.LookupFieldOrMethod on its receiver type. Functions and methods are
// compared verbatim. A type entry whose underlying type is a struct is compared with that struct
// reduced to its exported fields: the owners declare these types as struct{ /* unexported */ },
// so the internal fields of the real types are not API and the stub does not reproduce them. Type
// parameters, the kind of the type and the exported fields stay compared. An entry that a package
// does not declare has the object string "(not declared)".
func compareStub(real, stub *types.Package) []string {
	var diffs []string
	for _, entry := range apiEntries {
		want, got := objectString(real, entry), objectString(stub, entry)
		if want != got {
			diffs = append(diffs, "stub entry "+entry+" differs:\n  lockstep: "+want+"\n  stub:     "+got)
		}
	}
	return diffs
}

// objectString returns the object string of the API-table entry in pkg, or missing.
func objectString(pkg *types.Package, entry string) string {
	typeName, methodName, isMethod := strings.Cut(entry, ".")
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		return missing
	}
	q := func(p *types.Package) string { return p.Name() }
	if !isMethod {
		if tn, ok := obj.(*types.TypeName); ok {
			obj = exportedFieldsOnly(tn)
		}
		return types.ObjectString(obj, q)
	}
	m, _, _ := types.LookupFieldOrMethod(obj.Type(), true, pkg, methodName)
	if _, ok := m.(*types.Func); !ok {
		return missing
	}
	return types.ObjectString(m, q)
}

// exportedFieldsOnly returns tn, or, when tn declares a defined type whose underlying type is a
// struct, a type name of the same name, package and type parameters whose struct has only the
// exported fields of the original.
func exportedFieldsOnly(tn *types.TypeName) *types.TypeName {
	named, ok := tn.Type().(*types.Named)
	if !ok {
		return tn
	}
	st, ok := named.Underlying().(*types.Struct)
	if !ok {
		return tn
	}
	var fields []*types.Var
	var tags []string
	for i := range st.NumFields() {
		if f := st.Field(i); f.Exported() {
			fields = append(fields, f)
			tags = append(tags, st.Tag(i))
		}
	}
	// ObjectString prefixes a package-level name with its package, so the copy goes into a scope.
	pkg := types.NewPackage(tn.Pkg().Path(), tn.Pkg().Name())
	reduced := types.NewTypeName(token.NoPos, pkg, tn.Name(), nil)
	pkg.Scope().Insert(reduced)
	n := types.NewNamed(reduced, types.NewStruct(fields, tags), nil)
	tparams := make([]*types.TypeParam, 0, named.TypeParams().Len())
	for tp := range named.TypeParams().TypeParams() {
		name := types.NewTypeName(token.NoPos, pkg, tp.Obj().Name(), nil)
		tparams = append(tparams, types.NewTypeParam(name, tp.Constraint()))
	}
	n.SetTypeParams(tparams)
	return reduced
}

// loadTypes loads the package at pattern with the given directory and environment and returns its
// type information.
func loadTypes(t *testing.T, dir, pattern string, env ...string) *types.Package {
	t.Helper()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedTypes,
		Dir:  dir,
		Env:  append(os.Environ(), env...),
	}
	pkgs, err := packages.Load(cfg, pattern)
	if err != nil {
		t.Fatalf("load %s: %v", pattern, err)
	}
	if len(pkgs) != 1 {
		t.Fatalf("load %s: got %d packages, want 1", pattern, len(pkgs))
	}
	for _, e := range pkgs[0].Errors {
		t.Errorf("load %s: %v", pattern, e)
	}
	if t.Failed() || pkgs[0].Types == nil {
		t.FailNow()
	}
	return pkgs[0].Types
}

// loadStub loads the stub of lockstep from the GOPATH tree gopath.
func loadStub(t *testing.T, gopath string) *types.Package {
	t.Helper()
	return loadTypes(t, gopath, lockstepPath, "GOPATH="+gopath, "GO111MODULE=off", "GOFLAGS=")
}

// stubGOPATH is the GOPATH tree that holds the stub.
func stubGOPATH(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// moduleName returns the module path that the go.mod file at path declares, or "" if it has none.
func moduleName(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if fields := strings.Fields(sc.Text()); len(fields) == 2 && fields[0] == "module" {
			return strings.Trim(fields[1], `"`)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return ""
}

// TestStubMatchesLockstep checks ANL-020 and ANL-380: every entry of the API table has the same
// object string in the real package and in the stub of the test corpus. It skips outside a
// checkout of lockstep (ANL-160).
func TestStubMatchesLockstep(t *testing.T) {
	if _, err := os.Stat(filepath.Join("..", "go.mod")); err != nil {
		t.Skipf("no root module: %v", err)
	}
	if m := moduleName(t, filepath.Join("..", "go.mod")); m != lockstepPath {
		t.Skipf("../go.mod declares module %q, not %s", m, lockstepPath)
	}
	real := loadTypes(t, "..", lockstepPath, "GOWORK=off")
	stub := loadStub(t, stubGOPATH(t))
	for _, d := range compareStub(real, stub) {
		t.Error(d)
	}
}

// mutatedStub writes a copy of the stub with the single occurrence of old replaced by repl into a
// temporary GOPATH tree and returns the stub package loaded from it.
func mutatedStub(t *testing.T, old, repl string) *types.Package {
	t.Helper()
	rel := filepath.Join("src", filepath.FromSlash(lockstepPath), "lockstep.go")
	src, err := os.ReadFile(filepath.Join("testdata", rel))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(src), old); n != 1 {
		t.Fatalf("stub has %d occurrences of %q, want 1", n, old)
	}
	gopath := t.TempDir()
	dst := filepath.Join(gopath, rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte(strings.Replace(string(src), old, repl, 1)), 0o600); err != nil { //nolint:gosec // dst is inside t.TempDir()
		t.Fatal(err)
	}
	return loadStub(t, gopath)
}

// TestCompareStub checks that compareStub finds a stub that differs from the real declaration
// (AT-ANL-02) and ignores what is not API. The stub itself stands in for the real package.
func TestCompareStub(t *testing.T) {
	stub := loadStub(t, stubGOPATH(t))

	tests := []struct {
		name      string
		old, repl string
		want      []string
	}{
		{
			name: "same stub",
			old:  "type TimerID uint64",
			repl: "type TimerID uint64",
		},
		{
			name: "send without result",
			old:  "func Send[M any](from Sender, to PID[M], msg M) error {",
			repl: "func Send[M any](from Sender, to PID[M], msg M) {",
			want: []string{"stub entry Send differs:\n" +
				"  lockstep: func lockstep.Send[M any](from lockstep.Sender, to lockstep.PID[M], msg M) error\n" +
				"  stub:     func lockstep.Send[M any](from lockstep.Sender, to lockstep.PID[M], msg M)"},
		},
		{
			name: "unexported field added",
			old:  "\tnode string\n",
			repl: "\tnode string\n\textra bool\n",
		},
		{
			name: "exported field added",
			old:  "\tnode string\n",
			repl: "\tnode string\n\tExtra bool\n",
			want: []string{"stub entry Address differs:\n" +
				"  lockstep: type lockstep.Address struct{}\n" +
				"  stub:     type lockstep.Address struct{Extra bool}"},
		},
		{
			name: "type parameter renamed",
			old:  "type Reply[R any] struct{}",
			repl: "type Reply[T any] struct{}",
			want: []string{"stub entry Reply differs:\n" +
				"  lockstep: type lockstep.Reply[R any] struct{}\n" +
				"  stub:     type lockstep.Reply[T any] struct{}"},
		},
		{
			name: "method removed",
			old:  "func (c *Context[M]) Every(d time.Duration, msg M) TimerID { panic(\"stub\") }",
			repl: "",
			want: []string{"stub entry Context.Every differs:\n" +
				"  lockstep: func (*lockstep.Context[M]).Every(d time.Duration, msg M) lockstep.TimerID\n" +
				"  stub:     " + missing},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareStub(stub, mutatedStub(t, tt.old, tt.repl))
			if len(got) != len(tt.want) {
				t.Fatalf("got %d lines, want %d:\n%s", len(got), len(tt.want), strings.Join(got, "\n"))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("line %d:\n got: %s\nwant: %s", i, got[i], tt.want[i])
				}
			}
		})
	}
}
