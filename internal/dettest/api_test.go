// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// update makes TestAPISnapshot rewrite api/lockstep.txt (DET-120). TestGolden shares the flag and
// rewrites testdata/hashes.txt with it; -run selects which one runs.
var update = flag.Bool("update", false, "rewrite api/lockstep.txt (TestAPISnapshot) or testdata/hashes.txt (TestGolden)")

// outOfDate is the failure header of DET-120.
const outOfDate = "api/lockstep.txt is out of date (run: go test ./internal/dettest -run TestAPISnapshot -update)"

// snapshotDiff returns "" if the snapshot file content equals generated, otherwise the failure
// message of DET-120: the header, then the lines only in the file ("-") and the lines only in the
// generated text ("+"), each group sorted.
func snapshotDiff(file, generated string) string {
	split := func(s string) []string {
		var out []string
		for _, l := range strings.Split(s, "\n") {
			if l != "" {
				out = append(out, l)
			}
		}
		return out
	}
	have, want := split(file), split(generated)
	var removed, added []string
	for _, l := range have {
		if !slices.Contains(want, l) {
			removed = append(removed, "-"+l)
		}
	}
	for _, l := range want {
		if !slices.Contains(have, l) {
			added = append(added, "+"+l)
		}
	}
	if len(removed) == 0 && len(added) == 0 && file == generated {
		return ""
	}
	slices.Sort(removed)
	slices.Sort(added)
	lines := append([]string{outOfDate}, removed...)
	return strings.Join(append(lines, added...), "\n")
}

// checkSnapshot fails t if the snapshot file at path differs from generated (DET-120); with write
// it creates the directory and writes the file instead.
func checkSnapshot(t testing.TB, path, generated string, write bool) {
	t.Helper()
	if write {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(generated), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if msg := snapshotDiff(string(data), generated); msg != "" {
		t.Error(msg)
	}
}

// errorRecorder records the errors that checkSnapshot reports.
type errorRecorder struct {
	testing.TB
	errs []string
}

func (r *errorRecorder) Helper()           {}
func (r *errorRecorder) Error(args ...any) { r.errs = append(r.errs, fmt.Sprint(args...)) }

// apiTestdata returns the module root of the API fixtures.
func apiTestdata(t *testing.T) Module {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("testdata", "api"))
	if err != nil {
		t.Fatal(err)
	}
	return Module{Root: root, Path: "example.com"}
}

// AT-DET-09 (faultline AT-DET-08): the fixture fix, a package that is not there, and DET-110's
// faultline lines for constants, variables, functions, methods, structs, interfaces and aliases.
func TestAPIFix(t *testing.T) {
	got, err := API(apiTestdata(t), []string{"fix", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	const want = `pkg example.com/fix, const A = 1
pkg example.com/fix, const A ideal-int
pkg example.com/fix, const B = 2
pkg example.com/fix, const B time.Duration
pkg example.com/fix, const KX = "x"
pkg example.com/fix, const KX K
pkg example.com/fix, func G(int, int, ...string) *S
pkg example.com/fix, method (*S) Ptr(time.Duration)
pkg example.com/fix, method (S) Val() int
pkg example.com/fix, type Al = int
pkg example.com/fix, type F func(...string) bool
pkg example.com/fix, type I interface { M, unexported methods }
pkg example.com/fix, type I interface, M(int) (string, error)
pkg example.com/fix, type K string
pkg example.com/fix, type S struct
pkg example.com/fix, type S struct, X int
pkg example.com/fix, type S struct, embedded time.Time
pkg example.com/fix, var V error
`
	if got != want {
		t.Errorf("API:\n%s\nwant:\n%s", got, want)
	}
}

// AT-DET-09, DET-110: generic functions, generic types, methods of generic types, constraint
// interfaces and the empty interface.
func TestAPIGeneric(t *testing.T) {
	got, err := API(apiTestdata(t), []string{"generic"})
	if err != nil {
		t.Fatal(err)
	}
	const want = `pkg example.com/generic, func Map[T any, U any]([]T, func(T) U) []U
pkg example.com/generic, func Sum[N Num](...N) N
pkg example.com/generic, method (*Box[X]) Get() X
pkg example.com/generic, type Box[T any] struct
pkg example.com/generic, type Box[T any] struct, V T
pkg example.com/generic, type Empty interface {}
pkg example.com/generic, type Getter[T any] interface { Get }
pkg example.com/generic, type Getter[T any] interface, Get() T
pkg example.com/generic, type Num interface{~int | ~int64}
`
	if got != want {
		t.Errorf("API:\n%s\nwant:\n%s", got, want)
	}
}

// DET-110: receiver type parameter names (renamed, blank, value receiver), a constraint interface
// with methods and type terms, comparable, interfaces with unexported methods only, and a generic
// alias.
func TestAPIGenericMore(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{"g/g.go": `package g

type Pair[K comparable, V any] struct{ Key K; Val V }

func (p Pair[A, B]) Swap() (B, A) { return p.Val, p.Key }

func (p *Pair[_, V]) Set(v V) {}

type Stringish interface {
	~string
	String() string
}

type Key interface{ comparable }

type Sealed interface{ seal() }

type hidden interface{ x() }

type Mixed interface {
	hidden
	Do()
}

type List[T any] = []T

func Keys[M ~map[K]V, K comparable, V any](m M) []K { return nil }
`})
	got, err := API(Module{Root: root, Path: "example.com/m"}, []string{"g"})
	if err != nil {
		t.Fatal(err)
	}
	const want = `pkg example.com/m/g, func Keys[M ~map[K]V, K comparable, V any](M) []K
pkg example.com/m/g, method (*Pair[_, V]) Set(V)
pkg example.com/m/g, method (Pair[A, B]) Swap() (B, A)
pkg example.com/m/g, type Key interface{comparable}
pkg example.com/m/g, type List[T any] = []T
pkg example.com/m/g, type Mixed interface { Do, unexported methods }
pkg example.com/m/g, type Mixed interface, Do()
pkg example.com/m/g, type Pair[K comparable, V any] struct
pkg example.com/m/g, type Pair[K comparable, V any] struct, Key K
pkg example.com/m/g, type Pair[K comparable, V any] struct, Val V
pkg example.com/m/g, type Sealed interface { unexported methods }
pkg example.com/m/g, type Stringish interface{String() string; ~string}
`
	if got != want {
		t.Errorf("API:\n%s\nwant:\n%s", got, want)
	}
}

// DET-110: API returns the error of a package path that is not a directory, a bad build line, a
// syntax error and a type error, the last prefixed "dettest: type-checking <P>: ".
func TestAPIErrors(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"notdir":        "",
		"badbuild/b.go": "//go:build (\n\npackage badbuild\n",
		"syntax/b.go":   "package syntax\n\nvar = 1\n",
		"typeerr/b.go":  "package typeerr\n\nvar X int = \"s\"\n",
	})
	for _, tc := range []struct{ pkg, want string }{
		{"notdir", filepath.Join(root, "notdir") + ": "},
		{"badbuild", "b.go: parsing //go:build line: "},
		{"syntax", filepath.Join(root, "syntax", "b.go") + ":3:5: "},
		{"typeerr", "dettest: type-checking example.com/typeerr: "},
	} {
		_, err := API(Module{Root: root, Path: "example.com"}, []string{tc.pkg})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %v, want one containing %q", tc.pkg, err, tc.want)
		}
	}
}

// DET-120: the diff names removed and added lines; a missing file counts as empty; a file that
// differs only in order or newlines fails with the header alone.
func TestSnapshotDiff(t *testing.T) {
	const nowLine = "pkg example.com/m, method (*Sim) Now() Time"
	generated := "pkg example.com/m, const A = 1\n" + nowLine + "\n"
	dropped := strings.Replace(generated, nowLine+"\n", "", 1)
	if msg := snapshotDiff(dropped, generated); !strings.Contains(msg, "\n+"+nowLine) || !strings.HasPrefix(msg, outOfDate) {
		t.Errorf("dropped line: %q", msg)
	}
	if msg := snapshotDiff(generated+"pkg x, var Y int\n", generated); !strings.Contains(msg, "\n-pkg x, var Y int") {
		t.Errorf("extra line: %q", msg)
	}
	if msg := snapshotDiff(generated, generated); msg != "" {
		t.Errorf("identical: %q", msg)
	}
	if msg := snapshotDiff("", generated); !strings.Contains(msg, "\n+"+nowLine) {
		t.Error("a missing file is not treated as empty")
	}
	if msg, want := snapshotDiff("b\nz\nc\ny\n", "a\nb\nc\nx\n"), outOfDate+"\n-y\n-z\n+a\n+x"; msg != want {
		t.Errorf("removed and added: %q, want %q", msg, want)
	}
	for _, file := range []string{"a\na\nb\n", "b\na\n", "a\nb"} {
		if msg := snapshotDiff(file, "a\nb\n"); msg != outOfDate {
			t.Errorf("file %q: %q, want %q", file, msg, outOfDate)
		}
	}
}

// AT-DET-10: on a temp module, a missing file fails with every line prefixed "+", -update creates
// api/ and writes the file, an equal file passes, and a file with an extra line fails with the
// header and "-pkg x, var Y int".
func TestAPISnapshotWorkflow(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod": "module example.com/m\n\ngo 1.26\n",
		"p/p.go": "package p\n\nconst A = 1\n\nvar V error\n",
	})
	m := Module{Root: root, Path: "example.com/m"}
	generated, err := API(m, []string{"p", "nothere"})
	if err != nil {
		t.Fatal(err)
	}
	const lines = `pkg example.com/m/p, const A = 1
pkg example.com/m/p, const A ideal-int
pkg example.com/m/p, var V error
`
	if generated != lines {
		t.Fatalf("API:\n%s\nwant:\n%s", generated, lines)
	}
	path := filepath.Join(root, "api", "lockstep.txt")

	var tb errorRecorder
	checkSnapshot(&tb, path, generated, false) // the file and api/ are missing
	missing := outOfDate + "\n+pkg example.com/m/p, const A = 1\n+pkg example.com/m/p, const A ideal-int\n+pkg example.com/m/p, var V error"
	if !slices.Equal(tb.errs, []string{missing}) {
		t.Fatalf("missing file: errors %q, want %q", tb.errs, []string{missing})
	}

	tb.errs = nil
	checkSnapshot(&tb, path, generated, true) // -update
	if data, err := os.ReadFile(path); err != nil || string(data) != generated {
		t.Fatalf("written file %q, %v; want %q", data, err, generated)
	}
	checkSnapshot(&tb, path, generated, false) // equal
	if len(tb.errs) != 0 {
		t.Fatalf("equal file: errors %q", tb.errs)
	}

	if err := os.WriteFile(path, []byte(generated+"pkg x, var Y int\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	checkSnapshot(&tb, path, generated, false) // extra line
	if want := outOfDate + "\n-pkg x, var Y int"; !slices.Equal(tb.errs, []string{want}) {
		t.Fatalf("extra line: errors %q, want %q", tb.errs, []string{want})
	}
}

// AT-DET-10, DET-120: the committed api/lockstep.txt equals the exported API of APIPackages. It is
// empty until package lockstep exports a name. Run with -update to rewrite it.
func TestAPISnapshot(t *testing.T) {
	root := repoRoot(t)
	got, err := API(Lockstep(root), APIPackages)
	if err != nil {
		t.Fatal(err)
	}
	checkSnapshot(t, filepath.Join(root, "api", "lockstep.txt"), got, *update)
}
