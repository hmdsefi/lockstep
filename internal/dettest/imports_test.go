// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"slices"
	"strings"
	"testing"
)

// checkImports runs CheckImports on the module example.com/m at root and returns the findings as
// strings.
func checkImports(t *testing.T, root string, imports map[string]Allow) []string {
	t.Helper()
	fs, err := CheckImports(Module{Root: root, Path: "example.com/m", Imports: imports})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range fs {
		out = append(out, f.String())
	}
	return out
}

// AT-DET-07, DET-090
func TestCheckImports(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod": "module example.com/m\n\ngo 1.26\n",
		"sim/s.go": "package sim\n\nimport (\n" +
			"\t_ \"github.com/hmdsefi/faultline/kernel\"\n" +
			"\t_ \"github.com/hmdsefi/gograph/connectivity\"\n" +
			"\t_ \"github.com/hmdsefi/faultline/kernel/exhaust\"\n)\n",
		"r.go":         "package m\n\nimport (\n\t_ \"example.com/m/sim\"\n\t\"testing\"\n)\n",
		"persist/p.go": "package persist\n\nimport _ \"example.com/m/internal/engine\"\n",
		// A test file is not checked.
		"sim/x_test.go": "package sim\n\nimport _ \"go.uber.org/goleak\"\n",
	})
	imports := map[string]Allow{
		"sim": {Stdlib: true, Testing: []string{"testing"},
			External: []string{"github.com/hmdsefi/faultline/kernel", "github.com/hmdsefi/gograph/..."}},
		"":        {Stdlib: true},
		"persist": {Stdlib: true, Packages: []string{""}},
	}
	fs, err := CheckImports(Module{Root: root, Path: "example.com/m", Imports: imports})
	if err != nil {
		t.Fatal(err)
	}
	want := []Finding{
		{File: "persist/p.go", Line: 3, Col: 10, Rule: RuleImport,
			Msg: `import "example.com/m/internal/engine" is not allowed in package example.com/m/persist`},
		{File: "r.go", Line: 4, Col: 4, Rule: RuleImport,
			Msg: `import "example.com/m/sim" is not allowed in package example.com/m`},
		{File: "r.go", Line: 5, Col: 2, Rule: RuleImport,
			Msg: `import "testing" is not allowed in package example.com/m`},
		{File: "sim/s.go", Line: 6, Col: 4, Rule: RuleImport,
			Msg: `import "github.com/hmdsefi/faultline/kernel/exhaust" is not allowed in package example.com/m/sim`},
	}
	if !slices.Equal(fs, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", findingLines(fs), findingLines(want))
	}
}

// DET-090: the allowed-import decision, import by import.
func TestImportAllowed(t *testing.T) {
	m := Module{Path: "example.com/m"}
	const (
		fl = "github.com/hmdsefi/faultline"
		gg = "github.com/hmdsefi/gograph"
	)
	a := Allow{
		Stdlib:   true,
		Testing:  []string{"testing"},
		External: []string{"go.uber.org/goleak", fl + "/kernel", gg + "/..."},
		Packages: []string{"", "codec", "internal/engine"},
	}
	cases := []struct {
		path string
		want bool
	}{
		{"fmt", true},
		{"net/http", true},
		{"internal/abi", true}, // standard-library internal path: no dot in the first element
		{"testing", true},
		{"testing/synctest", false}, // only the listed testing packages
		{"testing/cryptotest", false},
		{"C", false},
		{"example.com/m", true},
		{"example.com/m/codec", true},
		{"example.com/m/internal/engine", true},
		{"example.com/m/sim", false},
		{"example.com/m/codec/protobuf", false}, // a package below an allowed one is not allowed
		{"example.com/mx", false},               // a module whose path only starts alike
		{"go.uber.org/goleak", true},
		{"go.uber.org/goleak/internal/x", false}, // an entry without "/..." covers one path
		{fl + "/kernel", true},
		{fl + "/kernel/exhaust", false},
		{fl, false},
		{gg, true}, // "q/..." covers q itself
		{gg + "/graph", true},
		{gg + "/connectivity/deep", true},
		{gg + "x", false},        // "q/..." does not cover a path that merely starts with q
		{gg + "-other/x", false}, // nor a sibling module
		{"golang.org/x/tools", false},
		{"github.com/other/pkg", false},
	}
	for _, c := range cases {
		if got := allowed(m, a, c.path); got != c.want {
			t.Errorf("allowed(%q) = %t, want %t", c.path, got, c.want)
		}
	}

	var none Allow
	for _, p := range []string{"fmt", "testing", "example.com/m", "go.uber.org/goleak", gg + "/graph"} {
		if allowed(m, none, p) {
			t.Errorf("an empty rule allows %q", p)
		}
	}
	root := Allow{Stdlib: true, Packages: []string{""}}
	if !allowed(m, root, "example.com/m") {
		t.Error(`Packages [""] does not allow the module's root package`)
	}
	if allowed(m, Allow{Stdlib: true}, "example.com/m") {
		t.Error("the module's root package is allowed without a Packages entry")
	}
}

func TestCheckImportsMissingRule(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod":             "module example.com/m\n\ngo 1.26\n",
		"extra/e.go":         "package extra\n\nimport \"example.com/m/sim\"\n",
		"sim/s.go":           "package sim\n",
		"internal/deps/d.go": "//go:build tools\n\npackage deps\n\nimport _ \"example.com/other\"\n",
		"testdata/t/t.go":    "package t\n\nimport _ \"example.com/other\"\n",
		"sub/go.mod":         "module example.com/sub\n\ngo 1.26\n",
		"sub/s.go":           "package sub\n\nimport _ \"example.com/other\"\n",
		"tonly/x_test.go":    "package tonly\n\nimport _ \"example.com/other\"\n",
	})
	got := checkImports(t, root, map[string]Allow{"sim": {Stdlib: true}})
	want := []string{
		"extra:0:0: DL000: package example.com/m/extra has no import rule; add it to DET §5.2 and internal/dettest",
	}
	if !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestCheckImportsFileSelection(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod": "module example.com/m\n\ngo 1.26\n",
		// Matched by no check context: ignored.
		"p/a_plan9.go": "package p\n\nimport _ \"example.com/bad1\"\n",
		"p/b_tag.go":   "//go:build ignore\n\npackage p\n\nimport _ \"example.com/bad2\"\n",
		// Matched by some check contexts: checked.
		"p/c_linux.go": "package p\n\nimport _ \"example.com/bad3\"\n",
		// A test file: not checked.
		"p/d_test.go": "package p\n\nimport _ \"example.com/bad4\"\n",
		// Not a Go file.
		"p/e.txt": "import \"example.com/bad5\"\n",
	})
	got := checkImports(t, root, map[string]Allow{"p": {Stdlib: true}})
	want := []string{`p/c_linux.go:3:10: DL012: import "example.com/bad3" is not allowed in package example.com/m/p`}
	if !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestCheckImportsSorted(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod":    "module example.com/m\n\ngo 1.26\n",
		"b/b.go":    "package b\n\nimport _ \"example.com/z\"\nimport _ \"example.com/y\"\n",
		"a/z.go":    "package a\n\nimport _ \"example.com/x\"\n",
		"a/a.go":    "package a\n\nimport _ \"example.com/x\"\n",
		"none/n.go": "package none\n",
	})
	rule := Allow{Stdlib: true}
	got := checkImports(t, root, map[string]Allow{"a": rule, "b": rule, "none": rule})
	want := []string{
		`a/a.go:3:10: DL012: import "example.com/x" is not allowed in package example.com/m/a`,
		`a/z.go:3:10: DL012: import "example.com/x" is not allowed in package example.com/m/a`,
		`b/b.go:3:10: DL012: import "example.com/z" is not allowed in package example.com/m/b`,
		`b/b.go:4:10: DL012: import "example.com/y" is not allowed in package example.com/m/b`,
	}
	if !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestCheckImportsErrors(t *testing.T) {
	t.Run("syntax error", func(t *testing.T) {
		root := t.TempDir()
		writeTree(t, root, map[string]string{
			"go.mod": "module example.com/m\n\ngo 1.26\n",
			"p/p.go": "package p\n\nimport (\n",
		})
		_, err := CheckImports(Module{Root: root, Path: "example.com/m", Imports: map[string]Allow{"p": {Stdlib: true}}})
		if err == nil || !strings.HasPrefix(err.Error(), "dettest: imports of example.com/m/p: ") {
			t.Fatalf("error %v, want prefix %q", err, "dettest: imports of example.com/m/p: ")
		}
	})
	t.Run("missing root", func(t *testing.T) {
		if _, err := CheckImports(Module{Root: t.TempDir() + "/nope", Path: "example.com/m"}); err == nil {
			t.Fatal("no error for a missing module root")
		}
	})
}

// AT-REP-08: the real tree, with an import that the root package must not have.
func TestCheckImportsLockstepRootRule(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod":   "module " + lockstepModule + "\n\ngo 1.26\n",
		"doc.go":   "package lockstep\n\nimport _ \"github.com/hmdsefi/faultline/kernel\"\n",
		"sim/s.go": "package sim\n\nimport _ \"github.com/hmdsefi/faultline/kernel/simnet\"\n",
	})
	fs, err := CheckImports(Lockstep(root))
	if err != nil {
		t.Fatal(err)
	}
	want := []Finding{{File: "doc.go", Line: 3, Col: 10, Rule: RuleImport,
		Msg: `import "github.com/hmdsefi/faultline/kernel" is not allowed in package github.com/hmdsefi/lockstep`}}
	if !slices.Equal(fs, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", findingLines(fs), findingLines(want))
	}
}
