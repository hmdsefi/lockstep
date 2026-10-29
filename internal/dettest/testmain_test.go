// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	// noMainFormat is the line of DET-150 for a directory whose tests run simulations.
	noMainFormat = "%s: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare"
	// noPrepareFormat is the line of DET-155 for a TestMain that calls goleak.VerifyTestMain.
	noPrepareFormat = "%s: TestMain calls goleak.VerifyTestMain without calling dettest.Prepare first"
)

// testMainInfo is what the checks of DET-150, DET-155 and BEN-205 read from the test files of one
// directory.
type testMainInfo struct {
	dir        string // slash-separated, relative to the module root ("." for the root itself)
	importsSim bool
	mains      []*ast.FuncDecl // every func TestMain
	mainFiles  []*ast.File     // the file of each TestMain
}

// testDirs returns, in directory order, what the test files of each directory of the module at
// root declare, for the directories that have test files. It reads the files with go/parser and
// does no type checking, so the imports of the files need not resolve. The walk skips testdata,
// vendor, directories starting with "." or "_" and nested modules (DET-060).
func testDirs(root string) ([]*testMainInfo, error) {
	infos := map[string]*testMainInfo{}
	var dirs []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == root {
				return nil
			}
			if name := d.Name(); name == "testdata" || name == "vendor" || skippedDir(name) {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir // nested module
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		dir := filepath.ToSlash(rel)
		info := infos[dir]
		if info == nil {
			info = &testMainInfo{dir: dir}
			infos[dir] = info
			dirs = append(dirs, dir)
		}
		for _, imp := range f.Imports {
			if p, err := strconv.Unquote(imp.Path.Value); err == nil && p == simPkg {
				info.importsSim = true
			}
		}
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "TestMain" {
				info.mains = append(info.mains, fn)
				info.mainFiles = append(info.mainFiles, f)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(dirs)
	out := make([]*testMainInfo, len(dirs))
	for i, dir := range dirs {
		out[i] = infos[dir]
	}
	return out, nil
}

// testMainViolations returns the lines of DET-150 and DET-155 for the module at root. A line names
// the slash-separated directory relative to root ("." for root itself). Lines come in directory
// order, a directory's DET-150 line first. The directories are those of testDirs.
func testMainViolations(root string) ([]string, error) {
	infos, err := testDirs(root)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, info := range infos {
		if (info.importsSim || info.dir == "sim") && !info.callsMainOrPrepare() {
			lines = append(lines, strings.Replace(noMainFormat, "%s", info.dir, 1))
		}
		if info.callsGoleakWithoutPrepare() {
			lines = append(lines, strings.Replace(noPrepareFormat, "%s", info.dir, 1))
		}
	}
	return lines, nil
}

// callsMainOrPrepare reports whether the directory has exactly one TestMain and its body contains
// a call dettest.Main(m) or dettest.Prepare() (DET-150).
func (info *testMainInfo) callsMainOrPrepare() bool {
	if len(info.mains) != 1 {
		return false
	}
	return info.hasCall(0, "Main") || info.hasCall(0, "Prepare")
}

// callsGoleakWithoutPrepare reports whether a TestMain of the directory calls
// goleak.VerifyTestMain without a call of Prepare in an earlier statement (DET-155).
func (info *testMainInfo) callsGoleakWithoutPrepare() bool {
	for i, fn := range info.mains {
		if fn.Body == nil {
			continue
		}
		verify := -1
		for j, stmt := range fn.Body.List {
			if containsCall(stmt, "goleak", "VerifyTestMain", "") {
				verify = j
				break
			}
		}
		if verify < 0 {
			continue
		}
		prepared := false
		for _, stmt := range fn.Body.List[:verify] {
			if containsCall(stmt, "dettest", "Prepare", info.mainFiles[i].Name.Name) {
				prepared = true
			}
		}
		if !prepared {
			return true
		}
	}
	return false
}

// hasCall reports whether TestMain number i calls dettest.<name>, or <name> unqualified when its
// file is in package dettest.
func (info *testMainInfo) hasCall(i int, name string) bool {
	fn := info.mains[i]
	return fn.Body != nil && containsCall(fn.Body, "dettest", name, info.mainFiles[i].Name.Name)
}

// containsCall reports whether n contains a call pkg.name(...), or name(...) when file is the
// package "dettest" itself (selfPkg is the package name of the file; "" never matches).
func containsCall(n ast.Node, pkg, name, selfPkg string) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return !found
		}
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			if x, ok := fun.X.(*ast.Ident); ok && x.Name == pkg && fun.Sel.Name == name {
				found = true
			}
		case *ast.Ident:
			if pkg == "dettest" && selfPkg == "dettest" && fun.Name == name {
				found = true
			}
		}
		return !found
	})
	return found
}

// AT-DET-13 (repository clause), DET-150, DET-155
func TestSimTestsUseMain(t *testing.T) {
	t.Run("repository", func(t *testing.T) {
		lines, err := testMainViolations(repoRoot(t))
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) > 0 {
			t.Errorf("TestMain rule broken:\n  %s", strings.Join(lines, "\n  "))
		}
	})

	const (
		simImport = "import _ \"" + simPkg + "\"\n"
		mainOnly  = "func TestMain(m *testing.M) { dettest.Main(m) }\n"
	)
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{name: "empty module"},
		{
			// AT-DET-13
			name: "AT-DET-13",
			files: map[string]string{
				"a/a_test.go": "package a\n\n" + simImport,
				"b/b_test.go": "package b\n\n" + simImport + mainOnly,
				"c/c_test.go": "package c\n\nfunc TestMain(m *testing.M) {\n\tdettest.Prepare()\n\tgoleak.VerifyTestMain(m)\n}\n",
				"d/d_test.go": "package d\n\nfunc TestMain(m *testing.M) { goleak.VerifyTestMain(m) }\n",
				"e/e_test.go": "package e\n\nfunc TestMain(m *testing.M) {\n\tgoleak.VerifyTestMain(m)\n\tdettest.Prepare()\n}\n",
				"f/f_test.go": "package f\n\n" + mainOnly,
			},
			want: []string{
				"a: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
				"d: TestMain calls goleak.VerifyTestMain without calling dettest.Prepare first",
				"e: TestMain calls goleak.VerifyTestMain without calling dettest.Prepare first",
			},
		},
		{
			name: "no sim tests need no TestMain",
			files: map[string]string{
				"x/x_test.go":  "package x\n\nfunc TestX(t *testing.T) {}\n",
				"y/y.go":       "package y\n\n" + simImport,
				"y/y_test.go":  "package y\n",
				"z/z_test.go":  "package z\n\nfunc TestMain(m *testing.M) { os.Exit(m.Run()) }\n",
				"w/w_test.go":  "package w_test\n\nimport _ \"example.com/other/sim\"\n",
				"v/v_test.go":  "package v\n\nimport _ \"" + simPkg + "/inner\"\n",
				"top_test.go":  "package top\n",
				"u/u_test.go":  "package u\n\nfunc (T) TestMain(m *testing.M) {}\n\nfunc TestMain2(m *testing.M) {}\n",
				"t/t_test.go":  "package t\n\nfunc TestMain() {}\n",
				"s/s_test.go":  "package s\n\nfunc TestMain(m *testing.M) { dettest.Main(m) }\n",
				"s/s2_test.go": "package s\n",
			},
		},
		{
			name: "sim tests in any test file or package",
			files: map[string]string{
				"p/p_test.go":  "package p\n",
				"p/p2_test.go": "package p_test\n\n" + simImport,
			},
			want: []string{"p: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare"},
		},
		{
			name: "the root package",
			files: map[string]string{
				"root_test.go": "package lockstep_test\n\n" + simImport,
			},
			want: []string{".: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare"},
		},
		{
			name: "sim directory with a test file needs TestMain",
			files: map[string]string{
				"sim/sim.go":        "package sim\n",
				"sim/other_test.go": "package sim\n",
			},
			want: []string{"sim: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare"},
		},
		{
			name: "sim directory with TestMain",
			files: map[string]string{
				"sim/a_test.go": "package sim\n\nfunc TestMain(m *testing.M) { dettest.Prepare(); goleak.VerifyTestMain(m) }\n",
			},
		},
		{
			name: "sim directory without test files",
			files: map[string]string{
				"sim/sim.go": "package sim\n",
			},
		},
		{
			name: "exactly one TestMain",
			files: map[string]string{
				"two/a_test.go":   "package two\n\n" + simImport + mainOnly,
				"two/b_test.go":   "package two_test\n\n" + mainOnly,
				"other/a_test.go": "package other\n\n" + simImport + "func TestMain(m *testing.M) { os.Exit(m.Run()) }\n",
			},
			want: []string{
				"other: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
				"two: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
			},
		},
		{
			name: "call in the body, not elsewhere",
			files: map[string]string{
				"nested/a_test.go":   "package nested\n\n" + simImport + "func TestMain(m *testing.M) {\n\tif true {\n\t\tdettest.Prepare()\n\t}\n\tos.Exit(m.Run())\n}\n",
				"helper/a_test.go":   "package helper\n\n" + simImport + "func TestMain(m *testing.M) { os.Exit(m.Run()) }\n\nfunc helper() { dettest.Main(nil) }\n",
				"wrongpkg/a_test.go": "package wrongpkg\n\n" + simImport + "func TestMain(m *testing.M) { other.Main(m) }\n",
				"unqual/a_test.go":   "package unqual\n\n" + simImport + "func TestMain(m *testing.M) { Main(m) }\n",
			},
			want: []string{
				"helper: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
				"unqual: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
				"wrongpkg: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
			},
		},
		{
			name: "unqualified calls inside package dettest",
			files: map[string]string{
				"internal/dettest/main_test.go": "package dettest\n\n" + simImport + "func TestMain(m *testing.M) { Main(m) }\n",
				"internal/other/a_test.go":      "package dettest\n\nfunc TestMain(m *testing.M) {\n\tPrepare()\n\tgoleak.VerifyTestMain(m)\n}\n",
				"internal/ext/a_test.go":        "package dettest_test\n\nfunc TestMain(m *testing.M) {\n\tPrepare()\n\tgoleak.VerifyTestMain(m)\n}\n",
			},
			want: []string{
				"internal/ext: TestMain calls goleak.VerifyTestMain without calling dettest.Prepare first",
			},
		},
		{
			name: "Prepare in an earlier statement",
			files: map[string]string{
				"ok/a_test.go":       "package ok\n\n" + simImport + "func TestMain(m *testing.M) {\n\tdettest.Prepare()\n\tvar opts []goleak.Option\n\tgoleak.VerifyTestMain(m, opts...)\n}\n",
				"deferred/a_test.go": "package deferred\n\nfunc TestMain(m *testing.M) {\n\tdefer goleak.VerifyTestMain(m)\n\tdettest.Prepare()\n}\n",
				"inside/a_test.go":   "package inside\n\nfunc TestMain(m *testing.M) {\n\tgoleak.VerifyTestMain(m, func() { dettest.Prepare() }())\n}\n",
				"prepmain/a_test.go": "package prepmain\n\nfunc TestMain(m *testing.M) {\n\tdettest.Prepare()\n\tdettest.Main(m)\n}\n",
			},
			want: []string{
				"deferred: TestMain calls goleak.VerifyTestMain without calling dettest.Prepare first",
				"inside: TestMain calls goleak.VerifyTestMain without calling dettest.Prepare first",
			},
		},
		{
			name: "both lines for one directory",
			files: map[string]string{
				"both/a_test.go": "package both\n\n" + simImport + "func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }\n",
			},
			want: []string{
				"both: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
				"both: TestMain calls goleak.VerifyTestMain without calling dettest.Prepare first",
			},
		},
		{
			name: "skipped directories",
			files: map[string]string{
				"testdata/a/a_test.go":  "package a\n\n" + simImport,
				"a/testdata/a_test.go":  "package a\n\n" + simImport,
				"vendor/v/v_test.go":    "package v\n\n" + simImport,
				".hidden/h_test.go":     "package h\n\n" + simImport,
				"_old/o_test.go":        "package o\n\n" + simImport,
				"nested/go.mod":         "module example.com/nested\n",
				"nested/n_test.go":      "package n\n\n" + simImport,
				"nested/sub/s_test.go":  "package s\n\n" + simImport,
				"notgo/a_test.go.txt":   "package a\n\n" + simImport,
				"real/a_test.go":        "package real\n\n" + simImport,
				"real/sub/ok_test.go":   "package sub\n\n" + simImport + mainOnly,
				"real/sub/skip.go":      "not parsed\n",
				"real/sub/skip.go.test": "not parsed\n",
			},
			want: []string{"real: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare"},
		},
		{
			name: "lines are sorted by directory",
			files: map[string]string{
				"z/a_test.go":   "package z\n\n" + simImport,
				"a/b/a_test.go": "package b\n\n" + simImport,
				"a-b/a_test.go": "package ab\n\n" + simImport,
				"a/a_test.go":   "package a\n\n" + simImport,
			},
			want: []string{
				"a: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
				"a-b: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
				"a/b: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
				"z: tests run simulations but TestMain does not call dettest.Main or dettest.Prepare",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			files := map[string]string{"go.mod": "module example.com/m\n"}
			for name, content := range tc.files {
				if content != "" {
					files[name] = content
				}
			}
			writeTree(t, root, files)
			got, err := testMainViolations(root)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Errorf("violations:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
			}
		})
	}

	t.Run("syntax error", func(t *testing.T) {
		root := t.TempDir()
		writeTree(t, root, map[string]string{"go.mod": "module example.com/m\n", "a/a_test.go": "package a\n\nfunc {\n"})
		if _, err := testMainViolations(root); err == nil {
			t.Error("testMainViolations succeeded on a syntax error; want the parse error")
		}
	})
}
