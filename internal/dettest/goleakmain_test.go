// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"go/ast"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// noGoleakFormat is the line of BEN-205 for a directory whose tests do not run under goleak.
const noGoleakFormat = "dettest: %s has tests but no TestMain calling dettest.Main, or dettest.Prepare and then goleak.VerifyTestMain"

// dettestPkg is the import path of this package.
const dettestPkg = lockstepModule + "/internal/dettest"

// goleakMainViolations returns the lines of BEN-205 for the module at root, one for each directory
// of testDirs whose TestMain is not one of BEN-200's forms, in directory order. A line names the
// slash-separated directory relative to root ("." for root itself). Besides the directories BEN-205
// skips (testdata, hidden directories and nested modules), testDirs skips vendor and directories
// starting with "_": the go command builds no package of the module there.
func goleakMainViolations(root string) ([]string, error) {
	infos, err := testDirs(root)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, info := range infos {
		if !info.runsGoleak() {
			lines = append(lines, strings.Replace(noGoleakFormat, "%s", info.dir, 1))
		}
	}
	return lines, nil
}

// runsGoleak reports whether the directory has exactly one TestMain, declared as
// func TestMain(m *testing.M), whose body is one of BEN-200's forms (BEN-205): exactly the
// statement dettest.Main(m), or exactly dettest.Prepare() and then goleak.VerifyTestMain(...).
// The package names are the file's import names of their paths; in package dettest the calls of
// Main and Prepare are unqualified.
func (info *testMainInfo) runsGoleak() bool {
	if len(info.mains) != 1 {
		return false
	}
	fn, f := info.mains[0], info.mainFiles[0]
	m := mParam(fn, importName(f, "testing"))
	if m == "" || fn.Body == nil {
		return false
	}
	dettest := "" // the calls are unqualified inside package dettest
	if f.Name.Name != "dettest" {
		if dettest = importName(f, dettestPkg); dettest == "" {
			return false
		}
	}
	switch body := fn.Body.List; len(body) {
	case 1:
		call := callStmt(body[0], dettest, "Main")
		if call == nil || len(call.Args) != 1 {
			return false
		}
		arg, ok := call.Args[0].(*ast.Ident)
		return ok && arg.Name == m
	case 2:
		goleak := importName(f, goleakMod)
		return callStmt(body[0], dettest, "Prepare") != nil &&
			goleak != "" && callStmt(body[1], goleak, "VerifyTestMain") != nil
	default:
		return false
	}
}

// mParam returns the name of the one parameter of fn when its type is *<testingName>.M, where
// testingName is the file's import name of package testing, and "" otherwise.
func mParam(fn *ast.FuncDecl, testingName string) string {
	params := fn.Type.Params.List
	if testingName == "" || len(params) != 1 || len(params[0].Names) != 1 {
		return ""
	}
	star, ok := params[0].Type.(*ast.StarExpr)
	if !ok {
		return ""
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "M" {
		return ""
	}
	if x, ok := sel.X.(*ast.Ident); !ok || x.Name != testingName {
		return ""
	}
	return params[0].Names[0].Name
}

// callStmt returns the call of stmt when stmt is an expression statement calling pkg.name, or name
// unqualified when pkg is "", and nil otherwise.
func callStmt(stmt ast.Stmt, pkg, name string) *ast.CallExpr {
	es, ok := stmt.(*ast.ExprStmt)
	if !ok {
		return nil
	}
	call, ok := es.X.(*ast.CallExpr)
	if !ok {
		return nil
	}
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		if pkg == "" && fun.Name == name {
			return call
		}
	case *ast.SelectorExpr:
		if x, ok := fun.X.(*ast.Ident); ok && pkg != "" && x.Name == pkg && fun.Sel.Name == name {
			return call
		}
	}
	return nil
}

// importName returns the name f uses for the package with import path path: the import's own name,
// or the last element of path. It returns "" when f does not import path, or imports it as _ or ".".
func importName(f *ast.File, path string) string {
	for _, imp := range f.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err != nil || p != path {
			continue
		}
		if imp.Name == nil {
			return path[strings.LastIndex(path, "/")+1:]
		}
		if name := imp.Name.Name; name != "_" && name != "." {
			return name
		}
		return ""
	}
	return ""
}

// AT-BEN-12 (repository clause), BEN-200, BEN-205
func TestGoleakMain(t *testing.T) {
	lines, err := goleakMainViolations(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) > 0 {
		t.Errorf("packages without the goroutine leak check:\n  %s", strings.Join(lines, "\n  "))
	}
}

// testFiles returns the go.mod and _test.go files of the tree at src by slash-separated path
// relative to src, leaving out hidden directories such as .git, which the walk skips too.
func testFiles(t *testing.T, src string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != src && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if name := d.Name(); name != "go.mod" && !strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path) //nolint:gosec // a test reading the developer's own checkout; no untrusted symlinks
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = string(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// AT-BEN-12, BEN-200, BEN-205
func TestGoleakMainCases(t *testing.T) {
	// line is the failure of BEN-205 for dir.
	line := func(dir string) string {
		return "dettest: " + dir + " has tests but no TestMain calling dettest.Main, or dettest.Prepare and then goleak.VerifyTestMain"
	}
	// file is a test file of package pkg that imports os, testing, dettest and goleak and declares
	// decls.
	file := func(pkg, decls string) string {
		return "package " + pkg + "\n\nimport (\n\t\"os\"\n\t\"testing\"\n\n\t\"" + lockstepModule +
			"/internal/dettest\"\n\t\"" + goleakMod + "\"\n)\n\n" + decls
	}
	// testMain is a test file of package pkg whose TestMain has the given body.
	testMain := func(pkg, body string) string {
		return file(pkg, "func TestMain(m *testing.M) {\n"+body+"}\n")
	}
	const noMain = "package x\n\nimport \"testing\"\n\nfunc TestX(t *testing.T) {}\n"

	t.Run("repository copy", func(t *testing.T) {
		// AT-BEN-12: a package x with a test file and no TestMain added to a copy of the
		// repository is the only one reported.
		files := testFiles(t, repoRoot(t))
		files["x/x_test.go"] = noMain
		root := t.TempDir()
		writeTree(t, root, files)
		got, err := goleakMainViolations(root)
		if err != nil {
			t.Fatal(err)
		}
		want := "dettest: x has tests but no TestMain calling dettest.Main, or dettest.Prepare and then goleak.VerifyTestMain"
		if strings.Join(got, "\n") != want {
			t.Errorf("violations:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), want)
		}
	})

	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{name: "empty module"},
		{
			// AT-BEN-12
			name: "AT-BEN-12",
			files: map[string]string{
				"nomain/a_test.go":      noMain,
				"run/a_test.go":         testMain("run", "\tm.Run()\n"),
				"verify/a_test.go":      testMain("verify", "\tgoleak.VerifyTestMain(m)\n"),
				"verifyfirst/a_test.go": testMain("verifyfirst", "\tgoleak.VerifyTestMain(m)\n\tdettest.Prepare()\n"),
				"mainverify/a_test.go":  testMain("mainverify", "\tdettest.Main(m)\n\tgoleak.VerifyTestMain(m)\n"),
				"main/a_test.go":        testMain("main", "\tdettest.Main(m)\n"),
				"prepare/a_test.go":     testMain("prepare", "\tdettest.Prepare()\n\tgoleak.VerifyTestMain(m)\n"),
				"nested/go.mod":         "module example.com/nested\n",
				"nested/a_test.go":      noMain,
			},
			want: []string{line("mainverify"), line("nomain"), line("run"), line("verify"), line("verifyfirst")},
		},
		{
			name: "goleak options",
			files: map[string]string{
				"ignore/a_test.go": testMain("ignore", "\tdettest.Prepare()\n\tgoleak.VerifyTestMain(m,\n"+
					"\t\t// The reason for the entry.\n\t\tgoleak.IgnoreTopFunction(\"example.com/p.f\"),\n\t)\n"),
				"spread/a_test.go": file("spread", "var opts []goleak.Option\n\n"+
					"func TestMain(m *testing.M) {\n\tdettest.Prepare()\n\tgoleak.VerifyTestMain(m, opts...)\n}\n"),
			},
		},
		{
			name: "os.Exit around m.Run",
			files: map[string]string{
				"exit/a_test.go": testMain("exit", "\tos.Exit(m.Run())\n"),
			},
			want: []string{line("exit")},
		},
		{
			name: "exact statements",
			files: map[string]string{
				"prepmain/a_test.go":  testMain("prepmain", "\tdettest.Prepare()\n\tdettest.Main(m)\n"),
				"prepare/a_test.go":   testMain("prepare", "\tdettest.Prepare()\n"),
				"three/a_test.go":     testMain("three", "\tdettest.Prepare()\n\tos.Setenv(\"A\", \"1\")\n\tgoleak.VerifyTestMain(m)\n"),
				"before/a_test.go":    testMain("before", "\tos.Setenv(\"A\", \"1\")\n\tdettest.Main(m)\n"),
				"inif/a_test.go":      testMain("inif", "\tif true {\n\t\tdettest.Main(m)\n\t}\n"),
				"deferred/a_test.go":  testMain("deferred", "\tdefer dettest.Main(m)\n"),
				"other/a_test.go":     testMain("other", "\tdettest.Prepare()\n\tgoleak.VerifyNone(nil)\n"),
				"argument/a_test.go":  testMain("argument", "\tdettest.Main(nil)\n"),
				"empty/a_test.go":     testMain("empty", ""),
				"semicolon/a_test.go": file("semicolon", "func TestMain(m *testing.M) { dettest.Prepare(); goleak.VerifyTestMain(m) }\n"),
			},
			want: []string{
				line("argument"), line("before"), line("deferred"), line("empty"), line("inif"),
				line("other"), line("prepare"), line("prepmain"), line("three"),
			},
		},
		{
			name: "import names",
			files: map[string]string{
				"alias/a_test.go": "package alias\n\nimport (\n\t\"testing\"\n\n\tdt \"" + lockstepModule +
					"/internal/dettest\"\n)\n\nfunc TestMain(m *testing.M) { dt.Main(m) }\n",
				"leakalias/a_test.go": "package leakalias\n\nimport (\n\t\"testing\"\n\n\t\"" + lockstepModule +
					"/internal/dettest\"\n\tgl \"" + goleakMod + "\"\n)\n\nfunc TestMain(m *testing.M) {\n\tdettest.Prepare()\n\tgl.VerifyTestMain(m)\n}\n",
				"oldname/a_test.go": "package oldname\n\nimport (\n\t\"testing\"\n\n\tdt \"" + lockstepModule +
					"/internal/dettest\"\n)\n\nfunc TestMain(m *testing.M) { dettest.Main(m) }\n",
				"noimport/a_test.go": "package noimport\n\nimport \"testing\"\n\nfunc TestMain(m *testing.M) { dettest.Main(m) }\n",
				"otherleak/a_test.go": "package otherleak\n\nimport (\n\t\"testing\"\n\n\t\"" + lockstepModule +
					"/internal/dettest\"\n\t\"example.com/goleak\"\n)\n\nfunc TestMain(m *testing.M) {\n\tdettest.Prepare()\n\tgoleak.VerifyTestMain(m)\n}\n",
				"dot/a_test.go": "package dot\n\nimport (\n\t\"testing\"\n\n\t. \"" + lockstepModule +
					"/internal/dettest\"\n)\n\nfunc TestMain(m *testing.M) { Main(m) }\n",
				"testingalias/a_test.go": "package testingalias\n\nimport (\n\tt \"testing\"\n\n\t\"" + lockstepModule +
					"/internal/dettest\"\n)\n\nfunc TestMain(m *t.M) { dettest.Main(m) }\n",
			},
			want: []string{line("dot"), line("noimport"), line("oldname"), line("otherleak")},
		},
		{
			name: "unqualified calls inside package dettest",
			files: map[string]string{
				"internal/dettest/main_test.go": "package dettest\n\nimport \"testing\"\n\nfunc TestMain(m *testing.M) { Main(m) }\n",
				"internal/two/a_test.go": "package dettest\n\nimport (\n\t\"testing\"\n\n\t\"" + goleakMod +
					"\"\n)\n\nfunc TestMain(m *testing.M) {\n\tPrepare()\n\tgoleak.VerifyTestMain(m)\n}\n",
				"internal/ext/a_test.go":  "package dettest_test\n\nimport \"testing\"\n\nfunc TestMain(m *testing.M) { Main(m) }\n",
				"internal/qual/a_test.go": "package qual\n\nimport \"testing\"\n\nfunc TestMain(m *testing.M) { Main(m) }\n",
			},
			want: []string{line("internal/ext"), line("internal/qual")},
		},
		{
			name: "the TestMain declaration",
			files: map[string]string{
				"renamed/a_test.go":  file("renamed", "func TestMain(tm *testing.M) { dettest.Main(tm) }\n"),
				"wrongarg/a_test.go": file("wrongarg", "func TestMain(tm *testing.M) { dettest.Main(m) }\n"),
				"external/a_test.go": "package external\n\nimport \"testing\"\n\nfunc TestE(t *testing.T) {}\n",
				"external/b_test.go": testMain("external_test", "\tdettest.Main(m)\n"),
				"two/a_test.go":      testMain("two", "\tdettest.Main(m)\n"),
				"two/b_test.go":      testMain("two_test", "\tdettest.Main(m)\n"),
				"method/a_test.go":   file("method", "type T struct{}\n\nfunc (T) TestMain(m *testing.M) { dettest.Main(m) }\n"),
				"helper/a_test.go":   file("helper", "func helper(m *testing.M) { dettest.Main(m) }\n"),
				"noparam/a_test.go":  file("noparam", "func TestMain() { dettest.Main(nil) }\n"),
				"testingt/a_test.go": file("testingt", "func TestMain(m *testing.T) { dettest.Main(m) }\n"),
				"twoparam/a_test.go": file("twoparam", "func TestMain(m, n *testing.M) { dettest.Main(m) }\n"),
			},
			want: []string{
				line("helper"), line("method"), line("noparam"), line("testingt"), line("two"),
				line("twoparam"), line("wrongarg"),
			},
		},
		{
			name: "skipped directories and files",
			files: map[string]string{
				"testdata/a/a_test.go":  noMain,
				"a/testdata/a_test.go":  noMain,
				".hidden/a_test.go":     noMain,
				"vendor/v/a_test.go":    noMain,
				"_old/a_test.go":        noMain,
				"nested/go.mod":         "module example.com/nested\n",
				"nested/a_test.go":      noMain,
				"nested/sub/a_test.go":  noMain,
				"code/a.go":             "package code\n",
				"code/a_test.go.txt":    noMain,
				"code/sub/a_test.go":    testMain("sub", "\tdettest.Main(m)\n"),
				"code/sub/notparsed.go": "not parsed\n",
			},
		},
		{
			name: "the root package and sorted lines",
			files: map[string]string{
				"root_test.go":  noMain,
				"z/a_test.go":   noMain,
				"a/b/a_test.go": noMain,
				"a-b/a_test.go": noMain,
				"a/a_test.go":   noMain,
			},
			want: []string{line("."), line("a"), line("a-b"), line("a/b"), line("z")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			files := map[string]string{"go.mod": "module example.com/m\n"}
			for name, content := range tc.files {
				files[name] = content
			}
			writeTree(t, root, files)
			got, err := goleakMainViolations(root)
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
		if _, err := goleakMainViolations(root); err == nil {
			t.Error("goleakMainViolations succeeded on a syntax error; want the parse error")
		}
	})
}
