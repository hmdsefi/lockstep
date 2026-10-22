// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// wantRE matches a fixture expectation: "// want DL001 DL002", "/* want DL000 */", "// want@1 DL011",
// and the class-qualified forms "want(core) …" and "want(runtime) …".
var wantRE = regexp.MustCompile(`^(?://|/\*)\s*want(?:@(\d+))?(?:\((\w+)\))?((?:\s+DL\d{3})+)\s*(?:\*/)?$`)

// fixtureWants returns the expected "file:line rule" pairs of the fixture package in dir for class c.
func fixtureWants(t *testing.T, dir string, c Class) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var wants []string
	fset := token.NewFileSet()
	for _, e := range entries {
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, cg := range f.Comments {
			for _, cm := range cg.List {
				m := wantRE.FindStringSubmatch(cm.Text)
				if m == nil || (m[2] != "" && m[2] != c.String()) {
					continue
				}
				line := fset.Position(cm.Pos()).Line
				if m[1] != "" {
					line, _ = strconv.Atoi(m[1])
				}
				for _, r := range strings.Fields(m[3]) {
					wants = append(wants, fmt.Sprintf("%s:%d %s", e.Name(), line, r))
				}
			}
		}
	}
	slices.Sort(wants)
	return wants
}

// findingLines returns one finding per line, each followed by a newline and an indent.
func findingLines(fs []Finding) string {
	var b strings.Builder
	for _, f := range fs {
		b.WriteString(f.String() + "\n  ")
	}
	return b.String()
}

// fileRules returns "file:line rule" for each finding, sorted.
func fileRules(fs []Finding) []string {
	var out []string
	for _, f := range fs {
		out = append(out, fmt.Sprintf("%s:%d %s", f.File, f.Line, f.Rule))
	}
	slices.Sort(out)
	return out
}

// AT-DET-01, AT-DET-02, AT-DET-21
func TestFixtures(t *testing.T) {
	cases := []struct {
		name    string
		classes []Class
	}{
		{"dl001", []Class{ClassCore}},
		{"dl002", []Class{ClassCore}},
		{"dl003", []Class{ClassCore}},
		{"dl004", []Class{ClassCore}},
		{"dl005", []Class{ClassCore}},
		{"dl006", []Class{ClassCore}},
		{"dl007", []Class{ClassCore}},
		{"dl008", []Class{ClassCore}},
		{"dl009", []Class{ClassCore}},
		{"dl010", []Class{ClassCore}},
		{"dl011", []Class{ClassCore}},
		{"clean", []Class{ClassCore}},
		{"directives", []Class{ClassCore, ClassRuntime}},
	}
	for _, tc := range cases {
		for _, c := range tc.classes {
			t.Run(tc.name+"/"+c.String(), func(t *testing.T) {
				dir := filepath.Join("testdata", "src", tc.name)
				fs, err := LintDir(dir, "fixture/"+tc.name, c)
				if err != nil {
					t.Fatal(err)
				}
				got, want := fileRules(fs), fixtureWants(t, dir, c)
				if !slices.Equal(got, want) {
					t.Errorf("findings:\n  %s\nwant:\n  %s\nall findings:\n  %s",
						strings.Join(got, "\n  "), strings.Join(want, "\n  "), findingLines(fs))
				}
			})
		}
	}

	t.Run("dl001 message", func(t *testing.T) {
		fs, err := LintDir(filepath.Join("testdata", "src", "dl001"), "fixture/dl001", ClassCore)
		if err != nil {
			t.Fatal(err)
		}
		const want = "wall-clock time: time.Now is forbidden; use engine.Clock (ctx.Now, ctx.After in actors)"
		for _, f := range fs {
			if f.File == "a.go" && f.Line == 9 {
				if f.Msg != want {
					t.Errorf("message = %q, want %q", f.Msg, want)
				}
				return
			}
		}
		t.Errorf("no finding at a.go:9 in\n  %s", findingLines(fs))
	})
}

// DET-020: the messages of DL001 to DL011.
func TestMessages(t *testing.T) {
	cases := []struct {
		fixture string
		line    int
		msg     string
	}{
		{"dl001", 9, "wall-clock time: time.Now is forbidden; use engine.Clock (ctx.Now, ctx.After in actors)"},
		{"dl001", 22, "wall-clock time: context.WithTimeout is forbidden; use engine.Clock (ctx.Now, ctx.After in actors)"},
		{"dl002", 9, "global randomness: math/rand/v2.IntN is forbidden; use engine.Entropy (ctx.Rand in actors)"},
		{"dl002", 10, "global randomness: math/rand.Intn is forbidden; use engine.Entropy (ctx.Rand in actors)"},
		{"dl003", 3, "crypto/rand is forbidden in simulation code"},
		{"dl004", 11, "go statement is forbidden in simulation code"},
		{"dl004", 15, "(*sync.WaitGroup).Go starts a goroutine, which is forbidden in simulation code"},
		{"dl004", 18, "context.AfterFunc starts a goroutine, which is forbidden in simulation code"},
		{"dl004", 8, "runtime.SetFinalizer runs a function on a runtime goroutine, which is forbidden in simulation code"},
		{"dl004", 9, "runtime.AddCleanup runs a function on a runtime goroutine, which is forbidden in simulation code"},
		{"dl005", 4, "select statement is forbidden in simulation code"},
		{"dl006", 8, "range over map map[string]int has random order; range over sorted keys or annotate //lockstep:maporder <reason>"},
		{"dl006", 11, "range over map Named has random order; range over sorted keys or annotate //lockstep:maporder <reason>"},
		{"dl007", 6, "floating-point type float64 in decision code; use integers (ppm, nanoseconds)"},
		{"dl008", 9, "environment access: os.Getenv is forbidden in simulation code"},
		{"dl008", 13, "environment access: syscall.Getenv is forbidden in simulation code"},
		{"dl008", 20, "environment access: os.TempDir is forbidden in simulation code"},
		{"dl009", 9, "unordered iteration: (reflect.Value).MapKeys; sort the keys or annotate //lockstep:maporder <reason>"},
		{"dl009", 12, "unordered iteration: (*sync.Map).Range; sort the keys or annotate //lockstep:maporder <reason>"},
		{"dl009", 15, "unordered iteration: maps.DeleteFunc; sort the keys or annotate //lockstep:maporder <reason>"},
		{"dl010", 10, "maps.Keys yields in random order; wrap it in slices.Sorted or annotate //lockstep:maporder <reason>"},
		{"dl011", 1, "platform-specific file x_linux.go in a simulation package"},
	}
	// Lint each fixture once: LintDir type-checks the fixture's imports from source on every call.
	linted := map[string][]Finding{}
	for _, c := range cases {
		fs, ok := linted[c.fixture]
		if !ok {
			var err error
			fs, err = LintDir("testdata/src/"+c.fixture, "fixture/"+c.fixture, ClassCore)
			if err != nil {
				t.Fatal(err)
			}
			linted[c.fixture] = fs
		}
		found := false
		for _, f := range fs {
			if f.Line == c.line && f.Msg == c.msg {
				found = true
			}
		}
		if !found {
			t.Errorf("%s line %d: no finding %q in\n  %s", c.fixture, c.line, c.msg, findingLines(fs))
		}
	}
}

// DET-030: directive messages, and what each class honors.
func TestDirectiveMessages(t *testing.T) {
	const (
		mapRange = "range over map map[string]int has random order; range over sorted keys or annotate //lockstep:maporder <reason>"
		clock    = "wall-clock time: time.Now is forbidden; use engine.Clock (ctx.Now, ctx.After in actors)"
		notCore  = "//lockstep:blocking is not allowed in core packages"
	)
	core := []string{
		"a.go:13:21: DL000: //lockstep:maporder needs a reason",
		"a.go:14:2: DL006: " + mapRange,
		"a.go:18:2: DL006: " + mapRange,
		"a.go:21:19: DL000: unknown directive //lockstep:sorted",
		"a.go:23:19: DL000: //lockstep:maporder suppresses no finding",
		"a.go:28:25: DL000: " + notCore,
		"a.go:29:2: DL005: select statement is forbidden in simulation code",
		"a.go:33:36: DL000: " + notCore,
		"a.go:34:18: DL004: context.AfterFunc starts a goroutine, which is forbidden in simulation code",
		"b.go:7:3: DL006: " + mapRange,
		"b.go:14:19: DL000: //lockstep:maporder suppresses no finding",
		"b.go:15:11: DL001: " + clock,
		"b.go:21:4: DL000: //lockstep:maporder suppresses no finding",
		"b.go:22:2: DL006: " + mapRange,
	}
	// In a runtime package //lockstep:blocking is honored: the select and the AfterFunc call are
	// suppressed and the only differences are in lines 28 to 34.
	runtime := slices.DeleteFunc(slices.Clone(core), func(s string) bool {
		return strings.HasPrefix(s, "a.go:28:") || strings.HasPrefix(s, "a.go:29:") ||
			strings.HasPrefix(s, "a.go:33:") || strings.HasPrefix(s, "a.go:34:")
	})
	for _, tc := range []struct {
		class Class
		want  []string
	}{{ClassCore, core}, {ClassRuntime, runtime}} {
		t.Run(tc.class.String(), func(t *testing.T) {
			fs, err := LintDir("testdata/src/directives", "fixture/directives", tc.class)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, f := range fs {
				got = append(got, f.String())
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
			}
		})
	}
}

// DET-030: a directive that suppresses nothing, and maporder in an engine package.
func TestDirectiveClasses(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod": "module example.com/m\n\ngo 1.26\n",
		"rt/r.go": "package rt\n\n" +
			"func R() {\n\t//lockstep:blocking nothing blocks here\n\t_ = 0\n}\n",
		"eng/e.go": "package eng\n\n" +
			"func E(m map[string]int) {\n\t//lockstep:maporder honored in engine packages\n\tfor range m {\n\t}\n}\n",
		"ex/x.go": "package ex\n\n" +
			"func X(m map[string]int) {\n\t//lockstep:wallclock gone\n\tfor range m {\n\t}\n}\n",
	})
	fs, err := Lint(Module{Root: root, Path: "example.com/m",
		Classes: map[string]Class{"rt": ClassRuntime, "eng": ClassEngine, "ex": ClassExempt}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		// The engine class runs none of DL006, DL009 and DL010, so maporder has nothing to suppress.
		"eng/e.go:4:2: DL000: //lockstep:maporder suppresses no finding",
		"rt/r.go:4:2: DL000: //lockstep:blocking suppresses no finding",
	}
	var got []string
	for _, f := range fs {
		got = append(got, f.String())
	}
	if !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// AT-DET-03
func TestClassRules(t *testing.T) {
	all := []Rule{RuleWallClock, RuleGlobalRand, RuleCryptoRand, RuleGo, RuleSelect, RuleMapRange,
		RuleFloat, RuleEnv, RuleUnordered, RuleMapIter, RulePlatform}
	cases := []struct {
		c    Class
		want []Rule
	}{
		{ClassCore, all},
		{ClassRuntime, all},
		{ClassEngine, []Rule{RuleGlobalRand, RuleEnv, RulePlatform}},
		{ClassExempt, nil},
	}
	for _, c := range cases {
		if got := c.c.Rules(); !slices.Equal(got, c.want) {
			t.Errorf("%v.Rules() = %v, want %v", c.c, got, c.want)
		}
	}
	if got := fmt.Sprint(ClassEngine.Rules()); got != "[DL002 DL008 DL011]" {
		t.Errorf("ClassEngine.Rules() = %s, want [DL002 DL008 DL011]", got)
	}

	names := map[Class]string{ClassCore: "core", ClassRuntime: "runtime", ClassEngine: "engine",
		ClassExempt: "exempt", Class(0): "Class(0)", Class(9): "Class(9)"}
	for c, want := range names {
		if got := c.String(); got != want {
			t.Errorf("Class(%d).String() = %q, want %q", c, got, want)
		}
	}

	fs, err := LintDir("testdata/src/dl004", "fixture/dl004", ClassEngine)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 0 {
		t.Errorf("dl004 as engine: %v", fs)
	}

	asCore, err := LintDir("testdata/src/dl008", "fixture/dl008", ClassCore)
	if err != nil {
		t.Fatal(err)
	}
	asEngine, err := LintDir("testdata/src/dl008", "fixture/dl008", ClassEngine)
	if err != nil {
		t.Fatal(err)
	}
	if len(asCore) == 0 || !slices.Equal(asCore, asEngine) {
		t.Errorf("dl008 as engine:\n  %s\nwant the findings of core:\n  %s", findingLines(asEngine), findingLines(asCore))
	}
}

// AT-DET-04
func TestFindingString(t *testing.T) {
	f := Finding{File: "sim/node.go", Line: 12, Col: 3, Rule: RuleGo, Msg: "go statement is forbidden in simulation code"}
	if got, want := f.String(), "sim/node.go:12:3: DL004: go statement is forbidden in simulation code"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// AT-DET-06
func TestLintUnclassified(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod":                "module example.com/m\n\ngo 1.26\n",
		"sim/s.go":              "package sim\n\nfunc S() int { return 1 }\n",
		"extra/e.go":            "package extra\n\nfunc E() int { return 2 }\n",
		"internal/deps/deps.go": "//go:build tools\n\npackage deps\n",
		"testdata/t/t.go":       "package t\n\nfunc T() { go T() }\n",
		"_old/o.go":             "package old\n\nfunc O() { go O() }\n",
		".git/g/g.go":           "package g\n\nfunc G() { go G() }\n",
		"sub/go.mod":            "module example.com/sub\n\ngo 1.26\n",
		"sub/s.go":              "package sub\n\nfunc S() { go S() }\n",
		"sim/s_test.go":         "package sim\n\nfunc init() { go S() }\n",
		"vendor/v/v.go":         "package v\n\nfunc V() { go V() }\n",
		"tonly/x_test.go":       "package tonly\n",
	})
	fs, err := Lint(Module{Root: root, Path: "example.com/m", Classes: map[string]Class{"sim": ClassCore}})
	if err != nil {
		t.Fatal(err)
	}
	want := Finding{File: "extra", Line: 0, Col: 0, Rule: RuleMeta,
		Msg: "package example.com/m/extra has no determinism class; add it to DET §5.1.3 and internal/dettest"}
	if len(fs) != 1 || fs[0] != want {
		t.Fatalf("findings %v, want [%v]", fs, want)
	}
	dirs, err := packageDirs(root)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dirs, []string{"extra", "sim"}) {
		t.Fatalf("package directories %v", dirs)
	}
}

// AT-DET-20, DET-055
func TestLintFileClasses(t *testing.T) {
	lint := func(t *testing.T, root string, classes, files map[string]Class) []string {
		t.Helper()
		fs, err := Lint(Module{Root: root, Path: "example.com/m", Classes: classes, Files: files})
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, f := range fs {
			out = append(out, fmt.Sprintf("%s:%d %s", f.File, f.Line, f.Rule))
		}
		return out
	}

	t.Run("engine file in a core package", func(t *testing.T) {
		root := t.TempDir()
		writeTree(t, root, map[string]string{
			"go.mod":        "module example.com/m\n\ngo 1.26\n",
			"ov/gate.go":    "package ov\n\nimport \"time\"\n\nvar _ = time.Now()\n",
			"ov/sampler.go": "package ov\n\nimport \"time\"\n\nvar _ = time.Now()\n\nfunc init() { go func() {}() }\n",
		})
		classes := map[string]Class{"ov": ClassCore}
		if got, want := lint(t, root, classes, map[string]Class{"ov/sampler.go": ClassEngine}),
			[]string{"ov/gate.go:5 DL001"}; !slices.Equal(got, want) {
			t.Errorf("with Files: %q, want %q", got, want)
		}
		want := []string{"ov/gate.go:5 DL001", "ov/sampler.go:5 DL001", "ov/sampler.go:7 DL004"}
		if got := lint(t, root, classes, nil); !slices.Equal(got, want) {
			t.Errorf("without Files: %q, want %q", got, want)
		}
		if got := lint(t, root, classes, map[string]Class{"ov/missing.go": ClassExempt}); !slices.Equal(got, want) {
			t.Errorf("missing file in Files: %q, want %q", got, want)
		}
	})

	t.Run("exempt file", func(t *testing.T) {
		root := t.TempDir()
		writeTree(t, root, map[string]string{
			"go.mod":       "module example.com/m\n\ngo 1.26\n",
			"sim/env.go":   "package sim\n\nimport \"os\"\n\nvar _ = os.Getenv(\"X\")\n",
			"sim/other.go": "package sim\n\nimport \"os\"\n\nvar _ = os.Getenv(\"X\")\n",
		})
		got := lint(t, root, map[string]Class{"sim": ClassCore}, map[string]Class{"sim/env.go": ClassExempt})
		if want := []string{"sim/other.go:5 DL008"}; !slices.Equal(got, want) {
			t.Errorf("findings %q, want %q", got, want)
		}
	})

	t.Run("directives take the file's class", func(t *testing.T) {
		root := t.TempDir()
		writeTree(t, root, map[string]string{
			"go.mod": "module example.com/m\n\ngo 1.26\n",
			// A directive in an exempt file is not reported, even though it suppresses nothing.
			"p/exempt.go": "package p\n\nfunc A() {\n\t//lockstep:blocking not reported\n\t_ = 0\n}\n",
			// blocking is honored only in runtime: in the core package it is reported ...
			"p/core.go": "package p\n\nfunc B() {\n\t//lockstep:blocking reported\n\t_ = 0\n}\n",
			// ... and in a file mapped to runtime it is honored and suppresses the select.
			"p/rt.go": "package p\n\nfunc C() {\n\t//lockstep:blocking only BlockSender reaches this\n\tselect {}\n}\n",
		})
		got := lint(t, root, map[string]Class{"p": ClassCore},
			map[string]Class{"p/exempt.go": ClassExempt, "p/rt.go": ClassRuntime})
		if want := []string{"p/core.go:4 DL000"}; !slices.Equal(got, want) {
			t.Errorf("findings %q, want %q", got, want)
		}
	})

	t.Run("floats and platform files", func(t *testing.T) {
		root := t.TempDir()
		writeTree(t, root, map[string]string{
			"go.mod":         "module example.com/m\n\ngo 1.26\n",
			"p/a.go":         "package p\n\nvar F float64\n",
			"p/b.go":         "package p\n\nvar G float64\n",
			"p/c_linux.go":   "package p\n\nvar H int\n",
			"p/d_windows.go": "package p\n\nvar I int\n",
		})
		classes := map[string]Class{"p": ClassCore}
		got := lint(t, root, classes, map[string]Class{"p/b.go": ClassExempt, "p/c_linux.go": ClassExempt})
		if want := []string{"p/a.go:3 DL007", "p/d_windows.go:1 DL011"}; !slices.Equal(got, want) {
			t.Errorf("findings %q, want %q", got, want)
		}
	})
}

// faultline DET-015: Lint returns its findings sorted, across files and directories.
func TestLintSorted(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod":   "module example.com/m\n\ngo 1.26\n",
		"sim/a.go": "package sim\n\nvar F float64\n",
		"sim/b.go": "package sim\n\nfunc G() { go G() }\n",
		"a-b/x.go": "package x\n\nfunc X() { go X() }\n",
		"a/y/y.go": "package y\n\nfunc Y() { go Y() }\n",
	})
	fs, err := Lint(Module{Root: root, Path: "example.com/m",
		Classes: map[string]Class{"sim": ClassCore, "a-b": ClassCore, "a/y": ClassCore}})
	if err != nil {
		t.Fatal(err)
	}
	got := fileRules(fs)
	slices.Sort(got)
	want := []string{"a-b/x.go:3 DL004", "a/y/y.go:3 DL004", "sim/a.go:3 DL007", "sim/b.go:3 DL004"}
	if !slices.Equal(got, want) {
		t.Fatalf("findings %q, want %q", got, want)
	}
	var order []string
	for _, f := range fs {
		order = append(order, f.File)
	}
	if !slices.Equal(order, []string{"a-b/x.go", "a/y/y.go", "sim/a.go", "sim/b.go"}) {
		t.Errorf("findings are not sorted by file: %q", order)
	}
}

// faultline DET-015: a type-checking error is an error, not a finding.
func TestLintTypeError(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod":   "module example.com/m\n\ngo 1.26\n",
		"sim/k.go": "package sim\n\nfunc K() int { return \"x\" }\n",
	})
	_, err := Lint(Module{Root: root, Path: "example.com/m", Classes: map[string]Class{"sim": ClassCore}})
	if err == nil || !strings.Contains(err.Error(), "dettest: type-checking example.com/m/sim") {
		t.Fatalf("err = %v", err)
	}
}
