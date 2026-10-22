// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	lockstepModule = "github.com/hmdsefi/lockstep"
	faultlineMod   = "github.com/hmdsefi/faultline"
	gographMod     = "github.com/hmdsefi/gograph"
	goleakMod      = "go.uber.org/goleak"

	// pinnedFaultline is the faultline version of the root go.mod (REP-135). Upgrading faultline
	// changes this constant, go.mod, go.sum and REP-010 in one change.
	pinnedFaultline = "v0.1.2"
	// faultlineGograph is the gograph version the pinned faultline requires (REP-135).
	faultlineGograph = "v0.8.2"
	// pinnedGoleak is the goleak version of the root go.mod (REP-010).
	pinnedGoleak = "v1.3.0"

	goVersion = "1.26.0"
)

// repoRoot returns the root of the lockstep module that contains the test's directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := FindRoot(".", lockstepModule)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// goModEdit is the part of `go mod edit -json` that REP-135 reads.
type goModEdit struct {
	Module    struct{ Path string }
	Go        string
	Toolchain string
	Require   []struct{ Path, Version string }
	Exclude   []struct{ Path, Version string }
	Replace   []struct {
		Old struct{ Path, Version string }
		New struct{ Path, Version string }
	}
	Retract []struct{ Low, High string }
}

// readGoMod runs `go mod edit -json` on the go.mod at path.
func readGoMod(t *testing.T, path string) goModEdit {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(goBin, "mod", "edit", "-json", path)
	cmd.Env = os.Environ()
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go mod edit -json %s: %v", path, err)
	}
	var m goModEdit
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("go mod edit -json %s: %v", path, err)
	}
	return m
}

func modRef(path, version string) string {
	if version == "" {
		return path
	}
	return path + "@" + version
}

// goModViolations returns one line per way in which the go.mod at path breaks REP-010.
func goModViolations(t *testing.T, path string) []string {
	t.Helper()
	m := readGoMod(t, path)
	var lines []string
	if m.Module.Path != lockstepModule {
		lines = append(lines, fmt.Sprintf("go.mod: module %s, want %s", m.Module.Path, lockstepModule))
	}
	if m.Go != goVersion {
		got := m.Go
		if got == "" {
			got = "(none)"
		}
		lines = append(lines, fmt.Sprintf("go.mod: go %s, want %s", got, goVersion))
	}
	if m.Toolchain != "" {
		lines = append(lines, fmt.Sprintf("go.mod: toolchain %s is not allowed", m.Toolchain))
	}
	for _, r := range m.Replace {
		lines = append(lines, fmt.Sprintf("go.mod: replace %s => %s is not allowed",
			modRef(r.Old.Path, r.Old.Version), modRef(r.New.Path, r.New.Version)))
	}
	for _, e := range m.Exclude {
		lines = append(lines, fmt.Sprintf("go.mod: exclude %s %s is not allowed", e.Path, e.Version))
	}
	for _, r := range m.Retract {
		lines = append(lines, fmt.Sprintf("go.mod: retract [%s, %s] is not allowed", r.Low, r.High))
	}

	// The root module requires faultline, gograph and goleak only, each at the version REP-010
	// pins (goleak is there for dettest.Main).
	want := map[string]string{faultlineMod: pinnedFaultline, gographMod: faultlineGograph, goleakMod: pinnedGoleak}
	seen := map[string]bool{}
	for _, r := range m.Require {
		seen[r.Path] = true
		v, ok := want[r.Path]
		switch {
		case !ok:
			lines = append(lines, fmt.Sprintf(
				"go.mod: requires %s %s; the root module may not require it (move the code into a nested module)",
				r.Path, r.Version))
		case r.Version != v:
			lines = append(lines, fmt.Sprintf("go.mod: requires %s %s, want %s", r.Path, r.Version, v))
		}
	}
	for _, p := range []string{faultlineMod, gographMod, goleakMod} {
		if !seen[p] {
			lines = append(lines, fmt.Sprintf("go.mod: missing requirement %s %s", p, want[p]))
		}
	}
	return lines
}

func TestGoMod(t *testing.T) {
	root := repoRoot(t)
	gomod := filepath.Join(root, "go.mod")

	if lines := goModViolations(t, gomod); len(lines) > 0 {
		t.Errorf("go.mod breaks REP-010:\n  %s", strings.Join(lines, "\n  "))
	}

	// Temp copies (AT-REP-01): each change to the real go.mod yields the line described.
	base, err := os.ReadFile(gomod)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(string) string
		want   []string // substrings of one reported line
	}{
		{
			name:   "extra requirement",
			change: func(s string) string { return s + "\nrequire golang.org/x/tools v0.30.0\n" },
			want:   []string{"golang.org/x/tools"},
		},
		{
			name:   "goleak removed",
			change: func(s string) string { return dropRequire(s, goleakMod) },
			want:   []string{"missing", goleakMod, pinnedGoleak},
		},
		{
			name: "goleak at another version",
			change: func(s string) string {
				return strings.Replace(s, goleakMod+" "+pinnedGoleak, goleakMod+" v1.2.0", 1)
			},
			want: []string{"requires " + goleakMod + " v1.2.0, want " + pinnedGoleak},
		},
		{
			name: "faultline at another version",
			change: func(s string) string {
				return strings.Replace(s, faultlineMod+" "+pinnedFaultline, faultlineMod+" v0.1.1", 1)
			},
			want: []string{"v0.1.1", pinnedFaultline},
		},
		{
			name: "gograph at another version",
			change: func(s string) string {
				return strings.Replace(s, gographMod+" "+faultlineGograph, gographMod+" v0.8.1", 1)
			},
			want: []string{"v0.8.1", faultlineGograph},
		},
		{
			name:   "toolchain directive",
			change: func(s string) string { return s + "\ntoolchain go1.27.1\n" },
			want:   []string{"toolchain go1.27.1"},
		},
		{
			name: "replace directive",
			change: func(s string) string {
				return s + "\nreplace " + faultlineMod + " => ../faultline\n"
			},
			want: []string{"replace", faultlineMod, "../faultline"},
		},
		{
			name:   "exclude directive",
			change: func(s string) string { return s + "\nexclude " + faultlineMod + " v0.1.0\n" },
			want:   []string{"exclude", faultlineMod, "v0.1.0"},
		},
		{
			name:   "retract directive",
			change: func(s string) string { return s + "\nretract v0.0.1\n" },
			want:   []string{"retract", "v0.0.1"},
		},
		{
			name:   "other go version",
			change: func(s string) string { return strings.Replace(s, "go "+goVersion, "go 1.25.0", 1) },
			want:   []string{"go 1.25.0", goVersion},
		},
		{
			name: "other module path",
			change: func(s string) string {
				return strings.Replace(s, "module "+lockstepModule, "module example.com/lockstep", 1)
			},
			want: []string{"example.com/lockstep", lockstepModule},
		},
		{
			name:   "faultline removed",
			change: func(s string) string { return dropRequire(s, faultlineMod) },
			want:   []string{"missing", faultlineMod},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "go.mod")
			if err := os.WriteFile(path, []byte(tc.change(string(base))), 0o600); err != nil {
				t.Fatal(err)
			}
			lines := goModViolations(t, path)
			for _, l := range lines {
				if containsAll(l, tc.want) {
					return
				}
			}
			t.Errorf("no line contains all of %q; got:\n  %s", tc.want, strings.Join(lines, "\n  "))
		})
	}
}

// dropRequire removes the requirement of module from a go.mod text, in a require block or alone.
func dropRequire(gomod, module string) string {
	re := regexp.MustCompile(`(?m)^(require\s+|\s+)` + regexp.QuoteMeta(module) + ` .*\n`)
	return re.ReplaceAllString(gomod, "")
}

func containsAll(s string, subs []string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

var copyrightLine = regexp.MustCompile(`^// Copyright 20[0-9]{2} Hamed Yousefi$`)

const spdxLine = "// SPDX-License-Identifier: MPL-2.0"

// hasLicenseHeader reports whether data starts with the two header lines and a blank line (REP-060).
func hasLicenseHeader(data []byte) bool {
	lines := strings.SplitN(string(data), "\n", 4)
	return len(lines) >= 3 &&
		copyrightLine.MatchString(lines[0]) &&
		lines[1] == spdxLine &&
		lines[2] == ""
}

// headerViolations returns the slash-separated paths, relative to root and in walk order, of the
// .go files that do not start with the license header. It skips directories named testdata and
// directories whose name starts with "." or "_" (REP-060).
func headerViolations(root string) ([]string, error) {
	var bad []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		data, err := os.ReadFile(path) //nolint:gosec // a test reading the developer's own checkout; no untrusted symlinks
		if err != nil {
			return err
		}
		if !hasLicenseHeader(data) {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			bad = append(bad, filepath.ToSlash(rel))
		}
		return nil
	})
	return bad, err
}

// headerMessage is the failure text of REP-060.
func headerMessage(bad []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d file(s) do not start with the license header:\n", len(bad))
	for _, p := range bad {
		fmt.Fprintf(&b, "  %s\n", p)
	}
	b.WriteString("start each one with these two lines and a blank line:\n")
	b.WriteString("  // Copyright 2026 Hamed Yousefi\n")
	b.WriteString("  // SPDX-License-Identifier: MPL-2.0")
	return b.String()
}

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLicenseHeaders(t *testing.T) {
	t.Run("repository", func(t *testing.T) {
		bad, err := headerViolations(repoRoot(t))
		if err != nil {
			t.Fatal(err)
		}
		if len(bad) > 0 {
			t.Error(headerMessage(bad))
		}
	})

	t.Run("temp tree", func(t *testing.T) {
		const header = "// Copyright 2026 Hamed Yousefi\n// SPDX-License-Identifier: MPL-2.0\n"
		root := t.TempDir()
		writeTree(t, root, map[string]string{
			"a.go": header + "\npackage a\n",
			"b.go": "package b\n",
			"c.go": "// SPDX-License-Identifier: MPL-2.0\n// Copyright 2026 Hamed Yousefi\n\npackage c\n",
			"d.go": "// Copyright 2026 Someone Else\n// SPDX-License-Identifier: MPL-2.0\n\npackage d\n",
			"e.go": header + "package e\n",
			// Not Go files, and directories the walk skips.
			"notes.txt":           "no header\n",
			"testdata/x.go":       "package x\n",
			"sub/testdata/q.go":   "package q\n",
			".hidden/y.go":        "package y\n",
			"_old/z.go":           "package z\n",
			"sub/ok.go":           header + "\npackage ok\n",
			"sub/.cache/nope.go":  "package nope\n",
			"sub/_draft/nope2.go": "package nope2\n",
		})
		bad, err := headerViolations(root)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := strings.Join(bad, ","), "b.go,c.go,d.go,e.go"; got != want {
			t.Fatalf("violations = %s, want %s", got, want)
		}
		want := "4 file(s) do not start with the license header:\n" +
			"  b.go\n  c.go\n  d.go\n  e.go\n" +
			"start each one with these two lines and a blank line:\n" +
			"  // Copyright 2026 Hamed Yousefi\n" +
			"  // SPDX-License-Identifier: MPL-2.0"
		if got := headerMessage(bad); got != want {
			t.Errorf("message:\n%s\nwant:\n%s", got, want)
		}
	})

	t.Run("copyright year", func(t *testing.T) {
		root := t.TempDir()
		writeTree(t, root, map[string]string{
			"later.go": "// Copyright 2031 Hamed Yousefi\n// SPDX-License-Identifier: MPL-2.0\n\npackage later\n",
			"bad.go":   "// Copyright 203 Hamed Yousefi\n// SPDX-License-Identifier: MPL-2.0\n\npackage bad\n",
		})
		bad, err := headerViolations(root)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := strings.Join(bad, ","), "bad.go"; got != want {
			t.Errorf("violations = %s, want %s", got, want)
		}
	})
}

func TestFindRoot(t *testing.T) {
	t.Run("from this package", func(t *testing.T) {
		got, err := FindRoot(".", lockstepModule)
		if err != nil {
			t.Fatal(err)
		}
		want, err := filepath.Abs(filepath.Join("..", ".."))
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("FindRoot = %s, want %s", got, want)
		}
	})

	t.Run("nearest matching go.mod", func(t *testing.T) {
		root := t.TempDir()
		writeTree(t, root, map[string]string{
			"go.mod":              "module example.com/outer // comment\n\ngo 1.26.0\n",
			"inner/go.mod":        "module example.com/inner\n",
			"inner/pkg/x/keep.go": "package x\n",
		})
		start := filepath.Join(root, "inner", "pkg", "x")
		got, err := FindRoot(start, "example.com/outer")
		if err != nil {
			t.Fatal(err)
		}
		if got != root {
			t.Errorf("FindRoot = %s, want %s", got, root)
		}
		got, err = FindRoot(start, "example.com/inner")
		if err != nil {
			t.Fatal(err)
		}
		if want := filepath.Join(root, "inner"); got != want {
			t.Errorf("FindRoot = %s, want %s", got, want)
		}
	})

	t.Run("unreadable go.mod", func(t *testing.T) {
		dir := t.TempDir()
		// A directory named go.mod cannot be read; the error is not "does not exist".
		if err := os.Mkdir(filepath.Join(dir, "go.mod"), 0o750); err != nil {
			t.Fatal(err)
		}
		if _, err := FindRoot(dir, "example.com/any"); err == nil {
			t.Fatal("FindRoot succeeded; want the read error")
		} else if strings.Contains(err.Error(), "no go.mod for module") {
			t.Errorf("error = %q; want the read error, not the not-found error", err)
		}
	})

	t.Run("no matching go.mod", func(t *testing.T) {
		dir := t.TempDir()
		writeTree(t, dir, map[string]string{"go.mod": "module example.com/other\n"})
		_, err := FindRoot(dir, "example.com/missing")
		if err == nil {
			t.Fatal("FindRoot succeeded; want an error")
		}
		want := "dettest: no go.mod for module example.com/missing above " + dir
		if err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
	})
}

func TestModulePath(t *testing.T) {
	tests := []struct {
		name, gomod, want string
	}{
		{"plain", "module example.com/a\n\ngo 1.26.0\n", "example.com/a"},
		{"trailing comment", "module example.com/a // note\n", "example.com/a"},
		{"quoted", "module \"example.com/q\"\n", "example.com/q"},
		{"tab separated", "module\texample.com/t\n", "example.com/t"},
		{"after other lines", "// header\n\ngo 1.26.0\nmodule example.com/late\n", "example.com/late"},
		{"longer keyword is not module", "modulefoo example.com/x\n", ""},
		{"bare keyword", "module\n", ""},
		{"no module line", "go 1.26.0\n", ""},
		{"empty", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := modulePath(tc.gomod); got != tc.want {
				t.Errorf("modulePath(%q) = %q, want %q", tc.gomod, got, tc.want)
			}
		})
	}
}

// skippedDir reports whether the REP-130 walk does not enter the directory called name.
func skippedDir(name string) bool {
	return strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
}

// underTestdata reports whether the slash-separated path has a directory named testdata.
func underTestdata(rel string) bool {
	return slices.Contains(strings.Split(rel, "/"), "testdata")
}

// nestedModuleViolations returns one REP-130 line per way in which a nested module below root
// breaks the rules, and one line per go.mod under a testdata directory. Lines come in walk order
// (filepath.WalkDir, lexical), each module's lines in check order: module path, go version,
// replace, then imports. Paths in the lines are slash-separated and relative to root. It skips
// directories whose name starts with "." or "_". It stops the test if it cannot read a file or
// run go.
func nestedModuleViolations(t *testing.T, root string) []string {
	t.Helper()
	var lines []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && skippedDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() != "go.mod" || filepath.Dir(path) == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if underTestdata(rel) {
			lines = append(lines, rel+": go.mod under testdata")
			return nil
		}
		dir := filepath.ToSlash(filepath.Dir(filepath.FromSlash(rel)))
		lines = append(lines, nestedGoModViolations(t, root, dir)...)
		lines = append(lines, nestedImportViolations(t, root, dir)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return lines
}

// nestedGoModViolations checks items 1 and 2 of REP-130 for the module in dir, which is
// slash-separated and relative to root.
func nestedGoModViolations(t *testing.T, root, dir string) []string {
	t.Helper()
	m := readGoMod(t, filepath.Join(root, filepath.FromSlash(dir), "go.mod"))
	file := dir + "/go.mod"
	var lines []string
	if want := lockstepModule + "/" + dir; m.Module.Path != want {
		lines = append(lines, fmt.Sprintf("%s: module %s, want %s", file, m.Module.Path, want))
	}
	if m.Go != goVersion {
		got := m.Go
		if got == "" {
			got = "(none)"
		}
		lines = append(lines, fmt.Sprintf("%s: go %s, want %s", file, got, goVersion))
	}
	back, err := filepath.Rel(filepath.Join(root, filepath.FromSlash(dir)), root)
	if err != nil {
		t.Fatal(err)
	}
	back = filepath.ToSlash(back)
	replaced := false
	for _, r := range m.Replace {
		if r.Old.Path == lockstepModule && r.Old.Version == "" && r.New.Path == back && r.New.Version == "" {
			replaced = true
		}
	}
	if !replaced {
		lines = append(lines, fmt.Sprintf("%s: requires %s without replace => %s", file, lockstepModule, back))
	}
	return lines
}

// nestedImportViolations checks item 3 of REP-130 for every .go file of the module in dir, test
// files included. The walk skips testdata, directories starting with "." or "_" and the
// directories of other modules.
func nestedImportViolations(t *testing.T, root, dir string) []string {
	t.Helper()
	internalPath := lockstepModule + "/internal"
	modRoot := filepath.Join(root, filepath.FromSlash(dir))
	var lines []string
	err := filepath.WalkDir(modRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == modRoot {
				return nil
			}
			if skippedDir(d.Name()) || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir // another module
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if p == internalPath || strings.HasPrefix(p, internalPath+"/") {
				lines = append(lines, fmt.Sprintf("%s: imports %s; nested modules use public packages only",
					filepath.ToSlash(rel), p))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return lines
}

// nestedGoMod returns the go.mod of a valid nested module (REP-130 items 1 and 2).
func nestedGoMod(module, goVersion, replace string) string {
	s := "module " + module + "\n\ngo " + goVersion + "\n\n" +
		"require " + lockstepModule + " v0.0.0-00010101000000-000000000000\n"
	if replace != "" {
		s += "\nreplace " + lockstepModule + " => " + replace + "\n"
	}
	return s
}

// validNestedTree is a root module with two nested modules that satisfy REP-130, plus files the
// walk must ignore. Each test case of TestNestedModules changes one entry.
func validNestedTree() map[string]string {
	return map[string]string{
		"go.mod":                "module " + lockstepModule + "\n\ngo " + goVersion + "\n",
		"examples/raft/go.mod":  nestedGoMod(lockstepModule+"/examples/raft", goVersion, "../.."),
		"examples/raft/raft.go": "package raft\n\nimport _ \"" + lockstepModule + "\"\n",
		"analysis/go.mod":       nestedGoMod(lockstepModule+"/analysis", goVersion, ".."),
		"analysis/analysis.go":  "package analysis\n\nimport _ \"" + lockstepModule + "/sim\"\n",
		"internal/x/x.go":       "package x\n\nimport _ \"" + lockstepModule + "/internal/engine\"\n",
		// The nested module's own internal package is not the root's.
		"examples/raft/internal/own/own.go": "package own\n\nimport _ \"" + lockstepModule + "/examples/raft/internal/other\"\n",
		// Directories the walk skips.
		".cache/go.mod":               "not a go.mod\n",
		"_old/go.mod":                 "not a go.mod\n",
		"examples/raft/.hidden/h.go":  "package h\n\nimport _ \"" + lockstepModule + "/internal/engine\"\n",
		"examples/raft/_draft/d.go":   "package d\n\nimport _ \"" + lockstepModule + "/internal/engine\"\n",
		"examples/raft/testdata/f.go": "package f\n\nimport _ \"" + lockstepModule + "/internal/engine\"\n",
	}
}

func TestNestedModules(t *testing.T) {
	t.Run("repository", func(t *testing.T) {
		if lines := nestedModuleViolations(t, repoRoot(t)); len(lines) > 0 {
			t.Errorf("nested modules break REP-130:\n  %s", strings.Join(lines, "\n  "))
		}
	})

	tests := []struct {
		name   string
		change map[string]string // entries added to or replaced in validNestedTree
		want   []string          // the exact violation lines
	}{
		{name: "valid"},
		{
			name:   "module path",
			change: map[string]string{"examples/raft/go.mod": nestedGoMod("example.com/raft", goVersion, "../..")},
			want:   []string{"examples/raft/go.mod: module example.com/raft, want github.com/hmdsefi/lockstep/examples/raft"},
		},
		{
			name:   "go version",
			change: map[string]string{"examples/raft/go.mod": nestedGoMod(lockstepModule+"/examples/raft", "1.25", "../..")},
			want:   []string{"examples/raft/go.mod: go 1.25, want 1.26.0"},
		},
		{
			name:   "no go version",
			change: map[string]string{"examples/raft/go.mod": "module " + lockstepModule + "/examples/raft\n"},
			want: []string{
				"examples/raft/go.mod: go (none), want 1.26.0",
				"examples/raft/go.mod: requires github.com/hmdsefi/lockstep without replace => ../..",
			},
		},
		{
			name:   "dropped replace",
			change: map[string]string{"examples/raft/go.mod": nestedGoMod(lockstepModule+"/examples/raft", goVersion, "")},
			want:   []string{"examples/raft/go.mod: requires github.com/hmdsefi/lockstep without replace => ../.."},
		},
		{
			name:   "replace to the wrong path",
			change: map[string]string{"analysis/go.mod": nestedGoMod(lockstepModule+"/analysis", goVersion, "../..")},
			want:   []string{"analysis/go.mod: requires github.com/hmdsefi/lockstep without replace => .."},
		},
		{
			name: "internal import in a test file",
			change: map[string]string{
				"examples/raft/raft_test.go": "package raft\n\nimport _ \"" + lockstepModule + "/internal/engine\"\n",
			},
			want: []string{"examples/raft/raft_test.go: imports github.com/hmdsefi/lockstep/internal/engine; nested modules use public packages only"},
		},
		{
			name: "internal import in a subdirectory, grouped import",
			change: map[string]string{
				"analysis/sub/s.go": "package sub\n\nimport (\n\t\"fmt\"\n\t_ \"" + lockstepModule + "/internal\"\n)\n",
			},
			want: []string{"analysis/sub/s.go: imports github.com/hmdsefi/lockstep/internal; nested modules use public packages only"},
		},
		{
			name:   "go.mod in testdata",
			change: map[string]string{"internal/x/testdata/m/go.mod": nestedGoMod(lockstepModule+"/m", goVersion, "")},
			want:   []string{"internal/x/testdata/m/go.mod: go.mod under testdata"},
		},
		{
			name:   "go.mod in testdata of a nested module",
			change: map[string]string{"examples/raft/testdata/m/go.mod": "module example.com/m\n"},
			want:   []string{"examples/raft/testdata/m/go.mod: go.mod under testdata"},
		},
		{
			name: "walk order, then check order",
			change: map[string]string{
				"examples/raft/go.mod":       nestedGoMod("example.com/raft", "1.25", "../.."),
				"analysis/a_test.go":         "package analysis\n\nimport _ \"" + lockstepModule + "/internal/engine\"\n",
				"internal/y/testdata/go.mod": "module example.com/y\n",
			},
			want: []string{
				"analysis/a_test.go: imports github.com/hmdsefi/lockstep/internal/engine; nested modules use public packages only",
				"examples/raft/go.mod: module example.com/raft, want github.com/hmdsefi/lockstep/examples/raft",
				"examples/raft/go.mod: go 1.25, want 1.26.0",
				"internal/y/testdata/go.mod: go.mod under testdata",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := validNestedTree()
			for name, content := range tc.change {
				files[name] = content
			}
			root := t.TempDir()
			writeTree(t, root, files)
			got := nestedModuleViolations(t, root)
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Errorf("violations:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
			}
		})
	}
}

const (
	simPkg      = lockstepModule + "/sim"
	vizPkg      = lockstepModule + "/internal/viz"
	lockstepCmd = lockstepModule + "/cmd/lockstep"
)

func TestProductionPackages(t *testing.T) {
	all := []string{
		lockstepModule,
		simPkg,
		lockstepModule + "/sim/inner", // only the three listed packages are tooling
		vizPkg,
		lockstepModule + "/internal/vizzy",
		lockstepCmd,
		lockstepModule + "/cmd/lockstep/sub",
		lockstepModule + "/internal/engine",
	}
	want := []string{
		lockstepModule,
		lockstepModule + "/sim/inner",
		lockstepModule + "/internal/vizzy",
		lockstepModule + "/cmd/lockstep/sub",
		lockstepModule + "/internal/engine",
	}
	got := productionPackages(all)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("productionPackages =\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestSimulationOnlyDeps(t *testing.T) {
	deps := []string{
		"fmt",
		lockstepModule + "/internal/engine",
		gographMod + "/graph",
		faultlineMod + "/kernel",
		faultlineMod,           // the root package of faultline
		gographMod + "/graph",  // listed twice: reported once
		faultlineMod + "extra", // a different module whose path only starts alike
		"example.com/hmdsefi/faultline/kernel",
		gographMod,
	}
	want := []string{
		faultlineMod,
		faultlineMod + "/kernel",
		gographMod,
		gographMod + "/graph",
	}
	got := simulationOnlyDeps(deps)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("simulationOnlyDeps =\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	if got := simulationOnlyDeps([]string{"fmt", lockstepModule}); len(got) != 0 {
		t.Errorf("simulationOnlyDeps with no offender = %q, want none", got)
	}

	wantMsg := "production packages depend on simulation-only packages (architecture §5.1, D5):\n" +
		"  github.com/hmdsefi/faultline\n  github.com/hmdsefi/faultline/kernel\n" +
		"  github.com/hmdsefi/gograph\n  github.com/hmdsefi/gograph/graph"
	if got := depsMessage(want); got != wantMsg {
		t.Errorf("message:\n%s\nwant:\n%s", got, wantMsg)
	}
}

func TestProductionDeps(t *testing.T) {
	root := repoRoot(t)
	pkgs := productionPackages(goList(t, root, "./..."))
	args := append([]string{"-deps"}, pkgs...)
	if bad := simulationOnlyDeps(goList(t, root, args...)); len(bad) > 0 {
		t.Error(depsMessage(bad))
	}
}

// productionPackages returns the packages of all, an import path list from `go list ./...`, that
// are production packages: all but the simulation tooling (REP-110, §3 of the REP spec).
func productionPackages(all []string) []string {
	tooling := []string{simPkg, vizPkg, lockstepCmd}
	var prod []string
	for _, p := range all {
		if !slices.Contains(tooling, p) {
			prod = append(prod, p)
		}
	}
	return prod
}

// simulationOnlyDeps returns, sorted and without duplicates, the paths in deps that belong to
// faultline or gograph (D5).
func simulationOnlyDeps(deps []string) []string {
	var bad []string
	for _, p := range deps {
		for _, mod := range []string{faultlineMod, gographMod} {
			if p == mod || strings.HasPrefix(p, mod+"/") {
				bad = append(bad, p)
			}
		}
	}
	sort.Strings(bad)
	return slices.Compact(bad)
}

// depsMessage is the failure text of REP-110.
func depsMessage(bad []string) string {
	var b strings.Builder
	b.WriteString("production packages depend on simulation-only packages (architecture §5.1, D5):")
	for _, p := range bad {
		b.WriteString("\n  " + p)
	}
	return b.String()
}

// goList runs `go list args...` in dir with the test's environment and returns the lines it prints.
func goList(t *testing.T, dir string, args ...string) []string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(goBin, append([]string{"list"}, args...)...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return strings.Fields(string(out))
}
