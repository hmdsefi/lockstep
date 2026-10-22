// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package dettest is lockstep's determinism lint, import-boundary check, exported-API snapshot
// generator, run-twice switch and golden-hash file format. It is a port of faultline's
// internal/detlint (tag v0.1.2); the tests in this package apply it to the root module.
package dettest

import (
	"cmp"
	"errors"
	"fmt"
	"go/build"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Rule identifies a check. IDs and conditions are faultline's (faultline DET-001).
type Rule string

const (
	RuleMeta       Rule = "DL000" // directive problems, unclassified packages
	RuleWallClock  Rule = "DL001" // wall-clock time and real timers
	RuleGlobalRand Rule = "DL002" // package-level math/rand and math/rand/v2 functions
	RuleCryptoRand Rule = "DL003" // import of crypto/rand
	RuleGo         Rule = "DL004" // goroutines
	RuleSelect     Rule = "DL005" // select statement
	RuleMapRange   Rule = "DL006" // range over a map
	RuleFloat      Rule = "DL007" // floating-point and complex types
	RuleEnv        Rule = "DL008" // environment access
	RuleUnordered  Rule = "DL009" // reflect map iteration, sync.Map.Range, maps.DeleteFunc, maps.EqualFunc
	RuleMapIter    Rule = "DL010" // maps.All, maps.Keys, maps.Values
	RulePlatform   Rule = "DL011" // GOOS/GOARCH-specific files
	RuleImport     Rule = "DL012" // import boundary (architecture §5.1)
)

// Class selects the lint rules for a package (DET-040).
type Class uint8

const (
	ClassCore    Class = iota + 1 // simulation path
	ClassRuntime                  // simulation path that external callers can also block in
	ClassEngine                   // production engines
	ClassExempt                   // not linted (import boundaries still apply)
)

// Rules returns the lint rules (DL001–DL011) that apply to c, in ID order.
func (c Class) Rules() []Rule {
	switch c {
	case ClassCore, ClassRuntime:
		return []Rule{RuleWallClock, RuleGlobalRand, RuleCryptoRand, RuleGo, RuleSelect, RuleMapRange,
			RuleFloat, RuleEnv, RuleUnordered, RuleMapIter, RulePlatform}
	case ClassEngine:
		return []Rule{RuleGlobalRand, RuleEnv, RulePlatform}
	}
	return nil
}

// String returns "core", "runtime", "engine", "exempt", or "Class(<n>)".
func (c Class) String() string {
	switch c {
	case ClassCore:
		return "core"
	case ClassRuntime:
		return "runtime"
	case ClassEngine:
		return "engine"
	case ClassExempt:
		return "exempt"
	}
	return "Class(" + strconv.Itoa(int(c)) + ")"
}

// Finding is one violation.
type Finding struct {
	File string // slash-separated path relative to the module root ("" for module-level findings)
	Line int    // 1-based; 0 for directory-level findings
	Col  int    // 1-based; 0 for directory-level findings
	Rule Rule
	Msg  string
}

// String returns "<File>:<Line>:<Col>: <Rule>: <Msg>".
func (f Finding) String() string {
	return fmt.Sprintf("%s:%d:%d: %s: %s", f.File, f.Line, f.Col, f.Rule, f.Msg)
}

// sortFindings sorts by (File, Line, Col, Rule, Msg).
func sortFindings(fs []Finding) {
	slices.SortFunc(fs, func(a, b Finding) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Line, b.Line), cmp.Compare(a.Col, b.Col),
			cmp.Compare(a.Rule, b.Rule), cmp.Compare(a.Msg, b.Msg))
	})
}

// Allow lists what the non-test files of one package may import (DET-080).
type Allow struct {
	Stdlib   bool     // standard library except "testing" and "testing/..."
	Testing  []string // allowed packages of the testing tree, for example "testing"
	External []string // allowed third-party import paths; "p/..." means p and every package below it
	Packages []string // module packages, as paths relative to Module.Path ("" = root package)
}

// Module describes a module to check.
type Module struct {
	Root    string           // absolute path of the directory containing go.mod
	Path    string           // module path
	Classes map[string]Class // relative package path → class
	Files   map[string]Class // relative file path → class replacing its package's (DET-055)
	Imports map[string]Allow // relative package path → allowed imports
}

// checkContexts are the four go/build contexts of DET §3: linux/amd64, linux/arm64,
// darwin/arm64 and windows/amd64, without cgo.
func checkContexts() []build.Context {
	targets := [][2]string{{"linux", "amd64"}, {"linux", "arm64"}, {"darwin", "arm64"}, {"windows", "amd64"}}
	out := make([]build.Context, len(targets))
	for i, t := range targets {
		c := build.Default
		c.GOOS, c.GOARCH, c.CgoEnabled = t[0], t[1], false
		out[i] = c
	}
	return out
}

// hostContext returns a copy of build.Default without cgo, as in the check contexts: it selects the
// files that are type-checked (faultline DET-015), so the result does not depend on CGO_ENABLED.
func hostContext() *build.Context {
	c := build.Default
	c.CgoEnabled = false
	return &c
}

// contextMatches returns how many check contexts match the file name in dir.
func contextMatches(dir, name string) (int, error) {
	n := 0
	for _, c := range checkContexts() {
		ok, err := c.MatchFile(dir, name)
		if err != nil {
			return 0, err
		}
		if ok {
			n++
		}
	}
	return n, nil
}

// FindRoot walks up from dir to the first directory whose go.mod declares module path.
func FindRoot(dir, path string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for d := abs; ; d = filepath.Dir(d) {
		data, err := os.ReadFile(filepath.Join(d, "go.mod"))
		if err == nil && modulePath(string(data)) == path {
			return d, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if filepath.Dir(d) == d {
			return "", fmt.Errorf("dettest: no go.mod for module %s above %s", path, abs)
		}
	}
}

// modulePath returns the module path declared by go.mod content, or "".
func modulePath(gomod string) string {
	for _, line := range strings.Split(gomod, "\n") {
		line = strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(line, "module")
		if !ok || rest == "" || (rest[0] != ' ' && rest[0] != '\t') {
			continue
		}
		rest, _, _ = strings.Cut(rest, "//")
		rest = strings.TrimSpace(rest)
		if p, err := strconv.Unquote(rest); err == nil {
			return p
		}
		return rest
	}
	return ""
}

// packageDirs returns the package directories of the module at root, as slash-separated paths
// relative to root ("" for root itself), in walk order (faultline DET-015, DET §3).
func packageDirs(root string) ([]string, error) {
	var out []string
	var walk func(dir, rel string) error
	walk = func(dir, rel string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			n, err := contextMatches(dir, name)
			if err != nil {
				return err
			}
			if n > 0 {
				out = append(out, rel)
				break
			}
		}
		for _, e := range entries {
			name := e.Name()
			if !e.IsDir() || name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
				continue
			}
			sub := filepath.Join(dir, name)
			if _, err := os.Stat(filepath.Join(sub, "go.mod")); err == nil {
				continue // nested module
			}
			if err := walk(sub, path.Join(rel, name)); err != nil {
				return err
			}
		}
		return nil
	}
	return out, walk(root, "")
}

// importPath returns the import path of the package at rel in module m.
func importPath(m Module, rel string) string {
	if rel == "" {
		return m.Path
	}
	return m.Path + "/" + rel
}

// Lint type-checks and lints every package directory of m whose class is not ClassExempt and
// returns the findings sorted by (File, Line, Col, Rule, Msg).
func Lint(m Module) ([]Finding, error) {
	dirs, err := packageDirs(m.Root)
	if err != nil {
		return nil, err
	}
	l := newLinter()
	var out []Finding
	for _, rel := range dirs {
		c, ok := m.Classes[rel]
		if !ok {
			out = append(out, Finding{File: rel, Rule: RuleMeta, Msg: fmt.Sprintf(
				"package %s has no determinism class; add it to DET §5.1.3 and internal/dettest", importPath(m, rel))})
			continue
		}
		if c == ClassExempt {
			continue
		}
		fs, err := l.lintPackage(m.Root, filepath.Join(m.Root, filepath.FromSlash(rel)), importPath(m, rel), c, m.Files)
		if err != nil {
			return nil, err
		}
		out = append(out, fs...)
	}
	sortFindings(out)
	return out, nil
}
