// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/internal/dettest"
)

// AT-DLV-21, DLV-300, DLV-310: dedup imports only the standard library, and internal/dettest
// classifies it as core, allows it the standard library only and snapshots its API.
func TestATDLV21PackageConstraints(t *testing.T) {
	root, err := dettest.FindRoot(".", "github.com/hmdsefi/lockstep")
	if err != nil {
		t.Fatal(err)
	}
	m := dettest.Lockstep(root)
	if got := m.Classes["dedup"]; got != dettest.ClassCore {
		t.Errorf(`Classes["dedup"] = %v, want core`, got)
	}
	if got, want := m.Imports["dedup"], (dettest.Allow{Stdlib: true}); !reflect.DeepEqual(got, want) {
		t.Errorf(`Imports["dedup"] = %+v, want %+v`, got, want)
	}
	if !slices.Contains(dettest.APIPackages, "dedup") {
		t.Errorf("APIPackages = %q, lacks dedup", dettest.APIPackages)
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files++
		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if first, _, _ := strings.Cut(path, "/"); strings.Contains(first, ".") {
				t.Errorf("%s imports %s, which is not in the standard library", name, path)
			}
		}
	}
	if files == 0 {
		t.Error("found no non-test files")
	}

	api, err := os.ReadFile(filepath.Join(root, "api", "lockstep.txt"))
	if err != nil {
		t.Fatal(err)
	}
	const line = "pkg github.com/hmdsefi/lockstep/dedup, func NewWindow[K comparable](Config) *Window[K]\n"
	if !strings.Contains(string(api), line) {
		t.Errorf("api/lockstep.txt lacks the line %q", strings.TrimSuffix(line, "\n"))
	}
}
