// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
)

// sorted returns a sorted copy of s.
func sorted(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

// sameAllow reports whether a and b allow the same imports, ignoring the order of the lists.
func sameAllow(a, b Allow) bool {
	return a.Stdlib == b.Stdlib &&
		slices.Equal(sorted(a.Testing), sorted(b.Testing)) &&
		slices.Equal(sorted(a.External), sorted(b.External)) &&
		slices.Equal(sorted(a.Packages), sorted(b.Packages))
}

// allowText formats a for failure messages.
func allowText(a Allow) string {
	return fmt.Sprintf("{Stdlib:%t Testing:%q External:%q Packages:%q}", a.Stdlib, sorted(a.Testing), sorted(a.External), sorted(a.Packages))
}

// AT-DET-08, DET-050, DET-055, DET-080
func TestLockstepTables(t *testing.T) {
	m := Lockstep("/some/root")
	if m.Root != "/some/root" || m.Path != lockstepModule {
		t.Fatalf("Root, Path = %q, %q; want /some/root, %q", m.Root, m.Path, lockstepModule)
	}

	t.Run("classes", func(t *testing.T) {
		want := map[string]Class{}
		for _, p := range []string{"internal/engine", "internal/engine/enginetest", "internal/hooks", "internal/supervise",
			"internal/registry", "internal/overload", "internal/remoting", "internal/toys", "internal/viz", "sim",
			"persist", "persist/filestore", "codec", "cluster"} {
			want[p] = ClassCore
		}
		for _, p := range []string{"", "internal/mailbox"} {
			want[p] = ClassRuntime
		}
		for _, p := range []string{"internal/engine/local", "internal/prodengine", "internal/shrink", "remote"} {
			want[p] = ClassEngine
		}
		for _, p := range []string{"internal/dettest", "cmd/lockstep"} {
			want[p] = ClassExempt
		}
		if !maps.Equal(m.Classes, want) {
			t.Errorf("Classes differ from DET-050:\n got %v\nwant %v", m.Classes, want)
		}
		if _, ok := m.Classes["internal/deps"]; ok {
			t.Error(`Classes["internal/deps"] is present: it is not a package directory (REP-030)`)
		}
		if got := m.Classes["internal/remoting"]; got != ClassCore {
			t.Errorf(`Classes["internal/remoting"] = %v, want core`, got)
		}
	})

	t.Run("imports", func(t *testing.T) {
		const (
			fl = "github.com/hmdsefi/faultline"
			gg = "github.com/hmdsefi/gograph/..."
		)
		std := func(pkgs ...string) Allow { return Allow{Stdlib: true, Packages: pkgs} }
		engine := std("internal/engine")
		want := map[string]Allow{
			"": std("internal/engine", "internal/engine/local", "internal/hooks", "internal/mailbox",
				"internal/supervise", "internal/registry", "internal/prodengine", "internal/overload"),
			"internal/engine":            std(),
			"internal/shrink":            std(),
			"internal/hooks":             engine,
			"internal/engine/local":      engine,
			"internal/engine/enginetest": engine,
			"internal/mailbox":           engine,
			"internal/supervise":         engine,
			"internal/registry":          engine,
			"internal/prodengine":        engine,
			"internal/overload":          engine,
			"internal/remoting":          std("", "codec", "internal/engine", "internal/hooks"),
			"internal/dettest":           {Stdlib: true, Testing: []string{"testing"}, External: []string{"go.uber.org/goleak"}},
			"internal/toys":              std(""),
			"internal/viz":               {Stdlib: true, External: []string{fl + "/artifact", fl + "/kernel", gg}},
			"sim": {Stdlib: true, Testing: []string{"testing"},
				External: []string{fl, fl + "/kernel", fl + "/kernel/simnet", fl + "/kernel/simdisk", fl + "/kernel/fault", gg},
				Packages: []string{"", "codec", "remote", "persist", "persist/filestore", "internal/engine",
					"internal/hooks", "internal/mailbox", "internal/supervise", "internal/registry",
					"internal/overload", "internal/remoting", "internal/viz"}},
			"persist":           std("", "internal/hooks"),
			"persist/filestore": std("", "persist"),
			"codec":             std(""),
			"remote":            std("", "codec", "internal/engine", "internal/hooks", "internal/remoting"),
			"cluster":           std("", "codec", "internal/hooks"),
			"cmd/lockstep": {Stdlib: true, External: []string{fl + "/artifact", fl + "/kernel", fl + "/kernel/fault"},
				Packages: []string{"internal/shrink", "internal/viz"}},
		}
		for _, p := range slices.Sorted(maps.Keys(want)) {
			got, ok := m.Imports[p]
			if !ok {
				t.Errorf("Imports[%q] is missing", p)
			} else if !sameAllow(got, want[p]) {
				t.Errorf("Imports[%q] = %s, want %s", p, allowText(got), allowText(want[p]))
			}
		}
		for p := range m.Imports {
			if _, ok := want[p]; !ok {
				t.Errorf("Imports[%q] is not in DET-080", p)
			}
		}
		if _, ok := m.Imports["internal/deps"]; ok {
			t.Error(`Imports["internal/deps"] is present: it is not a package directory (REP-030)`)
		}
	})

	t.Run("one rule per package in both tables", func(t *testing.T) {
		if got, want := slices.Sorted(maps.Keys(m.Imports)), slices.Sorted(maps.Keys(m.Classes)); !slices.Equal(got, want) {
			t.Errorf("Imports rows %q differ from Classes rows %q", got, want)
		}
		if n := len(m.Classes); n != 22 {
			t.Errorf("%d classified packages, want the 22 of DET-050", n)
		}
	})

	t.Run("sim row", func(t *testing.T) {
		sim := m.Imports["sim"]
		if len(sim.External) != 6 {
			t.Errorf("Imports[sim].External has %d entries, want 6: %q", len(sim.External), sim.External)
		}
		if !slices.Equal(sim.Testing, []string{"testing"}) {
			t.Errorf("Imports[sim].Testing = %q, want [testing]", sim.Testing)
		}
		for _, p := range []string{"codec", "remote", "persist", "persist/filestore"} {
			if !slices.Contains(sim.Packages, p) {
				t.Errorf("Imports[sim].Packages lacks %q", p)
			}
		}
		for _, p := range []string{"cluster", "persist"} {
			if !slices.Contains(m.Imports[p].Packages, "internal/hooks") {
				t.Errorf("Imports[%s].Packages lacks internal/hooks", p)
			}
		}
	})

	t.Run("dettest row", func(t *testing.T) {
		d := m.Imports["internal/dettest"]
		if !d.Stdlib || !slices.Equal(d.Testing, []string{"testing"}) ||
			!slices.Equal(d.External, []string{"go.uber.org/goleak"}) || len(d.Packages) != 0 {
			t.Errorf("Imports[internal/dettest] = %s", allowText(d))
		}
	})

	t.Run("files", func(t *testing.T) {
		want := map[string]Class{
			"internal/overload/sampler.go":  ClassEngine,
			"internal/overload/watchdog.go": ClassEngine,
			"internal/overload/monitor.go":  ClassEngine,
			"sim/env.go":                    ClassExempt,
		}
		if !maps.Equal(m.Files, want) {
			t.Errorf("Files = %v, want %v", m.Files, want)
		}
	})

	t.Run("tables are not shared between calls", func(t *testing.T) {
		a, b := Lockstep("/a"), Lockstep("/b")
		a.Classes["extra"] = ClassCore
		a.Imports["extra"] = Allow{Stdlib: true}
		a.Files["extra.go"] = ClassCore
		if _, ok := b.Classes["extra"]; ok {
			t.Error("a change to one module's Classes shows in another")
		}
		if _, ok := b.Imports["extra"]; ok {
			t.Error("a change to one module's Imports shows in another")
		}
		if _, ok := b.Files["extra.go"]; ok {
			t.Error("a change to one module's Files shows in another")
		}
	})
}

// DET-050, DET-055 end to end: a tree laid out like lockstep, linted with Lockstep's tables.
func TestLockstepLint(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"go.mod": "module " + lockstepModule + "\n\ngo 1.26\n",
		// engine is core: the wall clock is reported.
		"internal/engine/e.go": "package engine\n\nimport \"time\"\n\nvar _ = time.Now()\n",
		// local is an engine class: goroutines are fine.
		"internal/engine/local/l.go": "package local\n\nfunc init() { go func() {}() }\n",
		// overload is core, except for sampler.go, watchdog.go and monitor.go.
		"internal/overload/cap.go":      "package overload\n\nfunc init() { go func() {}() }\n",
		"internal/overload/sampler.go":  "package overload\n\nfunc init() { go func() {}() }\n",
		"internal/overload/watchdog.go": "package overload\n\nfunc init() { go func() {}() }\n",
		"internal/overload/monitor.go":  "package overload\n\nfunc init() { go func() {}() }\n",
		// sim is core, except for env.go.
		"sim/env.go":   "package sim\n\nimport \"os\"\n\nvar _ = os.Getenv(\"X\")\n",
		"sim/other.go": "package sim\n\nimport \"os\"\n\nvar _ = os.Getenv(\"X\")\n",
		// the root package is runtime: blocking is honored there.
		"r.go": "package lockstep\n\nfunc f() {\n\t//lockstep:blocking only BlockSender reaches this\n\tselect {}\n}\n",
		// exempt packages are not linted.
		"internal/dettest/d.go": "package dettest\n\nfunc init() { go func() {}() }\n",
		"cmd/lockstep/main.go":  "package main\n\nfunc main() { go main() }\n",
		// a nested module has no row.
		"examples/raft/go.mod": "module " + lockstepModule + "/examples/raft\n\ngo 1.26\n",
		"examples/raft/r.go":   "package raft\n\nfunc init() { go func() {}() }\n",
	})
	fs, err := Lint(Lockstep(root))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"internal/engine/e.go:5:14: DL001: wall-clock time: time.Now is forbidden; use engine.Clock (ctx.Now, ctx.After in actors)",
		"internal/overload/cap.go:3:15: DL004: go statement is forbidden in simulation code",
		"sim/other.go:5:12: DL008: environment access: os.Getenv is forbidden in simulation code",
	}
	var got []string
	for _, f := range fs {
		got = append(got, f.String())
	}
	if !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", findingLines(fs), strings.Join(want, "\n  "))
	}
}
