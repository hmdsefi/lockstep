// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

// Lockstep returns lockstep's root module at root with the tables of DET-050, DET-055 and DET-080.
// Rows for packages and files that do not exist yet are harmless. Each call builds new maps, so a
// caller may change the result.
func Lockstep(root string) Module {
	const (
		faultline = "github.com/hmdsefi/faultline"
		gograph   = "github.com/hmdsefi/gograph/..."
		goleak    = "go.uber.org/goleak"
	)

	// DET-050: the determinism class of every package directory.
	classes := map[string]Class{}
	for _, row := range []struct {
		class Class
		pkgs  []string
	}{
		{ClassCore, []string{"internal/engine", "internal/engine/enginetest", "internal/hooks", "internal/supervise",
			"internal/registry", "internal/overload", "internal/remoting", "internal/toys", "internal/viz", "sim",
			"persist", "persist/filestore", "codec", "cluster"}},
		{ClassRuntime, []string{"", "internal/mailbox"}},
		{ClassEngine, []string{"internal/engine/local", "internal/prodengine", "internal/shrink", "remote"}},
		{ClassExempt, []string{"internal/dettest", "cmd/lockstep"}},
	} {
		for _, p := range row.pkgs {
			classes[p] = row.class
		}
	}

	// DET-055: files whose class replaces their package's.
	files := map[string]Class{
		"internal/overload/sampler.go":  ClassEngine,
		"internal/overload/watchdog.go": ClassEngine,
		"internal/overload/monitor.go":  ClassEngine,
		"sim/env.go":                    ClassExempt,
	}

	// DET-080: what the non-test files of each package may import (architecture §5.1).
	std := func(pkgs ...string) Allow { return Allow{Stdlib: true, Packages: pkgs} }
	engine := std("internal/engine")
	imports := map[string]Allow{
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
		"internal/dettest":           {Stdlib: true, Testing: []string{"testing"}, External: []string{goleak}},
		"internal/toys":              std(""),
		"internal/viz":               {Stdlib: true, External: []string{faultline + "/artifact", faultline + "/kernel", gograph}},
		"sim": {
			Stdlib:  true,
			Testing: []string{"testing"},
			External: []string{faultline, faultline + "/kernel", faultline + "/kernel/simnet",
				faultline + "/kernel/simdisk", faultline + "/kernel/fault", gograph},
			Packages: []string{"", "codec", "remote", "persist", "persist/filestore", "internal/engine",
				"internal/hooks", "internal/mailbox", "internal/supervise", "internal/registry",
				"internal/overload", "internal/remoting", "internal/viz"},
		},
		"persist":           std("", "internal/hooks"),
		"persist/filestore": std("", "persist"),
		"codec":             std(""),
		"remote":            std("", "codec", "internal/engine", "internal/hooks", "internal/remoting"),
		"cluster":           std("", "codec", "internal/hooks"),
		"cmd/lockstep": {
			Stdlib:   true,
			External: []string{faultline + "/artifact", faultline + "/kernel", faultline + "/kernel/fault"},
			Packages: []string{"internal/shrink", "internal/viz"},
		},
	}

	return Module{Root: root, Path: "github.com/hmdsefi/lockstep", Classes: classes, Files: files, Imports: imports}
}
