// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/internal/dettest"
)

const (
	rootPkg     = "github.com/hmdsefi/lockstep"
	remotingPkg = rootPkg + "/internal/remoting"
)

// goListDeps returns the import paths `go list -deps pkg` prints, run in the module root.
func goListDeps(t *testing.T, root, pkg string) []string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(goBin, "list", "-deps", pkg)
	cmd.Dir = root
	cmd.Env = os.Environ()
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps %s: %v\n%s", pkg, err, stderr.String())
	}
	return strings.Fields(string(out))
}

// simulationOnly returns the packages of deps that belong to faultline or gograph.
func simulationOnly(deps []string) []string {
	var bad []string
	for _, p := range deps {
		if strings.HasPrefix(p, "github.com/hmdsefi/faultline") || strings.HasPrefix(p, "github.com/hmdsefi/gograph") {
			bad = append(bad, p)
		}
	}
	return bad
}

// TestATREM39Boundaries checks REM-350 and §6 mechanically: the TCP driver (remote) and the
// simulation (sim) are built on the one core, which itself, like the root package, links neither
// faultline nor gograph.
func TestATREM39Boundaries(t *testing.T) {
	root, err := dettest.FindRoot(".", rootPkg)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range []string{rootPkg + "/remote", rootPkg + "/sim"} {
		if deps := goListDeps(t, root, pkg); !slices.Contains(deps, remotingPkg) {
			t.Errorf("go list -deps %s does not list %s: the driver does not run the shared core", pkg, remotingPkg)
		}
	}
	for _, pkg := range []string{remotingPkg, rootPkg} {
		deps := goListDeps(t, root, pkg)
		if len(deps) == 0 {
			t.Errorf("go list -deps %s listed nothing", pkg)
		}
		if bad := simulationOnly(deps); len(bad) > 0 {
			t.Errorf("go list -deps %s lists simulation-only packages: %s", pkg, strings.Join(bad, ", "))
		}
	}
	// The check itself finds what it looks for.
	if bad := simulationOnly(goListDeps(t, root, rootPkg+"/sim")); len(bad) == 0 {
		t.Error("go list -deps sim lists no faultline package, so the check above could not fail")
	}
}
