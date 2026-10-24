// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"bytes"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	modulePath    = "github.com/hmdsefi/lockstep/examples/raft"
	faultlinePath = "github.com/hmdsefi/faultline"
	simPath       = "github.com/hmdsefi/lockstep/sim"
)

// TestATRFT01Deps checks RFT-010: the raft, kv and client packages depend on
// neither faultline nor lockstep/sim, directly or through another package.
func TestATRFT01Deps(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go tool not found: %v", err)
	}

	packages := []string{modulePath + "/raft", modulePath + "/kv", modulePath + "/client"}
	args := append([]string{"list", "-deps"}, packages...)
	cmd := exec.Command(goBin, args...) //nolint:gosec // G204: test runs the go tool
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, stderr.String())
	}

	listed := strings.Fields(string(out))
	if len(listed) == 0 {
		t.Fatal("go list -deps printed no packages")
	}
	for _, pkg := range listed {
		if pkg == simPath || strings.HasPrefix(pkg, faultlinePath) {
			t.Errorf("raft, kv and client depend on %s; only raftsim may import it", pkg)
		}
	}
}

// TestATRFT02NoAllowDirectives checks RFT-020: no .go file of the module,
// test files included, carries a lockstep allow directive.
func TestATRFT02NoAllowDirectives(t *testing.T) {
	// Built from parts so that this file does not contain the directive.
	directive := "lockstep:" + "allow"

	root := ".."
	files := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		files++
		src, err := os.ReadFile(path) //nolint:gosec // G304: walks the module's own sources
		if err != nil {
			return err
		}
		if bytes.Contains(src, []byte(directive)) {
			t.Errorf("%s contains the %s directive", path, directive)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if files == 0 {
		t.Fatalf("no .go files found under %s", root)
	}
}
