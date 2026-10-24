// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// goCmd runs the go tool with args in the module directory, outside any workspace, and returns
// its standard output.
func goCmd(t *testing.T, args ...string) []byte {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(goBin, args...) //nolint:gosec // test runs the go tool
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		var stderr []byte
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderr = exitErr.Stderr
		}
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, stderr)
	}
	return out
}

// TestModuleRequirements checks AT-ANL-01: the module requires neither lockstep nor faultline
// (ANL-010) and says go 1.26.0.
func TestModuleRequirements(t *testing.T) {
	const lockstepModule = "github.com/hmdsefi/lockstep"
	for line := range strings.Lines(string(goCmd(t, "list", "-m", "all"))) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == lockstepModule {
			t.Errorf("go list -m all: the module requires %s: %q", lockstepModule, strings.TrimSpace(line))
		}
		if strings.HasPrefix(line, "github.com/hmdsefi/faultline") {
			t.Errorf("go list -m all: the module requires faultline: %q", strings.TrimSpace(line))
		}
	}

	var mod struct{ Go string }
	if err := json.Unmarshal(goCmd(t, "mod", "edit", "-json"), &mod); err != nil {
		t.Fatalf("go mod edit -json: %v", err)
	}
	if mod.Go != "1.26.0" {
		t.Errorf("go mod edit -json: Go is %q, want %q", mod.Go, "1.26.0")
	}
}
