// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/tools/go/analysis/analysistest"
)

// commandTimeout bounds every run of the go tool or of the built lockstep-vet.
const commandTimeout = 5 * time.Minute

// wallClockLine is the start of the LS100 diagnostic for time.Now (ANL-130).
const wallClockLine = "LS100: time.Now uses the wall clock"

// result is the outcome of a command.
type result struct {
	stdout, stderr []byte
	code           int
}

// run runs name with args in dir with env added to the environment, bounded by commandTimeout.
// It fails the test if the command cannot start or does not exit on its own.
func run(t *testing.T, dir string, env []string, name string, args ...string) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // test runs the go tool
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("%s %s: did not finish within %v\n%s", name, strings.Join(args, " "), commandTimeout, stderr.Bytes())
	}
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("%s %s: %v", name, strings.Join(args, " "), err)
		}
		code = exitErr.ExitCode()
	}
	return result{stdout: stdout.Bytes(), stderr: stderr.Bytes(), code: code}
}

// goTool returns the path of the go command.
func goTool(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	return goBin
}

// buildVetTool builds ./cmd/lockstep-vet into dir and returns the binary's path.
func buildVetTool(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "lockstep-vet")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	res := run(t, ".", []string{"GOWORK=off"}, goTool(t), "build", "-o", bin, "./cmd/lockstep-vet")
	if res.code != 0 {
		t.Fatalf("go build ./cmd/lockstep-vet: exit %d\n%s", res.code, res.stderr)
	}
	return bin
}

// moduleEnv makes the go tool and lockstep-vet work offline on the module of vetModule, outside
// any workspace (AT-ANL-27).
var moduleEnv = []string{"GOWORK=off", "GOFLAGS=-mod=mod", "GOPROXY=off"}

// vetModule writes, under dir, a copy of the stub as module github.com/hmdsefi/lockstep in
// dir/stub and the module example.com/vetcheck in dir/vetcheck, which requires the stub through a
// replace, and returns the directory of vetcheck.
func vetModule(t *testing.T, dir string) string {
	t.Helper()
	stub := filepath.Join(dir, "stub")
	copyGoFiles(t, filepath.Join(analysistest.TestData(), "src", "github.com", "hmdsefi", "lockstep"), stub)
	writeFile(t, filepath.Join(stub, "go.mod"), "module github.com/hmdsefi/lockstep\n\ngo 1.26.0\n")

	mod := filepath.Join(dir, "vetcheck")
	writeFile(t, filepath.Join(mod, "go.mod"), "module example.com/vetcheck\n\ngo 1.26.0\n\n"+
		"require github.com/hmdsefi/lockstep v0.0.0\n\n"+
		"replace github.com/hmdsefi/lockstep => "+stub+"\n")
	return mod
}

// copyGoFiles copies the .go files of directory from into directory to.
func copyGoFiles(t *testing.T, from, to string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(from, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("%s: no .go file", from)
	}
	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(to, filepath.Base(p)), string(content))
	}
}

// writeFile writes content to path, creating its directory.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil { //nolint:gosec // test writes under t.TempDir()
		t.Fatal(err)
	}
}

// clockActor is an actor that reads the wall clock in Receive.
const clockActor = `package vetcheck

import (
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is Clock's message type.
type Msg struct{}

// Clock records when it last received a message.
type Clock struct {
	last time.Time
}

// Receive reads the wall clock.
func (c *Clock) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	c.last = time.Now()
}
`

// TestVetTool checks AT-ANL-27: lockstep-vet speaks the go vet -vettool protocol and runs on its
// own, reports LS100 for time.Now in Receive and exits non-zero, and -determinism=false turns the
// check off (ANL-400).
func TestVetTool(t *testing.T) {
	tmp := t.TempDir()
	bin := buildVetTool(t, tmp)
	mod := vetModule(t, tmp)
	writeFile(t, filepath.Join(mod, "clock.go"), clockActor)

	vet := run(t, mod, moduleEnv, goTool(t), "vet", "-vettool="+bin, "./...")
	if vet.code == 0 || !bytes.Contains(vet.stderr, []byte(wallClockLine)) {
		t.Errorf("go vet -vettool: exit %d, stderr:\n%s\nwant a non-zero exit and a line containing %q",
			vet.code, vet.stderr, wallClockLine)
	}

	alone := run(t, mod, moduleEnv, bin, "./...")
	if alone.code == 0 || !bytes.Contains(alone.stderr, []byte(wallClockLine)) {
		t.Errorf("lockstep-vet ./...: exit %d, stderr:\n%s\nwant a non-zero exit and a line containing %q",
			alone.code, alone.stderr, wallClockLine)
	}

	off := run(t, mod, moduleEnv, bin, "-determinism=false", "./...")
	if off.code != 0 || len(off.stdout) != 0 || len(off.stderr) != 0 {
		t.Errorf("lockstep-vet -determinism=false ./...: exit %d, stdout:\n%s\nstderr:\n%s\nwant exit 0 and no output",
			off.code, off.stdout, off.stderr)
	}
}

// TestDeterministicOutput checks AT-ANL-28: two runs of lockstep-vet -json over a copy of the
// ls100/bad corpus package print byte-equal output (ANL-120).
func TestDeterministicOutput(t *testing.T) {
	tmp := t.TempDir()
	bin := buildVetTool(t, tmp)
	mod := vetModule(t, tmp)
	copyGoFiles(t, filepath.Join(analysistest.TestData(), "src", "ls100", "bad"), filepath.Join(mod, "ls100bad"))

	first := run(t, mod, moduleEnv, bin, "-json", "./...")
	if !bytes.Contains(first.stdout, []byte(wallClockLine)) {
		t.Fatalf("lockstep-vet -json ./...: exit %d, stdout:\n%s\nstderr:\n%s\nwant a diagnostic containing %q",
			first.code, first.stdout, first.stderr, wallClockLine)
	}
	second := run(t, mod, moduleEnv, bin, "-json", "./...")
	if second.code != first.code {
		t.Errorf("lockstep-vet -json ./...: exit %d, then %d", first.code, second.code)
	}
	if !bytes.Equal(first.stdout, second.stdout) {
		t.Errorf("lockstep-vet -json ./...: the two outputs differ\nfirst:\n%s\nsecond:\n%s", first.stdout, second.stdout)
	}
}
