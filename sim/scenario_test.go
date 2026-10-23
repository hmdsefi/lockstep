// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"flag"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/artifact"
)

// scenarios are the child-process scenarios of this package, by name. Tests look them up by key
// and never range over the map.
var scenarios = map[string]func(t *testing.T){}

// scenario registers fn under name; use it in package-level var declarations. A name used twice
// panics when the test binary starts, so a scenario never silently replaces another.
func scenario(name string, fn func(t *testing.T)) bool { //nolint:unparam // var _ = scenario(...) registers a scenario at package level
	if _, dup := scenarios[name]; dup {
		panic("scenario " + name + " registered twice")
	}
	scenarios[name] = fn
	return true
}

// scenarioEnv names the scenario that TestScenario runs in a child process.
const scenarioEnv = "LOCKSTEP_TEST_SCENARIO"

// TestScenario runs the scenario named by LOCKSTEP_TEST_SCENARIO and does nothing otherwise.
func TestScenario(t *testing.T) {
	name := os.Getenv(scenarioEnv)
	if name == "" {
		return
	}
	fn, ok := scenarios[name]
	if !ok {
		t.Fatalf("unknown scenario %q", name)
	}
	fn(t)
}

// scenarioTimeout bounds a scenario child when the parent test has no deadline (-timeout 0).
const scenarioTimeout = 10 * time.Minute

// runScenario runs scenario name in a child process of this test binary: the parent environment
// minus every FAULTLINE_* and LOCKSTEP_* variable, plus LOCKSTEP_TEST_SCENARIO,
// FAULTLINE_ARTIFACTS=<t.TempDir()>, plus env (later entries win). It returns the combined output
// and the exit code. Under go test -cover the child adds its coverage to this binary's.
//
// A hung child fails the calling test: it stops itself with a stack dump (-test.timeout) after a
// fifth of the time left before the parent's deadline, and is killed at nine tenths of it.
func runScenario(t *testing.T, name string, env ...string) (string, int) {
	t.Helper()
	limit, kill := scenarioTimeout, scenarioTimeout+time.Minute
	if d, ok := t.Deadline(); ok {
		left := time.Until(d)
		limit, kill = left/5, left-left/10
	}
	ctx, cancel := context.WithTimeout(t.Context(), kill)
	defer cancel()
	args := append([]string{"-test.run=^TestScenario$", "-test.v", "-test.timeout=" + limit.String()}, coverArgs()...)
	cmd := exec.CommandContext(ctx, os.Args[0], args...) //nolint:gosec // runs this test binary again
	cmd.WaitDelay = 5 * time.Second
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(k, "FAULTLINE_") && !strings.HasPrefix(k, "LOCKSTEP_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	cmd.Env = append(cmd.Env, scenarioEnv+"="+name, "FAULTLINE_ARTIFACTS="+t.TempDir())
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("scenario %s killed after %s: %v\n%s", name, kill, ctx.Err(), out)
	}
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return string(out), code
}

// coverArgs returns the flag that makes a child process of this test binary write its coverage
// data where go test collects this binary's, when go test measures coverage.
func coverArgs() []string {
	if f := flag.Lookup("test.gocoverdir"); f != nil && f.Value.String() != "" {
		return []string{"-test.gocoverdir=" + f.Value.String()}
	}
	return nil
}

// failure is the part of faultline's report of a failing seed that tests assert on.
type failure struct {
	Kind      string // invariant, final, panic, ...
	Check     string // the check name
	Signature string // <kind>:<check>
	Message   string
}

// failingSeed runs scenario name in a child process with FAULTLINE_ARTIFACTS set to a temporary
// directory, finds the one report.json beneath it and returns the failure it records. The test
// fails if the child exits 0 or does not leave exactly one report.json. The scenario must run one
// seed, so that faultline writes one report.
func failingSeed(t *testing.T, name string) failure {
	t.Helper()
	f, _ := failingSeedOutput(t, name)
	return f
}

// failingSeedOutput is failingSeed that also returns the child's combined output.
func failingSeedOutput(t *testing.T, name string) (failure, string) {
	t.Helper()
	a, out := failingSeedArtifact(t, name)
	f := a.Report.Failure
	return failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}, out
}

// failingSeedArtifact is failingSeedOutput that returns the whole artifact of the failing seed:
// its report and, when the child wrote one, its trace.
func failingSeedArtifact(t *testing.T, name string) (*artifact.Artifact, string) {
	t.Helper()
	dir := t.TempDir()
	out, code := runScenario(t, name, "FAULTLINE_ARTIFACTS="+dir)
	if code == 0 {
		t.Fatalf("scenario %s exited 0, want a failing seed\n%s", name, out)
	}
	var reports []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "report.json" {
			reports = append(reports, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 {
		t.Fatalf("scenario %s left %d report.json files under %s, want 1\n%s", name, len(reports), dir, out)
	}
	a, err := artifact.Read(filepath.Dir(reports[0]))
	if err != nil {
		t.Fatal(err)
	}
	if a.Report.Failure == nil {
		t.Fatalf("scenario %s: report.json has status %q and no failure\n%s", name, a.Report.Status, out)
	}
	return a, out
}
