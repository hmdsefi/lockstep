// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// AT-DET-11, DET-130
func TestEnabled(t *testing.T) {
	tests := []struct {
		value   string
		want    bool
		wantErr string
	}{
		{value: "", want: false},
		{value: "0", want: false},
		{value: "1", want: true},
		{value: "yes", wantErr: `dettest: invalid LOCKSTEP_CHECK_DETERMINISM="yes": want 0 or 1`},
		{value: "true", wantErr: `dettest: invalid LOCKSTEP_CHECK_DETERMINISM="true": want 0 or 1`},
		{value: "01", wantErr: `dettest: invalid LOCKSTEP_CHECK_DETERMINISM="01": want 0 or 1`},
		{value: " 1", wantErr: `dettest: invalid LOCKSTEP_CHECK_DETERMINISM=" 1": want 0 or 1`},
		{value: "2", wantErr: `dettest: invalid LOCKSTEP_CHECK_DETERMINISM="2": want 0 or 1`},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%q", tc.value), func(t *testing.T) {
			t.Setenv(EnvCheckDeterminism, tc.value)
			got, err := Enabled()
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("Enabled() error = %v, want %q", err, tc.wantErr)
				}
				if got {
					t.Error("Enabled() = true with an error, want false")
				}
				return
			}
			if err != nil {
				t.Fatalf("Enabled() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("Enabled() = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("unset", func(t *testing.T) {
		// t.Setenv registers the restore of the original value; Unsetenv then removes it.
		t.Setenv(EnvCheckDeterminism, "")
		if err := os.Unsetenv(EnvCheckDeterminism); err != nil {
			t.Fatal(err)
		}
		got, err := Enabled()
		if err != nil || got {
			t.Errorf("Enabled() = %v, %v; want false, nil", got, err)
		}
	})
}

func TestEnvNames(t *testing.T) {
	if EnvCheckDeterminism != "LOCKSTEP_CHECK_DETERMINISM" {
		t.Errorf("EnvCheckDeterminism = %q", EnvCheckDeterminism)
	}
	if EnvWorkload != "LOCKSTEP_WORKLOAD" {
		t.Errorf("EnvWorkload = %q", EnvWorkload)
	}
}

const faultlineEnv = "FAULTLINE_CHECK_DETERMINISM"

// TestHelperMain is the child of TestMainHelper. Without DETTEST_HELPER=1 it skips.
func TestHelperMain(t *testing.T) {
	if os.Getenv("DETTEST_HELPER") != "1" {
		t.Skip("child of TestMainHelper")
	}
	if os.Getenv("DETTEST_SECOND_PREPARE") == "1" {
		// A second Prepare acts on nothing: it neither resets FAULTLINE_CHECK_DETERMINISM nor
		// reads the invalid LOCKSTEP_CHECK_DETERMINISM again (which would exit 2).
		t.Setenv(faultlineEnv, "0")
		t.Setenv(EnvCheckDeterminism, "yes")
		Prepare()
	}
	fmt.Println(faultlineEnv + "=" + os.Getenv(faultlineEnv))
	if os.Getenv("DETTEST_LEAK") == "1" {
		go func() { select {} }()
	}
}

// helperResult is the outcome of one run of the test binary as the child of TestMainHelper.
type helperResult struct {
	stdout, stderr string
	exit           int
}

// runHelper re-runs the test binary on TestHelperMain. The child's environment is the parent's
// without the variables of the switch and of the helper, plus DETTEST_HELPER=1 and env.
func runHelper(t *testing.T, env ...string) helperResult {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHelperMain$") //nolint:gosec // the test binary itself
	cmd.WaitDelay = 10 * time.Second
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch {
		case name == EnvCheckDeterminism, name == EnvWorkload, name == faultlineEnv, strings.HasPrefix(name, "DETTEST_"):
		default:
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "DETTEST_HELPER=1")
	cmd.Env = append(cmd.Env, env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	res := helperResult{stdout: stdout.String(), stderr: stderr.String()}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		res.exit = exitErr.ExitCode()
	default:
		t.Fatalf("running %s: %v", os.Args[0], err)
	}
	return res
}

// printed returns the value the child printed for FAULTLINE_CHECK_DETERMINISM, and whether it
// printed the line at all.
func (r helperResult) printed() (string, bool) {
	for line := range strings.SplitSeq(r.stdout, "\n") {
		if v, ok := strings.CutPrefix(line, faultlineEnv+"="); ok {
			return v, true
		}
	}
	return "", false
}

// AT-DET-12, DET-130, DET-140
func TestMainHelper(t *testing.T) {
	const invalid = `dettest: invalid LOCKSTEP_CHECK_DETERMINISM="yes": want 0 or 1` + "\n"
	tests := []struct {
		name      string
		env       []string
		want      string // the value the child prints
		wantExit  int
		wantErr   string // the whole of the child's stderr, when wantExit is 2
		noPrinted bool
	}{
		{name: "enabled sets faultline", env: []string{EnvCheckDeterminism + "=1"}, want: "1"},
		{name: "enabled replaces 0", env: []string{EnvCheckDeterminism + "=1", faultlineEnv + "=0"}, want: "1"},
		{name: "enabled replaces empty", env: []string{EnvCheckDeterminism + "=1", faultlineEnv + "="}, want: "1"},
		{name: "enabled leaves other values to faultline", env: []string{EnvCheckDeterminism + "=1", faultlineEnv + "=yes"}, want: "yes"},
		{name: "disabled", env: []string{EnvCheckDeterminism + "=0"}, want: ""},
		{name: "disabled keeps faultline", env: []string{EnvCheckDeterminism + "=0", faultlineEnv + "=1"}, want: "1"},
		{name: "unset", want: ""},
		{name: "unset keeps faultline", env: []string{faultlineEnv + "=1"}, want: "1"},
		{
			name: "invalid exits 2", env: []string{EnvCheckDeterminism + "=yes"},
			wantExit: 2, wantErr: invalid, noPrinted: true,
		},
		{name: "workload changes nothing", env: []string{EnvCheckDeterminism + "=1", EnvWorkload + "=w.json"}, want: "1"},
		{name: "workload alone changes nothing", env: []string{EnvWorkload + "=w.json"}, want: ""},
		{
			name: "second Prepare does nothing",
			env:  []string{EnvCheckDeterminism + "=1", "DETTEST_SECOND_PREPARE=1"}, want: "0",
		},
		{
			name: "second Prepare after a disabled first",
			env:  []string{"DETTEST_SECOND_PREPARE=1"}, want: "0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := runHelper(t, tc.env...)
			if res.exit != tc.wantExit {
				t.Fatalf("exit status %d, want %d\nstdout:\n%s\nstderr:\n%s", res.exit, tc.wantExit, res.stdout, res.stderr)
			}
			got, ok := res.printed()
			if tc.noPrinted {
				if ok {
					t.Errorf("child printed %q, want no output from the test", got)
				}
				if res.stderr != tc.wantErr {
					t.Errorf("stderr = %q, want %q", res.stderr, tc.wantErr)
				}
				return
			}
			if !ok {
				t.Fatalf("child printed no %s line\nstdout:\n%s\nstderr:\n%s", faultlineEnv, res.stdout, res.stderr)
			}
			if got != tc.want {
				t.Errorf("child printed %s=%q, want %q", faultlineEnv, got, tc.want)
			}
		})
	}

	t.Run("leak fails the child", func(t *testing.T) {
		res := runHelper(t, "DETTEST_LEAK=1")
		if res.exit == 0 {
			t.Fatalf("child exited 0 with a leaked goroutine\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
		}
		out := res.stdout + res.stderr
		for _, want := range []string{"goleak", "found unexpected goroutines"} {
			if !strings.Contains(out, want) {
				t.Errorf("child output lacks %q:\n%s", want, out)
			}
		}
	})

	t.Run("no leak passes the child", func(t *testing.T) {
		res := runHelper(t)
		if res.exit != 0 {
			t.Fatalf("exit status %d, want 0\nstdout:\n%s\nstderr:\n%s", res.exit, res.stdout, res.stderr)
		}
	})
}
