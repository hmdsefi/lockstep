// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orders_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/examples/orders"
)

// TestPlantedBugs runs each scenario with its bug planted, in a child process, and checks that
// the seed pinned in this package is the first to fail, with the expected check, and that the
// regression test fails on it too. The scenario tests themselves pass, because they run without
// the bug.
func TestPlantedBugs(t *testing.T) {
	for _, c := range []struct {
		bug        orders.Bugs
		test       string
		regression string
		seed       uint64
		check      string
	}{
		{orders.BugNoDedup, "TestRetry", "TestRetryRegression", retrySeed, checkChargedOnce},
		{orders.BugDropEarlyCancel, "TestCancelBeforeCreate", "TestCancelBeforeCreateRegression", earlyCancelSeed, checkCancelRefunds},
	} {
		t.Run(c.bug.String(), func(t *testing.T) {
			for _, test := range []string{c.test, c.regression} {
				r := firstFailure(t, test, "ORDERS_BUG="+c.bug.String())
				if r.seed != c.seed || r.Check != c.check {
					t.Errorf("%s with %s: first failure is seed %#x, check %q (%s); want seed %#x, check %q",
						test, c.bug, r.seed, r.Check, r.Message, c.seed, c.check)
				}
			}
		})
	}
}

// seedResult is the part of a FAULTLINE_RESULTS line the test reads.
type seedResult struct {
	Seed    string `json:"seed"`
	Status  string `json:"status"`
	Check   string `json:"check"`
	Message string `json:"message"`
	seed    uint64
}

// firstFailure runs test in a child process of this test binary with env added and returns the
// first failing seed's result. It fails t if the child passes or reports no failing seed.
func firstFailure(t *testing.T, test string, env ...string) seedResult {
	t.Helper()
	results := filepath.Join(t.TempDir(), "results.jsonl")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+test+"$", "-test.count=1") //nolint:gosec // G204: runs this test binary
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "FAULTLINE_") && !strings.HasPrefix(kv, "ORDERS_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "FAULTLINE_ARTIFACTS=off", "FAULTLINE_RESULTS="+results)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("%s: want a failing child, got %v\n%s", test, err, out)
	}
	f, err := os.Open(results) //nolint:gosec // G304: the test's own temp file
	if err != nil {
		t.Fatalf("%s: %v\n%s", test, err, out)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r seedResult
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("%s: results line %q: %v", test, sc.Text(), err)
		}
		if r.Status != "fail" {
			continue
		}
		if r.seed, err = strconv.ParseUint(r.Seed, 0, 64); err != nil {
			t.Fatalf("%s: results seed %q: %v", test, r.Seed, err)
		}
		return r
	}
	t.Fatalf("%s: no failing seed in the results (scan error %v)\n%s", test, sc.Err(), out)
	return seedResult{}
}
