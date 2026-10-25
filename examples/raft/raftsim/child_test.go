// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// childTimeout bounds a child process when the parent test has no deadline.
const childTimeout = 10 * time.Minute

// runChild runs this test binary again as a child process that runs only the test named test,
// and returns the child's combined output and exit code. The child gets the parent's environment
// without the FAULTLINE_*, RAFTDEMO_* and LOCKSTEP_CHECK_DETERMINISM settings, plus
// FAULTLINE_ARTIFACTS=off and env. Its -test.timeout is a fifth of the parent's time left, and it
// is killed at nine tenths of it, so a hung child fails the parent before go test kills it.
func runChild(t *testing.T, test string, env ...string) (output string, code int) {
	t.Helper()
	limit, kill := childTimeout, childTimeout+time.Minute
	if deadline, ok := t.Deadline(); ok {
		left := time.Until(deadline)
		limit, kill = left/5, left-left/10
	}
	ctx, cancel := context.WithTimeout(t.Context(), kill)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+test+"$", "-test.timeout="+limit.String()) //nolint:gosec // G204: runs this test binary
	cmd.WaitDelay = 5 * time.Second
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "FAULTLINE_") && !strings.HasPrefix(kv, "RAFTDEMO_") &&
			!strings.HasPrefix(kv, "LOCKSTEP_CHECK_DETERMINISM=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	cmd.Env = append(cmd.Env, "FAULTLINE_ARTIFACTS=off")
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("child %s killed after %s: %v\n%s", test, kill, ctx.Err(), out)
	}
	var exit *exec.ExitError
	switch {
	case err == nil:
		return string(out), 0
	case errors.As(err, &exit):
		return string(out), exit.ExitCode()
	}
	t.Fatalf("running child %s: %v", test, err)
	return "", -1
}
