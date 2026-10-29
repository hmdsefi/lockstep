// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bench

import (
	"context"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	// childTimeout is the -timeout of a child go test when the parent test has no deadline.
	childTimeout = 10 * time.Minute

	// childMargin is how long before the parent's deadline the child's own -timeout ends. It leaves
	// time to build the child, whose timeout starts only when its test binary does, and to report
	// the goroutines the child prints when it times out.
	childMargin = time.Minute

	// killMargin is how long before the parent's deadline the parent kills a child go command that
	// is still running, so the parent still reports what the child printed.
	killMargin = 10 * time.Second
)

// goTestCommand returns a command that runs go test with flags on the packages pkgs in the
// package's directory, for a test that checks what a child go test does. Whatever GOFLAGS says,
// the child is built without -race and runs each test or benchmark once, without -v; flags may
// set -count again.
//
// The child's -timeout ends childMargin before the parent's deadline, so a child that hangs fails
// with its own goroutine dump while the parent still runs. If the go command is still running
// killMargin before the deadline, for instance because building the child took longer than
// childMargin, the parent kills it, so the caller can report what it printed. Without a parent
// deadline the child gets childTimeout.
func goTestCommand(t *testing.T, flags []string, pkgs ...string) *exec.Cmd {
	t.Helper()
	ctx, timeout := t.Context(), childTimeout
	if deadline, ok := t.Deadline(); ok {
		timeout = time.Until(deadline) - childMargin
		if timeout < time.Second {
			t.Fatalf("the test deadline is %v away, too close to run a child go test", time.Until(deadline).Round(time.Second))
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-killMargin))
		t.Cleanup(cancel)
	}
	args := slices.Concat(
		[]string{"test", "-race=false", "-count=1", "-v=false"}, flags,
		[]string{"-timeout", timeout.Truncate(time.Second).String()}, pkgs,
	)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Cancel = func() error {
		t.Logf("killing go %s: the test deadline is %v away", strings.Join(args, " "), killMargin)
		return cmd.Process.Kill()
	}
	// The killed go command may leave its test binary running, holding the output pipes open.
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

// TestGoTestCommand checks the arguments goTestCommand gives a child go test: -race, -count and
// -v first, so the caller's flags come after them and may set -count again, then the timeout, then
// the packages. The timeout ends at least childMargin before this test's deadline.
func TestGoTestCommand(t *testing.T) {
	cmd := goTestCommand(t, []string{"-run", "^X$", "-count", "2"}, "./a", "./b")
	head := []string{"go", "test", "-race=false", "-count=1", "-v=false", "-run", "^X$", "-count", "2", "-timeout"}
	args := cmd.Args
	if len(args) != len(head)+3 || !slices.Equal(args[:len(head)], head) || !slices.Equal(args[len(head)+1:], []string{"./a", "./b"}) {
		t.Fatalf("arguments %q, want %q, a timeout, ./a and ./b", args, head)
	}
	timeout, err := time.ParseDuration(args[len(head)])
	if err != nil {
		t.Fatal(err)
	}
	deadline, ok := t.Deadline()
	if !ok {
		if timeout != childTimeout {
			t.Errorf("-timeout %v without a test deadline, want %v", timeout, childTimeout)
		}
		return
	}
	if latest := time.Until(deadline) - childMargin; timeout < time.Second || timeout > latest {
		t.Errorf("-timeout %v, want at least 1s and at most %v, childMargin before the test deadline", timeout, latest)
	}
}
