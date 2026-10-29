// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package protoreg

import (
	"os"
	"testing"
)

// TestConflictPolicy checks that the protobuf runtime finds a registration conflict policy once
// the package is initialized: "ignore", or the value the environment already had.
func TestConflictPolicy(t *testing.T) {
	if v := os.Getenv(conflictPolicy); v == "" {
		t.Errorf("%s is empty after initialization", conflictPolicy)
	}
}

// TestSetIfUnset checks that setIfUnset leaves a set variable alone and sets an unset or empty
// one.
func TestSetIfUnset(t *testing.T) {
	const key = "LOCKSTEP_BENCH_PROTOREG_TEST"
	t.Setenv(key, "kept") // restores the environment when the test ends
	if setIfUnset(key, "new") {
		t.Error("setIfUnset changed a set variable")
	}
	if v := os.Getenv(key); v != "kept" {
		t.Errorf("%s = %q, want %q", key, v, "kept")
	}

	t.Setenv(key, "")
	if !setIfUnset(key, "new") {
		t.Error("setIfUnset left an empty variable empty")
	}
	if v := os.Getenv(key); v != "new" {
		t.Errorf("%s = %q, want %q", key, v, "new")
	}

	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	if !setIfUnset(key, "new") {
		t.Error("setIfUnset left an unset variable unset")
	}
	if v := os.Getenv(key); v != "new" {
		t.Errorf("%s = %q, want %q", key, v, "new")
	}
}
