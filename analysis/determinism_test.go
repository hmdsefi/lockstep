// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestDeterminism checks the LS100 corpus (AT-ANL-08, AT-ANL-09, AT-ANL-10): every finding of
// ls100/bad with its message and chain, none in ls100/good, and the diagnostic format.
func TestDeterminism(t *testing.T) {
	runCorpus(t, Determinism, "ls100/bad", "ls100/good")
}

// TestATANL07AllowScope checks the allow scopes on LS100 findings (AT-ANL-07), and that
// Directives reports exactly one problem in the same package: the allow without a reason.
func TestATANL07AllowScope(t *testing.T) {
	runCorpus(t, Determinism, "allow")

	// The want comments of the package are for Determinism, so analysistest's own checks fail
	// for Directives; the recorder keeps them away from t.
	var rec recorder
	results := analysistest.Run(&rec, analysistest.TestData(), Directives, "allow")
	if len(results) != 1 {
		t.Fatalf("Directives on allow: %d results, want 1 (errors: %q)", len(results), rec.errors)
	}
	res := results[0]
	if len(res.Diagnostics) != 1 {
		t.Fatalf("Directives on allow: %d diagnostics, want 1: %v", len(res.Diagnostics), res.Diagnostics)
	}
	d := res.Diagnostics[0]
	if want := "LS001: //lockstep:allow LS100 needs a reason"; d.Message != want {
		t.Errorf("Directives on allow: message %q, want %q", d.Message, want)
	}
	pos := res.Pass.Fset.Position(d.Pos)
	src, err := os.ReadFile(pos.Filename)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(src), "\n")
	if got := strings.TrimSpace(lines[pos.Line-1]); got != "//lockstep:allow LS100" {
		t.Errorf("Directives on allow: diagnostic on line %d %q, want the allow without a reason", pos.Line, got)
	}
}

// recorder is an analysistest.Testing that records errors instead of failing a test.
type recorder struct {
	errors []string
}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

// TestSubRules checks the LS100 sub-rule table against ANL-130: names in the table's order, each
// once, with the origin of its rule (ANL-150).
func TestSubRules(t *testing.T) {
	want := []subRule{
		{"wallclock", originFaultline},
		{"globalrand", originFaultline},
		{"cryptorand", originFaultline},
		{"goroutine", originFaultline},
		{"select", originFaultline},
		{"maprange", originFaultline},
		{"unordered", originFaultline},
		{"mapiter", originFaultline},
		{"env", originFaultline},
		{"blocking", originLockstep},
	}
	if !slices.Equal(ls100SubRules, want) {
		t.Errorf("ls100SubRules = %v, want %v", ls100SubRules, want)
	}
}
