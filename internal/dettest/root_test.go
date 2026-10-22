// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"strings"
	"testing"
)

// findingsMessage returns the failure text of DET-070 and DET-100: "<prefix>: <n> finding(s):" and
// one Finding.String() per line.
func findingsMessage(prefix string, fs []Finding) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d finding(s):", prefix, len(fs))
	for _, f := range fs {
		b.WriteString("\n" + f.String())
	}
	return b.String()
}

func TestFindingsMessage(t *testing.T) {
	fs := []Finding{
		{File: "a.go", Line: 3, Col: 9, Rule: RuleWallClock, Msg: "m1"},
		{File: "extra", Rule: RuleMeta, Msg: "m2"},
	}
	want := "determinism lint: 2 finding(s):\na.go:3:9: DL001: m1\nextra:0:0: DL000: m2"
	if got := findingsMessage("determinism lint", fs); got != want {
		t.Errorf("message %q, want %q", got, want)
	}
	if got, want := findingsMessage("import boundaries", nil), "import boundaries: 0 finding(s):"; got != want {
		t.Errorf("message %q, want %q", got, want)
	}
}

// AT-DET-05 (repository clause), DET-070
func TestDeterminismLint(t *testing.T) {
	fs, err := Lint(Lockstep(repoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) > 0 {
		t.Error(findingsMessage("determinism lint", fs))
	}
}

// AT-DET-05 (repository clause), AT-REP-08, DET-100
func TestImportBoundaries(t *testing.T) {
	fs, err := CheckImports(Lockstep(repoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) > 0 {
		t.Error(findingsMessage("import boundaries", fs))
	}
}
