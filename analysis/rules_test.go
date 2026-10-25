// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const rulesSnapshotPath = "testdata/faultline/determinism-rules.md"

// TestRulesMatchFaultline checks that the snapshot of faultline's determinism rules, the
// faultline version the root module requires and faultlineRules agree (ANL-150, ANL-160).
func TestRulesMatchFaultline(t *testing.T) {
	snapshot, err := os.ReadFile(rulesSnapshotPath)
	if err != nil {
		t.Fatalf("reading the snapshot: %v", err)
	}
	rootGoMod, err := os.ReadFile(filepath.Join("..", "go.mod"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reading the root go.mod: %v", err)
	}
	if err := compareRules(snapshot, rootGoMod); err != nil {
		t.Fatal(err)
	}
}

// cloneRules copies the rule entries deeply enough to change one in a test.
func cloneRules(rules []faultlineRule) []faultlineRule {
	out := slices.Clone(rules)
	for i := range out {
		out[i].subRules = slices.Clone(out[i].subRules)
	}
	return out
}

// TestCompareRules checks each ANL-160 error, in order, on modified inputs (AT-ANL-11).
func TestCompareRules(t *testing.T) {
	raw, err := os.ReadFile(rulesSnapshotPath)
	if err != nil {
		t.Fatalf("reading the snapshot: %v", err)
	}
	snapshot := string(raw)
	const rootHeader = "module github.com/hmdsefi/lockstep\n\ngo 1.26.0\n\n"
	rootMatching := []byte(rootHeader + "require github.com/hmdsefi/faultline " + faultlineVersion + "\n")

	// edit returns the snapshot with the first occurrence of old replaced; it fails when old is
	// missing, so a changed snapshot cannot turn a case into a no-op.
	edit := func(t *testing.T, old, repl string) []byte {
		t.Helper()
		if !strings.Contains(snapshot, old) {
			t.Fatalf("the snapshot does not contain %q", old)
		}
		return []byte(strings.Replace(snapshot, old, repl, 1))
	}

	// Indexes into faultlineRules are 0-based; the errors number the rules from 1.
	cases := []struct {
		name     string
		snapshot func(t *testing.T) []byte
		root     []byte
		rules    func() []faultlineRule
		want     string
	}{
		{
			name:     "match",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     rootMatching,
		},
		{
			name:     "no root go.mod",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
		},
		{
			name: "error 1: snapshot version",
			snapshot: func(t *testing.T) []byte {
				return edit(t, `faultline `+faultlineVersion+` AGENTS.md`, `faultline v0.1.3 AGENTS.md`)
			},
			root: rootMatching,
			want: "analysis: snapshot is faultline v0.1.3, rules.go pins " + faultlineVersion,
		},
		{
			name:     "error 2: root module version",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     []byte(rootHeader + "require github.com/hmdsefi/faultline v0.1.3\n"),
			want: "analysis: root module requires faultline v0.1.3, rules.go pins " + faultlineVersion +
				"; copy the \"Determinism rules\" section of faultline v0.1.3 into " +
				"analysis/testdata/faultline/determinism-rules.md and review LS100",
		},
		{
			name:     "error 2 is skipped for another module",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root: []byte("module example.com/other\n\ngo 1.26.0\n\n" +
				"require github.com/hmdsefi/faultline v0.1.3\n"),
		},
		{
			name:     "error 3: ninth bullet",
			snapshot: func(*testing.T) []byte { return []byte(snapshot + "- a ninth rule.\n") },
			root:     rootMatching,
			want:     "analysis: snapshot has 9 faultline rules, rules.go has 8",
		},
		{
			name: "errors 1 and 3: the first failing check wins",
			snapshot: func(t *testing.T) []byte {
				changed := edit(t, `faultline `+faultlineVersion+` AGENTS.md`, `faultline v0.1.3 AGENTS.md`)
				return append(changed, "- a ninth rule.\n"...)
			},
			root: rootMatching,
			want: "analysis: snapshot is faultline v0.1.3, rules.go pins " + faultlineVersion,
		},
		{
			name: "error 4: one word of bullet 4 changed",
			snapshot: func(t *testing.T) []byte {
				return edit(t, "never ranges over a map", "never iterates over a map")
			},
			root: rootMatching,
			want: "analysis: faultline rule 4 changed:\n" +
				"  snapshot: " + strings.Replace(faultlineRules[3].text, "never ranges", "never iterates", 1) + "\n" +
				"  rules.go: " + faultlineRules[3].text,
		},
		{
			name:     "error 5: adopted rule without a sub-rule",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     rootMatching,
			rules: func() []faultlineRule {
				rules := cloneRules(faultlineRules)
				rules[0].subRules = nil
				return rules
			},
			want: "analysis: faultline rule 1 is adopted but names no sub-rule",
		},
		{
			name:     "error 5: adapted rule without a sub-rule",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     rootMatching,
			rules: func() []faultlineRule {
				rules := cloneRules(faultlineRules)
				rules[5].subRules = nil
				return rules
			},
			want: "analysis: faultline rule 6 is adapted but names no sub-rule",
		},
		{
			name:     "error 6: excluded rule without a reason",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     rootMatching,
			rules: func() []faultlineRule {
				rules := cloneRules(faultlineRules)
				rules[4].reason = ""
				return rules
			},
			want: "analysis: faultline rule 5 is excluded without a reason",
		},
		{
			// Removing env from rule 6 would leave an adapted rule with no sub-rule (error 5), so
			// the case excludes rule 6 with a reason instead, which leaves env unmapped.
			name:     "error 7: sub-rule env is not mapped",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     rootMatching,
			rules: func() []faultlineRule {
				rules := cloneRules(faultlineRules)
				rules[5].disposition = dispositionExcluded
				rules[5].subRules = nil
				rules[5].reason = "test"
				return rules
			},
			want: "analysis: LS100 sub-rule env is not mapped to a faultline rule",
		},
		{
			name:     "error 7: sub-rule is mapped twice",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     rootMatching,
			rules: func() []faultlineRule {
				rules := cloneRules(faultlineRules)
				rules[0].subRules = append(rules[0].subRules, "env")
				return rules
			},
			want: "analysis: LS100 sub-rule env is mapped to more than one faultline rule",
		},
		{
			name:     "blocking needs no faultline rule",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     rootMatching,
			rules: func() []faultlineRule {
				rules := cloneRules(faultlineRules)
				rules[0].subRules = append(rules[0].subRules, "blocking")
				return rules
			},
		},
		{
			name: "malformed header",
			snapshot: func(*testing.T) []byte {
				return []byte("Determinism rules\n\n- a rule.\n")
			},
			root: rootMatching,
			want: `analysis: snapshot header "Determinism rules" is not 'faultline <version> AGENTS.md "Determinism rules"'`,
		},
		{
			name:     "malformed root go.mod",
			snapshot: func(*testing.T) []byte { return []byte(snapshot) },
			root:     []byte("module\n"),
			want:     "analysis: reading the root go.mod:",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rules := faultlineRules
			if tc.rules != nil {
				rules = tc.rules()
			}
			err := compareRulesWith(tc.snapshot(t), tc.root, faultlineVersion, rules, ls100SubRules)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("got error %q, want nil", err)
			case tc.want == "" && err == nil:
			case err == nil:
				t.Fatalf("got nil, want error %q", tc.want)
			case strings.HasSuffix(tc.want, ":"):
				if !strings.HasPrefix(err.Error(), tc.want) {
					t.Fatalf("got error %q, want prefix %q", err, tc.want)
				}
			case err.Error() != tc.want:
				t.Fatalf("got error:\n%s\nwant:\n%s", err, tc.want)
			}
		})
	}
}

// TestCompareRulesEntryPoint checks that compareRules uses the pinned version, entries and
// sub-rule table, so a changed pin fails the repository check.
func TestCompareRulesEntryPoint(t *testing.T) {
	raw, err := os.ReadFile(rulesSnapshotPath)
	if err != nil {
		t.Fatalf("reading the snapshot: %v", err)
	}
	changed := strings.Replace(string(raw), "faultline "+faultlineVersion+" AGENTS.md", "faultline v0.1.3 AGENTS.md", 1)
	err = compareRules([]byte(changed), nil)
	if err == nil || !strings.HasPrefix(err.Error(), "analysis: snapshot is faultline v0.1.3, rules.go pins ") {
		t.Fatalf("got %v, want the version error", err)
	}
}
