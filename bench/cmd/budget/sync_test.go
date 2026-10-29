// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

// baselineFile is the committed baseline, as a path from this package's directory.
const baselineFile = "../../results/baseline.txt"

// derived describes a committed file that is the output of one budget command on the baseline.
type derived struct {
	command  string // the budget command that writes the file
	file     string // the file, as a path from this package's directory
	repoPath string // the file, as a path from the repository root
}

// The files that must always equal the output of budget on the baseline (BEN-190).
var (
	budgetsFile = derived{"derive", "../../budgets.txt", "bench/budgets.txt"}
	reportFile  = derived{"report", "../../RESULTS.md", "bench/RESULTS.md"}
)

// load returns the committed text of the file and the text that the budget command writes for the
// committed baseline, which it runs in this process.
func (d derived) load(t *testing.T) (have, want string) {
	t.Helper()
	committed, err := os.ReadFile(d.file)
	if err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := budgetRun(t, d.command, baselineFile)
	if code != 0 || stderr != "" {
		t.Fatalf("budget %s %s: exit code %d, stderr %q", d.command, baselineFile, code, stderr)
	}
	return string(committed), stdout
}

// compare returns nil when have, the text of the committed file, equals want byte for byte.
// Otherwise it returns an error that names the file and the first line that differs, shows both
// versions of that line, and says how to regenerate the file.
func (d derived) compare(have, want string) error {
	if have == want {
		return nil
	}
	haveLines, wantLines := splitLines(have), splitLines(want)
	n := 0
	for n < len(haveLines) && n < len(wantLines) && haveLines[n] == wantLines[n] {
		n++
	}
	return fmt.Errorf("%s differs from the output of budget %s results/baseline.txt at line %d:\n"+
		"  have: %s\n  want: %s\n"+
		"regenerate it with: go -C bench run ./cmd/budget %[2]s results/baseline.txt > %[1]s",
		d.repoPath, d.command, n+1, lineAt(haveLines, n), lineAt(wantLines, n))
}

// splitLines splits text into lines that keep their newline, so a missing final newline shows.
func splitLines(text string) []string {
	lines := strings.SplitAfter(text, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// lineAt returns line i quoted, or a marker when the text has no such line.
func lineAt(lines []string, i int) string {
	if i >= len(lines) {
		return "<end of file>"
	}
	return strconv.Quote(lines[i])
}

// TestBudgetsMatchBaseline covers AT-BEN-11 (sync half) and BEN-190: bench/budgets.txt and
// bench/RESULTS.md are the output of budget derive and budget report on
// bench/results/baseline.txt, byte for byte, and a budgets file that differs by one byte fails.
func TestBudgetsMatchBaseline(t *testing.T) {
	for _, d := range []derived{budgetsFile, reportFile} {
		t.Run(d.command, func(t *testing.T) {
			have, want := d.load(t)
			if err := d.compare(have, want); err != nil {
				t.Error(err)
			}
		})
	}

	t.Run("a changed byte of budgets.txt fails", func(t *testing.T) {
		d := budgetsFile
		have, want := d.load(t)
		if d.compare(have, want) != nil {
			t.Skip("budgets.txt is out of sync, which the test above reports")
		}
		for i := range have {
			changed := []byte(have)
			changed[i] ^= 1
			wantLine := "at line " + strconv.Itoa(1+strings.Count(have[:i], "\n")) + ":"
			err := d.compare(string(changed), want)
			if err == nil {
				t.Fatalf("changing byte %d of budgets.txt did not fail the comparison", i)
			}
			if !strings.Contains(err.Error(), d.repoPath) || !strings.Contains(err.Error(), wantLine) {
				t.Fatalf("changing byte %d of budgets.txt gave %q, want a message naming %s and %q",
					i, err, d.repoPath, wantLine)
			}
		}
		for name, changed := range map[string]string{
			"a missing last byte": have[:len(have)-1],
			"an extra line":       have + "x\n",
		} {
			if d.compare(changed, want) == nil {
				t.Errorf("%s of budgets.txt did not fail the comparison", name)
			}
		}
	})
}
