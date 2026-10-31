// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// baselineFile is the committed baseline, as a path from this package's directory.
const baselineFile = "../../results/baseline.txt"

// derived describes a committed file that one budget command writes from the baseline: all of it,
// or the part between the marker lines of README.md.
type derived struct {
	command  string // the budget command that writes the file
	file     string // the file, as a path from this package's directory
	repoPath string // the file, as a path from the repository root
	inPlace  bool   // whether the command rewrites the file, given after the baseline, instead of printing it
}

// The files that must always equal the output of budget on the baseline (BEN-190), and README.md,
// whose benchmark table must equal the output of budget readme.
var (
	budgetsFile = derived{"derive", "../../budgets.txt", "bench/budgets.txt", false}
	reportFile  = derived{"report", "../../RESULTS.md", "bench/RESULTS.md", false}
	readmeFile  = derived{"readme", "../../../README.md", "README.md", true}
)

// load returns the committed text of the file and the text that the budget command writes for the
// committed baseline.
func (d derived) load(t *testing.T) (have, want string) {
	t.Helper()
	committed, err := os.ReadFile(d.file)
	if err != nil {
		t.Fatal(err)
	}
	return string(committed), d.generate(t, string(committed))
}

// generate returns the text that the budget command writes for the committed baseline, which it
// runs in this process. A command that rewrites the file runs on a copy that holds text.
func (d derived) generate(t *testing.T, text string) string {
	t.Helper()
	args := []string{d.command, baselineFile}
	var copied string
	if d.inPlace {
		copied = filepath.Join(t.TempDir(), filepath.Base(d.file))
		if err := os.WriteFile(copied, []byte(text), 0o600); err != nil { //nolint:gosec // test writes under t.TempDir()
			t.Fatal(err)
		}
		args = append(args, copied)
	}
	code, stdout, stderr := budgetRun(t, args...)
	if code != 0 || stderr != "" {
		t.Fatalf("budget %s: exit code %d, stderr %q", strings.Join(args, " "), code, stderr)
	}
	if !d.inPlace {
		return stdout
	}
	if stdout != "" {
		t.Fatalf("budget %s printed %q, want nothing", strings.Join(args, " "), stdout)
	}
	rewritten, err := os.ReadFile(copied)
	if err != nil {
		t.Fatal(err)
	}
	return string(rewritten)
}

// invocation returns the budget command and its arguments, as run from bench/.
func (d derived) invocation() string {
	if d.inPlace {
		return d.command + " results/baseline.txt ../" + d.repoPath
	}
	return d.command + " results/baseline.txt"
}

// regenerate returns the command, run from the repository root, that rewrites the file.
func (d derived) regenerate() string {
	if d.inPlace {
		return "go -C bench run ./cmd/budget " + d.invocation()
	}
	return "go -C bench run ./cmd/budget " + d.invocation() + " > " + d.repoPath
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
	return fmt.Errorf("%s differs from what budget %s writes, at line %d:\n"+
		"  have: %s\n  want: %s\n"+
		"regenerate it with: %s",
		d.repoPath, d.invocation(), n+1, lineAt(haveLines, n), lineAt(wantLines, n), d.regenerate())
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
// The table between the marker lines of README.md must also be what budget readme writes for
// that baseline, and a changed line of it fails.
func TestBudgetsMatchBaseline(t *testing.T) {
	for _, d := range []derived{budgetsFile, reportFile, readmeFile} {
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

	t.Run("a changed line of README.md's table fails", func(t *testing.T) {
		d := readmeFile
		have, want := d.load(t)
		if d.compare(have, want) != nil {
			t.Skip("README.md is out of sync, which the test above reports")
		}
		i := strings.Index(have, readmeIntro)
		if i < 0 {
			t.Fatalf("README.md lacks %q", readmeIntro)
		}
		changed := have[:i] + "Higher" + have[i+len("Lower"):]
		wantLine := "at line " + strconv.Itoa(1+strings.Count(have[:i], "\n")) + ":"
		err := d.compare(changed, d.generate(t, changed))
		if err == nil || !strings.Contains(err.Error(), d.repoPath) || !strings.Contains(err.Error(), wantLine) {
			t.Errorf("a changed line of README.md's table gave %v, want an error naming %s and %q", err, d.repoPath, wantLine)
		}
	})
}
