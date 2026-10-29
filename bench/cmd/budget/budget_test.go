// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// budgetRun runs the budget command with args and returns its exit code, stdout and stderr.
func budgetRun(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut strings.Builder
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// writeFile writes lines, each ending in a newline, to a new file and returns its path.
func writeFile(t *testing.T, name string, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	text := ""
	for _, l := range lines {
		text += l + "\n"
	}
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// wantRun checks the exit code and both outputs of one budget run.
func wantRun(t *testing.T, args []string, wantCode int, wantStdout, wantStderr string) {
	t.Helper()
	code, stdout, stderr := budgetRun(t, args...)
	if code != wantCode {
		t.Errorf("exit code = %d, want %d", code, wantCode)
	}
	if stdout != wantStdout {
		t.Errorf("stdout:\n%s\nwant:\n%s", stdout, wantStdout)
	}
	if stderr != wantStderr {
		t.Errorf("stderr:\n%s\nwant:\n%s", stderr, wantStderr)
	}
}

// lines joins lines, each ending in a newline.
func lines(l ...string) string {
	if len(l) == 0 {
		return ""
	}
	return strings.Join(l, "\n") + "\n"
}

// TestATBEN08Parsing covers AT-BEN-08: configuration lines are kept, test output lines are
// ignored, a result line matches its name without the GOMAXPROCS suffix, and a value that is not
// a number fails with the file and line (BEN-140).
func TestATBEN08Parsing(t *testing.T) {
	parse := filepath.Join("testdata", "parse.txt")

	t.Run("result lines match without the GOMAXPROCS suffix", func(t *testing.T) {
		budgets := writeFile(t, "budgets.txt",
			"BenchmarkX/lib=lockstep ns/op abs 512",
			"BenchmarkX/lib=lockstep B/op abs 0",
			"BenchmarkX/lib=lockstep allocs/op abs 0",
			"BenchmarkSimSeeds/workload=crash3 ns/op abs 20000",
		)
		wantRun(t, []string{"check", "-budgets", budgets, parse}, 0, lines(
			"ok   BenchmarkX/lib=lockstep ns/op 512 <= 512",
			"ok   BenchmarkX/lib=lockstep B/op 0 <= 0",
			"ok   BenchmarkX/lib=lockstep allocs/op 0 <= 0",
			"ok   BenchmarkSimSeeds/workload=crash3 ns/op 20000 <= 20000",
		), "")
	})

	t.Run("configuration lines are kept", func(t *testing.T) {
		res, err := readResults([]string{parse})
		if err != nil {
			t.Fatal(err)
		}
		want := []config{
			{"goos", "linux"},
			{"goarch", "amd64"},
			{"pkg", "github.com/hmdsefi/lockstep/bench"},
			{"cpu", "AMD EPYC 7763 64-Core Processor"},
			{"goos", "linux"},
			{"goarch", "amd64"},
			{"pkg", "github.com/hmdsefi/lockstep/bench"},
		}
		if !slices.Equal(res.config, want) {
			t.Errorf("config = %q, want %q", res.config, want)
		}
	})

	t.Run("invalid value", func(t *testing.T) {
		budgets := writeFile(t, "budgets.txt", "BenchmarkY ns/op abs 1")
		malformed := filepath.Join("testdata", "malformed.txt")
		wantRun(t, []string{"check", "-budgets", budgets, malformed}, 2, "",
			`budget: `+malformed+`:3: invalid value "abc"`+"\n")
	})
}

// TestResultsMalformed checks that every result line that does not parse fails with the file,
// the line and the reason, among them values that are not finite (BEN-140, BEN §7).
func TestResultsMalformed(t *testing.T) {
	for _, c := range []struct {
		line, reason string
	}{
		{"BenchmarkY", "missing iteration count"},
		{"BenchmarkY x 1 ns/op", `invalid iteration count "x"`},
		{"BenchmarkY 0 1 ns/op", `invalid iteration count "0"`},
		{"BenchmarkY-8 \t--- FAIL: BenchmarkY-8", `invalid iteration count "---"`},
		{"BenchmarkY 10", "missing values"},
		{"BenchmarkY 10 512", `value "512" has no unit`},
		{"BenchmarkY 10 512 ns/op 3", `value "3" has no unit`},
		{"BenchmarkY 10 512 ns/op abc B/op", `invalid value "abc"`},
		{"BenchmarkY 10 NaN ns/op", `invalid value "NaN"`},
		{"BenchmarkY 10 +Inf ns/op", `invalid value "+Inf"`},
		{"BenchmarkY 10 512 ns/op -Inf B/op", `invalid value "-Inf"`},
		{"BenchmarkY 10 inf ns/op", `invalid value "inf"`},
		{"BenchmarkY 10 1e400 ns/op", `invalid value "1e400"`},
	} {
		t.Run(c.line, func(t *testing.T) {
			budgets := writeFile(t, "budgets.txt", "BenchmarkY ns/op abs 1")
			results := writeFile(t, "results.txt", "goos: linux", c.line)
			wantRun(t, []string{"check", "-budgets", budgets, results}, 2, "",
				"budget: "+results+":2: "+c.reason+"\n")
		})
	}

	t.Run("missing results file", func(t *testing.T) {
		budgets := writeFile(t, "budgets.txt", "BenchmarkY ns/op abs 1")
		missing := filepath.Join(t.TempDir(), "results.txt")
		code, stdout, stderr := budgetRun(t, "check", "-budgets", budgets, missing)
		if code != 2 || stdout != "" || !strings.HasPrefix(stderr, "budget: ") || !strings.Contains(stderr, missing) {
			t.Errorf("exit code %d, stdout %q, stderr %q; want 2, nothing, an error naming %s", code, stdout, stderr, missing)
		}
	})
}

// TestATBEN09Check covers AT-BEN-09: medians over all results files, the ok and FAIL lines of abs
// and rel budgets, a missing result, and a missing budgets file (BEN-150, BEN-160).
func TestATBEN09Check(t *testing.T) {
	results := []string{filepath.Join("testdata", "check-results.txt"), filepath.Join("testdata", "check-sim.txt")}
	check := func(t *testing.T, budgetLines []string, wantCode int, wantStdout ...string) {
		t.Helper()
		budgets := writeFile(t, "budgets.txt", budgetLines...)
		wantRun(t, append([]string{"check", "-budgets", budgets}, results...), wantCode, lines(wantStdout...), "")
	}

	t.Run("budgets hold", func(t *testing.T) {
		check(t, []string{
			"BenchmarkA ns/op rel BenchmarkR ns/op 1.90",
			"BenchmarkC allocs/op abs 2",
		}, 0,
			"ok   BenchmarkA ns/op 100 / 55 = 1.82 <= 1.90",
			"ok   BenchmarkC allocs/op 2 <= 2",
		)
	})

	t.Run("budget exceeded", func(t *testing.T) {
		check(t, []string{
			"BenchmarkA ns/op rel BenchmarkR ns/op 1.80",
			"BenchmarkC allocs/op abs 2",
		}, 1,
			"FAIL BenchmarkA ns/op 100 / 55 = 1.82 > 1.80",
			"ok   BenchmarkC allocs/op 2 <= 2",
		)
	})

	t.Run("abs budget exceeded", func(t *testing.T) {
		check(t, []string{"BenchmarkC allocs/op abs 1.5"}, 1,
			"FAIL BenchmarkC allocs/op 2 > 1.5",
		)
	})

	t.Run("missing result", func(t *testing.T) {
		check(t, []string{
			"BenchmarkA ns/op rel BenchmarkR ns/op 1.90",
			"BenchmarkD ns/op abs 1",
			"BenchmarkC allocs/op abs 2",
		}, 2,
			"ok   BenchmarkA ns/op 100 / 55 = 1.82 <= 1.90",
			"ok   BenchmarkC allocs/op 2 <= 2",
			"budget: missing result BenchmarkD ns/op",
		)
	})

	t.Run("missing result and budget exceeded", func(t *testing.T) {
		check(t, []string{
			"BenchmarkD ns/op abs 1",
			"BenchmarkA ns/op rel BenchmarkR ns/op 1.80",
		}, 2,
			"FAIL BenchmarkA ns/op 100 / 55 = 1.82 > 1.80",
			"budget: missing result BenchmarkD ns/op",
		)
	})

	t.Run("missing reference, unit and repeats", func(t *testing.T) {
		check(t, []string{
			"BenchmarkA ns/op rel BenchmarkZ ns/op 2",
			"BenchmarkC p99-ns abs 1",
			"BenchmarkC allocs/op abs 2",
			"BenchmarkR ns/op rel BenchmarkZ ns/op 2",
			"BenchmarkD ns/op rel BenchmarkZ ns/op 2",
		}, 2,
			"ok   BenchmarkC allocs/op 2 <= 2",
			"budget: missing result BenchmarkZ ns/op",
			"budget: missing result BenchmarkC p99-ns",
			"budget: missing result BenchmarkD ns/op",
		)
	})

	t.Run("comments and blank lines", func(t *testing.T) {
		check(t, []string{
			"# allocations",
			"",
			"BenchmarkC allocs/op abs 2",
			"",
		}, 0,
			"ok   BenchmarkC allocs/op 2 <= 2",
		)
	})

	t.Run("no budgets", func(t *testing.T) {
		check(t, []string{"# none yet"}, 0)
	})

	t.Run("no budgets file", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "budgets.txt")
		wantRun(t, append([]string{"check", "-budgets", missing}, results...), 0,
			"budget: no budgets file; nothing enforced\n", "")
	})

	t.Run("no budgets file and malformed results", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "budgets.txt")
		malformed := filepath.Join("testdata", "malformed.txt")
		wantRun(t, []string{"check", "-budgets", missing, malformed}, 2, "",
			`budget: `+malformed+`:3: invalid value "abc"`+"\n")
	})

	t.Run("unreadable budgets file", func(t *testing.T) {
		dir := t.TempDir()
		code, stdout, stderr := budgetRun(t, append([]string{"check", "-budgets", dir}, results...)...)
		if code != 2 || stdout != "" || !strings.HasPrefix(stderr, "budget: ") || !strings.Contains(stderr, dir) {
			t.Errorf("exit code %d, stdout %q, stderr %q; want 2, nothing, an error naming %s", code, stdout, stderr, dir)
		}
	})
}

// TestCheckTolerance checks that comparisons use the unrounded median and ratio with a tolerance
// of 1e-9 (BEN-150, BEN §11): 0.15000000000000002 holds for 0.15 and 5.7 / 3 =
// 1.9000000000000001 holds for 1.9, while a budget 1e-6 below the median fails.
func TestCheckTolerance(t *testing.T) {
	budgets := writeFile(t, "budgets.txt",
		"BenchmarkT ns/op abs 0.15",
		"BenchmarkU ns/op rel BenchmarkV ns/op 1.9",
		"BenchmarkT ns/op abs 0.149999",
	)
	wantRun(t, []string{"check", "-budgets", budgets, filepath.Join("testdata", "boundary.txt")}, 1, lines(
		"ok   BenchmarkT ns/op 0.15000000000000002 <= 0.15",
		"ok   BenchmarkU ns/op 5.7 / 3 = 1.90 <= 1.9",
		"FAIL BenchmarkT ns/op 0.15000000000000002 > 0.149999",
	), "")
}

// TestCheckUnroundedRatio checks that a rel budget compares the unrounded ratio: 18249 / 10000
// prints as 1.82 but exceeds a max-ratio of 1.82 (BEN-150).
func TestCheckUnroundedRatio(t *testing.T) {
	budgets := writeFile(t, "budgets.txt", "BenchmarkA ns/op rel BenchmarkR ns/op 1.82")
	results := writeFile(t, "results.txt", "BenchmarkA-4 1 18249 ns/op", "BenchmarkR-4 1 10000 ns/op")
	wantRun(t, []string{"check", "-budgets", budgets, results}, 1,
		"FAIL BenchmarkA ns/op 18249 / 10000 = 1.82 > 1.82\n", "")
}

// TestTrimProcs checks that trimProcs removes only a suffix of a dash and digits, the one go test
// appends to a benchmark name.
func TestTrimProcs(t *testing.T) {
	for _, c := range []struct{ name, want string }{
		{"BenchmarkX-8", "BenchmarkX"},
		{"BenchmarkX/lib=lockstep/n=10000-16", "BenchmarkX/lib=lockstep/n=10000"},
		{"BenchmarkX", "BenchmarkX"},
		{"BenchmarkX/a-b", "BenchmarkX/a-b"},
		{"BenchmarkX-", "BenchmarkX-"},
		{"BenchmarkX-4a", "BenchmarkX-4a"},
		{"BenchmarkX/n=-1-4", "BenchmarkX/n=-1"},
	} {
		if got := trimProcs(c.name); got != c.want {
			t.Errorf("trimProcs(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestBudgetsMalformed checks that every budgets line outside the grammar fails with the file,
// the line and the reason (BEN-150, BEN §7).
func TestBudgetsMalformed(t *testing.T) {
	units := "ns/op, p50-ns, p99-ns, allocs/op, B/op, B/actor, goroutines/actor"
	for _, c := range []struct {
		line, reason string
	}{
		{"BenchmarkA", "missing kind, want abs or rel"},
		{"BenchmarkA ns/op", "missing kind, want abs or rel"},
		{"BenchmarkA ns/op max 2", `unknown kind "max", want abs or rel`},
		{"BenchmarkA ns/op abs", "abs budget has 3 fields, want 4"},
		{"BenchmarkA ns/op abs 2 3", "abs budget has 5 fields, want 4"},
		{"BenchmarkA ns/op rel BenchmarkR ns/op", "rel budget has 5 fields, want 6"},
		{"BenchmarkA ns/op rel BenchmarkR ns/op 2 3", "rel budget has 7 fields, want 6"},
		{"BenchmarkA msgs/s abs 2", `unit "msgs/s" cannot have a budget, want one of ` + units},
		{"BenchmarkA seeds/s rel BenchmarkR seeds/s 2", `unit "seeds/s" cannot have a budget, want one of ` + units},
		{"BenchmarkA ns/op abs x", `invalid max "x"`},
		{"BenchmarkA ns/op abs -1", `invalid max "-1"`},
		{"BenchmarkA ns/op abs NaN", `invalid max "NaN"`},
		{"BenchmarkA ns/op abs +Inf", `invalid max "+Inf"`},
		{"BenchmarkA ns/op rel BenchmarkR ns/op x", `invalid max-ratio "x"`},
		{"BenchmarkA  ns/op abs 2", "fields must be separated by single spaces"},
		{"BenchmarkA ns/op abs 2 ", "fields must be separated by single spaces"},
		{" BenchmarkA ns/op abs 2", "fields must be separated by single spaces"},
		{"BenchmarkA\tns/op abs 2", "fields must be separated by single spaces"},
	} {
		t.Run(c.line, func(t *testing.T) {
			budgets := writeFile(t, "budgets.txt", "# budgets", "", c.line)
			results := filepath.Join("testdata", "check-results.txt")
			wantRun(t, []string{"check", "-budgets", budgets, results}, 2, "",
				"budget: "+budgets+":3: "+c.reason+"\n")
		})
	}

	t.Run("CRLF line endings", func(t *testing.T) {
		budgets := filepath.Join(t.TempDir(), "budgets.txt")
		if err := os.WriteFile(budgets, []byte("# budgets\r\nBenchmarkC allocs/op abs 2\r\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		wantRun(t, []string{"check", "-budgets", budgets, filepath.Join("testdata", "check-results.txt")}, 0,
			"ok   BenchmarkC allocs/op 2 <= 2\n", "")
	})
}

// TestUsage checks the usage errors, which go to stderr with exit code 2, and the help request.
func TestUsage(t *testing.T) {
	budgets := writeFile(t, "budgets.txt", "BenchmarkC allocs/op abs 2")
	for _, c := range []struct {
		name       string
		args       []string
		wantCode   int
		wantStderr string
	}{
		{"no command", nil, 2, usage},
		{"unknown command", []string{"derive", "baseline.txt"}, 2, `budget: unknown command "derive"` + "\n" + usage},
		{"help", []string{"-h"}, 0, usage},
		{"check help", []string{"check", "-h"}, 0, usage},
		{"check without budgets", []string{"check", "results.txt"}, 2, "budget: missing -budgets flag\n" + usage},
		{"check without results", []string{"check", "-budgets", budgets}, 2, "budget: missing results files\n" + usage},
		{"check with an unknown flag", []string{"check", "-x"}, 2, "budget: flag provided but not defined: -x\n" + usage},
	} {
		t.Run(c.name, func(t *testing.T) {
			wantRun(t, c.args, c.wantCode, "", c.wantStderr)
		})
	}
}
