// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// readmeHeader holds the header lines of the baselines of the readme tests.
var readmeHeader = []string{
	"# commit: 0f3a9c1d5b7e2a4c6d8e0f1a2b3c4d5e6f708192",
	"# date: 2026-11-03",
	"cpu: AMD EPYC 7763 64-Core Processor",
}

// readmeResults holds a result line for the lockstep benchmark of every headline, and some of the
// other libraries.
var readmeResults = []string{
	"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 783.7 ns/op 1 allocs/op",
	"BenchmarkPingPong/lib=protoactor/pairs=1-4 100 2758.5 ns/op 1 allocs/op",
	"BenchmarkPingPong/lib=goakt/pairs=1-4 100 431.85 ns/op 0 allocs/op",
	"BenchmarkPingPong/lib=lockstep/pairs=procs-4 100 330.3 ns/op 1 allocs/op",
	"BenchmarkFanOut/lib=lockstep/consumers=1024-4 100 1234567.4 ns/op 3 allocs/op",
	"BenchmarkIdleActors/lib=lockstep/n=100000-4 1 264596463 ns/op 2025 B/actor 9 ns/actor",
	"BenchmarkIdleActors/lib=lockstep/n=100000-4 1 264596463 ns/op 2026 B/actor 9 ns/actor",
	"BenchmarkIdleActors/lib=goakt/n=100000-4 1 4957887550 ns/op 1237 B/actor 9 ns/actor",
	"BenchmarkSendLatency/lib=lockstep-4 10 1406 ns/op 761 p50-ns 12250 p99-ns",
	"BenchmarkSendLatency/lib=hollywood-4 10 1406 ns/op 331 p50-ns 10000 p99-ns",
	"BenchmarkAskLatency/lib=lockstep/callers=1-4 10 3956 ns/op 2835 p50-ns 16160 p99-ns",
	"BenchmarkAskLatency/lib=hollywood/callers=1-4 10 3956 ns/op 1749 p50-ns 9999.9 p99-ns",
}

// readmeBlock is the block that budget readme writes for readmeHeader and readmeResults.
var readmeBlock = strings.Join([]string{
	"Lower is better in every row.",
	"| Result | lockstep | protoactor-go | Hollywood | GoAkt |\n" +
		"|---|---:|---:|---:|---:|\n" +
		"| Ping-pong, 1 pair (ns per message) | 784 | 2,759 | n/a | 432 |\n" +
		"| Ping-pong, `GOMAXPROCS` pairs (ns per message) | 330 | n/a | n/a | n/a |\n" +
		"| Fan-out to 1,024 consumers (µs per item) | 1,234.6 | n/a | n/a | n/a |\n" +
		"| 100,000 idle actors (heap bytes per actor) | 2,025.5 | n/a | n/a | 1,237 |\n" +
		"| Spawning 100,000 actors and sending each a message (ns per actor) | 2,646 | n/a | n/a | 49,579 |\n" +
		"| Send latency, p50 (ns) | 761 | n/a | 331 | n/a |\n" +
		"| Send latency, p99 (µs) | 12.3 | n/a | 10.0 | n/a |\n" +
		"| Ask latency, 1 caller, p50 (ns) | 2,835 | n/a | 1,749 | n/a |\n" +
		"| Ask latency, 1 caller, p99 (ns) | 16,160 | n/a | 10,000 | n/a |\n" +
		"| Ping-pong, 1 pair (allocations per message) | 1 | 1 | n/a | 0 |",
	"Each number comes from a median of the run in [`bench/results/baseline.txt`](bench/results/baseline.txt): " +
		"commit [`0f3a9c1`](https://github.com/hmdsefi/lockstep/commit/0f3a9c1d5b7e2a4c6d8e0f1a2b3c4d5e6f708192), " +
		"measured on 2026-11-03 with the CPU `AMD EPYC 7763 64-Core Processor`. " +
		"[`bench/RESULTS.md`](bench/RESULTS.md) has more results of that run, library by library.",
}, "\n\n")

// readmeText returns a README with the marker lines around the given block lines, between a
// paragraph before and one after.
func readmeText(block ...string) string {
	return lines(slices.Concat([]string{"# Title", "", "Before.", "", readmeBegin}, block,
		[]string{readmeEnd, "", "After."})...)
}

// writeText writes text to a new file named README.md and returns its path.
func writeText(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "README.md")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// readFile returns the contents of the file at path.
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestReadme checks that budget readme replaces the lines between the marker lines with the table,
// prints nothing, and leaves the rest of the file as it is: the rows in the order of the
// headlines, n/a for a library with no result, the spawn time per actor from ns/op and not from
// ns/actor, times in whole nanoseconds or, when every cell of the row is at least 10,000 ns, in
// microseconds, both rounded half away from zero, and commas between groups of three digits.
func TestReadme(t *testing.T) {
	baseline := writeFile(t, "baseline.txt", slices.Concat(readmeHeader, readmeResults)...)
	want := readmeText("", readmeBlock, "")

	for name, block := range map[string][]string{
		"an old table":    {"", "| old | table |", ""},
		"nothing between": nil,
		"the same table":  {"", readmeBlock, ""},
	} {
		t.Run(name, func(t *testing.T) {
			readme := writeText(t, readmeText(block...))
			wantRun(t, []string{"readme", baseline, readme}, 0, "", "")
			if got := readFile(t, readme); got != want {
				t.Errorf("README.md:\n%s\nwant:\n%s", got, want)
			}
		})
	}

	t.Run("end marker without a newline", func(t *testing.T) {
		readme := writeText(t, strings.TrimSuffix(lines(readmeBegin, "old", readmeEnd), "\n"))
		wantRun(t, []string{"readme", baseline, readme}, 0, "", "")
		if got, want := readFile(t, readme), readmeBegin+"\n\n"+readmeBlock+"\n\n"+readmeEnd; got != want {
			t.Errorf("README.md:\n%q\nwant:\n%q", got, want)
		}
	})

	t.Run("CRLF line endings outside the block", func(t *testing.T) {
		readme := writeText(t, strings.ReplaceAll(readmeText("old"), "\n", "\r\n"))
		wantRun(t, []string{"readme", baseline, readme}, 0, "", "")
		wantCRLF := "# Title\r\n\r\nBefore.\r\n\r\n" + readmeBegin + "\r\n\n" + readmeBlock + "\n\n" +
			readmeEnd + "\r\n\r\nAfter.\r\n"
		if got := readFile(t, readme); got != wantCRLF {
			t.Errorf("README.md:\n%q\nwant:\n%q", got, wantCRLF)
		}
	})
}

// TestReadmeErrors checks that budget readme fails with exit code 2 and an error, and leaves the
// README as it was, when the baseline lacks a lockstep result of a headline or a header line, has
// a commit that is not a hash or a negative median, or the README lacks a marker line or has one
// twice or out of order.
func TestReadmeErrors(t *testing.T) {
	full := slices.Concat(readmeHeader, readmeResults)
	without := func(prefix string) []string {
		return slices.DeleteFunc(slices.Clone(full), func(l string) bool { return strings.HasPrefix(l, prefix) })
	}
	markers := "budget: %[2]s: want the line " + readmeBegin + " once, and the line " + readmeEnd + " once after it\n"

	for _, c := range []struct {
		name     string
		baseline []string
		readme   string
		want     string // the error, with %[1]s for the baseline and %[2]s for the README
	}{
		{"missing lockstep result", without("BenchmarkAskLatency/lib=lockstep/"), readmeText(),
			"budget: %[1]s: no result for BenchmarkAskLatency/lib=lockstep/callers=1 p50-ns\n"},
		{"missing commit", without("# commit:"), readmeText(), "budget: %[1]s: no # commit: line\n"},
		{"empty commit", append(without("# commit:"), "# commit: "), readmeText(), "budget: %[1]s: no # commit: line\n"},
		{"short commit", append(without("# commit:"), "# commit: 39107e"), readmeText(),
			"budget: %[1]s: # commit: 39107e is not a hexadecimal commit hash\n"},
		{"commit with more than a hash", append(without("# commit:"), "# commit: 39107e0 dirty"), readmeText(),
			"budget: %[1]s: # commit: 39107e0 dirty is not a hexadecimal commit hash\n"},
		{"missing date", without("# date:"), readmeText(), "budget: %[1]s: no # date: line\n"},
		{"missing cpu", without("cpu:"), readmeText(), "budget: %[1]s: no cpu: line\n"},
		{"negative median", append(slices.Clone(full), "BenchmarkPingPong/lib=hollywood/pairs=1-4 100 -5 ns/op"), readmeText(),
			"budget: %[1]s: BenchmarkPingPong/lib=hollywood/pairs=1 ns/op has median -5, want a non-negative number\n"},
		{"no markers", full, lines("# Title"), markers},
		{"no end marker", full, lines(readmeBegin, "x"), markers},
		{"no begin marker", full, lines("x", readmeEnd), markers},
		{"end before begin", full, lines(readmeEnd, readmeBegin), markers},
		{"begin twice", full, lines(readmeBegin, readmeBegin, readmeEnd), markers},
		{"end twice", full, lines(readmeBegin, readmeEnd, readmeEnd), markers},
		{"marker with text around it", full, lines("x "+readmeBegin, readmeEnd), markers},
	} {
		t.Run(c.name, func(t *testing.T) {
			baseline := writeFile(t, "baseline.txt", c.baseline...)
			readme := writeText(t, c.readme)
			wantRun(t, []string{"readme", baseline, readme}, 2, "", fmt.Sprintf(c.want, baseline, readme))
			if got := readFile(t, readme); got != c.readme {
				t.Errorf("README.md changed to:\n%s", got)
			}
		})
	}

	t.Run("missing README", func(t *testing.T) {
		baseline := writeFile(t, "baseline.txt", full...)
		missing := filepath.Join(t.TempDir(), "README.md")
		code, stdout, stderr := budgetRun(t, "readme", baseline, missing)
		if code != 2 || stdout != "" || !strings.HasPrefix(stderr, "budget: ") || !strings.Contains(stderr, missing) {
			t.Errorf("exit code %d, stdout %q, stderr %q; want 2, nothing, an error naming %s", code, stdout, stderr, missing)
		}
		if _, err := os.Stat(missing); !os.IsNotExist(err) {
			t.Errorf("budget readme created %s", missing)
		}
	})

	t.Run("malformed baseline", func(t *testing.T) {
		malformed := filepath.Join("testdata", "malformed.txt")
		readme := writeText(t, lines(readmeBegin, readmeEnd))
		wantRun(t, []string{"readme", malformed, readme}, 2, "", `budget: `+malformed+`:3: invalid value "abc"`+"\n")
	})
}

// TestGroupThousands checks the commas between groups of three digits of the integer part.
func TestGroupThousands(t *testing.T) {
	for in, want := range map[string]string{
		"0":          "0",
		"999":        "999",
		"1000":       "1,000",
		"12.5":       "12.5",
		"100000":     "100,000",
		"1234567.25": "1,234,567.25",
		"0.0004":     "0.0004",
	} {
		if got := groupThousands(in); got != want {
			t.Errorf("groupThousands(%q) = %q, want %q", in, got, want)
		}
	}
}
