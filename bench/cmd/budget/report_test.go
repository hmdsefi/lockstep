// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// wantIntro is the sentence under the title of every report.
const wantIntro = "Each number is the median of the benchmark runs in [`results/baseline.txt`](results/baseline.txt), " +
	"and `n/a` marks a library with no result for that row."

// wantHeaderIntro is the sentence before the header lines of the baseline.
const wantHeaderIntro = "The header lines of that file give the platform, the commit and the date:"

// wantFamilyIntro is the sentence under the heading of each family.
var wantFamilyIntro = map[string]string{
	"PingPong":    "Pairs of actors exchange `Ping` and `Pong` messages; one op is one message, and `pairs=procs` runs `GOMAXPROCS` pairs.",
	"FanOut":      "One producer sends items to many consumers under credit flow control; one op is one item.",
	"IdleActors":  "Each op spawns `n` idle actors and sends each one a `Hello` message; `B/actor` is the heap growth per actor.",
	"SendLatency": "One outside goroutine sends to an echo actor; one op is one send, and latency runs from the send call until `Receive` starts.",
	"AskLatency":  "Callers ask an echo actor and wait for the reply; one op is one ask, and `callers=procs` runs `GOMAXPROCS` callers in parallel.",
	"AskPipe":     "Only lockstep runs this workload: one actor asks another with `Ask` and pipes the reply to its mailbox; one op is one round trip.",
	"SimSeeds":    "Only lockstep runs this workload: `sim.Simulate` runs one simulated world per seed, and one op is one seed.",
}

// wantTable returns the expected lead-in and table of one unit, two paragraphs separated by a
// blank line. Each row is written without its outer bars.
func wantTable(unit string, rows ...string) string {
	text := "Median `" + unit + "`:\n\n" +
		"| parameter | lockstep | protoactor | hollywood | goakt |\n" +
		"|---|---:|---:|---:|---:|"
	for _, row := range rows {
		text += "\n| " + row + " |"
	}
	return text
}

// wantSection returns the expected heading, sentence and tables of one family, as paragraphs.
func wantSection(family string, tables ...string) string {
	return strings.Join(append([]string{"## " + family, wantFamilyIntro[family]}, tables...), "\n\n")
}

// wantCodeBlock returns the expected header lines of the baseline in a code block.
func wantCodeBlock(lines ...string) string {
	return wantHeaderIntro + "\n\n```text\n" + strings.Join(lines, "\n") + "\n```"
}

// wantReport checks that budget report succeeds on a baseline made of results and prints the
// title, the sentence under it, the header block (omitted when empty) and the sections, as
// paragraphs separated by one blank line.
func wantReport(t *testing.T, results []string, header string, sections ...string) {
	t.Helper()
	blocks := []string{"# Benchmark results", wantIntro}
	if header != "" {
		blocks = append(blocks, header)
	}
	want := strings.Join(append(blocks, sections...), "\n\n") + "\n"
	wantRun(t, []string{"report", writeFile(t, "baseline.txt", results...)}, 0, want, "")
}

// TestATBEN11Report covers the report half of AT-BEN-11: the PingPong ns/op table of the fixture
// baseline has the columns lockstep, protoactor, hollywood and goakt and a row pairs=1 with 100,
// 80, 60 and 90, and the header holds goos, goarch, cpu, commit and date (BEN-180). It compares
// the whole document.
func TestATBEN11Report(t *testing.T) {
	want := strings.Join([]string{
		"# Benchmark results",
		"Each number is the median of the benchmark runs in [`results/baseline.txt`](results/baseline.txt), " +
			"and `n/a` marks a library with no result for that row.",
		"The header lines of that file give the platform, the commit and the date:",
		"```text\n" +
			"goos: linux\n" +
			"goarch: amd64\n" +
			"cpu: AMD EPYC 7763 64-Core Processor\n" +
			"# commit: 0f3a9c1d5b7e2a4c6d8e0f1a2b3c4d5e6f708192\n" +
			"# date: 2026-11-03\n" +
			"```",
		"## PingPong",
		"Pairs of actors exchange `Ping` and `Pong` messages; one op is one message, and `pairs=procs` runs `GOMAXPROCS` pairs.",
		"Median `ns/op`:",
		"| parameter | lockstep | protoactor | hollywood | goakt |\n" +
			"|---|---:|---:|---:|---:|\n" +
			"| pairs=1 | 100 | 80 | 60 | 90 |",
		"Median `allocs/op`:",
		"| parameter | lockstep | protoactor | hollywood | goakt |\n" +
			"|---|---:|---:|---:|---:|\n" +
			"| pairs=1 | 1 | 2 | 2 | 3 |",
		"## IdleActors",
		"Each op spawns `n` idle actors and sends each one a `Hello` message; `B/actor` is the heap growth per actor.",
		"Median `B/actor`:",
		"| parameter | lockstep | protoactor | hollywood | goakt |\n" +
			"|---|---:|---:|---:|---:|\n" +
			"| n=10000 | 100 | n/a | n/a | n/a |",
		"Median `allocs/op`:",
		"| parameter | lockstep | protoactor | hollywood | goakt |\n" +
			"|---|---:|---:|---:|---:|\n" +
			"| n=10000 | 110021.5 | n/a | n/a | n/a |",
		"## SendLatency",
		"One outside goroutine sends to an echo actor; one op is one send, and latency runs from the send call until `Receive` starts.",
		"Median `p99-ns`:",
		"| parameter | lockstep | protoactor | hollywood | goakt |\n" +
			"|---|---:|---:|---:|---:|\n" +
			"| - | 900 | 700 | 600 | n/a |",
		"## SimSeeds",
		"Only lockstep runs this workload: `sim.Simulate` runs one simulated world per seed, and one op is one seed.",
		"Median `ns/op`:",
		"| parameter | lockstep | protoactor | hollywood | goakt |\n" +
			"|---|---:|---:|---:|---:|\n" +
			"| workload=pingpong | 20000 | n/a | n/a | n/a |",
	}, "\n\n") + "\n"
	wantRun(t, []string{"report", baseline}, 0, want, "")
	wantStable(t, "report", baseline)

	// AT-BEN-11 names the columns and the row; they are in the document as written.
	for _, text := range []string{
		"| lockstep | protoactor | hollywood | goakt |",
		"| pairs=1 | 100 | 80 | 60 | 90 |",
	} {
		if !strings.Contains(want, text) {
			t.Errorf("the report lacks %q", text)
		}
	}
}

// TestReportLayout checks the order of families, units, rows and columns and what the report
// leaves out (BEN-180). The baseline lists the families in reverse, a larger parameter before a
// smaller one and the libraries out of order, and holds results the report does not show.
func TestReportLayout(t *testing.T) {
	wantReport(t, []string{
		"goos: linux",
		"BenchmarkSimSeeds/workload=crash3-4 50 30000 ns/op 33.33 seeds/s",
		"BenchmarkSimSeeds/workload=pingpong-4 50 20000 ns/op 50.00 seeds/s",
		"BenchmarkIdleActors/lib=goakt/n=100000-4 1 900 ns/op 310 B/actor 0.004 goroutines/actor 9 ns/actor 4 allocs/op",
		"BenchmarkIdleActors/lib=goakt/n=10000-4 1 100 ns/op 300 B/actor 0.004 goroutines/actor 9 ns/actor 4 allocs/op",
		"BenchmarkIdleActors/lib=lockstep/n=10000-4 1 120 ns/op 200 B/actor 0.002 goroutines/actor 7 ns/actor 3 allocs/op",
		"BenchmarkFanOut/lib=hollywood/consumers=1024-4 100 50 ns/op 20000000 msgs/s 40 B/op 2 allocs/op",
		"BenchmarkFanOut/lib=lockstep/consumers=16-4 100 60 ns/op 16000000 msgs/s 30 B/op 1 allocs/op",
		"BenchmarkFanOut/lib=lockstep/consumers=1024-4 100 70 ns/op 14000000 msgs/s 30 B/op 1 allocs/op",
		"BenchmarkAskPipe/lib=lockstep-4 10 4000 ns/op 3000 p50-ns 9000 p99-ns 5 allocs/op",
		"BenchmarkAskLatency/lib=protoactor/callers=1-4 10 500 ns/op 400 p50-ns 700 p99-ns 3 allocs/op",
		"BenchmarkAskLatency/lib=lockstep/callers=1-4 10 300 ns/op 200 p50-ns 600 p99-ns 1 allocs/op",
		"BenchmarkAskLatency/lib=lockstep/callers=procs-4 10 350 ns/op 250 p50-ns 650 p99-ns 1 allocs/op",
		"BenchmarkCalibrate-4 10 1000 ns/op",
		"BenchmarkOther/lib=lockstep 10 1 ns/op",
		"BenchmarkPingPong/lib=lockstep/pairs=procs-4 100 25 ns/op 40000000 msgs/s 8 B/op 0 allocs/op",
	}, wantCodeBlock("goos: linux"),
		wantSection("PingPong",
			wantTable("ns/op", "pairs=procs | 25 | n/a | n/a | n/a"),
			wantTable("allocs/op", "pairs=procs | 0 | n/a | n/a | n/a"),
		),
		wantSection("FanOut",
			wantTable("ns/op",
				"consumers=1024 | 70 | n/a | 50 | n/a",
				"consumers=16 | 60 | n/a | n/a | n/a"),
			wantTable("allocs/op",
				"consumers=1024 | 1 | n/a | 2 | n/a",
				"consumers=16 | 1 | n/a | n/a | n/a"),
		),
		wantSection("IdleActors",
			wantTable("ns/op",
				"n=100000 | n/a | n/a | n/a | 900",
				"n=10000 | 120 | n/a | n/a | 100"),
			wantTable("B/actor",
				"n=100000 | n/a | n/a | n/a | 310",
				"n=10000 | 200 | n/a | n/a | 300"),
			wantTable("allocs/op",
				"n=100000 | n/a | n/a | n/a | 4",
				"n=10000 | 3 | n/a | n/a | 4"),
		),
		wantSection("AskLatency",
			wantTable("ns/op",
				"callers=1 | 300 | 500 | n/a | n/a",
				"callers=procs | 350 | n/a | n/a | n/a"),
			wantTable("p50-ns",
				"callers=1 | 200 | 400 | n/a | n/a",
				"callers=procs | 250 | n/a | n/a | n/a"),
			wantTable("p99-ns",
				"callers=1 | 600 | 700 | n/a | n/a",
				"callers=procs | 650 | n/a | n/a | n/a"),
			wantTable("allocs/op",
				"callers=1 | 1 | 3 | n/a | n/a",
				"callers=procs | 1 | n/a | n/a | n/a"),
		),
		wantSection("AskPipe",
			wantTable("ns/op", "- | 4000 | n/a | n/a | n/a"),
			wantTable("p50-ns", "- | 3000 | n/a | n/a | n/a"),
			wantTable("p99-ns", "- | 9000 | n/a | n/a | n/a"),
			wantTable("allocs/op", "- | 5 | n/a | n/a | n/a"),
		),
		wantSection("SimSeeds",
			wantTable("ns/op",
				"workload=crash3 | 30000 | n/a | n/a | n/a",
				"workload=pingpong | 20000 | n/a | n/a | n/a"),
		),
	)
}

// TestReportMedians checks that each cell is the median over all samples, printed with the
// fewest digits that show its rounded value.
func TestReportMedians(t *testing.T) {
	wantReport(t, []string{
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 300 ns/op 1 allocs/op",
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 100 ns/op 1 allocs/op",
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 150 ns/op 1 allocs/op",
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 250 ns/op 2 allocs/op",
		"BenchmarkPingPong/lib=protoactor/pairs=1-4 100 0.1 ns/op 0 allocs/op",
		"BenchmarkPingPong/lib=protoactor/pairs=1-4 100 0.2 ns/op 0 allocs/op",
		"BenchmarkPingPong/lib=protoactor/pairs=1-4 100 12345.678 ns/op 0 allocs/op",
	}, "",
		wantSection("PingPong",
			wantTable("ns/op", "pairs=1 | 200 | 0.2 | n/a | n/a"),
			wantTable("allocs/op", "pairs=1 | 1 | 0 | n/a | n/a"),
		),
	)
}

// TestReportMedianNoise checks that the mean of two middle samples prints without float noise:
// 231.6 and 232.7 (hollywood pairs=1 in the baseline) give 232.15 and 318.9 and 319.2 give 319.05,
// which a float64 holds as 232.14999999999998 and 319.04999999999995 (BEN-180).
func TestReportMedianNoise(t *testing.T) {
	wantReport(t, []string{
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 318.9 ns/op 1 allocs/op",
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 319.2 ns/op 1 allocs/op",
		"BenchmarkPingPong/lib=hollywood/pairs=1-4 100 232.7 ns/op 2 allocs/op",
		"BenchmarkPingPong/lib=hollywood/pairs=1-4 100 231.6 ns/op 2 allocs/op",
	}, "",
		wantSection("PingPong",
			wantTable("ns/op", "pairs=1 | 319.05 | n/a | 232.15 | n/a"),
			wantTable("allocs/op", "pairs=1 | 1 | n/a | 2 | n/a"),
		),
	)
}

// TestReportHeader checks the header: the first value of each configuration line and comment,
// in the order goos, goarch, cpu, commit, date, and no sentence or code block when the baseline
// has none.
func TestReportHeader(t *testing.T) {
	result := "BenchmarkSimSeeds/workload=w 50 20000 ns/op"
	simSeeds := wantSection("SimSeeds", wantTable("ns/op", "workload=w | 20000 | n/a | n/a | n/a"))

	t.Run("first values, fixed order", func(t *testing.T) {
		wantReport(t, []string{
			"# runner: ubuntu-latest",
			"# date: 2026-11-04",
			"cpu: first cpu  ",
			"# commit: abcdef0",
			"goarch: arm64",
			"goos: darwin",
			"goos: linux",
			"cpu: second cpu",
			"# commit: 1234567",
			result,
		}, wantCodeBlock("goos: darwin", "goarch: arm64", "cpu: first cpu", "# commit: abcdef0", "# date: 2026-11-04"),
			simSeeds)
	})

	t.Run("lines the baseline lacks", func(t *testing.T) {
		wantReport(t, []string{"goarch: arm64", "# date: 2026-11-04", result},
			wantCodeBlock("goarch: arm64", "# date: 2026-11-04"), simSeeds)
	})

	t.Run("no header at all", func(t *testing.T) {
		wantReport(t, []string{"pkg: x", "# note: nothing", "# commit:abc", result}, "", simSeeds)
	})

	t.Run("no results", func(t *testing.T) {
		wantReport(t, []string{"goos: linux"}, wantCodeBlock("goos: linux"))
	})
}

// TestReportErrors checks that an unreadable or malformed baseline fails with exit code 2 and
// prints no report.
func TestReportErrors(t *testing.T) {
	t.Run("malformed baseline", func(t *testing.T) {
		malformed := filepath.Join("testdata", "malformed.txt")
		wantRun(t, []string{"report", malformed}, 2, "", `budget: `+malformed+`:3: invalid value "abc"`+"\n")
	})

	t.Run("missing baseline", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "baseline.txt")
		code, stdout, stderr := budgetRun(t, "report", missing)
		if code != 2 || stdout != "" || !strings.HasPrefix(stderr, "budget: ") || !strings.Contains(stderr, missing) {
			t.Errorf("exit code %d, stdout %q, stderr %q; want 2, nothing, an error naming %s", code, stdout, stderr, missing)
		}
	})
}
