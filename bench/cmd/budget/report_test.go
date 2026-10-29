// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// dash is the cell of a library with no result, an em dash.
const dash = "\u2014"

// wantIntro is the sentence under the title of every report.
const wantIntro = "Each number is the median of the runs in [`results/baseline.txt`](results/baseline.txt), " +
	"which the `bench` CI job measured on the configuration below. " +
	"A cell that holds `" + dash + "` has no result."

// reportRun runs budget report on a baseline made of lines and returns the exit code and outputs.
func reportRun(t *testing.T, lines ...string) (code int, stdout, stderr string) {
	t.Helper()
	return budgetRun(t, "report", writeFile(t, "baseline.txt", lines...))
}

// wantTable returns the expected heading and table of one family and unit. Each row is written
// without its outer bars, and NONE stands for the cell of a library with no result.
func wantTable(family, unit string, rows ...string) string {
	text := "### " + family + " (" + unit + ")\n\n" +
		"| parameter | lockstep | protoactor | hollywood | goakt |\n" +
		"|---|---:|---:|---:|---:|"
	for _, row := range rows {
		text += "\n| " + strings.ReplaceAll(row, "NONE", dash) + " |"
	}
	return text
}

// wantReport checks that report succeeded and printed the title, the sentence, the header block
// (omitted when empty) and the tables, as paragraphs separated by one blank line.
func wantReport(t *testing.T, code int, stdout, stderr, header string, tables ...string) {
	t.Helper()
	blocks := []string{"# Benchmark results", wantIntro}
	if header != "" {
		blocks = append(blocks, header)
	}
	want := strings.Join(append(blocks, tables...), "\n\n") + "\n"
	if code != 0 || stdout != want || stderr != "" {
		t.Errorf("exit code %d, stdout:\n%s\nstderr %q; want 0, stdout:\n%s\nand no stderr", code, stdout, stderr, want)
	}
}

// codeBlock returns the expected header block with lines, one per line.
func codeBlock(lines ...string) string {
	return "```text\n" + strings.Join(lines, "\n") + "\n```"
}

// TestATBEN11Report covers the report half of AT-BEN-11: the PingPong table of the fixture
// baseline has the columns lockstep, protoactor, hollywood and goakt and a row pairs=1 with
// 100, 80, 60 and 90, and the header holds goos, goarch, cpu, commit and date (BEN-180).
func TestATBEN11Report(t *testing.T) {
	// NONE stands for the cell of a library with no result.
	want := "# Benchmark results\n" + `
Each number is the median of the runs in [` + "`results/baseline.txt`" + `](results/baseline.txt), which the ` + "`bench`" + ` CI job measured on the configuration below. A cell that holds ` + "`" + dash + "`" + ` has no result.

` + "```text" + `
goos: linux
goarch: amd64
cpu: AMD EPYC 7763 64-Core Processor
# commit: 0f3a9c1d5b7e2a4c6d8e0f1a2b3c4d5e6f708192
# date: 2026-11-03
` + "```" + `

### PingPong (ns/op)

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| pairs=1 | 100 | 80 | 60 | 90 |

### PingPong (allocs/op)

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| pairs=1 | 1 | 2 | 2 | 3 |

### SendLatency (p99-ns)

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 900 | 700 | 600 | NONE |

### SimSeeds (ns/op)

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| workload=pingpong | 20000 | NONE | NONE | NONE |
`
	want = strings.ReplaceAll(want, "NONE", dash)
	wantRun(t, []string{"report", baseline}, 0, want, "")

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
	code, stdout, stderr := reportRun(t,
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
	)
	wantReport(t, code, stdout, stderr, codeBlock("goos: linux"),
		wantTable("PingPong", "ns/op", "pairs=procs | 25 | NONE | NONE | NONE"),
		wantTable("PingPong", "allocs/op", "pairs=procs | 0 | NONE | NONE | NONE"),
		wantTable("FanOut", "ns/op",
			"consumers=1024 | 70 | NONE | 50 | NONE",
			"consumers=16 | 60 | NONE | NONE | NONE"),
		wantTable("FanOut", "allocs/op",
			"consumers=1024 | 1 | NONE | 2 | NONE",
			"consumers=16 | 1 | NONE | NONE | NONE"),
		wantTable("IdleActors", "ns/op",
			"n=100000 | NONE | NONE | NONE | 900",
			"n=10000 | 120 | NONE | NONE | 100"),
		wantTable("IdleActors", "B/actor",
			"n=100000 | NONE | NONE | NONE | 310",
			"n=10000 | 200 | NONE | NONE | 300"),
		wantTable("IdleActors", "allocs/op",
			"n=100000 | NONE | NONE | NONE | 4",
			"n=10000 | 3 | NONE | NONE | 4"),
		wantTable("AskLatency", "ns/op",
			"callers=1 | 300 | 500 | NONE | NONE",
			"callers=procs | 350 | NONE | NONE | NONE"),
		wantTable("AskLatency", "p50-ns",
			"callers=1 | 200 | 400 | NONE | NONE",
			"callers=procs | 250 | NONE | NONE | NONE"),
		wantTable("AskLatency", "p99-ns",
			"callers=1 | 600 | 700 | NONE | NONE",
			"callers=procs | 650 | NONE | NONE | NONE"),
		wantTable("AskLatency", "allocs/op",
			"callers=1 | 1 | 3 | NONE | NONE",
			"callers=procs | 1 | NONE | NONE | NONE"),
		wantTable("AskPipe", "ns/op", "- | 4000 | NONE | NONE | NONE"),
		wantTable("AskPipe", "p50-ns", "- | 3000 | NONE | NONE | NONE"),
		wantTable("AskPipe", "p99-ns", "- | 9000 | NONE | NONE | NONE"),
		wantTable("AskPipe", "allocs/op", "- | 5 | NONE | NONE | NONE"),
		wantTable("SimSeeds", "ns/op",
			"workload=crash3 | 30000 | NONE | NONE | NONE",
			"workload=pingpong | 20000 | NONE | NONE | NONE"),
	)
}

// TestReportMedians checks that each cell is the median over all samples, printed with the
// fewest digits that represent it.
func TestReportMedians(t *testing.T) {
	code, stdout, stderr := reportRun(t,
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 300 ns/op 1 allocs/op",
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 100 ns/op 1 allocs/op",
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 150 ns/op 1 allocs/op",
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 250 ns/op 2 allocs/op",
		"BenchmarkPingPong/lib=protoactor/pairs=1-4 100 0.1 ns/op 0 allocs/op",
		"BenchmarkPingPong/lib=protoactor/pairs=1-4 100 0.2 ns/op 0 allocs/op",
		"BenchmarkPingPong/lib=protoactor/pairs=1-4 100 12345.678 ns/op 0 allocs/op",
	)
	wantReport(t, code, stdout, stderr, "",
		wantTable("PingPong", "ns/op", "pairs=1 | 200 | 0.2 | NONE | NONE"),
		wantTable("PingPong", "allocs/op", "pairs=1 | 1 | 0 | NONE | NONE"),
	)
}

// TestReportHeader checks the header: the first value of each configuration line and comment,
// in the order goos, goarch, cpu, commit, date, and no code block when the baseline has none.
func TestReportHeader(t *testing.T) {
	result := "BenchmarkSimSeeds/workload=w 50 20000 ns/op"
	simSeeds := wantTable("SimSeeds", "ns/op", "workload=w | 20000 | NONE | NONE | NONE")

	t.Run("first values, fixed order", func(t *testing.T) {
		code, stdout, stderr := reportRun(t,
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
		)
		wantReport(t, code, stdout, stderr,
			codeBlock("goos: darwin", "goarch: arm64", "cpu: first cpu", "# commit: abcdef0", "# date: 2026-11-04"),
			simSeeds)
	})

	t.Run("lines the baseline lacks", func(t *testing.T) {
		code, stdout, stderr := reportRun(t, "goarch: arm64", "# date: 2026-11-04", result)
		wantReport(t, code, stdout, stderr, codeBlock("goarch: arm64", "# date: 2026-11-04"), simSeeds)
	})

	t.Run("no header at all", func(t *testing.T) {
		code, stdout, stderr := reportRun(t, "pkg: x", "# note: nothing", "# commit:abc", result)
		wantReport(t, code, stdout, stderr, "", simSeeds)
	})

	t.Run("no results", func(t *testing.T) {
		code, stdout, stderr := reportRun(t, "goos: linux")
		wantReport(t, code, stdout, stderr, codeBlock("goos: linux"))
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
