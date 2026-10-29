// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bench

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/bench/libs"
)

// benchLibs are the lib= values, in the order of libs.Libs. The budget tool keeps its own copy of
// this list, which orders its report's columns and breaks ties between references (BEN-170,
// BEN-180).
var benchLibs = []string{"lockstep", "protoactor", "hollywood", "goakt"}

// benchUnits are the units a result line may use (BEN-120).
var benchUnits = []string{
	"ns/op", "B/op", "allocs/op",
	"msgs/s", "p50-ns", "p99-ns", "B/actor", "goroutines/actor", "ns/actor", "seeds/s",
}

// familyUnits are the units each benchmark family reports: ns/op, and B/op and allocs/op since
// every benchmark reports its allocations, then the family's own (BEN-040 to BEN-110).
var familyUnits = map[string][]string{
	"PingPong":    {"ns/op", "B/op", "allocs/op", "msgs/s"},
	"FanOut":      {"ns/op", "B/op", "allocs/op", "msgs/s"},
	"IdleActors":  {"ns/op", "B/op", "allocs/op", "B/actor", "goroutines/actor", "ns/actor"},
	"SendLatency": {"ns/op", "B/op", "allocs/op", "p50-ns", "p99-ns"},
	"AskLatency":  {"ns/op", "B/op", "allocs/op", "p50-ns", "p99-ns"},
	"AskPipe":     {"ns/op", "B/op", "allocs/op", "p50-ns", "p99-ns"},
	"Calibrate":   {"ns/op", "B/op", "allocs/op"},
}

// procsSuffix matches the -N that go test appends to a benchmark's name when GOMAXPROCS is N > 1.
var procsSuffix = regexp.MustCompile(`-[0-9]+$`)

// goTestLine matches the lines other than result lines that go test -bench writes for a passing
// package: the configuration lines goos, goarch, pkg and cpu, PASS, and the ok and no-test-files
// lines. A line a library writes, such as "warning: ...", matches none of them.
var goTestLine = regexp.MustCompile(`^(?:(?:goos|goarch|pkg|cpu): |PASS$|ok  \t|\?   \t)`)

// TestLibsOrder checks that libs.Libs returns the libraries in the order lockstep, protoactor,
// hollywood, goakt, the order of benchLibs and of the budget tool's copy.
func TestLibsOrder(t *testing.T) {
	var got []string
	for _, lib := range libs.Libs() {
		got = append(got, lib.Name())
	}
	if !slices.Equal(got, benchLibs) {
		t.Errorf("libs.Libs() names are %q, want %q", got, benchLibs)
	}
}

// wantBenchNames returns the names of AT-BEN-05 with LOCKSTEP_BENCH_LARGE unset, without the
// GOMAXPROCS suffix: nine per library, then BenchmarkAskPipe/lib=lockstep and BenchmarkCalibrate.
func wantBenchNames() []string {
	var names []string
	for _, name := range benchLibs {
		lib := "/lib=" + name
		names = append(names,
			"BenchmarkPingPong"+lib+"/pairs=1",
			"BenchmarkPingPong"+lib+"/pairs=procs",
			"BenchmarkFanOut"+lib+"/consumers=16",
			"BenchmarkFanOut"+lib+"/consumers=1024",
			"BenchmarkIdleActors"+lib+"/n=10000",
			"BenchmarkIdleActors"+lib+"/n=100000",
			"BenchmarkSendLatency"+lib,
			"BenchmarkAskLatency"+lib+"/callers=1",
			"BenchmarkAskLatency"+lib+"/callers=procs",
		)
	}
	return append(names, "BenchmarkAskPipe/lib=lockstep", "BenchmarkCalibrate")
}

// AT-BEN-05, BEN-040 to BEN-120: go test -run '^$' -bench . -benchtime 1x ./... in the module
// prints one result line for each of the 38 names of wantBenchNames, once the GOMAXPROCS suffix
// is removed, and no other, with BEN-120's units only.
//
// Every line that starts with Benchmark must parse as a result line, as the budget tool parses it
// (BEN-140). go test merges what the test binary writes to standard error into standard output, and
// it prints a benchmark's name before it runs the benchmark again with a larger b.N, so a library
// that logs while a benchmark runs breaks that benchmark's result line. With -benchtime 1x no
// benchmark runs again, and such a write lands on a line of its own, so every other line must be
// one that go test writes itself (BEN-030).
//
// The child go test gets the parent's environment without LOCKSTEP_BENCH_LARGE, so it runs the 38
// benchmarks however the parent was started. goTestCommand builds it without -race and runs each
// benchmark once without -v, whatever GOFLAGS says, with a timeout that ends before the parent's.
func TestATBEN05Names(t *testing.T) {
	if testing.Short() {
		t.Skip("runs every benchmark once in a child go test")
	}
	cmd := goTestCommand(t, []string{"-run", "^$", "-bench", ".", "-benchtime", "1x"}, "./...")
	cmd.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool {
		return strings.HasPrefix(kv, largeEnv+"=")
	})
	desc := strings.Join(cmd.Args, " ")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s: %v\n%s%s", desc, err, stdout.Bytes(), stderr.Bytes())
	}

	count := map[string]int{} // result lines per name
	for line := range strings.Lines(stdout.String()) {
		line = strings.TrimRight(line, "\n")
		if !strings.HasPrefix(line, "Benchmark") {
			if !goTestLine.MatchString(line) {
				t.Errorf("a line go test did not write; in a longer run it may break a result line:\n%s", line)
			}
			continue
		}
		name, units, err := parseResultLine(line)
		if err != nil {
			t.Errorf("malformed result line: %v\n%s", err, line)
			continue
		}
		count[name]++
		checkUnits(t, name, units)
	}

	want := wantBenchNames()
	for _, name := range want {
		if count[name] == 0 {
			t.Errorf("no result line for %s", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(count)) {
		switch {
		case !slices.Contains(want, name):
			t.Errorf("result line for %s, which is not one of the %d names", name, len(want))
		case count[name] > 1:
			t.Errorf("%d result lines for %s, want 1", count[name], name)
		}
	}
	if t.Failed() {
		t.Logf("%s printed:\n%s", desc, stdout.Bytes())
	}
}

// parseResultLine parses a result line, Benchmark<name> <iterations> <value> <unit>..., and returns
// the name without the GOMAXPROCS suffix and the units in order. Like the budget tool, it wants an
// iteration count above zero and values that parse as float64; it also wants each value finite and
// at least one value.
func parseResultLine(line string) (string, []string, error) {
	fields := strings.Fields(line)
	if len(fields) < 4 || len(fields)%2 != 0 {
		return "", nil, errors.New("want a name, an iteration count and value-unit pairs")
	}
	if n, err := strconv.ParseUint(fields[1], 10, 64); err != nil || n == 0 {
		return "", nil, fmt.Errorf("invalid iteration count %q", fields[1])
	}
	var units []string
	for i := 2; i < len(fields); i += 2 {
		v, err := strconv.ParseFloat(fields[i], 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return "", nil, fmt.Errorf("invalid value %q", fields[i])
		}
		units = append(units, fields[i+1])
	}
	return procsSuffix.ReplaceAllString(fields[0], ""), units, nil
}

// checkUnits fails t unless the units of the result line of name are within BEN-120's list and
// are exactly those of the benchmark's family.
func checkUnits(t *testing.T, name string, units []string) {
	t.Helper()
	for _, u := range units {
		if !slices.Contains(benchUnits, u) {
			t.Errorf("%s reports %q, which is not a unit of BEN-120", name, u)
		}
	}
	family, _, _ := strings.Cut(strings.TrimPrefix(name, "Benchmark"), "/")
	want, ok := familyUnits[family]
	if !ok {
		return // not one of the names, which TestATBEN05Names reports
	}
	if !slices.Equal(slices.Sorted(slices.Values(units)), slices.Sorted(slices.Values(want))) {
		t.Errorf("%s reports %q, want %q", name, units, want)
	}
}

// TestGoTestLine checks goTestLine on the lines go test -bench writes for a passing package and on
// lines a library might write, which a longer run would put inside a result line.
func TestGoTestLine(t *testing.T) {
	for _, c := range []struct {
		line string
		want bool
	}{
		{"goos: linux", true},
		{"goarch: amd64", true},
		{"pkg: github.com/hmdsefi/lockstep/bench", true},
		{"cpu: AMD EPYC 7763 64-Core Processor", true},
		{"PASS", true},
		{"ok  \tgithub.com/hmdsefi/lockstep/bench\t25.699s", true},
		{"?   \tgithub.com/hmdsefi/lockstep/bench/libs\t[no test files]", true},
		{"warning: queue is full", false},
		{"error: actor stopped", false},
		{"goos:linux", false},
		{"os: linux", false},
		{"PASS: 3 tests", false},
		{"FAIL", false},
		{"2026/10/29 05:03:50 INFO shutdown started", false},
		{"time=2026-10-29T05:03:50Z level=INFO msg=stopped", false},
		{"", false},
	} {
		if got := goTestLine.MatchString(c.line); got != c.want {
			t.Errorf("goTestLine.MatchString(%q) = %v, want %v", c.line, got, c.want)
		}
	}
}

// TestParseResultLine checks parseResultLine on well-formed result lines and on lines that start
// with Benchmark but are not result lines, among them a result line a log line broke into.
func TestParseResultLine(t *testing.T) {
	for _, c := range []struct {
		line  string
		name  string   // "" for a malformed line
		units []string // of a well-formed line
	}{
		{
			line:  "BenchmarkX/lib=lockstep-8   \t     1000\t       512 ns/op\t       0 B/op\t       0 allocs/op",
			name:  "BenchmarkX/lib=lockstep",
			units: []string{"ns/op", "B/op", "allocs/op"},
		},
		{
			line:  "BenchmarkIdleActors/lib=goakt/n=10000 \t 1\t 5.5e+07 ns/op\t 0.0001000 goroutines/actor",
			name:  "BenchmarkIdleActors/lib=goakt/n=10000",
			units: []string{"ns/op", "goroutines/actor"},
		},
		{line: "BenchmarkLog/lib=lockstep-16  \t2026/10/29 05:03:50 INFO shutdown started"},
		{line: "BenchmarkY 10 abc ns/op"},
		{line: "BenchmarkY 0 1 ns/op"},
		{line: "BenchmarkY 1 +Inf msgs/s"},
		{line: "BenchmarkY 1 1 ns/op 2"},
		{line: "BenchmarkY 1"},
		{line: "BenchmarkY"},
	} {
		name, units, err := parseResultLine(c.line)
		switch {
		case c.name == "" && err == nil:
			t.Errorf("parseResultLine(%q) = %q, %q, want an error", c.line, name, units)
		case c.name != "" && err != nil:
			t.Errorf("parseResultLine(%q): %v", c.line, err)
		case c.name != "" && (name != c.name || !slices.Equal(units, c.units)):
			t.Errorf("parseResultLine(%q) = %q, %q, want %q, %q", c.line, name, units, c.name, c.units)
		}
	}
}
