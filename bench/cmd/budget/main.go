// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command budget checks Go benchmark results against performance budgets.
//
// Usage:
//
//	budget check -budgets <budgets.txt> <results>...
//
// check reads go test -bench output from the results files and, for each benchmark and unit,
// takes the median of the samples in all files. It ignores the -N GOMAXPROCS suffix that go test
// appends to benchmark names. Then it prints one line per budget, in budgets file order, and after
// them each result a budget names but no results file reports:
//
//	ok   BenchmarkPingPong/lib=lockstep/pairs=1 allocs/op 1 <= 1
//	FAIL BenchmarkPingPong/lib=lockstep/pairs=1 ns/op 100 / 55 = 1.82 > 1.80
//	budget: missing result BenchmarkFanOut/lib=lockstep/consumers=3 ns/op
//
// A budgets file holds one budget per line, with fields separated by single spaces:
//
//	<name> <unit> abs <max>
//	<name> <unit> rel <reference-name> <reference-unit> <max-ratio>
//
// An abs budget caps the median. A rel budget caps the median divided by the median of the
// reference benchmark in the reference unit; the ratio is printed rounded to two decimals and
// compared unrounded. Budgets take only units where lower is better: ns/op, p50-ns, p99-ns,
// allocs/op, B/op, B/actor and goroutines/actor. Lines that start with # are comments, and blank
// lines are ignored.
//
// check exits 0 when every budget holds or the budgets file does not exist, 1 when a budget is
// exceeded, and 2 on malformed input or a missing result.
package main

import (
	"fmt"
	"io"
	"os"
)

// usage is the command line summary printed with every usage error.
const usage = "usage: budget check -budgets <budgets.txt> <results>...\n"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run runs the subcommand args[0] with the remaining arguments and returns the exit code: 0 for
// success, 1 for an exceeded budget, 2 for malformed input, a missing result or a usage error.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "check":
		return check(args[1:], stdout, stderr)
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stderr, usage)
		return 0
	default:
		return usageError(stderr, fmt.Sprintf("unknown command %q", args[0]))
	}
}

// usageError prints msg and the usage summary to stderr and returns exit code 2.
func usageError(stderr io.Writer, msg string) int {
	fmt.Fprintf(stderr, "budget: %s\n%s", msg, usage)
	return 2
}
