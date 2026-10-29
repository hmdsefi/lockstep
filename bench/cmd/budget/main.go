// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command budget checks Go benchmark results against performance budgets, derives the budgets
// from a baseline, and writes the results report.
//
// Usage:
//
//	budget check -budgets <budgets.txt> <results>...
//	budget derive <baseline.txt>
//	budget report <baseline.txt>
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
//
// derive reads a baseline and writes the budgets file for it to stdout. A baseline is a results
// file with the comment lines # commit: <sha>, # date: <YYYY-MM-DD> and # runner: <label> in
// front. derive writes a budget for every lockstep result and every BenchmarkSimSeeds result, in
// the units below, and none for msgs/s, seeds/s and ns/actor, which restate ns/op. A result in a
// rel unit that has no reference makes the baseline malformed, and so does a baseline that yields
// no budget.
//
//	allocs/op          abs   c + floor(c/100), where c is the median rounded up to an integer
//	B/op, B/actor      abs   the median plus 10 percent, rounded up to an integer
//	goroutines/actor   abs   the median, rounded up to hundredths
//	ns/op, p50-ns      rel   the ratio to the reference plus 25 percent, rounded up to hundredths
//	p99-ns             rel   the ratio to the reference plus 50 percent, rounded up to hundredths
//
// The reference of a rel budget is the same benchmark in another library, the one with the lowest
// median in that unit; protoactor wins a tie over hollywood, and hollywood over goakt. The
// reference of BenchmarkAskPipe and BenchmarkSimSeeds, which no other library runs, is
// BenchmarkCalibrate ns/op. The budgets come out sorted by name, then by unit.
//
// report reads a baseline and writes a Markdown document to stdout: a title, the goos, goarch and
// cpu lines and the # commit: and # date: lines of the baseline, then a section for each
// benchmark family with a table for each unit. A table has a row for each parameter value and a
// column for each library, in the order lockstep, protoactor, hollywood, goakt. Each cell is a
// median, and n/a marks a library with no result.
//
// derive and report exit 0 on success and 2 on a malformed baseline or a usage error.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

// usage is the command line summary printed with every usage error.
const usage = "usage: budget check -budgets <budgets.txt> <results>...\n" +
	"       budget derive <baseline.txt>\n" +
	"       budget report <baseline.txt>\n"

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
	case "derive":
		return baselineCommand("derive", args[1:], stdout, stderr, derive)
	case "report":
		return baselineCommand("report", args[1:], stdout, stderr,
			func(_ string, res *results) (string, error) { return report(res), nil })
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stderr, usage)
		return 0
	default:
		return usageError(stderr, fmt.Sprintf("unknown command %q", args[0]))
	}
}

// baselineCommand runs budget derive or budget report. It reads the baseline named by args, and
// writes the text that render builds from it to stdout, or nothing when render fails. render
// receives the baseline's file name for its error messages.
func baselineCommand(name string, args []string, stdout, stderr io.Writer, render func(file string, res *results) (string, error)) int {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	if code, done := parseFlags(flags, args, stderr); done {
		return code
	}
	switch flags.NArg() {
	case 0:
		return usageError(stderr, "missing baseline file")
	case 1:
	default:
		return usageError(stderr, "too many baseline files")
	}
	file := flags.Arg(0)
	res, err := readResults([]string{file})
	if err != nil {
		return inputError(stderr, err)
	}
	text, err := render(file, res)
	if err != nil {
		return inputError(stderr, err)
	}
	fmt.Fprint(stdout, text)
	return 0
}

// parseFlags parses the arguments of a subcommand. It reports done and the exit code when the
// command is over: a help request prints the usage and exits 0, and a bad flag is a usage error.
func parseFlags(flags *flag.FlagSet, args []string, stderr io.Writer) (code int, done bool) {
	flags.SetOutput(io.Discard)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stderr, usage)
			return 0, true
		}
		return usageError(stderr, err.Error()), true
	}
	return 0, false
}

// usageError prints msg and the usage summary to stderr and returns exit code 2.
func usageError(stderr io.Writer, msg string) int {
	fmt.Fprintf(stderr, "budget: %s\n%s", msg, usage)
	return 2
}
