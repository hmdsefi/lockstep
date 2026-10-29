// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
)

// tolerance absorbs floating-point error when a median or ratio equals its limit (BEN §11).
const tolerance = 1e-9

// check runs budget check -budgets <file> <results>... (BEN-160) and returns the exit code.
func check(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	budgetsPath := flags.String("budgets", "", "the budgets file")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stderr, usage)
			return 0
		}
		return usageError(stderr, err.Error())
	}
	if *budgetsPath == "" {
		return usageError(stderr, "missing -budgets flag")
	}
	if flags.NArg() == 0 {
		return usageError(stderr, "missing results files")
	}

	var budgets []budget
	data, err := os.ReadFile(*budgetsPath)
	noBudgets := errors.Is(err, fs.ErrNotExist)
	switch {
	case noBudgets:
	case err != nil:
		return inputError(stderr, err)
	default:
		if budgets, err = parseBudgets(*budgetsPath, string(data)); err != nil {
			return inputError(stderr, err)
		}
	}
	res, err := readResults(flags.Args())
	if err != nil {
		return inputError(stderr, err)
	}
	if noBudgets {
		fmt.Fprintln(stdout, "budget: no budgets file; nothing enforced")
		return 0
	}

	var failed bool
	var missing []series // in order of first mention, each once
	seen := map[series]bool{}
	for _, b := range budgets {
		ok, line, absent := evaluate(b, res)
		for _, s := range absent {
			if !seen[s] {
				seen[s] = true
				missing = append(missing, s)
			}
		}
		if len(absent) > 0 {
			continue
		}
		fmt.Fprintln(stdout, line)
		failed = failed || !ok
	}
	for _, s := range missing {
		fmt.Fprintf(stdout, "budget: missing result %s %s\n", s.name, s.unit)
	}
	switch {
	case len(missing) > 0:
		return 2
	case failed:
		return 1
	default:
		return 0
	}
}

// evaluate checks one budget against res. It returns whether the budget holds and its output
// line, or the series the budget needs that res lacks.
func evaluate(b budget, res *results) (ok bool, line string, absent []series) {
	median, found := res.median(b.series)
	if !found {
		absent = append(absent, b.series)
	}
	value, shown := median, formatFloat(median)
	if b.kind == kindRel {
		ref, found := res.median(b.ref)
		if !found {
			absent = append(absent, b.ref)
		}
		value = median / ref
		shown += " / " + formatFloat(ref) + " = " + strconv.FormatFloat(value, 'f', 2, 64)
	}
	if len(absent) > 0 {
		return false, "", absent
	}
	// A NaN value fails: every comparison with NaN is false.
	if value <= b.max+tolerance {
		return true, fmt.Sprintf("ok   %s %s %s <= %s", b.name, b.unit, shown, b.maxText), nil
	}
	return false, fmt.Sprintf("FAIL %s %s %s > %s", b.name, b.unit, shown, b.maxText), nil
}

// formatFloat formats a median with the fewest digits that represent it exactly.
func formatFloat(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// inputError prints err, an unreadable or malformed file, to stderr and returns exit code 2.
func inputError(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "budget: %v\n", err)
	return 2
}
