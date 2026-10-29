// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

// series names the samples of one benchmark in one unit. The name has no GOMAXPROCS suffix.
type series struct {
	name, unit string
}

// results holds what readResults read from go test -bench output (BEN-140).
type results struct {
	// config holds the configuration lines, such as goos: linux, in input order.
	config []config
	// comments holds the comment lines of the form # key: value, such as # commit: 0f3a9c1, in
	// input order.
	comments []config
	// order lists each series once, in the order its first sample appears.
	order []series
	// samples holds every value of each series, in input order.
	samples map[series][]float64
}

// config is one configuration line, key: value, without trailing white space.
type config struct {
	key, value string
}

// configLine matches the start of a configuration line, up to the value.
var configLine = regexp.MustCompile(`^[a-z][^\s:]*: `)

// commentLine matches the start of a comment line of the form # key: value, up to the value, and
// captures the key.
var commentLine = regexp.MustCompile(`^# ([a-z][^\s:]*): `)

// libs lists the libraries in the order of Libs in package bench/libs: the order of the report's
// columns, and the order that breaks a tie between references. This package cannot call Libs,
// since the package that holds it imports every library, and it keeps to the standard library.
var libs = []string{"lockstep", "protoactor", "hollywood", "goakt"}

// lineError is malformed input at one line of a file. Its text is <file>:<line>: <reason>.
type lineError struct {
	file   string
	line   int
	reason error
}

func (e *lineError) Error() string { return fmt.Sprintf("%s:%d: %v", e.file, e.line, e.reason) }

// readResults reads and parses the results files at paths. The samples of a series pool across
// all files.
func readResults(paths []string) (*results, error) {
	r := &results{samples: map[series][]float64{}}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		for n, line := range lineNumbers(string(data)) {
			switch {
			case strings.HasPrefix(line, "Benchmark"):
				if err := r.addResult(line); err != nil {
					return nil, &lineError{path, n, err}
				}
			case configLine.MatchString(line):
				key, value, _ := strings.Cut(line, ": ")
				r.config = append(r.config, config{key, strings.TrimRightFunc(value, unicode.IsSpace)})
			default:
				if m := commentLine.FindStringSubmatch(line); m != nil {
					value := strings.TrimRightFunc(line[len(m[0]):], unicode.IsSpace)
					r.comments = append(r.comments, config{m[1], value})
				}
			}
		}
	}
	return r, nil
}

// addResult parses a result line, Benchmark<name> <iterations> <value> <unit>..., and adds its
// values to the samples.
func (r *results) addResult(line string) error {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return errors.New("missing iteration count")
	}
	if n, err := strconv.ParseUint(fields[1], 10, 64); err != nil || n == 0 {
		return fmt.Errorf("invalid iteration count %q", fields[1])
	}
	pairs := fields[2:]
	if len(pairs) == 0 {
		return errors.New("missing values")
	}
	name := trimProcs(fields[0])
	for i := 0; i < len(pairs); i += 2 {
		v, err := strconv.ParseFloat(pairs[i], 64)
		if err != nil {
			return fmt.Errorf("invalid value %q", pairs[i])
		}
		if i+1 == len(pairs) {
			return fmt.Errorf("value %q has no unit", pairs[i])
		}
		s := series{name, pairs[i+1]}
		if _, seen := r.samples[s]; !seen {
			r.order = append(r.order, s)
		}
		r.samples[s] = append(r.samples[s], v)
	}
	return nil
}

// trimProcs removes the -N GOMAXPROCS suffix go test appends to a benchmark name.
func trimProcs(name string) string {
	i := strings.LastIndexByte(name, '-')
	if i < 0 || i == len(name)-1 {
		return name
	}
	for _, c := range name[i+1:] {
		if c < '0' || c > '9' {
			return name
		}
	}
	return name[:i]
}

// median returns the median of the samples of s, and false when there are none: the middle
// value, or the mean of the two middle values for an even count.
func (r *results) median(s series) (float64, bool) {
	v := slices.Clone(r.samples[s])
	if len(v) == 0 {
		return 0, false
	}
	slices.Sort(v)
	m := len(v) / 2
	if len(v)%2 == 1 {
		return v[m], true
	}
	return (v[m-1] + v[m]) / 2, true
}

// Budget kinds (BEN-150).
const (
	kindAbs = "abs"
	kindRel = "rel"
)

// budgetUnits are the units where lower is better, the only ones a budget may name (BEN-150).
var budgetUnits = []string{"ns/op", "p50-ns", "p99-ns", "allocs/op", "B/op", "B/actor", "goroutines/actor"}

// budget is one line of a budgets file (BEN-150).
type budget struct {
	series
	// kind is kindAbs or kindRel.
	kind string
	// ref is the reference series of a rel budget.
	ref series
	// max is the largest median of an abs budget, or the largest ratio of a rel budget.
	max float64
	// maxText is max as the budgets file writes it.
	maxText string
}

// parseBudgets parses the text of the budgets file named file.
func parseBudgets(file, text string) ([]budget, error) {
	var budgets []budget
	for n, line := range lineNumbers(text) {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		b, err := parseBudget(line)
		if err != nil {
			return nil, &lineError{file, n, err}
		}
		budgets = append(budgets, b)
	}
	return budgets, nil
}

// parseBudget parses one budget line.
func parseBudget(line string) (budget, error) {
	fields := strings.Split(line, " ")
	otherSpace := func(c rune) bool { return c != ' ' && unicode.IsSpace(c) }
	if slices.Contains(fields, "") || strings.ContainsFunc(line, otherSpace) {
		return budget{}, errors.New("fields must be separated by single spaces")
	}
	if len(fields) < 3 {
		return budget{}, errors.New("missing kind, want abs or rel")
	}
	b := budget{series: series{fields[0], fields[1]}, kind: fields[2]}
	var want int
	switch b.kind {
	case kindAbs:
		want = 4
	case kindRel:
		want = 6
	default:
		return budget{}, fmt.Errorf("unknown kind %q, want abs or rel", b.kind)
	}
	if len(fields) != want {
		return budget{}, fmt.Errorf("%s budget has %d fields, want %d", b.kind, len(fields), want)
	}
	if !slices.Contains(budgetUnits, b.unit) {
		return budget{}, fmt.Errorf("unit %q cannot have a budget, want one of %s", b.unit, strings.Join(budgetUnits, ", "))
	}
	maxName := "max"
	if b.kind == kindRel {
		b.ref = series{fields[3], fields[4]}
		maxName = "max-ratio"
	}
	b.maxText = fields[want-1]
	v, err := strconv.ParseFloat(b.maxText, 64)
	if err != nil || v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return budget{}, fmt.Errorf("invalid %s %q", maxName, b.maxText)
	}
	b.max = v
	return b, nil
}

// lineNumbers yields each line of text with its 1-based number, without the line ending.
func lineNumbers(text string) iter.Seq2[int, string] {
	return func(yield func(int, string) bool) {
		n := 0
		for line := range strings.Lines(text) {
			n++
			line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
			if !yield(n, line) {
				return
			}
		}
	}
}
