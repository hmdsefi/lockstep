// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
)

// calibration is the reference of the benchmarks that have no counterpart in another library
// (BEN-110, BEN-170).
var calibration = series{"BenchmarkCalibrate", "ns/op"}

// rule says how derive turns the median of one unit into a budget (BEN-170).
type rule struct {
	// kind is kindAbs, or kindRel for a ratio against the reference.
	kind string
	// factor is the headroom the budget leaves above the baseline: a rel budget is the ratio to the
	// reference, which is another library, times factor.
	factor float64
	// calibrationFactor is the factor of a rel budget whose reference is BenchmarkCalibrate. It is
	// larger than factor because the CPU-bound loop of BenchmarkCalibrate moves more against
	// lockstep from one runner CPU model to another than another library does. An abs rule has no
	// reference and leaves it zero.
	calibrationFactor float64
	// ceil rounds a value up to the precision of the budget.
	ceil func(float64) float64
}

// rules holds the rule of every unit that gets a budget. The units msgs/s, seeds/s and ns/actor
// restate ns/op and have none.
var rules = map[string]rule{
	"allocs/op":        {kind: kindAbs, factor: 1, ceil: ceilAllocs},
	"B/op":             {kind: kindAbs, factor: 1.10, ceil: ceilInt},
	"B/actor":          {kind: kindAbs, factor: 1.10, ceil: ceilInt},
	"goroutines/actor": {kind: kindAbs, factor: 1, ceil: ceilHundredths},
	"ns/op":            {kind: kindRel, factor: 1.5, calibrationFactor: 2, ceil: ceil3},
	"p50-ns":           {kind: kindRel, factor: 1.5, calibrationFactor: 2, ceil: ceil3},
	"p99-ns":           {kind: kindRel, factor: 2, calibrationFactor: 3, ceil: ceil3},
}

// ceilInt returns the smallest integer that is at least x, within the tolerance of floating-point
// error: 100 * 1.10 is 110.00000000000001 and rounds to 110 (BEN §11).
func ceilInt(x float64) float64 { return noNegativeZero(math.Ceil(x - tolerance)) }

// ceilAllocs returns c + max(1, floor(c/100)), where c is ceilInt(x): one allocation of headroom
// below 200 allocations and 1 percent from there up. Go prints allocs/op truncated to an integer,
// so a count a fraction below the next integer prints one higher after a change too small to
// matter, and the count of a parallel benchmark moves with contention. Those of
// BenchmarkIdleActors also vary by a few between runs, as its op spawns n actors while the
// engine's workers run.
func ceilAllocs(x float64) float64 {
	c := ceilInt(x)
	return c + max(1, math.Floor(c/100))
}

// ceilHundredths returns the smallest multiple of 0.01 that is at least x, within the same
// tolerance, so that a median of 0.07, which times 100 is 7.000000000000001, gives 0.07 and not
// 0.08. Only the goroutines/actor budget uses it.
func ceilHundredths(x float64) float64 { return noNegativeZero(math.Ceil(x*100-tolerance) / 100) }

// ceil3 returns the smallest number with three significant digits that is at least x, within the
// same tolerance: 0 for 0, and otherwise ceil(x * 10^(2-e)) / 10^(2-e), where e is the exponent of
// x's leading digit (BEN-170). 160/100 * 1.5 is 2.4000000000000004 and gives 2.4 and not 2.41, and
// a small ratio keeps its precision: 0.0013928 gives 0.0014, not the 0.01 that hundredths would.
func ceil3(x float64) float64 {
	if x == 0 {
		return 0
	}
	return atThreeDigits(x, func(v float64) float64 { return math.Ceil(v - tolerance) })
}

// atThreeDigits scales x so that three significant digits lie before the decimal point, applies
// round to the scaled value, and scales the result back. When x has more than three digits before
// the decimal point it divides by the power of ten instead of multiplying by its inverse, because
// 10^n is exact for the n that matter here and 10^-n is not. The display of a ratio in check uses
// the same form, which agrees with the multiplying formula below 1e7. x must be finite and not
// zero.
func atThreeDigits(x float64, round func(float64) float64) float64 {
	e := int(math.Floor(math.Log10(math.Abs(x))))
	if e > 2 {
		unit := math.Pow10(e - 2)
		return round(x/unit) * unit
	}
	scale := math.Pow10(2 - e)
	return round(x*scale) / scale
}

// noNegativeZero returns 0 for -0, which formats as "-0", and v otherwise.
func noNegativeZero(v float64) float64 {
	if v == 0 {
		return 0
	}
	return v
}

// derive builds the budgets file for the baseline in res, read from file (BEN-170): a budget for
// every lockstep result and every BenchmarkSimSeeds result in a unit that has a rule, sorted by
// name then unit. A baseline that yields no budget is malformed.
func derive(file string, res *results) (string, error) {
	var picked []series
	for _, s := range res.order {
		_, hasRule := rules[s.unit]
		if hasRule && (isLockstep(s.name) || strings.HasPrefix(s.name, "BenchmarkSimSeeds/")) {
			picked = append(picked, s)
		}
	}
	if len(picked) == 0 {
		return "", fmt.Errorf("%s: no budgets", file)
	}
	slices.SortFunc(picked, func(a, b series) int {
		return cmp.Or(strings.Compare(a.name, b.name), strings.Compare(a.unit, b.unit))
	})

	var out strings.Builder
	for _, s := range picked {
		b, err := deriveBudget(res, s)
		if err != nil {
			return "", fmt.Errorf("%s: %w", file, err)
		}
		out.WriteString(b.line())
		out.WriteByte('\n')
	}
	return out.String(), nil
}

// deriveBudget builds the budget for series s, whose unit has a rule.
func deriveBudget(res *results, s series) (budget, error) {
	r := rules[s.unit]
	median, _ := res.median(s) // readResults accepts finite values only
	if median < 0 {
		return budget{}, fmt.Errorf("%s %s has median %s, want a non-negative number", s.name, s.unit, formatMedian(median))
	}
	b := budget{series: s, kind: r.kind}
	if r.kind == kindAbs {
		b.max = r.ceil(median * r.factor)
		return b, nil
	}

	ref, err := reference(res, s)
	if err != nil {
		return budget{}, err
	}
	refMedian, _ := res.median(ref)
	if refMedian <= 0 {
		return budget{}, fmt.Errorf("reference %s %s has median %s, want a positive number",
			ref.name, ref.unit, formatMedian(refMedian))
	}
	b.ref = ref
	factor := r.factor
	if ref == calibration {
		factor = r.calibrationFactor
	}
	b.max = r.ceil(median / refMedian * factor)
	return b, nil
}

// reference returns the series that a rel budget for s divides by: BenchmarkCalibrate for
// BenchmarkAskPipe and BenchmarkSimSeeds, which no other library runs, and otherwise the same
// benchmark in the other library with the lowest median in s's unit. A tie goes to the library
// that comes first in libs.
func reference(res *results, s series) (series, error) {
	if strings.HasPrefix(s.name, "BenchmarkAskPipe/") || strings.HasPrefix(s.name, "BenchmarkSimSeeds/") {
		if _, ok := res.median(calibration); !ok {
			return series{}, fmt.Errorf("no reference for %s %s: %s %s is missing",
				s.name, s.unit, calibration.name, calibration.unit)
		}
		return calibration, nil
	}

	var best series
	var bestMedian float64
	var found bool
	for _, lib := range libs {
		if lib == "lockstep" {
			continue
		}
		c := series{counterpart(s.name, lib), s.unit}
		if m, ok := res.median(c); ok && (!found || m < bestMedian) {
			best, bestMedian, found = c, m, true
		}
	}
	if !found {
		return series{}, fmt.Errorf("no reference for %s %s: no other library reports it", s.name, s.unit)
	}
	return best, nil
}

// isLockstep reports whether name is a benchmark of lockstep: it has the path element lib=lockstep
// and something before it.
func isLockstep(name string) bool {
	return strings.Contains(name, "/lib=lockstep/") || strings.HasSuffix(name, "/lib=lockstep")
}

// counterpart returns the name of the lockstep benchmark name in library lib.
func counterpart(name, lib string) string {
	if prefix, ok := strings.CutSuffix(name, "/lib=lockstep"); ok {
		return prefix + "/lib=" + lib
	}
	return strings.Replace(name, "/lib=lockstep/", "/lib="+lib+"/", 1)
}

// line formats b as a line of a budgets file, without the line ending.
func (b budget) line() string {
	if b.kind == kindAbs {
		return fmt.Sprintf("%s %s abs %s", b.name, b.unit, formatFloat(b.max))
	}
	return fmt.Sprintf("%s %s rel %s %s %s", b.name, b.unit, b.ref.name, b.ref.unit, formatFloat(b.max))
}
