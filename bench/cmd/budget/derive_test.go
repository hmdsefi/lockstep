// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// baseline is the AT-BEN-10 fixture: PingPong pairs=1 at 100, 80, 60 and 90 ns/op for lockstep,
// protoactor, hollywood and goakt, with several samples each, a SendLatency p99 without goakt,
// BenchmarkCalibrate and one BenchmarkSimSeeds result.
var baseline = filepath.Join("testdata", "derive-report.txt")

// deriveRun runs budget derive on a baseline made of lines and returns the exit code and outputs.
func deriveRun(t *testing.T, lines ...string) (code int, stdout, stderr string) {
	t.Helper()
	return budgetRun(t, "derive", writeFile(t, "baseline.txt", lines...))
}

// TestATBEN10Derive covers AT-BEN-10: the fixture baseline gives exactly five budgets, sorted,
// and none for the other libraries, msgs/s or seeds/s (BEN-170).
func TestATBEN10Derive(t *testing.T) {
	wantRun(t, []string{"derive", baseline}, 0, lines(
		"BenchmarkPingPong/lib=lockstep/pairs=1 B/op abs 37",
		"BenchmarkPingPong/lib=lockstep/pairs=1 allocs/op abs 1",
		"BenchmarkPingPong/lib=lockstep/pairs=1 ns/op rel BenchmarkPingPong/lib=hollywood/pairs=1 ns/op 2.09",
		"BenchmarkSendLatency/lib=lockstep p99-ns rel BenchmarkSendLatency/lib=hollywood p99-ns 2.25",
		"BenchmarkSimSeeds/workload=pingpong ns/op rel BenchmarkCalibrate ns/op 25",
	), "")
}

// TestDeriveHoldOnTheirBaseline checks that the derived budgets pass budget check on the
// baseline they come from, which is what the first run of the CI job after committing both does.
func TestDeriveHoldOnTheirBaseline(t *testing.T) {
	code, stdout, stderr := budgetRun(t, "derive", baseline)
	if code != 0 || stderr != "" {
		t.Fatalf("derive: exit code %d, stderr %q", code, stderr)
	}
	budgets := filepath.Join(t.TempDir(), "budgets.txt")
	if err := os.WriteFile(budgets, []byte(stdout), 0o600); err != nil {
		t.Fatal(err)
	}
	wantRun(t, []string{"check", "-budgets", budgets, baseline}, 0, lines(
		"ok   BenchmarkPingPong/lib=lockstep/pairs=1 B/op 33 <= 37",
		"ok   BenchmarkPingPong/lib=lockstep/pairs=1 allocs/op 1 <= 1",
		"ok   BenchmarkPingPong/lib=lockstep/pairs=1 ns/op 100 / 60 = 1.67 <= 2.09",
		"ok   BenchmarkSendLatency/lib=lockstep p99-ns 900 / 600 = 1.50 <= 2.25",
		"ok   BenchmarkSimSeeds/workload=pingpong ns/op 20000 / 1000 = 20.00 <= 25",
	), "")
}

// TestDeriveRules checks BEN-170's rule for each unit, with values at the exact boundaries where
// the product of two floats lands a hair above a limit: ceil(x - 1e-9) for integer budgets and
// ceil(x*100 - 1e-9) / 100 for two-decimal ones (BEN §11). Without the tolerance, the median
// 100 B/op gives 111, not 110.
func TestDeriveRules(t *testing.T) {
	// For a rel budget the counterpart is BenchmarkA/lib=hollywood, with the median in ref.
	for _, c := range []struct {
		unit   string
		median string
		ref    string // empty for an abs budget
		want   string // the rest of the budget line, after the unit
	}{
		{"allocs/op", "0", "", "abs 0"},
		{"allocs/op", "1", "", "abs 1"},
		{"allocs/op", "1.0000000001", "", "abs 1"},
		{"allocs/op", "1.5", "", "abs 2"},
		{"allocs/op", "2.01", "", "abs 3"},
		{"B/op", "0", "", "abs 0"},
		{"B/op", "33", "", "abs 37"},
		{"B/op", "100", "", "abs 110"},
		{"B/op", "190", "", "abs 209"},
		{"B/op", "101", "", "abs 112"},
		{"B/actor", "200", "", "abs 220"},
		{"B/actor", "1000", "", "abs 1100"},
		{"B/actor", "2199.5", "", "abs 2420"},
		{"goroutines/actor", "0", "", "abs 0"},
		{"goroutines/actor", "1", "", "abs 1"},
		{"goroutines/actor", "0.07", "", "abs 0.07"},
		{"goroutines/actor", "1.005", "", "abs 1.01"},
		{"goroutines/actor", "1.15", "", "abs 1.15"},
		{"ns/op", "100", "60", "rel BenchmarkA/lib=hollywood ns/op 2.09"},
		{"ns/op", "160", "100", "rel BenchmarkA/lib=hollywood ns/op 2"},
		{"ns/op", "80", "100", "rel BenchmarkA/lib=hollywood ns/op 1"},
		{"ns/op", "1", "3", "rel BenchmarkA/lib=hollywood ns/op 0.42"},
		{"ns/op", "0", "3", "rel BenchmarkA/lib=hollywood ns/op 0"},
		{"ns/op", "11", "25", "rel BenchmarkA/lib=hollywood ns/op 0.55"},
		{"p50-ns", "100", "60", "rel BenchmarkA/lib=hollywood p50-ns 2.09"},
		{"p50-ns", "100", "100", "rel BenchmarkA/lib=hollywood p50-ns 1.25"},
		{"p99-ns", "900", "600", "rel BenchmarkA/lib=hollywood p99-ns 2.25"},
		{"p99-ns", "100", "100", "rel BenchmarkA/lib=hollywood p99-ns 1.5"},
		{"p99-ns", "400", "500", "rel BenchmarkA/lib=hollywood p99-ns 1.2"},
		{"p99-ns", "8", "5", "rel BenchmarkA/lib=hollywood p99-ns 2.4"},
		{"p99-ns", "1", "5", "rel BenchmarkA/lib=hollywood p99-ns 0.3"},
	} {
		t.Run(c.unit+" "+c.median+" "+c.ref, func(t *testing.T) {
			results := []string{"BenchmarkA/lib=lockstep-8 10 " + c.median + " " + c.unit}
			if c.ref != "" {
				results = append(results, "BenchmarkA/lib=hollywood-8 10 "+c.ref+" "+c.unit)
			}
			wantLine := "BenchmarkA/lib=lockstep " + c.unit + " " + c.want + "\n"
			code, stdout, stderr := deriveRun(t, results...)
			if code != 0 || stdout != wantLine || stderr != "" {
				t.Errorf("exit code %d, stdout %q, stderr %q; want 0, %q, nothing", code, stdout, stderr, wantLine)
			}
		})
	}

	t.Run("medians of several samples", func(t *testing.T) {
		code, stdout, stderr := deriveRun(t,
			"BenchmarkA/lib=lockstep 10 300 B/op",
			"BenchmarkA/lib=lockstep 10 100 B/op",
			"BenchmarkA/lib=lockstep 10 150 B/op",
			"BenchmarkA/lib=lockstep 10 250 B/op",
		)
		const want = "BenchmarkA/lib=lockstep B/op abs 220\n" // the median is 200
		if code != 0 || stdout != want || stderr != "" {
			t.Errorf("exit code %d, stdout %q, stderr %q; want 0, %q, nothing", code, stdout, stderr, want)
		}
	})
}

// TestDeriveIdleActorsBoundary restates the AT-BEN-10 baseline with an IdleActors row whose
// lockstep result is 100 B/actor, where 100 * 1.10 is 110.00000000000001 in floating point, and
// checks that its budget is 110, not 111.
func TestDeriveIdleActorsBoundary(t *testing.T) {
	code, stdout, stderr := deriveRun(t,
		"BenchmarkPingPong/lib=lockstep/pairs=1-4 100 100 ns/op 33 B/op 1.0 allocs/op",
		"BenchmarkPingPong/lib=protoactor/pairs=1-4 100 80 ns/op",
		"BenchmarkPingPong/lib=hollywood/pairs=1-4 100 60 ns/op",
		"BenchmarkPingPong/lib=goakt/pairs=1-4 100 90 ns/op",
		"BenchmarkIdleActors/lib=lockstep/n=10000-4 1 5000 ns/actor 100 B/actor",
		"BenchmarkSendLatency/lib=lockstep-4 100 900 p99-ns",
		"BenchmarkSendLatency/lib=protoactor-4 100 700 p99-ns",
		"BenchmarkSendLatency/lib=hollywood-4 100 600 p99-ns",
		"BenchmarkCalibrate-4 100 1000 ns/op",
		"BenchmarkSimSeeds/workload=pingpong 50 20000 ns/op 50000 seeds/s",
	)
	want := lines(
		"BenchmarkIdleActors/lib=lockstep/n=10000 B/actor abs 110",
		"BenchmarkPingPong/lib=lockstep/pairs=1 B/op abs 37",
		"BenchmarkPingPong/lib=lockstep/pairs=1 allocs/op abs 1",
		"BenchmarkPingPong/lib=lockstep/pairs=1 ns/op rel BenchmarkPingPong/lib=hollywood/pairs=1 ns/op 2.09",
		"BenchmarkSendLatency/lib=lockstep p99-ns rel BenchmarkSendLatency/lib=hollywood p99-ns 2.25",
		"BenchmarkSimSeeds/workload=pingpong ns/op rel BenchmarkCalibrate ns/op 25",
	)
	if code != 0 || stdout != want || stderr != "" {
		t.Errorf("exit code %d, stdout:\n%s\nstderr %q; want 0, stdout:\n%s\nand no stderr", code, stdout, stderr, want)
	}
}

// TestDeriveSelection checks which results get a budget: the lockstep results and the
// BenchmarkSimSeeds results, in the units that have a rule (BEN-170).
func TestDeriveSelection(t *testing.T) {
	code, stdout, stderr := deriveRun(t,
		"goos: linux",
		"BenchmarkA/lib=lockstep/n=1 10 5 allocs/op 7 msgs/s 9 ns/actor 11 seeds/s 13 widgets/s",
		"BenchmarkA/lib=protoactor/n=1 10 6 allocs/op",
		"BenchmarkB/lib=lockstep 10 3 allocs/op",
		"BenchmarkB/lib=lockstepx 10 4 allocs/op",
		"BenchmarkB/lib=goakt 10 9 allocs/op",
		"BenchmarkC/kind=lockstep 10 1 allocs/op",
		"BenchmarkCalibrate-8 10 1000 ns/op 8 allocs/op",
		"BenchmarkSimSeeds/workload=w 50 20000 ns/op 50 seeds/s 12 allocs/op",
		"BenchmarkSimSeedsX 50 20000 ns/op",
	)
	const want = `BenchmarkA/lib=lockstep/n=1 allocs/op abs 5
BenchmarkB/lib=lockstep allocs/op abs 3
BenchmarkSimSeeds/workload=w allocs/op abs 12
BenchmarkSimSeeds/workload=w ns/op rel BenchmarkCalibrate ns/op 25
`
	if code != 0 || stdout != want || stderr != "" {
		t.Errorf("exit code %d, stdout:\n%s\nstderr %q; want 0, stdout:\n%s\nand no stderr", code, stdout, stderr, want)
	}
}

// TestDeriveSort checks that budgets come out sorted by name then unit in byte order, whatever
// the order of the baseline (BEN-170).
func TestDeriveSort(t *testing.T) {
	code, stdout, stderr := deriveRun(t,
		"BenchmarkPingPong/lib=lockstep/pairs=procs 10 5 ns/op 7 B/op 1 allocs/op",
		"BenchmarkPingPong/lib=hollywood/pairs=procs 10 5 ns/op",
		"BenchmarkPingPong/lib=lockstep/pairs=1 10 5 ns/op 7 B/op 1 allocs/op",
		"BenchmarkPingPong/lib=hollywood/pairs=1 10 5 ns/op",
		"BenchmarkIdleActors/lib=lockstep/n=100000 10 3 B/actor",
		"BenchmarkIdleActors/lib=lockstep/n=10000 10 3 B/actor",
		"BenchmarkAskPipe/lib=lockstep/extra 10 3 allocs/op",
		"BenchmarkAskPipe/lib=lockstep 10 3 allocs/op",
		"BenchmarkCalibrate 10 1 ns/op",
	)
	const want = `BenchmarkAskPipe/lib=lockstep allocs/op abs 3
BenchmarkAskPipe/lib=lockstep/extra allocs/op abs 3
BenchmarkIdleActors/lib=lockstep/n=10000 B/actor abs 4
BenchmarkIdleActors/lib=lockstep/n=100000 B/actor abs 4
BenchmarkPingPong/lib=lockstep/pairs=1 B/op abs 8
BenchmarkPingPong/lib=lockstep/pairs=1 allocs/op abs 1
BenchmarkPingPong/lib=lockstep/pairs=1 ns/op rel BenchmarkPingPong/lib=hollywood/pairs=1 ns/op 1.25
BenchmarkPingPong/lib=lockstep/pairs=procs B/op abs 8
BenchmarkPingPong/lib=lockstep/pairs=procs allocs/op abs 1
BenchmarkPingPong/lib=lockstep/pairs=procs ns/op rel BenchmarkPingPong/lib=hollywood/pairs=procs ns/op 1.25
`
	if code != 0 || stdout != want || stderr != "" {
		t.Errorf("exit code %d, stdout:\n%s\nstderr %q; want 0, stdout:\n%s\nand no stderr", code, stdout, stderr, want)
	}
}

// TestDeriveReference checks the reference of a rel budget: the counterpart with the lowest
// median in the same unit, the earliest library when medians tie, and BenchmarkCalibrate for
// BenchmarkAskPipe and BenchmarkSimSeeds (BEN-170).
func TestDeriveReference(t *testing.T) {
	t.Run("lowest median per unit", func(t *testing.T) {
		code, stdout, stderr := deriveRun(t,
			"BenchmarkA/lib=lockstep 10 100 ns/op 200 p99-ns",
			"BenchmarkA/lib=protoactor 10 80 ns/op 700 p99-ns",
			"BenchmarkA/lib=hollywood 10 60 ns/op 900 p99-ns",
			"BenchmarkA/lib=goakt 10 90 ns/op 800 p99-ns",
		)
		const want = `BenchmarkA/lib=lockstep ns/op rel BenchmarkA/lib=hollywood ns/op 2.09
BenchmarkA/lib=lockstep p99-ns rel BenchmarkA/lib=protoactor p99-ns 0.43
`
		if code != 0 || stdout != want || stderr != "" {
			t.Errorf("exit code %d, stdout:\n%s\nstderr %q; want 0, stdout:\n%s\nand no stderr", code, stdout, stderr, want)
		}
	})

	t.Run("a tie goes to the earliest library, not the first line", func(t *testing.T) {
		code, stdout, stderr := deriveRun(t,
			"BenchmarkA/lib=goakt 10 60 ns/op",
			"BenchmarkA/lib=hollywood 10 60 ns/op",
			"BenchmarkA/lib=lockstep 10 100 ns/op",
			"BenchmarkA/lib=protoactor 10 60 ns/op",
			"BenchmarkB/lib=goakt 10 60 ns/op",
			"BenchmarkB/lib=hollywood 10 60 ns/op",
			"BenchmarkB/lib=lockstep 10 100 ns/op",
		)
		const want = `BenchmarkA/lib=lockstep ns/op rel BenchmarkA/lib=protoactor ns/op 2.09
BenchmarkB/lib=lockstep ns/op rel BenchmarkB/lib=hollywood ns/op 2.09
`
		if code != 0 || stdout != want || stderr != "" {
			t.Errorf("exit code %d, stdout:\n%s\nstderr %q; want 0, stdout:\n%s\nand no stderr", code, stdout, stderr, want)
		}
	})

	t.Run("the median of several samples decides", func(t *testing.T) {
		code, stdout, stderr := deriveRun(t,
			"BenchmarkA/lib=lockstep 10 100 ns/op",
			"BenchmarkA/lib=protoactor 10 10 ns/op",
			"BenchmarkA/lib=protoactor 10 90 ns/op",
			"BenchmarkA/lib=protoactor 10 95 ns/op",
			"BenchmarkA/lib=hollywood 10 60 ns/op",
			"BenchmarkA/lib=hollywood 10 70 ns/op",
			"BenchmarkA/lib=hollywood 10 80 ns/op",
		)
		// hollywood has the lowest median, 70; protoactor has the lowest mean and minimum.
		const want = "BenchmarkA/lib=lockstep ns/op rel BenchmarkA/lib=hollywood ns/op 1.79\n"
		if code != 0 || stdout != want || stderr != "" {
			t.Errorf("exit code %d, stdout %q, stderr %q; want 0, %q, nothing", code, stdout, stderr, want)
		}
	})

	t.Run("lockstep is not a counterpart of itself", func(t *testing.T) {
		code, stdout, stderr := deriveRun(t,
			"BenchmarkA/lib=lockstep 10 100 ns/op",
			"BenchmarkA/lib=protoactor 10 200 ns/op",
		)
		const want = "BenchmarkA/lib=lockstep ns/op rel BenchmarkA/lib=protoactor ns/op 0.63\n"
		if code != 0 || stdout != want || stderr != "" {
			t.Errorf("exit code %d, stdout %q, stderr %q; want 0, %q, nothing", code, stdout, stderr, want)
		}
	})

	t.Run("AskPipe and SimSeeds use the calibration", func(t *testing.T) {
		code, stdout, stderr := deriveRun(t,
			"BenchmarkAskPipe/lib=lockstep 10 4000 ns/op 3000 p50-ns 9000 p99-ns 5 allocs/op",
			"BenchmarkSimSeeds/workload=crash3 50 40000 ns/op 25 seeds/s",
			"BenchmarkCalibrate-8 10 1000 ns/op",
			"BenchmarkCalibrate-8 10 1000 p50-ns",
		)
		const want = `BenchmarkAskPipe/lib=lockstep allocs/op abs 5
BenchmarkAskPipe/lib=lockstep ns/op rel BenchmarkCalibrate ns/op 5
BenchmarkAskPipe/lib=lockstep p50-ns rel BenchmarkCalibrate ns/op 3.75
BenchmarkAskPipe/lib=lockstep p99-ns rel BenchmarkCalibrate ns/op 13.5
BenchmarkSimSeeds/workload=crash3 ns/op rel BenchmarkCalibrate ns/op 50
`
		if code != 0 || stdout != want || stderr != "" {
			t.Errorf("exit code %d, stdout:\n%s\nstderr %q; want 0, stdout:\n%s\nand no stderr", code, stdout, stderr, want)
		}
	})
}

// TestDeriveErrors checks that a baseline that cannot give a budget fails with exit code 2, the
// file and a short reason, and prints no budget (BEN §7).
func TestDeriveErrors(t *testing.T) {
	for _, c := range []struct {
		name    string
		results []string
		reason  string
	}{
		{
			"no counterpart",
			[]string{"BenchmarkA/lib=lockstep 10 100 ns/op"},
			"no reference for BenchmarkA/lib=lockstep ns/op: no other library reports it",
		},
		{
			"counterparts only in another unit",
			[]string{"BenchmarkA/lib=lockstep 10 100 ns/op", "BenchmarkA/lib=hollywood 10 100 p50-ns"},
			"no reference for BenchmarkA/lib=lockstep ns/op: no other library reports it",
		},
		{
			"counterparts with another parameter",
			[]string{"BenchmarkA/lib=lockstep/n=1 10 100 ns/op", "BenchmarkA/lib=hollywood/n=2 10 100 ns/op"},
			"no reference for BenchmarkA/lib=lockstep/n=1 ns/op: no other library reports it",
		},
		{
			"SimSeeds without BenchmarkCalibrate",
			[]string{"BenchmarkSimSeeds/workload=w 50 20000 ns/op"},
			"no reference for BenchmarkSimSeeds/workload=w ns/op: BenchmarkCalibrate ns/op is missing",
		},
		{
			"AskPipe with a calibration in another unit",
			[]string{"BenchmarkAskPipe/lib=lockstep 10 20000 p99-ns", "BenchmarkCalibrate 10 1000 p99-ns"},
			"no reference for BenchmarkAskPipe/lib=lockstep p99-ns: BenchmarkCalibrate ns/op is missing",
		},
		{
			"a reference of zero",
			[]string{"BenchmarkA/lib=lockstep 10 100 ns/op", "BenchmarkA/lib=hollywood 10 0 ns/op"},
			"reference BenchmarkA/lib=hollywood ns/op has median 0, want a positive finite number",
		},
		{
			"a reference that is not a number",
			[]string{"BenchmarkA/lib=lockstep 10 100 ns/op", "BenchmarkA/lib=hollywood 10 NaN ns/op"},
			"reference BenchmarkA/lib=hollywood ns/op has median NaN, want a positive finite number",
		},
		{
			"an infinite median",
			[]string{"BenchmarkA/lib=lockstep 10 +Inf allocs/op"},
			"BenchmarkA/lib=lockstep allocs/op has median +Inf, want a non-negative finite number",
		},
		{
			"a negative median",
			[]string{"BenchmarkA/lib=lockstep 10 -3 B/op"},
			"BenchmarkA/lib=lockstep B/op has median -3, want a non-negative finite number",
		},
		{
			"the first error wins",
			[]string{
				"BenchmarkB/lib=lockstep 10 100 ns/op",
				"BenchmarkA/lib=lockstep 10 100 ns/op",
				"BenchmarkA/lib=lockstep 10 5 allocs/op",
			},
			"no reference for BenchmarkA/lib=lockstep ns/op: no other library reports it",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := writeFile(t, "baseline.txt", c.results...)
			wantRun(t, []string{"derive", path}, 2, "", "budget: "+path+": "+c.reason+"\n")
		})
	}

	t.Run("malformed baseline", func(t *testing.T) {
		malformed := filepath.Join("testdata", "malformed.txt")
		wantRun(t, []string{"derive", malformed}, 2, "", `budget: `+malformed+`:3: invalid value "abc"`+"\n")
	})

	t.Run("missing baseline", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "baseline.txt")
		code, stdout, stderr := budgetRun(t, "derive", missing)
		if code != 2 || stdout != "" || !strings.HasPrefix(stderr, "budget: ") || !strings.Contains(stderr, missing) {
			t.Errorf("exit code %d, stdout %q, stderr %q; want 2, nothing, an error naming %s", code, stdout, stderr, missing)
		}
	})
}

// TestDeriveEmpty checks that a baseline with no lockstep result gives an empty budgets file.
func TestDeriveEmpty(t *testing.T) {
	code, stdout, stderr := deriveRun(t,
		"BenchmarkA/lib=hollywood 10 100 ns/op",
		"BenchmarkCalibrate 10 1000 ns/op",
	)
	if code != 0 || stdout != "" || stderr != "" {
		t.Errorf("exit code %d, stdout %q, stderr %q; want 0 and no output", code, stdout, stderr)
	}
}
