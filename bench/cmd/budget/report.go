// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"slices"
	"strings"
)

// noResult is the cell of a library that has no result in a row, an em dash.
const noResult = "\u2014"

// noParam is the parameter of a benchmark that has none, such as BenchmarkSendLatency.
const noParam = "-"

// intro is the sentence under the title of the report.
const intro = "Each number is the median of the runs in [`results/baseline.txt`](results/baseline.txt), " +
	"which the `bench` CI job measured on the configuration below. " +
	"A cell that holds `" + noResult + "` has no result."

// families lists the benchmark families in the order of the report (BEN-180).
var families = []string{"PingPong", "FanOut", "IdleActors", "SendLatency", "AskLatency", "AskPipe", "SimSeeds"}

// reportUnits lists the units the report shows, in the order of its tables (BEN-180).
var reportUnits = []string{"ns/op", "p50-ns", "p99-ns", "B/actor", "allocs/op"}

// table identifies one table of the report.
type table struct {
	family, unit string
}

// cell identifies one cell of the report: a row of a table and a library.
type cell struct {
	table
	param, lib string
}

// report builds the Markdown report for the baseline in res (BEN-180): the title, the header
// lines of the baseline, and one table for each family and unit with one row per parameter and
// one column per library.
func report(res *results) string {
	rows := map[table][]string{} // parameters in the order they first appear
	cells := map[cell]string{}
	for _, s := range res.order {
		family, lib, param := splitName(s.name)
		if !slices.Contains(families, family) || !slices.Contains(libs, lib) || !slices.Contains(reportUnits, s.unit) {
			continue
		}
		t := table{family, s.unit}
		if !slices.Contains(rows[t], param) {
			rows[t] = append(rows[t], param)
		}
		median, _ := res.median(s)
		cells[cell{t, param, lib}] = formatFloat(median)
	}

	blocks := []string{"# Benchmark results", intro}
	if h := header(res); h != "" {
		blocks = append(blocks, h)
	}
	for _, family := range families {
		for _, unit := range reportUnits {
			t := table{family, unit}
			if len(rows[t]) == 0 {
				continue
			}
			text := []string{
				"| parameter | " + strings.Join(libs, " | ") + " |",
				"|---" + strings.Repeat("|---:", len(libs)) + "|",
			}
			for _, param := range rows[t] {
				row := []string{param}
				for _, lib := range libs {
					v, ok := cells[cell{t, param, lib}]
					if !ok {
						v = noResult
					}
					row = append(row, v)
				}
				text = append(text, "| "+strings.Join(row, " | ")+" |")
			}
			blocks = append(blocks, "### "+family+" ("+unit+")", strings.Join(text, "\n"))
		}
	}
	return strings.Join(blocks, "\n\n") + "\n"
}

// header returns the code block that holds the goos, goarch and cpu configuration lines and the
// # commit: and # date: comment lines of the baseline, as the baseline has them, each from its
// first occurrence. It returns "" when the baseline has none of them.
func header(res *results) string {
	var lines []string
	for _, key := range []string{"goos", "goarch", "cpu"} {
		if v, ok := firstValue(res.config, key); ok {
			lines = append(lines, key+": "+v)
		}
	}
	for _, key := range []string{"commit", "date"} {
		if v, ok := firstValue(res.comments, key); ok {
			lines = append(lines, "# "+key+": "+v)
		}
	}
	if len(lines) == 0 {
		return ""
	}
	return "```text\n" + strings.Join(lines, "\n") + "\n```"
}

// firstValue returns the value of the first entry of kv with the given key.
func firstValue(kv []config, key string) (string, bool) {
	i := slices.IndexFunc(kv, func(c config) bool { return c.key == key })
	if i < 0 {
		return "", false
	}
	return kv[i].value, true
}

// splitName splits the benchmark name Benchmark<Family>[/lib=<lib>][/<param>=<value>]... into its
// family, library and parameter. A name without lib= belongs to lockstep, which is the only
// library that runs BenchmarkSimSeeds. A name without a parameter gets noParam.
func splitName(name string) (family, lib, param string) {
	parts := strings.Split(name, "/")
	family = strings.TrimPrefix(parts[0], "Benchmark")
	lib = "lockstep"
	var params []string
	for _, part := range parts[1:] {
		if l, ok := strings.CutPrefix(part, "lib="); ok {
			lib = l
		} else {
			params = append(params, part)
		}
	}
	param = strings.Join(params, "/")
	if param == "" {
		param = noParam
	}
	return family, lib, param
}
