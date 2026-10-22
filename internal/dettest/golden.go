// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// goldenHeader is line 1 of the golden file (DET-160); goldenColumns is line 2.
const (
	goldenHeader  = "# lockstep golden v1"
	goldenColumns = "# scenario seed hash events"
)

// Entry is one golden entry.
type Entry struct {
	Scenario string
	Seed     uint64
	Hash     uint64
	Events   uint64
}

// compareEntries orders entries by scenario (bytewise), then seed (numeric) (DET-160).
func compareEntries(a, b Entry) int {
	return cmp.Or(strings.Compare(a.Scenario, b.Scenario), cmp.Compare(a.Seed, b.Seed))
}

// ParseGolden parses the golden file format of DET-160: the header line, then entries sorted by
// scenario and seed, one per scenario and seed; lines starting with "#" after line 1 are skipped.
// It returns the error for the first problem and no entries.
func ParseGolden(data []byte) ([]Entry, error) {
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if lines[0] != goldenHeader {
		return nil, errors.New(`hashes.txt: missing header "` + goldenHeader + `"`)
	}
	var entries []Entry
	for i, l := range lines[1:] {
		if strings.HasPrefix(l, "#") {
			continue
		}
		n := i + 2
		e, ok := parseEntry(l)
		if !ok {
			return nil, fmt.Errorf("hashes.txt:%d: malformed entry", n)
		}
		if len(entries) > 0 {
			switch c := compareEntries(entries[len(entries)-1], e); {
			case c == 0:
				return nil, fmt.Errorf("hashes.txt:%d: duplicate entry %s 0x%016x", n, e.Scenario, e.Seed)
			case c > 0:
				return nil, fmt.Errorf("hashes.txt:%d: entries out of order", n)
			}
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// parseEntry parses "<scenario> <seed> <hash> <events>".
func parseEntry(l string) (Entry, bool) {
	f := strings.Split(l, " ")
	if len(f) != 4 || f[0] == "" {
		return Entry{}, false
	}
	seed, ok1 := parseHex16(f[1])
	hash, ok2 := parseHex16(f[2])
	events, err := strconv.ParseUint(f[3], 10, 64)
	if !ok1 || !ok2 || err != nil {
		return Entry{}, false
	}
	return Entry{Scenario: f[0], Seed: seed, Hash: hash, Events: events}, true
}

// parseHex16 parses "0x" followed by exactly 16 hexadecimal digits.
func parseHex16(s string) (uint64, bool) {
	digits, ok := strings.CutPrefix(s, "0x")
	if !ok || len(digits) != 16 {
		return 0, false
	}
	v, err := strconv.ParseUint(digits, 16, 64)
	return v, err == nil
}

// FormatGolden returns the golden file for entries, sorted by (Scenario bytewise, Seed): the two
// header lines of DET-160, then one line per entry. It does not change entries.
func FormatGolden(entries []Entry) []byte {
	sorted := slices.Clone(entries)
	slices.SortStableFunc(sorted, compareEntries)
	var b strings.Builder
	b.WriteString(goldenHeader + "\n" + goldenColumns + "\n")
	for _, e := range sorted {
		fmt.Fprintf(&b, "%s 0x%016x 0x%016x %d\n", e.Scenario, e.Seed, e.Hash, e.Events)
	}
	return []byte(b.String())
}

// Result is one line of a faultline results file, reduced to what the golden suite reads.
type Result struct {
	Test   string // the t.Name() passed to faultline.Run
	Seed   uint64
	Status string // "pass" or "fail"
	Events uint64
	Hash   uint64
}

// resultLine is the part of a results line that ParseResults reads. Events is a pointer so that
// an absent field differs from zero.
type resultLine struct {
	Version   int     `json:"faultline_result"`
	Test      string  `json:"test"`
	Seed      string  `json:"seed"`
	Status    string  `json:"status"`
	Events    *uint64 `json:"events"`
	TraceHash string  `json:"trace_hash"`
}

// ParseResults parses a faultline v0.1.2 results file (DET-170): one JSON object per non-empty
// line. Results keep file order. Errors name the 1-based line of the file.
func ParseResults(data []byte) ([]Result, error) {
	var results []Result
	for i, l := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(l) == "" {
			continue
		}
		r, err := parseResult(l)
		if err != nil {
			return nil, fmt.Errorf("dettest: results line %d: %w", i+1, err)
		}
		results = append(results, r)
	}
	return results, nil
}

// parseResult parses one results line; its errors have no "dettest: results line" prefix.
func parseResult(l string) (Result, error) {
	var line resultLine
	if err := json.Unmarshal([]byte(l), &line); err != nil {
		return Result{}, err
	}
	if line.Version != 1 {
		return Result{}, fmt.Errorf("faultline_result %d, want 1", line.Version)
	}
	r := Result{Test: line.Test, Status: line.Status}
	if line.Status == "pass" {
		switch {
		case line.Seed == "":
			return Result{}, errors.New("missing seed")
		case line.Events == nil:
			return Result{}, errors.New("missing events")
		case line.TraceHash == "":
			return Result{}, errors.New("missing trace_hash")
		}
	}
	if line.Events != nil {
		r.Events = *line.Events
	}
	var ok bool
	if line.Seed != "" {
		if r.Seed, ok = parseHex16(line.Seed); !ok {
			return Result{}, fmt.Errorf("invalid seed %q", line.Seed)
		}
	}
	if line.TraceHash != "" {
		if r.Hash, ok = parseHex16(line.TraceHash); !ok {
			return Result{}, fmt.Errorf("invalid trace_hash %q", line.TraceHash)
		}
	}
	return r, nil
}

// CompareGolden returns the report lines of DET-180, sorted by (scenario, seed), or nil if got
// equals want. An entry is identified by its scenario and seed; neither argument is changed.
func CompareGolden(got, want []Entry) []string {
	got, want = slices.Clone(got), slices.Clone(want)
	slices.SortStableFunc(got, compareEntries)
	slices.SortStableFunc(want, compareEntries)
	var lines []string
	for len(got) > 0 || len(want) > 0 {
		switch {
		case len(want) == 0 || (len(got) > 0 && compareEntries(got[0], want[0]) < 0):
			lines = append(lines, fmt.Sprintf("golden: %s seed 0x%016x: missing from testdata/hashes.txt", got[0].Scenario, got[0].Seed))
			got = got[1:]
		case len(got) == 0 || compareEntries(got[0], want[0]) > 0:
			lines = append(lines, fmt.Sprintf("golden: %s seed 0x%016x: in testdata/hashes.txt but not produced", want[0].Scenario, want[0].Seed))
			want = want[1:]
		default:
			g, w := got[0], want[0]
			if g.Hash != w.Hash || g.Events != w.Events {
				lines = append(lines, fmt.Sprintf("golden: %s seed 0x%016x: got hash 0x%016x events %d, want hash 0x%016x events %d",
					g.Scenario, g.Seed, g.Hash, g.Events, w.Hash, w.Events))
			}
			got, want = got[1:], want[1:]
		}
	}
	return lines
}
