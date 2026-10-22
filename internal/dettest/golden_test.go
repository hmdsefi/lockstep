// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
)

const (
	goldenFile = "testdata/hashes.txt"

	// goldenSeeds is the number of seeds of every golden scenario (DET-260).
	goldenSeeds = 3

	// updateHint is the line DET-270 step 7 adds after a report.
	updateHint = `golden hashes changed: if intended, run "GOWORK=off go test ./internal/dettest -run TestGolden -update" and explain the change in a "Golden-Update:" commit trailer`

	// header is the file of DET-160 with no entries.
	header = goldenHeader + "\n" + goldenColumns + "\n"
)

// scenario is one golden scenario (DET-260): a name and a test body that calls faultline.Run with
// Seeds: 3.
type scenario struct {
	name string
	run  func(t *testing.T)
}

// scenarios is the list TestGolden runs. Plan 1.5 adds the scenarios of DET-260; until then the
// list is empty and the committed file holds the two header lines only (AT-DET-18).
var scenarios []scenario

// goldenSkipVars are the variables that make TestGolden skip, in the order DET-270 step 1 checks
// them.
var goldenSkipVars = []string{
	"FAULTLINE_SEED", "FAULTLINE_SEEDS", "FAULTLINE_BASE_SEED", "FAULTLINE_EXPLORE",
	"FAULTLINE_SEED_LIST", "FAULTLINE_SCHEDULE", "FAULTLINE_RESULTS",
}

// skipReason returns the skip text of DET-270 step 1 for the first variable of goldenSkipVars that
// getenv reports non-empty, or "".
func skipReason(getenv func(string) string) string {
	for _, v := range goldenSkipVars {
		if getenv(v) != "" {
			return fmt.Sprintf("golden: %s is set; golden hashes need the default seed list", v)
		}
	}
	return ""
}

// entriesFromResults is DET-270 step 5: every result belongs to a scenario of list as
// "<prefix>/<scenario>", passed, and carries a seed no earlier result of its scenario has, and
// every scenario has exactly goldenSeeds results. It returns one Entry per result.
func entriesFromResults(prefix string, list []scenario, results []Result) ([]Entry, error) {
	counts := make([]int, len(list))
	var entries []Entry
	for _, r := range results {
		i := slices.IndexFunc(list, func(s scenario) bool { return prefix+"/"+s.name == r.Test })
		dup := i >= 0 && slices.ContainsFunc(entries, func(e Entry) bool {
			return e.Scenario == list[i].name && e.Seed == r.Seed
		})
		if i < 0 || r.Status != "pass" || dup || counts[i] == goldenSeeds {
			return nil, fmt.Errorf("golden: unexpected results line for %s seed 0x%016x", r.Test, r.Seed)
		}
		counts[i]++
		entries = append(entries, Entry{Scenario: list[i].name, Seed: r.Seed, Hash: r.Hash, Events: r.Events})
	}
	for i, n := range counts {
		if n != goldenSeeds {
			return nil, fmt.Errorf("golden: scenario %s has %d results, want %d", list[i].name, n, goldenSeeds)
		}
	}
	return entries, nil
}

// gowork returns the path `go env GOWORK` prints, or "" if it prints nothing, "off", or fails.
func gowork() string {
	out, err := exec.Command("go", "env", "GOWORK").Output()
	if err != nil {
		return ""
	}
	if p := strings.TrimSpace(string(out)); p != "off" {
		return p
	}
	return ""
}

// goldenSuite is TestGolden (DET-270) for list, the golden file at path and -update set by
// rewrite. The results of scenario <name> are those of t.Name()+"/"+<name>.
func goldenSuite(t *testing.T, list []scenario, path string, rewrite bool) {
	t.Helper()
	if why := skipReason(os.Getenv); why != "" {
		t.Skip(why)
	}
	results := filepath.Join(t.TempDir(), "results.jsonl")
	t.Setenv("FAULTLINE_RESULTS", results)
	failed := false
	for _, sc := range list {
		if !t.Run(sc.name, sc.run) {
			failed = true
		}
	}
	if failed {
		t.Fatal("golden: a scenario failed; no hashes compared")
	}
	data, err := os.ReadFile(results)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	parsed, err := ParseResults(data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := entriesFromResults(t.Name(), list, parsed)
	if err != nil {
		t.Fatal(err)
	}
	if rewrite {
		if err := os.WriteFile(path, FormatGolden(got), 0o644); err != nil { //nolint:gosec // hashes.txt is a committed, world-readable file
			t.Fatal(err)
		}
		// The umask reduces WriteFile's mode, and WriteFile keeps an existing file's mode;
		// DET-270 step 6 wants 0644 either way.
		if err := os.Chmod(path, 0o644); err != nil { //nolint:gosec // path is the golden file or a test's temporary file
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(path) //nolint:gosec // path is the golden file or a test's temporary file
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	want, err := ParseGolden(committed)
	if err != nil {
		t.Fatal(err)
	}
	lines := CompareGolden(got, want)
	if lines == nil {
		return
	}
	lines = append(lines, updateHint)
	if p := gowork(); p != "" {
		lines = append(lines, fmt.Sprintf("golden: go.work %s is in use; hashes are pinned against go.mod's faultline, run with GOWORK=off", p))
	}
	t.Error(strings.Join(lines, "\n"))
}

// TestGolden is DET-270 over scenarios. -update rewrites testdata/hashes.txt.
func TestGolden(t *testing.T) { goldenSuite(t, scenarios, goldenFile, *update) }

// AT-DET-14, DET-160
func TestFormatGolden(t *testing.T) {
	in := []Entry{{"fanin", 2, 0xa, 5}, {"ask", 9, 0xb, 7}, {"ask", 3, 0xc, 1}}
	const want = header +
		"ask 0x0000000000000003 0x000000000000000c 1\n" +
		"ask 0x0000000000000009 0x000000000000000b 7\n" +
		"fanin 0x0000000000000002 0x000000000000000a 5\n"
	before := slices.Clone(in)
	if got := string(FormatGolden(in)); got != want {
		t.Errorf("FormatGolden:\n%s\nwant:\n%s", got, want)
	}
	if !slices.Equal(in, before) {
		t.Errorf("FormatGolden reordered its argument: %v", in)
	}
	for _, empty := range [][]Entry{nil, {}} {
		if got := string(FormatGolden(empty)); got != header {
			t.Errorf("FormatGolden(%v) = %q, want the two header lines", empty, got)
		}
	}
	// Bytewise: "B" < "a" < "a-b" < "ab"; numeric seeds: 0x9 < 0x10.
	order := []Entry{{"ab", 1, 0, 0}, {"a", 0x10, 0, 0}, {"B", 1, 0, 0}, {"a-b", 1, 0, 0}, {"a", 9, 0, 0}}
	var names []string
	for _, line := range strings.Split(strings.TrimSuffix(string(FormatGolden(order)), "\n"), "\n")[2:] {
		f := strings.Fields(line)
		names = append(names, f[0]+" "+f[1][len(f[1])-2:])
	}
	if want := []string{"B 01", "a 09", "a 10", "a-b 01", "ab 01"}; !slices.Equal(names, want) {
		t.Errorf("order %q, want %q", names, want)
	}
	// Max values keep their full width.
	extreme := string(FormatGolden([]Entry{{"x", ^uint64(0), ^uint64(0), ^uint64(0)}}))
	if want := header + "x 0xffffffffffffffff 0xffffffffffffffff 18446744073709551615\n"; extreme != want {
		t.Errorf("max entry: %q, want %q", extreme, want)
	}
}

// AT-DET-14, DET-160
func TestParseGolden(t *testing.T) {
	const three = header +
		"ask 0x0000000000000003 0x000000000000000c 1\n" +
		"ask 0x0000000000000009 0x000000000000000b 7\n" +
		"fanin 0x0000000000000002 0x000000000000000a 5\n"
	want := []Entry{{"ask", 3, 0xc, 1}, {"ask", 9, 0xb, 7}, {"fanin", 2, 0xa, 5}}
	got, err := ParseGolden([]byte(three))
	if err != nil || !slices.Equal(got, want) {
		t.Errorf("ParseGolden(three entries) = %v, %v; want %v", got, err, want)
	}
	// A "# note" line between entries is skipped, wherever it is, and the second header line is
	// only a comment.
	noted := header + "ask 0x0000000000000003 0x000000000000000c 1\n# note\nfanin 0x0000000000000002 0x000000000000000a 5\n"
	if got, err := ParseGolden([]byte(noted)); err != nil || !slices.Equal(got, []Entry{want[0], want[2]}) {
		t.Errorf("ParseGolden(with note) = %v, %v", got, err)
	}
	// The header alone, with or without the second line or the final newline, holds no entries.
	for _, s := range []string{header, "# lockstep golden v1\n", "# lockstep golden v1"} {
		if got, err := ParseGolden([]byte(s)); err != nil || len(got) != 0 {
			t.Errorf("ParseGolden(%q) = %v, %v; want no entries", s, got, err)
		}
	}
	// Round trip, including the extremes.
	all := []Entry{{"a", 0, 0, 0}, {"a", ^uint64(0), ^uint64(0), ^uint64(0)}, {"b", 1, 2, 3}}
	if got, err := ParseGolden(FormatGolden(all)); err != nil || !slices.Equal(got, all) {
		t.Errorf("round trip = %v, %v; want %v", got, err, all)
	}

	const missing = `hashes.txt: missing header "# lockstep golden v1"`
	const a3 = "ask 0x0000000000000003 0x000000000000000c 1\n"
	errs := []struct{ name, data, want string }{
		{"empty file", "", missing},
		{"newline only", "\n", missing},
		{"other header", "# lockstep golden v2\n", missing},
		{"longer header", "# lockstep golden v10\n", missing},
		{"faultline header", "# faultline golden v1\n", missing},
		{"header on line 2", "\n# lockstep golden v1\n", missing},
		{"three fields", header + "ask 0x1 0x2\n", "hashes.txt:3: malformed entry"},
		{"five fields", header + a3[:len(a3)-1] + " 2\n", "hashes.txt:3: malformed entry"},
		{"short seed", header + "ask 0x3 0x000000000000000c 1\n", "hashes.txt:3: malformed entry"},
		{"seed without 0x", header + "ask 000000000000000003 0x000000000000000c 1\n", "hashes.txt:3: malformed entry"},
		{"seed not hex", header + "ask 0x000000000000000g 0x000000000000000c 1\n", "hashes.txt:3: malformed entry"},
		{"hash not hex", header + "ask 0x0000000000000003 0x000000000000000z 1\n", "hashes.txt:3: malformed entry"},
		{"hash too long", header + "ask 0x0000000000000003 0x0000000000000000c 1\n", "hashes.txt:3: malformed entry"},
		{"events hex", header + "ask 0x0000000000000003 0x000000000000000c 0x1\n", "hashes.txt:3: malformed entry"},
		{"events negative", header + "ask 0x0000000000000003 0x000000000000000c -1\n", "hashes.txt:3: malformed entry"},
		{"events overflow", header + "ask 0x0000000000000003 0x000000000000000c 18446744073709551616\n", "hashes.txt:3: malformed entry"},
		{"double space", header + "ask  0x0000000000000003 0x000000000000000c 1\n", "hashes.txt:3: malformed entry"},
		{"empty scenario", header + " 0x0000000000000003 0x000000000000000c 1\n", "hashes.txt:3: malformed entry"},
		{"blank line", header + a3 + "\n", "hashes.txt:4: malformed entry"},
		{"malformed after note", header + "# note\nask\n", "hashes.txt:4: malformed entry"},
		{"duplicate", header + a3 + a3, "hashes.txt:4: duplicate entry ask 0x0000000000000003"},
		{"duplicate with other values", header + a3 + "ask 0x0000000000000003 0x0000000000000001 9\n", "hashes.txt:4: duplicate entry ask 0x0000000000000003"},
		{"scenarios out of order", header + "fanin 0x0000000000000002 0x000000000000000a 5\n" + a3, "hashes.txt:4: entries out of order"},
		{"seeds out of order", header + "ask 0x0000000000000009 0x000000000000000b 7\n" + a3, "hashes.txt:4: entries out of order"},
		{"seeds out of order numerically", header + "ask 0x0000000000000010 0x0000000000000001 1\nask 0x000000000000000f 0x0000000000000001 1\n", "hashes.txt:4: entries out of order"},
		{"first problem wins", header + "ask 0x1 0x2\nask\n", "hashes.txt:3: malformed entry"},
		{"out of order before later malformed", header + "b 0x0000000000000001 0x0000000000000001 1\na 0x0000000000000001 0x0000000000000001 1\nx\n", "hashes.txt:4: entries out of order"},
	}
	for _, tc := range errs {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseGolden([]byte(tc.data))
			if err == nil || err.Error() != tc.want {
				t.Errorf("ParseGolden error = %v, want %q", err, tc.want)
			}
			if got != nil {
				t.Errorf("ParseGolden entries on error = %v, want nil", got)
			}
		})
	}
}

const (
	okLine  = `{"faultline_result":1,"package":"example.com/kv","test":"TestKV","seed":"0x00000000000000ff","index":1,"status":"pass","events":12,"trace_hash":"0x00000000000000aa","wall_ns":24765084}`
	badLine = `{"faultline_result":1,"package":"example.com/kv","test":"TestKV","seed":"","index":-1,"status":"fail","kind":"setup","message":"faultline: Options.Duration is -1s","wall_ns":1}`
)

// AT-DET-15, DET-170
func TestParseResults(t *testing.T) {
	got, err := ParseResults([]byte(okLine + "\n" + badLine + "\n"))
	want := []Result{{"TestKV", 0xff, "pass", 12, 0xaa}, {"TestKV", 0, "fail", 0, 0}}
	if err != nil || !slices.Equal(got, want) {
		t.Errorf("ParseResults = %v, %v; want %v", got, err, want)
	}
	// No data, blank lines, a missing final newline; results keep file order.
	for _, s := range []string{"", "\n", "\n\n"} {
		if got, err := ParseResults([]byte(s)); err != nil || len(got) != 0 {
			t.Errorf("ParseResults(%q) = %v, %v; want no results", s, got, err)
		}
	}
	got, err = ParseResults([]byte("\n" + badLine + "\n\n" + okLine))
	if err != nil || !slices.Equal(got, []Result{want[1], want[0]}) {
		t.Errorf("ParseResults(blank lines, no final newline) = %v, %v", got, err)
	}
	// A pass line may report zero events; a fail line may carry a seed, events and a hash.
	zero := `{"faultline_result":1,"test":"T","seed":"0x0000000000000001","status":"pass","events":0,"trace_hash":"0x0000000000000000"}`
	if got, err := ParseResults([]byte(zero)); err != nil || !slices.Equal(got, []Result{{"T", 1, "pass", 0, 0}}) {
		t.Errorf("ParseResults(zero events) = %v, %v", got, err)
	}
	failing := `{"faultline_result":1,"test":"T","seed":"0x00000000000000ff","status":"fail","kind":"invariant","events":7,"trace_hash":"0x00000000000000bb"}`
	if got, err := ParseResults([]byte(failing)); err != nil || !slices.Equal(got, []Result{{"T", 0xff, "fail", 7, 0xbb}}) {
		t.Errorf("ParseResults(fail with fields) = %v, %v", got, err)
	}

	cut := func(s, sub string) string {
		if !strings.Contains(s, sub) {
			t.Fatalf("%q does not contain %q", s, sub)
		}
		return strings.Replace(s, sub, "", 1)
	}
	errs := []struct{ name, data, want string }{
		{"missing trace_hash", cut(okLine, `,"trace_hash":"0x00000000000000aa"`), "dettest: results line 1: missing trace_hash"},
		{"empty trace_hash", strings.Replace(okLine, `"trace_hash":"0x00000000000000aa"`, `"trace_hash":""`, 1), "dettest: results line 1: missing trace_hash"},
		{"missing seed", cut(okLine, `"seed":"0x00000000000000ff",`), "dettest: results line 1: missing seed"},
		{"empty seed", strings.Replace(okLine, `"seed":"0x00000000000000ff"`, `"seed":""`, 1), "dettest: results line 1: missing seed"},
		{"missing events", cut(okLine, `"events":12,`), "dettest: results line 1: missing events"},
		{"missing first of several", cut(cut(okLine, `"events":12,`), `,"trace_hash":"0x00000000000000aa"`), "dettest: results line 1: missing events"},
		{"result 2", strings.Replace(okLine, `"faultline_result":1`, `"faultline_result":2`, 1), "dettest: results line 1: faultline_result 2, want 1"},
		{"result absent", cut(okLine, `"faultline_result":1,`), "dettest: results line 1: faultline_result 0, want 1"},
		{"on the later line", okLine + "\n\n" + strings.Replace(okLine, `"faultline_result":1`, `"faultline_result":3`, 1), "dettest: results line 3: faultline_result 3, want 1"},
		{"missing field on line 2", okLine + "\n" + cut(okLine, `"events":12,`), "dettest: results line 2: missing events"},
		{"bad seed", strings.Replace(okLine, "0x00000000000000ff", "255", 1), `dettest: results line 1: invalid seed "255"`},
		{"bad hash", strings.Replace(okLine, "0x00000000000000aa", "0xaa", 1), `dettest: results line 1: invalid trace_hash "0xaa"`},
		{"bad seed on fail line", strings.Replace(badLine, `"seed":""`, `"seed":"x"`, 1), `dettest: results line 1: invalid seed "x"`},
	}
	for _, tc := range errs {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseResults([]byte(tc.data))
			if err == nil || err.Error() != tc.want {
				t.Errorf("ParseResults error = %v, want %q", err, tc.want)
			}
			if got != nil {
				t.Errorf("ParseResults results on error = %v, want nil", got)
			}
		})
	}
	for _, s := range []string{"{", okLine + "\n{", "[]", `{"faultline_result":"1"}`, okLine + " trailing"} {
		_, err := ParseResults([]byte(s))
		line := 1 + strings.Count(s, "\n")
		if prefix := fmt.Sprintf("dettest: results line %d: ", line); err == nil || !strings.HasPrefix(err.Error(), prefix) || err.Error() == prefix {
			t.Errorf("ParseResults(%q) error = %v, want %q and a JSON error", s, err, prefix)
		}
	}
	if _, err := ParseResults([]byte("{")); err == nil || err.Error() != "dettest: results line 1: unexpected end of JSON input" {
		t.Errorf("ParseResults(%q) error = %v", "{", err)
	}
}

// AT-DET-15: a real faultline.Run with two seeds writes a file ParseResults reads as two passes.
func TestParseResultsFaultline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.jsonl")
	t.Setenv("FAULTLINE_RESULTS", path)
	for _, v := range goldenSkipVars[:6] {
		t.Setenv(v, "")
	}
	faultline.Run(t, faultline.Options{Seeds: 2, BaseSeed: 7, Duration: time.Second}, func(w *faultline.World) {
		w.Logf("run %d", w.Seed())
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseResults(data)
	if err != nil {
		t.Fatalf("ParseResults(%q): %v", data, err)
	}
	if len(got) != 2 {
		t.Fatalf("ParseResults(%q) = %v, want 2 results", data, got)
	}
	for i, r := range got {
		if r.Test != t.Name() || r.Status != "pass" || r.Hash == 0 || r.Seed == 0 {
			t.Errorf("result %d = %+v, want a pass of %s with a seed and a hash", i, r, t.Name())
		}
	}
	if got[0].Seed == got[1].Seed {
		t.Errorf("both results have seed 0x%016x", got[0].Seed)
	}
}

// AT-DET-16, DET-180
func TestCompareGolden(t *testing.T) {
	got := []Entry{{"a", 1, 0x1, 5}, {"b", 2, 0x2, 6}}
	want := []Entry{{"a", 1, 0x9, 5}, {"c", 3, 0x3, 7}}
	wantLines := []string{
		"golden: a seed 0x0000000000000001: got hash 0x0000000000000001 events 5, want hash 0x0000000000000009 events 5",
		"golden: b seed 0x0000000000000002: missing from testdata/hashes.txt",
		"golden: c seed 0x0000000000000003: in testdata/hashes.txt but not produced",
	}
	if lines := CompareGolden(got, want); !slices.Equal(lines, wantLines) {
		t.Errorf("CompareGolden:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(wantLines, "\n"))
	}
	for _, tc := range []struct {
		name      string
		got, want []Entry
	}{
		{"equal", want, want},
		{"equal, different order", []Entry{want[1], want[0]}, want},
		{"both nil", nil, nil},
		{"both empty", []Entry{}, nil},
	} {
		if lines := CompareGolden(tc.got, tc.want); lines != nil {
			t.Errorf("%s: CompareGolden = %q, want nil", tc.name, lines)
		}
	}
	// Sorted by (scenario, seed) over both lists; events alone make a difference; inputs are not
	// reordered.
	got = []Entry{{"b", 0x10, 1, 1}, {"d", 2, 9, 4}, {"a", 1, 2, 3}, {"b", 9, 1, 1}}
	want = []Entry{{"c", 7, 6, 2}, {"a", 1, 2, 3}, {"d", 2, 9, 5}, {"b", 9, 1, 1}}
	before := [2][]Entry{slices.Clone(got), slices.Clone(want)}
	wantLines = []string{
		"golden: b seed 0x0000000000000010: missing from testdata/hashes.txt",
		"golden: c seed 0x0000000000000007: in testdata/hashes.txt but not produced",
		"golden: d seed 0x0000000000000002: got hash 0x0000000000000009 events 4, want hash 0x0000000000000009 events 5",
	}
	if lines := CompareGolden(got, want); !slices.Equal(lines, wantLines) {
		t.Errorf("CompareGolden:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(wantLines, "\n"))
	}
	if !slices.Equal(got, before[0]) || !slices.Equal(want, before[1]) {
		t.Error("CompareGolden reordered its arguments")
	}
	// Against an empty file every produced entry is missing.
	if lines := CompareGolden([]Entry{{"a", 1, 1, 1}}, nil); len(lines) != 1 || !strings.HasSuffix(lines[0], ": missing from testdata/hashes.txt") {
		t.Errorf("CompareGolden(entries, nil) = %q", lines)
	}
}

// AT-DET-18: the committed file is exactly the header, and parses to no entries.
func TestGoldenFile(t *testing.T) {
	data, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != header {
		t.Errorf("%s = %q, want %q", goldenFile, data, header)
	}
	if es, err := ParseGolden(data); err != nil || len(es) != 0 {
		t.Errorf("ParseGolden(%s) = %v, %v", goldenFile, es, err)
	}
	if string(FormatGolden(nil)) != string(data) {
		t.Errorf("FormatGolden(nil) differs from %s", goldenFile)
	}
}

// DET-270 step 1
func TestSkipReason(t *testing.T) {
	env := func(kv ...string) func(string) string {
		return func(name string) string {
			for i := 0; i < len(kv); i += 2 {
				if kv[i] == name {
					return kv[i+1]
				}
			}
			return ""
		}
	}
	if got := skipReason(env()); got != "" {
		t.Errorf("no variables: %q", got)
	}
	if got := skipReason(env("FAULTLINE_SEED", "")); got != "" {
		t.Errorf("empty variable: %q", got)
	}
	for _, v := range goldenSkipVars {
		want := "golden: " + v + " is set; golden hashes need the default seed list"
		if got := skipReason(env(v, "x")); got != want {
			t.Errorf("%s: %q, want %q", v, got, want)
		}
	}
	if got, want := skipReason(env("FAULTLINE_RESULTS", "r", "FAULTLINE_SCHEDULE", "s", "FAULTLINE_SEEDS", "2")),
		"golden: FAULTLINE_SEEDS is set; golden hashes need the default seed list"; got != want {
		t.Errorf("order: %q, want %q", got, want)
	}
	if want := []string{"FAULTLINE_SEED", "FAULTLINE_SEEDS", "FAULTLINE_BASE_SEED", "FAULTLINE_EXPLORE", "FAULTLINE_SEED_LIST", "FAULTLINE_SCHEDULE", "FAULTLINE_RESULTS"}; !slices.Equal(goldenSkipVars, want) {
		t.Errorf("goldenSkipVars = %q", goldenSkipVars)
	}
}

// DET-270 step 5
func TestEntriesFromResults(t *testing.T) {
	list := []scenario{{name: "a"}, {name: "b"}}
	r := func(test string, seed uint64) Result { return Result{test, seed, "pass", seed * 10, seed * 100} }
	full := []Result{r("T/a", 1), r("T/b", 4), r("T/a", 2), r("T/a", 3), r("T/b", 5), r("T/b", 6)}
	want := []Entry{{"a", 1, 100, 10}, {"b", 4, 400, 40}, {"a", 2, 200, 20}, {"a", 3, 300, 30}, {"b", 5, 500, 50}, {"b", 6, 600, 60}}
	if got, err := entriesFromResults("T", list, full); err != nil || !slices.Equal(got, want) {
		t.Errorf("entriesFromResults = %v, %v; want %v", got, err, want)
	}
	if got, err := entriesFromResults("T", nil, nil); err != nil || len(got) != 0 {
		t.Errorf("empty list, no results = %v, %v", got, err)
	}
	failed := slices.Clone(full)
	failed[2].Status = "fail"
	skipped := slices.Clone(full)
	skipped[2].Status = "skip"
	errs := []struct {
		name    string
		results []Result
		want    string
	}{
		{"unlisted test", append(slices.Clone(full), r("T/c", 1)), "golden: unexpected results line for T/c seed 0x0000000000000001"},
		{"other test name", append(slices.Clone(full), r("TestKV", 8)), "golden: unexpected results line for TestKV seed 0x0000000000000008"},
		{"no prefix", []Result{r("a", 1)}, "golden: unexpected results line for a seed 0x0000000000000001"},
		{"failed result", failed, "golden: unexpected results line for T/a seed 0x0000000000000002"},
		{"skipped result", skipped, "golden: unexpected results line for T/a seed 0x0000000000000002"},
		{"duplicate seed", []Result{r("T/a", 1), r("T/a", 1)}, "golden: unexpected results line for T/a seed 0x0000000000000001"},
		{"fourth result", append(slices.Clone(full), r("T/a", 9)), "golden: unexpected results line for T/a seed 0x0000000000000009"},
		{"results with no scenarios", full, "golden: unexpected results line for T/a seed 0x0000000000000001"},
		{"too few", full[:5], "golden: scenario b has 2 results, want 3"},
		{"none", nil, "golden: scenario a has 0 results, want 3"},
	}
	for _, tc := range errs {
		t.Run(tc.name, func(t *testing.T) {
			l := list
			if tc.name == "results with no scenarios" {
				l = nil
			}
			got, err := entriesFromResults("T", l, tc.results)
			if err == nil || err.Error() != tc.want {
				t.Errorf("error = %v, want %q", err, tc.want)
			}
			if got != nil {
				t.Errorf("entries on error = %v", got)
			}
		})
	}
	if _, err := entriesFromResults("T", list, []Result{r("T/a", 1), r("U/a", 2)}); err == nil {
		t.Error("a result of another test accepted")
	}
}

// goldenHelperEnv names the case TestHelperGolden runs; the helper skips without it.
const goldenHelperEnv = "DETTEST_GOLDEN_CASE"

// helperScenario is a scenario of the helper: Seeds 3 on faultline.Run, the world logging
// "<note>" so that a different note changes the trace hash.
func helperScenario(name, note string) scenario {
	return scenario{name: name, run: func(t *testing.T) {
		faultline.Run(t, faultline.Options{Seeds: goldenSeeds, BaseSeed: 0x1001, Duration: time.Second}, func(w *faultline.World) {
			w.Logf("%s %d", note, w.Seed())
		})
	}}
}

// TestHelperGolden is the child of TestGoldenSuite: goldenSuite on the list and file of the case
// named by DETTEST_GOLDEN_CASE.
func TestHelperGolden(t *testing.T) {
	c := os.Getenv(goldenHelperEnv)
	if c == "" {
		t.Skip("child of TestGoldenSuite")
	}
	path := os.Getenv("DETTEST_GOLDEN_FILE")
	switch c {
	case "failing":
		bad := scenario{name: "bad", run: func(t *testing.T) { t.Fatal("scenario body failed") }}
		goldenSuite(t, []scenario{helperScenario("good", "x"), bad}, path, false)
	case "update":
		goldenSuite(t, []scenario{helperScenario("two", "x"), helperScenario("one", "x")}, path, true)
	case "check":
		goldenSuite(t, []scenario{helperScenario("two", "x"), helperScenario("one", "x")}, path, false)
	case "changed":
		goldenSuite(t, []scenario{helperScenario("two", "x"), helperScenario("one", "changed")}, path, false)
	case "wrong-seeds":
		bad := scenario{name: "short", run: func(t *testing.T) {
			faultline.Run(t, faultline.Options{Seeds: 2, Duration: time.Second}, func(w *faultline.World) { w.Logf("x") })
		}}
		goldenSuite(t, []scenario{bad}, path, false)
	default:
		t.Fatalf("unknown case %q", c)
	}
}

// goldenChild is the outcome of one run of the test binary on a test.
type goldenChild struct {
	out  string
	exit int
}

// runGoldenChild re-runs the test binary on the test named by run, with -test.v. The child's
// environment is the parent's without FAULTLINE_* and DETTEST_* variables, plus env.
func runGoldenChild(t *testing.T, run string, env ...string) goldenChild {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+run+"$", "-test.v") //nolint:gosec // the test binary itself
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); !strings.HasPrefix(name, "FAULTLINE_") && !strings.HasPrefix(name, "DETTEST_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	res := goldenChild{out: out.String()}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		res.exit = exitErr.ExitCode()
	default:
		t.Fatalf("running %s: %v", os.Args[0], err)
	}
	return res
}

// goldenCase runs TestHelperGolden on the case against the file at path.
func goldenCase(t *testing.T, c, path string, env ...string) goldenChild {
	t.Helper()
	return runGoldenChild(t, "TestHelperGolden", append([]string{goldenHelperEnv + "=" + c, "DETTEST_GOLDEN_FILE=" + path}, env...)...)
}

// DET-270 steps 3 to 7, with scenarios that run faultline.Run with 3 seeds.
func TestGoldenSuite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hashes.txt")

	// Step 4: a failing scenario stops the run before any comparison, even with no file.
	res := goldenCase(t, "failing", path)
	if res.exit == 0 || !strings.Contains(res.out, "golden: a scenario failed; no hashes compared") {
		t.Errorf("failing scenario: exit %d, output:\n%s", res.exit, res.out)
	}
	if strings.Contains(res.out, "hashes.txt") {
		t.Errorf("failing scenario compared hashes:\n%s", res.out)
	}

	// Step 5: a scenario with the wrong number of seeds.
	res = goldenCase(t, "wrong-seeds", path)
	if res.exit == 0 || !strings.Contains(res.out, "golden: scenario short has 2 results, want 3") {
		t.Errorf("wrong seed count: exit %d, output:\n%s", res.exit, res.out)
	}

	// Step 7 with no file: ParseGolden's missing-header error stops the run.
	res = goldenCase(t, "check", path)
	if res.exit == 0 || !strings.Contains(res.out, `hashes.txt: missing header "# lockstep golden v1"`) {
		t.Errorf("no file: exit %d, output:\n%s", res.exit, res.out)
	}

	// Step 6: -update writes the file, sorted by scenario and seed, with mode 0644.
	res = goldenCase(t, "update", path)
	if res.exit != 0 {
		t.Fatalf("update: exit %d, output:\n%s", res.exit, res.out)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := ParseGolden(data)
	if err != nil || len(entries) != 2*goldenSeeds || string(FormatGolden(entries)) != string(data) {
		t.Fatalf("written file %q: %v entries, %v", data, entries, err)
	}
	if entries[0].Scenario != "one" || entries[goldenSeeds].Scenario != "two" {
		t.Errorf("written file not sorted by scenario: %q", data)
	}
	for _, e := range entries {
		if e.Hash == 0 || e.Seed == 0 {
			t.Errorf("entry %+v lacks a seed or hash", e)
		}
	}
	checkMode := func(when string) {
		t.Helper()
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if perm := fi.Mode().Perm(); perm != 0o644 {
			t.Errorf("mode of the file written by %s: %v, want %v", when, perm, fs.FileMode(0o644))
		}
	}
	checkMode("-update")

	// Step 7: the same scenarios match, and -update changes nothing.
	if res = goldenCase(t, "check", path); res.exit != 0 {
		t.Errorf("check against the written file: exit %d, output:\n%s", res.exit, res.out)
	}
	// A second -update over an existing file with another mode restores 0644.
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if res = goldenCase(t, "update", path); res.exit != 0 {
		t.Fatalf("second update: exit %d, output:\n%s", res.exit, res.out)
	}
	if again, err := os.ReadFile(path); err != nil || !bytes.Equal(again, data) {
		t.Errorf("second update changed the file: %q, %v", again, err)
	}
	checkMode("-update over a file with mode 0600")

	// A changed scenario: three "got ... want" lines and the hint, no GOWORK line.
	res = goldenCase(t, "changed", path, "GOWORK=off")
	if res.exit == 0 {
		t.Fatalf("changed scenario passed:\n%s", res.out)
	}
	var got []string
	for line := range strings.SplitSeq(res.out, "\n") {
		if _, s, ok := strings.Cut(line, "golden: one seed 0x"); ok {
			got = append(got, "golden: one seed 0x"+s)
		}
	}
	if len(got) != goldenSeeds {
		t.Fatalf("report lines %q in output:\n%s", got, res.out)
	}
	for i, line := range got {
		w := entries[i]
		prefix := fmt.Sprintf("golden: one seed 0x%016x: got hash 0x", w.Seed)
		suffix := fmt.Sprintf(" events %d, want hash 0x%016x events %d", w.Events, w.Hash, w.Events)
		if !strings.HasPrefix(line, prefix) || !strings.Contains(line, suffix) {
			t.Errorf("line %q\nwant %s<16 hex digits>...%s", line, prefix, suffix)
		}
	}
	if strings.Contains(res.out, "golden: two seed") {
		t.Errorf("the unchanged scenario is reported:\n%s", res.out)
	}
	if !strings.Contains(res.out, updateHint) || strings.Contains(res.out, "go.work") {
		t.Errorf("hint missing or GOWORK line present with GOWORK=off:\n%s", res.out)
	}

	// Under a go.work the report ends with the GOWORK line.
	work := filepath.Join(t.TempDir(), "go.work")
	if err := os.WriteFile(work, []byte("go 1.26.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	res = goldenCase(t, "changed", path, "GOWORK="+work)
	line := fmt.Sprintf("golden: go.work %s is in use; hashes are pinned against go.mod's faultline, run with GOWORK=off", work)
	hintAt, lineAt := strings.Index(res.out, updateHint), strings.Index(res.out, line)
	if res.exit == 0 || hintAt < 0 || lineAt < hintAt {
		t.Errorf("GOWORK report: exit %d, want the hint then the GOWORK line:\n%s", res.exit, res.out)
	}

	// A pinned entry the run does not produce, and a produced entry that is not pinned.
	pinned := append(slices.Clone(entries[1:]), Entry{"gone", 1, 2, 3})
	if err := os.WriteFile(path, FormatGolden(pinned), 0o600); err != nil {
		t.Fatal(err)
	}
	res = goldenCase(t, "check", path, "GOWORK=off")
	missing := fmt.Sprintf("golden: one seed 0x%016x: missing from testdata/hashes.txt", entries[0].Seed)
	notProduced := "golden: gone seed 0x0000000000000001: in testdata/hashes.txt but not produced"
	if res.exit == 0 || !strings.Contains(res.out, missing) || !strings.Contains(res.out, notProduced) {
		t.Errorf("missing and unproduced entries: exit %d, output:\n%s", res.exit, res.out)
	}
}

// AT-DET-18: TestGolden with the header-only file and the empty list passes and compares nothing;
// with a variable of DET-270 step 1 set it skips with that variable's text, FAULTLINE_RESULTS
// included.
func TestGoldenSkips(t *testing.T) {
	res := runGoldenChild(t, "TestGolden")
	if res.exit != 0 || !strings.Contains(res.out, "--- PASS: TestGolden") {
		t.Errorf("default run: exit %d, output:\n%s", res.exit, res.out)
	}
	for _, c := range []struct{ env, name string }{
		{"FAULTLINE_SEED=0x1", "FAULTLINE_SEED"},
		{"FAULTLINE_SEEDS=2", "FAULTLINE_SEEDS"},
		{"FAULTLINE_RESULTS=" + filepath.Join(t.TempDir(), "r.jsonl"), "FAULTLINE_RESULTS"},
	} {
		res := runGoldenChild(t, "TestGolden", c.env)
		want := "golden: " + c.name + " is set; golden hashes need the default seed list"
		if res.exit != 0 || !strings.Contains(res.out, "--- SKIP: TestGolden") || !strings.Contains(res.out, want) {
			t.Errorf("%s: exit %d, want a skip with %q; output:\n%s", c.env, res.exit, want, res.out)
		}
	}
}
