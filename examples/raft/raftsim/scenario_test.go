// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/examples/raft/raft"
)

// plantedBug is one row of the RFT-540 table: a bug switch, the preset that finds it, the checks
// that may fail first, and the number of seeds within which some seed must fail.
type plantedBug struct {
	bug    raft.Bug
	faults Faults
	checks []string
	bound  int
}

// plantedBugs is the RFT-540 table.
var plantedBugs = []plantedBug{
	{raft.BugVoteIgnoresLog, FaultsPartition, []string{CheckLeaderCompleteness, CheckStateMachineSafety}, 200},
	{raft.BugStaleRead, FaultsPartition, []string{CheckLinearizable}, 300},
	{raft.BugVoteNotDurable, FaultsCrash, []string{CheckDurableState, CheckElectionSafety}, 100},
}

// TestScenario runs DefaultCluster() with the bug switch named by RAFTDEMO_BUG (its Bug.String(),
// such as vote-ignores-log), that switch's fault preset and Options(bound) (RFT-570). It skips
// when RAFTDEMO_BUG is unset. The planted-bug tests run it as a child process; run it yourself
// with FAULTLINE_SEED to replay one failing seed.
func TestScenario(t *testing.T) {
	name := os.Getenv("RAFTDEMO_BUG")
	if name == "" {
		t.Skip("set RAFTDEMO_BUG to a bug switch, such as vote-ignores-log, to run this scenario")
	}
	i := slices.IndexFunc(plantedBugs, func(p plantedBug) bool { return p.bug.String() == name })
	if i < 0 {
		names := make([]string, len(plantedBugs))
		for j, p := range plantedBugs {
			names[j] = p.bug.String()
		}
		t.Fatalf("RAFTDEMO_BUG=%q: want one of %s", name, strings.Join(names, ", "))
	}
	p := plantedBugs[i]
	c := DefaultCluster()
	c.Faults = p.faults
	c.Bugs = p.bug
	Test(t, Options(p.bound), c)
}

// seedResult is the part of a FAULTLINE_RESULTS line the tests read.
type seedResult struct {
	Seed      string `json:"seed"` // hex, as FAULTLINE_SEED accepts it
	Index     int    `json:"index"`
	Status    string `json:"status"`
	Kind      string `json:"kind"`
	Check     string `json:"check"`
	Message   string `json:"message"`
	TraceHash string `json:"trace_hash"`
}

// runScenario runs TestScenario for bug in a child process (RFT-570) with env added, and returns
// the child's results lines, its output and its exit code.
func runScenario(t *testing.T, bug raft.Bug, env ...string) (results []seedResult, output string, code int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "results.jsonl")
	env = append([]string{"RAFTDEMO_BUG=" + bug.String(), "FAULTLINE_RESULTS=" + path}, env...)
	output, code = runChild(t, "TestScenario", env...)
	return readResults(t, path), output, code
}

// readResults parses a FAULTLINE_RESULTS file. A child that stopped before its first seed wrote
// no file; the result is then nil.
func readResults(t *testing.T, path string) []seedResult {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // G304: the test's own temp file
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("results: %v", err)
	}
	defer func() { _ = f.Close() }()
	var out []seedResult
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r seedResult
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("results line %q: %v", sc.Text(), err)
		}
		out = append(out, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("results: %v", err)
	}
	return out
}

// firstFailure runs bug's scenario for its bound and returns the first failing seed's results
// line. It fails the test if no seed fails or the failing check is not one the bug may fail first
// (RFT-550).
func firstFailure(t *testing.T, p plantedBug) seedResult {
	t.Helper()
	rs, out, code := runScenario(t, p.bug, "FAULTLINE_SEEDS="+strconv.Itoa(p.bound))
	i := slices.IndexFunc(rs, func(r seedResult) bool { return r.Status == "fail" })
	if i < 0 {
		t.Fatalf("%s: no seed among %d of %d failed (exit %d)\n%s", p.bug, len(rs), p.bound, code, tail(out))
	}
	first := rs[i]
	if code == 0 {
		t.Fatalf("%s: the child exited 0 although seed %s failed\n%s", p.bug, first.Seed, tail(out))
	}
	if !slices.Contains(p.checks, first.Check) {
		t.Fatalf("%s: seed %s (index %d) failed %s %q: %s; want one of %q",
			p.bug, first.Seed, first.Index, first.Kind, first.Check, first.Message, p.checks)
	}
	return first
}

// tail returns the last 40 lines of s.
func tail(s string) string {
	lines := strings.Split(s, "\n")
	if len(lines) > 40 {
		lines = lines[len(lines)-40:]
	}
	return strings.Join(lines, "\n")
}

// AT-RFT-46: every bug switch fails a seed within its bound, and the first failing check is one
// the switch may fail first.
func TestATRFT46BugsDetected(t *testing.T) {
	skipHeavy(t)
	for _, p := range plantedBugs {
		t.Run(p.bug.String(), func(t *testing.T) {
			first := firstFailure(t, p)
			t.Logf("%s: first failing seed is index %d (%s) of %d: %s: %s",
				p.bug, first.Index, first.Seed, p.bound, first.Check, first.Message)
		})
	}
}

// AT-RFT-48 (RFT-580): the launch demo. It finds the first seed on which the planted
// vote-ignores-log bug breaks the cluster, replays that seed alone, and checks that the replay
// fails the same way, with the same trace hash.
func TestDemoPartitionBug(t *testing.T) {
	skipHeavy(t)
	i := slices.IndexFunc(plantedBugs, func(p plantedBug) bool { return p.bug == raft.BugVoteIgnoresLog })
	if i < 0 {
		t.Fatal("vote-ignores-log has no row in plantedBugs")
	}
	p := plantedBugs[i]
	first := firstFailure(t, p)
	t.Logf("Planted bug vote-ignores-log: a server grants its vote without checking that the candidate's log is up to date.")
	t.Logf("The cluster runs %d servers and %d clients under random network partitions, for up to %d seeds of %v simulated time each.",
		DefaultCluster().Servers, DefaultCluster().Clients, p.bound, Options(p.bound).Duration)
	t.Logf("Seed %s (index %d of the first %d) is the first to fail.", first.Seed, first.Index, p.bound)
	t.Logf("It fails the %q check: %s", first.Check, first.Message)

	rs, out, code := runScenario(t, p.bug, "FAULTLINE_SEED="+first.Seed)
	if len(rs) != 1 || code == 0 {
		t.Fatalf("replaying seed %s: %d results lines and exit %d, want 1 line and a failure\n%s", first.Seed, len(rs), code, tail(out))
	}
	again := rs[0]
	if again.Status != "fail" || again.Check != first.Check || again.Message != first.Message || again.TraceHash != first.TraceHash {
		t.Fatalf("replaying seed %s gave %s %q %q (trace hash %s), want fail %q %q (trace hash %s)",
			first.Seed, again.Status, again.Check, again.Message, again.TraceHash, first.Check, first.Message, first.TraceHash)
	}
	t.Logf("Replaying seed %s alone fails the same check with the same message and trace hash %s.", first.Seed, first.TraceHash)
	t.Logf("To watch the failure, run this in examples/raft:")
	t.Logf("RAFTDEMO_BUG=vote-ignores-log FAULTLINE_SEED=%s go test -v -run '^TestScenario$' ./raftsim", first.Seed)
}
