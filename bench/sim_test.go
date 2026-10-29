// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bench

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/bench/harness"
	"github.com/hmdsefi/lockstep/sim"
)

const (
	// seedsOutEnv names the file TestSimSeeds appends its result lines to.
	seedsOutEnv = "LOCKSTEP_BENCH_OUT"

	// simSeeds is the number of seeds each subtest of TestSimSeeds runs, from base seed 1.
	simSeeds = 50
)

// runChanging lists the variables that change the work done per seed (BEN-100). TestSimSeeds
// appends nothing while any of them is set, since seeds per second would no longer compare
// identical work.
var runChanging = []string{
	"FAULTLINE_SEED",
	"FAULTLINE_SEEDS",
	"FAULTLINE_SEED_LIST",
	"FAULTLINE_BASE_SEED",
	"FAULTLINE_EXPLORE",
	"FAULTLINE_TRACE",
	"FAULTLINE_CHECK_DETERMINISM",
	"LOCKSTEP_CHECK_DETERMINISM",
}

// seedsResult matches the result line of one TestSimSeeds subtest (BEN-100).
var seedsResult = regexp.MustCompile(`^BenchmarkSimSeeds/workload=(pingpong|crash3) 50 ([0-9]+) ns/op ([0-9]+\.[0-9]{2}) seeds/s$`)

// TestSimSeeds measures simulation seeds per second (BEN-100). Each subtest runs one world for 50
// seeds and times the sim.Simulate call. If LOCKSTEP_BENCH_OUT names a file, and no variable of
// runChanging is set, it appends the time per seed and the seeds per second to the file in Go
// benchmark format.
//
// The actors follow lockstep's actor rules and BaseSeed is 1, so every run executes the same 50
// seeds and seeds per second compares identical work. The wall clock is read around the call to
// sim.Simulate, never inside the simulation.
func TestSimSeeds(t *testing.T) {
	for _, w := range []struct {
		name     string
		duration time.Duration // the virtual length of each run
		nodes    []string
		crashes  bool
	}{
		{"pingpong", time.Second, []string{"n1"}, false},
		{"crash3", 10 * time.Second, []string{"n1", "n2", "n3"}, true},
	} {
		t.Run("workload="+w.name, func(t *testing.T) {
			opts := sim.Options{Options: faultline.Options{Seeds: simSeeds, BaseSeed: 1, Duration: w.duration}}
			out := seedsOutPath()
			start := time.Now()
			sim.Simulate(t, opts, pairsWorld(w.nodes, w.crashes))
			elapsed := time.Since(start)
			if out != "" && !t.Failed() {
				appendSeedsResult(t, out, w.name, elapsed)
			}
		})
	}
}

// TestSeedsOutPath checks BEN-100's gate: the result file is the one LOCKSTEP_BENCH_OUT names,
// unless a variable that changes the work per seed is set. Like faultline, it takes a variable
// whose value is empty after trimming as not set.
func TestSeedsOutPath(t *testing.T) {
	for _, name := range runChanging {
		t.Setenv(name, "")
	}
	t.Setenv(seedsOutEnv, "")
	if got := seedsOutPath(); got != "" {
		t.Errorf("seedsOutPath() = %q with %s empty, want none", got, seedsOutEnv)
	}
	t.Setenv(seedsOutEnv, "sim.txt")
	if got := seedsOutPath(); got != "sim.txt" {
		t.Errorf("seedsOutPath() = %q, want sim.txt", got)
	}
	for _, name := range runChanging {
		t.Setenv(name, " \t")
		if got := seedsOutPath(); got != "sim.txt" {
			t.Errorf("seedsOutPath() = %q with %s blank, want sim.txt", got, name)
		}
		t.Setenv(name, "1")
		if got := seedsOutPath(); got != "" {
			t.Errorf("seedsOutPath() = %q with %s=1, want none", got, name)
		}
		t.Setenv(name, "")
	}
}

// seedsOutPath returns the file TestSimSeeds appends to, or "" when it appends nothing: no
// LOCKSTEP_BENCH_OUT, or a variable of runChanging is set.
func seedsOutPath() string {
	for _, name := range runChanging {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			return ""
		}
	}
	return os.Getenv(seedsOutEnv)
}

// appendSeedsResult appends the block of one finished subtest to the file out, creating it if it
// does not exist: the configuration lines and the result line for workload name, which ran
// simSeeds seeds in elapsed.
func appendSeedsResult(t *testing.T, out, name string, elapsed time.Duration) {
	t.Helper()
	perSeed := float64(elapsed.Nanoseconds()) / simSeeds
	rate := simSeeds / elapsed.Seconds()
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(f, "goos: %s\ngoarch: %s\npkg: github.com/hmdsefi/lockstep/bench\n"+
		"BenchmarkSimSeeds/workload=%s %d %.0f ns/op %.2f seeds/s\n",
		runtime.GOOS, runtime.GOARCH, name, simSeeds, perSeed, rate)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatal(err)
	}
}

// pairsWorld returns the body of one subtest: one node for each of the names, each running a
// ping-pong pair, and, with crashes, a plan that crashes at most one node at a time, about once a
// second and for at most half a second. The body runs the world to the end of the run.
func pairsWorld(nodes []string, crashes bool) func(s *sim.Sim) {
	return func(s *sim.Sim) {
		w := s.World()
		for _, name := range nodes {
			s.Node(name, func(sys *lockstep.System) { spawnPair(sys, name) })
		}
		if crashes {
			w.Plan(&fault.Random{MaxDown: 1, Rules: []fault.Rule{
				{Kind: fault.KindCrash, Every: time.Second, MaxFor: 500 * time.Millisecond},
			}})
		}
		w.RunFor(w.End().Sub(w.Sim.Now()))

		// A world that fails or drops its messages would time no work.
		if c := s.Counts(); c.Deliveries == 0 || c.Failures != 0 || c.DeadLetters != 0 {
			w.T().Errorf("%d deliveries, %d failures and %d dead letters, want deliveries and no failures or dead letters",
				c.Deliveries, c.Failures, c.DeadLetters)
		}
	}
}

// spawnPair spawns the ping-pong pair on sys, the system of the node named node: actor a sends b
// a Ping every millisecond and b answers each with a Pong.
func spawnPair(sys *lockstep.System, node string) {
	a := lockstep.PIDAt[any](node, "/user/a")
	b := lockstep.PIDAt[harness.Ping](node, "/user/b")
	if _, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[any] { return &pinger{peer: b} }); err != nil {
		panic(err)
	}
	if _, err := lockstep.Spawn(sys, "b", func() lockstep.Actor[harness.Ping] { return &ponger{peer: a} }); err != nil {
		panic(err)
	}
}

// tick is the timer message of pinger.
type tick struct{}

// pinger is actor a of a pair. It receives its own tick and the Pong that answers each Ping.
type pinger struct {
	peer lockstep.PID[harness.Ping]
	seq  int64
}

func (p *pinger) Started(ctx *lockstep.Context[any]) { ctx.Every(time.Millisecond, tick{}) }

func (p *pinger) Receive(ctx *lockstep.Context[any], msg any) {
	switch msg.(type) {
	case tick:
		p.seq++
		if err := lockstep.Send(ctx, p.peer, harness.Ping{Seq: p.seq}); err != nil {
			ctx.Fail(err)
		}
	case harness.Pong:
		// The answer needs no work: receiving it is the turn.
	}
}

// ponger is actor b of a pair. It answers every Ping with a Pong.
type ponger struct{ peer lockstep.PID[any] }

func (p *ponger) Receive(ctx *lockstep.Context[harness.Ping], msg harness.Ping) {
	// A Pong has the fields of a Ping, and carries its Seq back.
	if err := lockstep.Send(ctx, p.peer, any(harness.Pong(msg))); err != nil {
		ctx.Fail(err)
	}
}

// AT-BEN-07, BEN-100: with LOCKSTEP_BENCH_OUT set, go test -run '^TestSimSeeds$' -count 2 appends
// four result blocks, the pingpong and crash3 lines twice each, each after the goos, goarch and
// pkg lines. With FAULTLINE_CHECK_DETERMINISM=1 or LOCKSTEP_CHECK_DETERMINISM=1 also set, the test
// passes and appends nothing.
func TestATBEN07SeedsOut(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test in a child process")
	}

	t.Run("appends", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "sim.txt") // absent: the test creates it
		goTestSimSeeds(t, out)
		checkSeedBlocks(t, readFile(t, out))
	})

	// The file already holds a line, so "appends nothing" means the line is all it holds.
	const existing = "BenchmarkExisting 1 1 ns/op\n"
	for _, env := range []string{"FAULTLINE_CHECK_DETERMINISM=1", "LOCKSTEP_CHECK_DETERMINISM=1"} {
		t.Run(env, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "sim.txt")
			if err := os.WriteFile(out, []byte(existing), 0o600); err != nil {
				t.Fatal(err)
			}
			goTestSimSeeds(t, out, env)
			if got := readFile(t, out); got != existing {
				t.Errorf("%s: file is %q after the run, want %q", env, got, existing)
			}
		})
	}
}

// goTestSimSeeds runs TestSimSeeds twice in a child go test with LOCKSTEP_BENCH_OUT=out and the
// variables extra, which are NAME=value pairs, and fails t unless the child passes and runs the
// test. The child gets the parent's environment without LOCKSTEP_BENCH_OUT and without the
// variables of runChanging, so a variable set around the parent cannot change what a case measures.
func goTestSimSeeds(t *testing.T, out string, extra ...string) {
	t.Helper()
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		name, _, _ := strings.Cut(kv, "=")
		return name == seedsOutEnv || slices.Contains(runChanging, name)
	})
	cmd := exec.Command("go", "test", "-run", "^TestSimSeeds$", "-count", "2", ".")
	cmd.Env = slices.Concat(env, []string{seedsOutEnv + "=" + out}, extra)
	got, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go test -run '^TestSimSeeds$' -count 2 . (%v): %v\n%s", extra, err, got)
	}
	if bytes.Contains(got, []byte("no tests to run")) {
		t.Fatalf("go test -run '^TestSimSeeds$' -count 2 . ran no test:\n%s", got)
	}
}

// readFile returns the content of the file name, or "" if it does not exist.
func readFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(b)
}

// checkSeedBlocks fails t unless content is four blocks of three configuration lines and one
// result line, in the order of two runs of TestSimSeeds: pingpong and crash3, twice.
func checkSeedBlocks(t *testing.T, content string) {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(content, "\n"), "\n")
	if len(lines) != 16 {
		t.Fatalf("file has %d lines, want 16 (four blocks of four lines):\n%s", len(lines), content)
	}
	config := []string{"goos: " + runtime.GOOS, "goarch: " + runtime.GOARCH, "pkg: github.com/hmdsefi/lockstep/bench"}
	for i, want := range []string{"pingpong", "crash3", "pingpong", "crash3"} {
		block := lines[4*i : 4*i+4]
		if got := block[:3]; !slices.Equal(got, config) {
			t.Errorf("block %d: configuration lines %q, want %q", i, got, config)
		}
		m := seedsResult.FindStringSubmatch(block[3])
		if m == nil {
			t.Errorf("block %d: result line %q does not match %v", i, block[3], seedsResult)
			continue
		}
		if m[1] != want {
			t.Errorf("block %d: workload %s, want %s", i, m[1], want)
		}
		// ns/op and seeds/s come from one measurement, so ns/op * seeds/s is 1e9 up to rounding.
		perSeed, _ := strconv.ParseFloat(m[2], 64)
		rate, _ := strconv.ParseFloat(m[3], 64)
		if perSeed <= 0 || rate <= 0 || math.Abs(perSeed*rate/1e9-1) > 0.01 {
			t.Errorf("block %d: %s ns/op and %s seeds/s are not one measurement", i, m[2], m[3])
		}
	}
}
