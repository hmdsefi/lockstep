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

// simRunChanging reports whether the environment variable name changes the work done per seed
// (BEN-100): LOCKSTEP_CHECK_DETERMINISM, and every FAULTLINE_ variable but the two that only say
// where faultline writes. That includes FAULTLINE_SEED, FAULTLINE_SEEDS, FAULTLINE_SEED_LIST,
// FAULTLINE_BASE_SEED, FAULTLINE_EXPLORE, FAULTLINE_TRACE and FAULTLINE_CHECK_DETERMINISM, and
// also FAULTLINE_SCHEDULE, which replays a schedule. TestSimSeeds appends nothing while one is
// set, since seeds per second would no longer compare identical work.
func simRunChanging(name string) bool {
	switch name {
	case "LOCKSTEP_CHECK_DETERMINISM":
		return true
	case "FAULTLINE_ARTIFACTS", "FAULTLINE_RESULTS":
		return false
	}
	return strings.HasPrefix(name, "FAULTLINE_")
}

// seedsResult matches the result line of one TestSimSeeds subtest (BEN-100).
var seedsResult = regexp.MustCompile(`^BenchmarkSimSeeds/workload=(pingpong|crash3) 50 ([0-9]+) ns/op ([0-9]+\.[0-9]{2}) seeds/s$`)

// TestSimSeeds measures simulation seeds per second (BEN-100). Each subtest runs one world for 50
// seeds and times the sim.Simulate call. If LOCKSTEP_BENCH_OUT names a file, no variable that
// simRunChanging names is set and -short is not, it appends the time per seed and the seeds per
// second to the file in Go benchmark format.
//
// The actors follow lockstep's actor rules and BaseSeed is 1, so without those variables and
// -short every run executes the same 50 seeds and seeds per second compares identical work. Under
// -short faultline runs 5 seeds: the test still runs, as a quick check, and appends nothing. The
// wall clock is read around the call to sim.Simulate, never inside the simulation.
//
// The race detector makes the crash3 world much slower, and the numbers would not be comparable,
// so the test skips under -race; TestATBEN07SeedsOut runs it in a child process without.
func TestSimSeeds(t *testing.T) {
	if raceEnabled {
		t.Skip("seeds per second is measured without -race; TestATBEN07SeedsOut runs it")
	}
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
			out := seedsOutPath(testing.Short())
			start := time.Now()
			sim.Simulate(t, opts, simPairsWorld(w.nodes, w.crashes))
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
	for _, kv := range os.Environ() { // the test must not depend on its caller's variables
		if name, _, _ := strings.Cut(kv, "="); simRunChanging(name) {
			t.Setenv(name, "")
		}
	}
	t.Setenv(seedsOutEnv, "")
	if got := seedsOutPath(false); got != "" {
		t.Errorf("seedsOutPath(false) = %q with %s empty, want none", got, seedsOutEnv)
	}
	t.Setenv(seedsOutEnv, "sim.txt")
	if got := seedsOutPath(false); got != "sim.txt" {
		t.Errorf("seedsOutPath(false) = %q, want sim.txt", got)
	}
	if got := seedsOutPath(true); got != "" {
		t.Errorf("seedsOutPath(true) = %q, want none: under -short faultline runs 5 seeds", got)
	}

	changing := []string{
		"FAULTLINE_SEED", "FAULTLINE_SEEDS", "FAULTLINE_SEED_LIST", "FAULTLINE_BASE_SEED",
		"FAULTLINE_EXPLORE", "FAULTLINE_TRACE", "FAULTLINE_CHECK_DETERMINISM", "LOCKSTEP_CHECK_DETERMINISM",
		"FAULTLINE_SCHEDULE", "FAULTLINE_MINIMIZE", "FAULTLINE_SWARM", "FAULTLINE_SWARM_CONFIG",
		"FAULTLINE_EXACT", "FAULTLINE_FUTURE_SETTING",
	}
	for _, name := range changing {
		t.Setenv(name, " \t")
		if got := seedsOutPath(false); got != "sim.txt" {
			t.Errorf("seedsOutPath(false) = %q with %s blank, want sim.txt", got, name)
		}
		t.Setenv(name, "1")
		if got := seedsOutPath(false); got != "" {
			t.Errorf("seedsOutPath(false) = %q with %s=1, want none", got, name)
		}
		t.Setenv(name, "")
	}
	for _, name := range []string{"FAULTLINE_ARTIFACTS", "FAULTLINE_RESULTS", "LOCKSTEP_BENCH_LARGE"} {
		t.Setenv(name, "1")
		if got := seedsOutPath(false); got != "sim.txt" {
			t.Errorf("seedsOutPath(false) = %q with %s=1, want sim.txt", got, name)
		}
		t.Setenv(name, "")
	}

	got := simWithoutRunChanging([]string{
		"PATH=/bin", "FAULTLINE_SEED=1", "LOCKSTEP_BENCH_OUT=x", "FAULTLINE_ARTIFACTS=/a",
		"FAULTLINE_SCHEDULE=", "LOCKSTEP_BENCH_LARGE=1", "LOCKSTEP_CHECK_DETERMINISM=0",
	})
	if want := []string{"PATH=/bin", "FAULTLINE_ARTIFACTS=/a", "LOCKSTEP_BENCH_LARGE=1"}; !slices.Equal(got, want) {
		t.Errorf("simWithoutRunChanging = %q, want %q", got, want)
	}
}

// seedsOutPath returns the file TestSimSeeds appends to, or "" when it appends nothing: no
// LOCKSTEP_BENCH_OUT, a variable that simRunChanging names is set, or short is true. Under -short
// faultline runs 5 seeds instead of the 50 the result line would name, so seeds per second would
// not be the rate of 50 seeds.
func seedsOutPath(short bool) string {
	if short {
		return ""
	}
	for _, kv := range os.Environ() {
		if name, value, _ := strings.Cut(kv, "="); simRunChanging(name) && strings.TrimSpace(value) != "" {
			return ""
		}
	}
	return os.Getenv(seedsOutEnv)
}

// simWithoutRunChanging returns the environment environ without LOCKSTEP_BENCH_OUT and without the
// variables that simRunChanging names, whatever their values.
func simWithoutRunChanging(environ []string) []string {
	return slices.DeleteFunc(slices.Clone(environ), func(kv string) bool {
		name, _, _ := strings.Cut(kv, "=")
		return name == seedsOutEnv || simRunChanging(name)
	})
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

// simPairsWorld returns the body of one subtest: one node for each of the names, each running a
// ping-pong pair, and, with crashes, a plan that crashes at most one node at a time, about once a
// second and for at most half a second. The body runs the world to the end of the run.
func simPairsWorld(nodes []string, crashes bool) func(s *sim.Sim) {
	return func(s *sim.Sim) {
		w := s.World()
		for _, name := range nodes {
			s.Node(name, func(sys *lockstep.System) { simSpawnPair(sys, name) })
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

// simSpawnPair spawns the ping-pong pair on sys, the system of the node named node: actor a sends b
// a Ping every millisecond and b answers each with a Pong.
func simSpawnPair(sys *lockstep.System, node string) {
	a := lockstep.PIDAt[any](node, "/user/a")
	b := lockstep.PIDAt[harness.Ping](node, "/user/b")
	if _, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[any] { return &simPinger{peer: b} }); err != nil {
		panic(err)
	}
	if _, err := lockstep.Spawn(sys, "b", func() lockstep.Actor[harness.Ping] { return &simPonger{peer: a} }); err != nil {
		panic(err)
	}
}

// simTick is the timer message of simPinger.
type simTick struct{}

// simPinger is actor a of a pair. It receives its own timer message and the Pong that answers
// each Ping.
type simPinger struct {
	peer lockstep.PID[harness.Ping]
	seq  int64
}

func (p *simPinger) Started(ctx *lockstep.Context[any]) { ctx.Every(time.Millisecond, simTick{}) }

func (p *simPinger) Receive(ctx *lockstep.Context[any], msg any) {
	switch msg.(type) {
	case simTick:
		p.seq++
		if err := lockstep.Send(ctx, p.peer, harness.Ping{Seq: p.seq}); err != nil {
			ctx.Fail(err)
		}
	case harness.Pong:
		// The answer needs no work: receiving it is the turn.
	}
}

// simPonger is actor b of a pair. It answers every Ping with a Pong.
type simPonger struct{ peer lockstep.PID[any] }

func (p *simPonger) Receive(ctx *lockstep.Context[harness.Ping], msg harness.Ping) {
	// A Pong has the fields of a Ping, and carries its Seq back.
	if err := lockstep.Send(ctx, p.peer, any(harness.Pong(msg))); err != nil {
		ctx.Fail(err)
	}
}

// AT-BEN-07, BEN-100: with LOCKSTEP_BENCH_OUT set, go test -run '^TestSimSeeds$' -count 2 appends
// four result blocks, the pingpong and crash3 lines twice each, each after the goos, goarch and
// pkg lines. With FAULTLINE_CHECK_DETERMINISM=1 or LOCKSTEP_CHECK_DETERMINISM=1 also set, the test
// passes and appends nothing; those two cases run with -count 1, since the variables make every
// seed run twice. So does go test -short, which runs 5 seeds instead of 50.
func TestATBEN07SeedsOut(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test in a child process")
	}

	t.Run("appends", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "sim.txt") // absent: the test creates it
		goTestSimSeeds(t, out, []string{"-count", "2"})
		checkSeedBlocks(t, simReadFile(t, out))
	})

	// The file already holds a line, so "appends nothing" means the line is all it holds.
	const existing = "BenchmarkExisting 1 1 ns/op\n"
	for _, c := range []struct {
		name  string
		flags []string // go test flags
		env   []string // NAME=value pairs
	}{
		{"FAULTLINE_CHECK_DETERMINISM=1", []string{"-count", "1"}, []string{"FAULTLINE_CHECK_DETERMINISM=1"}},
		{"LOCKSTEP_CHECK_DETERMINISM=1", []string{"-count", "1"}, []string{"LOCKSTEP_CHECK_DETERMINISM=1"}},
		{"-short", []string{"-count", "1", "-short"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "sim.txt")
			if err := os.WriteFile(out, []byte(existing), 0o600); err != nil {
				t.Fatal(err)
			}
			goTestSimSeeds(t, out, c.flags, c.env...)
			if got := simReadFile(t, out); got != existing {
				t.Errorf("file is %q after the run, want %q", got, existing)
			}
		})
	}
}

// goTestSimSeeds runs TestSimSeeds in a child go test with the go test flags flags and
// LOCKSTEP_BENCH_OUT=out and the variables env, which are NAME=value pairs, and fails t unless the
// child passes and runs the test. The child gets the parent's environment without
// LOCKSTEP_BENCH_OUT and without the variables simRunChanging names, so a variable set around the
// parent cannot change what a case measures. The child is built without -race (TestSimSeeds skips
// under it), even if GOFLAGS asks for it, and gets an explicit timeout.
func goTestSimSeeds(t *testing.T, out string, flags []string, env ...string) {
	t.Helper()
	args := slices.Concat([]string{"test", "-race=false", "-run", "^TestSimSeeds$"}, flags, []string{"-timeout", "10m", "."})
	cmd := exec.Command("go", args...)
	cmd.Env = slices.Concat(simWithoutRunChanging(os.Environ()), []string{seedsOutEnv + "=" + out}, env)
	desc := "go " + strings.Join(args, " ")
	if len(env) > 0 {
		desc += " with " + strings.Join(env, " ")
	}
	got, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", desc, err, got)
	}
	if bytes.Contains(got, []byte("no tests to run")) {
		t.Fatalf("%s ran no TestSimSeeds:\n%s", desc, got)
	}
}

// simReadFile returns the content of the file name, or "" if it does not exist.
func simReadFile(t *testing.T, name string) string {
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
