// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"flag"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/artifact"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// scenarios are the child-process scenarios of this package, by name. Tests look them up by key
// and never range over the map.
var scenarios = map[string]func(t *testing.T){}

// scenario registers fn under name; use it in package-level var declarations. A name used twice
// panics when the test binary starts, so a scenario never silently replaces another.
func scenario(name string, fn func(t *testing.T)) bool { //nolint:unparam // var _ = scenario(...) registers a scenario at package level
	if _, dup := scenarios[name]; dup {
		panic("scenario " + name + " registered twice")
	}
	scenarios[name] = fn
	return true
}

// scenarioEnv names the scenario that TestScenario runs in a child process.
const scenarioEnv = "LOCKSTEP_TEST_SCENARIO"

// TestScenario runs the scenario named by LOCKSTEP_TEST_SCENARIO and does nothing otherwise.
func TestScenario(t *testing.T) {
	name := os.Getenv(scenarioEnv)
	if name == "" {
		return
	}
	fn, ok := scenarios[name]
	if !ok {
		t.Fatalf("unknown scenario %q", name)
	}
	fn(t)
}

// scenarioTimeout bounds a scenario child when the parent test has no deadline (-timeout 0).
const scenarioTimeout = 10 * time.Minute

// runScenario runs scenario name in a child process of this test binary: the parent environment
// minus every FAULTLINE_* and LOCKSTEP_* variable, plus LOCKSTEP_TEST_SCENARIO,
// FAULTLINE_ARTIFACTS=<t.TempDir()>, plus env (later entries win). It returns the combined output
// and the exit code. Under go test -cover the child adds its coverage to this binary's.
//
// A hung child fails the calling test: it stops itself with a stack dump (-test.timeout) after a
// fifth of the time left before the parent's deadline, and is killed at nine tenths of it.
func runScenario(t *testing.T, name string, env ...string) (string, int) {
	t.Helper()
	limit, kill := scenarioTimeout, scenarioTimeout+time.Minute
	if d, ok := t.Deadline(); ok {
		left := time.Until(d)
		limit, kill = left/5, left-left/10
	}
	ctx, cancel := context.WithTimeout(t.Context(), kill)
	defer cancel()
	args := append([]string{"-test.run=^TestScenario$", "-test.v", "-test.timeout=" + limit.String()}, coverArgs()...)
	cmd := exec.CommandContext(ctx, os.Args[0], args...) //nolint:gosec // runs this test binary again
	cmd.WaitDelay = 5 * time.Second
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(k, "FAULTLINE_") && !strings.HasPrefix(k, "LOCKSTEP_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	cmd.Env = append(cmd.Env, scenarioEnv+"="+name, "FAULTLINE_ARTIFACTS="+t.TempDir())
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("scenario %s killed after %s: %v\n%s", name, kill, ctx.Err(), out)
	}
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return string(out), code
}

// coverArgs returns the flag that makes a child process of this test binary write its coverage
// data where go test collects this binary's, when go test measures coverage.
func coverArgs() []string {
	if f := flag.Lookup("test.gocoverdir"); f != nil && f.Value.String() != "" {
		return []string{"-test.gocoverdir=" + f.Value.String()}
	}
	return nil
}

// failure is the part of faultline's report of a failing seed that tests assert on.
type failure struct {
	Kind      string // invariant, final, panic, ...
	Check     string // the check name
	Signature string // <kind>:<check>
	Message   string
}

// failingSeed runs scenario name in a child process with FAULTLINE_ARTIFACTS set to a temporary
// directory, finds the one report.json beneath it and returns the failure it records. The test
// fails if the child exits 0 or does not leave exactly one report.json. The scenario must run one
// seed, so that faultline writes one report.
func failingSeed(t *testing.T, name string) failure {
	t.Helper()
	dir := t.TempDir()
	out, code := runScenario(t, name, "FAULTLINE_ARTIFACTS="+dir)
	if code == 0 {
		t.Fatalf("scenario %s exited 0, want a failing seed\n%s", name, out)
	}
	var reports []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "report.json" {
			reports = append(reports, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 {
		t.Fatalf("scenario %s left %d report.json files under %s, want 1\n%s", name, len(reports), dir, out)
	}
	f, err := os.Open(reports[0])
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read-only
	r, err := artifact.ReadReport(f)
	if err != nil {
		t.Fatal(err)
	}
	if r.Failure == nil {
		t.Fatalf("scenario %s: report.json has status %q and no failure\n%s", name, r.Status, out)
	}
	return failure{Kind: r.Failure.Kind, Check: r.Failure.Check, Signature: r.Failure.Signature, Message: r.Failure.Message}
}

// leakFetch is the effect of the leak tests; the simulation never calls its Run.
type leakFetch struct{ sym string }

func (leakFetch) Name() string                       { return "prices.fetch" }
func (leakFetch) Run(context.Context) (int64, error) { panic("Run called") }

// leakSlow is a second effect, to have two pending at once.
type leakSlow struct{}

func (leakSlow) Name() string                       { return "prices.slow" }
func (leakSlow) Run(context.Context) (int64, error) { panic("Run called") }

// leakMsg is the message of the leak tests' actor.
type leakMsg struct{ fail bool }

// leakScenario returns a scenario that runs one actor at /user/a on n1. The actor starts the
// effects of effs in every incarnation. skip switches the runtime's effect cancellation at stop
// off. latency is the handlers' latency. Then body acts on the node and the run goes on for a
// second.
func leakScenario(skip bool, latency time.Duration, effs []lockstep.Effect[int64], body func(s *sim.Sim, n1 *kernel.Node, a lockstep.PID[leakMsg])) func(t *testing.T) {
	return func(t *testing.T) {
		sim.Simulate(t, simOpts(1), func(s *sim.Sim) {
			w := s.World()
			var a lockstep.PID[leakMsg]
			s.HandleEffect("prices.fetch", func(sim.EffectCall) sim.EffectResult { return sim.EffectResult{Latency: latency} })
			s.HandleEffect("prices.slow", func(sim.EffectCall) sim.EffectResult { return sim.EffectResult{Latency: latency} })
			n1 := s.Node("n1", func(sys *lockstep.System) {
				if skip {
					lockstep.SkipEffectCancelAtStop(sys)
				}
				a = simSpawn(sys, "a", &simActor[leakMsg]{
					started: func(ctx *lockstep.Context[leakMsg]) {
						for _, e := range effs {
							lockstep.Do(ctx, e, func(int64, error) leakMsg { return leakMsg{} })
						}
					},
					receive: func(ctx *lockstep.Context[leakMsg], m leakMsg) {
						if m.fail {
							ctx.Fail(errors.New("boom"))
						}
					},
				})
			})
			w.RunFor(10 * time.Millisecond)
			body(s, n1, a)
			w.RunFor(time.Second)
		})
	}
}

// stopA stops the actor at /user/a.
func stopA(s *sim.Sim, _ *kernel.Node, a lockstep.PID[leakMsg]) {
	if err := lockstep.Stop(s.System("n1"), a.Address()); err != nil {
		s.World().T().Fatalf("Stop: %v", err)
	}
}

var (
	oneFetch = []lockstep.Effect[int64]{leakFetch{"X"}}
	twoSlow  = []lockstep.Effect[int64]{leakFetch{"X"}, leakSlow{}}

	// The effect is still pending when the stop does not cancel it.
	_ = scenario("leak-effect-pending", leakScenario(true, time.Minute, oneFetch, stopA))
	// Two pending effects: the first of them, by ID, is the failure.
	_ = scenario("leak-effects-two", leakScenario(true, time.Minute, twoSlow, stopA))
	// A stop that cancels its effects leaks none.
	_ = scenario("leak-effect-cancelled", leakScenario(false, time.Minute, oneFetch, stopA))
	// An effect that delivered before the stop is not held.
	_ = scenario("leak-effect-delivered", leakScenario(true, time.Millisecond, oneFetch, stopA))
	// A node crash discards the system: its actors are never checked.
	_ = scenario("leak-effect-crash", leakScenario(true, time.Minute, oneFetch, func(_ *sim.Sim, n1 *kernel.Node, _ lockstep.PID[leakMsg]) { n1.Crash() }))
	// A restart cancels the effects whatever the switch says; it is for a stop.
	_ = scenario("leak-effect-restart", leakScenario(true, time.Minute, oneFetch, func(s *sim.Sim, _ *kernel.Node, a lockstep.PID[leakMsg]) {
		if err := sim.Send(s, "n1", a, leakMsg{fail: true}); err != nil {
			s.World().T().Fatalf("Send: %v", err)
		}
	}))
)

// AT-SIM-26, SIM-290 (b): with the runtime's effect cancellation at stop switched off, an actor
// stopped with a pending effect fails the seed with check lockstep/leak.
func TestATSIM26PendingEffect(t *testing.T) {
	got := failingSeed(t, "leak-effect-pending")
	if got.Kind != "invariant" || got.Check != "lockstep/leak" || got.Signature != "invariant:lockstep/leak" {
		t.Errorf("failure %+v, want invariant lockstep/leak", got)
	}
	want := regexp.MustCompile(`^lockstep: leak: lockstep://n1/user/a#[0-9a-f]+ \(incarnation 1\): effect 1 "prices.fetch" still pending$`)
	if !want.MatchString(got.Message) {
		t.Errorf("message %q does not match %s", got.Message, want)
	}
}

// SIM-290 (b): the effects of a stopped actor are reported in ID order.
func TestSIM290PendingEffectsInIDOrder(t *testing.T) {
	got := failingSeed(t, "leak-effects-two")
	if !strings.HasSuffix(got.Message, `effect 1 "prices.fetch" still pending`) {
		t.Errorf("message %q, want the failure of effect 1, the lowest ID", got.Message)
	}
}

// SIM-290 (b): no leak for an effect the stop cancelled, one that delivered, one lost to a node
// crash (SUP-360), or a restart.
func TestSIM290NoLeak(t *testing.T) {
	for _, name := range []string{"leak-effect-cancelled", "leak-effect-delivered", "leak-effect-crash", "leak-effect-restart"} {
		t.Run(name, func(t *testing.T) {
			if out, code := runScenario(t, name); code != 0 {
				t.Errorf("scenario %s failed the seed (exit %d)\n%s", name, code, out)
			}
		})
	}
}
