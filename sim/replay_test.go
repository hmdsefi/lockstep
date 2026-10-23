// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/artifact"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// replayFaults is the world of AT-SFT-20: servers n1 to n3 under fault.Random crashes; on n1 a
// spawner that every 100ms sends a Deposit to the sink on n2 and spawns (and stops) a child; half
// of the spawner's messages to the sink are dropped (DropBetween) and n1 has random pressure
// episodes (Pressure). The spawner asserts it never sees ErrOverloaded from Spawn: when it does, it
// starts an effect no handler serves, which fails the seed through lockstep/no-effect-handler, so
// the first episode that meets a tick fails the seed. It runs to the end of the run.
func replayFaults(s *sim.Sim) {
	w := s.World()
	spawner := lockstep.PIDAt[Tick]("n1", "/user/spawner")
	sink := lockstep.PIDAt[Deposit]("n2", "/user/sink")
	s.Node("n1", func(sys *lockstep.System) {
		children := 0
		spawn(sys, "spawner", &actor[Tick]{
			started: func(ctx *lockstep.Context[Tick]) { ctx.After(100*time.Millisecond, Tick{}) },
			receive: func(ctx *lockstep.Context[Tick], _ Tick) {
				_ = lockstep.Send(ctx, sink, Deposit{N: children})
				children++
				child, err := lockstep.Spawn(ctx, "c"+strconv.Itoa(children), func() lockstep.Actor[Deposit] { return &actor[Deposit]{} })
				switch {
				case errors.Is(err, lockstep.ErrOverloaded):
					lockstep.Do(ctx, fetch{}, func(int, error) Tick { return Tick{} })
				case err != nil:
					panic(err)
				default:
					_ = lockstep.Stop(ctx, child.Address())
				}
				ctx.After(100*time.Millisecond, Tick{})
			},
		})
	})
	s.Node("n2", func(sys *lockstep.System) { spawn(sys, "sink", &actor[Deposit]{}) })
	s.Node("n3", noSetup)
	s.DropBetween(spawner.Address(), sink.Address(), 500_000)
	s.Pressure("n1", sim.PressureRule{Every: 2 * time.Second, MinFor: 200 * time.Millisecond, MaxFor: 500 * time.Millisecond})
	w.Plan(&fault.Random{Rules: []fault.Rule{{Kind: fault.KindCrash, Every: 2 * time.Second, MaxFor: time.Second}}})
	w.RunFor(w.End().Sub(w.Sim.Now()))
}

// The scenario of AT-SFT-20 and AT-SFT-21: replayFaults, one seed.
var _ = scenario("sft-replay-faults", func(t *testing.T) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 1, Duration: 10 * time.Second}}, replayFaults)
})

// failingRun runs the scenario sft-replay-faults in a child process with a full trace and env, and
// returns the report of its failing seed and its artifact directory.
func failingRun(t *testing.T, env ...string) (*artifact.Report, string) {
	t.Helper()
	dir := t.TempDir()
	env = append([]string{"FAULTLINE_ARTIFACTS=" + dir, "FAULTLINE_TRACE=full"}, env...)
	out, code := runScenario(t, "sft-replay-faults", env...)
	if code == 0 {
		t.Fatalf("scenario sft-replay-faults exited 0, want a failing seed\n%s", out)
	}
	r, d := artifactOf(t, dir)
	if r.Failure == nil {
		t.Fatalf("report.json has status %q and no failure\n%s", r.Status, out)
	}
	return r, d
}

// AT-SFT-20, SFT-170, SFT-180: a seed that fails under Pressure and DropBetween replays exactly
// from FAULTLINE_SEED (same trace hash) and from its schedule.json under FAULTLINE_SCHEDULE (same
// failure, at the same time and event, and the same lockstep records: faultline's schedule replay
// has no planner records, so its trace hash differs). schedule.json lists kernel faults only.
func TestATSFT20ReplayWithLockstepFaults(t *testing.T) {
	orig, origDir := failingRun(t)
	if f := orig.Failure; f.Kind != "invariant" || f.Check != sim.CheckNoEffectHandler {
		t.Fatalf("failure %s %s: %s, want invariant %s", f.Kind, f.Check, f.Message, sim.CheckNoEffectHandler)
	}

	// Both lockstep faults acted before the failure.
	recs := traceRecords(t, origDir)
	drops := slices.DeleteFunc(ofKind(recs, "lockstep.drop"), func(r kernel.Record) bool { return attr(r, "reason") != "injected" })
	if len(drops) == 0 {
		t.Error("no lockstep.drop record with reason injected before the failure")
	}
	episodes := slices.DeleteFunc(ofKind(recs, "lockstep.overload"), func(r kernel.Record) bool {
		return attr(r, "event") != "on" || attr(r, "reason") != "pressure"
	})
	if len(episodes) == 0 {
		t.Error("no lockstep.overload record with event on and reason pressure before the failure")
	}

	// schedule.json lists the kernel faults only.
	schedule := filepath.Join(origDir, "schedule.json")
	kinds := scheduleKinds(t, schedule)
	if !slices.Contains(kinds, string(fault.KindCrash)) {
		t.Errorf("schedule.json kinds %q list no crash", kinds)
	}
	for _, k := range kinds {
		if !slices.Contains(fault.Kinds(), fault.Kind(k)) || strings.HasPrefix(k, "lockstep") {
			t.Errorf("schedule.json lists kind %q, want kernel faults only", k)
		}
	}

	seed := orig.Replay.Env["FAULTLINE_SEED"]
	if seed != orig.Seed {
		t.Fatalf("the replay command sets FAULTLINE_SEED=%q, want the seed %s", seed, orig.Seed)
	}
	sameFailure := func(name string, r *artifact.Report) {
		t.Helper()
		got, want := r.Failure, orig.Failure
		if got.Signature != want.Signature || got.Message != want.Message || got.AtNS != want.AtNS || got.Event != want.Event {
			t.Errorf("%s fails with %s %q at %s event %d, want %s %q at %s event %d",
				name, got.Signature, got.Message, got.At, got.Event, want.Signature, want.Message, want.At, want.Event)
		}
	}

	bySeed, _ := failingRun(t, "FAULTLINE_SEED="+seed)
	sameFailure("the FAULTLINE_SEED replay", bySeed)
	if bySeed.Run.TraceHash != orig.Run.TraceHash {
		t.Errorf("the FAULTLINE_SEED replay has trace hash %s, the original %s, want equal", bySeed.Run.TraceHash, orig.Run.TraceHash)
	}

	bySchedule, scheduleDir := failingRun(t, "FAULTLINE_SEED="+seed, "FAULTLINE_SCHEDULE="+schedule)
	sameFailure("the FAULTLINE_SCHEDULE replay", bySchedule)
	want, got := lockstepTrace(t, origDir), lockstepTrace(t, scheduleDir)
	if !slices.Equal(got, want) {
		i := 0
		for i < min(len(got), len(want)) && got[i] == want[i] {
			i++
		}
		t.Errorf("the schedule replay's lockstep records and crashes differ from the original's from record %d of %d and %d", i, len(got), len(want))
	}
}

// sftFiles are the non-test files of sim that implement SFT.
var sftFiles = []string{"faults.go", "interpose.go", "pairs.go", "pressure.go"}

// AT-SFT-21, SFT-190: AT-SIM-31's static check covers the SFT files; every SFT misuse panics, and
// the calls that return errors store no failure; the failing scenario of AT-SFT-20 reports a check
// name of SIM-300.
func TestATSFT21NoDirectFailures(t *testing.T) {
	checked := checkNoDirectFailures(t)
	for _, f := range sftFiles {
		if !slices.Contains(checked, f) {
			t.Errorf("the static check did not read %s; it read %q", f, checked)
		}
	}

	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var pid, other lockstep.PID[CMsg]
		counterNode(s, "n1", &counter{}, &pid)
		counterNode(s, "n2", &counter{}, &other)
		w.RunFor(time.Second)
		a, b := pid.Address(), other.Address()

		valid := sim.PressureRule{Every: time.Second, MinFor: time.Millisecond, MaxFor: time.Millisecond}
		for _, c := range []struct {
			want string
			call func()
		}{
			{"lockstep: sim: InjectFail: nil error", func() { _ = s.InjectFail(a, nil) }},
			{`lockstep: sim: unknown node "zz"`, func() { _ = s.InjectFail(lockstep.PIDAt[CMsg]("zz", "/user/x").Address(), errBoom) }},
			{`lockstep: sim: unknown node "zz"`, func() { _ = s.StopActor(lockstep.PIDAt[CMsg]("zz", "/user/x").Address()) }},
			{"lockstep: sim: DelayBetween: invalid range [-1ns, 0s]", func() { s.DelayBetween(a, b, -1, 0) }},
			{"lockstep: sim: DelayBetween: invalid range [2ms, 1ms]", func() { s.DelayBetween(a, b, 2*time.Millisecond, time.Millisecond) }},
			{"lockstep: sim: DropBetween: ppm 1000001 > 1000000", func() { s.DropBetween(a, b, 1_000_001) }},
			{`lockstep: sim: unknown node "zz"`, func() { s.Pressure("zz", valid) }},
			{"lockstep: sim: Pressure: Every 0s must be positive", func() { s.Pressure("n1", sim.PressureRule{MinFor: time.Millisecond, MaxFor: time.Millisecond}) }},
			{"lockstep: sim: Pressure: MaxFor 1ms is less than MinFor 2ms", func() {
				s.Pressure("n1", sim.PressureRule{Every: time.Second, MinFor: 2 * time.Millisecond, MaxFor: time.Millisecond})
			}},
			{`lockstep: sim: unknown node "zz"`, func() { s.PressureFor("zz", time.Second) }},
			{"lockstep: sim: PressureFor: duration 0s must be positive", func() { s.PressureFor("n1", 0) }},
		} {
			expectPanic(tt, c.want, c.call)
		}
		if n := len(ofKind(w.Sim.Records(), "lockstep.fault")); n != 0 {
			tt.Errorf("%d lockstep.fault records after the misuse panics, want 0", n)
		}

		// A dead address and a down node are errors of the call, not failures of the seed.
		if err := s.StopActor(a); err != nil {
			tt.Fatalf("StopActor = %v", err)
		}
		w.RunFor(time.Second)
		if err := s.InjectFail(a, errBoom); !errors.Is(err, lockstep.ErrNotFound) {
			tt.Errorf("InjectFail of a stopped actor = %v, want ErrNotFound", err)
		}
		if err := s.StopActor(a); !errors.Is(err, lockstep.ErrNotFound) {
			tt.Errorf("StopActor of a stopped actor = %v, want ErrNotFound", err)
		}
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n2"})
		if err := s.InjectFail(b, errBoom); !errors.Is(err, lockstep.ErrStopped) {
			tt.Errorf("InjectFail on a down node = %v, want ErrStopped", err)
		}
		if err := s.StopActor(b); !errors.Is(err, lockstep.ErrStopped) {
			tt.Errorf("StopActor on a down node = %v, want ErrStopped", err)
		}
		w.RunFor(time.Second)
		for _, check := range []string{sim.CheckMisuse, sim.CheckNoEffectHandler, sim.CheckLeak, sim.CheckDeadlock} {
			if err := s.Stored(check); err != nil {
				tt.Errorf("%s stored %v, want nothing", check, err)
			}
		}
	})

	got := failingSeed(t, "sft-replay-faults")
	checks := []string{sim.CheckMisuse, sim.CheckNoEffectHandler, sim.CheckLeak, sim.CheckDeadlock}
	if !slices.Contains(checks, got.Check) || got.Signature != got.Kind+":"+got.Check {
		t.Errorf("the failing scenario of AT-SFT-20 reports %+v, want a check of SIM-300 (%q)", got, checks)
	}
	if want := `lockstep: no effect handler: effect "prices.fetch" called by lockstep://n1/user/spawner#`; !strings.HasPrefix(got.Message, want) {
		t.Errorf("failure message %q, want it to start with %q", got.Message, want)
	}
}
