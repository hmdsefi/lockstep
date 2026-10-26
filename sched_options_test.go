// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"runtime"
	"slices"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// AT-SCH-27: SCH-035, SCH-040. No option constructor panics for any argument; the values are
// validated by the systems and by Spawn.
func TestATSCH27OptionConstructorsNeverPanic(t *testing.T) {
	sysOpts := map[string]Option{
		"WithWorkers(0)":     WithWorkers(0),
		"WithWorkers(-5)":    WithWorkers(-5),
		"WithThroughput(0)":  WithThroughput(0),
		"WithEffectPool":     WithEffectPool(0, -1),
		"WithThroughput(-1)": WithThroughput(-1),
	}
	for name, o := range sysOpts {
		if o == nil {
			t.Errorf("%s returned a nil Option", name)
		}
	}
	if WithActorThroughput(0) == nil || WithActorThroughput(-3) == nil {
		t.Error("WithActorThroughput returned a nil SpawnOption")
	}

	built, err := hooks.NewSystem(enginetest.New("n", 1).Engine(), []Option{WithWorkers(0)})
	if built.System != nil || built.Host != nil {
		t.Errorf("hooks.NewSystem(WithWorkers(0)) returned a system: %#v", built)
	}
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("hooks.NewSystem(WithWorkers(0)): err = %v, want ErrInvalidOption", err)
	}
	if want := "lockstep: invalid option: WithWorkers(0): n must be at least 1"; err.Error() != want {
		t.Errorf("hooks.NewSystem(WithWorkers(0)): err = %q, want %q", err, want)
	}

	sys, _ := newManualSystem(t, WithWorkers(2), WithThroughput(7), WithEffectPool(3, 5))
	_, err = Spawn(sys, "a", idle[int](), WithActorThroughput(0))
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("Spawn(WithActorThroughput(0)): err = %v, want ErrInvalidOption", err)
	}
	if want := "lockstep: invalid option: WithActorThroughput(0): n must be at least 1"; err.Error() != want {
		t.Errorf("Spawn(WithActorThroughput(0)): err = %q, want %q", err, want)
	}
	if _, err := Spawn(sys, "a", idle[int](), WithActorThroughput(1)); err != nil {
		t.Errorf("Spawn(WithActorThroughput(1)) after a refused spawn: %v", err)
	}
}

// AT-SCH-03 (panic half): SCH-030. New panics with the section 7 error for each invalid system
// option, before any engine exists.
func TestNewRejectsSchedulerOptions(t *testing.T) {
	cases := []struct {
		name string
		opt  Option
		want string
	}{
		{"workers 0", WithWorkers(0), "lockstep: invalid option: WithWorkers(0): n must be at least 1"},
		{"workers -2", WithWorkers(-2), "lockstep: invalid option: WithWorkers(-2): n must be at least 1"},
		{"throughput 0", WithThroughput(0), "lockstep: invalid option: WithThroughput(0): n must be at least 1"},
		{"effect workers 0", WithEffectPool(0, 1),
			"lockstep: invalid option: WithEffectPool(0, 1): workers must be at least 1 and queue at least 0"},
		{"effect queue -1", WithEffectPool(1, -1),
			"lockstep: invalid option: WithEffectPool(1, -1): workers must be at least 1 and queue at least 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := mustPanicError(t, func() { New(tc.opt) })
			if !errors.Is(e, ErrInvalidOption) {
				t.Errorf("panic error %v does not match ErrInvalidOption", e)
			}
			if e.Error() != tc.want {
				t.Errorf("panic error = %q, want %q", e, tc.want)
			}
			_, err := newSystem(enginetest.New("n", 1).Engine(), []Option{tc.opt})
			if err == nil || err.Error() != tc.want || !errors.Is(err, ErrInvalidOption) {
				t.Errorf("newSystem error = %v, want %q wrapping ErrInvalidOption", err, tc.want)
			}
		})
	}
}

// SCH-030, SCH-035: defaults apply without options, of several valid options for a setting the
// last counts, and an invalid value is reported even when a later option is valid. The first
// invalid option in argument order is the error.
func TestSchedulerOptionValues(t *testing.T) {
	c, err := newConfig(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if c.workers != runtime.GOMAXPROCS(0) || c.throughput != 32 || c.effectWorkers != 64 || c.effectQueue != 1024 {
		t.Errorf("defaults = %d, %d, %d, %d; want GOMAXPROCS, 32, 64, 1024",
			c.workers, c.throughput, c.effectWorkers, c.effectQueue)
	}
	c, err = newConfig([]Option{WithWorkers(1), WithWorkers(2), WithThroughput(7), WithEffectPool(3, 0)}, "")
	if err != nil {
		t.Fatalf("valid options: %v", err)
	}
	if c.workers != 2 || c.throughput != 7 || c.effectWorkers != 3 || c.effectQueue != 0 {
		t.Errorf("values = %d, %d, %d, %d; want 2, 7, 3, 0", c.workers, c.throughput, c.effectWorkers, c.effectQueue)
	}
	_, err = newConfig([]Option{WithWorkers(0), WithWorkers(2)}, "")
	if want := "lockstep: invalid option: WithWorkers(0): n must be at least 1"; err == nil || err.Error() != want {
		t.Errorf("invalid then valid: err = %v, want %q", err, want)
	}
	_, err = newConfig([]Option{WithEffectPool(0, 0), WithThroughput(0), WithWorkers(0)}, "")
	if want := "lockstep: invalid option: WithEffectPool(0, 0): workers must be at least 1 and queue at least 0"; err == nil || err.Error() != want {
		t.Errorf("all three invalid: err = %v, want %q", err, want)
	}
}

// AT-SCH-25 (manual engine): SCH-040, ENG-310. The actor's throughput replaces the engine's budget
// unless the budget is 1.
func TestActorThroughputCapsTurn(t *testing.T) {
	// perStep sends msgs messages to an actor spawned with opts on a manual engine with the given
	// budget and returns how many messages each Step processed.
	perStep := func(t *testing.T, budget, msgs int, opts ...SpawnOption) []int {
		t.Helper()
		sys, eng := newManualSystem(t)
		eng.SetBudget(budget)
		var seen int
		pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
			receive: func(*Context[int], int) { seen++ },
		}), opts...)
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		for m := range msgs {
			send(t, sys, pid, m)
		}
		var got []int
		for last := seen; eng.Step(); last = seen {
			got = append(got, seen-last)
		}
		return got
	}

	for _, tc := range []struct {
		name   string
		budget int
		opts   []SpawnOption
		want   []int
	}{
		{"cap below the budget", 32, []SpawnOption{WithActorThroughput(4)}, []int{4, 4, 2}},
		{"cap above the budget", 2, []SpawnOption{WithActorThroughput(4)}, []int{4, 4, 2}},
		{"budget 1 is never raised", 1, []SpawnOption{WithActorThroughput(4)}, slices.Repeat([]int{1}, 10)},
		{"no option uses the budget", 32, nil, []int{10}},
		{"the last option wins", 32, []SpawnOption{WithActorThroughput(2), WithActorThroughput(5)}, []int{5, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := perStep(t, tc.budget, 10, tc.opts...); !slices.Equal(got, tc.want) {
				t.Errorf("messages per Step = %v, want %v", got, tc.want)
			}
		})
	}
}
