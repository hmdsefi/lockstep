// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// withLimits returns OneForOne(nil) modified by f.
func withLimits(f func(s *Strategy)) Strategy {
	s := OneForOne(nil)
	f(&s)
	return s
}

// invalidStrategies are the strategies of AT-SUP-35 with the SUP-370 text each one is refused with.
var invalidStrategies = []struct {
	name string
	s    Strategy
	text string
}{
	{"zero", Strategy{}, "Strategy must be built with OneForOne or AllForOne"},
	{"negative MaxRestarts", withLimits(func(s *Strategy) { s.MaxRestarts = -1 }), "MaxRestarts must not be negative, got -1"},
	{"zero Window", withLimits(func(s *Strategy) { s.Window = 0 }), "Window must be positive, got 0s"},
	{"negative Window", withLimits(func(s *Strategy) { s.Window = -time.Second }), "Window must be positive, got -1s"},
	{"negative MinBackoff", withLimits(func(s *Strategy) { s.MinBackoff = -1 }), "MinBackoff must not be negative, got -1ns"},
	{"MaxBackoff below MinBackoff", withLimits(func(s *Strategy) { s.MaxBackoff, s.MinBackoff = time.Second, 2*time.Second }),
		"MaxBackoff 1s is less than MinBackoff 2s"},
	// The checks run in the order of SUP-370: the first failing one is reported.
	{"first failing check", Strategy{Window: -1, MaxRestarts: -2}, "Strategy must be built with OneForOne or AllForOne"},
	{"MaxRestarts before Window", withLimits(func(s *Strategy) { s.MaxRestarts, s.Window = -2, 0 }), "MaxRestarts must not be negative, got -2"},
}

// AT-SUP-35: SUP-370.
func TestATSUP35StrategyValidation(t *testing.T) {
	sys := newLocalSystem(t)
	for _, c := range invalidStrategies {
		t.Run(c.name, func(t *testing.T) {
			opt := WithStrategy(c.s) // never panics: the strategy is validated when it is used
			pid, err := Spawn(sys, "c", idle[int](), opt)
			if pid != (PID[int]{}) {
				t.Errorf("Spawn returned PID %v, want the zero PID", pid)
			}
			want := "lockstep: invalid option: " + c.text
			if !errors.Is(err, ErrInvalidOption) || err == nil || err.Error() != want {
				t.Errorf("Spawn error = %v, want ErrInvalidOption with message %q", err, want)
			}

			err = mustPanicError(t, func() { New(WithGuardianStrategy(c.s)) })
			if !errors.Is(err, ErrInvalidOption) || err.Error() != want {
				t.Errorf("New panic = %v, want ErrInvalidOption with message %q", err, want)
			}
			_, err = hooks.NewSystem(enginetest.New("n1", 1).Engine(), []Option{WithGuardianStrategy(c.s)})
			if !errors.Is(err, ErrInvalidOption) || err == nil || err.Error() != want {
				t.Errorf("hooks.NewSystem error = %v, want ErrInvalidOption with message %q", err, want)
			}
		})
	}

	// A refused strategy registers nothing.
	if _, err := Spawn(sys, "c", idle[int]()); err != nil {
		t.Errorf("Spawn of the name an invalid strategy was refused for: %v", err)
	}
}

// SUP-370 and ACT-300: the strategy is checked after the name and in argument order with the other
// options, and a valid strategy is accepted.
func TestStrategyValidationOrder(t *testing.T) {
	sys := newLocalSystem(t)
	bad := WithStrategy(Strategy{})
	if _, err := Spawn(sys, "a b", idle[int](), bad); !errors.Is(err, ErrInvalidName) {
		t.Errorf("invalid name and strategy: %v, want ErrInvalidName", err)
	}
	other := SpawnOption(func(c *spawnConfig) { c.fail(errors.New("first")) })
	if _, err := Spawn(sys, "a", idle[int](), other, bad); err == nil || err.Error() != "first" {
		t.Errorf("an earlier invalid option: %v, want its error", err)
	}
	if _, err := Spawn(sys, "a", idle[int](), bad, other); err == nil || err.Error() == "first" {
		t.Errorf("an earlier invalid strategy: %v, want the strategy's error", err)
	}

	// The last valid strategy wins over an earlier one.
	for _, s := range []Strategy{OneForOne(nil), AllForOne(nil), withLimits(func(s *Strategy) { s.MaxRestarts = 0 })} {
		if _, err := Spawn(sys, "", idle[int](), WithStrategy(s)); err != nil {
			t.Errorf("Spawn with %+v: %v", s, err)
		}
	}
	cfg := newSpawnConfig([]SpawnOption{WithStrategy(AllForOne(nil)), WithStrategy(OneForOne(nil))})
	if cfg.err != nil || cfg.strategy.scope != uint8(supervise.OneForOne) {
		t.Errorf("last strategy: err %v scope %d, want none and one-for-one", cfg.err, cfg.strategy.scope)
	}

	if _, err := hooks.NewSystem(enginetest.New("n1", 1).Engine(), []Option{WithGuardianStrategy(AllForOne(nil))}); err != nil {
		t.Errorf("hooks.NewSystem with a valid guardian strategy: %v", err)
	}
	// A bad strategy before a bad node name is the error; after it, the node name's.
	_, err := newConfig([]Option{WithGuardianStrategy(Strategy{}), WithNodeName("")}, "")
	if err == nil || err.Error() != "lockstep: invalid option: Strategy must be built with OneForOne or AllForOne" {
		t.Errorf("strategy before node name: %v", err)
	}
	_, err = newConfig([]Option{WithNodeName(""), WithGuardianStrategy(Strategy{})}, "")
	if err == nil || err.Error() == "lockstep: invalid option: Strategy must be built with OneForOne or AllForOne" {
		t.Errorf("node name before strategy: %v", err)
	}
}

// AT-SUP-35: the two constructors and their defaults (SUP §4.1).
func TestStrategyConstructors(t *testing.T) {
	decide := func(error) Directive { return DirectiveStop }
	for _, c := range []struct {
		name  string
		s     Strategy
		scope supervise.Scope
	}{
		{"OneForOne", OneForOne(decide), supervise.OneForOne},
		{"AllForOne", AllForOne(decide), supervise.AllForOne},
	} {
		if c.s.scope != uint8(c.scope) {
			t.Errorf("%s: scope %d, want %d", c.name, c.s.scope, c.scope)
		}
		if c.s.MaxRestarts != 10 || c.s.Window != time.Minute || c.s.MinBackoff != 10*time.Millisecond || c.s.MaxBackoff != 10*time.Second {
			t.Errorf("%s: limits %d, %v, %v, %v; want 10, 1m, 10ms, 10s", c.name,
				c.s.MaxRestarts, c.s.Window, c.s.MinBackoff, c.s.MaxBackoff)
		}
		if c.s.Decider == nil || c.s.Decider(nil) != DirectiveStop {
			t.Errorf("%s: Decider is not the one passed", c.name)
		}
		if err := validateStrategy(c.s); err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	for _, s := range []Strategy{OneForOne(nil), AllForOne(nil)} {
		if s.Decider != nil {
			t.Error("a nil decider is kept as nil (DirectiveRestart for every error)")
		}
		if err := validateStrategy(s); err != nil {
			t.Errorf("%+v: %v", s, err)
		}
	}
	if OneForOne(nil).scope == AllForOne(nil).scope {
		t.Error("OneForOne and AllForOne give the same scope")
	}
	// The directives have the values of supervise's (SUP §4.2).
	for _, c := range []struct {
		d    Directive
		want supervise.Directive
	}{
		{DirectiveRestart, supervise.Restart}, {DirectiveStop, supervise.Stop},
		{DirectiveEscalate, supervise.Escalate}, {DirectiveResume, supervise.Resume},
	} {
		if supervise.Directive(c.d) != c.want {
			t.Errorf("%v = %d, supervise has %d", c.d, c.d, c.want)
		}
	}
}

// The defaults without an option: OneForOne(nil) for a child's strategy and for the guardian's.
func TestStrategyDefaults(t *testing.T) {
	cfg := newSpawnConfig(nil)
	if cfg.strategy.scope != uint8(supervise.OneForOne) || cfg.strategy.Decider != nil || cfg.strategy.MaxRestarts != 10 {
		t.Errorf("default spawn strategy %+v, want OneForOne(nil)", cfg.strategy)
	}
	sc, err := newConfig(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if sc.guardian.scope != uint8(supervise.OneForOne) || sc.guardian.Decider != nil || sc.guardian.Window != time.Minute {
		t.Errorf("default guardian strategy %+v, want OneForOne(nil)", sc.guardian)
	}
	sc, err = newConfig([]Option{WithGuardianStrategy(AllForOne(nil))}, "")
	if err != nil || sc.guardian.scope != uint8(supervise.AllForOne) {
		t.Errorf("WithGuardianStrategy(AllForOne): %+v, %v", sc.guardian, err)
	}
}

// ChildFailed is comparable and carries its fields (SUP §4.1).
func TestChildFailed(t *testing.T) {
	a := Address{node: "n", path: "/user/a", uid: 1}
	errX := errors.New("x")
	f := ChildFailed{Child: a, Err: errX, Directive: DirectiveRestart}
	if f != (ChildFailed{Child: a, Err: errX, Directive: DirectiveRestart}) {
		t.Error("equal ChildFailed values differ")
	}
	if c := (ChildFailed{Child: a, Err: errX, Cleanup: true}); c.Directive != 0 || !c.Cleanup {
		t.Errorf("cleanup ChildFailed %+v", c)
	}
}
