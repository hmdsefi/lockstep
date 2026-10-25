// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"errors"
	"testing"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// built calls hooks.NewSystem on a fresh manual engine for node, which binds its admission once.
func built(t *testing.T, node string, opts any) hooks.Built {
	t.Helper()
	b, err := hooks.NewSystem(enginetest.New(node, 1).Engine(), opts)
	if err != nil {
		t.Fatalf("hooks.NewSystem(%v): %v", opts, err)
	}
	return b
}

// systemOf returns b.System as a *lockstep.System.
func systemOf(t *testing.T, b hooks.Built) *lockstep.System {
	t.Helper()
	sys, ok := b.System.(*lockstep.System)
	if !ok || sys == nil {
		t.Fatalf("Built.System = %#v, want a non-nil *lockstep.System", b.System)
	}
	return sys
}

// AT-ENG-11: ENG-200.
func TestATENG11HookSet(t *testing.T) {
	if hooks.NewSystem == nil {
		t.Fatal("hooks.NewSystem is nil")
	}
	for _, opts := range []any{nil, []lockstep.Option{}} {
		b := built(t, "n1", opts)
		sys, ok := b.System.(*lockstep.System)
		if !ok || sys == nil {
			t.Fatalf("opts %v: Built.System = %#v, want a non-nil *lockstep.System", opts, b.System)
		}
		if b.Host == nil {
			t.Errorf("opts %v: Built.Host is nil", opts)
		}
		if got, want := lockstep.SystemGuardianUID(sys), uint64(1)<<32|2; got != want {
			t.Errorf("opts %v: system guardian uid %#x, want %#x", opts, got, want)
		}
	}

	_, err := hooks.NewSystem(enginetest.New("n1", 1).Engine(), 3)
	if want := "lockstep: hooks: opts has type int, want []lockstep.Option"; err == nil || err.Error() != want {
		t.Errorf("opts = 3: err = %v, want %q", err, want)
	}

	cleared := enginetest.New("n1", 1).Engine()
	cleared.Node = ""
	_, err = hooks.NewSystem(cleared, nil)
	if want := "lockstep: engine: Node is empty"; err == nil || err.Error() != want {
		t.Errorf("cleared Node: err = %v, want %q", err, want)
	}

	b := built(t, "n1", []lockstep.Option{lockstep.WithNodeName("x")})
	if got := systemOf(t, b).Node(); got != "n1" {
		t.Errorf("WithNodeName(x) on engine n1: Node() = %q, want n1", got)
	}

	_, err = hooks.NewSystem(enginetest.New("n1", 1).Engine(), []lockstep.Option{lockstep.WithMaxActors(0)})
	if !errors.Is(err, lockstep.ErrInvalidOption) {
		t.Errorf("WithMaxActors(0): err = %v, want ErrInvalidOption", err)
	}
}

// AT-ACT-35: ACT-015, ACT-020, ACT-045. The spec's text reuses one bundle for three calls; a
// manual engine's Bind panics the second time (ENG-560), so each successful call gets its own
// engine. The failing call binds nothing, so it shares an engine with the call after it.
func TestATACT35EngineBuiltSystem(t *testing.T) {
	b := built(t, "n7", []lockstep.Option{lockstep.WithNodeName("other")})
	sys := systemOf(t, b)
	if sys.Node() != "n7" || sys.Incarnation() != 1 {
		t.Errorf("Node, Incarnation = %q, %d; want n7, 1", sys.Node(), sys.Incarnation())
	}
	p, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[int] { return nil })
	if err != nil {
		t.Fatalf("Spawn on the engine-built system: %v", err)
	}
	if got, want := p.Address().UID(), uint64(1)<<32|3; got != want {
		t.Errorf("first spawned uid %#x, want %#x", got, want)
	}
	if p.Address().Node() != "n7" {
		t.Errorf("first spawned node %q, want n7", p.Address().Node())
	}

	eng := enginetest.New("n7", 1)
	got, err := hooks.NewSystem(eng.Engine(), []lockstep.Option{nil})
	if got.System != nil || got.Host != nil {
		t.Errorf("nil Option: Built = %+v, want zero", got)
	}
	if !errors.Is(err, lockstep.ErrInvalidOption) {
		t.Errorf("nil Option: err = %v, want ErrInvalidOption", err)
	} else if want := "lockstep: invalid option: nil Option"; err.Error() != want {
		t.Errorf("nil Option: message %q, want %q", err.Error(), want)
	}

	// The same bundle still works: the failed call did not consume its Bind.
	got, err = hooks.NewSystem(eng.Engine(), []lockstep.Option{lockstep.WithNodeName("-a")})
	if err != nil {
		t.Fatalf("WithNodeName(-a): %v", err)
	}
	if n := systemOf(t, got).Node(); n != "n7" {
		t.Errorf("WithNodeName(-a): Node() = %q, want n7", n)
	}
}

// AT-ENG-27: ENG-125. NewSystem binds the admission exactly once, and a source change the engine
// reports makes the runtime emit OVL's event=on record. The String and Error clauses are in
// internal/engine's TestAdmissionValues.
func TestATENG27AdmissionChange(t *testing.T) {
	eng := enginetest.New("n1", 1)
	if _, err := hooks.NewSystem(eng.Engine(), nil); err != nil {
		t.Fatal(err)
	}
	// Usage panics unless Bind ran; a second Bind during NewSystem would have panicked already.
	if got := eng.Usage(); got != (engine.Usage{}) {
		t.Errorf("Usage() = %+v, want zero", got)
	}

	eng.ChangeAdmission(engine.AdmissionChange{Source: engine.SourcePressure, On: true, Shedding: true})
	var on []engine.Record
	for _, r := range eng.Records() {
		if r.Kind == "lockstep.overload" {
			on = append(on, r)
		}
	}
	if len(on) != 1 {
		t.Fatalf("%d lockstep.overload records after the change, want 1", len(on))
	}
	got := map[string]string{}
	for _, a := range on[0].Attrs {
		got[a.Key] = a.Value
	}
	if got["event"] != "on" || got["reason"] != "pressure" || got["shedding"] != "true" {
		t.Errorf("lockstep.overload attributes %v, want event=on reason=pressure shedding=true", got)
	}
}

// ENG-200: the hook never panics, whatever the input.
func TestHookNeverPanics(t *testing.T) {
	for _, opts := range []any{3, "x", []string{"a"}, (*lockstep.System)(nil), func() {}} {
		if _, err := hooks.NewSystem(enginetest.New("n1", 1).Engine(), opts); err == nil {
			t.Errorf("opts %T: no error", opts)
		}
	}
	if _, err := hooks.NewSystem(enginetest.New("n1", 1).Engine(), []lockstep.Option(nil)); err != nil {
		t.Errorf("a nil option slice: %v", err)
	}
	if _, err := hooks.NewSystem(engine.Engine{}, nil); err == nil {
		t.Error("zero engine bundle: no error")
	}
}
