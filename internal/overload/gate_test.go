// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/overload"
)

// panicText returns the text of the panic f raises, or "" when it does not panic.
func panicText(f func()) (text string) {
	defer func() {
		if r := recover(); r != nil {
			text, _ = r.(string)
		}
	}()
	f()
	return ""
}

// shedSource asserts that err is an overloaded refusal naming want, and returns nothing else.
func shedSource(t *testing.T, label string, err error, want engine.AdmissionSource) {
	t.Helper()
	var ae *engine.AdmissionError
	if !errors.As(err, &ae) {
		t.Fatalf("%s: error %v is not an *engine.AdmissionError", label, err)
	}
	if ae.Source != want {
		t.Fatalf("%s: refused by %v; want %v", label, ae.Source, want)
	}
	if !errors.Is(err, engine.ErrOverloaded) {
		t.Fatalf("%s: errors.Is(%v, engine.ErrOverloaded) is false", label, err)
	}
}

func TestATOVL18Gate(t *testing.T) {
	g := overload.NewGate()
	var changes []engine.AdmissionChange
	g.Bind(func(c engine.AdmissionChange) { changes = append(changes, c) },
		func() engine.Usage { return engine.Usage{Actors: 3, Bytes: 7} })

	if err := g.AdmitSend(); err != nil {
		t.Fatalf("AdmitSend on a new Gate = %v; want nil", err)
	}
	if err := g.AdmitSpawn(false); err != nil {
		t.Fatalf("AdmitSpawn on a new Gate = %v; want nil", err)
	}

	g.Set(engine.SourcePressure, true, 0, 0, 0)
	shedSource(t, "pressure on", g.AdmitSend(), engine.SourcePressure)
	g.Set(engine.SourceBudget, true, 5, 9, 4)
	shedSource(t, "both on", g.AdmitSend(), engine.SourcePressure)
	g.Set(engine.SourcePressure, false, 0, 0, 0)
	shedSource(t, "pressure off, budget on", g.AdmitSend(), engine.SourceBudget)
	g.Set(engine.SourceBudget, false, 3, 9, 4)
	if err := g.AdmitSend(); err != nil {
		t.Fatalf("AdmitSend after the last source turned off = %v; want nil", err)
	}

	want := []engine.AdmissionChange{
		{Source: engine.SourcePressure, On: true, Shedding: true},
		{Source: engine.SourceBudget, On: true, Live: 5, High: 9, Low: 4, Shedding: true},
		{Source: engine.SourcePressure, On: false, Shedding: true},
		{Source: engine.SourceBudget, On: false, Live: 3, High: 9, Low: 4, Shedding: false},
	}
	if len(changes) != len(want) {
		t.Fatalf("got %d changes %+v; want %d", len(changes), changes, len(want))
	}
	for i := range want {
		if changes[i] != want[i] {
			t.Errorf("change %d = %+v; want %+v", i, changes[i], want[i])
		}
	}

	g.Set(engine.SourceBudget, false, 1, 2, 3) // repeated: changes nothing
	if len(changes) != len(want) {
		t.Fatalf("a repeated Set reported %+v", changes[len(want):])
	}

	if got := g.Usage(); got != (engine.Usage{Actors: 3, Bytes: 7}) {
		t.Fatalf("Usage() = %+v; want {3 7}", got)
	}

	if got := panicText(func() { overload.NewGate().Set(engine.SourceMemory, true, 0, 0, 0) }); got != "overload: Gate.Set before Bind" {
		t.Fatalf("Set before Bind panicked with %q", got)
	}
	if got := panicText(func() { g.Bind(nil, nil) }); got != "overload: Gate.Bind called twice" {
		t.Fatalf("second Bind panicked with %q", got)
	}
}

func TestGateSpawn(t *testing.T) {
	g := overload.NewGate()
	g.Bind(func(engine.AdmissionChange) {}, nil)
	g.Set(engine.SourceMemory, true, 950, 900, 800)
	shedSource(t, "normal spawn", g.AdmitSpawn(false), engine.SourceMemory)
	if err := g.AdmitSpawn(true); err != nil {
		t.Fatalf("reserve AdmitSpawn while shedding = %v; want nil", err)
	}
	g.Set(engine.SourceMemory, false, 700, 900, 800)
	if err := g.AdmitSpawn(false); err != nil {
		t.Fatalf("AdmitSpawn after shedding ended = %v; want nil", err)
	}
}

func TestGateUsageBeforeBind(t *testing.T) {
	g := overload.NewGate()
	if got := g.Usage(); got != (engine.Usage{}) {
		t.Fatalf("Usage() before Bind = %+v; want the zero Usage", got)
	}
	g.Bind(func(engine.AdmissionChange) {}, nil)
	if got := g.Usage(); got != (engine.Usage{}) {
		t.Fatalf("Usage() with a nil usage function = %+v; want the zero Usage", got)
	}
}

// A source that turns on while another is already the reason does not take over, and the reason
// after the first turns off is the source that has been on longest.
func TestGateReasonOrder(t *testing.T) {
	g := overload.NewGate()
	g.Bind(func(engine.AdmissionChange) {}, nil)
	g.Set(engine.SourcePressure, true, 0, 0, 0)
	g.Set(engine.SourceMemory, true, 1, 2, 1)
	g.Set(engine.SourceBudget, true, 1, 2, 1)
	g.Set(engine.SourcePressure, false, 0, 0, 0)
	shedSource(t, "memory next", g.AdmitSend(), engine.SourceMemory)
	g.Set(engine.SourceBudget, false, 1, 2, 1) // not the reason: the reason stays
	shedSource(t, "memory still", g.AdmitSend(), engine.SourceMemory)
	g.Set(engine.SourceMemory, false, 1, 2, 1)
	if err := g.AdmitSend(); err != nil {
		t.Fatalf("AdmitSend with every source off = %v; want nil", err)
	}
}

// onChange runs outside the Gate's lock: it can call every method that does not Set.
func TestGateOnChangeOutsideLock(t *testing.T) {
	g := overload.NewGate()
	var inside error
	g.Bind(func(c engine.AdmissionChange) {
		inside = g.AdmitSend()
		_ = g.AdmitSpawn(false)
		_ = g.Usage()
	}, func() engine.Usage { return engine.Usage{Actors: 1} })
	g.Set(engine.SourcePressure, true, 0, 0, 0)
	shedSource(t, "AdmitSend from onChange", inside, engine.SourcePressure)
}

// Concurrent Sets of different sources are reported one at a time, each change's Shedding
// consistent with the order they were reported in.
func TestGateConcurrentSet(t *testing.T) {
	g := overload.NewGate()
	var (
		running bool
		bad     string
		got     []engine.AdmissionChange
	)
	g.Bind(func(c engine.AdmissionChange) {
		if running {
			bad = "onChange ran concurrently with itself"
		}
		running = true
		got = append(got, c)
		running = false
	}, nil)
	var wg sync.WaitGroup
	for _, src := range []engine.AdmissionSource{engine.SourceMemory, engine.SourceBudget, engine.SourcePressure} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				g.Set(src, true, 0, 0, 0)
				g.Set(src, false, 0, 0, 0)
			}
		}()
	}
	wg.Wait()
	if bad != "" {
		t.Fatal(bad)
	}
	on := 0
	for i, c := range got {
		if c.On {
			on++
		} else {
			on--
		}
		if c.Shedding != (on > 0) {
			t.Fatalf("change %d = %+v with %d sources on; Shedding is inconsistent", i, c, on)
		}
	}
	if on != 0 {
		t.Fatalf("%d sources still on after every one was turned off", on)
	}
	if err := g.AdmitSend(); err != nil {
		t.Fatalf("AdmitSend with every source off = %v; want nil", err)
	}
}
