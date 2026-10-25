// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"runtime"
	"testing"
	"time"
)

// TestATSCH01DefaultsAndValidation covers SCH-010 (AT-SCH-01).
func TestATSCH01DefaultsAndValidation(t *testing.T) {
	got := newEngine(t, Config{Node: "n"}).Config()
	want := Config{
		Node:          "n",
		Workers:       runtime.GOMAXPROCS(0),
		Throughput:    32,
		EffectWorkers: 64,
		EffectQueue:   1024,
	}
	if got != want {
		t.Errorf("New(Config{Node: \"n\"}).Config() = %+v, want %+v", got, want)
	}

	if q := newEngine(t, Config{Node: "n", EffectQueue: -1}).Config().EffectQueue; q != 0 {
		t.Errorf("EffectQueue -1: Config().EffectQueue = %d, want 0", q)
	}

	set := Config{Node: "m", Workers: 2, Throughput: 7, EffectWorkers: 3, EffectQueue: 5}
	if got := newEngine(t, set).Config(); got != set {
		t.Errorf("New(%+v).Config() = %+v, want the same", set, got)
	}

	for _, tc := range []struct {
		cfg  Config
		want string
	}{
		{Config{}, "lockstep: prodengine: empty node name"},
		{Config{Workers: -1}, "lockstep: prodengine: empty node name"},
		{Config{Node: "n", Workers: -1}, "lockstep: prodengine: invalid Workers -1"},
		{Config{Node: "n", Throughput: -1}, "lockstep: prodengine: invalid Throughput -1"},
		{Config{Node: "n", EffectWorkers: -1}, "lockstep: prodengine: invalid EffectWorkers -1"},
		{Config{Node: "n", EffectQueue: -2}, "lockstep: prodengine: invalid EffectQueue -2"},
		{Config{Node: "n", Workers: -3, Throughput: -4}, "lockstep: prodengine: invalid Workers -3"},
	} {
		got := panicText(func() {
			e := New(tc.cfg)
			e.Stop()
		})
		if got != tc.want {
			t.Errorf("New(%+v) panicked with %q, want %q", tc.cfg, got, tc.want)
		}
	}
}

// TestATSCH02GoroutineCount covers SCH-020 for the engine (AT-SCH-02): New starts exactly
// Workers goroutines, and Stop ends them.
func TestATSCH02GoroutineCount(t *testing.T) {
	// Workers of engines stopped by earlier tests may still be returning.
	waitFor(t, time.Second, "earlier workers to exit", func() bool { return workerGoroutines() == 0 })
	g0 := runtime.NumGoroutine()
	e := newEngine(t, Config{Node: "n", Workers: 3})
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() != g0+3 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if g := runtime.NumGoroutine(); g != g0+3 {
		t.Fatalf("after New(Workers: 3): %d goroutines, want %d", g, g0+3)
	}
	if n := workerGoroutines(); n != 3 {
		t.Fatalf("after New(Workers: 3): %d worker goroutines, want 3", n)
	}
	e.Stop()
	waitFor(t, time.Second, "workers to exit after Stop", func() bool { return workerGoroutines() == 0 })
}
