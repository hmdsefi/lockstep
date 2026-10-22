// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package engine_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// reach returns a Slot in state st, reached through legal transitions only.
func reach(t *testing.T, st engine.SlotState) *engine.Slot {
	t.Helper()
	s := new(engine.Slot)
	switch st {
	case engine.SlotIdle:
	case engine.SlotQueued:
		s.Ready()
	case engine.SlotRunning:
		s.Ready()
		s.Begin()
	case engine.SlotRunningReady:
		s.Ready()
		s.Begin()
		s.Ready()
	default:
		t.Fatalf("reach: no legal path to %v", st)
	}
	if got := s.State(); got != st {
		t.Fatalf("reach(%v): State() = %v", st, got)
	}
	return s
}

// call runs method on s and returns its result, or the text of its panic.
func call(s *engine.Slot, method string) (result bool, panicked string) {
	defer func() {
		if p := recover(); p != nil {
			panicked = fmt.Sprint(p)
		}
	}()
	switch method {
	case "Ready":
		return s.Ready(), ""
	case "Begin":
		s.Begin()
		return false, ""
	case "End(false)":
		return s.End(false), ""
	case "End(true)":
		return s.End(true), ""
	}
	panic("unknown method " + method)
}

// AT-ENG-03, ENG-040
func TestATENG03SlotTable(t *testing.T) {
	const (
		idle         = engine.SlotIdle
		queued       = engine.SlotQueued
		running      = engine.SlotRunning
		runningReady = engine.SlotRunningReady
	)
	tests := []struct {
		state  engine.SlotState
		method string
		result bool
		next   engine.SlotState
		panic  string // non-empty: the call panics with this text and leaves the state unchanged
	}{
		{idle, "Ready", true, queued, ""},
		{idle, "Begin", false, idle, "engine: Slot.Begin in state idle"},
		{idle, "End(false)", false, idle, "engine: Slot.End in state idle"},
		{idle, "End(true)", false, idle, "engine: Slot.End in state idle"},

		{queued, "Ready", false, queued, ""},
		{queued, "Begin", false, running, ""},
		{queued, "End(false)", false, queued, "engine: Slot.End in state queued"},
		{queued, "End(true)", false, queued, "engine: Slot.End in state queued"},

		{running, "Ready", false, runningReady, ""},
		{running, "Begin", false, running, "engine: Slot.Begin in state running"},
		{running, "End(false)", false, idle, ""},
		{running, "End(true)", true, queued, ""},

		{runningReady, "Ready", false, runningReady, ""},
		{runningReady, "Begin", false, runningReady, "engine: Slot.Begin in state running-ready"},
		{runningReady, "End(false)", true, queued, ""},
		{runningReady, "End(true)", true, queued, ""},
	}
	for _, tt := range tests {
		t.Run(tt.state.String()+"/"+tt.method, func(t *testing.T) {
			s := reach(t, tt.state)
			result, panicked := call(s, tt.method)
			if panicked != tt.panic {
				t.Errorf("%s in %v: panic %q, want %q", tt.method, tt.state, panicked, tt.panic)
			}
			if result != tt.result {
				t.Errorf("%s in %v: result %t, want %t", tt.method, tt.state, result, tt.result)
			}
			if got := s.State(); got != tt.next {
				t.Errorf("%s in %v: next state %v, want %v", tt.method, tt.state, got, tt.next)
			}
		})
	}
}

// AT-ENG-03, ENG-040
func TestSlotStateString(t *testing.T) {
	tests := []struct {
		s    engine.SlotState
		want string
	}{
		{engine.SlotIdle, "idle"},
		{engine.SlotQueued, "queued"},
		{engine.SlotRunning, "running"},
		{engine.SlotRunningReady, "running-ready"},
		{engine.SlotState(9), "SlotState(9)"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("SlotState(%d).String() = %q, want %q", uint32(tt.s), got, tt.want)
		}
	}
	if got := new(engine.Slot).State(); got != engine.SlotIdle {
		t.Errorf("zero Slot: State() = %v, want idle", got)
	}
}

// AT-ENG-03: Hint is not part of the state machine.
func TestSlotHint(t *testing.T) {
	for _, st := range []engine.SlotState{engine.SlotIdle, engine.SlotQueued, engine.SlotRunning, engine.SlotRunningReady} {
		s := reach(t, st)
		s.Hint.Store(7)
		if got := s.State(); got != st {
			t.Errorf("Hint.Store(7) in %v: State() = %v", st, got)
		}
		if st == engine.SlotIdle {
			s.Ready()
			s.Begin()
			s.End(false)
		}
		if got := s.Hint.Load(); got != 7 {
			t.Errorf("in %v: Hint.Load() = %d, want 7", st, got)
		}
	}
}

// fakeActor is the AT-ENG-04 fake: RunTurn drains an atomic work counter. A field holds the Slot,
// because a field named Slot cannot sit next to the Slot method.
type fakeActor struct {
	slot    engine.Slot
	work    atomic.Int64
	inTurn  atomic.Bool
	overlap atomic.Int64 // RunTurn calls that found inTurn already set
	turns   int          // touched by the single runner only
	more    bool         // RunTurn reports more while work is left
}

var _ engine.ActorRef = (*fakeActor)(nil)

func (a *fakeActor) Address() engine.Address {
	return engine.Address{Node: "n1", Path: "/user/a", UID: 1}
}
func (a *fakeActor) Slot() *engine.Slot { return &a.slot }

func (a *fakeActor) RunTurn(budget int) bool {
	if !a.inTurn.CompareAndSwap(false, true) {
		a.overlap.Add(1)
	}
	defer a.inTurn.Store(false)
	if budget < 1 {
		panic(fmt.Sprintf("budget %d < 1", budget))
	}
	a.turns++
	a.work.Swap(0)
	// Yield so that producers often call Ready after the drain and before End: the window in
	// which a lost wakeup would strand work.
	runtime.Gosched()
	return a.more && a.work.Load() > 0
}

// contend runs AT-ENG-04 on a: producers goroutines each add work and call Ready perTask times,
// and the calling goroutine runs every queued turn (ENG-050 step 2) until the producers are done
// and the queue is empty. A Slot panic in the runner is reported through t, not left to crash
// the test binary.
func contend(t *testing.T, a *fakeActor, producers, perTask int) {
	t.Helper()
	queue := make(chan engine.ActorRef, producers*perTask+1) // never blocks, even if the Slot misbehaves
	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for range perTask {
				a.work.Add(1)
				if a.Slot().Ready() {
					queue <- a
				}
			}
		})
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	run := func(r engine.ActorRef) {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("runner: %v", p)
			}
		}()
		r.Slot().Begin()
		more := r.RunTurn(1)
		if r.Slot().End(more) {
			queue <- r
		}
	}
	for {
		select {
		case r := <-queue:
			run(r)
		case <-done:
			for {
				select {
				case r := <-queue:
					run(r)
				default:
					return
				}
			}
		}
	}
}

// AT-ENG-04, ENG-040, ENG-050
func TestATENG04SlotContention(t *testing.T) {
	const producers, perTask = 8, 10_000
	for _, more := range []bool{false, true} {
		t.Run(fmt.Sprintf("more=%t", more), func(t *testing.T) {
			a := &fakeActor{more: more}
			contend(t, a, producers, perTask)
			if n := a.overlap.Load(); n != 0 {
				t.Errorf("RunTurn started %d times while a turn was running", n)
			}
			if w := a.work.Load(); w != 0 {
				t.Errorf("work = %d after the queue drained, want 0", w)
			}
			if st := a.slot.State(); st != engine.SlotIdle {
				t.Errorf("State() = %v after the queue drained, want idle", st)
			}
			// Without more, every turn is queued by a Ready call that changed the state.
			upper := producers * perTask
			if more {
				upper = 2 * producers * perTask
			}
			if a.turns < 1 || a.turns > upper {
				t.Errorf("turns = %d, want between 1 and %d", a.turns, upper)
			}
			t.Logf("turns = %d", a.turns)
		})
	}
}

// TestSlotNoLostWakeup repeats short AT-ENG-04 bursts on one Slot. Only the last Ready of a burst
// can strand work, so many checked bursts catch a lost wakeup that one long run rarely shows.
func TestSlotNoLostWakeup(t *testing.T) {
	const rounds, producers, perTask = 300, 4, 50
	a := &fakeActor{}
	for round := range rounds {
		contend(t, a, producers, perTask)
		if w, st := a.work.Load(), a.slot.State(); w != 0 || st != engine.SlotIdle {
			t.Fatalf("round %d: work = %d, State() = %v after the queue drained; want 0, idle", round, w, st)
		}
	}
	if n := a.overlap.Load(); n != 0 {
		t.Errorf("RunTurn started %d times while a turn was running", n)
	}
}

// BenchmarkSlot measures one Ready/Begin/End cycle (ENG §9).
func BenchmarkSlot(b *testing.B) {
	var s engine.Slot
	b.ReportAllocs()
	for b.Loop() {
		if !s.Ready() {
			b.Fatal("Ready() = false on an idle Slot")
		}
		s.Begin()
		if s.End(false) {
			b.Fatal("End(false) = true after a turn with no Ready")
		}
	}
}
