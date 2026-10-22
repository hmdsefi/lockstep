// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// ENG-310, ENG-320: the budget bounds user messages per turn; more reports what is left.
func TestRunTurnBudget(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got []int
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(_ *Context[int], m int) { got = append(got, m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, pid)

	if v := mustPanic(t, func() { c.RunTurn(0) }); v != "lockstep: RunTurn budget 0 < 1" {
		t.Errorf("RunTurn(0) panicked with %v", v)
	}
	if more := c.RunTurn(2); more {
		t.Error("RunTurn(2) on an empty cell: more = true")
	}

	for m := 1; m <= 5; m++ {
		send(t, sys, pid, m)
	}
	// The sends queued a turn on the manual engine; run the turns directly instead.
	for i, want := range []struct {
		n    int
		more bool
	}{{2, true}, {2, true}, {1, false}} {
		before := len(got)
		more := c.RunTurn(2)
		if n := len(got) - before; n != want.n || more != want.more {
			t.Errorf("turn %d: processed %d, more %v; want %d, %v", i+1, n, more, want.n, want.more)
		}
	}
	if want := []int{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Errorf("received %v, want %v", got, want)
	}

	// A system message is processed whatever the budget, and makes more true until it is.
	c.mu.Lock()
	c.sysq.push(begin{})
	c.mu.Unlock()
	if !c.hasWork() {
		t.Error("hasWork with a queued system message = false")
	}
	if more := c.RunTurn(1); more {
		t.Error("RunTurn after the system message: more = true")
	}
}

// ENG-330: a panic in Receive stays inside RunTurn, goes to handlerFailed, and the cell pops no
// further user message.
func TestRunTurnRecoversPanic(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got []int
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(_ *Context[int], m int) {
			got = append(got, m)
			panic("boom")
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, pid, 1)
	send(t, sys, pid, 2)
	send(t, sys, pid, 3)
	eng.RunUntilIdle() // panics if the handler's panic escapes RunTurn

	c := cellOf(t, sys, pid)
	if !slices.Equal(got, []int{1}) {
		t.Errorf("received %v, want [1]", got)
	}
	if c.state != cellFailed || c.failure == nil || !strings.Contains(c.failure.Error(), "boom") {
		t.Errorf("state %d, failure %v; want cellFailed and the panic", c.state, c.failure)
	}
	if c.ctx.inTurn.Load() {
		t.Error("in-turn flag still set after the panic")
	}
	if more := c.RunTurn(10); more || len(got) != 1 {
		t.Errorf("RunTurn after the failure: more %v, received %v", more, got)
	}
	send(t, sys, pid, 4)
	eng.RunUntilIdle()
	if len(got) != 1 {
		t.Errorf("a message after the failure was received: %v", got)
	}

	// A factory that returns nil or panics fails the start: no incarnation, no Started.
	for name, factory := range map[string]func() Actor[int]{
		"nil":   func() Actor[int] { return nil },
		"panic": func() Actor[int] { panic("factory boom") },
	} {
		p, err := Spawn(sys, name, factory)
		if err != nil {
			t.Fatal(err)
		}
		send(t, sys, p, 1)
		eng.RunUntilIdle()
		c := cellOf(t, sys, p)
		if c.state != cellFailed || c.inc != 0 || c.ctx != nil {
			t.Errorf("%s factory: state %d, inc %d, ctx %p; want cellFailed, 0, nil", name, c.state, c.inc, c.ctx)
		}
		if name == "nil" && !errors.Is(c.failure, errNilActor) {
			t.Errorf("nil factory: failure %v", c.failure)
		}
	}
}

// ENG-340: lockstep.turn ends every turn that processed a message, and only those.
func TestTurnRecord(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	pid, err := Spawn(sys, "a", idle[int]())
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, pid)
	c.RunTurn(1) // empty: no record
	send(t, sys, pid, 7)
	send(t, sys, pid, 8)
	eng.RunUntilIdle()

	got := recordsOf(eng.Records(), "lockstep.turn")
	want := [][]string{
		{"actor=" + pid.String(), "system=1", "user=0", "msg="},
		{"actor=" + pid.String(), "system=0", "user=2", "msg=" + sys.eng.Tracer.Describe(7)},
	}
	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.turn records:\n got %q\nwant %q", got, want)
	}
}

// ENG-340: a Receive that panics still leaves its message as the turn record's msg.
func TestTurnRecordPanickingReceive(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(_ *Context[int], _ int) { panic("boom") },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, pid, 7)
	eng.RunUntilIdle()
	turns := recordsOf(eng.Records(), "lockstep.turn")
	want := []string{"actor=" + pid.String(), "system=0", "user=1", "msg=" + sys.eng.Tracer.Describe(7)}
	if last := turns[len(turns)-1]; !slices.Equal(last, want) {
		t.Errorf("last lockstep.turn = %q, want %q", last, want)
	}
}

// logActor spawns "a" under sys, an int actor that appends every message to the returned log, and
// runs the engine until its start message is processed.
func logActor(t *testing.T, sys *System, eng *enginetest.Engine) (PID[int], *[]int) {
	t.Helper()
	var log []int
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(_ *Context[int], m int) { log = append(log, m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	return pid, &log
}

// AT-ENG-20: ENG-310, ENG-340. Budget 2: three turns for five messages, one record each.
func TestATENG20Budget(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(2)
	pid, log := logActor(t, sys, eng)
	for m := 1; m <= 5; m++ {
		send(t, sys, pid, m)
	}
	for i, want := range [][]int{{1, 2}, {1, 2, 3, 4}, {1, 2, 3, 4, 5}} {
		if !eng.Step() || !slices.Equal(*log, want) {
			t.Fatalf("Step %d: log %v, want %v", i+1, *log, want)
		}
	}
	if eng.Step() {
		t.Error("a fourth Step ran a turn")
	}
	a := "actor=" + pid.String()
	want := [][]string{
		{a, "system=1", "user=0", "msg="}, // the start turn
		{a, "system=0", "user=2", "msg=1"},
		{a, "system=0", "user=2", "msg=3"},
		{a, "system=0", "user=1", "msg=5"},
	}
	if got := recordsOf(eng.Records(), "lockstep.turn"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.turn records:\n got %q\nwant %q", got, want)
	}
}

// AT-ENG-22: ENG-350. Three sends before any Step queue one turn at a time.
func TestATENG22ReadyDeduplication(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, _ := logActor(t, sys, eng)
	send(t, sys, pid, 1)
	send(t, sys, pid, 2)
	send(t, sys, pid, 3)
	for i := 1; i <= 3; i++ {
		if !eng.Step() {
			t.Fatalf("Step %d returned false", i)
		}
	}
	if eng.Step() {
		t.Error("a fourth Step ran a turn: Ready was not deduplicated")
	}
	if st := cellOf(t, sys, pid).Slot().State(); st != engine.SlotIdle {
		t.Errorf("slot state %v at the end, want idle", st)
	}
}

// AT-ENG-23: a Ready call without work runs an empty turn that records nothing (ENG-340).
func TestATENG23EmptyTurn(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, _ := logActor(t, sys, eng)
	send(t, sys, pid, 1)
	if !eng.Step() {
		t.Fatal("Step after the send returned false")
	}
	before := len(recordsOf(eng.Records(), "lockstep.turn"))
	sys.eng.Scheduler.Ready(cellOf(t, sys, pid))
	if !eng.Step() {
		t.Fatal("Step after Ready returned false")
	}
	if after := len(recordsOf(eng.Records(), "lockstep.turn")); after != before {
		t.Errorf("the empty turn emitted a lockstep.turn record (%d -> %d)", before, after)
	}
}

// countingScheduler counts Ready calls for the actor at addr.
type countingScheduler struct {
	engine.Scheduler
	addr  engine.Address
	ready int
}

func (s *countingScheduler) Ready(a engine.ActorRef) {
	if a.Address() == s.addr {
		s.ready++
	}
	s.Scheduler.Ready(a)
}

// AT-ENG-28: ENG-350. The turn that follows a turn with more work comes from Slot.End, not Ready.
func TestATENG28NoReadyAfterMore(t *testing.T) {
	eng := enginetest.New("local", 1)
	b := eng.Engine()
	cs := &countingScheduler{Scheduler: b.Scheduler}
	b.Scheduler = cs
	sys, err := newSystem(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	pid, log := logActor(t, sys, eng)
	cs.addr, cs.ready = pid.Address().engine(), 0
	send(t, sys, pid, 1)
	send(t, sys, pid, 2)
	if cs.ready != 2 {
		t.Fatalf("Ready calls after two sends = %d, want 2", cs.ready)
	}
	if n := eng.RunUntilIdle(); n != 2 {
		t.Errorf("RunUntilIdle ran %d turns, want 2", n)
	}
	if cs.ready != 2 || !slices.Equal(*log, []int{1, 2}) {
		t.Errorf("Ready calls %d, log %v; want 2 and [1 2]", cs.ready, *log)
	}
}
