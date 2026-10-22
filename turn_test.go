// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"slices"
	"strings"
	"testing"
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
