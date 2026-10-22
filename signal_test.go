// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// sigActor is an any actor that logs, in order, the strings it receives ("recv <m>") and the
// signals it gets ("signal"), keeping the signals. onStart, if set, runs in Started, and onRecv
// after Receive's log entry.
type sigActor struct {
	log     []string
	sigs    []Signal
	onStart func(ctx *Context[any])
	onRecv  func(ctx *Context[any], m string)
}

func (a *sigActor) Started(ctx *Context[any]) {
	if a.onStart != nil {
		a.onStart(ctx)
	}
}

func (a *sigActor) Receive(ctx *Context[any], m any) {
	s, _ := m.(string)
	a.log = append(a.log, "recv "+s)
	if a.onRecv != nil {
		a.onRecv(ctx, s)
	}
}

func (a *sigActor) Signal(_ *Context[any], sig Signal) {
	a.log = append(a.log, "signal")
	a.sigs = append(a.sigs, sig)
}

// spawnSigActor spawns a sigActor and runs the engine until it started.
func spawnSigActor(t *testing.T, sys *System, eng *enginetest.Engine, name string, a *sigActor) (PID[any], *cell[any]) {
	t.Helper()
	pid, err := Spawn(sys, name, actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	return pid, cellOf(t, sys, pid)
}

// MSG-810, ENG-310 step 1. A signal pushed to the system queue reaches Signal of a SignalReceiver,
// never Receive, before the user messages queued ahead of it and also when it arrives during a turn,
// before the remaining ones; an actor without SignalReceiver ignores it without a record.
//
// The test obtains its Terminated through Watch of a not-alive PIDAt address, which puts the
// address in the watched set: ACT-630's skip rule drops a Terminated whose Addr is not in it
// (ruling B6).
func TestSignalDelivery(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	term := Terminated{Addr: gone(), Reason: NoActor}
	watchGone := func(ctx *Context[any]) {
		if err := Watch(ctx, term.Addr); err != nil {
			t.Errorf("Watch: %v", err)
		}
	}

	t.Run("receiver", func(t *testing.T) {
		// Started watches; the user messages queued before the engine runs wait for the begin
		// message, so the signal is queued ahead of them in the turn that starts the actor.
		a := &sigActor{onStart: watchGone}
		pid, err := Spawn(sys, "recv", actorOf[any](a))
		if err != nil {
			t.Fatal(err)
		}
		send(t, sys, pid, any("u1"))
		send(t, sys, pid, any("u2"))
		eng.RunUntilIdle()
		if want := []string{"signal", "recv u1", "recv u2"}; !slices.Equal(a.log, want) {
			t.Errorf("log = %q, want %q: the signal must come before the queued user messages", a.log, want)
		}
		if len(a.sigs) != 1 || a.sigs[0] != Signal(term) {
			t.Errorf("signals = %v, want [%v]", a.sigs, term)
		}
	})

	t.Run("during a turn", func(t *testing.T) {
		a := &sigActor{}
		pid, _ := spawnSigActor(t, sys, eng, "mid", a)
		a.onRecv = func(ctx *Context[any], m string) {
			if m == "u1" {
				watchGone(ctx)
			}
		}
		send(t, sys, pid, any("u1"))
		send(t, sys, pid, any("u2"))
		eng.RunUntilIdle()
		if want := []string{"recv u1", "signal", "recv u2"}; !slices.Equal(a.log, want) {
			t.Errorf("log = %q, want %q: ENG-310 step 1 runs the signal before the remaining messages", a.log, want)
		}
	})

	t.Run("no SignalReceiver", func(t *testing.T) {
		var got []any
		pid, err := Spawn(sys, "plain", actorOf[any](&fnActor[any]{
			started: watchGone,
			receive: func(_ *Context[any], m any) { got = append(got, m) },
		}))
		if err != nil {
			t.Fatal(err)
		}
		before := len(eng.Records())
		eng.RunUntilIdle()
		var kinds []string
		for _, r := range eng.Records()[before:] {
			kinds = append(kinds, r.Kind)
		}
		// Started's Watch records the watch; begin and the signal are the two system messages of
		// the one turn. Nothing else is recorded for the ignored signal.
		if want := []string{"lockstep.watch", "lockstep.turn"}; !slices.Equal(kinds, want) {
			t.Errorf("the ignored signal emitted records %q, want only %q", kinds, want)
		}
		turns := recordsOf(eng.Records()[before:], "lockstep.turn")
		if want := []string{"actor=" + pid.String(), "system=2", "user=0", "msg="}; len(turns) != 1 || !slices.Equal(turns[0], want) {
			t.Errorf("turn record of the ignored signal = %q, want %q", turns, want)
		}
		send(t, sys, pid, any("after"))
		eng.RunUntilIdle()
		if len(got) != 1 || got[0] != "after" {
			t.Errorf("Receive got %v, want [after]: a signal must never reach Receive", got)
		}
	})
}
