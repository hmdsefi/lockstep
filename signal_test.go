// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// sigActor is an any actor that logs, in order, the strings it receives ("recv <m>") and the
// signals it gets ("signal"), keeping the signals. onRecv, if set, runs after Receive's log entry.
type sigActor struct {
	log    []string
	sigs   []Signal
	onRecv func(ctx *Context[any], m string)
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
// The test pushes the Terminated straight to the system queue (pushSignal). Task 1.2-10 changes it
// to obtain the Terminated through Watch of a not-alive PIDAt address, because ACT-630's skip rule
// drops a Terminated whose Addr is not in the watched set (ruling B6).
func TestSignalDelivery(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	term := Terminated{Addr: Address{node: "local", path: "/user/gone", uid: 5}, Reason: NoActor}

	t.Run("receiver", func(t *testing.T) {
		a := &sigActor{}
		pid, c := spawnSigActor(t, sys, eng, "recv", a)
		send(t, sys, pid, any("u1"))
		send(t, sys, pid, any("u2"))
		c.pushSignal(term)
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
		pid, c := spawnSigActor(t, sys, eng, "mid", a)
		a.onRecv = func(_ *Context[any], m string) {
			if m == "u1" {
				c.pushSignal(term)
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
			receive: func(_ *Context[any], m any) { got = append(got, m) },
		}))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		c := cellOf(t, sys, pid)
		before := len(eng.Records())
		c.pushSignal(term)
		eng.RunUntilIdle()
		for _, r := range eng.Records()[before:] {
			if r.Kind != "lockstep.turn" {
				t.Errorf("the ignored signal emitted a %s record", r.Kind)
			}
		}
		send(t, sys, pid, any("after"))
		eng.RunUntilIdle()
		if len(got) != 1 || got[0] != "after" {
			t.Errorf("Receive got %v, want [after]: a signal must never reach Receive", got)
		}
	})
}
