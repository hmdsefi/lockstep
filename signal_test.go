// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"slices"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/supervise"
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
// address in the watched set: ACT-630's skip rule drops a Terminated whose Addr is not in it.
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
		// begin records the start and Started's Watch the watch; begin and the signal are the two
		// system messages of the one turn. Nothing else is recorded for the ignored signal.
		if want := []string{"lockstep.start", "lockstep.watch", "lockstep.turn"}; !slices.Equal(kinds, want) {
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

// AT-MSG-32: MSG-800, MSG-810. Terminated, ChildFailed and Overloaded are Signals. When a watched
// t stops, an any actor without SignalReceiver gets nothing in Receive, and one with SignalReceiver
// gets the Terminated only in Signal. A watcher whose mailbox is at capacity with one queued
// message and that cannot run a turn still gets the Terminated once it runs again. The watcher is
// held rather than blocked in Receive: the local engine runs every turn on one goroutine, so a
// blocked Receive would also keep t from stopping.
func TestATMSG32Signals(t *testing.T) {
	var _ Signal = Terminated{}
	var _ Signal = ChildFailed{}
	var _ Signal = Overloaded{}

	sys := newLocalSystem(t)
	spawnIdle := func(name string) PID[any] {
		pid, err := Spawn(sys, name, idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}
	stopTarget := func(pid PID[any]) {
		if err := Stop(sys, pid.Address()); err != nil {
			t.Fatal(err)
		}
	}

	tp := spawnIdle("t")
	plainGot := make(chan any, 16)
	plain, err := Spawn(sys, "plain", actorOf[any](&fnActor[any]{
		started: func(ctx *Context[any]) {
			if err := Watch(ctx, tp.Address()); err != nil {
				panic(err)
			}
		},
		receive: func(_ *Context[any], m any) { plainGot <- m },
	}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, plain, any("started")) // queued behind begin: plain watches t before it gets this
	if m := recv(t, plainGot); m != "started" {
		t.Fatalf("plain received %v, want started", m)
	}
	recvGot := make(chan any, 16)
	w, wp := spawnProbe(t, sys, "receiver", func(_ *Context[any], m any) { recvGot <- m })
	watchFrom(t, sys, w, tp.Address())

	stopTarget(tp)
	if term := awaitTerminated(t, wp, tp.Address()); term.Reason != Normal {
		t.Errorf("Terminated reason %v, want normal", term.Reason)
	}
	// Each watcher's Terminated was queued before the receiver got its own: a message sent now
	// comes after it, and is the first thing either Receive gets.
	send(t, sys, plain, any("after"))
	send(t, sys, w, any("after"))
	if m := recv(t, plainGot); m != "after" {
		t.Errorf("plain's Receive got %v, want only after", m)
	}
	if m := recv(t, recvGot); m != "after" {
		t.Errorf("the receiver's Receive got %v, want only after", m)
	}

	// The full watcher.
	t3 := spawnIdle("t3")
	fullGot := make(chan any, 16)
	fp := &probe[any]{receive: func(_ *Context[any], m any) { fullGot <- m }, signals: make(chan Signal, probeSignals)}
	full, err := Spawn(sys, "full", actorOf[any](fp), WithMailbox(1, FailSend))
	if err != nil {
		t.Fatal(err)
	}
	watchFrom(t, sys, full, t3.Address())
	watchFrom(t, sys, w, t3.Address())
	release := hold(t, sys, full.Address())
	t.Cleanup(release)
	send(t, sys, full, any("queued"))
	if err := Send(sys, full, any("refused")); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("Send to the full watcher = %v, want ErrMailboxFull", err)
	}
	stopTarget(t3)
	awaitTerminated(t, wp, t3.Address()) // t3 has stopped; full's Terminated is queued
	release()
	awaitTerminated(t, fp, t3.Address())
	if m := recv(t, fullGot); m != "queued" {
		t.Errorf("the full watcher's Receive got %v, want queued", m)
	}
}

// SUP-060 step 5, SUP-320, ACT-630: a signal that reaches a cell outside Running is recorded as
// lockstep.drop reason not-running, except a Terminated that a delivery to the incarnation that
// ended would skip (its address is not in that incarnation's watched set, or is there under another
// Watch call's sequence number), which is discarded without a record. In its turn A asks for its
// own stop and then watches a not-alive address, whose Terminated is queued behind the stop; with
// Unwatch it cancels that watch. The Terminated reaches the stop's drain (no child), or the
// Stopping cell while it waits for its child. In Restarting, the Terminated of a current watch is
// recorded and one under another sequence number is not.
func TestNotRunningSkipsUnwatchedTerminated(t *testing.T) {
	term := Terminated{Addr: gone(), Reason: NoActor}
	for _, tc := range []struct {
		name           string
		child, unwatch bool
		wantNotRunning bool
	}{
		{"drain, watched", false, false, true},
		{"drain, unwatched", false, true, false},
		{"stopping, watched", true, false, true},
		{"stopping, unwatched", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sys, eng := newManualSystem(t)
			a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
				started: func(ctx *Context[any]) {
					if tc.child {
						if _, err := Spawn(ctx, "c", idle[any]()); err != nil {
							panic(err)
						}
					}
				},
				receive: func(ctx *Context[any], _ any) {
					if err := Stop(ctx, ctx.Self().Address()); err != nil {
						panic(err)
					}
					if err := Watch(ctx, term.Addr); err != nil {
						panic(err)
					}
					if tc.unwatch {
						Unwatch(ctx, term.Addr)
					}
				},
			}))
			if err != nil {
				t.Fatal(err)
			}
			send(t, sys, a, any("go"))
			eng.RunUntilIdle()
			if recordIndex(eng.Records(), "lockstep.stop", "actor="+a.String()) < 0 {
				t.Fatal("a did not stop")
			}
			var want [][]string
			if tc.wantNotRunning {
				want = append(want, []string{"to=" + a.String(), "reason=not-running", "msg=" + sys.eng.Tracer.Describe(term)})
			}
			sameRecords(t, recordsOf(eng.Records(), "lockstep.drop"), "lockstep.drop", want...)
		})
	}

	t.Run("restarting", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		tp, err := Spawn(sys, "t", idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{receive: func(ctx *Context[any], m any) {
			switch m {
			case "watch":
				if err := Watch(ctx, tp.Address()); err != nil {
					panic(err)
				}
			case "fail":
				ctx.Fail(errX)
			}
		}}))
		if err != nil {
			t.Fatal(err)
		}
		send(t, sys, a, any("watch"))
		eng.RunUntilIdle()
		c := cellOf(t, sys, a)
		seq := c.watchSeq
		send(t, sys, a, any("fail"))
		stepUntilSuspended(t, eng, c)
		release := hold(t, sys, a.Address())
		eng.RunUntilIdle() // the user guardian decides the restart: end waits in a's system queue
		if err := Stop(sys, tp.Address()); err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle() // t's Terminated, of a's current watch, queues behind end
		// A Terminated for t under another Watch call's sequence number, which a delivery skips.
		c.pushSystem(signalMsg{sig: Terminated{Addr: tp.Address(), Reason: Normal}, seq: seq + 1}, nil)
		release()
		eng.RunUntilIdle()
		if st, _ := c.lifecycle(); st != supervise.Restarting {
			t.Fatalf("a is %v, want restarting: its restart waits for the delay", st)
		}
		sameRecords(t, recordsOf(eng.Records(), "lockstep.drop"), "lockstep.drop",
			[]string{"to=" + a.String(), "reason=not-running", "msg=" + sys.eng.Tracer.Describe(Terminated{Addr: tp.Address(), Reason: Normal})})
		eng.Advance(defaultMinBackoff)
		eng.RunUntilIdle()
		if st, inc := c.lifecycle(); st != supervise.Running || inc != 2 {
			t.Errorf("a is %v in incarnation %d, want running in 2", st, inc)
		}
	})
}
