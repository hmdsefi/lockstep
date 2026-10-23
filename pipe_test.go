// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/supervise"
)

// Balance is the message the pipe tests turn a reply into: the answer or the error of an ask.
type Balance struct {
	V   int64
	Err error
}

// String formats b for a test log.
func (b Balance) String() string { return fmt.Sprintf("Balance{%d, %v}", b.V, b.Err) }

// toBalance is the to function of the pipe tests.
func toBalance(v int64, err error) any { return Balance{v, err} }

// pipeActor spawns name, an any actor that appends to log every string it receives and
// "Receive(<Balance>)" for every Balance. On the string "go" it calls start. Pipes that start makes
// with logTo append "to" to log when to runs.
func pipeActor(t *testing.T, sys *System, name string, log *[]string, start func(ctx *Context[any], logTo func(int64, error) any)) PID[any] {
	t.Helper()
	logTo := func(v int64, err error) any {
		*log = append(*log, "to")
		return toBalance(v, err)
	}
	pid, err := Spawn(sys, name, actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			switch m := m.(type) {
			case string:
				*log = append(*log, m)
				if m == "go" {
					start(ctx, logTo)
				}
			case Balance:
				*log = append(*log, "Receive("+m.String()+")")
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// queued returns the number of entries in c's user mailbox.
func queued[M any](c *cell[M]) int {
	return c.user.Len()
}

// pipeRun is the outcome of runATMSG24.
type pipeRun struct {
	log   []string
	a     PID[any]
	sys   *System
	turns [][]string // the lockstep.turn records
}

// runATMSG24 plays AT-MSG-24 on a manual engine with the given turn budget: A asks acct (balance
// 30) and pipes the answer; x is queued for A before the answer arrives and y after it.
func runATMSG24(t *testing.T, budget int) pipeRun {
	t.Helper()
	sys, eng := newManualSystem(t)
	eng.SetBudget(budget)
	acct := spawnAcct(t, sys, nil)
	var run pipeRun
	run.sys = sys
	run.a = pipeActor(t, sys, "a", &run.log, func(ctx *Context[any], to func(int64, error) any) {
		Pipe(ctx, Ask(ctx, acct, getBalance), to)
	})
	send(t, sys, acct, acctMsg(Deposit{30}))
	eng.RunUntilIdle()
	send(t, sys, run.a, any("go"))
	eng.Step() // A asks; the request waits in acct's mailbox
	send(t, sys, run.a, any("x"))
	eng.Step() // acct answers: the piped reply joins A's mailbox behind x
	send(t, sys, run.a, any("y"))
	eng.RunUntilIdle()
	run.turns = recordsOf(eng.Records(), "lockstep.turn")
	return run
}

// AT-MSG-24: MSG-600, MSG-620. A pipes an ask of acct's balance. The log of to and Receive reads
// to, Receive(Balance{30, nil}) in one turn, with no other Receive between them. The message of the
// lockstep.turn record of a piped entry is the Balance Receive got (ENG-340).
func TestATMSG24Pipe(t *testing.T) {
	t.Run("one turn", func(t *testing.T) {
		run := runATMSG24(t, 32)
		want := []string{"go", "x", "to", "Receive(Balance{30, <nil>})", "y"}
		if !slices.Equal(run.log, want) {
			t.Errorf("log = %q, want %q", run.log, want)
		}
		// x, the piped reply and y are three user messages of one turn.
		wantTurn := []string{"actor=" + run.a.String(), "system=0", "user=3", "msg=" + run.sys.eng.Tracer.Describe(any("x"))}
		if last := run.turns[len(run.turns)-1]; !slices.Equal(last, wantTurn) {
			t.Errorf("last lockstep.turn = %q, want %q", last, wantTurn)
		}
	})
	t.Run("turn record", func(t *testing.T) {
		run := runATMSG24(t, 1)
		want := []string{"actor=" + run.a.String(), "system=0", "user=1", "msg=" + run.sys.eng.Tracer.Describe(Balance{30, nil})}
		if !slices.ContainsFunc(run.turns, func(r []string) bool { return slices.Equal(r, want) }) {
			t.Errorf("no lockstep.turn record %q in %q", want, run.turns)
		}
	})
}

// AT-MSG-25, the Pipe half: MSG-600, MSG-610. A reply resolved with ErrTerminated at Ask (a dead
// letter through a PIDAt) is piped. The continuation runs during Pipe and enqueues then, but A
// receives the Balance in a later turn, not during Pipe's.
func TestPipeResolvedReply(t *testing.T) {
	for _, budget := range []int{1, 32} {
		t.Run(fmt.Sprintf("budget %d", budget), func(t *testing.T) {
			sys, eng := newManualSystem(t)
			eng.SetBudget(budget)
			ghost := PIDAt[acctMsg]("local", "/user/ghost")
			var log []string
			var got []Balance
			a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
				receive: func(ctx *Context[any], m any) {
					switch m := m.(type) {
					case string:
						Pipe(ctx, Ask(ctx, ghost, getBalance), func(v int64, err error) any {
							log = append(log, "to")
							return toBalance(v, err)
						})
						log = append(log, "Pipe returned")
					case Balance:
						log = append(log, "Receive")
						got = append(got, m)
					}
				},
			}))
			if err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle()
			c := cellOf(t, sys, a)

			send(t, sys, a, any("go"))
			eng.Step()
			if want := []string{"Pipe returned"}; budget == 1 && !slices.Equal(log, want) {
				t.Fatalf("log after Pipe's turn = %q, want %q", log, want) // the next turn delivers
			}
			eng.RunUntilIdle()
			if want := []string{"Pipe returned", "to", "Receive"}; !slices.Equal(log, want) {
				t.Errorf("log = %q, want %q: to ran during Pipe", log, want)
			}
			if len(got) != 1 || got[0].V != 0 || !errors.Is(got[0].Err, ErrTerminated) {
				t.Errorf("received %v, want Balance{0, ErrTerminated}", got)
			}
			if n := queued(c); n != 0 {
				t.Errorf("%d entries left in the mailbox", n)
			}
		})
	}
	// MSG-600: the continuation of a resolved reply enqueues during the Pipe call.
	t.Run("enqueued by Pipe", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var n int
		var c *cell[any]
		a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
			receive: func(ctx *Context[any], m any) {
				if _, ok := m.(string); ok {
					c = cellOf(t, sys, ctx.Self())
					Pipe(ctx, resolvedReply[int64](sys, ErrTimeout), toBalance)
					n = queued(c)
				}
			},
		}))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		send(t, sys, a, any("go"))
		eng.RunUntilIdle()
		if n != 1 {
			t.Errorf("%d entries queued when Pipe returned, want 1", n)
		}
	})
}

// MSG-610: a piped reply that a local ReplyTo.Send enqueues follows the replier's earlier messages
// to the asker and precedes its later ones.
func TestPipeLocalOrder(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	var log []string
	var a PID[any]
	replier, err := Spawn(sys, "replier", actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			rt, ok := m.(ReplyTo[int64])
			if !ok {
				return
			}
			_ = Send(ctx, a, any("m1"))
			rt.Send(ctx, 5)
			_ = Send(ctx, a, any("m2"))
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	a = pipeActor(t, sys, "a", &log, func(ctx *Context[any], to func(int64, error) any) {
		Pipe(ctx, Ask(ctx, replier, func(rt ReplyTo[int64]) any { return rt }), to)
	})
	eng.RunUntilIdle()
	send(t, sys, a, any("go"))
	eng.RunUntilIdle()
	want := []string{"go", "m1", "to", "Receive(Balance{5, <nil>})", "m2"}
	if !slices.Equal(log, want) {
		t.Errorf("log = %q, want %q", log, want)
	}
}

// AT-MSG-27: MSG-600, MSG-630. The four Pipe panics, in MSG-600's order, and fan-out: several Pipe
// calls on one reply, from one actor or several, each deliver a message, in Pipe order.
func TestATMSG27PipeMisuseAndFanOut(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	sys2, _ := newManualSystem(t)
	_, foreign := NewReply[int64](sys2)

	var panics []any
	var saved *Context[any]
	var nilCtx *Context[any]
	var shared *Reply[int64]
	var logA, logB []string
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			switch m {
			case "misuse":
				saved = ctx
				_, r := NewReply[int64](ctx)
				panics = append(panics,
					mustPanic(t, func() { Pipe[int64, any](ctx, nil, toBalance) }),
					mustPanic(t, func() { Pipe[int64, any](ctx, r, nil) }),
					mustPanic(t, func() { Pipe(nilCtx, r, toBalance) }),
					mustPanic(t, func() { Pipe(ctx, foreign, toBalance) }),
					// MSG-600's order: ctx before r before to.
					mustPanic(t, func() { Pipe[int64, any](nilCtx, nil, nil) }),
					mustPanic(t, func() { Pipe[int64, any](ctx, nil, nil) }),
				)
			case "fan":
				Pipe(ctx, shared, func(v int64, err error) any { logA = append(logA, "a1"); return toBalance(v, err) })
				Pipe(ctx, shared, func(v int64, err error) any { logA = append(logA, "a2"); return toBalance(v, err) })
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Spawn(sys, "b", actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			if m == "fan" {
				Pipe(ctx, shared, func(v int64, err error) any { logB = append(logB, "b"); return toBalance(v, err) })
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	send(t, sys, a, any("misuse"))
	eng.RunUntilIdle()
	want := []any{
		"lockstep: nil Reply",
		"lockstep: Pipe with nil function",
		"lockstep: Pipe with nil Context",
		"lockstep: Pipe of a Reply from another system",
		"lockstep: Pipe with nil Context",
		"lockstep: nil Reply",
	}
	if !slices.Equal(panics, want) {
		t.Errorf("panics:\n got %q\nwant %q", panics, want)
	}
	// A Context outside its turn: ACT-120's panic comes before the other-system check.
	outside := mustPanic(t, func() { Pipe(saved, foreign, toBalance) })
	if want := "lockstep: Context of " + a.String() + " used outside its turn"; outside != want {
		t.Errorf("Pipe outside the turn panicked with %v, want %q", outside, want)
	}
	if f := eng.Failures(); len(f) != 0 || cellOf(t, sys, a).state != supervise.Running {
		t.Errorf("the misuse panics leaked: failures %v, state %d", f, cellOf(t, sys, a).state)
	}

	// Fan-out: a pipes twice, b once; the answer enqueues all three before Send returns.
	var rt ReplyTo[int64]
	rt, shared = NewReply[int64](sys)
	send(t, sys, a, any("fan"))
	send(t, sys, b, any("fan"))
	eng.RunUntilIdle()
	rt.Send(sys, 7)
	if na, nb := queued(cellOf(t, sys, a)), queued(cellOf(t, sys, b)); na != 2 || nb != 1 {
		t.Errorf("queued piped replies after Send: a %d, b %d; want 2, 1", na, nb)
	}
	eng.RunUntilIdle()
	if !slices.Equal(logA, []string{"a1", "a2"}) || !slices.Equal(logB, []string{"b"}) {
		t.Errorf("to calls: a %q, b %q; want [a1 a2] and [b]", logA, logB)
	}
}

// MSG-620: the runtime discards a piped reply that a different incarnation dequeues, calls neither
// to nor Receive, and emits lockstep.pipe.drop. The entry is built by hand with another
// incarnation's number.
func TestPipeDropIncarnation(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	var log []string
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
		receive: func(_ *Context[any], m any) { log = append(log, fmt.Sprint(m)) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, a)
	ask := Address{node: "local", path: "/temp/9", uid: 9}
	called := false
	c.offerUser(entry{
		msg:  pipedReply[any]{ask: ask, call: func() any { called = true; return "piped" }},
		kind: entrySelf,
		inc:  c.inc + 1,
	}, nil)
	send(t, sys, a, any("after"))
	eng.RunUntilIdle()

	if called || !slices.Equal(log, []string{"after"}) {
		t.Errorf("to called %v, Receive log %q; want neither for the old incarnation", called, log)
	}
	want := []string{"addr=" + a.String(), "ask=" + ask.String(), "reason=incarnation"}
	if got := recordsOf(eng.Records(), "lockstep.pipe.drop"); len(got) != 1 || !slices.Equal(got[0], want) {
		t.Errorf("lockstep.pipe.drop records = %q, want [%q]", got, want)
	}
	// The discarded entry counts as a user message of the turn and has no description (ENG-340); the
	// turn's first user message is the discarded one.
	turns := recordsOf(eng.Records(), "lockstep.turn")
	if last := turns[len(turns)-1]; !slices.Equal(last, []string{"actor=" + a.String(), "system=0", "user=2", "msg="}) {
		t.Errorf("last lockstep.turn = %q", last)
	}
}

// MSG-620: a panic in to is a failure of Receive (SUP-150). Receive is not called, the in-turn flag
// is cleared and the Suspended cell pops no further user message.
func TestPipeToPanics(t *testing.T) {
	sys, eng := newManualSystem(t)
	var received []any
	var ctx1 *Context[any]
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			if m != "go" {
				received = append(received, m)
				return
			}
			ctx1 = ctx
			rt, r := NewReply[int](ctx)
			Pipe(ctx, r, func(int, error) any { panic("to boom") })
			rt.Send(ctx, 1)
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, a, any("go"))
	eng.Step() // the piped reply is queued
	send(t, sys, a, any("later"))
	eng.RunUntilIdle()

	c := cellOf(t, sys, a)
	fails := recordsOf(eng.Records(), "lockstep.fail")
	want := []string{"actor=" + a.String(), "inc=1", "source=receive", "err=lockstep: panic: to boom"}
	// The user guardian restarts the failed actor after the default backoff, which the manual
	// clock never reaches: the incarnation has ended and "later" waits for the next one.
	if c.state != supervise.Restarting || len(fails) != 1 || !slices.Equal(fails[0], want) {
		t.Errorf("state %v, lockstep.fail %q; want restarting and [%q]", c.state, fails, want)
	}
	if len(received) != 0 || ctx1.inTurn.Load() || queued(c) != 1 {
		t.Errorf("Receive got %v; in-turn flag %v; queued %d, want 1", received, ctx1.inTurn.Load(), queued(c))
	}
}

// MSG-610 on the local engine: Pipe races with the resolution, which runs the continuations on
// another goroutine (run under -race). Every actor receives exactly one piped reply, whether its
// Pipe came before or after the resolution.
func TestPipeLocalEngineRace(t *testing.T) {
	sys := newLocalSystem(t)
	const n = 40
	rt, shared := NewReply[int64](sys, WithTimeout(time.Minute))
	got := make(chan string, 2*n) // the receiving actor's name and the Balance
	pids := make([]PID[any], n)
	for i := range pids {
		var err error
		pids[i], err = Spawn(sys, fmt.Sprintf("a%d", i), actorOf[any](&fnActor[any]{
			receive: func(ctx *Context[any], m any) {
				switch m := m.(type) {
				case string:
					Pipe(ctx, shared, toBalance)
				case Balance:
					got <- ctx.Self().String() + " " + m.String()
				}
			},
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // resolves while the actors are piping
		defer wg.Done()
		rt.Send(sys, 3)
	}()
	for _, p := range pids {
		send(t, sys, p, any("go"))
	}
	wg.Wait()
	seen := make(map[string]bool)
	for range n {
		seen[recv(t, got)] = true
	}
	for _, p := range pids {
		if !seen[p.String()+" Balance{3, <nil>}"] {
			t.Errorf("%v received no piped Balance{3, <nil>}", p)
		}
	}
	select {
	case extra := <-got:
		t.Errorf("an actor received a second piped reply: %s", extra)
	case <-time.After(50 * time.Millisecond):
	}
}

// MSG-610: another actor answers on its own turn and the piped reply reaches the asker, on the
// local engine.
func TestPipeFromAnotherActor(t *testing.T) {
	sys := newLocalSystem(t)
	silent, stored := spawnSilent(t, sys)
	out := make(chan Balance, 1)
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			switch m := m.(type) {
			case string:
				Pipe(ctx, askSilent(ctx, silent), func(v int, err error) any { return toBalance(int64(v), err) })
			case Balance:
				out <- m
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, a, any("go"))
	recv(t, stored) // silent holds the request: the ask is pending
	send(t, sys, silent, any(answer{9}))
	if b := recv(t, out); b.V != 9 || b.Err != nil {
		t.Errorf("received %v, want Balance{9, nil}", b)
	}
}
