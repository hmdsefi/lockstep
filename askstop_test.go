// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// terminatedText is MSG §7's ErrTerminated message for the target addr.
func terminatedText(addr fmt.Stringer) string { return "lockstep: actor terminated: " + addr.String() }

// AT-MSG-13: MSG-310. An ask to the PID of a stopped actor resolves with ErrTerminated before
// Ask returns, long before its timer.
func TestATMSG13AskDeadActor(t *testing.T) {
	sys := newLocalSystem(t)
	w, wp := spawnProbe[any](t, sys, "w", nil)
	d, err := Spawn(sys, "d", idle[acctMsg]())
	if err != nil {
		t.Fatal(err)
	}
	stopAndWait(t, sys, w, wp, d.Address())
	r := Ask(sys, d, getBalance, WithTimeout(time.Hour))
	if !r.resolved.Load() {
		t.Error("the ask to a stopped actor is still pending when Ask returns")
	}
	v, err := r.Await(within(t, 10*time.Second))
	if v != 0 || !errors.Is(err, ErrTerminated) || err.Error() != terminatedText(d) {
		t.Errorf("Await = %d, %v, want 0 and ErrTerminated with text %q", v, err, terminatedText(d))
	}
}

// AT-MSG-16 (a): MSG-340, MSG-430. The target holds the ask's ReplyTo and stops: the ask
// resolves with ErrTerminated from the target's address, recorded once.
func TestATMSG16TerminatedOnStop(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	silent, stored := spawnSilent(t, sys)
	r := askSilent(sys, silent, WithTimeout(time.Hour))
	waitFor(t, stored) // silent holds the ReplyTo: the ask is tracked
	if err := Stop(sys, silent.Address()); err != nil {
		t.Fatal(err)
	}
	v, err := r.Await(within(t, 10*time.Second))
	if v != 0 || !errors.Is(err, ErrTerminated) || err.Error() != terminatedText(silent) {
		t.Errorf("Await = %d, %v, want 0 and ErrTerminated with text %q", v, err, terminatedText(silent))
	}
	want := []string{"ask=" + r.ask.String(), "from=" + silent.String(), "outcome=resolved", "err=" + terminatedText(silent)}
	if got := askRecords(rec.Records(), "lockstep.reply", r.ask); len(got) != 1 || !slices.Equal(got[0], want) {
		t.Errorf("lockstep.reply records = %q, want one %q", got, want)
	}
}

// MSG-340, ruling B8: an ask whose request is still queued when its target stops fails once,
// through the tracked asks, and the request is not delivered.
func TestStopQueuedAsk(t *testing.T) {
	sys, eng := newManualSystem(t)
	var received []any
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{receive: func(_ *Context[any], m any) { received = append(received, m) }}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	r := askSilent(sys, a, WithTimeout(time.Hour)) // queued and tracked, not processed
	c := cellOf(t, sys, a)
	if got := sys.asks.trackedBy(c); len(got) != 1 {
		t.Fatalf("a tracks %v, want the ask", got)
	}
	if err := Stop(sys, a.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if v, err := r.Await(context.Background()); v != 0 || !errors.Is(err, ErrTerminated) || err.Error() != terminatedText(a) {
		t.Errorf("Await = %d, %v, want 0 and ErrTerminated with text %q", v, err, terminatedText(a))
	}
	if got := askRecords(eng.Records(), "lockstep.reply", r.ask); len(got) != 1 || !slices.Contains(got[0], "outcome=resolved") {
		t.Errorf("lockstep.reply records = %q, want one resolution", got)
	}
	if len(received) != 0 {
		t.Errorf("a received %v", received)
	}
	if got := sys.asks.trackedBy(c); len(got) != 0 {
		t.Errorf("a still tracks %v after its stop", got)
	}
}

// AT-MSG-17 (stop clause): MSG-340, MSG-410. After B answered the ask A forwarded, stopping A
// fails nothing: no lockstep.reply outcome=late follows.
func TestATMSG17ForwardingStop(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	w, wp := spawnProbe[any](t, sys, "w", nil)
	b, err := Spawn(sys, "b", actorOf[fwdMsg](&fnActor[fwdMsg]{
		receive: func(ctx *Context[fwdMsg], m fwdMsg) { m.rt.Send(ctx, 7) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	a, err := Spawn(sys, "a", actorOf[fwdMsg](&fnActor[fwdMsg]{
		receive: func(ctx *Context[fwdMsg], m fwdMsg) {
			if err := Send(ctx, b, m); err != nil {
				panic(err)
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var rt ReplyTo[int]
	r := Ask(sys, a, func(x ReplyTo[int]) fwdMsg { rt = x; return fwdMsg{x} })
	if v, err := r.Await(within(t, 10*time.Second)); v != 7 || err != nil {
		t.Fatalf("Await = %d, %v, want 7, nil", v, err)
	}
	stopAndWait(t, sys, w, wp, a.Address())
	if got := askRecords(rec.Records(), "lockstep.reply", rt.addr); len(got) != 1 || !slices.Contains(got[0], "outcome=resolved") {
		t.Errorf("lockstep.reply records = %q, want only the resolution by b", got)
	}
	if i := recordIndex(rec.Records(), "lockstep.stop", "actor="+a.String()); i < 0 {
		t.Error("no lockstep.stop record for a")
	}
}

// AT-MSG-25: MSG-600, MSG-610. A pipes an ask to a dead actor's PID, already resolved with
// ErrTerminated at Ask: the piped reply arrives in a later turn.
func TestATMSG25PipeResolvedReply(t *testing.T) {
	sys, eng := newManualSystem(t)
	d, err := Spawn(sys, "d", idle[acctMsg]())
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, d.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	var log []string
	var got Balance
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			switch m := m.(type) {
			case string:
				r := Ask(ctx, d, getBalance, WithTimeout(time.Hour))
				if !r.resolved.Load() {
					t.Error("the ask to a dead actor is pending after Ask")
				}
				Pipe(ctx, r, toBalance)
				log = append(log, "go returns")
			case Balance:
				got = m
				log = append(log, "Balance")
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, a, any("go"))
	eng.RunUntilIdle()
	if !slices.Equal(log, []string{"go returns", "Balance"}) {
		t.Errorf("log %q, want the Balance after the turn that piped", log)
	}
	if got.V != 0 || !errors.Is(got.Err, ErrTerminated) || got.Err.Error() != terminatedText(d) {
		t.Errorf("received %v, want Balance{0, ErrTerminated of %v}", got, d)
	}
}

// MSG-610, m23: a continuation that fires after its actor completed its final stop enqueues
// nothing and records lockstep.pipe.drop.
func TestPipeAfterStop(t *testing.T) {
	sys, eng := newManualSystem(t)
	silent, stored := spawnSilent(t, sys)
	var r *Reply[int]
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			if m == "go" {
				r = askSilent(ctx, silent, WithTimeout(time.Hour))
				Pipe(ctx, r, func(v int, err error) any { return toBalance(int64(v), err) })
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, a, any("go"))
	eng.RunUntilIdle()
	<-stored
	c := cellOf(t, sys, a)
	if err := Stop(sys, a.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, silent, any(answer{5}))
	eng.RunUntilIdle()

	want := []string{"addr=" + a.String(), "ask=" + r.ask.String(), "reason=incarnation"}
	if got := recordsOf(eng.Records(), "lockstep.pipe.drop"); len(got) != 1 || !slices.Equal(got[0], want) {
		t.Errorf("lockstep.pipe.drop records = %q, want [%q]", got, want)
	}
	if n := queued(c); n != 0 {
		t.Errorf("the stopped actor's mailbox holds %d entries", n)
	}
}

// MBX-150 (b), carry items 3 and 4: a request offered to a closed mailbox is a dead letter; its
// ask resolves with ErrTerminated and leaves no tracked entry behind.
func TestOfferClosedMailbox(t *testing.T) {
	sys, eng := newManualSystem(t)
	a, err := Spawn(sys, "a", idle[any]())
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, a)
	c.mu.Lock()
	c.closed = true // the window between the router's lookup and the final stop's drain
	c.mu.Unlock()

	if o := sys.route(routeRequest{from: sys.user, to: a.Address(), msg: "x"}); o.kind != routeDead || o.reason != reasonStopped {
		t.Errorf("route to a closed mailbox = %+v, want a stopped dead letter", o)
	}
	r := askSilent(sys, a, WithTimeout(time.Hour))
	if v, err := r.Await(context.Background()); v != 0 || !errors.Is(err, ErrTerminated) || err.Error() != terminatedText(a) {
		t.Errorf("Await = %d, %v, want 0 and ErrTerminated with text %q", v, err, terminatedText(a))
	}
	sys.asks.mu.Lock()
	tracked, lists := len(sys.asks.tracked), len(sys.asks.byActor)
	sys.asks.mu.Unlock()
	if tracked != 0 || lists != 0 {
		t.Errorf("%d tracked asks and %d per-actor lists after a refused request, want none", tracked, lists)
	}
}

// MSG-340, carry item 4: a request whose ask resolved before it was routed (its builder leaked
// the ReplyTo) is not tracked, so the target's stop issues no late reply attempt.
func TestTrackOnlyPendingAsks(t *testing.T) {
	sys, eng := newManualSystem(t)
	a, err := Spawn(sys, "a", idle[any]())
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	r := Ask(sys, a, func(rt ReplyTo[int]) any {
		rt.Send(sys, 1) // resolves the ask before it is routed
		return "req"
	}, WithTimeout(time.Hour))
	if v, err := r.Await(context.Background()); v != 1 || err != nil {
		t.Fatalf("Await = %d, %v, want 1, nil", v, err)
	}
	if got := sys.asks.trackedBy(cellOf(t, sys, a)); len(got) != 0 {
		t.Errorf("a tracks %v, want nothing for a resolved ask", got)
	}
	if err := Stop(sys, a.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	for _, rec := range askRecords(eng.Records(), "lockstep.reply", r.ask) {
		if slices.Contains(rec, "outcome=late") {
			t.Errorf("late reply record %q after the stop", rec)
		}
	}
}

// MBX-150 (b), MSG-310, MSG-340 on the local engine (run under -race): asks race with their
// target's stop. Every ask resolves exactly once, with the answer or ErrTerminated, and no
// tracked entry survives the stop.
func TestAskRacingStop(t *testing.T) {
	sys := newLocalSystem(t)
	const n = 200
	for i := range n {
		tp, err := Spawn(sys, fmt.Sprintf("t%d", i), actorOf[acctMsg](&fnActor[acctMsg]{
			receive: func(ctx *Context[acctMsg], m acctMsg) {
				if g, ok := m.(GetBalance); ok {
					g.Reply.Send(ctx, 1)
				}
			},
		}))
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		replies := make([]*Reply[int64], 4)
		for j := range replies {
			wg.Add(1)
			go func() {
				defer wg.Done()
				replies[j] = Ask(sys, tp, getBalance, WithTimeout(time.Hour))
			}()
		}
		if err := Stop(sys, tp.Address()); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		for _, r := range replies {
			v, err := r.Await(within(t, 10*time.Second))
			if (v != 1 || err != nil) && !errors.Is(err, ErrTerminated) {
				t.Fatalf("iteration %d: Await = %d, %v, want 1 or ErrTerminated", i, v, err)
			}
		}
	}
	// Every ask resolved, so none stays tracked: a stop removes its tracked asks before it fails
	// them, and a reply attempt or a refused request removes its own.
	sys.asks.mu.Lock()
	tracked, lists := len(sys.asks.tracked), len(sys.asks.byActor)
	sys.asks.mu.Unlock()
	if tracked != 0 || lists != 0 {
		t.Errorf("%d tracked asks and %d per-actor lists left after every ask resolved", tracked, lists)
	}
}
