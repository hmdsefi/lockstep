// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// failActor fails its incarnation on every message. It has no Started hook.
type failActor struct{}

func (failActor) Receive(ctx *Context[int], _ int) { ctx.Fail(errProbe) }

// startedFailActor is a failActor with a Started hook that counts its calls.
type startedFailActor struct {
	failActor
	started *int
}

func (a startedFailActor) Started(*Context[int]) { *a.started++ }

// AT-ACT-09: ACT-100, ACT-340. It extends TestSpawnDoesNotCallFactory with the optional interface
// that exists only on every other incarnation and with the restart after ctx.Fail.
func TestATACT09FactoryAndOptionalInterfaces(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	calls, started := 0, 0
	pid, err := Spawn(sys, "a", func() Actor[int] {
		calls++
		if calls%2 == 0 {
			return startedFailActor{started: &started}
		}
		return failActor{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("factory called %d times inside Spawn, want 0", calls)
	}
	eng.RunUntilIdle()
	if calls != 1 || started != 0 {
		t.Errorf("after the start: factory calls %d, Started calls %d; want 1, 0", calls, started)
	}
	send(t, sys, pid, 1) // the first incarnation fails and restarts
	eng.RunUntilIdle()
	if calls != 2 || started != 1 {
		t.Errorf("after the restart: factory calls %d, Started calls %d; want 2, 1", calls, started)
	}
	if _, inc := cellOf(t, sys, pid).lifecycle(); inc != 2 {
		t.Errorf("incarnation %d, want 2", inc)
	}
}

// ctxSeen is what AT-ACT-10 records inside a turn.
type ctxSeen struct {
	ctx    *Context[any]
	self   PID[any]
	parent Address
}

// AT-ACT-10: ACT-110, ACT-210. It extends TestContextIdentityAndInTurn with the context of a
// restarted incarnation, the Parent of a child and the from attribute of the send records.
func TestATACT10ContextIdentity(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	a, _ := spawnProbe[any](t, sys, "a", func(ctx *Context[any], m any) {
		if m == "fail" {
			ctx.Fail(errProbe)
		}
	})
	observe := func(ctx *Context[any]) any { return ctxSeen{ctx, ctx.Self(), ctx.Parent()} }
	first, _ := inProbe(t, sys, a, observe).(ctxSeen)
	second, _ := inProbe(t, sys, a, observe).(ctxSeen)

	// A child of a sees a as its Parent, and a's context sends with from=a.
	var c PID[any]
	inProbe(t, sys, a, func(ctx *Context[any]) any {
		c, _ = spawnProbe[any](t, ctx, "c", nil)
		return nil
	})
	child, _ := inProbe(t, sys, c, observe).(ctxSeen)
	if child.parent != a.Address() {
		t.Errorf("Parent() of a's child = %v, want %v", child.parent, a.Address())
	}
	send(t, sys, a, any("from the system"))
	inProbe(t, sys, a, func(ctx *Context[any]) any {
		if err := Send(ctx, c, any("from a")); err != nil {
			t.Errorf("Send from a: %v", err)
		}
		return nil
	})
	sysAddr := fmt.Sprintf("lockstep://local/user#%x", uint64(sys.Incarnation())<<32|1)
	if sys.user.String() != sysAddr {
		t.Fatalf("the system's address is %v, want %s", sys.user, sysAddr)
	}
	got := recordsOf(eng.Records(), "lockstep.send")
	want := [][]string{
		{"from=" + sysAddr, "to=" + a.String(), "msg=" + sys.eng.Tracer.Describe(any("from the system"))},
		{"from=" + a.String(), "to=" + c.String(), "msg=" + sys.eng.Tracer.Describe(any("from a"))},
	}
	if len(got) < len(want) || !slices.EqualFunc(got[len(got)-len(want):], want, slices.Equal) {
		t.Errorf("last lockstep.send records:\n got %q\nwant %q", got, want)
	}

	send(t, sys, a, any("fail"))
	eng.RunUntilIdle()
	third, _ := inProbe(t, sys, a, observe).(ctxSeen)

	if first.ctx == nil || first.ctx != second.ctx {
		t.Errorf("contexts of the two turns of incarnation 1: %p, %p; want the same non-nil one", first.ctx, second.ctx)
	}
	if third.ctx == nil || third.ctx == first.ctx {
		t.Errorf("context after the restart %p, want a new one (was %p)", third.ctx, first.ctx)
	}
	for i, s := range []ctxSeen{first, second, third} {
		if s.self != a {
			t.Errorf("turn %d: Self() = %v, want %v", i+1, s.self, a)
		}
		if s.parent != sys.user {
			t.Errorf("turn %d: Parent() = %v, want the user guardian %v", i+1, s.parent, sys.user)
		}
	}
}

// AT-ACT-11: ACT-120, ACT-140. Self and Parent work after the turn; every other method listed by
// the test panics. It extends the single-method checks of TestContextIdentityAndInTurn and the
// Logf, Fail, Watch and Stop tests with Now and After and a table over the whole list.
func TestATACT11ContextOutsideItsTurn(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, _ := spawnProbe[any](t, sys, "p", nil)
	var saved *Context[any]
	inProbe(t, sys, pid, func(ctx *Context[any]) any { saved = ctx; return nil })
	other, _ := spawnProbe[any](t, sys, "other", nil)
	eng.RunUntilIdle()

	if got := saved.Self(); got != pid {
		t.Errorf("Self() after the turn = %v, want %v", got, pid)
	}
	if got := saved.Parent(); got != sys.user {
		t.Errorf("Parent() after the turn = %v, want %v", got, sys.user)
	}
	want := "lockstep: Context of " + pid.String() + " used outside its turn"
	for _, tc := range []struct {
		name string
		call func()
	}{
		{"Logf", func() { saved.Logf("x") }},
		{"Now", func() { _ = saved.Now() }},
		{"After", func() { saved.After(time.Second, any("m")) }},
		{"Fail", func() { saved.Fail(errProbe) }},
		{"Spawn", func() { _, _ = Spawn(saved, "c", idle[any]()) }},
		{"Watch", func() { _ = Watch(saved, other.Address()) }},
		{"Stop", func() { _ = Stop(saved, other.Address()) }},
	} {
		if v := mustPanic(t, tc.call); v != want {
			t.Errorf("%s outside the turn panicked with %v, want %q", tc.name, v, want)
		}
	}
	if n := len(recordsOf(eng.Records(), "lockstep.stop")); n != 0 {
		t.Errorf("%d lockstep.stop records: Stop outside the turn stopped an actor", n)
	}
	if got := sys.registryWatchers(other.Address()); len(got) != 0 {
		t.Errorf("Watch outside the turn registered %v", got)
	}
}

// AT-ACT-18 (the WithMailbox clause): ACT-340, ACT-350. It extends TestATACT18MessagesBeforeStart.
// An actor whose Started blocks accepts as many sends as its mailbox holds and refuses the next
// with ErrMailboxFull; a sibling without options accepts all three.
func TestATACT18MailboxOption(t *testing.T) {
	sys := newLocalSystem(t)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	// The cleanup runs before newLocalSystem's, so the engine can stop.
	t.Cleanup(func() { once.Do(func() { close(release) }) })

	aEvents, bEvents := make(chan string, 16), make(chan string, 16)
	a, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		started: func(*Context[int]) {
			entered <- struct{}{}
			<-release
			aEvents <- "started"
		},
		receive: func(_ *Context[int], m int) { aEvents <- strconv.Itoa(m) },
	}), WithMailbox(2, FailSend))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, entered) // a's Started is blocked
	b, err := Spawn(sys, "b", actorOf[int](&fnActor[int]{
		started: func(*Context[int]) { bEvents <- "started" },
		receive: func(_ *Context[int], m int) { bEvents <- strconv.Itoa(m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	for m := 1; m <= 3; m++ {
		err := Send(sys, a, m)
		if m <= 2 && err != nil {
			t.Errorf("Send(a, %d) = %v, want nil", m, err)
		}
		if m == 3 && !errors.Is(err, ErrMailboxFull) {
			t.Errorf("Send(a, 3) = %v, want ErrMailboxFull", err)
		}
		if err := Send(sys, b, m); err != nil {
			t.Errorf("Send(b, %d) = %v, want nil", m, err)
		}
	}
	once.Do(func() { close(release) })
	for _, tc := range []struct {
		name   string
		events chan string
		want   []string
	}{
		{"a", aEvents, []string{"started", "1", "2"}},
		{"b", bEvents, []string{"started", "1", "2", "3"}},
	} {
		var got []string
		for range tc.want {
			got = append(got, recv(t, tc.events))
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("events of %s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// AT-ACT-21: ACT-420. It extends TestATSUP20RestartKeepsPIDAndMailbox with a send to the
// pre-restart PID after the restart.
func TestATACT21RestartKeepsThePID(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var got []string
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{receive: func(ctx *Context[any], m any) {
		got = append(got, fmt.Sprintf("inc %d got %v", ctx.inc, m))
		if m == "fail" {
			ctx.Fail(errProbe)
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	before, err := Lookup[any](sys, "/user/a")
	if err != nil || before != a {
		t.Fatalf("Lookup before the restart = %v, %v; want %v", before, err, a)
	}
	send(t, sys, a, any("fail"))
	eng.RunUntilIdle()
	if _, inc := cellOf(t, sys, a).lifecycle(); inc != 2 {
		t.Fatalf("incarnation %d after the failure, want 2", inc)
	}
	after, err := Lookup[any](sys, "/user/a")
	if err != nil || after != a {
		t.Errorf("Lookup after the restart = %v, %v; want %v", after, err, a)
	}
	send(t, sys, a, any("hello")) // the PID from before the restart
	eng.RunUntilIdle()
	if want := []string{"inc 1 got fail", "inc 2 got hello"}; !slices.Equal(got, want) {
		t.Errorf("messages received %q, want %q", got, want)
	}
}

// AT-ACT-28: ACT-610, ACT-620. It extends TestATSUP32TerminatedOnceNotOnRestart with a second
// Watch of the same address.
func TestATACT28RestartAndDuplicateWatch(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	tgt, _ := spawnRunner(t, sys, eng, "t")
	w, r := spawnRunner(t, sys, eng, "w")
	var errs [2]error
	inTurn(t, sys, eng, w, func(ctx *Context[any]) {
		errs[0] = Watch(ctx, tgt.Address())
		errs[1] = Watch(ctx, tgt.Address())
	})
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("Watch twice = %v, %v; want nil, nil", errs[0], errs[1])
	}
	if got := sys.registryWatchers(tgt.Address()); len(got) != 1 || got[0] != w.Address() {
		t.Errorf("watchers of t after two Watch calls = %v, want [%v]", got, w.Address())
	}

	inTurn(t, sys, eng, tgt, func(ctx *Context[any]) { ctx.Fail(errProbe) })
	if _, inc := cellOf(t, sys, tgt).lifecycle(); inc != 2 {
		t.Fatalf("t is in incarnation %d, want 2: it did not restart", inc)
	}
	eng.Advance(100 * time.Millisecond)
	if len(r.sigs) != 0 {
		t.Errorf("signals after t's restart: %v, want none", r.sigs)
	}
	if got := sys.registryWatchers(tgt.Address()); len(got) != 1 {
		t.Errorf("watchers of t after its restart = %v, want w's one entry", got)
	}

	if err := Stop(sys, tgt.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if want := []Signal{Terminated{Addr: tgt.Address(), Reason: Normal}}; !slices.Equal(r.sigs, want) {
		t.Errorf("signals after Stop(t) = %v, want %v", r.sigs, want)
	}
}

// AT-ACT-30: ACT-640. It extends TestStopDropsWatches with the restart of the watcher.
func TestATACT30WatchesBelongToTheIncarnation(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	tgt, _ := spawnRunner(t, sys, eng, "t")
	w, r := spawnRunner(t, sys, eng, "w")
	inTurn(t, sys, eng, w, func(ctx *Context[any]) {
		if err := Watch(ctx, tgt.Address()); err != nil {
			t.Errorf("Watch: %v", err)
		}
	})
	if got := sys.registryWatchers(tgt.Address()); len(got) != 1 {
		t.Fatalf("watchers of t before the restart = %v, want one", got)
	}

	inTurn(t, sys, eng, w, func(ctx *Context[any]) { ctx.Fail(errProbe) })
	if _, inc := cellOf(t, sys, w).lifecycle(); inc != 2 {
		t.Fatalf("w is in incarnation %d, want 2: it did not restart", inc)
	}
	if got := sys.registryWatchers(tgt.Address()); len(got) != 0 {
		t.Errorf("watchers of t after w's restart = %v, want none", got)
	}
	if err := Stop(sys, tgt.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if len(r.sigs) != 0 {
		t.Errorf("w's new incarnation got %v after t stopped, want nothing", r.sigs)
	}
}

// AT-ENG-21: ENG-310, SUP-010. The stop command that ctx.Stop queued while message 1 was handled
// runs before message 2 in the same turn. The turn's record counts one system and one user
// message. No later turn of A runs: messages 2 and 3 are dead letters and Receive never sees them
// (the guardians' turns for A's stopped notice follow, so Step may still return true).
func TestATENG21SystemMessagesFirst(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	var log []string
	pid, err := Spawn(sys, "a", actorOf[int](&hookActor[int]{
		receive: func(ctx *Context[int], m int) {
			log = append(log, strconv.Itoa(m))
			if m == 1 {
				ctx.Stop()
			}
		},
		stopped: func(*Context[int], StopReason) { log = append(log, "stopped") },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	turnsOfA := func() [][]string {
		var out [][]string
		for _, r := range recordsOf(eng.Records(), "lockstep.turn") {
			if r[0] == "actor="+pid.String() {
				out = append(out, r)
			}
		}
		return out
	}
	before := len(turnsOfA())
	for m := 1; m <= 3; m++ {
		send(t, sys, pid, m)
	}
	if !eng.Step() {
		t.Fatal("Step returned false")
	}
	if want := []string{"1", "stopped"}; !slices.Equal(log, want) {
		t.Fatalf("log after one Step %q, want %q", log, want)
	}
	turns := turnsOfA()
	if len(turns) != before+1 {
		t.Fatalf("%d turns of A recorded, want %d", len(turns), before+1)
	}
	if last := turns[len(turns)-1]; len(last) < 3 || last[1] != "system=1" || last[2] != "user=1" {
		t.Errorf("A's turn record %q, want system=1 user=1", last)
	}

	eng.RunUntilIdle()
	if after := len(turnsOfA()); after != before+1 {
		t.Errorf("%d turns of A after RunUntilIdle, want no further one (%d)", after, before+1)
	}
	if want := []string{"1", "stopped"}; !slices.Equal(log, want) {
		t.Errorf("log after RunUntilIdle %q, want %q", log, want)
	}
	n := 0
	for _, r := range recordsOf(eng.Records(), "lockstep.deadletter") {
		if slices.Contains(r, "to="+pid.String()) {
			n++
		}
	}
	if n != 2 {
		t.Errorf("%d dead letters for A, want 2 (messages 2 and 3)", n)
	}
}

// AT-ENG-24: ENG-310, SUP-190. A panic on message 1 restarts A after the backoff timer; message 2
// waits in the mailbox until then. It extends TestATMBX22RestartKeepsSentDropsStaleSelf and
// TestATSUP39BackoffTimerLabel with the manual engine's view of both.
func TestATENG24RestartKeepsQueuedMessages(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	var log []int
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{receive: func(_ *Context[int], m int) {
		log = append(log, m)
		if m == 1 {
			panic("boom")
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, pid, 1)
	send(t, sys, pid, 2)
	eng.RunUntilIdle()
	if want := []int{1}; !slices.Equal(log, want) {
		t.Errorf("log before the backoff %v, want %v", log, want)
	}
	if labels := eng.Labels(); len(labels) == 0 || labels[len(labels)-1] != "lockstep/backoff" {
		t.Errorf("Labels() = %q, want it to end with lockstep/backoff", labels)
	}
	eng.Advance(time.Second)
	eng.RunUntilIdle()
	if want := []int{1, 2}; !slices.Equal(log, want) {
		t.Errorf("log after the backoff %v, want %v", log, want)
	}
}
