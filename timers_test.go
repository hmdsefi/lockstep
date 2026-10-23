// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// timerSeen is one message that a timerActor received: the message and the manual clock's elapsed
// time at its Receive.
type timerSeen struct {
	msg any
	at  time.Duration
}

// timerActor is an any actor for the timer tests. It runs a func(*Context[any]) it receives, so a
// test chooses what happens inside a turn, passes every other message to onMsg if set, and logs it
// in seen with the elapsed time of the manual clock.
type timerActor struct {
	started func(ctx *Context[any])
	onMsg   func(ctx *Context[any], msg any)
	seen    []timerSeen
}

func (a *timerActor) Started(ctx *Context[any]) {
	if a.started != nil {
		a.started(ctx)
	}
}

func (a *timerActor) Receive(ctx *Context[any], msg any) {
	if f, ok := msg.(func(*Context[any])); ok {
		f(ctx)
		return
	}
	a.seen = append(a.seen, timerSeen{msg, ctx.Now().Sub(enginetest.Epoch)})
	if a.onMsg != nil {
		a.onMsg(ctx, msg)
	}
}

// msgs returns the messages the actor received, in order.
func (a *timerActor) msgs() []any {
	out := make([]any, len(a.seen))
	for i, s := range a.seen {
		out[i] = s.msg
	}
	return out
}

// spawnTimerActor spawns a as "a" and runs the engine until it is idle.
func spawnTimerActor(t *testing.T, sys *System, eng *enginetest.Engine, a *timerActor) PID[any] {
	t.Helper()
	pid, err := Spawn(sys, "a", actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	return pid
}

// timerRecords returns the lockstep.timer records of recs, one string per record with the
// attributes after actor and inc joined by spaces.
func timerRecords(recs []engine.Record) []string {
	var out []string
	for _, attrs := range recordsOf(recs, "lockstep.timer") {
		s := ""
		for _, a := range attrs[2:] {
			if s != "" {
				s += " "
			}
			s += a
		}
		out = append(out, s)
	}
	return out
}

// TMR-010: ctx.Now returns the engine clock at the moment of the call, before and after the
// clock advanced.
func TestTMR010Now(t *testing.T) {
	sys, eng := newManualSystem(t)
	var inStarted, first, later time.Time
	a := &timerActor{started: func(ctx *Context[any]) { inStarted = ctx.Now() }}
	pid := spawnTimerActor(t, sys, eng, a)
	if want := eng.Now(); inStarted != want {
		t.Errorf("Now in Started = %v, want %v", inStarted, want)
	}
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { first = ctx.Now() })
	eng.Advance(1500 * time.Millisecond)
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { later = ctx.Now() })
	if want := enginetest.Epoch; first != want {
		t.Errorf("Now before Advance = %v, want %v", first, want)
	}
	if want := enginetest.Epoch.Add(1500 * time.Millisecond); later != want || eng.Now() != want {
		t.Errorf("Now after Advance = %v, engine clock %v, want %v", later, eng.Now(), want)
	}
}

// AT-TMR-03: TMR-020. After delivers in a later turn than the call, a negative d counts as 0, and
// the records are set (with the requested d, repeat false) and then fire.
func TestATTMR03After(t *testing.T) {
	sys, eng := newManualSystem(t)
	a := &timerActor{}
	pid := spawnTimerActor(t, sys, eng, a)
	eng.Advance(2 * time.Second)

	var ids [2]TimerID
	var seenInCall int
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		ids[0] = ctx.After(100*time.Millisecond, "x")
		ids[1] = ctx.After(-time.Second, "y")
		seenInCall = len(a.seen)
	})
	if ids != [2]TimerID{1, 2} {
		t.Errorf("After returned %v, want [1 2]", ids)
	}
	if seenInCall != 0 || len(a.seen) != 0 {
		t.Errorf("a message arrived inside the call or before the clock moved: %v", a.seen)
	}
	eng.Advance(0) // y is due now
	if want := []timerSeen{{"y", 2 * time.Second}}; !slices.Equal(a.seen, want) {
		t.Fatalf("after Advance(0): %v, want %v", a.seen, want)
	}
	eng.Advance(100 * time.Millisecond)
	want := []timerSeen{{"y", 2 * time.Second}, {"x", 2100 * time.Millisecond}}
	if !slices.Equal(a.seen, want) {
		t.Errorf("received %v, want %v", a.seen, want)
	}

	tr := eng.Engine().Tracer
	wantRecs := []string{
		"timer=1 op=set d=100ms repeat=false msg=" + tr.Describe("x"),
		"timer=2 op=set d=-1s repeat=false msg=" + tr.Describe("y"),
		"timer=2 op=fire msg=" + tr.Describe("y"),
		"timer=1 op=fire msg=" + tr.Describe("x"),
	}
	if got := timerRecords(eng.Records()); !slices.Equal(got, wantRecs) {
		t.Errorf("lockstep.timer records:\n got %q\nwant %q", got, wantRecs)
	}
	for _, attrs := range recordsOf(eng.Records(), "lockstep.timer") {
		if attrs[0] != "actor="+pid.String() || attrs[1] != "inc=1" {
			t.Errorf("record %q: want actor=%v inc=1 first", attrs, pid)
		}
	}
}

// AT-TMR-04: TMR-030. Every ticks at 50, 100 and 150 ms, and not after the third tick cancelled
// it; a non-positive interval fails the actor with the TMR-030 text.
func TestATTMR04Every(t *testing.T) {
	t.Run("ticks", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var id TimerID
		var cancelled bool
		a := &timerActor{started: func(ctx *Context[any]) { id = ctx.Every(50*time.Millisecond, "tick") }}
		a.onMsg = func(ctx *Context[any], _ any) {
			if len(a.seen) == 3 {
				cancelled = ctx.CancelTimer(id)
			}
		}
		spawnTimerActor(t, sys, eng, a)
		eng.Advance(time.Second)
		want := []timerSeen{{"tick", 50 * time.Millisecond}, {"tick", 100 * time.Millisecond}, {"tick", 150 * time.Millisecond}}
		if !slices.Equal(a.seen, want) {
			t.Errorf("ticks %v, want %v", a.seen, want)
		}
		if id != 1 || !cancelled {
			t.Errorf("Every returned %d, cancel of the third tick = %v; want 1, true", id, cancelled)
		}
	})
	for _, c := range []struct {
		d    time.Duration
		text string
	}{
		{0, "lockstep: panic: lockstep: Every interval must be positive, got 0s"},
		{-1, "lockstep: panic: lockstep: Every interval must be positive, got -1ns"},
	} {
		t.Run(c.d.String(), func(t *testing.T) {
			sys, eng := newManualSystem(t)
			pid := spawnTimerActor(t, sys, eng, &timerActor{})
			inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.Every(c.d, "tick") })
			want := [][]string{{"actor=" + pid.String(), "inc=1", "source=receive", "err=" + c.text}}
			if got := recordsOf(eng.Records(), "lockstep.fail"); !slices.EqualFunc(got, want, slices.Equal) {
				t.Errorf("lockstep.fail records:\n got %q\nwant %q", got, want)
			}
			if got := timerRecords(eng.Records()); len(got) != 0 {
				t.Errorf("lockstep.timer records %q, want none", got)
			}
		})
	}
}

// AT-TMR-06: TMR-050. A firing enqueues a Self entry that a full mailbox admits: the mailbox holds
// the sent message and the tick, and the actor receives the sent message first.
func TestATTMR06FiringEnqueuesSelfEntry(t *testing.T) {
	sys, eng := newManualSystem(t)
	a := &timerActor{started: func(ctx *Context[any]) { ctx.After(10*time.Millisecond, "tick") }}
	pid, err := Spawn(sys, "a", actorOf[any](a), WithMailbox(1, FailSend))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, pid)
	release := c.holdTurns()
	send(t, sys, pid, any("m"))

	eng.Advance(15 * time.Millisecond)
	if n, s := c.user.Len(), c.user.SentLen(); n != 2 || s != 1 {
		t.Errorf("at 15ms the mailbox holds %d entries, %d sent; want 2, 1", n, s)
	}
	release()
	eng.RunUntilIdle()
	if want := []any{"m", "tick"}; !slices.Equal(a.msgs(), want) {
		t.Errorf("received %v, want %v", a.msgs(), want)
	}
	tr := eng.Engine().Tracer
	want := []string{
		"timer=1 op=set d=10ms repeat=false msg=" + tr.Describe("tick"),
		"timer=1 op=fire msg=" + tr.Describe("tick"),
	}
	if got := timerRecords(eng.Records()); !slices.Equal(got, want) {
		t.Errorf("lockstep.timer records:\n got %q\nwant %q", got, want)
	}
}

// AT-TMR-07: TMR-060. While a tick waits in the mailbox the firings enqueue nothing and record
// skip; the next firing is still armed. The mailbox never holds two ticks.
func TestATTMR07Coalescing(t *testing.T) {
	sys, eng := newManualSystem(t)
	a := &timerActor{started: func(ctx *Context[any]) { ctx.Every(10*time.Millisecond, "tick") }}
	pid := spawnTimerActor(t, sys, eng, a)
	c := cellOf(t, sys, pid)

	eng.Advance(5 * time.Millisecond)
	release := c.holdTurns()
	for range 9 { // to 95 ms: firings at 10, 20, ... 90
		eng.Advance(10 * time.Millisecond)
		if n := c.user.Len(); n > 1 {
			t.Fatalf("at %v the mailbox holds %d ticks", eng.Now().Sub(enginetest.Epoch), n)
		}
	}
	if len(a.seen) != 0 {
		t.Fatalf("received %v while held", a.seen)
	}
	release()
	eng.RunUntilIdle()
	for range 3 { // firings at 100, 110, 120
		eng.Advance(10 * time.Millisecond)
	}
	ms := time.Millisecond
	want := []timerSeen{{"tick", 95 * ms}, {"tick", 100 * ms}, {"tick", 110 * ms}, {"tick", 120 * ms}}
	if !slices.Equal(a.seen, want) {
		t.Errorf("ticks %v, want %v", a.seen, want)
	}
	skips := 0
	for _, r := range timerRecords(eng.Records()) {
		if r == "timer=1 op=skip" {
			skips++
		}
	}
	if skips != 8 {
		t.Errorf("%d skip records, want 8", skips)
	}
}

// AT-TMR-18: ENG-070. After and Every arm AfterFunc timers labelled lockstep/timer, and the
// Every re-arm adds one more.
func TestATTMR18TimerLabel(t *testing.T) {
	sys, eng := newManualSystem(t)
	a := &timerActor{started: func(ctx *Context[any]) {
		ctx.After(time.Second, "x")
		ctx.Every(time.Second, "y")
	}}
	spawnTimerActor(t, sys, eng, a)
	if got, want := eng.Labels(), []string{"lockstep/timer", "lockstep/timer"}; !slices.Equal(got, want) {
		t.Errorf("labels after RunUntilIdle = %q, want %q", got, want)
	}
	eng.Advance(time.Second)
	if got, want := eng.Labels(), []string{"lockstep/timer", "lockstep/timer", "lockstep/timer"}; !slices.Equal(got, want) {
		t.Errorf("labels after Advance = %q, want %q", got, want)
	}
	if got := a.msgs(); !slices.Equal(got, []any{"x", "y"}) {
		t.Errorf("received %v, want [x y]", got)
	}
}

// TMR-070: a timer message of an incarnation that has ended is dropped when it is popped
// (MBX-170) with lockstep.drop incarnation-ended, and never reaches Receive.
func TestTMR070TimerMessageOfEndedIncarnationDropped(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	a := &timerActor{started: func(ctx *Context[any]) {
		if ctx.inc == 1 {
			ctx.After(10*time.Millisecond, "late")
		}
	}}
	a.onMsg = func(_ *Context[any], msg any) {
		if msg == "panic" {
			panic("boom")
		}
	}
	pid := spawnTimerActor(t, sys, eng, a)
	c := cellOf(t, sys, pid)
	release := c.holdTurns()
	send(t, sys, pid, any("panic")) // pops first, then the incarnation restarts
	eng.Advance(20 * time.Millisecond)
	if n := c.user.Len(); n != 2 {
		t.Fatalf("the mailbox holds %d entries, want 2", n)
	}
	release()
	eng.RunUntilIdle()

	if want := []any{"panic"}; !slices.Equal(a.msgs(), want) {
		t.Errorf("received %v, want %v", a.msgs(), want)
	}
	if _, inc := c.lifecycle(); inc != 2 {
		t.Errorf("incarnation %d, want 2", inc)
	}
	tr := eng.Engine().Tracer
	want := [][]string{{"to=" + pid.String(), "reason=incarnation-ended", "inc=1", "msg=" + tr.Describe("late")}}
	if got := recordsOf(eng.Records(), "lockstep.drop"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.drop records:\n got %q\nwant %q", got, want)
	}
}

// TMR-070: a timer message whose timer is no longer active is dropped when it is popped, with
// lockstep.drop reason timer-cancelled. A message of an unknown timer is treated the same way.
func TestTMR070CancelledTimerMessageDropped(t *testing.T) {
	sys, eng := newManualSystem(t)
	a := &timerActor{}
	pid := spawnTimerActor(t, sys, eng, a)
	c := cellOf(t, sys, pid)
	var id TimerID
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { id = ctx.After(time.Millisecond, "t1") })
	release := c.holdTurns()
	var cancelled bool
	send(t, sys, pid, any(func(ctx *Context[any]) { cancelled = ctx.CancelTimer(id) }))
	eng.Advance(2 * time.Millisecond) // the message of t1 queues behind the function
	if n := c.user.Len(); n != 2 {
		t.Fatalf("the mailbox holds %d entries, want 2", n)
	}
	c.offerSelf(entry{msg: "t2", kind: entrySelf, inc: c.inc, timer: 99})
	release()
	eng.RunUntilIdle()

	if !cancelled {
		t.Error("CancelTimer returned false for an armed timer")
	}
	if got := a.msgs(); len(got) != 0 {
		t.Errorf("received %v, want nothing", got)
	}
	tr := eng.Engine().Tracer
	want := [][]string{
		{"to=" + pid.String(), "reason=timer-cancelled", "inc=1", "msg=" + tr.Describe("t1")},
		{"to=" + pid.String(), "reason=timer-cancelled", "inc=1", "msg=" + tr.Describe("t2")},
	}
	if got := recordsOf(eng.Records(), "lockstep.drop"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.drop records:\n got %q\nwant %q", got, want)
	}
}

// TMR-050, TMR-070 on the local engine, where callbacks run on other goroutines: an After(0) and
// an Every arrive, the Every until it is cancelled. Run with -race.
func TestTimersOnLocalEngine(t *testing.T) {
	sys, _ := newRecordingSystem(t)
	got := make(chan string, 64)
	a := &fnActor[any]{}
	var id TimerID
	ticks := 0
	a.started = func(ctx *Context[any]) {
		ctx.After(0, "once")
		id = ctx.Every(time.Millisecond, "tick")
	}
	a.receive = func(ctx *Context[any], msg any) {
		if msg == "tick" {
			if ticks++; ticks == 3 && !ctx.CancelTimer(id) {
				t.Error("CancelTimer of the running Every returned false")
			}
			if ticks > 3 {
				t.Error("tick after the cancel")
			}
		}
		m, _ := msg.(string)
		got <- m
	}
	if _, err := Spawn(sys, "a", actorOf[any](a)); err != nil {
		t.Fatal(err)
	}
	var seen []string
	deadline := time.After(10 * time.Second)
	for len(seen) < 4 {
		select {
		case m := <-got:
			seen = append(seen, m)
		case <-deadline:
			t.Fatalf("received only %q within 10s", seen)
		}
	}
	if want := []string{"once", "tick", "tick", "tick"}; !slices.Equal(seen, want) {
		t.Errorf("received %q, want %q", seen, want)
	}
	time.Sleep(20 * time.Millisecond) // a tick after the cancel would reach got
	select {
	case m := <-got:
		t.Errorf("received %q after the cancel", m)
	default:
	}
}
