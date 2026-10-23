// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// pendingClock wraps an engine clock and counts the lockstep/timer callbacks that are armed and
// have neither run nor been cancelled, so a test sees that ending an incarnation called the
// Cancel of every armed AfterFunc (TMR-080). The manual engine runs callbacks on the test's
// goroutine, so the count needs no lock.
type pendingClock struct {
	engine.Clock
	pending int
}

func (c *pendingClock) AfterFunc(d time.Duration, label string, fn func()) engine.Cancel {
	if label != timerLabel {
		return c.Clock.AfterFunc(d, label, fn)
	}
	c.pending++
	cancel := c.Clock.AfterFunc(d, label, func() {
		c.pending--
		fn()
	})
	return func() bool {
		ok := cancel()
		if ok {
			c.pending--
		}
		return ok
	}
}

// newPendingClockSystem returns a manual system whose clock is a pendingClock.
func newPendingClockSystem(t *testing.T, opts ...Option) (*System, *enginetest.Engine, *pendingClock) {
	t.Helper()
	eng := enginetest.New("local", 1)
	b := eng.Engine()
	clk := &pendingClock{Clock: b.Clock}
	b.Clock = clk
	sys, err := newSystem(b, opts)
	if err != nil {
		t.Fatalf("newSystem on a manual engine: %v", err)
	}
	return sys, eng, clk
}

// timerStopper is a timerActor that implements Stopped.
type timerStopper struct {
	*timerActor
	stopped func(ctx *Context[any], r StopReason)
}

func (a *timerStopper) Stopped(ctx *Context[any], r StopReason) {
	if a.stopped != nil {
		a.stopped(ctx, r)
	}
}

// panicOn makes a timerActor panic on the message "panic".
func panicOn(_ *Context[any], msg any) {
	if msg == "panic" {
		panic("boom")
	}
}

// ticksOf returns the elapsed times at which a received "tick".
func ticksOf(a *timerActor) []time.Duration {
	var out []time.Duration
	for _, s := range a.seen {
		if s.msg == "tick" {
			out = append(out, s.at)
		}
	}
	return out
}

// countRecords returns how many of recs equal r.
func countRecords(recs []string, r string) int {
	n := 0
	for _, s := range recs {
		if s == r {
			n++
		}
	}
	return n
}

// timerMapLen returns how many timers c keeps.
func timerMapLen[M any](c *cell[M]) int {
	c.tmu.Lock()
	defer c.tmu.Unlock()
	return len(c.timers)
}

// AT-TMR-05: TMR-040, TMR-100. The TimerID counter survives restarts: inc 1 gets 1, 2 and 3, inc 2
// gets 4. After inside Stopped gets 0 and leaves the counter alone, so inc 3's first ID is 5.
func TestATTMR05TimerIDAllocation(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var started, stopped []TimerID
	a := &timerStopper{timerActor: &timerActor{onMsg: panicOn}}
	a.started = func(ctx *Context[any]) {
		n := 1
		if ctx.inc == 1 {
			n = 3
		}
		for range n {
			started = append(started, ctx.After(time.Second, "x"))
		}
	}
	a.stopped = func(ctx *Context[any], _ StopReason) {
		stopped = append(stopped, ctx.After(time.Second, "x"))
	}
	pid, err := Spawn(sys, "a", actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	for range 2 {
		send(t, sys, pid, any("panic"))
		eng.RunUntilIdle()
	}

	if want := []TimerID{1, 2, 3, 4, 5}; !slices.Equal(started, want) {
		t.Errorf("IDs from Started %v, want %v", started, want)
	}
	if want := []TimerID{0, 0}; !slices.Equal(stopped, want) {
		t.Errorf("IDs from Stopped %v, want %v", stopped, want)
	}
	if _, inc := cellOf(t, sys, pid).lifecycle(); inc != 3 {
		t.Errorf("incarnation %d, want 3", inc)
	}
}

// AT-TMR-08: TMR-070, TMR-090. t1 fired and waits in the mailbox behind a message whose handler
// cancels it: CancelTimer returns true, t1 never reaches Receive and its pop records lockstep.drop
// timer-cancelled with inc 1.
func TestATTMR08PopSemantics(t *testing.T) {
	sys, eng := newManualSystem(t)
	a := &timerActor{}
	pid := spawnTimerActor(t, sys, eng, a)
	c := cellOf(t, sys, pid)
	var id1 TimerID
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { id1 = ctx.After(time.Millisecond, "t1") })
	release := c.holdTurns()
	var cancelled bool
	send(t, sys, pid, any(func(ctx *Context[any]) { cancelled = ctx.CancelTimer(id1) }))
	eng.Advance(time.Millisecond) // t1 fires and queues behind the function
	if n := c.user.Len(); n != 2 {
		t.Fatalf("the mailbox holds %d entries, want 2", n)
	}
	release()
	eng.RunUntilIdle()

	if !cancelled {
		t.Error("CancelTimer of the queued t1 returned false")
	}
	if got := a.msgs(); len(got) != 0 {
		t.Errorf("received %v, want nothing", got)
	}
	tr := eng.Engine().Tracer
	want := [][]string{{"to=" + pid.String(), "reason=timer-cancelled", "inc=1", "msg=" + tr.Describe("t1")}}
	if got := recordsOf(eng.Records(), "lockstep.drop"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.drop records:\n got %q\nwant %q", got, want)
	}
	wantTimer := []string{
		"timer=1 op=set d=1ms repeat=false msg=" + tr.Describe("t1"),
		"timer=1 op=fire msg=" + tr.Describe("t1"),
		"timer=1 op=cancel",
	}
	if got := timerRecords(eng.Records()); !slices.Equal(got, wantTimer) {
		t.Errorf("lockstep.timer records:\n got %q\nwant %q", got, wantTimer)
	}
}

// AT-TMR-09: TMR-080. Ending an incarnation makes its timers inactive and cancels every armed
// AfterFunc: a restart and a stop deliver nothing more from them, and a stop drops the queued
// message at the drain. A Suspended incarnation's timers keep firing; the queued message waits
// for the resume.
func TestATTMR09RestartAndStopCancelTimers(t *testing.T) {
	t.Run("restart", func(t *testing.T) {
		sys, eng, clk := newPendingClockSystem(t, WithGuardianStrategy(r0()))
		a := &timerActor{onMsg: panicOn}
		a.started = func(ctx *Context[any]) {
			if ctx.inc == 1 {
				ctx.After(time.Second, "late")
				ctx.Every(100*time.Millisecond, "tick")
			}
		}
		pid := spawnTimerActor(t, sys, eng, a)
		eng.Advance(250 * time.Millisecond)
		send(t, sys, pid, any("panic"))
		eng.RunUntilIdle()
		if _, inc := cellOf(t, sys, pid).lifecycle(); inc != 2 {
			t.Fatalf("incarnation %d, want 2", inc)
		}
		if clk.pending != 0 {
			t.Errorf("%d timer callbacks still armed after the restart, want 0", clk.pending)
		}
		if n := timerMapLen(cellOf(t, sys, pid)); n != 0 {
			t.Errorf("the cell keeps %d timers after the restart, want 0", n)
		}
		eng.Advance(time.Second) // past late's 1 s

		ms := time.Millisecond
		want := []timerSeen{{"tick", 100 * ms}, {"tick", 200 * ms}, {"panic", 250 * ms}}
		if !slices.Equal(a.seen, want) {
			t.Errorf("received %v, want %v", a.seen, want)
		}
		recs := timerRecords(eng.Records())
		tr := eng.Engine().Tracer
		if n := countRecords(recs, "timer=2 op=fire msg="+tr.Describe("tick")); n != 2 {
			t.Errorf("%d fire records of the Every, want 2: %q", n, recs)
		}
		for _, r := range recs {
			if strings.HasPrefix(r, "timer=1 op=fire") {
				t.Errorf("late fired: %q", r)
			}
		}
	})

	t.Run("stop", func(t *testing.T) {
		sys, eng, clk := newPendingClockSystem(t)
		a := &timerActor{started: func(ctx *Context[any]) { ctx.Every(100*time.Millisecond, "tick") }}
		pid := spawnTimerActor(t, sys, eng, a)
		c := cellOf(t, sys, pid)
		eng.Advance(90 * time.Millisecond)
		release := c.holdTurns()
		eng.Advance(60 * time.Millisecond) // the tick at 100 ms queues
		if n := c.user.Len(); n != 1 {
			t.Fatalf("the mailbox holds %d entries at 150ms, want 1", n)
		}
		if err := Stop(sys, pid.Address()); err != nil {
			t.Fatal(err)
		}
		release()
		eng.RunUntilIdle()
		if clk.pending != 0 {
			t.Errorf("%d timer callbacks still armed after the stop, want 0", clk.pending)
		}
		eng.Advance(time.Second)

		if len(a.seen) != 0 {
			t.Errorf("received %v, want nothing", a.seen)
		}
		tr := eng.Engine().Tracer
		want := [][]string{{"to=" + pid.String(), "reason=incarnation-ended", "inc=1", "msg=" + tr.Describe("tick")}}
		if got := recordsOf(eng.Records(), "lockstep.drop"); !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("lockstep.drop records:\n got %q\nwant %q", got, want)
		}
		if n := countRecords(timerRecords(eng.Records()), "timer=1 op=fire msg="+tr.Describe("tick")); n != 1 {
			t.Errorf("%d fire records, want 1 (at 100ms)", n)
		}
	})

	t.Run("suspended", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		a := &timerActor{onMsg: panicOn}
		a.started = func(ctx *Context[any]) { ctx.Every(100*time.Millisecond, "tick") }
		var child PID[any]
		var releaseParent func()
		_, err := Spawn(sys, "s", actorOf[any](&fnActor[any]{started: func(ctx *Context[any]) {
			var err error
			if child, err = Spawn(ctx, "a", actorOf[any](a)); err != nil {
				panic(err)
			}
			releaseParent = ctx.cell.holdTurns() // the failed notice waits in the parent's queue
		}}), WithStrategy(deciding(DirectiveResume)))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		c := cellOf(t, sys, child)
		eng.Advance(50 * time.Millisecond)
		send(t, sys, child, any("panic"))
		eng.RunUntilIdle()
		if st, _ := c.lifecycle(); st != supervise.Suspended {
			t.Fatalf("A is %v, want suspended", st)
		}
		eng.Advance(300 * time.Millisecond) // firings at 100 (queued), 200 and 300 ms (skipped)
		if got := ticksOf(a); len(got) != 0 {
			t.Errorf("ticks while suspended: %v", got)
		}
		if n := c.user.Len(); n != 1 {
			t.Errorf("the mailbox holds %d entries while suspended, want 1", n)
		}
		releaseParent()
		eng.RunUntilIdle() // the parent resumes A
		eng.Advance(50 * time.Millisecond)

		ms := time.Millisecond
		if got, want := ticksOf(a), []time.Duration{350 * ms, 400 * ms}; !slices.Equal(got, want) {
			t.Errorf("ticks at %v, want %v", got, want)
		}
		if n := countRecords(timerRecords(eng.Records()), "timer=1 op=skip"); n != 2 {
			t.Errorf("%d skip records, want 2", n)
		}
	})
}

// AT-TMR-10: TMR-090. CancelTimer is true exactly for an active timer of the current incarnation;
// only the true results record op cancel.
func TestATTMR10CancelTimerResults(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var old TimerID
	a := &timerActor{onMsg: panicOn, started: func(ctx *Context[any]) {
		if ctx.inc == 1 {
			old = ctx.After(time.Hour, "old")
		}
	}}
	pid := spawnTimerActor(t, sys, eng, a)

	call := func(f func(ctx *Context[any]) bool) bool {
		t.Helper()
		var got bool
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { got = f(ctx) })
		return got
	}
	cancel := func(id TimerID) func(ctx *Context[any]) bool {
		return func(ctx *Context[any]) bool { return ctx.CancelTimer(id) }
	}
	var after, received, every TimerID
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		after = ctx.After(time.Second, "a")
		received = ctx.After(time.Millisecond, "r")
		every = ctx.Every(time.Second, "e")
	})
	eng.Advance(time.Millisecond) // r is received
	before := len(eng.Records())

	cases := []struct {
		name string
		id   TimerID
		want bool
	}{
		{"zero", 0, false},
		{"unknown", 99, false},
		{"armed After", after, true},
		{"cancelled After", after, false},
		{"received After", received, false},
		{"Every", every, true},
		{"cancelled Every", every, false},
	}
	for _, tc := range cases {
		if got := call(cancel(tc.id)); got != tc.want {
			t.Errorf("%s: CancelTimer(%d) = %v, want %v", tc.name, tc.id, got, tc.want)
		}
	}
	if got := a.msgs(); !slices.Equal(got, []any{"r"}) {
		t.Errorf("received %v, want [r]", got)
	}
	inc := "inc=1"
	want := [][]string{
		{"actor=" + pid.String(), inc, "timer=" + itoa(after), "op=cancel"},
		{"actor=" + pid.String(), inc, "timer=" + itoa(every), "op=cancel"},
	}
	if got := recordsOf(eng.Records()[before:], "lockstep.timer"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.timer records:\n got %q\nwant %q", got, want)
	}

	send(t, sys, pid, any("panic"))
	eng.RunUntilIdle()
	if _, inc := cellOf(t, sys, pid).lifecycle(); inc != 2 {
		t.Fatalf("incarnation %d, want 2", inc)
	}
	before = len(eng.Records())
	if call(cancel(old)) {
		t.Errorf("CancelTimer(%d) of inc 1's timer in inc 2 = true, want false", old)
	}
	if got := recordsOf(eng.Records()[before:], "lockstep.timer"); len(got) != 0 {
		t.Errorf("lockstep.timer records %q for a false result, want none", got)
	}
}

// itoa formats a TimerID in decimal.
func itoa(id TimerID) string { return strconv.FormatUint(uint64(id), 10) }

// AT-TMR-11: TMR-090 on the local engine, where the runtime timer of each After(0) fires on
// another goroutine and races CancelTimer's Cancel: every CancelTimer returns true, x never
// reaches Receive, and an x that a callback enqueued would be dropped as timer-cancelled when
// popped. Run with -race.
func TestATTMR11CancelVersusConcurrentFiring(t *testing.T) {
	const n = 10_000
	sys, rec := newRecordingSystem(t)
	done := make(chan struct{})
	falses, xs := 0, 0
	handled := 0
	a := &fnActor[any]{}
	a.started = func(ctx *Context[any]) {
		if err := Send(ctx, ctx.Self(), any("next")); err != nil {
			panic(err)
		}
	}
	a.receive = func(ctx *Context[any], msg any) {
		switch msg {
		case "x":
			xs++
		case "done":
			close(done)
		case "next":
			id := ctx.After(0, "x")
			if !ctx.CancelTimer(id) {
				falses++
			}
			next := any("next")
			if handled++; handled == n {
				next = "done" // queued behind every x a callback enqueued before its CancelTimer
			}
			if err := Send(ctx, ctx.Self(), next); err != nil {
				panic(err)
			}
		}
	}
	if _, err := Spawn(sys, "a", actorOf[any](a)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the actor did not handle its self messages within 10s")
	}
	// done closed in a turn after every other turn of A: handled, falses and xs are final.
	if handled != n || falses != 0 || xs != 0 {
		t.Errorf("handled %d, CancelTimer false %d times, x received %d times; want %d, 0, 0", handled, falses, xs, n)
	}
	var cancels, fires, drops int
	for _, r := range rec.Records() {
		switch {
		case r.Kind == "lockstep.timer" && r.Attrs[3].Value == "cancel":
			cancels++
		case r.Kind == "lockstep.timer" && r.Attrs[3].Value == "fire":
			fires++
		case r.Kind == "lockstep.drop" && r.Attrs[1].Value == dropTimerCancelled:
			drops++
		}
	}
	if cancels != n || drops != fires {
		t.Errorf("%d cancel records, %d fire records, %d timer-cancelled drops; want %d cancels and a drop per fire", cancels, fires, drops, n)
	}
}

// AT-TMR-12: TMR-100. After and Every inside Stopped return 0, arm nothing and record op refused
// with timer 0, d and repeat; nothing fires.
func TestATTMR12RefusedInStopped(t *testing.T) {
	sys, eng, clk := newPendingClockSystem(t)
	ids := []TimerID{99, 99}
	a := &timerStopper{timerActor: &timerActor{}, stopped: func(ctx *Context[any], _ StopReason) {
		ids[0] = ctx.After(time.Second, "x")
		ids[1] = ctx.Every(time.Second, "x")
	}}
	pid, err := Spawn(sys, "a", actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, pid.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	eng.Advance(2 * time.Second)

	if !slices.Equal(ids, []TimerID{0, 0}) {
		t.Errorf("After and Every in Stopped returned %v, want [0 0]", ids)
	}
	if clk.pending != 0 || len(eng.Labels()) != 0 {
		t.Errorf("armed %q, %d pending; want nothing", eng.Labels(), clk.pending)
	}
	if len(a.seen) != 0 {
		t.Errorf("received %v, want nothing", a.seen)
	}
	want := [][]string{
		{"actor=" + pid.String(), "inc=1", "timer=0", "op=refused", "d=1s", "repeat=false"},
		{"actor=" + pid.String(), "inc=1", "timer=0", "op=refused", "d=1s", "repeat=true"},
	}
	if got := recordsOf(eng.Records(), "lockstep.timer"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.timer records:\n got %q\nwant %q", got, want)
	}
}

// AT-SUP-12: SUP-100, TMR-100. Inside Stopped, After returns 0, ctx.Stop does nothing, Send
// reaches the observer and Own registers a resource that is closed once; the actor stops once.
func TestATSUP12ContextInsideStopped(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obsO, obsP, closed []string
	o, err := Spawn(sys, "o", actorOf[any](&supProbe{obs: &obsO}))
	if err != nil {
		t.Fatal(err)
	}
	id := TimerID(99)
	r := &closer{name: "r", log: &closed}
	p, err := Spawn(sys, "p", actorOf[any](&supProbe{
		obs: &obsP,
		onStop: func(ctx *Context[any], _ StopReason) {
			id = ctx.After(time.Second, "m")
			ctx.Stop()
			if err := Send(ctx, o, any("from stopped")); err != nil {
				panic(err)
			}
			ctx.Own(r)
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	eng.Advance(2 * time.Second)

	if id != 0 {
		t.Errorf("After inside Stopped = %d, want 0", id)
	}
	if want := []string{"started /user/o 1", "from stopped"}; !slices.Equal(obsO, want) {
		t.Errorf("the observer received %q, want %q", obsO, want)
	}
	if want := []string{"r"}; !slices.Equal(closed, want) {
		t.Errorf("closed %q, want %q", closed, want)
	}
	if got, want := stopRecordsOf(eng.Records()), []string{p.String() + " normal"}; !slices.Equal(got, want) {
		t.Errorf("lockstep.stop records %q, want %q", got, want)
	}
}

// AT-MBX-22: MBX-160, MBX-170, TMR-080. One turn arms After(0, tick), sends m to itself and
// panics; the restart keeps m queued and drops the tick of inc 1 when it is popped. The tick fires
// while the failed actor waits for its supervisor's decision, before the restart ends inc 1 (a
// timer that had not fired by then would be cancelled and enqueue nothing).
func TestATMBX22RestartKeepsSentDropsStaleSelf(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	a := &timerActor{onMsg: func(ctx *Context[any], msg any) {
		if msg == "go" {
			ctx.After(0, "tick")
			if err := Send(ctx, ctx.Self(), any("m")); err != nil {
				panic(err)
			}
			panic("boom")
		}
	}}
	pid := spawnTimerActor(t, sys, eng, a)
	c := cellOf(t, sys, pid)
	send(t, sys, pid, any("go"))
	stepUntilSuspended(t, eng, c)
	eng.Advance(0) // the tick fires into the mailbox behind m; then the restart runs

	if _, inc := c.lifecycle(); inc != 2 {
		t.Fatalf("incarnation %d, want 2", inc)
	}
	if got, want := a.msgs(), []any{"go", "m"}; !slices.Equal(got, want) {
		t.Errorf("received %v, want %v", got, want)
	}
	tr := eng.Engine().Tracer
	want := [][]string{{"to=" + pid.String(), "reason=incarnation-ended", "inc=1", "msg=" + tr.Describe("tick")}}
	if got := recordsOf(eng.Records(), "lockstep.drop"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.drop records:\n got %q\nwant %q", got, want)
	}
	if n := countRecords(timerRecords(eng.Records()), "timer=1 op=fire msg="+tr.Describe("tick")); n != 1 {
		t.Errorf("%d fire records, want 1", n)
	}
}
