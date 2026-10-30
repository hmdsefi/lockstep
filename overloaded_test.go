// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

var _ Signal = Overloaded{}

// gotOverload is one Overloaded signal and the subscriber that got it.
type gotOverload struct {
	who string
	sig Overloaded
}

// overloadWatcher is an any actor named who that appends every Overloaded signal it gets to log,
// in the order of delivery, runs the funcs it receives and panics on a string. onStart, when set, runs in Started.
type overloadWatcher struct {
	who     string
	log     *[]gotOverload
	onStart func(ctx *Context[any])
}

func (a *overloadWatcher) Started(ctx *Context[any]) {
	if a.onStart != nil {
		a.onStart(ctx)
	}
}

func (a *overloadWatcher) Receive(ctx *Context[any], m any) {
	switch m := m.(type) {
	case string:
		panic("overloadWatcher panic: " + m)
	case func(*Context[any]):
		m(ctx)
	}
}

func (a *overloadWatcher) Signal(_ *Context[any], sig Signal) {
	if o, ok := sig.(Overloaded); ok {
		*a.log = append(*a.log, gotOverload{a.who, o})
	}
}

// spawnWatcher spawns an overloadWatcher named who, runs the engine until it started and returns
// its PID.
func spawnWatcher(t *testing.T, sys *System, eng *enginetest.Engine, a *overloadWatcher) PID[any] {
	t.Helper()
	pid, err := Spawn(sys, a.who, actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	return pid
}

// subscribe makes pid's incarnation call SubscribeOverload in a turn.
func subscribe(t *testing.T, sys *System, eng *enginetest.Engine, pid PID[any]) {
	t.Helper()
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.SubscribeOverload() })
}

// budgetChange returns the change of the budget source to on, with the combined state shedding, the
// measured bytes live and the thresholds 100 and 80.
func budgetChange(on, shedding bool, live uint64) engine.AdmissionChange {
	return engine.AdmissionChange{Source: engine.SourceBudget, On: on, Shedding: shedding, Live: live, High: 100, Low: 80}
}

// wantGot fails the test unless got is want.
func wantGot(t *testing.T, got, want []gotOverload) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("Overloaded signals:\n got %v\nwant %v", got, want)
	}
}

func TestOverloadReasonString(t *testing.T) {
	for _, tt := range []struct {
		r    OverloadReason
		want string
	}{
		{OverloadMemory, "memory"},
		{OverloadBudget, "budget"},
		{OverloadPressure, "pressure"},
		{OverloadReason(9), "OverloadReason(9)"},
		{OverloadReason(0), "OverloadReason(0)"},
	} {
		if got := tt.r.String(); got != tt.want {
			t.Errorf("OverloadReason(%d).String() = %q, want %q", uint8(tt.r), got, tt.want)
		}
	}
	// The values equal those of engine.AdmissionSource.
	for _, p := range [...]struct {
		r OverloadReason
		s engine.AdmissionSource
	}{{OverloadMemory, engine.SourceMemory}, {OverloadBudget, engine.SourceBudget}, {OverloadPressure, engine.SourcePressure}} {
		if uint8(p.r) != uint8(p.s) {
			t.Errorf("%v = %d, want engine's %d", p.r, p.r, p.s)
		}
	}
}

// OVL-170: a restart ends the subscription; the new incarnation gets no signal until it
// subscribes again.
func TestOVL170SubscriptionEndsWithIncarnation(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var log []gotOverload
	pid := spawnWatcher(t, sys, eng, &overloadWatcher{who: "s", log: &log})
	subscribe(t, sys, eng, pid)
	send(t, sys, pid, any("panic"))
	eng.RunUntilIdle()
	if _, inc := cellOf(t, sys, pid).lifecycle(); inc != 2 {
		t.Fatalf("incarnation after the panic = %d, want 2", inc)
	}

	eng.ChangeAdmission(pressure(true, true))
	eng.RunUntilIdle()
	eng.ChangeAdmission(pressure(false, false))
	eng.RunUntilIdle()
	wantGot(t, log, nil)

	// Subscribing again, here during a period, starts the signals of the new incarnation.
	eng.ChangeAdmission(pressure(true, true))
	subscribe(t, sys, eng, pid)
	eng.ChangeAdmission(pressure(false, false))
	eng.RunUntilIdle()
	wantGot(t, log, []gotOverload{
		{"s", Overloaded{Active: true, Reason: OverloadPressure}},
		{"s", Overloaded{Active: false, Reason: OverloadPressure}},
	})
}

// OVL-170: a stop ends the subscription too.
func TestOVL170StopRemovesSubscription(t *testing.T) {
	sys, eng := newManualSystem(t)
	var log []gotOverload
	a := spawnWatcher(t, sys, eng, &overloadWatcher{who: "a", log: &log})
	b := spawnWatcher(t, sys, eng, &overloadWatcher{who: "b", log: &log})
	subscribe(t, sys, eng, a)
	subscribe(t, sys, eng, b)
	if n := sys.overloadSubscribers(); n != 2 {
		t.Fatalf("subscribers = %d, want 2", n)
	}
	if err := Stop(sys, a.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if n := sys.overloadSubscribers(); n != 1 {
		t.Errorf("subscribers after A stopped = %d, want 1", n)
	}
	eng.ChangeAdmission(pressure(true, true))
	eng.RunUntilIdle()
	wantGot(t, log, []gotOverload{{"b", Overloaded{Active: true, Reason: OverloadPressure}}})
}

// OVL-170: a signal queued for an incarnation that has ended does not reach the next one, even if
// that one subscribed.
func TestOVL170StaleSignalIsNotDelivered(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var log []gotOverload
	pid := spawnWatcher(t, sys, eng, &overloadWatcher{who: "s", log: &log})
	send(t, sys, pid, any("panic"))
	eng.RunUntilIdle()
	subscribe(t, sys, eng, pid) // incarnation 2

	c := cellOf(t, sys, pid)
	c.pushSystem(signalMsg{sig: Overloaded{Active: true, Reason: OverloadPressure}, seq: 1, inc: 1}, nil)
	c.pushSystem(signalMsg{sig: Overloaded{Active: true, Reason: OverloadMemory}, seq: 1, inc: 2}, nil)
	eng.RunUntilIdle()
	wantGot(t, log, []gotOverload{{"s", Overloaded{Active: true, Reason: OverloadMemory}}})
}

// OVL-180: one signal per change of the combined state, in subscription order; a source that turns
// on while shedding, or off while another stays on, signals nothing. Reason and values come from
// the change that caused the transition.
func TestOVL180SignalsInSubscriptionOrder(t *testing.T) {
	sys, eng := newManualSystem(t)
	var log []gotOverload
	s1 := spawnWatcher(t, sys, eng, &overloadWatcher{who: "s1", log: &log})
	s2 := spawnWatcher(t, sys, eng, &overloadWatcher{who: "s2", log: &log})
	subscribe(t, sys, eng, s1)
	subscribe(t, sys, eng, s2)

	eng.ChangeAdmission(pressure(true, true))
	eng.RunUntilIdle()
	eng.ChangeAdmission(pressure(false, false))
	eng.RunUntilIdle()
	wantGot(t, log, []gotOverload{
		{"s1", Overloaded{Active: true, Reason: OverloadPressure}},
		{"s2", Overloaded{Active: true, Reason: OverloadPressure}},
		{"s1", Overloaded{Active: false, Reason: OverloadPressure}},
		{"s2", Overloaded{Active: false, Reason: OverloadPressure}},
	})

	log = nil
	for _, c := range []engine.AdmissionChange{
		budgetChange(true, true, 90),
		pressure(true, true),  // a second source on while shedding
		pressure(false, true), // off while the budget stays on
		budgetChange(false, false, 70),
	} {
		eng.ChangeAdmission(c)
		eng.RunUntilIdle()
	}
	wantGot(t, log, []gotOverload{
		{"s1", Overloaded{Active: true, Reason: OverloadBudget, Live: 90, High: 100, Low: 80}},
		{"s2", Overloaded{Active: true, Reason: OverloadBudget, Live: 90, High: 100, Low: 80}},
		{"s1", Overloaded{Active: false, Reason: OverloadBudget, Live: 70, High: 100, Low: 80}},
		{"s2", Overloaded{Active: false, Reason: OverloadBudget, Live: 70, High: 100, Low: 80}},
	})
}

// OVL-190: a subscription while shedding queues Active: true with the values of the change that
// started the period, once; the end of the period queues one Active: false.
func TestOVL190SubscribeWhileShedding(t *testing.T) {
	sys, eng := newManualSystem(t)
	var log []gotOverload
	pid := spawnWatcher(t, sys, eng, &overloadWatcher{who: "a", log: &log})

	eng.ChangeAdmission(budgetChange(true, true, 90))
	eng.ChangeAdmission(pressure(true, true)) // not the change that started the period
	subscribe(t, sys, eng, pid)
	subscribe(t, sys, eng, pid)
	want := Overloaded{Active: true, Reason: OverloadBudget, Live: 90, High: 100, Low: 80}
	wantGot(t, log, []gotOverload{{"a", want}})

	eng.ChangeAdmission(budgetChange(false, true, 70))
	eng.ChangeAdmission(pressure(false, false))
	eng.RunUntilIdle()
	wantGot(t, log, []gotOverload{
		{"a", want},
		{"a", Overloaded{Active: false, Reason: OverloadPressure}},
	})

	// A subscription while admitting queues nothing, and the next period is its own.
	log = nil
	subscribe(t, sys, eng, pid)
	wantGot(t, log, nil)
}

// OVL-190: the signal of a subscription during a period precedes the signal that ends it.
func TestOVL190InitialSignalPrecedesLaterOnes(t *testing.T) {
	sys, eng := newManualSystem(t)
	var log []gotOverload
	pid := spawnWatcher(t, sys, eng, &overloadWatcher{who: "a", log: &log})
	eng.ChangeAdmission(pressure(true, true))
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.SubscribeOverload() })
	eng.ChangeAdmission(pressure(false, false))
	eng.RunUntilIdle()
	wantGot(t, log, []gotOverload{
		{"a", Overloaded{Active: true, Reason: OverloadPressure}},
		{"a", Overloaded{Active: false, Reason: OverloadPressure}},
	})
}

// UnsubscribeOverload cancels the subscription; with none it does nothing.
func TestUnsubscribeOverload(t *testing.T) {
	sys, eng := newManualSystem(t)
	var log []gotOverload
	pid := spawnWatcher(t, sys, eng, &overloadWatcher{who: "a", log: &log})
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.UnsubscribeOverload() }) // none: no-op

	subscribe(t, sys, eng, pid)
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.UnsubscribeOverload() })
	if n := sys.overloadSubscribers(); n != 0 {
		t.Errorf("subscribers after UnsubscribeOverload = %d, want 0", n)
	}
	eng.ChangeAdmission(pressure(true, true))
	eng.RunUntilIdle()
	wantGot(t, log, nil)
}

// A signal queued before UnsubscribeOverload is not delivered after it: the change and the
// unsubscription happen in one turn, so the signal is still queued when the actor unsubscribes.
func TestUnsubscribeOverloadDropsQueuedSignal(t *testing.T) {
	sys, eng := newManualSystem(t)
	var log []gotOverload
	pid := spawnWatcher(t, sys, eng, &overloadWatcher{who: "a", log: &log})
	subscribe(t, sys, eng, pid)
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		eng.ChangeAdmission(pressure(true, true))
		ctx.UnsubscribeOverload()
	})
	wantGot(t, log, nil)
}

// OVL-170, OVL-190: a signal queued for an earlier subscription of the incarnation is not
// delivered after it unsubscribes and subscribes again.
func TestOVL190ResubscribeDropsEarlierSignals(t *testing.T) {
	t.Run("subscribed again while shedding", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var log []gotOverload
		pid := spawnWatcher(t, sys, eng, &overloadWatcher{who: "a", log: &log})
		subscribe(t, sys, eng, pid)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			eng.ChangeAdmission(pressure(true, true))
			ctx.UnsubscribeOverload()
			ctx.SubscribeOverload()
		})
		wantGot(t, log, []gotOverload{{"a", Overloaded{Active: true, Reason: OverloadPressure}}})
	})
	t.Run("subscribed again while admitting", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var log []gotOverload
		pid := spawnWatcher(t, sys, eng, &overloadWatcher{who: "a", log: &log})
		subscribe(t, sys, eng, pid)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			eng.ChangeAdmission(pressure(true, true))
			ctx.UnsubscribeOverload()
			eng.ChangeAdmission(pressure(false, false))
			ctx.SubscribeOverload()
		})
		wantGot(t, log, nil)
	})
}

// The subscription calls need a turn of the incarnation, and Stopped, which runs after the end of
// the incarnation's subscription, subscribes nothing.
func TestSubscribeOverloadInStoppedAndOutsideTurn(t *testing.T) {
	sys, eng := newManualSystem(t)
	var ctxs []*Context[any]
	var log []gotOverload
	a := &overloadWatcher{who: "a", log: &log, onStart: func(ctx *Context[any]) { ctxs = append(ctxs, ctx) }}
	pid := spawnWatcher(t, sys, eng, a)
	want := "lockstep: Context of " + pid.Address().String() + " used outside its turn"
	if v := mustPanic(t, func() { ctxs[0].SubscribeOverload() }); v != want {
		t.Errorf("SubscribeOverload outside a turn panicked with %v, want %q", v, want)
	}
	if v := mustPanic(t, func() { ctxs[0].UnsubscribeOverload() }); v != want {
		t.Errorf("UnsubscribeOverload outside a turn panicked with %v, want %q", v, want)
	}

	stopper, err := Spawn(sys, "b", actorOf[any](&fnActorStopper{
		started: func(*Context[any]) {},
		stopped: func(ctx *Context[any], _ StopReason) { ctx.SubscribeOverload() },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, stopper.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if n := sys.overloadSubscribers(); n != 0 {
		t.Errorf("subscribers after a Stopped that subscribed = %d, want 0", n)
	}
}

// OVL-200: an actor without SignalReceiver may subscribe; it does not fail, and no signal reaches
// Receive.
func TestOVL200SubscriberWithoutSignal(t *testing.T) {
	sys, eng := newManualSystem(t)
	var received []any
	pid, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
		receive: func(ctx *Context[any], m any) {
			if f, ok := m.(func(*Context[any])); ok {
				f(ctx)
				return
			}
			received = append(received, m)
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	subscribe(t, sys, eng, pid)
	send(t, sys, pid, any("hello"))
	eng.RunUntilIdle()
	if len(received) != 1 {
		t.Fatalf("Receive got %v before any change, want [hello]", received)
	}
	received = nil

	eng.ChangeAdmission(pressure(true, true))
	eng.RunUntilIdle()
	eng.ChangeAdmission(pressure(false, false))
	eng.RunUntilIdle()
	if len(received) != 0 {
		t.Errorf("Receive got %v, want nothing", received)
	}
	if f := eng.Failures(); len(f) != 0 {
		t.Errorf("Failures() = %v, want none", f)
	}
	if st, inc := cellOf(t, sys, pid).lifecycle(); st != supervise.Running || inc != 1 {
		t.Errorf("the subscriber is %v in incarnation %d, want Running in 1", st, inc)
	}
}

// lockedWatcher is an any actor that subscribes in Started and keeps the Overloaded signals it
// gets, safe to read from the test's goroutine.
type lockedWatcher struct {
	mu  sync.Mutex
	got []bool // Active of each signal, in delivery order
}

func (a *lockedWatcher) Started(ctx *Context[any]) { ctx.SubscribeOverload() }

func (*lockedWatcher) Receive(*Context[any], any) {}

func (a *lockedWatcher) Signal(_ *Context[any], sig Signal) {
	if o, ok := sig.(Overloaded); ok {
		a.mu.Lock()
		a.got = append(a.got, o.Active)
		a.mu.Unlock()
	}
}

func (a *lockedWatcher) signals() []bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.got)
}

// OVL-180, OVL-190: subscriptions made while changes are reported get every later signal once, and
// a subscription during a period gets its Active: true first. Each subscriber's signals alternate,
// starting with Active: true and ending with the false of the last change.
func TestOVL190SubscribeRacesWithChanges(t *testing.T) {
	sys, _, adm := newAdmissionSystem(t)
	const watchers, toggles = 8, 300
	var as [watchers]*lockedWatcher
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range toggles {
			adm.change(pressure(true, true))
			adm.change(pressure(false, false))
		}
	}()
	for i := range as {
		as[i] = &lockedWatcher{}
		if _, err := Spawn(sys, "w"+strconv.Itoa(i), actorOf[any](as[i])); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the changes did not finish within 10 s")
	}
	for i, a := range as {
		// The last change turned shedding off: what is queued ends with an Active: false.
		waitUntil(t, "the signals of watcher "+strconv.Itoa(i), func() bool {
			got := a.signals()
			return len(got) == 0 || !got[len(got)-1]
		})
		got := a.signals()
		for j, active := range got {
			if active != (j%2 == 0) {
				t.Errorf("watcher %d: signal %d has Active %v, want alternating from true: %v", i, j, active, got)
				break
			}
		}
	}
}

// OVL-170: the end of a subscription removes it from the list, found from both ends, the newest
// first. The lists hold x's subscription twice, which a real list never does: a match at the
// newest end wins, then one at the oldest end, before any deeper match. Subscribers stop newest
// first in a shutdown, so a search from the oldest end only would take a step per remaining
// subscription.
func TestOverloadSubsRemoveSearchesBothEnds(t *testing.T) {
	x, o1, o2 := &cell[any]{}, &cell[any]{}, &cell[any]{}
	s := func(c actorCell, inc uint32, gen uint64) overloadSub { return overloadSub{cell: c, inc: inc, gen: gen} }
	for _, tc := range []struct {
		name     string
		in, want []overloadSub
	}{
		{"newest end first", []overloadSub{s(x, 1, 1), s(o1, 1, 2), s(o2, 1, 3), s(x, 1, 4)},
			[]overloadSub{s(x, 1, 1), s(o1, 1, 2), s(o2, 1, 3)}},
		{"oldest end before deeper", []overloadSub{s(x, 1, 1), s(o1, 1, 2), s(x, 1, 3), s(o2, 1, 4)},
			[]overloadSub{s(o1, 1, 2), s(x, 1, 3), s(o2, 1, 4)}},
		{"middle", []overloadSub{s(o1, 1, 1), s(x, 1, 2), s(o2, 1, 3)}, []overloadSub{s(o1, 1, 1), s(o2, 1, 3)}},
		{"another incarnation", []overloadSub{s(x, 1, 1), s(x, 2, 2)}, []overloadSub{s(x, 2, 2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backing := tc.in[:len(tc.in):len(tc.in)]
			o := &overloadSubs{list: tc.in}
			o.remove(x, 1)
			if got := o.snapshot(); !slices.Equal(got, tc.want) {
				t.Errorf("subscriptions = %v, want %v", got, tc.want)
			}
			if last := backing[len(backing)-1]; last != (overloadSub{}) {
				t.Errorf("the freed slot holds %v, want the zero subscription", last)
			}
		})
	}
}

// overloadSubscribers returns the number of subscriptions of the system.
func (s *System) overloadSubscribers() int { return len(s.ovl.snapshot()) }
