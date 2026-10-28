// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// supProbe is SUP §10's Probe for the manual engine. It observes "started <path> <inc>",
// "stopped <path> <reason>", "terminated <path> <reason>" for a Terminated signal, any other
// signal, and every message it receives; it panics on "panic", calls ctx.Fail(errProbe) on "fail"
// and ctx.Stop() on "stop". onStart, when set, runs first in Started; onStop, when set, runs last
// in Stopped; receive, when set, replaces the message handling.
type supProbe struct {
	obs     *[]string
	onStart func(ctx *Context[any])
	onStop  func(ctx *Context[any], r StopReason)
	receive func(ctx *Context[any], msg any)
}

// errProbe is the error a supProbe fails with on "fail".
var errProbe = errors.New("probe failed")

func (p *supProbe) Started(ctx *Context[any]) {
	if p.onStart != nil {
		p.onStart(ctx)
	}
	*p.obs = append(*p.obs, "started "+ctx.Self().Address().Path()+" "+strconv.FormatUint(uint64(ctx.inc), 10))
}

func (p *supProbe) Stopped(ctx *Context[any], r StopReason) {
	*p.obs = append(*p.obs, "stopped "+ctx.Self().Address().Path()+" "+r.String())
	if p.onStop != nil {
		p.onStop(ctx, r)
	}
}

func (p *supProbe) Signal(_ *Context[any], sig Signal) {
	if t, ok := sig.(Terminated); ok {
		*p.obs = append(*p.obs, "terminated "+t.Addr.Path()+" "+t.Reason.String())
		return
	}
	*p.obs = append(*p.obs, fmt.Sprint(sig))
}

func (p *supProbe) Receive(ctx *Context[any], msg any) {
	if p.receive != nil {
		p.receive(ctx, msg)
		return
	}
	*p.obs = append(*p.obs, fmt.Sprint(msg))
	switch msg {
	case "panic":
		panic("probe panic")
	case "fail":
		ctx.Fail(errProbe)
	case "stop":
		ctx.Stop()
	}
}

// supervised spawns a parent "s" from sys whose Started spawns one child per factory, named c1,
// c2, ... in that order, and then holds the parent, so every notice its children push stays in
// its system queue. It runs the engine until idle (the children begin) and returns the parent's
// cell and the children.
func supervised(t *testing.T, sys *System, eng *enginetest.Engine, factories ...func() Actor[any]) (*cell[any], []PID[any]) {
	t.Helper()
	var kids []PID[any]
	parent, err := Spawn(sys, "s", actorOf[any](&fnActor[any]{
		started: func(ctx *Context[any]) {
			for i, f := range factories {
				pid, err := Spawn(ctx, "c"+strconv.Itoa(i+1), f)
				if err != nil {
					panic(err)
				}
				kids = append(kids, pid)
			}
			ctx.cell.holdTurns() // the turn pops nothing more, and no later turn runs
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if len(kids) != len(factories) {
		t.Fatalf("the parent spawned %d children, want %d", len(kids), len(factories))
	}
	return cellOf(t, sys, parent), kids
}

// failedNotices empties c's system queue, which a hold keeps c from processing, and returns the
// failed notices in it, in queue order.
func failedNotices(c *cell[any]) []failed {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []failed
	for {
		m, ok := c.sysq.Pop()
		if !ok {
			return out
		}
		if f, ok := m.(failed); ok {
			out = append(out, f)
		}
	}
}

// AT-SUP-03: SUP-020, SUP-030. Spawn does not run the factory; begin records lockstep.start with
// inc 1 before Started, and a message sent right after Spawn is received after Started.
func TestATSUP03StartSequence(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	calls := 0
	p := &supProbe{obs: &obs}
	p.onStart = func(*Context[any]) {
		obs = append(obs, fmt.Sprint(recordsOf(eng.Records(), "lockstep.start")))
	}
	pid, err := Spawn(sys, "p", func() Actor[any] { calls++; return p })
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("the factory ran %d times inside Spawn, want 0", calls)
	}
	if st, inc := cellOf(t, sys, pid).lifecycle(); st != supervise.Starting || inc != 0 {
		t.Errorf("after Spawn: state %v, inc %d; want starting, 0", st, inc)
	}
	send(t, sys, pid, any("x"))
	eng.RunUntilIdle()

	start := fmt.Sprint([][]string{{"actor=" + pid.String(), "inc=1"}})
	want := []string{start, "started /user/p 1", "x"}
	if !slices.Equal(obs, want) {
		t.Errorf("observations:\n got %q\nwant %q", obs, want)
	}
	if calls != 1 {
		t.Errorf("factory calls = %d, want 1", calls)
	}
	if st, inc := cellOf(t, sys, pid).lifecycle(); st != supervise.Running || inc != 1 {
		t.Errorf("after the start: state %v, inc %d; want running, 1", st, inc)
	}
}

// AT-SUP-15: SUP-140, SUP-145. The first Fail of a handler wins, a panic beats Fail, and a panic
// becomes a *PanicError with a deterministic text and a stack.
func TestATSUP15PanicVersusFail(t *testing.T) {
	sys, eng := newManualSystem(t)
	e1, e2 := errors.New("e1"), errors.New("e2")
	xErr := errors.New("x")
	handlers := []func(ctx *Context[any]){
		func(ctx *Context[any]) { ctx.Fail(e1); ctx.Fail(e2) },
		func(ctx *Context[any]) { ctx.Fail(e1); panic(7) },
		func(*Context[any]) { panic(&struct{}{}) },
		func(*Context[any]) { panic(xErr) },
		func(ctx *Context[any]) { ctx.Fail(nil) },
	}
	var factories []func() Actor[any]
	for _, h := range handlers {
		factories = append(factories, actorOf[any](&fnActor[any]{
			receive: func(ctx *Context[any], _ any) { h(ctx) },
		}))
	}
	parent, kids := supervised(t, sys, eng, factories...)
	for _, k := range kids {
		send(t, sys, k, any("go"))
	}
	eng.RunUntilIdle()

	notices := failedNotices(parent)
	if len(notices) != len(kids) {
		t.Fatalf("%d failed notices, want %d: %+v", len(notices), len(kids), notices)
	}
	for i, n := range notices {
		if n.child != kids[i].Address() || n.inc != 1 || n.source != supervise.InReceive {
			t.Errorf("notice %d: child %v, inc %d, source %v; want %v, 1, receive", i, n.child, n.inc, n.source, kids[i])
		}
	}
	if notices[0].err != e1 {
		t.Errorf("Fail(e1), Fail(e2): error %v, want e1", notices[0].err)
	}
	wantPanics := []struct {
		i     int
		text  string
		value any
	}{
		{1, "lockstep: panic: 7", 7},
		{2, "lockstep: panic: *struct {}", nil},
		{3, "lockstep: panic: x", xErr},
		{4, "lockstep: panic: lockstep: Fail called with nil error", "lockstep: Fail called with nil error"},
	}
	for _, w := range wantPanics {
		var pe *PanicError
		if !errors.As(notices[w.i].err, &pe) {
			t.Errorf("handler %d: error %v (%T), want a *PanicError", w.i, notices[w.i].err, notices[w.i].err)
			continue
		}
		if pe.Error() != w.text {
			t.Errorf("handler %d: Error() = %q, want %q", w.i, pe.Error(), w.text)
		}
		if w.value != nil && pe.Value != w.value {
			t.Errorf("handler %d: Value = %v, want %v", w.i, pe.Value, w.value)
		}
		if len(pe.Stack) == 0 {
			t.Errorf("handler %d: empty Stack", w.i)
		}
	}
	if got := errors.Unwrap(notices[3].err); got != xErr {
		t.Errorf("errors.Unwrap of panic(x) = %v, want x", got)
	}
}

// SUP-150, SUP-010: a Receive panic records lockstep.fail, suspends the cell and pushes one
// failed notice to the supervisor. The failing message is consumed; later messages stay queued.
func TestSUP150FailureSuspends(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	parent, kids := supervised(t, sys, eng, actorOf[any](&supProbe{obs: &obs}))
	child := kids[0]
	send(t, sys, child, any("panic"))
	send(t, sys, child, any("a"))
	eng.RunUntilIdle()

	c := cellOf(t, sys, child)
	if st, inc := c.lifecycle(); st != supervise.Suspended || inc != 1 {
		t.Errorf("child: state %v, inc %d; want suspended, 1", st, inc)
	}
	if want := []string{"started /user/s/c1 1", "panic"}; !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
	if n := queued(c); n != 1 {
		t.Errorf("queued user messages = %d, want 1 (\"a\"; \"panic\" is consumed)", n)
	}
	notices := failedNotices(parent)
	if len(notices) != 1 {
		t.Fatalf("failed notices %+v, want one", notices)
	}
	n := notices[0]
	if n.child != child.Address() || n.inc != 1 || n.source != supervise.InReceive {
		t.Errorf("notice: child %v, inc %d, source %v; want %v, 1, receive", n.child, n.inc, n.source, child)
	}
	if n.err == nil || n.err.Error() != "lockstep: panic: probe panic" {
		t.Errorf("notice error %v, want lockstep: panic: probe panic", n.err)
	}
	want := [][]string{{"actor=" + child.String(), "inc=1", "source=receive", "err=lockstep: panic: probe panic"}}
	if got := recordsOf(eng.Records(), "lockstep.fail"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.fail records:\n got %q\nwant %q", got, want)
	}

	// A Suspended cell pops no user message in a later turn either.
	send(t, sys, child, any("b"))
	eng.RunUntilIdle()
	if n := queued(c); n != 2 || len(obs) != 2 {
		t.Errorf("after another send: queued %d, observations %q; want 2 queued and nothing new", n, obs)
	}
}

// SUP-040: a factory that returns nil, or panics, is a failure with source factory; no
// incarnation begins (inc stays 0), so no Stopped runs when the cell is stopped later.
func TestSUP040FactoryFailure(t *testing.T) {
	sys, eng := newManualSystem(t)
	parent, kids := supervised(t, sys, eng,
		func() Actor[any] { return nil },
		func() Actor[any] { panic("factory boom") },
	)
	eng.RunUntilIdle()

	want := [][]string{
		{"actor=" + kids[0].String(), "inc=0", "source=factory", "err=lockstep: factory returned nil actor"},
		{"actor=" + kids[1].String(), "inc=0", "source=factory", "err=lockstep: panic: factory boom"},
	}
	if got := recordsOf(eng.Records(), "lockstep.fail"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.fail records:\n got %q\nwant %q", got, want)
	}
	notices := failedNotices(parent)
	if len(notices) != 2 {
		t.Fatalf("failed notices %+v, want two", notices)
	}
	for i, n := range notices {
		if n.child != kids[i].Address() || n.inc != 0 || n.source != supervise.InFactory {
			t.Errorf("notice %d: child %v, inc %d, source %v; want %v, 0, factory", i, n.child, n.inc, n.source, kids[i])
		}
	}
	if !errors.Is(notices[0].err, errNilActor) || notices[0].err.Error() != "lockstep: factory returned nil actor" {
		t.Errorf("nil factory error %v", notices[0].err)
	}
	var pe *PanicError
	if !errors.As(notices[1].err, &pe) || pe.Value != "factory boom" {
		t.Errorf("panicking factory error %v, want a *PanicError of factory boom", notices[1].err)
	}
	for _, k := range kids {
		c := cellOf(t, sys, k)
		if st, inc := c.lifecycle(); st != supervise.Suspended || inc != 0 || c.ctx != nil {
			t.Errorf("%v: state %v, inc %d, ctx %p; want suspended, 0, nil", k, st, inc, c.ctx)
		}
	}

	// Stopping a cell whose factory failed calls no Stopped and records inc 0.
	if err := Stop(sys, kids[0].Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	stops := recordsOf(eng.Records(), "lockstep.stop")
	if len(stops) != 1 || !slices.Equal(stops[0], []string{"actor=" + kids[0].String(), "inc=0", "reason=normal", "err="}) {
		t.Errorf("lockstep.stop records %q", stops)
	}
}

// SUP-040, SUP-100: a failure in Started has source started; the incarnation exists, so stopping
// the suspended cell calls Stopped.
func TestSUP040StartedFailure(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	p := &supProbe{obs: &obs}
	p.onStart = func(*Context[any]) { panic("started boom") }
	parent, kids := supervised(t, sys, eng, actorOf[any](p))
	eng.RunUntilIdle()

	want := [][]string{{"actor=" + kids[0].String(), "inc=1", "source=started", "err=lockstep: panic: started boom"}}
	if got := recordsOf(eng.Records(), "lockstep.fail"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.fail records:\n got %q\nwant %q", got, want)
	}
	if n := failedNotices(parent); len(n) != 1 || n[0].source != supervise.InStarted || n[0].inc != 1 {
		t.Errorf("failed notices %+v, want one with source started and inc 1", n)
	}
	if st, _ := cellOf(t, sys, kids[0]).lifecycle(); st != supervise.Suspended {
		t.Errorf("state %v, want suspended", st)
	}
	if err := Stop(sys, kids[0].Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if want := []string{"stopped /user/s/c1 normal"}; !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
}

// signalFailer watches its target on "watch" and panics in Signal.
type signalFailer struct{ target Address }

func (a *signalFailer) Receive(ctx *Context[any], _ any) {
	if err := Watch(ctx, a.target); err != nil {
		panic(err)
	}
}

func (*signalFailer) Signal(*Context[any], Signal) { panic("signal boom") }

// SUP-150: a failure in Signal has source signal.
func TestSUP150SignalFailure(t *testing.T) {
	sys, eng := newManualSystem(t)
	target, err := Spawn(sys, "t", idle[any]())
	if err != nil {
		t.Fatal(err)
	}
	parent, kids := supervised(t, sys, eng, actorOf[any](&signalFailer{target: target.Address()}))
	send(t, sys, kids[0], any("watch"))
	eng.RunUntilIdle()
	if err := Stop(sys, target.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	n := failedNotices(parent)
	if len(n) != 1 || n[0].source != supervise.InSignal || n[0].err.Error() != "lockstep: panic: signal boom" {
		t.Errorf("failed notices %+v, want one with source signal", n)
	}
}

// SUP-010: after a handler calls Fail, the turn pops no further user message, though the budget
// allows it; the handler itself runs to completion (SUP-140).
func TestSUP010NoUserMessageAfterFail(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetBudget(32)
	var obs []string
	p := &supProbe{obs: &obs}
	p.receive = func(ctx *Context[any], msg any) {
		obs = append(obs, fmt.Sprint(msg))
		if msg == "fail" {
			ctx.Fail(errProbe)
			obs = append(obs, "after Fail")
		}
	}
	pid, err := Spawn(sys, "p", actorOf[any](p))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"a", "fail", "b", "c"} {
		send(t, sys, pid, any(m))
	}
	if !eng.Step() {
		t.Fatal("no turn ran")
	}
	if want := []string{"started /user/p 1", "a", "fail", "after Fail"}; !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
	c := cellOf(t, sys, pid)
	if st, _ := c.lifecycle(); st != supervise.Suspended || queued(c) != 2 {
		t.Errorf("state %v, queued %d; want suspended, 2", st, queued(c))
	}
	want := [][]string{{"actor=" + pid.String(), "inc=1", "source=receive", "err=" + errProbe.Error()}}
	if got := recordsOf(eng.Records(), "lockstep.fail"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.fail records:\n got %q\nwant %q", got, want)
	}
	// A Fail outside a turn is a misuse of the Context, not a failure.
	ctx := c.ctx
	if v := mustPanic(t, func() { ctx.Fail(errProbe) }); v != "lockstep: Context of "+pid.String()+" used outside its turn" {
		t.Errorf("Fail outside the turn panicked with %v", v)
	}
}

// ACT §7: every Context method, Watch and Unwatch panic on a nil *Context.
func TestNilContextPanics(t *testing.T) {
	var ctx *Context[int]
	addr := Address{node: "local", path: "/user/x", uid: 9}
	calls := []struct {
		name string
		f    func()
	}{
		{"Self", func() { ctx.Self() }},
		{"Parent", func() { ctx.Parent() }},
		{"Logf", func() { ctx.Logf("x") }},
		{"Logger", func() { ctx.Logger() }},
		{"Fail", func() { ctx.Fail(errProbe) }},
		{"Watch", func() { _ = Watch(ctx, addr) }},
		{"Unwatch", func() { Unwatch(ctx, addr) }},
	}
	for _, c := range calls {
		if v := mustPanic(t, c.f); v != "lockstep: nil Context" {
			t.Errorf("%s on a nil Context panicked with %v, want lockstep: nil Context", c.name, v)
		}
	}
}
