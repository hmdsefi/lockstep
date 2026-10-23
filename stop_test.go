// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/local"
	"github.com/hmdsefi/lockstep/internal/mailbox"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// hookActor is a test actor with a Receive and a Stopped hook; a nil function does nothing.
type hookActor[M any] struct {
	receive func(ctx *Context[M], msg M)
	stopped func(ctx *Context[M], reason StopReason)
}

func (a *hookActor[M]) Receive(ctx *Context[M], msg M) {
	if a.receive != nil {
		a.receive(ctx, msg)
	}
}

func (a *hookActor[M]) Stopped(ctx *Context[M], reason StopReason) {
	if a.stopped != nil {
		a.stopped(ctx, reason)
	}
}

// reportStopped returns a hookActor factory whose Stopped sends its reason to the returned channel
// (capacity 8).
func reportStopped[M any]() (func() Actor[M], <-chan StopReason) {
	reasons := make(chan StopReason, 8)
	return actorOf[M](&hookActor[M]{stopped: func(_ *Context[M], r StopReason) { reasons <- r }}), reasons
}

// watchFrom makes the probe w watch addr during one of its turns and fails the test on an error.
func watchFrom(t *testing.T, sys *System, w PID[any], addr Address) {
	t.Helper()
	if err, _ := inProbe(t, sys, w, func(ctx *Context[any]) any { return Watch(ctx, addr) }).(error); err != nil {
		t.Fatalf("Watch(%v): %v", addr, err)
	}
}

// awaitTerminated returns the next signal of the probe p, failing the test unless it is a
// Terminated for addr that arrives within 10 s.
func awaitTerminated(t *testing.T, p *probe[any], addr Address) Terminated {
	t.Helper()
	sig := recv(t, p.signals)
	term, ok := sig.(Terminated)
	if !ok || term.Addr != addr {
		t.Fatalf("signal %#v, want a Terminated for %v", sig, addr)
	}
	return term
}

// stopAndWait stops the actor at addr from sys and returns when it has completed its final stop,
// observed through a watch from the probe w (its probe value wp).
func stopAndWait(t *testing.T, sys *System, w PID[any], wp *probe[any], addr Address) Terminated {
	t.Helper()
	watchFrom(t, sys, w, addr)
	if err := Stop(sys, addr); err != nil {
		t.Fatalf("Stop(%v): %v", addr, err)
	}
	return awaitTerminated(t, wp, addr)
}

// noSignal fails the test if the probe p receives a signal within d.
func noSignal(t *testing.T, p *probe[any], d time.Duration) {
	t.Helper()
	select {
	case sig := <-p.signals:
		t.Errorf("unexpected signal %#v", sig)
	case <-time.After(d):
	}
}

// AT-ACT-32: ACT-700, ACT-710.
func TestATACT32Stop(t *testing.T) {
	sys := newLocalSystem(t)
	w, wp := spawnProbe[any](t, sys, "w", nil)

	t.Run("normal", func(t *testing.T) {
		f, reasons := reportStopped[int]()
		tp, err := Spawn(sys, "t", f)
		if err != nil {
			t.Fatal(err)
		}
		watchFrom(t, sys, w, tp.Address())
		if err := Stop(sys, tp.Address()); err != nil {
			t.Fatalf("Stop = %v, want nil", err)
		}
		if r := recv(t, reasons); r != Normal {
			t.Errorf("Stopped got %v, want Normal", r)
		}
		awaitTerminated(t, wp, tp.Address())
		// A stopped actor's address is not alive.
		want := "lockstep: not found: " + tp.Address().String()
		if err := Stop(sys, tp.Address()); !errors.Is(err, ErrNotFound) || err.Error() != want {
			t.Errorf("Stop of a stopped actor = %v, want ErrNotFound with text %q", err, want)
		}
	})

	t.Run("zero and remote", func(t *testing.T) {
		if err := Stop(sys, Address{}); !errors.Is(err, ErrNotFound) || err.Error() != "lockstep: not found: <zero>" {
			t.Errorf("Stop(zero) = %v, want ErrNotFound with text %q", err, "lockstep: not found: <zero>")
		}
		remote := Address{node: "other", path: "/user/x", uid: 7}
		want := "lockstep: address not local: " + remote.String()
		if err := Stop(sys, remote); !errors.Is(err, ErrNotLocal) || err.Error() != want {
			t.Errorf("Stop(remote) = %v, want ErrNotLocal with text %q", err, want)
		}
	})

	t.Run("second stop while stopping", func(t *testing.T) {
		entered, release := make(chan StopReason, 8), make(chan struct{})
		tp, err := Spawn(sys, "slow", actorOf[int](&hookActor[int]{stopped: func(_ *Context[int], r StopReason) {
			entered <- r
			<-release
		}}))
		if err != nil {
			t.Fatal(err)
		}
		watchFrom(t, sys, w, tp.Address())
		if err := Stop(sys, tp.Address()); err != nil {
			t.Fatalf("first Stop = %v", err)
		}
		if r := recv(t, entered); r != Normal {
			t.Errorf("Stopped got %v, want Normal", r)
		}
		if err := Stop(sys, tp.Address()); err != nil {
			t.Errorf("second Stop while Stopped blocks = %v, want nil (ACT-710)", err)
		}
		close(release)
		awaitTerminated(t, wp, tp.Address())
		if n := len(entered); n != 0 {
			t.Errorf("Stopped ran %d more times, want once", n)
		}
	})

	t.Run("self", func(t *testing.T) {
		stopErr := make(chan error, 1)
		reasons := make(chan StopReason, 1)
		var after []string
		self, err := Spawn(sys, "self", actorOf[string](&hookActor[string]{
			receive: func(ctx *Context[string], m string) {
				if m == "stop" {
					stopErr <- Stop(ctx, ctx.Self().Address())
					return
				}
				after = append(after, m)
			},
			stopped: func(_ *Context[string], r StopReason) { reasons <- r },
		}))
		if err != nil {
			t.Fatal(err)
		}
		watchFrom(t, sys, w, self.Address())
		send(t, sys, self, "stop")
		send(t, sys, self, "after")
		if err := recv(t, stopErr); err != nil {
			t.Errorf("Stop(ctx, self) = %v, want nil", err)
		}
		if r := recv(t, reasons); r != Normal {
			t.Errorf("Stopped got %v, want Normal", r)
		}
		awaitTerminated(t, wp, self.Address())
		if len(after) != 0 {
			t.Errorf("Receive got %q after Stop(ctx, self), want nothing", after)
		}
	})
}

// AT-ACT-03 (Stop clause): ACT-030, ACT-700. Stop of a guardian, by uid or as a path address, is
// ErrNotFound.
func TestATACT03StopGuardian(t *testing.T) {
	sys := newLocalSystem(t)
	w, _ := spawnProbe[any](t, sys, "w", nil)
	parent, _ := inProbe(t, sys, w, func(ctx *Context[any]) any { return ctx.Parent() }).(Address)
	if parent != sys.user {
		t.Fatalf("ctx.Parent() = %v, want the user guardian %v", parent, sys.user)
	}
	for _, g := range []Address{
		parent,
		sys.system,
		PIDAt[any]("local", "/user").Address(),
		PIDAt[any]("local", "/system").Address(),
	} {
		want := "lockstep: not found: " + g.String()
		if err := Stop(sys, g); !errors.Is(err, ErrNotFound) || err.Error() != want {
			t.Errorf("Stop(%v) = %v, want ErrNotFound with text %q", g, err, want)
		}
	}
}

// AT-ACT-13 (Stop clause): ACT-200, ACT-700. Stop panics for a nil Sender before any other check,
// and for a Context outside its turn before the address checks.
func TestATACT13StopNilSender(t *testing.T) {
	sys, eng := newManualSystem(t)
	if v := mustPanic(t, func() { _ = Stop(nil, Address{}) }); v != "lockstep: nil Sender" {
		t.Errorf("Stop(nil, addr) panicked with %v, want %q", v, "lockstep: nil Sender")
	}
	var c *Context[int]
	if v := mustPanic(t, func() { _ = Stop(c, Address{}) }); v != "lockstep: nil Sender" {
		t.Errorf("Stop(nil *Context, addr) panicked with %v, want %q", v, "lockstep: nil Sender")
	}
	pid, _ := spawnRunner(t, sys, eng, "a")
	var saved *Context[any]
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { saved = ctx })
	want := "lockstep: Context of " + pid.Address().String() + " used outside its turn"
	if v := mustPanic(t, func() { _ = Stop(saved, Address{}) }); v != want {
		t.Errorf("Stop with a Context outside its turn panicked with %v, want %q", v, want)
	}
}

// AT-ACT-17: ACT-330, ACT-420, ACT-540, ACT-550. A name is freed when the stop completes, not when
// it starts.
func TestATACT17NameFreedAtStopCompletion(t *testing.T) {
	sys := newLocalSystem(t)
	w, wp := spawnProbe[any](t, sys, "w", nil)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	old, err := Spawn(sys, "a", actorOf[int](&hookActor[int]{stopped: func(*Context[int], StopReason) {
		entered <- struct{}{}
		<-release
	}}))
	if err != nil {
		t.Fatal(err)
	}
	watchFrom(t, sys, w, old.Address())
	if err := Stop(sys, old.Address()); err != nil {
		t.Fatal(err)
	}
	recv(t, entered)

	want := `lockstep: name taken: "a" under ` + sys.user.String()
	if _, err := Spawn(sys, "a", idle[int]()); !errors.Is(err, ErrNameTaken) || err.Error() != want {
		t.Errorf("Spawn while the stop runs = %v, want ErrNameTaken with text %q", err, want)
	}
	if p, err := Lookup[int](sys, "/user/a"); err != nil || p != old {
		t.Errorf("Lookup while the stop runs = %v, %v, want the old PID %v (ACT-540)", p, err, old)
	}

	close(release)
	awaitTerminated(t, wp, old.Address())
	if _, err := Lookup[int](sys, "/user/a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Lookup after the stop = %v, want ErrNotFound", err)
	}
	p, err := Spawn(sys, "a", idle[int]())
	if err != nil {
		t.Fatalf("Spawn after the stop completed: %v", err)
	}
	if p == old || p.Address().UID() <= old.Address().UID() {
		t.Errorf("new PID %v, old %v: want a different PID with a greater uid", p, old)
	}
}

// AT-ACT-24: ACT-550, SUP-070, SUP-080. Stopping a tree removes every entry, children first, in
// reverse spawn order, with the parent's reason passed down as ParentStopping.
func TestATACT24NoOrphans(t *testing.T) {
	sys := newLocalSystem(t)
	w, wp := spawnProbe[any](t, sys, "w", nil)
	before := sys.reg.Len()

	var mu sync.Mutex
	var order []string
	reasons := map[string]StopReason{}
	ready := make(chan struct{}, 1)
	var node func(name, child string) func() Actor[any]
	node = func(name, child string) func() Actor[any] {
		return actorOf[any](&fnActorStopper{
			started: func(ctx *Context[any]) {
				if child == "" {
					ready <- struct{}{}
					return
				}
				if _, err := Spawn(ctx, child, node(child, childOf(child))); err != nil {
					t.Errorf("Spawn %s: %v", child, err)
				}
			},
			stopped: func(ctx *Context[any], r StopReason) {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, ctx.Self().Address().Path())
				reasons[name] = r
			},
		})
	}
	a, err := Spawn(sys, "a", node("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	recv(t, ready)
	if n := sys.reg.Len(); n != before+3 {
		t.Fatalf("registry holds %d entries with the tree, want %d", n, before+3)
	}

	stopAndWait(t, sys, w, wp, a.Address())
	for _, path := range []string{"/user/a", "/user/a/b", "/user/a/b/c"} {
		if _, err := Lookup[any](sys, path); !errors.Is(err, ErrNotFound) {
			t.Errorf("Lookup(%s) after the stop = %v, want ErrNotFound", path, err)
		}
	}
	if n := sys.reg.Len(); n != before {
		t.Errorf("registry holds %d entries after the stop, want %d", n, before)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"/user/a/b/c", "/user/a/b", "/user/a"}; !slices.Equal(order, want) {
		t.Errorf("Stopped order %q, want %q", order, want)
	}
	if reasons["a"] != Normal || reasons["b"] != ParentStopping || reasons["c"] != ParentStopping {
		t.Errorf("reasons %v, want a Normal, b and c ParentStopping", reasons)
	}
}

// childOf names the child of b in TestATACT24NoOrphans's tree a/b/c.
func childOf(name string) string {
	if name == "b" {
		return "c"
	}
	return ""
}

// fnActorStopper is an any actor with Started and Stopped hooks.
type fnActorStopper struct {
	started func(ctx *Context[any])
	stopped func(ctx *Context[any], r StopReason)
}

func (a *fnActorStopper) Started(ctx *Context[any])               { a.started(ctx) }
func (*fnActorStopper) Receive(*Context[any], any)                {}
func (a *fnActorStopper) Stopped(ctx *Context[any], r StopReason) { a.stopped(ctx, r) }

// AT-ACT-34 (stop, respawn, Watch and Stop clauses): ACT-440, ACT-600, ACT-700. A path address
// reaches whichever actor is registered at its path, across a stop and a respawn.
func TestATACT34PIDAtLifecycle(t *testing.T) {
	sys := newLocalSystem(t)
	w, wp := spawnProbe[any](t, sys, "w", nil)
	p := PIDAt[int]("local", "/user/a")
	got := make(chan int, 8)
	spawnA := func() PID[int] {
		t.Helper()
		pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{receive: func(_ *Context[int], m int) { got <- m }}))
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}

	first := spawnA()
	send(t, sys, p, 2)
	if m := recv(t, got); m != 2 {
		t.Errorf("a received %d, want 2", m)
	}
	// Watch(ctx, p) and Stop(sys, p.Address()): the Terminated carries the path address.
	if term := stopAndWait(t, sys, w, wp, p.Address()); term.Reason != Normal {
		t.Errorf("Terminated = %v, want reason Normal", term)
	}
	if _, err := Lookup[int](sys, "/user/a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Lookup after Stop(path address) = %v, want ErrNotFound", err)
	}

	second := spawnA()
	if second == first {
		t.Error("the respawned actor has the old PID")
	}
	send(t, sys, p, 3)
	if m := recv(t, got); m != 3 {
		t.Errorf("the respawned a received %d, want 3", m)
	}
	stopAndWait(t, sys, w, wp, p.Address())

	want := "lockstep: not found: " + p.Address().String()
	if err := Stop(sys, p.Address()); !errors.Is(err, ErrNotFound) || err.Error() != want {
		t.Errorf("Stop(path address) with none registered = %v, want ErrNotFound with text %q", err, want)
	}
	// Nothing is delivered after the stop: the path address resolves to no actor (MBX-150;
	// the lockstep.deadletter record is plan 1.3's).
	if o := sys.route(routeRequest{from: sys.user, to: p.Address(), typ: typeOf[int](), msg: 4}); o.kind != routeDead || o.reason != reasonNotFound {
		t.Errorf("route to the path address after the stop = %+v, want a not-found dead letter", o)
	}
	select {
	case m := <-got:
		t.Errorf("a stopped actor received %d", m)
	case <-time.After(50 * time.Millisecond):
	}
}

// SUP §8: lockstep.stop records the actor, its last incarnation (0 without one), the reason's
// kind and an empty err for a reason without an error.
func TestStopRecord(t *testing.T) {
	sys, eng := newManualSystem(t)
	a, err := Spawn(sys, "a", idle[int]())
	if err != nil {
		t.Fatal(err)
	}
	b, err := Spawn(sys, "b", func() Actor[int] { panic("factory boom") }) // no incarnation begins
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	for _, pid := range []PID[int]{a, b} {
		if err := Stop(sys, pid.Address()); err != nil {
			t.Fatalf("Stop(%v): %v", pid, err)
		}
	}
	eng.RunUntilIdle()
	want := [][]string{
		{"actor=" + a.String(), "inc=1", "reason=normal", "err="},
		{"actor=" + b.String(), "inc=0", "reason=normal", "err="},
	}
	if got := recordsOf(eng.Records(), "lockstep.stop"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.stop records = %q, want %q", got, want)
	}
}

// SUP-050, SUP-080: a stop request with reason Shutdown passes Shutdown to the children; a request
// that arrives while one is in progress is discarded, so the first reason wins.
func TestStopShutdownReasonAndOneRequest(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got []string
	stopper := func(name string) *hookActor[any] {
		return &hookActor[any]{stopped: func(_ *Context[any], r StopReason) { got = append(got, name+" "+r.String()) }}
	}
	pa := stopper("p")
	pa.receive = func(ctx *Context[any], _ any) {
		if _, err := Spawn(ctx, "c", actorOf[any](stopper("c"))); err != nil {
			t.Errorf("Spawn c: %v", err)
		}
	}
	p, err := Spawn(sys, "p", actorOf[any](pa))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, p, any("spawn"))
	eng.RunUntilIdle()
	c := cellOf(t, sys, p)
	c.requestStop(Shutdown, nil)
	c.requestStop(Normal, nil) // discarded (SUP-050)
	eng.RunUntilIdle()
	if want := []string{"c shutdown", "p shutdown"}; !slices.Equal(got, want) {
		t.Errorf("Stopped calls %q, want %q", got, want)
	}
}

// SUP-070: a child that stops on its own while the parent waits for another child is removed and
// skipped; the parent completes after both.
func TestStopChildStoppingOnItsOwn(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got []string
	stopper := func(name string) *hookActor[any] {
		return &hookActor[any]{stopped: func(*Context[any], StopReason) { got = append(got, name) }}
	}
	var kids []PID[any]
	pa := stopper("p")
	pa.receive = func(ctx *Context[any], _ any) {
		for _, n := range []string{"c1", "c2"} {
			k, err := Spawn(ctx, n, actorOf[any](stopper(n)))
			if err != nil {
				t.Errorf("Spawn %s: %v", n, err)
			}
			kids = append(kids, k)
		}
	}
	p, err := Spawn(sys, "p", actorOf[any](pa))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, p, any("spawn"))
	eng.RunUntilIdle()
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	if err := Stop(sys, kids[0].Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if len(got) != 3 || got[2] != "p" || !slices.Contains(got, "c1") || !slices.Contains(got, "c2") {
		t.Errorf("Stopped calls %q, want c1 and c2 once each, then p", got)
	}
	if n := sys.reg.Len(); n != 0 {
		t.Errorf("registry holds %d entries, want 0", n)
	}
}

// SUP-090, SUP-100: inside Stopped, Spawn with the stopping actor as parent returns ErrStopped and
// registers nothing; Send still works.
func TestSpawnFromStopped(t *testing.T) {
	sys, eng := newManualSystem(t)
	var spawnErr, sendErr error
	var self Address
	a, err := Spawn(sys, "a", actorOf[any](&hookActor[any]{stopped: func(ctx *Context[any], _ StopReason) {
		self = ctx.Self().Address()
		_, spawnErr = Spawn(ctx, "c", idle[any]())
		sendErr = Send(ctx, ctx.Self(), any("x"))
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, a.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	want := "lockstep: stopped: " + self.String()
	if !errors.Is(spawnErr, ErrStopped) || spawnErr.Error() != want {
		t.Errorf("Spawn from Stopped = %v, want ErrStopped with text %q", spawnErr, want)
	}
	if sendErr != nil {
		t.Errorf("Send from Stopped = %v, want nil", sendErr)
	}
	if n := sys.reg.Len(); n != 0 {
		t.Errorf("registry holds %d entries, want 0", n)
	}
}

// SUP-060 step 5, MBX-160: the final stop closes the queues and drains them, returning the Sent
// and the Self entries each in queue order and the signals of the system queue, without its other
// messages; a closed mailbox refuses every later entry and a closed system queue drops every later
// message.
func TestStopDrain(t *testing.T) {
	sys, eng := newManualSystem(t)
	var received []any
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{receive: func(_ *Context[any], m any) { received = append(received, m) }}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, a)
	c.offerUser(entry{msg: "m1", kind: entrySent}, nil, false)
	c.offerUser(entry{msg: "s1", kind: entrySelf, inc: c.inc}, nil, false)
	c.offerUser(entry{msg: "m2", kind: entrySent}, nil, false)
	sig := Terminated{Addr: gone(), Reason: NoActor}
	c.pushSystem(stopped{child: gone()}, nil)
	c.pushSystem(signalMsg{sig: sig}, nil)
	sent, self, signals := c.closeQueues()
	if len(sent) != 2 || sent[0].msg != "m1" || sent[1].msg != "m2" {
		t.Errorf("drained Sent entries %+v, want m1, m2", sent)
	}
	if len(self) != 1 || self[0].msg != "s1" || self[0].inc != c.inc {
		t.Errorf("drained Self entries %+v, want s1 of inc %d", self, c.inc)
	}
	if !slices.Equal(signals, []Signal{sig}) {
		t.Errorf("drained signals %v, want %v", signals, sig)
	}
	if out, _ := c.offerUser(entry{msg: "late", kind: entrySent}, nil, false); out != mailbox.Closed {
		t.Errorf("a closed mailbox answered %v to an entry, want closed", out)
	}
	c.pushSystem(begin{}, nil)
	n, m := c.user.Len(), c.sysq.Len()
	if n != 0 || m != 0 {
		t.Errorf("closed queues hold %d user and %d system messages, want none", n, m)
	}
	eng.RunUntilIdle()
	if len(received) != 0 {
		t.Errorf("Receive got %v", received)
	}
}

// probeTree returns a factory of supProbes observing into obs whose Started spawns one child per
// name, in order, each from the factory at the same index.
func probeTree(obs *[]string, names []string, factories []func() Actor[any]) func() Actor[any] {
	return actorOf[any](&supProbe{obs: obs, onStart: func(ctx *Context[any]) {
		for i, n := range names {
			if _, err := Spawn(ctx, n, factories[i]); err != nil {
				panic(err)
			}
		}
	}})
}

// leafProbe returns a factory of supProbes observing into obs.
func leafProbe(obs *[]string) func() Actor[any] { return actorOf[any](&supProbe{obs: obs}) }

// stopRecordsOf returns the "actor reason" pairs of recs' lockstep.stop records, in order.
func stopRecordsOf(recs []engine.Record) []string {
	var out []string
	for _, r := range recordsOf(recs, "lockstep.stop") {
		out = append(out, strings.TrimPrefix(r[0], "actor=")+" "+strings.TrimPrefix(r[2], "reason="))
	}
	return out
}

// indexOf returns the index in recs of the first record of kind whose attributes include every
// key=value of attrs, or -1.
func indexOf(recs []engine.Record, kind string, attrs ...string) int {
	for i, r := range recs {
		if r.Kind != kind {
			continue
		}
		got := recordsOf([]engine.Record{r}, kind)[0]
		ok := true
		for _, a := range attrs {
			if !slices.Contains(got, a) {
				ok = false
				break
			}
		}
		if ok {
			return i
		}
	}
	return -1
}

// AT-SUP-02: SUP-010, SUP-050, MBX-160. On the local engine with its default turn budget, a held
// Probe with "stop", "a" to "d" queued handles only "stop": ctx.Stop takes effect when Receive
// returns, and the four other messages become dead letters with reason stopped.
func TestATSUP02TurnStopsAfterSelfStop(t *testing.T) {
	if local.DefaultThroughput != 32 {
		t.Fatalf("local.DefaultThroughput = %d, want 32", local.DefaultThroughput)
	}
	sys, rec := newRecordingSystem(t)
	var obs []string
	p, err := Spawn(sys, "p", leafProbe(&obs))
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "p starts", func() bool { return indexOf(rec.Records(), "lockstep.start", "actor="+p.String()) >= 0 })
	release := Hold(sys, p.Address())
	for _, m := range []string{"stop", "a", "b", "c", "d"} {
		send(t, sys, p, any(m))
	}
	release()
	waitUntil(t, "p stops", func() bool { return indexOf(rec.Records(), "lockstep.stop", "actor="+p.String()) >= 0 })

	want := []string{"started /user/p 1", "stop", "stopped /user/p normal"}
	if !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
	var wantDead [][]string
	for _, m := range []string{"a", "b", "c", "d"} {
		wantDead = append(wantDead, []string{"from=" + sys.user.String(), "to=" + p.String(), "reason=stopped", "msg=" + rec.Describe(m)})
	}
	if got := recordsOf(rec.Records(), "lockstep.deadletter"); !slices.EqualFunc(got, wantDead, slices.Equal) {
		t.Errorf("dead letters %q, want %q", got, wantDead)
	}
}

// AT-SUP-06: SUP-050. Three stop requests to a held actor leave one in its system queue; it stops
// once, with reason normal.
func TestATSUP06StopRequestsCoalesce(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	p, err := Spawn(sys, "p", leafProbe(&obs))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, p)
	release := c.holdTurns()
	for range 3 {
		if err := Stop(sys, p.Address()); err != nil {
			t.Fatal(err)
		}
	}
	c.mu.Lock()
	n := c.sysq.Len()
	c.mu.Unlock()
	if n != 1 {
		t.Errorf("the system queue holds %d messages, want 1 stop request", n)
	}
	release()
	eng.RunUntilIdle()
	if got, want := stopRecordsOf(eng.Records()), []string{p.String() + " normal"}; !slices.Equal(got, want) {
		t.Errorf("lockstep.stop records %q, want %q", got, want)
	}
	if want := []string{"started /user/p 1", "stopped /user/p normal"}; !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
}

// AT-SUP-07: SUP-060, SUP-070, SUP-080. P's stop stops its children one at a time, most recent
// first, each subtree depth first, with reason parent-stopping; P's own lockstep.stop follows the
// dead letter of the message drained from its mailbox.
func TestATSUP07StopOrder(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	c2 := probeTree(&obs, []string{"g1", "g2"}, []func() Actor[any]{leafProbe(&obs), leafProbe(&obs)})
	p, err := Spawn(sys, "p", probeTree(&obs, []string{"c1", "c2", "c3"},
		[]func() Actor[any]{leafProbe(&obs), c2, leafProbe(&obs)}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	obs = obs[:0]
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	send(t, sys, p, any("late")) // queued behind the stop request: never received
	eng.RunUntilIdle()

	want := []string{
		"stopped /user/p/c3 parent-stopping",
		"stopped /user/p/c2/g2 parent-stopping",
		"stopped /user/p/c2/g1 parent-stopping",
		"stopped /user/p/c2 parent-stopping",
		"stopped /user/p/c1 parent-stopping",
		"stopped /user/p normal",
	}
	if !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
	recs := eng.Records()
	var wantRecs []string
	for _, o := range want {
		f := strings.Fields(o)
		wantRecs = append(wantRecs, "lockstep://local"+f[1]+"#")
	}
	got := stopRecordsOf(recs)
	if len(got) != len(want) {
		t.Fatalf("lockstep.stop records %q, want %d", got, len(want))
	}
	for i, g := range got {
		reason := strings.Fields(want[i])[2]
		if !strings.HasPrefix(g, wantRecs[i]) || !strings.HasSuffix(g, " "+reason) {
			t.Errorf("lockstep.stop record %d = %q, want %s... %s", i, g, wantRecs[i], reason)
		}
	}
	dead := indexOf(recs, "lockstep.deadletter", "to="+p.String(), "msg="+eng.Engine().Tracer.Describe("late"))
	stop := indexOf(recs, "lockstep.stop", "actor="+p.String())
	if dead < 0 || stop < dead {
		t.Errorf("P's dead letter at %d and lockstep.stop at %d, want the dead letter first", dead, stop)
	}
}

// AT-SUP-08: SUP-070. A child that another child's Stopped stops while the parent waits keeps the
// first reason, stops once, and the parent stops last.
func TestATSUP08ChildStopsWhileParentWaits(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	var c1 PID[any]
	p, err := Spawn(sys, "p", actorOf[any](&supProbe{obs: &obs, onStart: func(ctx *Context[any]) {
		var err error
		if c1, err = Spawn(ctx, "c1", leafProbe(&obs)); err != nil {
			panic(err)
		}
		c2 := &supProbe{obs: &obs, onStop: func(ctx *Context[any], _ StopReason) {
			if err := Stop(ctx, c1.Address()); err != nil {
				panic(err)
			}
		}}
		if _, err := Spawn(ctx, "c2", actorOf[any](c2)); err != nil {
			panic(err)
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	obs = obs[:0]
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	want := []string{"stopped /user/p/c2 parent-stopping", "stopped /user/p/c1 normal", "stopped /user/p normal"}
	if !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
	n := 0
	for _, r := range recordsOf(eng.Records(), "lockstep.stop") {
		if r[0] == "actor="+c1.String() {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d lockstep.stop records for c1, want 1", n)
	}
}

// AT-SUP-09: SUP-080, SUP-350. A child of an actor stopped with Stop gets parent-stopping; in a
// system that shuts down, a top-level actor and its child both get shutdown.
func TestATSUP09ReasonPropagation(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	p, err := Spawn(sys, "p", probeTree(&obs, []string{"c"}, []func() Actor[any]{leafProbe(&obs)}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	qSys, qEng := newManualSystem(t)
	var qObs []string
	if _, err := Spawn(qSys, "q", probeTree(&qObs, []string{"d"}, []func() Actor[any]{leafProbe(&qObs)})); err != nil {
		t.Fatal(err)
	}
	qEng.RunUntilIdle()
	if err := qSys.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	qEng.RunUntilIdle()

	if !slices.Contains(obs, "stopped /user/p/c parent-stopping") || !slices.Contains(obs, "stopped /user/p normal") {
		t.Errorf("observations %q, want c parent-stopping and p normal", obs)
	}
	if !slices.Contains(qObs, "stopped /user/q/d shutdown") || !slices.Contains(qObs, "stopped /user/q shutdown") {
		t.Errorf("observations %q, want d and q shutdown", qObs)
	}
}

// AT-SUP-10: SUP-090. Spawn with the context of a stopping actor, inside Stopped, and Spawn with
// the System after Shutdown has returned both return ErrStopped.
func TestATSUP10SpawnRefusedWhileStopping(t *testing.T) {
	sys := newLocalSystem(t)
	errs := make(chan error, 1)
	p, err := Spawn(sys, "p", actorOf[any](&lifeActor[any]{stopped: func(ctx *Context[any], _ StopReason) {
		_, err := Spawn(ctx, "x", idle[any]())
		errs <- err
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errs:
		if !errors.Is(err, ErrStopped) {
			t.Errorf("Spawn inside Stopped = %v, want ErrStopped", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("p's Stopped did not run within 10 s")
	}
	shutdownWithin(t, sys)
	if _, err := Spawn(sys, "x", idle[any]()); !errors.Is(err, ErrStopped) {
		t.Errorf("Spawn after Shutdown = %v, want ErrStopped", err)
	}
}

// SUP-090, ACT-600: while a cell ends its incarnation for a restart it is Restarting: Spawn with
// it as parent and Watch return ErrStopped. Its Stopped hook, the only code that runs then,
// observes them; a watch it registered would outlive the incarnation (SUP-340).
func TestSUP090SpawnUnderRestartingParent(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	x, err := Spawn(sys, "x", idle[any]())
	if err != nil {
		t.Fatal(err)
	}
	var spawnErr, watchErr error
	var state supervise.State
	p, err := Spawn(sys, "p", actorOf[any](&rProbe{obs: new([]string), onStop: func(ctx *Context[any], r StopReason) {
		if r.Kind != StopRestarting {
			return
		}
		state = ctx.cell.state
		_, spawnErr = Spawn(ctx, "x", idle[any]())
		watchErr = Watch(ctx, x.Address())
	}}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, p, any("panic"))
	eng.RunUntilIdle()
	if state != supervise.Restarting || !errors.Is(spawnErr, ErrStopped) || !errors.Is(watchErr, ErrStopped) {
		t.Errorf("in Stopped of a restart: state %v, Spawn = %v, Watch = %v; want restarting, ErrStopped twice",
			state, spawnErr, watchErr)
	}
	if got := sys.reg.Watchers(x.Address().Path()); len(got) != 0 {
		t.Errorf("watch entries of X %+v, want none", got)
	}
	if _, inc := cellOf(t, sys, p).lifecycle(); inc != 2 {
		t.Errorf("inc = %d after the restart, want 2", inc)
	}
}

// AT-SUP-31: SUP-310. After P's lockstep.stop, none of its 150 descendants is registered and the
// registry is back to its size before P was spawned.
func TestATSUP31NoOrphans(t *testing.T) {
	sys, eng := newManualSystem(t)
	before, liveBefore := sys.reg.Len(), sys.liveActors()
	var paths []string
	p, err := Spawn(sys, "p", actorOf[any](&fnActor[any]{started: func(ctx *Context[any]) {
		for i := range 50 {
			name := "c" + strconv.Itoa(i)
			_, err := Spawn(ctx, name, actorOf[any](&fnActor[any]{started: func(ctx *Context[any]) {
				for _, g := range []string{"g1", "g2"} {
					if _, err := Spawn(ctx, g, idle[any]()); err != nil {
						panic(err)
					}
				}
			}}))
			if err != nil {
				panic(err)
			}
			paths = append(paths, "/user/p/"+name, "/user/p/"+name+"/g1", "/user/p/"+name+"/g2")
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if n := sys.reg.Len(); n != before+151 {
		t.Fatalf("registry holds %d entries after the spawns, want %d", n, before+151)
	}
	if n := sys.liveActors(); n != liveBefore+151 {
		t.Fatalf("live count %d after the spawns, want %d", n, liveBefore+151)
	}
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if indexOf(eng.Records(), "lockstep.stop", "actor="+p.String()) < 0 {
		t.Fatal("no lockstep.stop for p")
	}
	for _, path := range paths {
		if _, err := Lookup[any](sys, path); !errors.Is(err, ErrNotFound) {
			t.Errorf("Lookup(%s) = %v, want ErrNotFound", path, err)
		}
	}
	if n := sys.reg.Len(); n != before {
		t.Errorf("registry holds %d entries, want %d", n, before)
	}
	if n := sys.liveActors(); n != liveBefore {
		t.Errorf("live count %d, want %d, its value before p was spawned", n, liveBefore)
	}
}

// AT-SUP-38: SUP-335. Watch of an actor that has stopped delivers Terminated with reason no-actor
// after Started returns, and no stop of any actor has reason no-actor; Watch then Unwatch in one
// turn delivers nothing.
func TestATSUP38NoActor(t *testing.T) {
	for _, unwatch := range []bool{false, true} {
		t.Run("unwatch="+strconv.FormatBool(unwatch), func(t *testing.T) {
			sys, eng := newManualSystem(t)
			x, err := Spawn(sys, "x", idle[any]())
			if err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle()
			if err := Stop(sys, x.Address()); err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle()
			var obs []string
			if _, err := Spawn(sys, "w", actorOf[any](&supProbe{obs: &obs, onStart: func(ctx *Context[any]) {
				if err := Watch(ctx, x.Address()); err != nil {
					panic(err)
				}
				if unwatch {
					Unwatch(ctx, x.Address())
				}
			}})); err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle()
			want := []string{"started /user/w 1", "terminated /user/x no-actor"}
			if unwatch {
				want = want[:1]
			}
			if !slices.Equal(obs, want) {
				t.Errorf("observations %q, want %q", obs, want)
			}
			for _, r := range recordsOf(eng.Records(), "lockstep.stop") {
				if slices.Contains(r, "reason=no-actor") {
					t.Errorf("lockstep.stop %q has reason no-actor", r)
				}
			}
		})
	}
}

// AT-MBX-21: MBX-160, SUP-060 step 5. After Stopped, the drain dead-letters the Sent entries in
// queue order, with the sender as from and the stopped actor as to, then records every Self entry,
// a piped reply included, as lockstep.drop incarnation-ended, and every signal left in the system
// queue as lockstep.drop not-running; lockstep.stop follows.
func TestATMBX21DrainAtStop(t *testing.T) {
	sys, eng := newManualSystem(t)
	tr := eng.Engine().Tracer
	stoppedAt := -1
	var obs []string
	a, err := Spawn(sys, "a", actorOf[any](&supProbe{obs: &obs, onStop: func(*Context[any], StopReason) {
		stoppedAt = len(eng.Records())
	}}))
	if err != nil {
		t.Fatal(err)
	}
	s, _ := spawnRunner(t, sys, eng, "s")
	c := cellOf(t, sys, a)
	release := c.holdTurns()

	send(t, sys, a, any("m1"))
	c.offerSelf(entry{msg: "t1", kind: entrySelf, inc: c.inc, timer: 1}) // a timer message of the live incarnation
	piped := pipedReply[any]{ask: gone(), call: func() any { return "r" }}
	c.offerSelf(entry{msg: piped, kind: entrySelf, inc: c.inc})
	inTurn(t, sys, eng, s, func(ctx *Context[any]) {
		if err := Send(ctx, a, any("m2")); err != nil {
			t.Errorf("Send m2: %v", err)
		}
	})
	if err := Stop(sys, a.Address()); err != nil {
		t.Fatal(err)
	}
	sig := Terminated{Addr: gone(), Reason: NoActor}
	c.pushSystem(signalMsg{sig: sig}, nil) // behind the stop request: never delivered
	release()
	eng.RunUntilIdle()

	if stoppedAt < 0 {
		t.Fatal("Stopped did not run")
	}
	var got [][]string
	recs := eng.Records()
	for _, r := range recs[stoppedAt:] {
		if r.Kind == "lockstep.deadletter" || r.Kind == "lockstep.drop" || r.Kind == "lockstep.stop" {
			got = append(got, append([]string{r.Kind}, recordsOf([]engine.Record{r}, r.Kind)[0]...))
		}
	}
	to := "to=" + a.String()
	want := [][]string{
		{"lockstep.deadletter", "from=" + sys.user.String(), to, "reason=stopped", "msg=" + tr.Describe("m1")},
		{"lockstep.deadletter", "from=" + s.String(), to, "reason=stopped", "msg=" + tr.Describe("m2")},
		{"lockstep.drop", to, "reason=incarnation-ended", "inc=1", "msg=" + tr.Describe("t1")},
		{"lockstep.drop", to, "reason=incarnation-ended", "inc=1", "msg=" + tr.Describe(piped)},
		{"lockstep.drop", to, "reason=not-running", "msg=" + tr.Describe(sig)},
		{"lockstep.stop", "actor=" + a.String(), "inc=1", "reason=normal", "err="},
	}
	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("records after Stopped:\n got %q\nwant %q", got, want)
	}
	for _, r := range recs[:stoppedAt] {
		if (r.Kind == "lockstep.deadletter" || r.Kind == "lockstep.drop") && slices.Contains(recordsOf([]engine.Record{r}, r.Kind)[0], to) {
			t.Errorf("record %s %v before Stopped", r.Kind, r.Attrs)
		}
	}
	if n := len(recordsOf(recs, "lockstep.pipe.drop")); n != 0 {
		t.Errorf("%d lockstep.pipe.drop records, want 0: the drain records the piped reply as lockstep.drop", n)
	}
}

// ACT-630: a Terminated{P, no-actor} queued by a Watch of P that Unwatch cancelled, in the same
// turn as a respawn of P and a new Watch of P, is skipped; the new watch still delivers one
// Terminated when P stops.
func TestACT630StaleTerminatedAfterRewatch(t *testing.T) {
	sys, eng := newManualSystem(t)
	w, r := spawnRunner(t, sys, eng, "w")
	first, err := Spawn(sys, "p", idle[any]())
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, first.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	path := PIDAt[any]("local", "/user/p").Address()
	var p PID[any]
	inTurn(t, sys, eng, w, func(ctx *Context[any]) {
		if err := Watch(ctx, path); err != nil { // P is dead: Terminated{P, no-actor} is queued
			t.Errorf("first Watch: %v", err)
		}
		Unwatch(ctx, path)
		var err error
		if p, err = Spawn(sys, "p", idle[any]()); err != nil {
			t.Errorf("respawn: %v", err)
		}
		if err := Watch(ctx, path); err != nil {
			t.Errorf("second Watch: %v", err)
		}
	})
	if len(r.sigs) != 0 {
		t.Errorf("signals after the rewatch %v, want none", r.sigs)
	}
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if want := []Signal{Terminated{Addr: path, Reason: Normal}}; !slices.Equal(r.sigs, want) {
		t.Errorf("signals %v, want %v", r.sigs, want)
	}
}

// SUP-350: Shutdown stops the user guardian's children, most recent first and each subtree depth
// first, then the system guardian's the same way, all with reason shutdown.
func TestSUP350ShutdownOrder(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	if _, err := Spawn(sys, "u1", probeTree(&obs, []string{"k"}, []func() Actor[any]{leafProbe(&obs)})); err != nil {
		t.Fatal(err)
	}
	if _, err := Spawn(sys, "u2", leafProbe(&obs)); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"s1", "s2"} {
		if _, err := spawnSystem(sys, n, leafProbe(&obs)); err != nil {
			t.Fatal(err)
		}
	}
	eng.RunUntilIdle()
	obs = obs[:0]
	if err := sys.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	want := []string{
		"stopped /user/u2 shutdown",
		"stopped /user/u1/k shutdown",
		"stopped /user/u1 shutdown",
		"stopped /system/s2 shutdown",
		"stopped /system/s1 shutdown",
	}
	if !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
	if got := len(stopRecordsOf(eng.Records())); got != len(want) {
		t.Errorf("%d lockstep.stop records, want %d", got, len(want))
	}
}

// A registry entry that is not an actor cell is a runtime bug: the stop sequence and the guardian
// sweep panic on it instead of looking it up forever.
func TestStopPanicsOnNonCellEntry(t *testing.T) {
	mustPanic := func(t *testing.T, fn func()) {
		t.Helper()
		defer func() {
			v := recover()
			if s, _ := v.(string); !strings.Contains(s, "is not an actor cell") {
				t.Errorf("panic %v, want one naming a non-cell entry", v)
			}
		}()
		fn()
	}
	t.Run("stopping parent", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		p, err := Spawn(sys, "p", idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		c := cellOf(t, sys, p)
		sys.reg.Add(p.Address().Path(), "bogus", sys.allocUID, "not a cell")
		c.stopReason = Normal
		mustPanic(t, c.stopNextChild)
	})
	t.Run("guardian", func(t *testing.T) {
		sys, _ := newManualSystem(t)
		sys.reg.Add(sys.user.Path(), "bogus", sys.allocUID, "not a cell")
		g := &sys.guardians
		g.phase = sweepUser
		mustPanic(t, func() { g.next() })
	})
}

// SUP-010, SUP-060: a stop request that reaches a cell before its begin takes it from Starting to
// Stopping with no incarnation: the factory never runs, Stopped is not called, and the one
// lockstep.stop reports inc 0.
func TestSUP010StopBeforeBegin(t *testing.T) {
	sys, eng := newManualSystem(t)
	sys.afterRegister = func(c actorCell) { c.requestStop(Normal, nil) }
	calls := 0
	var obs []string
	p, err := Spawn(sys, "p", func() Actor[any] { calls++; return &supProbe{obs: &obs} })
	if err != nil {
		t.Fatal(err)
	}
	sys.afterRegister = nil
	eng.RunUntilIdle()
	if calls != 0 || len(obs) != 0 {
		t.Errorf("factory calls %d, observations %q, want none", calls, obs)
	}
	if n := len(recordsOf(eng.Records(), "lockstep.start")); n != 0 {
		t.Errorf("%d lockstep.start records, want 0", n)
	}
	want := [][]string{{"actor=" + p.String(), "inc=0", "reason=normal", "err="}}
	if got := recordsOf(eng.Records(), "lockstep.stop"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.stop records %q, want %q", got, want)
	}
}

// SUP-050, SUP-100: ctx.Stop inside Stopped does nothing; ctx.Stop in Started stops the actor once
// Started returns.
func TestContextStop(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	p, err := Spawn(sys, "p", actorOf[any](&supProbe{
		obs:     &obs,
		onStart: func(ctx *Context[any]) { ctx.Stop() },
		onStop:  func(ctx *Context[any], _ StopReason) { ctx.Stop() },
	}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, p, any("x")) // never received
	eng.RunUntilIdle()
	if want := []string{"started /user/p 1", "stopped /user/p normal"}; !slices.Equal(obs, want) {
		t.Errorf("observations %q, want %q", obs, want)
	}
	if got, want := stopRecordsOf(eng.Records()), []string{p.String() + " normal"}; !slices.Equal(got, want) {
		t.Errorf("lockstep.stop records %q, want %q", got, want)
	}
}
