// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
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
	sig := waitFor(t, p.signals)
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
		if r := waitFor(t, reasons); r != Normal {
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
		if r := waitFor(t, entered); r != Normal {
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
		if err := waitFor(t, stopErr); err != nil {
			t.Errorf("Stop(ctx, self) = %v, want nil", err)
		}
		if r := waitFor(t, reasons); r != Normal {
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
	waitFor(t, entered)

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
	waitFor(t, ready)
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
	if m := waitFor(t, got); m != 2 {
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
	if m := waitFor(t, got); m != 3 {
		t.Errorf("the respawned a received %d, want 3", m)
	}
	stopAndWait(t, sys, w, wp, p.Address())

	want := "lockstep: not found: " + p.Address().String()
	if err := Stop(sys, p.Address()); !errors.Is(err, ErrNotFound) || err.Error() != want {
		t.Errorf("Stop(path address) with none registered = %v, want ErrNotFound with text %q", err, want)
	}
	// Nothing is delivered after the stop: the path address resolves to no actor (m25: the
	// lockstep.deadletter record is plan 1.3's).
	if o := sys.route(routeRequest{from: sys.user, to: p.Address(), typ: typeOf[int](), msg: 4}); o.kind != routeDead || o.reason != reasonNotFound {
		t.Errorf("route to the path address after the stop = %+v, want a not-found dead letter", o)
	}
	select {
	case m := <-got:
		t.Errorf("a stopped actor received %d", m)
	case <-time.After(50 * time.Millisecond):
	}
}

// SUP §8, m22: lockstep.stop records the actor, its last incarnation (0 without one), the reason's
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

// SUP-060 step 5, MBX-160, ruling B8: the final stop closes the queues and drains the mailbox,
// returning the Sent entries in queue order and discarding Self entries; a closed mailbox refuses
// every later entry and a closed system queue drops every later message.
func TestStopDrain(t *testing.T) {
	sys, eng := newManualSystem(t)
	var received []any
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{receive: func(_ *Context[any], m any) { received = append(received, m) }}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, a)
	c.offerUser(entry{msg: "m1", kind: entrySent}, nil)
	c.offerUser(entry{msg: "s1", kind: entrySelf, inc: c.inc}, nil)
	c.offerUser(entry{msg: "m2", kind: entrySent}, nil)
	sent := c.closeQueues()
	if len(sent) != 2 || sent[0].msg != "m1" || sent[1].msg != "m2" {
		t.Errorf("drained Sent entries %+v, want m1, m2", sent)
	}
	if c.offerUser(entry{msg: "late", kind: entrySent}, nil) {
		t.Error("a closed mailbox accepted an entry")
	}
	c.pushSystem(begin{}, nil)
	c.mu.Lock()
	n, m := c.user.size(), c.sysq.size()
	c.mu.Unlock()
	if n != 0 || m != 0 {
		t.Errorf("closed queues hold %d user and %d system messages, want none", n, m)
	}
	eng.RunUntilIdle()
	if len(received) != 0 {
		t.Errorf("Receive got %v", received)
	}
}
