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

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// runner is an any actor that runs the func(*Context[any]) it receives, so a test chooses what
// happens inside a turn, and keeps every signal it gets.
type runner struct{ sigs []Signal }

func (r *runner) Receive(ctx *Context[any], m any) {
	if f, ok := m.(func(*Context[any])); ok {
		f(ctx)
	}
}

func (r *runner) Signal(_ *Context[any], sig Signal) { r.sigs = append(r.sigs, sig) }

// spawnRunner spawns a runner named name and runs the engine until it started.
func spawnRunner(t *testing.T, sys *System, eng *enginetest.Engine, name string) (PID[any], *runner) {
	t.Helper()
	r := &runner{}
	pid, err := Spawn(sys, name, actorOf[any](r))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	return pid, r
}

// inTurn runs f during a turn of pid and returns when the engine is idle.
func inTurn(t *testing.T, sys *System, eng *enginetest.Engine, pid PID[any], f func(ctx *Context[any])) {
	t.Helper()
	send(t, sys, pid, any(f))
	eng.RunUntilIdle()
}

// gone is a path address that no actor is registered at: never alive.
func gone() Address { return PIDAt[any]("local", "/user/gone").Address() }

// AT-ACT-26 (zero, remote, self and not-alive clauses), ACT-600, ACT-660, SUP-335.
func TestATACT26WatchInvalidAndNotAlive(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, r := spawnRunner(t, sys, eng, "w")
	self := pid.Address()

	t.Run("zero", func(t *testing.T) {
		before := len(eng.Records())
		var err error
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { err = Watch(ctx, Address{}) })
		if !errors.Is(err, ErrNotFound) || err.Error() != "lockstep: not found: <zero>" {
			t.Errorf("Watch(zero) = %v, want ErrNotFound with text %q", err, "lockstep: not found: <zero>")
		}
		if n := len(recordsOf(eng.Records()[before:], "lockstep.watch")); n != 0 {
			t.Errorf("%d lockstep.watch records for an error, want none", n)
		}
	})

	t.Run("other node", func(t *testing.T) {
		other := PIDAt[any]("other", "/user/x").Address()
		var err error
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { err = Watch(ctx, other) })
		if want := "lockstep: address not local: " + other.String(); !errors.Is(err, ErrNotLocal) || err.Error() != want {
			t.Errorf("Watch(other node) = %v, want ErrNotLocal with text %q", err, want)
		}
	})

	t.Run("self", func(t *testing.T) {
		before := len(eng.Records())
		err := errors.New("unset")
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { err = Watch(ctx, self) })
		if err != nil {
			t.Errorf("Watch(self) = %v, want nil", err)
		}
		if n := len(recordsOf(eng.Records()[before:], "lockstep.watch")); n != 0 {
			t.Errorf("%d lockstep.watch records for Watch(self), want none", n)
		}
		if got := sys.registryWatchers(self); len(got) != 0 {
			t.Errorf("Watch(self) registered %v", got)
		}
	})

	t.Run("not alive", func(t *testing.T) {
		target := gone()
		before := len(eng.Records())
		var queuedBeforeReturn int
		err := errors.New("unset")
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			err = Watch(ctx, target)
			queuedBeforeReturn = ctx.cell.sysq.Len()
		})
		if err != nil {
			t.Fatalf("Watch(not alive) = %v, want nil", err)
		}
		if queuedBeforeReturn != 1 {
			t.Errorf("system queue holds %d messages when Watch returned, want 1 (SUP-335)", queuedBeforeReturn)
		}
		want := Terminated{Addr: target, Reason: NoActor}
		if len(r.sigs) != 1 || r.sigs[0] != Signal(want) {
			t.Errorf("signals = %v, want [%v]", r.sigs, want)
		}
		recs := recordsOf(eng.Records()[before:], "lockstep.watch")
		wantRec := []string{"watcher=" + self.String(), "addr=" + target.String(), "alive=false"}
		if len(recs) != 1 || !slices.Equal(recs[0], wantRec) {
			t.Errorf("lockstep.watch records = %q, want one %q", recs, wantRec)
		}
		if got := sys.registryWatchers(target); len(got) != 0 {
			t.Errorf("a not-alive watch registered %v", got)
		}
	})

	t.Run("outside the turn", func(t *testing.T) {
		var saved *Context[any]
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { saved = ctx })
		v := mustPanic(t, func() { _ = Watch(saved, gone()) })
		if want := "lockstep: Context of " + self.String() + " used outside its turn"; v != want {
			t.Errorf("Watch outside its turn panicked with %v, want %q", v, want)
		}
	})
}

// AT-ACT-03 (Watch clause), ACT-030, ACT-600: Watch of a guardian address is ErrNotFound, by uid
// or as a path address, and registers nothing.
func TestATACT03WatchGuardian(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, r := spawnRunner(t, sys, eng, "w")
	for _, g := range []Address{
		sys.user,
		sys.system,
		PIDAt[any]("local", "/user").Address(),
		PIDAt[any]("local", "/system").Address(),
	} {
		before := len(eng.Records())
		var err error
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { err = Watch(ctx, g) })
		if want := "lockstep: not found: " + g.String(); !errors.Is(err, ErrNotFound) || err.Error() != want {
			t.Errorf("Watch(%v) = %v, want ErrNotFound with text %q", g, err, want)
		}
		if n := len(recordsOf(eng.Records()[before:], "lockstep.watch")); n != 0 {
			t.Errorf("Watch(%v): %d lockstep.watch records, want none", g, n)
		}
	}
	if len(r.sigs) != 0 {
		t.Errorf("signals = %v, want none", r.sigs)
	}
}

// AT-ACT-29 (b), ACT-630, ACT-660, SUP-335: Unwatch right after a not-alive Watch suppresses the
// queued Terminated, and records the unwatch because the address was in the watched set.
func TestATACT29UnwatchQueued(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, r := spawnRunner(t, sys, eng, "w")
	target := gone()
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		if err := Watch(ctx, target); err != nil {
			t.Errorf("Watch: %v", err)
		}
		Unwatch(ctx, target)
	})
	if len(r.sigs) != 0 {
		t.Errorf("signals = %v, want none: Unwatch suppresses the queued Terminated", r.sigs)
	}
	self := pid.Address().String()
	if got, want := recordsOf(eng.Records(), "lockstep.unwatch"), [][]string{{"watcher=" + self, "addr=" + target.String()}}; !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.unwatch records = %q, want %q", got, want)
	}

	// The skipped Terminated left the set: a new Watch of the address signals again (ACT-620 only
	// covers a watch in effect).
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { _ = Watch(ctx, target) })
	if want := Signal(Terminated{Addr: target, Reason: NoActor}); len(r.sigs) != 1 || r.sigs[0] != want {
		t.Errorf("signals after a new Watch = %v, want [%v]", r.sigs, want)
	}
}

// AT-ACT-29 (c), ACT-630, ACT-660: Unwatch of an unwatched, zero or remote address does nothing and
// records nothing; outside the turn it panics.
func TestATACT29UnwatchNoop(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, r := spawnRunner(t, sys, eng, "w")
	other, _ := spawnRunner(t, sys, eng, "other")
	before := len(eng.Records())
	var saved *Context[any]
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		saved = ctx
		Unwatch(ctx, other.Address())                          // never watched
		Unwatch(ctx, Address{})                                // zero
		Unwatch(ctx, PIDAt[any]("other", "/user/x").Address()) // another node
		Unwatch(ctx, ctx.Self().Address())                     // self
	})
	if n := len(recordsOf(eng.Records()[before:], "lockstep.unwatch")); n != 0 {
		t.Errorf("%d lockstep.unwatch records, want none", n)
	}
	if len(r.sigs) != 0 || len(sys.registryWatchers(other.Address())) != 0 {
		t.Errorf("signals %v, watchers %v, want none", r.sigs, sys.registryWatchers(other.Address()))
	}
	v := mustPanic(t, func() { Unwatch(saved, other.Address()) })
	if want := "lockstep: Context of " + pid.Address().String() + " used outside its turn"; v != want {
		t.Errorf("Unwatch outside its turn panicked with %v, want %q", v, want)
	}
}

// AT-ACT-31, ACT-650: the registry lists watchers in the order their entries were added; a watcher
// that unwatches and watches again moves to the end.
func TestATACT31WatcherOrder(t *testing.T) {
	sys, eng := newManualSystem(t)
	tgt, _ := spawnRunner(t, sys, eng, "t")
	w3, _ := spawnRunner(t, sys, eng, "w3")
	w1, _ := spawnRunner(t, sys, eng, "w1")
	w2, _ := spawnRunner(t, sys, eng, "w2")
	watch := func(w PID[any]) {
		inTurn(t, sys, eng, w, func(ctx *Context[any]) {
			if err := Watch(ctx, tgt.Address()); err != nil {
				t.Errorf("Watch: %v", err)
			}
		})
	}
	watch(w2)
	watch(w3)
	watch(w1)
	addrs := func(ps ...PID[any]) []Address {
		out := make([]Address, len(ps))
		for i, p := range ps {
			out[i] = p.Address()
		}
		return out
	}
	if got, want := sys.registryWatchers(tgt.Address()), addrs(w2, w3, w1); !slices.Equal(got, want) {
		t.Errorf("watchers = %v, want %v", got, want)
	}
	inTurn(t, sys, eng, w2, func(ctx *Context[any]) { Unwatch(ctx, tgt.Address()) })
	if got, want := sys.registryWatchers(tgt.Address()), addrs(w3, w1); !slices.Equal(got, want) {
		t.Errorf("watchers after w2 unwatched = %v, want %v", got, want)
	}
	watch(w2)
	if got, want := sys.registryWatchers(tgt.Address()), addrs(w3, w1, w2); !slices.Equal(got, want) {
		t.Errorf("watchers after w2 watched again = %v, want %v", got, want)
	}
}

// ACT-600, ACT-620, ACT-630, ACT-660: a watch of an alive actor registers and records alive=true;
// a second Watch adds neither an entry nor a record; Unwatch removes the entry and records once.
func TestWatchAliveRecord(t *testing.T) {
	sys, eng := newManualSystem(t)
	tgt, _ := spawnRunner(t, sys, eng, "t")
	pid, r := spawnRunner(t, sys, eng, "w")
	self := pid.Address()
	wantRec := []string{"watcher=" + self.String(), "addr=" + tgt.Address().String(), "alive=true"}

	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		if err := Watch(ctx, tgt.Address()); err != nil {
			t.Errorf("Watch: %v", err)
		}
	})
	if got := recordsOf(eng.Records(), "lockstep.watch"); len(got) != 1 || !slices.Equal(got[0], wantRec) {
		t.Errorf("lockstep.watch records = %q, want one %q", got, wantRec)
	}
	if got := sys.registryWatchers(tgt.Address()); !slices.Equal(got, []Address{self}) {
		t.Errorf("watchers = %v, want [%v]", got, self)
	}

	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		if err := Watch(ctx, tgt.Address()); err != nil {
			t.Errorf("second Watch: %v", err)
		}
	})
	if got := recordsOf(eng.Records(), "lockstep.watch"); len(got) != 1 {
		t.Errorf("%d lockstep.watch records after a second Watch, want 1", len(got))
	}
	if got := sys.registryWatchers(tgt.Address()); len(got) != 1 {
		t.Errorf("watchers after a second Watch = %v, want one entry", got)
	}

	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { Unwatch(ctx, tgt.Address()); Unwatch(ctx, tgt.Address()) })
	if got := recordsOf(eng.Records(), "lockstep.unwatch"); len(got) != 1 {
		t.Errorf("lockstep.unwatch records = %q, want one", got)
	}
	if got := sys.registryWatchers(tgt.Address()); len(got) != 0 {
		t.Errorf("watchers after Unwatch = %v, want none", got)
	}
	if len(r.sigs) != 0 {
		t.Errorf("signals = %v, want none", r.sigs)
	}
}

// ACT-620, ACT-630: a second Watch of a not-alive address while its Terminated is queued adds
// nothing, and the one Terminated that arrives takes the address out of the watched set.
func TestWatchNotAliveDuplicate(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, r := spawnRunner(t, sys, eng, "w")
	target := gone()
	var c *cell[any]
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		c = ctx.cell
		_ = Watch(ctx, target)
		_ = Watch(ctx, target)
	})
	if len(r.sigs) != 1 {
		t.Errorf("signals = %v, want exactly one Terminated", r.sigs)
	}
	if c.ctx.watched.has(target) || len(c.ctx.watched.list) != 0 {
		t.Errorf("watched set still holds %v after the Terminated was delivered", c.ctx.watched.list)
	}
}

// ACT-630: delivery skips a Terminated whose Addr is not in the watched set, and an actor without
// SignalReceiver still takes the address out of the set.
func TestTerminatedSkipRule(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, r := spawnRunner(t, sys, eng, "w")
	c := cellOf(t, sys, pid)
	c.pushSignal(Terminated{Addr: gone(), Reason: NoActor}) // never watched
	eng.RunUntilIdle()
	if len(r.sigs) != 0 {
		t.Errorf("an unwatched Terminated was delivered: %v", r.sigs)
	}

	plain, err := Spawn(sys, "plain", actorOf[any](&fnActor[any]{}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	pc := cellOf(t, sys, plain)
	pc.ctx.watched.add(gone())
	pc.pushSignal(Terminated{Addr: gone(), Reason: NoActor})
	eng.RunUntilIdle()
	if pc.ctx.watched.has(gone()) {
		t.Error("the watched set of an actor without SignalReceiver still holds the delivered address")
	}
}

// AT-ACT-25: ACT-600, ACT-660, SUP-330. A watcher of an alive actor gets exactly one Terminated
// with the stop reason, and the watch record precedes the stop record.
func TestATACT25WatchAlive(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	tp, err := Spawn(sys, "t", idle[int]())
	if err != nil {
		t.Fatal(err)
	}
	w, wp := spawnProbe[any](t, sys, "w", nil)
	watchFrom(t, sys, w, tp.Address())
	if err := Stop(sys, tp.Address()); err != nil {
		t.Fatal(err)
	}
	if term := awaitTerminated(t, wp, tp.Address()); term != (Terminated{Addr: tp.Address(), Reason: Normal}) {
		t.Errorf("Terminated = %v, want reason Normal", term)
	}
	noSignal(t, wp, 100*time.Millisecond)

	recs := rec.Records()
	watch := recordIndex(recs, "lockstep.watch", "watcher="+w.String(), "addr="+tp.String(), "alive=true")
	stop := recordIndex(recs, "lockstep.stop", "actor="+tp.String())
	if watch < 0 || stop < 0 || watch > stop {
		t.Errorf("lockstep.watch at %d, lockstep.stop at %d: want both, the watch first", watch, stop)
	}
	want := []string{"actor=" + tp.String(), "inc=1", "reason=normal", "err="}
	if got := recordsOf(recs, "lockstep.stop"); len(got) != 1 || !slices.Equal(got[0], want) {
		t.Errorf("lockstep.stop records = %q, want one %q", got, want)
	}
}

// watchingProbe is a probe whose Stopped hook runs stopped.
type watchingProbe struct {
	*probe[any]
	stopped func(ctx *Context[any])
}

func (p *watchingProbe) Stopped(ctx *Context[any], _ StopReason) { p.stopped(ctx) }

// AT-ACT-26 (stopped-actor and Stopped clauses): ACT-600, ACT-660. The address of an actor that
// completed its stop is not alive; Watch from the watcher's own Stopped hook returns ErrStopped and
// registers nothing.
func TestATACT26WatchStoppedActor(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	d, err := Spawn(sys, "d", idle[int]())
	if err != nil {
		t.Fatal(err)
	}
	live, err := Spawn(sys, "live", idle[int]())
	if err != nil {
		t.Fatal(err)
	}
	stoppedErr := make(chan error, 1)
	wp := &watchingProbe{probe: &probe[any]{signals: make(chan Signal, probeSignals)}}
	wp.stopped = func(ctx *Context[any]) { stoppedErr <- Watch(ctx, live.Address()) }
	w, err := Spawn(sys, "w", actorOf[any](wp))
	if err != nil {
		t.Fatal(err)
	}

	stopAndWait(t, sys, w, wp.probe, d.Address())
	// The Terminated took d out of the watched set: this Watch is of a dead address.
	watchFrom(t, sys, w, d.Address())
	if term := awaitTerminated(t, wp.probe, d.Address()); term.Reason != NoActor {
		t.Errorf("Terminated = %v, want reason NoActor", term)
	}
	want := []string{"watcher=" + w.String(), "addr=" + d.String(), "alive=false"}
	if i := recordIndex(rec.Records(), "lockstep.watch", want...); i < 0 {
		t.Errorf("no lockstep.watch record %q", want)
	}

	if err := Stop(sys, w.Address()); err != nil {
		t.Fatal(err)
	}
	got := recv(t, stoppedErr)
	if want := "lockstep: stopped: " + w.String(); !errors.Is(got, ErrStopped) || got.Error() != want {
		t.Errorf("Watch from Stopped = %v, want ErrStopped with text %q", got, want)
	}
	if ws := sys.registryWatchers(live.Address()); len(ws) != 0 {
		t.Errorf("Watch from Stopped registered %v", ws)
	}
}

// AT-ACT-27: ACT-600. Watch races with the target's stop; every iteration delivers exactly one
// Terminated, with reason Normal or NoActor. Run with -race.
func TestATACT27WatchRace(t *testing.T) {
	sys := newLocalSystem(t)
	w, wp := spawnProbe[any](t, sys, "w", func(ctx *Context[any], m any) {
		if a, ok := m.(Address); ok {
			if err := Watch(ctx, a); err != nil {
				t.Errorf("Watch(%v): %v", a, err)
			}
		}
	})
	const n = 1000
	for i := range n {
		tp, err := Spawn(sys, fmt.Sprintf("t%d", i), idle[int]())
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := Stop(sys, tp.Address()); err != nil {
				t.Errorf("Stop: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := Send(sys, w, any(tp.Address())); err != nil {
				t.Errorf("Send: %v", err)
			}
		}()
		wg.Wait()
		term := awaitTerminated(t, wp, tp.Address())
		if term.Reason != Normal && term.Reason != NoActor {
			t.Fatalf("iteration %d: Terminated %v, want reason Normal or NoActor", i, term)
		}
	}
	noSignal(t, wp, 100*time.Millisecond)
}

// AT-ACT-29 (a): ACT-630. After Unwatch of an alive actor, its stop gives the former watcher
// nothing.
func TestATACT29UnwatchAlive(t *testing.T) {
	sys, eng := newManualSystem(t)
	tgt, _ := spawnRunner(t, sys, eng, "t")
	pid, r := spawnRunner(t, sys, eng, "w")
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		if err := Watch(ctx, tgt.Address()); err != nil {
			t.Errorf("Watch: %v", err)
		}
		Unwatch(ctx, tgt.Address())
	})
	if err := Stop(sys, tgt.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if len(r.sigs) != 0 {
		t.Errorf("signals = %v, want none after Unwatch", r.sigs)
	}
	if n := len(recordsOf(eng.Records(), "lockstep.stop")); n != 1 {
		t.Errorf("%d lockstep.stop records, want 1: the target stopped", n)
	}
}

// SUP-060 step 1, SUP-340, ACT-640: when a watcher starts its stop, its watches leave every
// watcher list and its watched set, so the target's later stop pushes it nothing.
func TestStopDropsWatches(t *testing.T) {
	sys, eng := newManualSystem(t)
	tgt, _ := spawnRunner(t, sys, eng, "t")
	pid, _ := spawnRunner(t, sys, eng, "w")
	c := cellOf(t, sys, pid)
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		if err := Watch(ctx, tgt.Address()); err != nil {
			t.Errorf("Watch: %v", err)
		}
	})
	if err := Stop(sys, pid.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if ws := sys.registryWatchers(tgt.Address()); len(ws) != 0 {
		t.Errorf("watchers of t after w stopped = %v, want none", ws)
	}
	if len(c.ctx.watched.list) != 0 {
		t.Errorf("w's watched set after its stop = %v, want empty", c.ctx.watched.list)
	}
}
