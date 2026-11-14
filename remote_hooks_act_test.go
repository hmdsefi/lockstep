// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

var errLink = errors.New("link refused")

// rwFunc is the message of a watcher: a function it runs during its turn.
type rwFunc func(ctx *lockstep.Context[rwFunc])

// rwWatcher runs the rwFunc it receives and appends every Terminated it gets to log, as
// "<own path> <watched> <reason>".
type rwWatcher struct{ log *[]string }

func (w rwWatcher) Receive(ctx *lockstep.Context[rwFunc], f rwFunc) { f(ctx) }

func (w rwWatcher) Signal(ctx *lockstep.Context[rwFunc], sig lockstep.Signal) {
	if t, ok := sig.(lockstep.Terminated); ok {
		*w.log = append(*w.log, fmt.Sprintf("%s %s %s", ctx.Self().Address().Path(), t.Addr, t.Reason))
	}
}

// spawnWatcher spawns an rwWatcher at /user/<name> on n that logs to log.
func spawnWatcher(t *testing.T, n *remoteNode, name string, log *[]string) lockstep.PID[rwFunc] {
	t.Helper()
	return spawnAt(t, n, name, func() lockstep.Actor[rwFunc] { return rwWatcher{log} })
}

// during runs f in a turn of pid on n and runs n until idle.
func during(t *testing.T, n *remoteNode, pid lockstep.PID[rwFunc], f rwFunc) {
	t.Helper()
	if err := lockstep.Send(n.sys, pid, f); err != nil {
		t.Fatalf("Send to %v: %v", pid, err)
	}
	n.eng.RunUntilIdle()
}

// watchIn calls Watch(addr) in a turn of pid and returns its error.
func watchIn(t *testing.T, n *remoteNode, pid lockstep.PID[rwFunc], addr lockstep.Address) error {
	t.Helper()
	err := errors.New("unset")
	during(t, n, pid, func(ctx *lockstep.Context[rwFunc]) { err = lockstep.Watch(ctx, addr) })
	return err
}

// unwatchIn calls Unwatch(addr) in a turn of pid.
func unwatchIn(t *testing.T, n *remoteNode, pid lockstep.PID[rwFunc], addr lockstep.Address) {
	t.Helper()
	during(t, n, pid, func(ctx *lockstep.Context[rwFunc]) { lockstep.Unwatch(ctx, addr) })
}

// eaddr converts a to the engine's address type.
func eaddr(a lockstep.Address) engine.Address {
	return engine.Address{Node: a.Node(), Path: a.Path(), UID: a.UID()}
}

// watchEnvelopes returns the watch envelopes n handed its transport from index from on, as
// "<kind> <from> <to>", failing the test for any other payload.
func watchEnvelopes(t *testing.T, n *remoteNode, from int) []string {
	t.Helper()
	var out []string
	for _, env := range n.eng.Delivered()[from:] {
		p, ok := hooks.Payload(env.Payload)
		if !ok {
			t.Fatalf("hooks.Payload(%T) reports no runtime payload", env.Payload)
		}
		var kind string
		switch p.Kind {
		case hooks.RemoteWatch:
			kind = "watch"
		case hooks.RemoteUnwatch:
			kind = "unwatch"
		default:
			t.Fatalf("envelope with kind %d, want a watch kind", p.Kind)
		}
		if p.Seq == 0 || p != (hooks.RemotePayload{Kind: p.Kind, Seq: p.Seq}) {
			t.Errorf("%s payload %+v, want its kind and a watch number only", kind, p)
		}
		out = append(out, kind+" "+env.From.String()+" "+env.To.String())
	}
	return out
}

// watchSeq returns the watch number of the last RemoteWatch from watcher to watched that n handed
// its transport, or 0 if there is none.
func watchSeq(t *testing.T, n *remoteNode, watcher, watched lockstep.Address) uint64 {
	t.Helper()
	var seq uint64
	for _, env := range n.eng.Delivered() {
		p, ok := hooks.Payload(env.Payload)
		if ok && p.Kind == hooks.RemoteWatch && env.From == eaddr(watcher) && env.To == eaddr(watched) {
			seq = p.Seq
		}
	}
	return seq
}

// terminate hands n's Host.Inbound a RemoteTerminated from watched to watcher with reason that
// echoes the number of watcher's last RemoteWatch of watched, as the remoting core does for a
// TERMINATED frame, and runs n until idle.
func terminate(t *testing.T, n *remoteNode, watched, watcher lockstep.Address, reason lockstep.StopReason) {
	t.Helper()
	terminateSeq(n, watched, watcher, reason, watchSeq(t, n, watcher, watched))
}

// terminateSeq is terminate with the echoed watch number seq.
func terminateSeq(n *remoteNode, watched, watcher lockstep.Address, reason lockstep.StopReason, seq uint64) {
	n.seq++
	n.eng.Inbound(engine.Envelope{
		From:    eaddr(watched),
		To:      eaddr(watcher),
		Seq:     n.seq,
		Payload: hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteTerminated, Reason: reason, Seq: seq}),
	})
	n.eng.RunUntilIdle()
}

// recordsSince returns the attributes of the records of kind on n from index from on.
func recordsSince(n *remoteNode, from int, kind string) []map[string]string {
	return attrsOf(n.eng.Records()[from:], kind)
}

// noTransport returns a system on a manual engine for node n1 whose bundle has no Transport, built
// through hooks.NewSystem.
func noTransport(t *testing.T) *remoteNode {
	t.Helper()
	eng := enginetest.New("n1", 1)
	b := eng.Engine()
	b.Transport = nil
	built, err := hooks.NewSystem(b, nil)
	if err != nil {
		t.Fatalf("hooks.NewSystem: %v", err)
	}
	eng.SetHost(built.Host)
	return &remoteNode{eng: eng, sys: systemOf(t, built)}
}

// REM §4.3: package lockstep sets the actor side of the remoting hooks in its init.
func TestRemoteActorHooksSet(t *testing.T) {
	for name, set := range map[string]bool{
		"AttachTransport": hooks.AttachTransport != nil,
		"LookupPath":      hooks.LookupPath != nil,
		"WatchFor":        hooks.WatchFor != nil,
		"WatchesDown":     hooks.WatchesDown != nil,
	} {
		if !set {
			t.Errorf("hooks.%s is nil", name)
		}
	}
}

// REM §4.3: hooks.Payload reads what hooks.NewPayload builds for RemoteTerminated, which needs a
// lockstep.StopReason. NewPayload builds nothing for RemoteWatch and RemoteUnwatch: the remoting
// core registers other nodes' watches with hooks.WatchFor, so Host.Inbound never gets one.
func TestRemoteWatchPayloads(t *testing.T) {
	for _, k := range []hooks.RemoteKind{hooks.RemoteWatch, hooks.RemoteUnwatch} {
		if got := hooks.NewPayload(hooks.RemotePayload{Kind: k, Seq: 3}); got != nil {
			t.Errorf("NewPayload of kind %d = %#v, want nil", k, got)
		}
	}
	for _, p := range []hooks.RemotePayload{
		{Kind: hooks.RemoteTerminated, Reason: lockstep.Normal, Seq: 4},
		{Kind: hooks.RemoteTerminated, Reason: lockstep.NoActor, Seq: 1},
		{Kind: hooks.RemoteTerminated, Reason: lockstep.Failed(errDown), Seq: 9},
	} {
		got, ok := hooks.Payload(hooks.NewPayload(p))
		if !ok || got != p {
			t.Errorf("Payload(NewPayload(%+v)) = %+v, %t, want it back and true", p, got, ok)
		}
	}
	panicsWith(t, "lockstep: hooks.NewPayload: RemoteTerminated with a reason of type <nil>, want lockstep.StopReason", func() {
		hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteTerminated})
	})
	panicsWith(t, "lockstep: hooks.NewPayload: RemoteTerminated with a reason of type string, want lockstep.StopReason", func() {
		hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteTerminated, Reason: "normal"})
	})
}

// AT-ACT-37 (defined here: actors.md gives it no text), ACT-602, ACT-630, ACT-640, ACT-660,
// REM-282: the remote-watch acceptance test on one node whose engine has a transport. Watch of
// another node's address hands the transport one RemoteWatch from the watcher to the address and
// records lockstep.watch alive=remote, and a repeated Watch does nothing; an inbound
// RemoteTerminated reaches the watcher once; Unwatch hands over a RemoteUnwatch, after which a
// RemoteTerminated is skipped; the end of an incarnation, a stop or a restart, hands over a
// RemoteUnwatch per remote address in insertion order and none for a local one.
func TestATACT37RemoteWatch(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	b := lockstep.PIDAt[int]("n2", "/user/b").Address()
	c := lockstep.PIDAt[int]("n3", "/user/c").Address()

	t.Run("watch, repeat, terminated once", func(t *testing.T) {
		var log []string
		w := spawnWatcher(t, n1, "w1", &log)
		d, r := len(n1.eng.Delivered()), len(n1.eng.Records())
		for i := range 2 {
			if err := watchIn(t, n1, w, b); err != nil {
				t.Fatalf("Watch %d of %v: %v", i, b, err)
			}
		}
		want := []string{"watch " + w.Address().String() + " " + b.String()}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes %q, want %q", got, want)
		}
		recs := recordsSince(n1, r, "lockstep.watch")
		wantRec := map[string]string{"watcher": w.Address().String(), "addr": b.String(), "alive": "remote"}
		if len(recs) != 1 || !mapsEqual(recs[0], wantRec) {
			t.Errorf("lockstep.watch records %v, want one %v", recs, wantRec)
		}

		terminate(t, n1, b, w.Address(), lockstep.Normal)
		terminate(t, n1, b, w.Address(), lockstep.Normal) // a duplicate: the watch has ended
		if want := []string{"/user/w1 " + b.String() + " normal"}; !slices.Equal(log, want) {
			t.Errorf("Terminated signals %q, want %q", log, want)
		}

		// The delivered Terminated consumed the watch: a new Watch goes to the transport again.
		d = len(n1.eng.Delivered())
		if err := watchIn(t, n1, w, b); err != nil {
			t.Fatal(err)
		}
		if got := watchEnvelopes(t, n1, d); len(got) != 1 {
			t.Errorf("envelopes after a new Watch %q, want one RemoteWatch", got)
		}
	})

	t.Run("unwatch", func(t *testing.T) {
		var log []string
		w := spawnWatcher(t, n1, "w2", &log)
		if err := watchIn(t, n1, w, b); err != nil {
			t.Fatal(err)
		}
		d, r := len(n1.eng.Delivered()), len(n1.eng.Records())
		unwatchIn(t, n1, w, b)
		unwatchIn(t, n1, w, b) // not watched any more: a no-op
		want := []string{"unwatch " + w.Address().String() + " " + b.String()}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes %q, want %q", got, want)
		}
		if recs := recordsSince(n1, r, "lockstep.unwatch"); len(recs) != 1 || recs[0]["addr"] != b.String() {
			t.Errorf("lockstep.unwatch records %v, want one for %v", recs, b)
		}
		terminate(t, n1, b, w.Address(), lockstep.Normal)
		if len(log) != 0 {
			t.Errorf("Terminated signals after Unwatch %q, want none", log)
		}

		// A Deliver error of the RemoteUnwatch changes nothing: the watch is gone.
		if err := watchIn(t, n1, w, b); err != nil {
			t.Fatal(err)
		}
		n1.eng.SetDeliverError(errLink)
		unwatchIn(t, n1, w, b)
		n1.eng.SetDeliverError(nil)
		terminate(t, n1, b, w.Address(), lockstep.Normal)
		if len(log) != 0 {
			t.Errorf("Terminated signals after a failed RemoteUnwatch %q, want none", log)
		}
	})

	t.Run("skipped when no longer watched", func(t *testing.T) {
		// The Terminated is queued while the watch is in effect, and the same turn unwatches.
		var log []string
		w := spawnWatcher(t, n1, "w3", &log)
		if err := watchIn(t, n1, w, b); err != nil {
			t.Fatal(err)
		}
		during(t, n1, w, func(ctx *lockstep.Context[rwFunc]) {
			hooks.WatchesDown(n1.sys, "n2", errDown)
			lockstep.Unwatch(ctx, b)
		})
		if len(log) != 0 {
			t.Errorf("Terminated signals %q, want none: Unwatch came before the delivery", log)
		}
	})

	t.Run("incarnation end", func(t *testing.T) {
		var log []string
		w := spawnWatcher(t, n1, "w4", &log)
		local := spawnWatcher(t, n1, "local", &log)
		for _, a := range []lockstep.Address{c, local.Address(), b} {
			if err := watchIn(t, n1, w, a); err != nil {
				t.Fatalf("Watch(%v): %v", a, err)
			}
		}
		d := len(n1.eng.Delivered())
		n1.eng.SetDeliverError(errLink) // ignored at the end of an incarnation
		during(t, n1, w, func(*lockstep.Context[rwFunc]) { panic("boom") })
		n1.eng.SetDeliverError(nil)
		self := w.Address().String()
		want := []string{"unwatch " + self + " " + c.String(), "unwatch " + self + " " + b.String()}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes at the restart %q, want %q", got, want)
		}
		terminate(t, n1, b, w.Address(), lockstep.Normal)
		if len(log) != 0 {
			t.Errorf("Terminated signals after the restart %q, want none", log)
		}

		// The new incarnation, after the restart's backoff, watches again; its stop ends that
		// watch the same way.
		n1.eng.Advance(time.Second)
		n1.eng.RunUntilIdle()
		if err := watchIn(t, n1, w, b); err != nil {
			t.Fatal(err)
		}
		d, r := len(n1.eng.Delivered()), len(n1.eng.Records())
		if err := lockstep.Stop(n1.sys, w.Address()); err != nil {
			t.Fatal(err)
		}
		n1.eng.RunUntilIdle()
		if got, want := watchEnvelopes(t, n1, d), []string{"unwatch " + self + " " + b.String()}; !slices.Equal(got, want) {
			t.Errorf("envelopes at the stop %q, want %q", got, want)
		}
		if recs := recordsSince(n1, r, "lockstep.unwatch"); len(recs) != 0 {
			t.Errorf("lockstep.unwatch records at the stop %v, want none", recs)
		}
	})
}

// mapsEqual reports whether a and b hold the same keys and values.
func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// ACT-602: a Deliver error of the RemoteWatch takes the address out of the watched set again and
// comes back wrapped, with no record; a later RemoteTerminated is dropped.
func TestRemoteWatchDeliverError(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	var log []string
	w := spawnWatcher(t, n1, "w", &log)
	b := lockstep.PIDAt[int]("n2", "/user/b").Address()
	r := len(n1.eng.Records())
	n1.eng.SetDeliverError(errLink)
	err := watchIn(t, n1, w, b)
	n1.eng.SetDeliverError(nil)
	if want := "lockstep: watch " + b.String() + ": link refused"; !errors.Is(err, errLink) || err.Error() != want {
		t.Errorf("Watch = %v, want errLink wrapped as %q", err, want)
	}
	if recs := recordsSince(n1, r, "lockstep.watch"); len(recs) != 0 {
		t.Errorf("lockstep.watch records %v, want none", recs)
	}
	terminate(t, n1, b, w.Address(), lockstep.Normal)
	if len(log) != 0 {
		t.Errorf("Terminated signals %q, want none", log)
	}
	// Not in the watched set: Unwatch does nothing, and Watch goes to the transport again.
	d := len(n1.eng.Delivered())
	unwatchIn(t, n1, w, b)
	if err := watchIn(t, n1, w, b); err != nil {
		t.Fatal(err)
	}
	want := []string{"watch " + w.Address().String() + " " + b.String()}
	if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
		t.Errorf("envelopes %q, want %q", got, want)
	}
}

// ACT-600: without a transport, Watch of another node's address is ErrNotLocal, nothing is handed
// over and nothing is recorded.
func TestWatchWithoutTransport(t *testing.T) {
	n1 := noTransport(t)
	var log []string
	w := spawnWatcher(t, n1, "w", &log)
	b := lockstep.PIDAt[int]("n2", "/user/b").Address()
	r := len(n1.eng.Records())
	err := watchIn(t, n1, w, b)
	if want := "lockstep: address not local: " + b.String(); !errors.Is(err, lockstep.ErrNotLocal) || err.Error() != want {
		t.Errorf("Watch = %v, want ErrNotLocal with text %q", err, want)
	}
	unwatchIn(t, n1, w, b)
	if recs := n1.eng.Records()[r:]; len(attrsOf(recs, "lockstep.watch"))+len(attrsOf(recs, "lockstep.unwatch")) != 0 {
		t.Errorf("watch records %v, want none", recs)
	}
	if d := n1.eng.Delivered(); len(d) != 0 {
		t.Errorf("delivered %v, want nothing", d)
	}
}

// watchCore plays the remoting core's part in death watch from src to dst: a RemoteWatch that src
// handed its transport becomes a hooks.WatchFor on dst, whose end, or a not-alive answer, comes
// back to src as an inbound RemoteTerminated; a RemoteUnwatch cancels the watch.
type watchCore struct {
	src, dst *remoteNode
	moved    int
	cancels  map[[2]engine.Address]func()
	back     []engine.Envelope // the RemoteTerminated envelopes for src, in order
}

// pump moves what src handed its transport since the last call to dst, runs dst until idle,
// hands src what came back and runs src until idle.
func (c *watchCore) pump(t *testing.T) {
	t.Helper()
	if c.cancels == nil {
		c.cancels = make(map[[2]engine.Address]func())
	}
	d := c.src.eng.Delivered()[c.moved:]
	c.moved += len(d)
	for _, env := range d {
		p, _ := hooks.Payload(env.Payload)
		key := [2]engine.Address{env.From, env.To}
		switch p.Kind {
		case hooks.RemoteWatch:
			watcher, watched, seq := env.From, env.To, p.Seq
			end := func(reason any) {
				c.back = append(c.back, engine.Envelope{From: watched, To: watcher,
					Payload: hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteTerminated, Reason: reason, Seq: seq})})
			}
			alive, cancel := hooks.WatchFor(c.dst.sys, watcher, watched, func(reason any) {
				if !c.dst.eng.Engine().Scheduler.InTurn() {
					t.Error("the WatchFor function runs outside engine context")
				}
				end(reason)
			})
			if !alive {
				end(lockstep.NoActor)
				continue
			}
			c.cancels[key] = cancel
		case hooks.RemoteUnwatch:
			if cancel := c.cancels[key]; cancel != nil {
				cancel()
				delete(c.cancels, key)
			}
		}
	}
	c.dst.eng.RunUntilIdle()
	back := c.back
	c.back = nil
	for _, env := range back {
		c.src.seq++
		env.Seq = c.src.seq
		c.src.eng.Inbound(env)
	}
	c.src.eng.RunUntilIdle()
}

// ACT-602, ACT-605, REM-280, REM-282: a watch across two nodes, carried as the core carries it:
// the watched actor's stop reaches the watcher with its reason, a not-alive address gets NoActor
// and an unwatched one nothing.
func TestRemoteWatchAcrossNodes(t *testing.T) {
	n1, n2 := newRemoteNode(t, "n1"), newRemoteNode(t, "n2")
	core := &watchCore{src: n1, dst: n2}
	var log []string
	w := spawnWatcher(t, n1, "w", &log)
	var got []int
	b := spawnAt(t, n2, "b", func() lockstep.Actor[int] { return collector{&got} })
	cAddr := spawnAt(t, n2, "c", func() lockstep.Actor[int] { return collector{&got} }).Address()
	byPath := lockstep.PIDAt[int]("n2", "/user/c").Address()
	none := lockstep.PIDAt[int]("n2", "/user/none").Address()

	for _, a := range []lockstep.Address{b.Address(), none, byPath} {
		if err := watchIn(t, n1, w, a); err != nil {
			t.Fatalf("Watch(%v): %v", a, err)
		}
	}
	core.pump(t)
	if want := []string{"/user/w " + none.String() + " no-actor"}; !slices.Equal(log, want) {
		t.Fatalf("Terminated signals %q, want %q", log, want)
	}

	unwatchIn(t, n1, w, byPath)
	core.pump(t)
	for _, a := range []lockstep.Address{b.Address(), cAddr} {
		if err := lockstep.Stop(n2.sys, a); err != nil {
			t.Fatal(err)
		}
	}
	n2.eng.RunUntilIdle()
	core.pump(t)
	want := []string{"/user/w " + none.String() + " no-actor", "/user/w " + b.Address().String() + " normal"}
	if !slices.Equal(log, want) {
		t.Errorf("Terminated signals %q, want %q", log, want)
	}
}

// ACT-605, REM-280: WatchFor registers a watcher on another node on a local actor's watcher list.
// A target that is not alive reports false; otherwise fn runs once, in engine context, with the
// stop reason, unless cancel ran first. A second registration of the pair replaces the first.
func TestRemoteWatchFor(t *testing.T) {
	n2 := newRemoteNode(t, "n2")
	var got []int
	spawn := func(name string) lockstep.Address {
		return spawnAt(t, n2, name, func() lockstep.Actor[int] { return collector{&got} }).Address()
	}
	watcher := engine.Address{Node: "n1", Path: "/user/w", UID: 7}
	var ends []string
	fn := func(tag string) func(any) {
		return func(reason any) {
			if !n2.eng.Engine().Scheduler.InTurn() {
				t.Errorf("%s: fn runs outside engine context", tag)
			}
			ends = append(ends, fmt.Sprintf("%s %v", tag, reason))
		}
	}
	stop := func(a lockstep.Address) {
		t.Helper()
		if err := lockstep.Stop(n2.sys, a); err != nil {
			t.Fatal(err)
		}
		n2.eng.RunUntilIdle()
	}

	a := spawn("a")
	for _, watched := range []engine.Address{
		{},
		{Node: "n2", Path: "/user/none"},
		{Node: "n2", Path: "/user"},
		{Node: "n2", Path: "/system"},
		{Node: "n2", Path: "/user/a", UID: a.UID() + 1},
		{Node: "n3", Path: "/user/a", UID: a.UID()},
	} {
		alive, cancel := hooks.WatchFor(n2.sys, watcher, watched, fn("dead"))
		if alive || cancel == nil {
			t.Errorf("WatchFor(%v) = %t, %v, want false and a cancel", watched, alive, cancel != nil)
			continue
		}
		cancel()
	}

	// By uid and by path; a second watcher; one cancelled.
	other := engine.Address{Node: "n3", Path: "/user/x", UID: 9}
	if alive, _ := hooks.WatchFor(n2.sys, watcher, eaddr(a), fn("uid")); !alive {
		t.Fatal("WatchFor by uid reports not alive")
	}
	if alive, _ := hooks.WatchFor(n2.sys, watcher, engine.Address{Node: "n2", Path: "/user/a"}, fn("path")); !alive {
		t.Fatal("WatchFor by path reports not alive")
	}
	alive, cancel := hooks.WatchFor(n2.sys, other, eaddr(a), fn("cancelled"))
	if !alive {
		t.Fatal("WatchFor of the second watcher reports not alive")
	}
	cancel()
	cancel()
	stop(a)
	if want := []string{"uid normal", "path normal"}; !slices.Equal(ends, want) {
		t.Errorf("ends %q, want %q", ends, want)
	}

	// The same pair again replaces the first registration: only the second fn runs, and the
	// first cancel does not end the second.
	ends = nil
	b := spawn("b")
	_, first := hooks.WatchFor(n2.sys, watcher, eaddr(b), fn("first"))
	hooks.WatchFor(n2.sys, watcher, eaddr(b), fn("second"))
	first()
	stop(b)
	if want := []string{"second normal"}; !slices.Equal(ends, want) {
		t.Errorf("ends %q, want %q", ends, want)
	}

	panicsWith(t, "lockstep: hooks.WatchFor: nil fn", func() { hooks.WatchFor(n2.sys, watcher, eaddr(b), nil) })
	panicsWith(t, "lockstep: hooks.WatchFor: watcher lockstep://n2/user/w#7 is not on another node", func() {
		hooks.WatchFor(n2.sys, engine.Address{Node: "n2", Path: "/user/w", UID: 7}, eaddr(b), fn("x"))
	})
	panicsWith(t, "lockstep: hooks.WatchFor: watcher <zero> is not on another node", func() {
		hooks.WatchFor(n2.sys, engine.Address{}, eaddr(b), fn("x"))
	})
}

// REM-294: WatchesDown ends every watch of an actor on the node with Failed(err), pushed in
// ascending (watched, watcher) text order, and leaves the watches of other nodes' actors alone.
func TestWatchesDown(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	var log []string
	w1 := spawnWatcher(t, n1, "w1", &log)
	w2 := spawnWatcher(t, n1, "w2", &log)
	w3 := spawnWatcher(t, n1, "w3", &log)
	a := lockstep.PIDAt[int]("n2", "/user/a").Address()
	b := lockstep.PIDAt[int]("n2", "/user/b").Address()
	c := lockstep.PIDAt[int]("n3", "/user/c").Address()
	for _, p := range []struct {
		w lockstep.PID[rwFunc]
		a lockstep.Address
	}{{w3, a}, {w1, b}, {w3, c}, {w2, a}} {
		if err := watchIn(t, n1, p.w, p.a); err != nil {
			t.Fatal(err)
		}
	}

	hooks.WatchesDown(n1.sys, "n2", errDown)
	n1.eng.RunUntilIdle()
	reason := lockstep.Failed(errDown).String()
	want := []string{
		"/user/w2 " + a.String() + " " + reason,
		"/user/w3 " + a.String() + " " + reason,
		"/user/w1 " + b.String() + " " + reason,
	}
	if !slices.Equal(log, want) {
		t.Errorf("Terminated signals %q, want %q", log, want)
	}

	// The ended watches take no Terminated any more; the watch of n3's actor still does.
	log = nil
	hooks.WatchesDown(n1.sys, "n2", errDown)
	hooks.WatchesDown(n1.sys, "", errDown)
	terminate(t, n1, b, w1.Address(), lockstep.Normal)
	terminate(t, n1, c, w3.Address(), lockstep.Normal)
	if want := []string{"/user/w3 " + c.String() + " normal"}; !slices.Equal(log, want) {
		t.Errorf("Terminated signals %q, want %q", log, want)
	}

	panicsWith(t, "lockstep: hooks.WatchesDown: nil error", func() { hooks.WatchesDown(n1.sys, "n2", nil) })
	panicsWith(t, "lockstep: hooks.WatchesDown: sys has type int, want a non-nil *lockstep.System", func() {
		hooks.WatchesDown(1, "n2", errDown)
	})
}

// rwInt has int's underlying type but is another type.
type rwInt int

// REM §4.3, REM-284: LookupPath finds the actor registered at a path, with its uid, when its
// message type has the type key, or for any type with an empty key; its errors are Lookup's.
func TestLookupPath(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	var got []int
	i := spawnAt(t, n1, "i", func() lockstep.Actor[int] { return collector{&got} })
	var log []string
	f := spawnWatcher(t, n1, "f", &log)

	for _, c := range []struct {
		path, key string
		want      lockstep.Address
	}{
		{"/user/i", "", i.Address()},
		{"/user/i", "int", i.Address()},
		{"/user/f", "github.com/hmdsefi/lockstep_test.rwFunc", f.Address()},
		{"/user/f", "", f.Address()},
	} {
		a, err := hooks.LookupPath(n1.sys, c.path, c.key)
		if err != nil || a != eaddr(c.want) {
			t.Errorf("LookupPath(%q, %q) = %v, %v, want %v", c.path, c.key, a, err, c.want)
		}
	}

	for _, c := range []struct {
		path, key string
		is        error
		text      string
	}{
		{"user/i", "", lockstep.ErrInvalidName, `lockstep: invalid name: path "user/i"`},
		{"/user/none", "", lockstep.ErrNotFound, `lockstep: not found: path "/user/none"`},
		{"/user", "", lockstep.ErrNotFound, `lockstep: not found: path "/user"`},
		{"/user/i", "github.com/hmdsefi/lockstep_test.rwInt", lockstep.ErrWrongType,
			"lockstep: wrong message type: " + i.Address().String() + ": spawned as int, looked up as github.com/hmdsefi/lockstep_test.rwInt"},
		{"/user/f", "lockstep_test.rwFunc", lockstep.ErrWrongType,
			"lockstep: wrong message type: " + f.Address().String() + ": spawned as lockstep_test.rwFunc, looked up as lockstep_test.rwFunc"},
	} {
		a, err := hooks.LookupPath(n1.sys, c.path, c.key)
		if !errors.Is(err, c.is) || err.Error() != c.text || a != (engine.Address{}) {
			t.Errorf("LookupPath(%q, %q) = %v, %v, want the zero address and %q", c.path, c.key, a, err, c.text)
		}
	}
	panicsWith(t, "lockstep: hooks.LookupPath: sys has type <nil>, want a non-nil *lockstep.System", func() {
		_, _ = hooks.LookupPath(nil, "/user/i", "")
	})
}

// errActor takes errors.
type errActor struct{}

func (errActor) Receive(*lockstep.Context[error], error) {}

// REM §4.3, REM-230: for an actor address, Assignable reports whether a live actor there (by uid,
// or by path when the uid is 0) takes v, as the actor's own type assertion does; a nil v needs an
// interface message type.
func TestAssignableActors(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	var got []int
	i := eaddr(spawnAt(t, n1, "i", func() lockstep.Actor[int] { return collector{&got} }).Address())
	e := eaddr(spawnAt(t, n1, "e", func() lockstep.Actor[error] { return errActor{} }).Address())
	stopped := spawnAt(t, n1, "gone", func() lockstep.Actor[int] { return collector{&got} }).Address()
	if err := lockstep.Stop(n1.sys, stopped); err != nil {
		t.Fatal(err)
	}
	gone := eaddr(stopped)
	n1.eng.RunUntilIdle()
	iPath := engine.Address{Node: "n1", Path: i.Path}

	for _, c := range []struct {
		to   engine.Address
		v    any
		want bool
	}{
		{i, 3, true},
		{iPath, 3, true},
		{i, rwInt(3), false}, // same underlying type, another type
		{i, int64(3), false},
		{i, nil, false},
		{e, errDown, true},
		{e, nil, true},
		{e, "x", false},
		{engine.Address{Node: "n1", Path: i.Path, UID: i.UID + 1}, 3, false}, // another uid
		{engine.Address{Node: "n2", Path: i.Path, UID: i.UID}, 3, false},     // another node
		{gone, 3, false},
		{engine.Address{Node: "n1", Path: "/user"}, 3, false},
		{engine.Address{Node: "n1", Path: "/user/none"}, 3, false},
	} {
		if got := hooks.Assignable(n1.sys, c.to, c.v); got != c.want {
			t.Errorf("Assignable(%v, %#v) = %t, want %t", c.to, c.v, got, c.want)
		}
	}
}

// fakeTransport records what it delivers.
type fakeTransport struct{ got []engine.Envelope }

func (f *fakeTransport) Deliver(env engine.Envelope) error {
	f.got = append(f.got, env)
	return nil
}

// ACT-050, REM §4.3: AttachTransport makes the transport the system's, whose send paths and
// watches use it, and returns the system's attachment. It refuses a second transport, the
// engine's own included, and a stopped system; Shutdown runs stop once, after the last actor
// stopped and before Engine.Stop.
func TestAttachTransport(t *testing.T) {
	n1 := noTransport(t)
	var got []int
	spawnAt(t, n1, "i", func() lockstep.Actor[int] { return collector{&got} })
	var log []string
	w := spawnWatcher(t, n1, "w", &log)
	b := lockstep.PIDAt[int]("n2", "/user/b")

	// Before the attachment a remote send is a dead letter.
	if err := lockstep.Send(n1.sys, b, 1); err != nil {
		t.Fatal(err)
	}
	if recs := attrsOf(n1.eng.Records(), "lockstep.deadletter"); len(recs) != 1 || recs[0]["reason"] != "unreachable" {
		t.Fatalf("lockstep.deadletter records %v, want one unreachable", recs)
	}

	tr := &fakeTransport{}
	stops := 0
	var stopSaw []string
	att, err := hooks.AttachTransport(n1.sys, tr, func() {
		stops++
		var phases []string
		for _, a := range attrsOf(n1.eng.Records(), "lockstep.shutdown") {
			phases = append(phases, a["phase"])
		}
		stopSaw = append(stopSaw, fmt.Sprintf("phases=%v engine-stopped=%t", phases, n1.eng.Stopped()))
	})
	if err != nil {
		t.Fatalf("AttachTransport: %v", err)
	}
	bundle := n1.eng.Engine()
	if att.Host == nil || att.Node != "n1" || att.Incarnation != n1.sys.Incarnation() || att.Clock != bundle.Clock ||
		att.Entropy != bundle.Entropy || att.Tracer != bundle.Tracer || att.Failer != bundle.Failer ||
		att.Log == nil || att.DeadLetter == nil {
		t.Errorf("attachment %+v does not carry the system's node, incarnation and engine parts", att)
	}

	// The send paths and Watch now use it.
	if err := lockstep.Send(n1.sys, b, 2); err != nil {
		t.Fatal(err)
	}
	if err := watchIn(t, n1, w, b.Address()); err != nil {
		t.Fatal(err)
	}
	if len(tr.got) != 2 {
		t.Fatalf("the transport got %d envelopes, want 2", len(tr.got))
	}
	if p, _ := hooks.Payload(tr.got[0].Payload); p != (hooks.RemotePayload{Kind: hooks.RemoteMsg, Value: 2}) {
		t.Errorf("first envelope's payload %+v, want RemoteMsg 2", p)
	}
	if p, _ := hooks.Payload(tr.got[1].Payload); p.Kind != hooks.RemoteWatch {
		t.Errorf("second envelope's payload %+v, want RemoteWatch", p)
	}

	// The attachment's Host is the system's.
	att.Host.Inbound(engine.Envelope{From: eaddr(b.Address()), To: engine.Address{Node: "n1", Path: "/user/i"}, Seq: 1,
		Payload: hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteMsg, Value: 5})})
	n1.eng.RunUntilIdle()
	if !slices.Equal(got, []int{5}) {
		t.Errorf("i received %v through the attachment's Host, want [5]", got)
	}

	// DeadLetter makes a dead letter with each of the transport's reasons.
	r := len(n1.eng.Records())
	reasons := []string{"queue-full", "unknown-node", "too-large-for-peer", "peer-restarted", "shutdown", "wrong-type"}
	for _, reason := range reasons {
		att.DeadLetter(eaddr(w.Address()), eaddr(b.Address()), reason, 7)
	}
	var gotReasons []string
	for _, a := range recordsSince(n1, r, "lockstep.deadletter") {
		if a["from"] != w.Address().String() || a["to"] != b.Address().String() || a["msg"] == "" {
			t.Errorf("lockstep.deadletter %v, want from %v to %v with the message", a, w.Address(), b.Address())
		}
		gotReasons = append(gotReasons, a["reason"])
	}
	if !slices.Equal(gotReasons, reasons) {
		t.Errorf("dead-letter reasons %q, want %q", gotReasons, reasons)
	}
	panicsWith(t, `lockstep: hooks.Attachment.DeadLetter: unknown reason "lost"`, func() {
		att.DeadLetter(eaddr(w.Address()), eaddr(b.Address()), "lost", 7)
	})

	// A second transport, and a nil one.
	if _, err := hooks.AttachTransport(n1.sys, &fakeTransport{}, nil); err == nil || err.Error() != "lockstep: transport already attached" {
		t.Errorf("second AttachTransport = %v, want lockstep: transport already attached", err)
	}
	panicsWith(t, "lockstep: hooks.AttachTransport: nil transport", func() { _, _ = hooks.AttachTransport(n1.sys, nil, nil) })
	panicsWith(t, "lockstep: hooks.AttachTransport: sys has type string, want a non-nil *lockstep.System", func() {
		_, _ = hooks.AttachTransport("x", tr, nil)
	})

	// Shutdown runs stop once, after phase=done and before Engine.Stop.
	if err := n1.sys.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	n1.eng.RunUntilIdle()
	if err := n1.sys.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"phases=[begin done] engine-stopped=false"}; stops != 1 || !slices.Equal(stopSaw, want) {
		t.Errorf("stop ran %d times, seeing %q, want once seeing %q", stops, stopSaw, want)
	}
	if !n1.eng.Stopped() {
		t.Error("the engine was not stopped")
	}
	if _, err := hooks.AttachTransport(n1.sys, &fakeTransport{}, nil); !errors.Is(err, lockstep.ErrStopped) {
		t.Errorf("AttachTransport after Shutdown = %v, want ErrStopped", err)
	}

	t.Run("engine transport", func(t *testing.T) {
		n := newRemoteNode(t, "n1")
		if _, err := hooks.AttachTransport(n.sys, &fakeTransport{}, nil); err == nil || err.Error() != "lockstep: transport already attached" {
			t.Errorf("AttachTransport over the engine's Transport = %v, want lockstep: transport already attached", err)
		}
	})

	t.Run("shutting down", func(t *testing.T) {
		n := noTransport(t)
		spawnAt(t, n, "i", func() lockstep.Actor[int] { return collector{&got} })
		if err := n.sys.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := hooks.AttachTransport(n.sys, &fakeTransport{}, nil); !errors.Is(err, lockstep.ErrStopped) {
			t.Errorf("AttachTransport during the shutdown = %v, want ErrStopped", err)
		}
		n.eng.RunUntilIdle()
	})

	t.Run("discarded", func(t *testing.T) {
		n := noTransport(t)
		hooks.Discard(n.sys)
		if _, err := hooks.AttachTransport(n.sys, &fakeTransport{}, nil); !errors.Is(err, lockstep.ErrStopped) {
			t.Errorf("AttachTransport after Discard = %v, want ErrStopped", err)
		}
	})
}

// ACT-050, OBS-060, OBS-120: on a production system the attachment's dead letters are logged and
// counted like the runtime's, and Shutdown has run stop when it returns.
func TestAttachTransportProduction(t *testing.T) {
	logger, h := newRecLogger(slog.LevelDebug)
	rec := &recMetrics{}
	sys := lockstep.New(lockstep.WithNodeName("n1"), lockstep.WithLogger(logger), lockstep.WithMetrics(rec))
	stopped := make(chan struct{}, 2)
	att, err := hooks.AttachTransport(sys, &fakeTransport{}, func() { stopped <- struct{}{} })
	if err != nil {
		sys.Shutdown(context.Background()) //nolint:errcheck // the test fails anyway
		t.Fatalf("AttachTransport: %v", err)
	}
	from := engine.Address{Node: "n1", Path: "/user/a", UID: 1}
	to := engine.Address{Node: "n2", Path: "/user/b", UID: 2}
	att.DeadLetter(from, to, "peer-restarted", 7)
	att.DeadLetter(from, to, "peer-restarted", 8)
	att.DeadLetter(from, to, "shutdown", 9)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := len(stopped); n != 1 {
		t.Errorf("stop ran %d times by the time Shutdown returned, want 1", n)
	}
	for reason, want := range map[string]uint64{"peer-restarted": 2, "shutdown": 1, "queue-full": 0} {
		if got := rec.count(t, "lockstep_deadletters_total", "reason", reason); got != want {
			t.Errorf("lockstep_deadletters_total{reason=%q} = %d, want %d", reason, got, want)
		}
	}
	var reasons []string
	for _, r := range h.withMsg("dead letter") {
		reasons = append(reasons, attrOf(t, r, "reason").String()+" "+attrOf(t, r, "to").String())
	}
	toText := to.String()
	if want := []string{"peer-restarted " + toText, "peer-restarted " + toText, "shutdown " + toText}; !slices.Equal(reasons, want) {
		t.Errorf("dead letter log events %q, want %q", reasons, want)
	}
}

// ACT-630, REM-282: a RemoteTerminated names the watch it ends by the watcher's number. After
// Unwatch and a new Watch of the same address, a TERMINATED still in flight for the old watch is
// dropped and leaves the new watch in effect; the new watch's own TERMINATED is delivered.
func TestRemoteTerminatedStaleWatch(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	var log []string
	w := spawnWatcher(t, n1, "w", &log)
	b := lockstep.PIDAt[int]("n2", "/user/b").Address()
	if err := watchIn(t, n1, w, b); err != nil {
		t.Fatal(err)
	}
	old := watchSeq(t, n1, w.Address(), b)
	during(t, n1, w, func(ctx *lockstep.Context[rwFunc]) {
		lockstep.Unwatch(ctx, b)
		if err := lockstep.Watch(ctx, b); err != nil {
			t.Error(err)
		}
	})
	cur := watchSeq(t, n1, w.Address(), b)
	if cur == old || old == 0 {
		t.Fatalf("watch numbers %d then %d, want two different non-zero numbers", old, cur)
	}
	terminateSeq(n1, b, w.Address(), lockstep.Normal, old)
	if len(log) != 0 {
		t.Errorf("Terminated signals %q for the old watch's TERMINATED, want none", log)
	}
	terminateSeq(n1, b, w.Address(), lockstep.Failed(errDown), cur)
	if want := []string{"/user/w " + b.String() + " " + lockstep.Failed(errDown).String()}; !slices.Equal(log, want) {
		t.Errorf("Terminated signals %q, want %q", log, want)
	}
}

// ACT-605, -race: on a production system a WatchFor cancel that races the target's final stop
// runs fn at most once.
func TestRemoteWatchForRace(t *testing.T) {
	sys := newProdSystem(t, lockstep.WithNodeName("n1"))
	watcher := engine.Address{Node: "n2", Path: "/user/w", UID: 7}
	var got []int
	for i := range 50 {
		name := fmt.Sprintf("t%d", i)
		pid, err := lockstep.Spawn(sys, name, func() lockstep.Actor[int] { return collector{&got} })
		if err != nil {
			t.Fatal(err)
		}
		var ran atomic.Int32
		alive, cancel := hooks.WatchFor(sys, watcher, eaddr(pid.Address()), func(any) { ran.Add(1) })
		if !alive {
			t.Fatalf("WatchFor(%v) reports not alive", pid)
		}
		var wg sync.WaitGroup
		wg.Go(cancel)
		wg.Go(func() {
			if err := lockstep.Stop(sys, pid.Address()); err != nil {
				t.Error(err)
			}
		})
		wg.Wait()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := lockstep.Lookup[int](sys, "/user/"+name); errors.Is(err, lockstep.ErrNotFound) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%v did not stop", pid)
			}
			time.Sleep(time.Millisecond)
		}
		cancel()
		if n := ran.Load(); n > 1 {
			t.Fatalf("fn ran %d times, want at most once", n)
		}
	}
}

// ACT-050, -race: AttachTransport racing Shutdown on a production system is either refused with
// ErrStopped, and then stop never runs, or succeeds, and then stop has run once when Shutdown
// returns.
func TestAttachTransportShutdownRace(t *testing.T) {
	var got []int
	for range 20 {
		sys := lockstep.New(lockstep.WithNodeName("n1"))
		if _, err := lockstep.Spawn(sys, "i", func() lockstep.Actor[int] { return collector{&got} }); err != nil {
			t.Fatal(err)
		}
		var stops atomic.Int32
		var attachErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			_, attachErr = hooks.AttachTransport(sys, &fakeTransport{}, func() { stops.Add(1) })
		})
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := sys.Shutdown(ctx)
		cancel()
		wg.Wait()
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		switch n := stops.Load(); {
		case attachErr == nil && n != 1:
			t.Fatalf("attached, and stop ran %d times by the end of Shutdown, want once", n)
		case attachErr != nil && !errors.Is(attachErr, lockstep.ErrStopped):
			t.Fatalf("AttachTransport = %v, want nil or ErrStopped", attachErr)
		case attachErr != nil && n != 0:
			t.Fatalf("refused, and stop ran %d times, want never", n)
		}
	}
}
