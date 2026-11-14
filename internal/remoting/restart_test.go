// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// logAll makes the core's dead letters and host calls go to the hook call log of h's fakeSys too,
// so that one list shows the order of every call the core makes outside its lock.
func (h *watchHarness) logAll() {
	dead, host := h.core.dead, h.core.host
	h.core.dead = func(from, to engine.Address, reason string, msg any) {
		h.sys.call(fmt.Sprintf("dead %s %s -> %s %v", reason, addrText(from), addrText(to), msg))
		dead(from, to, reason, msg)
	}
	h.core.host = hostFunc(func(env engine.Envelope) {
		h.sys.call("inbound " + envText(env))
		host.Inbound(env)
	})
}

// hostFunc is a Host that is a function.
type hostFunc func(engine.Envelope)

func (f hostFunc) Inbound(env engine.Envelope) { f(env) }

// TestPeerRestartSteps checks REM-294 when a HELLO shows a new incarnation of b: in that one
// event the core records the restart, makes the frames queued for the old incarnation dead letters
// "peer-restarted", calls hooks.FailAsks and hooks.WatchesDown with an error matching
// lockstep.ErrTerminated and ErrPeerRestarted, drops the outgoing watches, cancels b's watches of
// local actors, fails the lookups sent to b, takes the new incarnation and closes the old
// incarnation's outgoing session. Every runtime call runs after the lock is released, in that
// order.
func TestPeerRestartSteps(t *testing.T) {
	h := newWatchHarness(t)
	h.logAll()
	bV := engine.Address{Node: "b", Path: "/user/v", UID: 4}
	h.deliver(engine.Envelope{From: aY, To: bV, Payload: hooks.RemotePayload{Kind: hooks.RemoteWatch, Seq: 5}})
	out := h.outLink()
	out.full = true
	h.deliver(askEnv(ping{1}, askAddr))
	h.deliver(msgEnv(ping{2}))
	if err := h.core.Lookup(askAddr2, "b", "/user/v", ""); err != nil {
		t.Fatal(err)
	}
	h.watch(false, bX, aY, 3)
	h.recs()
	h.sys.takeCalls()

	l := &fakeLink{t: t, id: 10}
	h.core.Accepted(l)
	h.core.Frame(l, hello("b", "a", 8, "json", ""))
	same(t, "records", h.recs(), []string{
		"accept conn=10",
		"down peer=b conn=3 dir=in reason=replaced",
		"restart peer=b old-inc=7 new-inc=8",
		"down peer=b conn=1 dir=out reason=replaced",
		"up peer=b conn=10 dir=in peer-inc=8 max-frame=8192",
	})
	restarted := "remote: peer restarted: b (lockstep: actor terminated)"
	same(t, "calls", h.sys.takeCalls(), []string{
		"dead peer-restarted lockstep://a/user/x#1 -> lockstep://b/user/y#2 {1}",
		"dead peer-restarted lockstep://a/user/x#1 -> lockstep://b/user/y#2 {2}",
		"dead peer-restarted lockstep://a/temp/2#4 -> lockstep://b/user/v#0 /user/v",
		"FailAsks b: " + restarted,
		"WatchesDown b: " + restarted,
		"cancel lockstep://b/user/x#1 -> lockstep://a/user/y#2",
		"inbound 0 FAIL lockstep://b/user/v#0 -> lockstep://a/temp/2#4 " + restarted,
	})
	err := h.core.errs.PeerRestarted("b")
	if !errors.Is(err, lockstep.ErrTerminated) || !errors.Is(err, errPeerRestarted) {
		t.Errorf("the restart error %v does not match both sentinels", err)
	}
	p := h.core.peers["b"]
	if p.inc != 8 || len(p.queue) != 0 || p.watches != nil || p.watchers != nil || p.lookups != nil {
		t.Errorf("after the restart: inc %d, %d queued, watches %v, watchers %v, lookups %v",
			p.inc, len(p.queue), p.watches, p.watchers, p.lookups)
	}
	if !out.closed || p.out != nil {
		t.Error("the outgoing session to the old incarnation is still open")
	}
	if p.redial != nil || len(h.drv.links) != 1 {
		t.Error("the core dials b again with nothing waiting")
	}
	// The same incarnation again is no restart.
	h.acceptUp(11)
	h.core.Frame(h.drv.links[0], welcome("b", "a", 8, "json"))
	if recs := h.recs(); slices.ContainsFunc(recs, func(r string) bool { return r[:7] == "restart" }) {
		t.Errorf("records %q show a restart for a known incarnation", recs)
	}
}

// TestPeerRestartOnWelcome checks a restart seen by the outgoing session's WELCOME: the messages
// queued while b's incarnation was unknown are kept and written to the new incarnation, the WATCH,
// UNWATCH and LOOKUP frames are dead letters whatever their stamp, and the incoming session of the
// old one closes.
func TestPeerRestartOnWelcome(t *testing.T) {
	h := newWatchHarness(t)
	h.core.peers["b"].inc = 0 // b's incarnation is not known yet
	h.deliver(msgEnv(ping{1}))
	bV := engine.Address{Node: "b", Path: "/user/v", UID: 4}
	h.deliver(engine.Envelope{From: aY, To: bV, Payload: hooks.RemotePayload{Kind: hooks.RemoteWatch, Seq: 1}})
	h.deliver(engine.Envelope{From: aZ, To: bV, Payload: hooks.RemotePayload{Kind: hooks.RemoteUnwatch, Seq: 2}})
	if err := h.core.Lookup(askAddr, "b", "/user/v", ""); err != nil {
		t.Fatal(err)
	}
	h.core.peers["b"].inc = 7 // known now, from the incoming session
	h.deliver(msgEnv(ping{2}))
	h.recs()
	l := h.drv.links[0]
	h.core.Frame(l, welcome("b", "a", 8, "json"))
	same(t, "records", h.recs(), []string{
		"restart peer=b old-inc=7 new-inc=8",
		"down peer=b conn=3 dir=in reason=replaced",
		"up peer=b conn=1 dir=out peer-inc=8 max-frame=8192",
		"send kind=MSG seq=1 from=lockstep://a/user/x#1 to=lockstep://b/user/y#2 type=test.ping bytes=78",
	})
	same(t, "dead letters", h.takeDead(), []string{
		"peer-restarted lockstep://a/user/y#2 -> lockstep://b/user/v#4 <nil>",
		"peer-restarted lockstep://a/user/z#3 -> lockstep://b/user/v#4 <nil>",
		"peer-restarted lockstep://a/temp/1#3 -> lockstep://b/user/v#0 /user/v",
		"peer-restarted lockstep://a/user/x#1 -> lockstep://b/user/y#2 {2}",
	})
	same(t, "old incoming session", h.in.kinds(), []string{"WELCOME", "CLOSE replaced"})
}

// TestPeerRestartRuntime runs a restart into package lockstep's runtime: a local actor's watch of
// an actor on b ends with Terminated{Failed(err)}, err matching lockstep.ErrTerminated and
// ErrPeerRestarted.
func TestPeerRestartRuntime(t *testing.T) {
	h := newRuntimeHarness(t)
	var got []lockstep.Terminated
	watched := lockstep.PIDAt[ping]("b", "/user/v").Address()
	if _, err := lockstep.Spawn(h.sys, "w", func() lockstep.Actor[ping] {
		return watcherActor{watch: watched, got: &got}
	}); err != nil {
		t.Fatal(err)
	}
	h.eng.RunUntilIdle()
	h.core.Frame(h.drv.links[0], welcome("b", "a", 7, "json"))
	l := &fakeLink{t: t, id: 9}
	h.core.Accepted(l)
	h.core.Frame(l, hello("b", "a", 8, "json", ""))
	h.eng.RunUntilIdle()
	if len(got) != 1 || got[0].Addr != watched || got[0].Reason.Kind != lockstep.StopFailed ||
		!errors.Is(got[0].Reason.Err, lockstep.ErrTerminated) || !errors.Is(got[0].Reason.Err, errPeerRestarted) {
		t.Errorf("Terminated signals = %+v, want one Failed with the restart error", got)
	}
}

// trackClock is a Clock that knows which of its timers are armed: neither fired nor stopped.
type trackClock struct {
	engine.Clock
	next int
	live map[int]string
}

func (c *trackClock) AfterFunc(d time.Duration, label string, fn func()) engine.Cancel {
	c.next++
	id := c.next
	c.live[id] = label
	cancel := c.Clock.AfterFunc(d, label, func() {
		delete(c.live, id)
		fn()
	})
	return func() bool {
		delete(c.live, id)
		return cancel()
	}
}

// armed returns the labels of the armed timers, in arming order.
func (c *trackClock) armed() []string {
	var out []string
	for _, id := range slices.Sorted(maps.Keys(c.live)) {
		out = append(out, c.live[id])
	}
	return out
}

// TestCoreShutdown checks REM-296: Shutdown writes CLOSE shutdown on every session (peers by
// name, outgoing first, then sessions still waiting for a HELLO), makes the frames still queued
// dead letters "shutdown", closes every link and records each down; it stops every timer, cancels
// the watches of other nodes' actors and leaves the table. Afterwards nothing is sent.
func TestCoreShutdown(t *testing.T) {
	var tc *trackClock
	h := &watchHarness{}
	sys := newFakeSys()
	sys.t = t
	h.recvHarness = &recvHarness{sys: sys}
	h.ev = &evHost{t: t, core: &h.core}
	h.sendHarness = newSendHarness(t, func(c *Config) {
		tc = &trackClock{Clock: c.Clock, live: map[int]string{}}
		c.Clock = tc
		c.System = sys
		c.Host = h.ev
	})
	h.core = h.sendHarness.core
	sys.core = &h.core
	h.in = h.acceptUp(3)
	if !Register(h.core) || CoreOf(sys) != h.core {
		t.Fatal("Register did not make the core the system's")
	}
	if Register(h.core) {
		t.Error("a second Register succeeded")
	}

	h.deliver(msgEnv(ping{1}))
	bOut := h.outLink()
	bOut.full = true
	h.deliver(msgEnv(ping{2}))              // queued on a blocked link
	h.deliver(toNode(msgEnv(ping{3}), "c")) // c is dialing
	h.drv.errs["d"] = errors.New("refused") // d: a redial is armed
	h.deliver(toNode(msgEnv(ping{4}), "d")) //
	h.drv.errs["e"] = ErrUnknownNode        // e: an ask fails in a later callback
	h.deliver(toNode(askEnv(ping{5}, askAddr), "e"))
	waiting := &fakeLink{t: t, id: 20}
	h.core.Accepted(waiting)
	h.watch(false, bX, aY, 1)
	if err := h.core.Lookup(askAddr2, "c", "/user/v", ""); err != nil {
		t.Fatal(err)
	}
	same(t, "armed timers", tc.armed(), []string{
		labelTick, labelDialTimeout, labelRedial, labelFail, labelDialTimeout,
	})
	h.recs()
	h.takeDead()
	h.sys.takeCalls()

	h.core.Shutdown()
	same(t, "records", h.recs(), []string{
		"down peer=b conn=1 dir=out reason=shutdown",
		"down peer=b conn=3 dir=in reason=shutdown",
		"down peer=c conn=2 dir=out reason=shutdown",
		"down peer= conn=20 dir=in reason=shutdown",
	})
	same(t, "b's outgoing link (blocked)", bOut.kinds(), []string{"HELLO", "MSG"})
	same(t, "b's incoming link", h.in.kinds(), []string{"WELCOME", "CLOSE shutdown"})
	same(t, "c's link", h.drv.links[1].kinds(), []string{"HELLO", "CLOSE shutdown"})
	same(t, "waiting link", waiting.kinds(), []string{"CLOSE shutdown"})
	for _, l := range []*fakeLink{bOut, h.in, h.drv.links[1], waiting} {
		if !l.closed {
			t.Errorf("conn %d is still open", l.id)
		}
	}
	same(t, "dead letters", h.takeDead(), []string{
		"shutdown lockstep://a/user/x#1 -> lockstep://b/user/y#2 {2}",
		"shutdown lockstep://a/user/x#1 -> lockstep://c/user/y#2 {3}",
		"shutdown lockstep://a/temp/2#4 -> lockstep://c/user/v#0 /user/v",
		"shutdown lockstep://a/user/x#1 -> lockstep://d/user/y#2 {4}",
	})
	same(t, "calls", h.sys.takeCalls(), []string{"cancel lockstep://b/user/x#1 -> lockstep://a/user/y#2"})
	same(t, "armed timers", tc.armed(), nil)
	if CoreOf(sys) != nil {
		t.Error("the core is still in the table")
	}
	if len(h.core.links) != 0 || len(h.core.closed) != 0 || len(h.core.failTimers) != 0 {
		t.Errorf("left %d links, %d closed, %d fail timers", len(h.core.links), len(h.core.closed), len(h.core.failTimers))
	}

	// Afterwards: nothing opens, nothing is sent, nothing runs.
	h.deliver(msgEnv(ping{6}))
	same(t, "dead letters after", h.takeDead(), []string{"shutdown lockstep://a/user/x#1 -> lockstep://b/user/y#2 {6}"})
	if err := h.core.Connect("b"); err == nil {
		t.Error("Connect after Shutdown returned nil")
	}
	if err := h.core.Lookup(askAddr, "b", "/user/v", ""); err == nil {
		t.Error("Lookup after Shutdown returned nil")
	}
	late := &fakeLink{t: t, id: 21}
	h.core.Accepted(late)
	h.core.Frame(h.in, ctrl(KindPing, ""))
	h.core.Shutdown()
	h.core.Start()
	h.eng.Advance(time.Minute)
	if !late.closed || len(late.frames) != 0 {
		t.Errorf("a link accepted after Shutdown: closed %v, frames %q", late.closed, late.kinds())
	}
	same(t, "records after", h.recs(), nil)
	same(t, "events after", h.takeEvents(), nil)
	if n := len(h.drv.links); n != 2 {
		t.Errorf("%d dials, want 2", n)
	}
}

// TestCoreShutdownTable checks the table of CoreOf: Unregister removes any core of a system, and a
// shut down core does not remove another core registered for its system since.
func TestCoreShutdownTable(t *testing.T) {
	h1 := newSendHarness(t, nil)
	sys := h1.core.sys
	if !Register(h1.core) {
		t.Fatal("Register failed")
	}
	Unregister(sys)
	if CoreOf(sys) != nil {
		t.Fatal("Unregister left the core")
	}
	h2 := newSendHarness(t, func(c *Config) { c.System = sys })
	if !Register(h2.core) {
		t.Fatal("Register after Unregister failed")
	}
	h1.core.Shutdown()
	if CoreOf(sys) != h2.core {
		t.Error("the first core's Shutdown removed the second core")
	}
	h2.core.Shutdown()
	if CoreOf(sys) != nil {
		t.Error("Shutdown left the core in the table")
	}
}
