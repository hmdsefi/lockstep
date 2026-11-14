// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// fakeWatch is one registration of fakeSys.watchFor.
type fakeWatch struct {
	watcher engine.Address
	fn      func(reason any)
	gone    bool // cancelled, replaced or ended
}

// call logs a hook call of s, in the shared log when there is one, and checks that the core's lock
// is free.
func (s *fakeSys) call(what string) {
	if s.core != nil && *s.core != nil {
		if !(*s.core).mu.TryLock() {
			s.t.Errorf("%s called with the core's lock held", what)
		} else {
			(*s.core).mu.Unlock()
		}
	}
	s.calls = append(s.calls, what)
}

// takeCalls returns the hook calls so far and forgets them.
func (s *fakeSys) takeCalls() []string {
	c := s.calls
	s.calls = nil
	return c
}

// keyOf is the type key of t, as hooks.LookupPath compares it.
func keyOf(t reflect.Type) string {
	if t.PkgPath() != "" {
		return t.PkgPath() + "." + t.Name()
	}
	return t.String()
}

// lookupPath is hooks.LookupPath for s: its errors wrap lockstep.ErrNotFound and ErrWrongType.
func (s *fakeSys) lookupPath(path, typeKey string) (engine.Address, error) {
	a, ok := s.actors[path]
	if !ok {
		return engine.Address{}, fmt.Errorf("%w: path %q", lockstep.ErrNotFound, path)
	}
	if typeKey != "" && typeKey != keyOf(a.typ) {
		return engine.Address{}, fmt.Errorf("%w: %s is not %s", lockstep.ErrWrongType, path, typeKey)
	}
	return engine.Address{Node: "a", Path: path, UID: a.uid}, nil
}

// watchFor is hooks.WatchFor for s. A second registration of a pair replaces the first. With
// s.endNow set, the watched actor ends before watchFor returns, as a stop racing the registration
// on another goroutine would.
func (s *fakeSys) watchFor(watcher, watched engine.Address, fn func(reason any)) (bool, func()) {
	s.call("WatchFor " + watcher.String() + " -> " + watched.String())
	a, ok := s.actors[watched.Path]
	if !ok || (watched.UID != 0 && watched.UID != a.uid) {
		return false, func() {}
	}
	if s.watches == nil {
		s.watches = map[string][]*fakeWatch{}
	}
	for _, w := range s.watches[watched.Path] {
		if w.watcher == watcher {
			w.gone = true
		}
	}
	w := &fakeWatch{watcher: watcher, fn: fn}
	s.watches[watched.Path] = append(s.watches[watched.Path], w)
	if r := s.endNow; r != nil {
		s.endNow = nil
		w.gone = true
		fn(*r)
	}
	return true, func() {
		s.call("cancel " + watcher.String() + " -> " + watched.String())
		w.gone = true
	}
}

// stop ends the actor at path with r: the function of every live registration on it runs.
func (s *fakeSys) stop(path string, r lockstep.StopReason) {
	delete(s.actors, path)
	for _, w := range s.watches[path] {
		if !w.gone {
			w.gone = true
			w.fn(r)
		}
	}
	delete(s.watches, path)
}

// live returns the registrations on the actor at path that are still live, as watcher texts.
func (s *fakeSys) live(path string) []string {
	var out []string
	for _, w := range s.watches[path] {
		if !w.gone {
			out = append(out, w.watcher.String())
		}
	}
	return out
}

// watchHarness is a recvHarness whose hook calls are checked for the lock, with a numbering of the
// frames sent on the incoming session.
type watchHarness struct {
	*recvHarness
	seq uint64 // the number of the last frame sent on in
}

func newWatchHarness(t *testing.T) *watchHarness {
	t.Helper()
	h := &watchHarness{recvHarness: newRecvHarness(t)}
	h.sys.core = &h.core
	h.sys.t = t
	h.sys.actors["/user/z"] = fakeActor{uid: 3, typ: reflect.TypeFor[blob]()}
	return h
}

// peerFrame sends the next data frame on the incoming session from b.
func (h *watchHarness) peerFrame(k Kind, from, to, typ string, payload []byte) {
	h.seq++
	h.send(h.in, k, h.seq, from, to, typ, payload)
}

// watch sends WATCH, or UNWATCH when un is set, of to by from numbered n.
func (h *watchHarness) watch(un bool, from, to engine.Address, n uint64) {
	k := KindWatch
	if un {
		k = KindUnwatch
	}
	h.peerFrame(k, addrText(from), addrText(to), "", AppendWatch(nil, n))
}

// outLink returns the outgoing link to b, bringing it up with a WELCOME of incarnation 7 if it is
// not up yet.
func (h *watchHarness) outLink() *fakeLink {
	h.t.Helper()
	p := h.core.peers["b"]
	if p == nil || p.out == nil {
		h.t.Fatal("no outgoing session to b")
	}
	l, _ := p.out.link.(*fakeLink)
	if !p.out.up {
		h.core.Frame(l, welcome("b", "a", 7, "json"))
	}
	return l
}

// terminated returns the TERMINATED frames written on l, as "<sender> -> <recipient> #<n>
// <kind> [<code> <message>]".
func terminated(t *testing.T, l *fakeLink) []string {
	t.Helper()
	var out []string
	for _, f := range l.frames {
		if f.Kind != KindTerminated {
			continue
		}
		tm, err := ParseTerminated(f.Payload)
		if err != nil {
			t.Fatal(err)
		}
		s := fmt.Sprintf("%s -> %s #%d %d", f.Sender, f.Recipient, tm.Watch, tm.Kind)
		if tm.Kind == StopFailed {
			s += fmt.Sprintf(" %d %s", tm.Failure.Code, tm.Failure.Message)
		}
		out = append(out, s)
	}
	return out
}

var (
	bW = engine.Address{Node: "b", Path: "/user/w", UID: 7} // another watcher on b
	aZ = engine.Address{Node: "a", Path: "/user/z", UID: 3} // a second live actor of node a
)

// TestRemoteWatchTerminatedWithReason checks REM-280: a WATCH registers its watcher with
// hooks.WatchFor, outside the lock, and the watched actor's end answers it with TERMINATED,
// carrying the reason and echoing the watch number. A failure goes as its code and message.
func TestRemoteWatchTerminatedWithReason(t *testing.T) {
	h := newWatchHarness(t)
	h.watch(false, bX, aY, 4)
	h.watch(false, bW, aZ, 9)
	same(t, "hook calls", h.sys.takeCalls(), []string{
		"WatchFor lockstep://b/user/x#1 -> lockstep://a/user/y#2",
		"WatchFor lockstep://b/user/w#7 -> lockstep://a/user/z#3",
	})
	if len(h.drv.links) != 0 {
		t.Fatal("a WATCH of a live actor sent something")
	}
	if got := h.sys.live("/user/y"); !reflect.DeepEqual(got, []string{bX.String()}) {
		t.Errorf("registrations on /user/y = %q", got)
	}
	h.sys.stop("/user/y", lockstep.Failed(fmt.Errorf("crashed: %w", errCodeFive)))
	h.sys.stop("/user/z", lockstep.Normal)
	l := h.outLink()
	same(t, "TERMINATED frames", terminated(t, l), []string{
		"lockstep://a/user/y#2 -> lockstep://b/user/x#1 #4 1 5 crashed: an error with code 5",
		"lockstep://a/user/z#3 -> lockstep://b/user/w#7 #9 0",
	})
	if n := len(h.core.peers["b"].watchers); n != 0 {
		t.Errorf("the core still holds %d watches of b", n)
	}
	// Each kind of reason has its byte.
	for _, tc := range []struct {
		r    lockstep.StopReason
		kind StopKind
	}{
		{lockstep.ParentStopping, StopParentStopping},
		{lockstep.Shutdown, StopShutdown},
		{lockstep.NoActor, StopNoActor},
		{lockstep.Restarting(errors.New("x")), StopFailed},
		{lockstep.StopReason{Kind: lockstep.StopRestarting}, StopNormal},
	} {
		if k := stopKindOf(tc.r); k != tc.kind {
			t.Errorf("stopKindOf(%v) = %d, want %d", tc.r, k, tc.kind)
		}
	}
}

// TestRemoteWatchNotAlive checks REM-280's not-alive rule: TERMINATED no-actor at once, and no
// registration to cancel later.
func TestRemoteWatchNotAlive(t *testing.T) {
	h := newWatchHarness(t)
	h.watch(false, bX, engine.Address{Node: "a", Path: "/user/gone"}, 2)
	h.watch(false, bX, engine.Address{Node: "a", Path: "/user/y", UID: 99}, 3) // an earlier incarnation
	same(t, "TERMINATED frames", terminated(t, h.outLink()), []string{
		"lockstep://a/user/gone#0 -> lockstep://b/user/x#1 #2 4",
		"lockstep://a/user/y#63 -> lockstep://b/user/x#1 #3 4",
	})
	h.watch(true, bX, engine.Address{Node: "a", Path: "/user/gone"}, 2)
	h.sys.takeCalls()
	same(t, "calls after UNWATCH", h.sys.takeCalls(), nil)
	if n := len(h.core.peers["b"].watchers); n != 0 {
		t.Errorf("the core holds %d watches of b", n)
	}
}

// TestRemoteWatchUnwatch checks REM-280's UNWATCH: it cancels the registration of its watch
// number, outside the lock; one of another number changes nothing; after the cancel the actor's
// end sends nothing.
func TestRemoteWatchUnwatch(t *testing.T) {
	h := newWatchHarness(t)
	h.watch(false, bX, aY, 1)
	h.watch(true, bX, aY, 2) // an earlier watch's number
	h.watch(true, bW, aY, 1) // another watcher
	same(t, "hook calls", h.sys.takeCalls(), []string{"WatchFor lockstep://b/user/x#1 -> lockstep://a/user/y#2"})
	h.watch(true, bX, aY, 1)
	same(t, "hook calls", h.sys.takeCalls(), []string{"cancel lockstep://b/user/x#1 -> lockstep://a/user/y#2"})
	h.sys.stop("/user/y", lockstep.Normal)
	if len(h.drv.links) != 0 {
		t.Error("the end of an unwatched actor sent something")
	}
}

// TestRemoteWatchReplaced checks that a second WATCH of the same pair replaces the first: the
// actor's end answers the newer number only.
func TestRemoteWatchReplaced(t *testing.T) {
	h := newWatchHarness(t)
	h.watch(false, bX, aY, 1)
	h.watch(false, bX, aY, 2)
	h.watch(true, bX, aY, 1) // the replaced watch: nothing to cancel
	same(t, "hook calls", h.sys.takeCalls(), []string{
		"WatchFor lockstep://b/user/x#1 -> lockstep://a/user/y#2",
		"WatchFor lockstep://b/user/x#1 -> lockstep://a/user/y#2",
	})
	h.sys.stop("/user/y", lockstep.Normal)
	same(t, "TERMINATED frames", terminated(t, h.outLink()), []string{"lockstep://a/user/y#2 -> lockstep://b/user/x#1 #2 0"})
}

// TestRemoteWatchEndsWhileRegistering checks the race of a stop with the registration: the
// actor ends before hooks.WatchFor returns. The watcher gets one TERMINATED with the reason, and
// the registration that comes back is cancelled.
func TestRemoteWatchEndsWhileRegistering(t *testing.T) {
	h := newWatchHarness(t)
	r := lockstep.ParentStopping
	h.sys.endNow = &r
	h.watch(false, bX, aY, 6)
	same(t, "hook calls", h.sys.takeCalls(), []string{
		"WatchFor lockstep://b/user/x#1 -> lockstep://a/user/y#2",
		"cancel lockstep://b/user/x#1 -> lockstep://a/user/y#2",
	})
	same(t, "TERMINATED frames", terminated(t, h.outLink()), []string{"lockstep://a/user/y#2 -> lockstep://b/user/x#1 #6 2"})
	if n := len(h.core.peers["b"].watchers); n != 0 {
		t.Errorf("the core holds %d watches of b", n)
	}
}

// TestRemoteWatchUnwatchWhileRegistering checks an UNWATCH that the core applies before
// hooks.WatchFor returns: the registration is cancelled when it returns.
func TestRemoteWatchUnwatchWhileRegistering(t *testing.T) {
	h := newWatchHarness(t)
	p := h.core.peer("b")
	pair := watchPair{watcher: bX, watched: aY}
	h.core.mu.Lock()
	h.core.receiveWatch(p, pair, 1)
	h.core.receiveUnwatch(p, pair, 1)
	h.core.unlock()
	same(t, "hook calls", h.sys.takeCalls(), []string{
		"WatchFor lockstep://b/user/x#1 -> lockstep://a/user/y#2",
		"cancel lockstep://b/user/x#1 -> lockstep://a/user/y#2",
	})
	if got := h.sys.live("/user/y"); got != nil {
		t.Errorf("live registrations = %q, want none", got)
	}
}

// TestRemoteWatchMisrouted checks the address rules of WATCH: the watched actor must be on this
// node and the watcher on the peer's; a sender that is not an address is a decode discard.
func TestRemoteWatchMisrouted(t *testing.T) {
	h := newWatchHarness(t)
	h.watch(false, bX, engine.Address{Node: "c", Path: "/user/y"}, 1)
	h.watch(false, engine.Address{Node: "c", Path: "/user/x", UID: 1}, aY, 1)
	h.watch(false, engine.Address{Node: "a", Path: "/user/x", UID: 1}, aY, 1)
	h.peerFrame(KindWatch, "garbage", addrText(aY), "", AppendWatch(nil, 1))
	h.peerFrame(KindTerminated, "lockstep://c/user/y#2", addrText(engine.Address{Node: "a", Path: "/user/x", UID: 1}), "",
		AppendTerminated(nil, &Terminated{Watch: 1}))
	h.peerFrame(KindLookup, "lockstep://c/temp/1#5", "lockstep://a/user/y#0", "", nil)
	same(t, "records", h.recs(), []string{
		"discard peer=b conn=3 kind=WATCH seq=1 reason=misrouted",
		"discard peer=b conn=3 kind=WATCH seq=2 reason=misrouted",
		"discard peer=b conn=3 kind=WATCH seq=3 reason=misrouted",
		"discard peer=b conn=3 kind=WATCH seq=4 reason=decode",
		"discard peer=b conn=3 kind=TERMINATED seq=5 reason=misrouted",
		"discard peer=b conn=3 kind=LOOKUP seq=6 reason=misrouted",
	})
	same(t, "hook calls", h.sys.takeCalls(), nil)
	ev := h.takeEvents()
	if len(ev) != 1 || !strings.HasPrefix(ev[0], "fail lockstep: remote: a: WATCH from b: sender \"garbage\" is not an address") {
		t.Errorf("events = %q, want one failure of the WATCH's sender", ev)
	}
}

// TestRemoteWatchOutgoing checks the outgoing side of REM-280: RemoteWatch and RemoteUnwatch
// envelopes become WATCH and UNWATCH frames carrying the watch number, and the core keeps the
// outgoing watches they start, which count as work waiting for the peer (REM-100).
func TestRemoteWatchOutgoing(t *testing.T) {
	h := newSendHarness(t, nil)
	watchEnv := func(k hooks.RemoteKind, from, to engine.Address, n uint64) engine.Envelope {
		return engine.Envelope{From: from, To: to, Payload: hooks.RemotePayload{Kind: k, Seq: n}}
	}
	h.deliver(watchEnv(hooks.RemoteWatch, sender, target, 5))
	h.deliver(watchEnv(hooks.RemoteWatch, aY, target, 6))
	h.deliver(watchEnv(hooks.RemoteUnwatch, sender, target, 4)) // another number: the watch stays
	h.deliver(watchEnv(hooks.RemoteUnwatch, aY, target, 6))
	if err := h.core.Deliver(watchEnv(hooks.RemoteWatch, sender, target, 0)); err == nil {
		t.Error("Deliver of a RemoteWatch numbered 0 returned nil")
	}
	if err := h.core.Deliver(watchEnv(hooks.RemoteTerminated, target, sender, 1)); err == nil {
		t.Error("Deliver of a RemoteTerminated without a reason returned nil")
	}
	tEnv := watchEnv(hooks.RemoteTerminated, aY, bX, 0)
	tEnv.Payload = hooks.RemotePayload{Kind: hooks.RemoteTerminated, Reason: lockstep.Normal}
	if err := h.core.Deliver(tEnv); err == nil {
		t.Error("Deliver of a RemoteTerminated numbered 0 returned nil")
	}
	tEnv.Payload = hooks.RemotePayload{Kind: hooks.RemoteTerminated, Reason: lockstep.NoActor, Seq: 3}
	h.deliver(tEnv)
	l := h.up()
	var got []string
	for _, f := range l.frames {
		switch f.Kind {
		case KindWatch, KindUnwatch:
			n, _ := ParseWatch(f.Payload)
			got = append(got, fmt.Sprintf("%s %s -> %s #%d", f.Kind, f.Sender, f.Recipient, n))
		case KindTerminated:
			got = append(got, terminated(t, &fakeLink{frames: []Frame{f}})...)
		}
	}
	same(t, "frames", got, []string{
		"WATCH lockstep://a/user/x#1 -> lockstep://b/user/y#2 #5",
		"WATCH lockstep://a/user/y#2 -> lockstep://b/user/y#2 #6",
		"UNWATCH lockstep://a/user/x#1 -> lockstep://b/user/y#2 #4",
		"UNWATCH lockstep://a/user/y#2 -> lockstep://b/user/y#2 #6",
		"lockstep://a/user/y#2 -> lockstep://b/user/x#1 #3 4",
	})
	p := h.core.peers["b"]
	if want := map[watchPair]uint64{{watcher: sender, watched: target}: 5}; !reflect.DeepEqual(p.watches, want) {
		t.Errorf("outgoing watches = %v, want %v", p.watches, want)
	}

	// The session goes down with nothing queued: the outgoing watch alone makes the core dial
	// again, and retry a dial that fails.
	if p.want || len(p.queue) != 0 {
		t.Fatalf("want %v, %d frames queued; want neither", p.want, len(p.queue))
	}
	h.core.Closed(l, nil)
	if n := len(h.drv.links); n != 2 {
		t.Fatalf("%d dials, want 2: the outgoing watch waits for b", n)
	}
	h.core.Closed(h.link(), errors.New("refused"))
	if p.redial == nil {
		t.Error("a failed dial is not retried while an outgoing watch waits")
	}
}

// TestRemoteWatchUnknownNode checks that an outgoing watch of an actor on a node the driver does
// not know keeps the peer: the WATCH is a dead letter "unknown-node", and the core redials with
// backoff, so the watch is sent once the node can be dialed.
func TestRemoteWatchUnknownNode(t *testing.T) {
	h := newSendHarness(t, nil)
	h.drv.errs["b"] = fmt.Errorf("no address: %w", ErrUnknownNode)
	h.deliver(engine.Envelope{From: sender, To: target, Payload: hooks.RemotePayload{Kind: hooks.RemoteWatch, Seq: 1}})
	same(t, "dead letters", h.takeDead(), []string{"unknown-node lockstep://a/user/x#1 -> lockstep://b/user/y#2 <nil>"})
	if recs := h.recs(); len(recs) != 1 || !strings.HasPrefix(recs[0], "redial peer=b failures=1 delay=") {
		t.Errorf("records = %q, want one redial of b", recs)
	}
	delete(h.drv.errs, "b")
	h.eng.Advance(h.core.set.RedialMax)
	l := h.up()
	if got := data(l); !reflect.DeepEqual(got, []string{"WATCH seq=1"}) {
		t.Errorf("frames after the redial = %q, want [WATCH seq=1]", got)
	}
}

// TestTerminatedFrameReason checks REM-282: a TERMINATED becomes a RemoteTerminated for the host
// with the decoded reason and the echoed watch number; a failure's error is Errors.Remote's. It
// ends the outgoing watch only when the number is the watch's.
func TestTerminatedFrameReason(t *testing.T) {
	h := newWatchHarness(t)
	watched := engine.Address{Node: "b", Path: "/user/v", UID: 4}
	h.deliver(engine.Envelope{From: aY, To: watched, Payload: hooks.RemotePayload{Kind: hooks.RemoteWatch, Seq: 5}})
	pair := watchPair{watcher: aY, watched: watched}
	term := func(n uint64, kind StopKind, fl Failure) {
		h.peerFrame(KindTerminated, addrText(watched), addrText(aY), "", AppendTerminated(nil, &Terminated{Watch: n, Kind: kind, Failure: fl}))
	}
	term(4, StopNormal, Failure{})
	if n, ok := h.core.peers["b"].watches[pair]; !ok || n != 5 {
		t.Errorf("a TERMINATED of another number ended the watch: %v", h.core.peers["b"].watches)
	}
	term(5, StopFailed, Failure{Code: 3, Message: "boom"})
	if len(h.core.peers["b"].watches) != 0 {
		t.Errorf("the TERMINATED of the watch's number left it: %v", h.core.peers["b"].watches)
	}
	term(6, StopParentStopping, Failure{})
	term(7, StopShutdown, Failure{})
	term(8, StopNoActor, Failure{})
	var got []string
	for _, env := range h.ev.envs {
		p, _ := env.Payload.(hooks.RemotePayload)
		r, _ := p.Reason.(lockstep.StopReason)
		got = append(got, fmt.Sprintf("%d %s -> %s #%d %s", env.Seq, addrText(env.From), addrText(env.To), p.Seq, r))
		if p.Kind != hooks.RemoteTerminated {
			t.Errorf("kind %d, want RemoteTerminated", p.Kind)
		}
	}
	same(t, "host", got, []string{
		"1 lockstep://b/user/v#4 -> lockstep://a/user/y#2 #4 normal",
		"2 lockstep://b/user/v#4 -> lockstep://a/user/y#2 #5 failed: remote: b: boom (code 3)",
		"3 lockstep://b/user/v#4 -> lockstep://a/user/y#2 #6 parent-stopping",
		"4 lockstep://b/user/v#4 -> lockstep://a/user/y#2 #7 shutdown",
		"5 lockstep://b/user/v#4 -> lockstep://a/user/y#2 #8 no-actor",
	})
}

// TestTerminatedFrameRuntime runs a TERMINATED into package lockstep's actors: the watcher gets
// Terminated once, with the reason, for the watch it echoes; a second one is skipped.
func TestTerminatedFrameRuntime(t *testing.T) {
	h := newRuntimeHarness(t)
	var got []lockstep.Terminated
	watched := lockstep.PIDAt[ping]("b", "/user/v").Address()
	pid, err := lockstep.Spawn(h.sys, "w", func() lockstep.Actor[ping] {
		return watcherActor{watch: watched, got: &got}
	})
	if err != nil {
		t.Fatal(err)
	}
	h.eng.RunUntilIdle()
	l := h.drv.links[0]
	h.core.Frame(l, welcome("b", "a", 7, "json"))
	f := lastData(t, l)
	n, _ := ParseWatch(f.Payload)
	if f.Kind != KindWatch || n == 0 {
		t.Fatalf("sent %s #%d, want a WATCH", f.Kind, n)
	}
	from, to := "lockstep://b/user/v#0", addrText(engine.Address{Node: "a", Path: pid.Address().Path(), UID: pid.Address().UID()})
	h.send(KindTerminated, from, to, "", AppendTerminated(nil, &Terminated{Watch: n, Kind: StopFailed, Failure: Failure{Code: 2, Message: "gone"}}))
	h.send(KindTerminated, from, to, "", AppendTerminated(nil, &Terminated{Watch: n, Kind: StopNormal}))
	if len(got) != 1 || got[0].Reason.Kind != lockstep.StopFailed || got[0].Reason.Err.Error() != "remote: b: gone (code 2)" {
		t.Errorf("Terminated signals = %+v, want one failed with remote: b: gone (code 2)", got)
	}
	if len(h.core.peers["b"].watches) != 0 {
		t.Errorf("the outgoing watch is still held: %v", h.core.peers["b"].watches)
	}
}

// watcherActor watches watch when it starts and keeps the Terminated signals it gets.
type watcherActor struct {
	watch lockstep.Address
	got   *[]lockstep.Terminated
}

func (a watcherActor) Started(ctx *lockstep.Context[ping]) {
	if err := lockstep.Watch(ctx, a.watch); err != nil {
		panic(err)
	}
}

func (a watcherActor) Receive(*lockstep.Context[ping], ping) {}

func (a watcherActor) Signal(_ *lockstep.Context[ping], s lockstep.Signal) {
	if t, ok := s.(lockstep.Terminated); ok {
		*a.got = append(*a.got, t)
	}
}

// TestWatchesResent checks REM-284: an outgoing session that comes up to the same incarnation
// writes a WATCH for every outgoing watch, sorted by watched then watcher, before the queued
// frames; a watch whose WATCH is queued is not written twice, and an ended one not at all.
func TestWatchesResent(t *testing.T) {
	h := newSendHarness(t, nil)
	bV := engine.Address{Node: "b", Path: "/user/v", UID: 4}
	env := func(k hooks.RemoteKind, from, to engine.Address, n uint64) engine.Envelope {
		return engine.Envelope{From: from, To: to, Payload: hooks.RemotePayload{Kind: k, Seq: n}}
	}
	h.deliver(env(hooks.RemoteWatch, sender, target, 1)) // a/user/x watches b/user/y
	h.deliver(env(hooks.RemoteWatch, aY, target, 2))     // a/user/y watches b/user/y
	h.deliver(env(hooks.RemoteWatch, sender, bV, 3))     // a/user/x watches b/user/v
	l := h.up()
	if got := data(l); !reflect.DeepEqual(got, []string{"WATCH seq=1", "WATCH seq=2", "WATCH seq=3"}) {
		t.Fatalf("first session = %q: queued WATCH frames are written once, in order", got)
	}
	h.core.Closed(l, nil) // the core dials again at once: watches wait
	h.deliver(msgEnv(ping{1}))
	h.deliver(env(hooks.RemoteWatch, aZ, target, 4)) // queued: not written twice
	h.deliver(env(hooks.RemoteUnwatch, aY, target, 2))
	l2 := h.up()
	var got []string
	for _, f := range l2.frames {
		if f.Kind.Data() {
			n, _ := ParseWatch(f.Payload)
			got = append(got, fmt.Sprintf("%d %s %s -> %s #%d", f.Seq, f.Kind, f.Sender, f.Recipient, n))
		}
	}
	same(t, "second session", got, []string{
		"1 WATCH lockstep://a/user/x#1 -> lockstep://b/user/v#4 #3",
		"2 WATCH lockstep://a/user/x#1 -> lockstep://b/user/y#2 #1",
		"3 MSG lockstep://a/user/x#1 -> lockstep://b/user/y#2 #0",
		"4 WATCH lockstep://a/user/z#3 -> lockstep://b/user/y#2 #4",
		"5 UNWATCH lockstep://a/user/y#2 -> lockstep://b/user/y#2 #2",
	})
}

// TestRemoteWatchLongFailure checks that a failure's message is cut, at a rune boundary, so that
// the TERMINATED fits the smallest frame limit any peer announces.
func TestRemoteWatchLongFailure(t *testing.T) {
	h := newWatchHarness(t)
	h.watch(false, bX, aY, 1)
	long := strings.Repeat("é", MinFrameLimit) // two bytes a rune
	h.sys.stop("/user/y", lockstep.Failed(errors.New(long)))
	f := lastData(t, h.outLink())
	tm, err := ParseTerminated(f.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(frame(f)) - prefixLen; n > MinFrameLimit || n < MinFrameLimit-1 {
		t.Errorf("TERMINATED of %d bytes, want %d or one less", n, MinFrameLimit)
	}
	if !strings.HasPrefix(long, tm.Failure.Message) || len(tm.Failure.Message)%2 != 0 {
		t.Errorf("message of %d bytes is not a rune-aligned prefix", len(tm.Failure.Message))
	}
}

// TestRemoteWatchAfterShutdown checks that Shutdown cancels the watches of other nodes' actors,
// and that a watch that ends afterwards sends nothing.
func TestRemoteWatchAfterShutdown(t *testing.T) {
	h := newWatchHarness(t)
	h.watch(false, bX, aY, 1)
	h.sys.takeCalls()
	h.core.Shutdown()
	same(t, "calls", h.sys.takeCalls(), []string{"cancel lockstep://b/user/x#1 -> lockstep://a/user/y#2"})
	h.sys.stop("/user/y", lockstep.Normal)
	pair := watchPair{watcher: bX, watched: aY}
	h.core.watchEnded(h.core.peer("b"), pair, &inWatch{n: 2}, lockstep.Normal)
	if len(h.drv.links) != 0 {
		t.Error("a TERMINATED was sent after Shutdown")
	}
}
