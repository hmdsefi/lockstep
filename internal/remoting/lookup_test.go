// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// TestLookupSend checks the LOOKUP frame of Core.Lookup: the reply address as sender, the path
// address with uid 0 as recipient, the type key as type; and the calls it refuses.
func TestLookupSend(t *testing.T) {
	h := newSendHarness(t, func(c *Config) { c.Settings.MaxFrame = MinFrameLimit })
	if err := h.core.Lookup(askAddr, "b", "/user/y", "example.com/m.Ping"); err != nil {
		t.Fatal(err)
	}
	if err := h.core.Lookup(askAddr2, "b", "/user/v", ""); err != nil {
		t.Fatal(err)
	}
	l := h.up()
	var got []string
	for _, f := range l.frames {
		if f.Kind == KindLookup {
			got = append(got, fmt.Sprintf("%d %s -> %s type=%q", f.Seq, f.Sender, f.Recipient, f.Type))
		}
	}
	same(t, "LOOKUP frames", got, []string{
		`1 lockstep://a/temp/1#3 -> lockstep://b/user/y#0 type="example.com/m.Ping"`,
		`2 lockstep://a/temp/2#4 -> lockstep://b/user/v#0 type=""`,
	})
	same(t, "records", h.recs()[2:], []string{
		"send kind=LOOKUP seq=1 from=lockstep://a/temp/1#3 to=lockstep://b/user/y#0 type=example.com/m.Ping bytes=80",
		"send kind=LOOKUP seq=2 from=lockstep://a/temp/2#4 to=lockstep://b/user/v#0 type= bytes=62",
	})
	if n := len(h.core.peers["b"].lookups); n != 2 {
		t.Errorf("%d lookups pending, want 2", n)
	}
	for _, tc := range []struct {
		reply      engine.Address
		node, path string
		typ        string
		want       string
	}{
		{askAddr, "a", "/user/y", "", `remoting: cannot look up on node "a"`},
		{askAddr, "B!", "/user/y", "", `remoting: cannot look up on node "B!"`},
		{bAsk, "b", "/user/y", "", `remoting: lookup reply address lockstep://b/temp/9#5 is not an ask address of node "a"`},
		{engine.Address{Node: "a", Path: "/temp/1"}, "b", "/user/y", "", `remoting: lookup reply address lockstep://a/temp/1#0 is not an ask address of node "a"`},
		{askAddr, "b", "/user/y", strings.Repeat("t", MinFrameLimit), "remote: frame too large: 4155 bytes > max 4096"},
	} {
		if err := h.core.Lookup(tc.reply, tc.node, tc.path, tc.typ); err == nil || err.Error() != tc.want {
			t.Errorf("Lookup(%s, %s, %s) = %v, want %s", tc.reply, tc.node, tc.path, err, tc.want)
		}
	}
	if n := len(h.core.peers["b"].lookups); n != 2 {
		t.Errorf("refused lookups were kept: %d pending, want 2", n)
	}
}

// TestLookupAnswer checks REM-290 on the answering side: a LOOKUP is answered with a REPLY of
// lockstep.address carrying the actor's address with its uid, from the actor, or with a FAIL from
// the looked-up path, of code not-found or wrong-type. hooks.LookupPath runs without the lock.
func TestLookupAnswer(t *testing.T) {
	h := newWatchHarness(t)
	lookupCalled := 0
	real := hooks.LookupPath
	hooks.LookupPath = func(sys any, path, typeKey string) (engine.Address, error) {
		lookupCalled++
		h.sys.call("LookupPath " + path)
		return real(sys, path, typeKey)
	}
	t.Cleanup(func() { hooks.LookupPath = real })
	pingKey := keyOf(reflect.TypeFor[ping]())
	h.peerFrame(KindLookup, addrText(bAsk), "lockstep://a/user/y#0", "", nil)
	h.peerFrame(KindLookup, addrText(bAsk), "lockstep://a/user/y#0", pingKey, nil)
	h.peerFrame(KindLookup, addrText(bAsk), "lockstep://a/user/y#0", "example.com/m.Other", nil)
	h.peerFrame(KindLookup, addrText(bAsk), "lockstep://a/user/gone#0", "", nil)
	if lookupCalled != 4 {
		t.Errorf("LookupPath called %d times, want 4", lookupCalled)
	}
	l := h.outLink()
	var got []string
	for _, f := range l.frames {
		switch f.Kind {
		case KindReply:
			v, err := codec.Default().Decode(codec.JSON{}, f.Type, f.Payload)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("REPLY %s -> %s %s %v", f.Sender, f.Recipient, f.Type, v))
		case KindFail:
			fl, _ := ParseFailure(f.Payload)
			got = append(got, fmt.Sprintf("FAIL %s -> %s %d %s", f.Sender, f.Recipient, fl.Code, fl.Message))
		}
	}
	same(t, "answers", got, []string{
		"REPLY lockstep://a/user/y#2 -> lockstep://b/temp/9#5 lockstep.address lockstep://a/user/y#2",
		"REPLY lockstep://a/user/y#2 -> lockstep://b/temp/9#5 lockstep.address lockstep://a/user/y#2",
		"FAIL lockstep://a/user/y#0 -> lockstep://b/temp/9#5 4 lockstep: wrong message type: /user/y is not example.com/m.Other",
		`FAIL lockstep://a/user/gone#0 -> lockstep://b/temp/9#5 3 lockstep: not found: path "/user/gone"`,
	})
}

// TestLookupAnswerEncodeFailure checks that an address that does not encode, because the registry
// lacks lockstep.address's codec, is answered with FAIL code other.
func TestLookupAnswerEncodeFailure(t *testing.T) {
	h := newWatchHarness(t)
	h.core.set.Codec = failCodec{codec.JSON{}}
	h.peerFrame(KindLookup, addrText(bAsk), "lockstep://a/user/y#0", "", nil)
	f := lastData(t, h.outLink())
	fl, _ := ParseFailure(f.Payload)
	if f.Kind != KindFail || fl.Code != codeOther || !strings.Contains(fl.Message, "no marshal") {
		t.Errorf("answer %s %+v, want FAIL code other with the encode error", f.Kind, fl)
	}
}

// failCodec is a JSON codec whose Marshal fails.
type failCodec struct{ codec.JSON }

func (failCodec) Marshal(any) ([]byte, error) { return nil, errors.New("no marshal") }

// TestLookupPending checks REM-290's pending lookups: an answer, REPLY or FAIL, forgets its lookup;
// a heartbeat tick forgets the lookups whose asks are no longer pending (hooks.Pending, called
// without the lock); a LOOKUP that becomes a dead letter fails its ask with Errors.Unreachable.
func TestLookupPending(t *testing.T) {
	h := newWatchHarness(t)
	gone := engine.Address{Node: "a", Path: "/temp/7", UID: 8} // not a pending ask of fakeSys
	for _, a := range []engine.Address{askAddr, askAddr2, gone} {
		if err := h.core.Lookup(a, "b", "/user/v", ""); err != nil {
			t.Fatal(err)
		}
	}
	h.outLink()
	asks := func() []string {
		var out []string
		for _, l := range h.core.peers["b"].lookups {
			out = append(out, addrText(l.ask))
		}
		return out
	}
	h.peerFrame(KindFail, "lockstep://b/user/v#0", addrText(askAddr2), "", AppendFailure(nil, &Failure{Code: 3, Message: "not found"}))
	same(t, "after FAIL", asks(), []string{addrText(askAddr), addrText(gone)})
	// The tick's calls of hooks.Pending, which run without the lock, are logged.
	tick := func() {
		real := hooks.Pending
		hooks.Pending = func(sys any, ask engine.Address) bool {
			h.sys.call("Pending " + addrText(ask))
			return real(sys, ask)
		}
		defer func() { hooks.Pending = real }()
		h.eng.Advance(h.core.set.Heartbeat)
	}
	h.sys.takeCalls()
	tick()
	same(t, "after the tick", asks(), []string{addrText(askAddr)})
	same(t, "Pending calls", h.sys.takeCalls(), []string{"Pending " + addrText(askAddr), "Pending " + addrText(gone)})
	h.peerFrame(KindReply, "lockstep://b/user/v#4", addrText(askAddr), "test.ping", jsonOf(t, ping{1}))
	same(t, "after REPLY", asks(), nil)
	tick()
	same(t, "no lookups, no Pending calls", h.sys.takeCalls(), nil)

	// A LOOKUP for a node the driver does not know is a dead letter; its ask fails.
	h.drv.errs["c"] = ErrUnknownNode
	if err := h.core.Lookup(askAddr, "c", "/user/v", ""); err != nil {
		t.Fatal(err)
	}
	same(t, "dead letters", h.takeDead(), []string{"unknown-node lockstep://a/temp/1#3 -> lockstep://c/user/v#0 /user/v"})
	if p := h.core.peers["c"]; p != nil && len(p.lookups) != 0 {
		t.Errorf("the dead-lettered lookup is still pending: %v", p.lookups)
	}
	h.takeEvents()
	h.eng.Advance(0)
	ev := h.takeEvents()
	if len(ev) != 1 || !strings.Contains(ev[0], "FAIL lockstep://c/user/v#0 -> lockstep://a/temp/1#3 remote: node unreachable: c") {
		t.Errorf("events = %q, want the lookup failed unreachable", ev)
	}
}

// TestLookupRuntime runs a lookup between two cores of package lockstep's systems over a loopback:
// a NewReply ask on node a resolves with the address of the actor that node b finds, and with a
// failure wrapping lockstep.ErrNotFound for a path where none is.
func TestLookupRuntime(t *testing.T) {
	rh := newRuntimeHarness(t)
	h, sys := rh.harness, rh.sys
	pid, err := lockstep.Spawn(sys, "y", func() lockstep.Actor[ping] { return pingActor{new([]ping)} })
	if err != nil {
		t.Fatal(err)
	}
	h.eng.RunUntilIdle()
	// b looks up a's actor: the REPLY carries its address.
	rh.send(KindLookup, addrText(bAsk), "lockstep://a/user/y#0", keyOf(reflect.TypeFor[ping]()), nil)
	l := h.drv.links[0]
	h.core.Frame(l, welcome("b", "a", 7, "json"))
	f := lastData(t, l)
	v, err := codec.Default().Decode(codec.JSON{}, f.Type, f.Payload)
	if err != nil || f.Kind != KindReply || v != pid.Address() {
		t.Errorf("answer %s %v (%v), want REPLY %s", f.Kind, v, err, pid.Address())
	}

	// a looks up an actor of b: the answer resolves a NewReply ask.
	_, r := lockstep.NewReply[lockstep.Address](sys, lockstep.WithTimeout(time.Hour))
	ask := hooks.PendingAsks(sys)[0].Ask
	if err := h.core.Lookup(ask, "b", "/user/v", ""); err != nil {
		t.Fatal(err)
	}
	if f := lastData(t, l); f.Kind != KindLookup || f.Sender != addrText(ask) {
		t.Fatalf("sent %s from %s, want LOOKUP from %s", f.Kind, f.Sender, addrText(ask))
	}
	found := lockstep.PIDAt[ping]("b", "/user/v").Address()
	var a lockstep.Address
	_ = a.UnmarshalText([]byte("lockstep://b/user/v#2a"))
	name, data, err := codec.Default().Encode(codec.JSON{}, a)
	if err != nil {
		t.Fatal(err)
	}
	rh.send(KindReply, "lockstep://b/user/v#2a", addrText(ask), name, data)
	if got, err := r.Await(context.Background()); err != nil || got != a || got.Path() != found.Path() {
		t.Errorf("Await = %v, %v; want %s", got, err, a)
	}
	if n := len(h.core.peers["b"].lookups); n != 0 {
		t.Errorf("%d lookups still pending after the answer", n)
	}

	_, r2 := lockstep.NewReply[lockstep.Address](sys, lockstep.WithTimeout(time.Hour))
	ask2 := hooks.PendingAsks(sys)[0].Ask
	if err := h.core.Lookup(ask2, "b", "/user/none", ""); err != nil {
		t.Fatal(err)
	}
	rh.send(KindFail, "lockstep://b/user/none#0", addrText(ask2), "", AppendFailure(nil, &Failure{Code: 3, Message: "not found"}))
	if _, err := r2.Await(context.Background()); err == nil || err.Error() != "remote: b: not found (code 3)" {
		t.Errorf("Await = %v, want the FAIL's error", err)
	}
}
