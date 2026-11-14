// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// refuser answers every Echo with a failure.
type refuser struct{}

func (refuser) Receive(ctx *lockstep.Context[Echo], m Echo) { m.Reply.Fail(ctx, errors.New("refused")) }

// TestATREM40GoldenFrames checks every frame kind's layout from golden bytes over the wire: a raw
// peer r and a system a exchange each kind, in every direction the kind table allows, and every
// frame either side writes is written out field by field (wire). The frames a writes are compared
// byte for byte; the frames r writes are the golden bytes, and a's answer shows it read them as
// the layout says. The values that only a run knows (a's incarnation and port, actor uids, ask
// addresses) are filled into the golden layout.
func TestATREM40GoldenFrames(t *testing.T) {
	const (
		hello, welcome, reject = remoting.KindHello, remoting.KindWelcome, remoting.KindReject
		ping, pong, closeK     = remoting.KindPing, remoting.KindPong, remoting.KindClose
		msg, askK, reply, fail = remoting.KindMsg, remoting.KindAsk, remoting.KindReply, remoting.KindFail
		watch, unwatch, term   = remoting.KindWatch, remoting.KindUnwatch, remoting.KindTerminated
		lookup                 = remoting.KindLookup
		rS, rW, rEcho          = "lockstep://r/user/s#5", "lockstep://r/user/w#6", "lockstep://r/user/echo#3"
	)
	reg := newRegistry()
	ln := rawListener(t)
	a := startNode(t, "a", reg, func(c *remote.Config) {
		c.Peers = map[string]string{"r": ln.Addr().String()}
		c.Heartbeat, c.IdleTimeout = 50*time.Millisecond, 10*time.Second
	})
	inc := a.sys.Incarnation()
	notes := make(chan Note, 1)
	spawn[Note](t, a.sys, "notes", forward[Note]{notes})
	echoPID := spawn[Echo](t, a.sys, "echo", echo{tag: "a"})
	refPID := spawn[Echo](t, a.sys, "refuser", refuser{})
	h1 := spawn[Halt](t, a.sys, "h1", halter{})
	h2 := spawn[Halt](t, a.sys, "h2", halter{})
	terms := make(chan lockstep.Terminated, 4)
	d := spawn[do](t, a.sys, "d", doer{terms: terms})
	ctx, cancel := context.WithTimeout(context.Background(), 2*rawWait)
	defer cancel()

	// HELLO in, WELCOME out.
	in := dialRaw(t, a.addr)
	in.writeBytes(wire(control(hello), helloHex("r", "a", rawInc, 1<<20, ln.Addr().String())))
	in.expectBytes("WELCOME", wire(control(welcome), helloHex("a", "r", inc, remote.DefaultMaxFrame, a.addr)))

	// PING in, PONG out.
	in.writeBytes(wire(control(ping)))
	in.expectBytes("PONG", wire(control(pong)))

	// MSG in, with a header.
	in.writeBytes(wire("01 10 0000", h64(1), "01 01"+hs("k")+"01"+hs("v"),
		hstr(rS), hstr("lockstep://a/user/notes#0"), hstr("remotetest.Note"), hs(`{"N":7}`)))
	if got := recv(t, notes); got.N != 7 {
		t.Errorf("MSG in: received %+v", got)
	}

	// ASK in, and its REPLY out on the session a dials to r: HELLO out, WELCOME in.
	const ask1, ask2 = "lockstep://r/temp/1#8", "lockstep://r/temp/2#9"
	in.writeBytes(wire(dataHead(askK, 2, rS, "lockstep://a/user/echo#0", "remotetest.Echo"),
		hstr(ask1), hs(`{"Text":"hi","Reply":"lockstep-reply://r/temp/1#8"}`)))
	out := acceptRaw(t, ln)
	out.expectBytes("HELLO", wire(control(hello), helloHex("a", "r", inc, remote.DefaultMaxFrame, a.addr)))
	out.writeBytes(wire(control(welcome), helloHex("r", "a", rawInc, 1<<20, "")))
	out.expectBytes("REPLY", wire(dataHead(reply, 1, echoPID.String(), ask1, "lockstep.string"), hs(`"hi from a"`)))

	// ASK in, FAIL out: code other and the message.
	in.writeBytes(wire(dataHead(askK, 3, rS, "lockstep://a/user/refuser#0", "remotetest.Echo"),
		hstr(ask2), hs(`{"Text":"no","Reply":"lockstep-reply://r/temp/2#9"}`)))
	out.expectBytes("FAIL", wire(dataHead(fail, 2, refPID.String(), ask2, ""), "00", hs("refused")))

	// PING out from the idle dialer, PONG in.
	out.expectBytes("PING", wire(control(ping)))
	out.writeBytes(wire(control(pong)))

	// MSG out.
	runIn(t, a.sys, d, func(ctx *lockstep.Context[do]) {
		if err := lockstep.Send(ctx, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: 3}); err != nil {
			t.Error(err)
		}
	})
	out.expectBytes("MSG", wire(dataHead(msg, 3, d.String(), "lockstep://r/user/x#0", "remotetest.Note"), hs(`{"N":3,"Items":null}`)))

	// ASK out, REPLY in; ASK out, FAIL in.
	askOut := func(seq uint64, text string) (*lockstep.Reply[string], string) {
		t.Helper()
		var r *lockstep.Reply[string]
		runIn(t, a.sys, d, func(ctx *lockstep.Context[do]) {
			r = lockstep.Ask(ctx, lockstep.PIDAt[Echo]("r", "/user/echo"), func(rt lockstep.ReplyTo[string]) Echo {
				return Echo{Text: text, Reply: rt}
			}, lockstep.WithTimeout(rawWait))
		})
		b, f := out.expect(askK)
		addr, _, err := remoting.ParseAsk(f.Payload)
		if err != nil || !strings.HasPrefix(addr, "lockstep://a/temp/") {
			t.Fatalf("ASK out: ask address %q, %v", addr, err)
		}
		replyTo := "lockstep-reply://" + strings.TrimPrefix(addr, "lockstep://")
		sameBytes(t, "ASK", b, wire(dataHead(askK, seq, d.String(), "lockstep://r/user/echo#0", "remotetest.Echo"),
			hstr(addr), hs(`{"Text":"`+text+`","Reply":"`+replyTo+`"}`)))
		return r, addr
	}
	r1, addr1 := askOut(4, "q1")
	in.writeBytes(wire(dataHead(reply, 4, rEcho, addr1, "lockstep.string"), hs(`"ok"`)))
	if v, err := r1.Await(ctx); err != nil || v != "ok" {
		t.Errorf("REPLY in: %q, %v", v, err)
	}
	r2, addr2 := askOut(5, "q2")
	in.writeBytes(wire(dataHead(fail, 5, rEcho, addr2, ""), "03", hs("gone")))
	var re *remote.Error
	if _, err := r2.Await(ctx); !errors.As(err, &re) || *re != (remote.Error{Node: "r", Code: remote.CodeNotFound, Message: "gone"}) ||
		!errors.Is(err, lockstep.ErrNotFound) {
		t.Errorf("FAIL in: %v, want the remote error not-found \"gone\"", err)
	}

	// WATCH out, UNWATCH out, WATCH out, and TERMINATED in for the second watch: failed, code
	// wrong-type.
	x := lockstep.PIDAt[Note]("r", "/user/x").Address()
	runIn(t, a.sys, d, func(ctx *lockstep.Context[do]) {
		if err := lockstep.Watch(ctx, x); err != nil {
			t.Error(err)
		}
	})
	out.expectBytes("WATCH", wire(dataHead(watch, 6, d.String(), x.String(), ""), "01"))
	runIn(t, a.sys, d, func(ctx *lockstep.Context[do]) { lockstep.Unwatch(ctx, x) })
	out.expectBytes("UNWATCH", wire(dataHead(unwatch, 7, d.String(), x.String(), ""), "01"))
	runIn(t, a.sys, d, func(ctx *lockstep.Context[do]) {
		if err := lockstep.Watch(ctx, x); err != nil {
			t.Error(err)
		}
	})
	out.expectBytes("WATCH", wire(dataHead(watch, 8, d.String(), x.String(), ""), "02"))
	in.writeBytes(wire(dataHead(term, 6, x.String(), d.String(), ""), "02 01 04", hs("bad")))
	got := recv(t, terms)
	if got.Addr != x || got.Reason.Kind != lockstep.StopFailed || !errors.As(got.Reason.Err, &re) ||
		*re != (remote.Error{Node: "r", Code: remote.CodeWrongType, Message: "bad"}) {
		t.Errorf("TERMINATED in: %+v, want %v failed with the remote error wrong-type \"bad\"", got, x)
	}

	// WATCH in of an actor that is not alive, TERMINATED out: no-actor.
	in.writeBytes(wire(dataHead(watch, 7, rW, "lockstep://a/user/none#0", ""), "07"))
	out.expectBytes("TERMINATED", wire(dataHead(term, 9, "lockstep://a/user/none#0", rW, ""), "07 04"))

	// WATCH in of h1, UNWATCH in, WATCH in of h2: h1's stop then sends nothing, and h2's sends
	// TERMINATED normal. The PONG shows that a applied the three frames.
	in.writeBytes(wire(dataHead(watch, 8, rW, h1.String(), ""), "08"))
	in.writeBytes(wire(dataHead(unwatch, 9, rW, h1.String(), ""), "08"))
	in.writeBytes(wire(dataHead(watch, 10, rW, h2.String(), ""), "09"))
	in.writeBytes(wire(control(ping)))
	in.expect(pong)
	if err := lockstep.Send(a.sys, h1, Halt{}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(rawWait); ; time.Sleep(time.Millisecond) {
		if _, err := lockstep.Lookup[Halt](a.sys, "/user/h1"); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("h1 did not stop")
		}
	}
	if err := lockstep.Send(a.sys, h2, Halt{}); err != nil {
		t.Fatal(err)
	}
	out.expectBytes("TERMINATED", wire(dataHead(term, 10, h2.String(), rW, ""), "09 00"))

	// LOOKUP out, REPLY in.
	type found struct {
		pid lockstep.PID[Echo]
		err error
	}
	res := make(chan found, 1)
	go func() {
		pid, err := remote.Lookup[Echo](ctx, a.sys, "r", "/user/echo")
		res <- found{pid, err}
	}()
	b, f := out.expect(lookup)
	if !strings.HasPrefix(f.Sender, "lockstep://a/temp/") {
		t.Fatalf("LOOKUP out from %q, want an ask address of a", f.Sender)
	}
	echoType := reflect.TypeFor[Echo]()
	sameBytes(t, "LOOKUP", b, wire(dataHead(lookup, 11, f.Sender, "lockstep://r/user/echo#0", echoType.PkgPath()+"."+echoType.Name())))
	in.writeBytes(wire(dataHead(reply, 11, rEcho, f.Sender, "lockstep.address"), hs(`"`+rEcho+`"`)))
	if r := recv(t, res); r.err != nil || r.pid.String() != rEcho {
		t.Errorf("Lookup over REPLY in: %v, %v; want %s", r.pid, r.err, rEcho)
	}

	// LOOKUP in, REPLY out: the actor's address.
	const look = "lockstep://r/temp/3#a"
	in.writeBytes(wire(dataHead(lookup, 12, look, "lockstep://a/user/echo#0", "")))
	out.expectBytes("REPLY", wire(dataHead(reply, 12, echoPID.String(), look, "lockstep.address"), hs(`"`+echoPID.String()+`"`)))

	// REJECT out: a HELLO from a node that calls itself a.
	rej := dialRaw(t, a.addr)
	rej.writeBytes(wire(control(hello), helloHex("a", "a", rawInc, 1<<20, "")))
	rej.expectBytes("REJECT", wire(control(reject), hs("self")))
	rej.expectEOF()

	// CLOSE in, on each session: a closes both connections.
	in.writeBytes(wire(control(closeK), hs("shutdown")))
	in.expectEOF()
	out.writeBytes(wire(control(closeK), hs("shutdown")))
	out.expectEOF()

	// REJECT in: a dials again for its next message, is refused, and dials once more.
	runIn(t, a.sys, d, func(ctx *lockstep.Context[do]) {
		if err := lockstep.Send(ctx, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: 4}); err != nil {
			t.Error(err)
		}
	})
	refused := acceptRaw(t, ln)
	refused.expect(hello)
	refused.writeBytes(wire(control(reject), hs("codec")))
	refused.expectEOF()
	last := acceptRaw(t, ln)
	last.welcome("a")
	last.expectBytes("MSG", wire(dataHead(msg, 1, d.String(), "lockstep://r/user/x#0", "remotetest.Note"), hs(`{"N":4,"Items":null}`)))

	// CLOSE out: the shutdown.
	done := make(chan struct{})
	go func() {
		defer close(done)
		shutdown(t, a.sys)
	}()
	last.expectBytes("CLOSE", wire(control(closeK), hs("shutdown")))
	last.expectEOF()
	recv(t, done)
}
