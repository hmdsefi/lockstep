// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// TestATREM04RawPeerHandshake checks the handshake and a MSG frame byte for byte, in both
// directions, between a raw peer r and a system b: r's HELLO is answered with b's WELCOME and r's
// MSG reaches b's actor; b dials r with its HELLO and, after r's WELCOME, writes its MSG.
func TestATREM04RawPeerHandshake(t *testing.T) {
	reg := newRegistry()
	ln := rawListener(t)
	b := startNode(t, "b", reg, func(c *remote.Config) { c.Peers = map[string]string{"r": ln.Addr().String()} })
	notes := make(chan Note, 1)
	spawn[Note](t, b.sys, "notes", forward[Note]{notes})
	inc := b.sys.Incarnation()

	// r dials b.
	in := dialRaw(t, b.addr)
	in.writeBytes(wire(control(remoting.KindHello), helloHex("r", "b", rawInc, 1<<20, ln.Addr().String())))
	in.expectBytes("WELCOME", wire(control(remoting.KindWelcome), helloHex("b", "r", inc, remote.DefaultMaxFrame, b.addr)))
	in.writeBytes(wire(dataHead(remoting.KindMsg, 1, "lockstep://r/user/s#5", "lockstep://b/user/notes#0", "remotetest.Note"),
		hs(`{"N":7,"Items":[1,2]}`)))
	if got := recv(t, notes); got.N != 7 || !slices.Equal(got.Items, []int{1, 2}) {
		t.Errorf("b received %+v, want {N:7 Items:[1 2]}", got)
	}
	// The stream is cut into frames wherever the writes fall: a frame written one byte at a time,
	// then two frames in one write.
	noteFrame := func(seq uint64, n int) []byte {
		return wire(dataHead(remoting.KindMsg, seq, "lockstep://r/user/s#5", "lockstep://b/user/notes#0", "remotetest.Note"),
			hs(fmt.Sprintf(`{"N":%d}`, n)))
	}
	for _, c := range noteFrame(2, 8) {
		in.writeBytes([]byte{c})
	}
	in.writeBytes(append(noteFrame(3, 9), noteFrame(4, 10)...))
	for n := 8; n <= 10; n++ {
		if got := recv(t, notes); got.N != n {
			t.Errorf("b received %+v, want N %d", got, n)
		}
	}

	// b dials r.
	d := spawn[do](t, b.sys, "d", doer{})
	runIn(t, b.sys, d, func(ctx *lockstep.Context[do]) {
		if err := lockstep.Send(ctx, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: 1}); err != nil {
			t.Error(err)
		}
	})
	out := acceptRaw(t, ln)
	out.expectBytes("HELLO", wire(control(remoting.KindHello), helloHex("b", "r", inc, remote.DefaultMaxFrame, b.addr)))
	out.writeBytes(wire(control(remoting.KindWelcome), helloHex("r", "b", rawInc, 1<<20, "")))
	out.expectBytes("MSG", wire(dataHead(remoting.KindMsg, 1, d.String(), "lockstep://r/user/x#0", "remotetest.Note"),
		hs(`{"N":1,"Items":null}`)))
}

// goodHello is the HELLO of raw peer r to b; edit changes it.
func goodHello(edit func(*remoting.Hello)) []byte {
	h := remoting.Hello{Sender: "r", Receiver: "b", Incarnation: rawInc, FrameLimit: 1 << 20, Codec: "json"}
	if edit != nil {
		edit(&h)
	}
	return remoting.AppendFrame(nil, &remoting.Frame{Kind: remoting.KindHello, Payload: remoting.AppendHello(nil, &h)})
}

// withByte returns a copy of b with byte i set to v.
func withByte(b []byte, i int, v byte) []byte {
	b = slices.Clone(b)
	b[i] = v
	return b
}

// TestATREM05RejectReasons checks every REJECT reason of REM-070 from a raw peer: the first frame's
// version, kind and payload, then self, wrong-node and codec. Each REJECT is answered byte for
// byte and the connection then ends; b still accepts a good HELLO afterwards. On the dialer's side,
// a REJECT and a WELCOME from the wrong node (REM-075) end the session and b dials again.
func TestATREM05RejectReasons(t *testing.T) {
	reg := newRegistry()
	ln := rawListener(t)
	b := startNode(t, "b", reg, func(c *remote.Config) { c.Peers = map[string]string{"r": ln.Addr().String()} })
	notes := make(chan Note, 1)
	spawn[Note](t, b.sys, "notes", forward[Note]{notes})

	hello := goodHello(nil)
	for _, tc := range []struct {
		name   string
		first  []byte
		reason string
	}{
		{"version 2", withByte(hello, 4, 2), remoting.RejectVersion},
		{"version 0", withByte(hello, 4, 0), remoting.RejectVersion},
		{"PING first", wire(control(remoting.KindPing)), remoting.RejectKind},
		{"unknown kind", withByte(hello, 5, 99), remoting.RejectKind},
		{"MSG first", remoting.AppendFrame(nil, &remoting.Frame{Kind: remoting.KindMsg, Seq: 1}), remoting.RejectKind},
		{"incarnation 0", goodHello(func(h *remoting.Hello) { h.Incarnation = 0 }), remoting.RejectPayload},
		{"frame limit too small", goodHello(func(h *remoting.Hello) { h.FrameLimit = 4095 }), remoting.RejectPayload},
		{"empty codec", goodHello(func(h *remoting.Hello) { h.Codec = "" }), remoting.RejectPayload},
		{"invalid sender name", goodHello(func(h *remoting.Hello) { h.Sender = "R!" }), remoting.RejectPayload},
		{"trailing bytes", wire(control(remoting.KindHello), helloHex("r", "b", rawInc, 1<<20, ""), "00"), remoting.RejectPayload},
		{"HELLO with a sequence number", withByte(hello, 15, 1), remoting.RejectPayload},
		{"HELLO with flags", withByte(hello, 7, 1), remoting.RejectPayload},
		{"length below the minimum", hx("0000000f 01 01 0000 0000000000000000 000000"), remoting.RejectPayload},
		{"length above the frame limit", hx("7fffffff"), remoting.RejectPayload},
		{"self", goodHello(func(h *remoting.Hello) { h.Sender = "b" }), remoting.RejectSelf},
		{"wrong node", goodHello(func(h *remoting.Hello) { h.Receiver = "c" }), remoting.RejectWrongNode},
		{"codec", goodHello(func(h *remoting.Hello) { h.Codec = "gob" }), remoting.RejectCodec},
		// The order of the checks: a HELLO that breaks several rules gets the first reason.
		{"self before wrong node", goodHello(func(h *remoting.Hello) { h.Sender, h.Receiver = "b", "c" }), remoting.RejectSelf},
		{"wrong node before codec", goodHello(func(h *remoting.Hello) { h.Receiver, h.Codec = "c", "gob" }), remoting.RejectWrongNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := dialRaw(t, b.addr)
			r.writeBytes(tc.first)
			r.expectBytes("REJECT", wire(control(remoting.KindReject), hs(tc.reason)))
			r.expectEOF()
		})
	}

	t.Run("accepts afterwards", func(t *testing.T) {
		r := dialRaw(t, b.addr)
		r.handshake("r")
		r.send(data(remoting.KindMsg, "lockstep://b/user/notes#0", "remotetest.Note", `{"N":1}`))
		if got := recv(t, notes); got.N != 1 {
			t.Errorf("b received %+v after the rejections", got)
		}
	})

	t.Run("dialer", func(t *testing.T) {
		if err := lockstep.Send(b.sys, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: 2}); err != nil {
			t.Fatal(err)
		}
		// A REJECT ends the session; b dials again after its backoff.
		first := acceptRaw(t, ln)
		first.expect(remoting.KindHello)
		first.writeBytes(wire(control(remoting.KindReject), hs(remoting.RejectCodec)))
		first.expectEOF()
		// A WELCOME from another node than r ends the session as wrong-node (REM-075).
		second := acceptRaw(t, ln)
		second.expect(remoting.KindHello)
		second.write(*helloFrame(remoting.KindWelcome, "q", "b", ""))
		second.expectEOF()
		waitLogs(t, b, "remoting: welcome from the wrong node", map[string]string{"peer": "r", "sender": "q"}, 1)
		// The next session comes up and carries the message.
		third := acceptRaw(t, ln)
		third.welcome("b")
		_, f := third.expect(remoting.KindMsg)
		if f.Seq != 1 || string(f.Payload) != `{"N":2,"Items":null}` {
			t.Errorf("MSG seq %d %s, want seq 1 with the message", f.Seq, f.Payload)
		}
	})
}

// TestATREM06ReplacedIncoming checks REM-085: a second HELLO from a peer replaces its incoming
// session; the first reads CLOSE replaced and ends, and the second carries the peer's frames,
// numbered from 1 again.
func TestATREM06ReplacedIncoming(t *testing.T) {
	reg := newRegistry()
	b := startNode(t, "b", reg, nil)
	notes := make(chan Note, 4)
	spawn[Note](t, b.sys, "notes", forward[Note]{notes})
	note := func(n int) remoting.Frame {
		return data(remoting.KindMsg, "lockstep://b/user/notes#0", "remotetest.Note", fmt.Sprintf(`{"N":%d}`, n))
	}

	first := dialRaw(t, b.addr)
	first.handshake("r")
	first.send(note(1))
	first.send(note(2))
	for i := 1; i <= 2; i++ {
		if got := recv(t, notes); got.N != i {
			t.Fatalf("message %d is %d", i, got.N)
		}
	}

	second := dialRaw(t, b.addr)
	second.handshake("r")
	first.expectClose(remoting.CloseReplaced)
	second.send(note(3)) // sequence number 1 on the new session
	if got := recv(t, notes); got.N != 3 {
		t.Errorf("message on the new session is %d, want 3", got.N)
	}

	// A third HELLO, from another peer, replaces nothing.
	other := dialRaw(t, b.addr)
	other.handshake("q")
	second.send(note(4))
	if got := recv(t, notes); got.N != 4 {
		t.Errorf("message after q's HELLO is %d, want 4", got.N)
	}
}

// TestATREM07MalformedFrames checks REM-390: a frame that breaks the protocol on an up session is
// answered with CLOSE protocol, logged, and ends the connection, whichever side dialed; the system
// keeps running and serves the next session.
func TestATREM07MalformedFrames(t *testing.T) {
	reg := newRegistry()
	ln := rawListener(t)
	b := startNode(t, "b", reg, func(c *remote.Config) { c.Peers = map[string]string{"r": ln.Addr().String()} })
	notes := make(chan Note, 1)
	spawn[Note](t, b.sys, "notes", forward[Note]{notes})

	const sender, recipient = rawSender, "lockstep://b/user/notes#0"
	msg := func(edit func(*remoting.Frame)) []byte {
		f := data(remoting.KindMsg, recipient, "remotetest.Note", `{"N":1}`)
		f.Seq = 1
		if edit != nil {
			edit(&f)
		}
		return remoting.AppendFrame(nil, &f)
	}
	many := make([]engine.Header, remoting.MaxHeaders+1)
	for i := range many {
		many[i] = engine.Header{Key: fmt.Sprintf("k%d", i)}
	}
	longKey := strings.Repeat("k", remoting.MaxHeaderKey+1)
	longValue := strings.Repeat("v", remoting.MaxHeaderValue+1)
	// typed returns the frame of kind k with payload whose type field is not empty.
	typed := func(k remoting.Kind, payload string) []byte {
		return wire(dataHead(k, 1, sender, recipient, "remotetest.Note"), payload)
	}
	cases := []struct {
		name  string
		frame []byte
	}{
		{"version 2", withByte(msg(nil), 4, 2)},
		{"unknown kind", withByte(msg(nil), 5, 15)},
		{"flags", withByte(msg(nil), 7, 1)},
		{"PING with a sequence number", withByte(wire(control(remoting.KindPing)), 15, 1)},
		{"MSG with sequence number 0", withByte(msg(nil), 15, 0)},
		{"PING with a sender", wire("01 04 0000", h64(0), "00", hstr(sender), "00 00")},
		{"headers on WATCH", wire(fmt.Sprintf("01 %02x 0000", byte(remoting.KindWatch)), h64(1), "01 01"+hs("k")+"00", hstr(sender), hstr(recipient), "00", "01")},
		{"33 headers", msg(func(f *remoting.Frame) { f.Headers = many })},
		{"header key empty", msg(func(f *remoting.Frame) { f.Headers = []engine.Header{{Value: "v"}} })},
		{"header key of 257 bytes", msg(func(f *remoting.Frame) { f.Headers = []engine.Header{{Key: longKey}} })},
		{"header value of 8193 bytes", msg(func(f *remoting.Frame) { f.Headers = []engine.Header{{Key: "k", Value: longValue}} })},
		{"CLOSE reason of 65 bytes", wire(control(remoting.KindClose), hs(strings.Repeat("x", remoting.MaxReason+1)))},
		{"type on WATCH", typed(remoting.KindWatch, "01")},
		{"type on FAIL", typed(remoting.KindFail, "00"+hs("no"))},
		{"type on TERMINATED", typed(remoting.KindTerminated, "01 00")},
		{"sender past the end", wire(fmt.Sprintf("01 %02x 0000", byte(remoting.KindMsg)), h64(1), "00", "7f"+hs("lockstep://r/x#1"))},
		{"length that overflows", wire(fmt.Sprintf("01 %02x 0000", byte(remoting.KindMsg)), h64(1), "00", "ffffffffffffffffffff01")},
		{"CLOSE reason not printable", wire(control(remoting.KindClose), "01")},
		{"CLOSE reason empty", wire(control(remoting.KindClose))},
		{"FAIL message not UTF-8", wire(dataHead(remoting.KindFail, 1, sender, "lockstep://b/temp/1#5", ""), "00 ff")},
		{"TERMINATED unknown stop kind", wire(dataHead(remoting.KindTerminated, 1, sender, recipient, ""), "01 05")},
		{"WATCH number 0", wire(dataHead(remoting.KindWatch, 1, sender, recipient, ""), "00")},
		{"ASK address past the end", wire(dataHead(remoting.KindAsk, 1, sender, recipient, "remotetest.Note"), "40")},
		{"length below the minimum", hx("00000005 0110000000")},
		{"length above the frame limit", hx("00100001 0110")},
		{"WELCOME on an incoming session", remoting.AppendFrame(nil, helloFrame(remoting.KindWelcome, "r", "b", ""))},
		{"HELLO on an up session", remoting.AppendFrame(nil, helloFrame(remoting.KindHello, "r", "b", ""))},
		{"PONG on an incoming session", wire(control(remoting.KindPong))},
		{"REJECT on an up session", wire(control(remoting.KindReject), hs("codec"))},
	}
	warnings := 0
	for _, tc := range cases {
		t.Run("incoming/"+tc.name, func(t *testing.T) {
			r := dialRaw(t, b.addr)
			r.handshake("r")
			r.writeBytes(tc.frame)
			r.expectClose(remoting.CloseProtocol)
			warnings++
			waitLogs(t, b, "remoting: protocol error", map[string]string{"peer": "r", "dir": "in"}, warnings)
		})
	}

	// b's outgoing session to r: a frame before the WELCOME, or one the dialer must not receive.
	out := 0
	for _, tc := range []struct {
		name   string
		up     bool // WELCOME first
		frames []byte
	}{
		{"PONG before WELCOME", false, wire(control(remoting.KindPong))},
		{"WELCOME with another codec", false, goodHelloKind(remoting.KindWelcome, func(h *remoting.Hello) { h.Sender, h.Receiver, h.Codec = "r", "b", "gob" })},
		{"malformed WELCOME", false, withByte(goodHelloKind(remoting.KindWelcome, func(h *remoting.Hello) { h.Sender, h.Receiver = "r", "b" }), 7, 1)},
		{"PING to the dialer", true, wire(control(remoting.KindPing))},
		{"MSG to the dialer", true, msg(nil)},
		{"length above the frame limit", true, hx("7fffffff")},
	} {
		t.Run("outgoing/"+tc.name, func(t *testing.T) {
			out++
			if err := lockstep.Send(b.sys, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: out}); err != nil {
				t.Fatal(err)
			}
			r := acceptRaw(t, ln)
			if tc.up {
				r.welcome("b")
				r.expect(remoting.KindMsg)
			} else {
				r.expect(remoting.KindHello)
			}
			r.writeBytes(tc.frames)
			r.expectClose(remoting.CloseProtocol)
			waitLogs(t, b, "remoting: protocol error", map[string]string{"peer": "r", "dir": "out"}, out)
			// The message of a session that never came up waits for the next one.
			if !tc.up {
				next := acceptRaw(t, ln)
				next.welcome("b")
				next.expect(remoting.KindMsg)
				next.write(remoting.Frame{Kind: remoting.KindClose, Payload: []byte(remoting.CloseShutdown)})
				next.expectEOF()
			}
		})
	}

	// The system still serves new sessions.
	r := dialRaw(t, b.addr)
	r.handshake("r")
	r.send(data(remoting.KindMsg, recipient, "remotetest.Note", `{"N":42}`))
	if got := recv(t, notes); got.N != 42 {
		t.Errorf("after the protocol errors b received %+v", got)
	}
}

// goodHelloKind is goodHello as a frame of kind k.
func goodHelloKind(k remoting.Kind, edit func(*remoting.Hello)) []byte {
	b := goodHello(edit)
	b[5] = byte(k)
	return b
}

// TestATREM08SequenceRule checks REM-200 and REM-210 from a raw peer: a frame numbered above the
// next one expected is accepted after the gap, one numbered below it is discarded late, and frames
// whose recipient is on another node, or whose ask address is on this one, are discarded
// misrouted. The frames of a session are applied in order, so the message that follows the
// discarded ones shows that nothing else arrived.
func TestATREM08SequenceRule(t *testing.T) {
	reg := newRegistry()
	b := startNode(t, "b", reg, nil)
	seqs := make(chan Seq, 16)
	spawn[Seq](t, b.sys, "seq", forward[Seq]{seqs})
	const recipient = "lockstep://b/user/seq#0"
	seqMsg := func(n int, to string) remoting.Frame {
		return data(remoting.KindMsg, to, "remotetest.Seq", fmt.Sprintf(`{"N":%d}`, n))
	}

	r := dialRaw(t, b.addr)
	r.handshake("r")
	at := func(seq uint64, f remoting.Frame) {
		f.Seq = seq
		r.write(f)
	}
	at(1, seqMsg(1, recipient))
	at(3, seqMsg(3, recipient)) // a gap: 2 is missing
	at(2, seqMsg(2, recipient)) // late
	at(3, seqMsg(33, recipient))
	at(4, seqMsg(4, "lockstep://c/user/seq#0")) // misrouted: another node
	ask := data(remoting.KindAsk, recipient, "remotetest.Seq", "")
	ask.Payload = remoting.AppendAsk(nil, "lockstep://b/temp/1#9", []byte(`{"N":5}`))
	at(5, ask) // misrouted: the answer would come back to b
	at(9, seqMsg(9, recipient))
	at(10, seqMsg(10, "lockstep://b/user/seq#0"))

	var got []int
	for len(got) < 4 {
		got = append(got, recv(t, seqs).N)
	}
	if want := []int{1, 3, 9, 10}; !slices.Equal(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
	select {
	case s := <-seqs:
		t.Errorf("also delivered %d", s.N)
	case <-time.After(50 * time.Millisecond):
	}
	if n := len(b.log.find("remoting: protocol error", nil)); n != 0 {
		t.Errorf("%d protocol errors; late and misrouted frames are discarded quietly", n)
	}
}

// TestATREM29FrameSize checks the frame limits: a message whose frame exceeds the sender's own
// MaxFrame is refused by Send with ErrFrameTooLarge and nothing is sent; one that exceeds the
// limit the peer announced in its WELCOME is a dead letter too-large-for-peer, an ask whose request
// does fails with ErrFrameTooLarge, and neither takes a sequence number.
func TestATREM29FrameSize(t *testing.T) {
	big := Note{Items: make([]int, 2500)} // about 5,000 bytes of JSON
	tooLarge := regexp.MustCompile(`(^|: )remote: frame too large: \d+ bytes > max 4096$`)
	bigEcho := func(rt lockstep.ReplyTo[string]) Echo { return Echo{Text: strings.Repeat("x", 5000), Reply: rt} }

	t.Run("own limit", func(t *testing.T) {
		reg := newRegistry()
		ln := rawListener(t)
		b := startNode(t, "b", reg, func(c *remote.Config) {
			c.Peers = map[string]string{"r": ln.Addr().String()}
			c.MaxFrame = 4096
		})
		err := lockstep.Send(b.sys, lockstep.PIDAt[Note]("r", "/user/x"), big)
		if !errors.Is(err, remote.ErrFrameTooLarge) || !tooLarge.MatchString(err.Error()) {
			t.Errorf("Send of a large message = %v, want %q", err, tooLarge)
		}
		ctx, cancel := context.WithTimeout(context.Background(), rawWait)
		defer cancel()
		_, err = lockstep.Ask(b.sys, lockstep.PIDAt[Echo]("r", "/user/echo"), bigEcho).Await(ctx)
		if !errors.Is(err, remote.ErrFrameTooLarge) || !tooLarge.MatchString(err.Error()) {
			t.Errorf("Ask with a large request = %v, want %q", err, tooLarge)
		}
		if conn := acceptWithin(t, ln, 100*time.Millisecond); conn != nil {
			t.Fatal("b dialed r for frames it refused")
		}
		if err := lockstep.Send(b.sys, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: 1}); err != nil {
			t.Fatal(err)
		}
		r := acceptRaw(t, ln)
		r.welcome("b")
		if _, f := r.expect(remoting.KindMsg); f.Seq != 1 || string(f.Payload) != `{"N":1,"Items":null}` {
			t.Errorf("first MSG: seq %d %s, want seq 1 with the small message", f.Seq, f.Payload)
		}
	})

	t.Run("peer limit", func(t *testing.T) {
		reg := newRegistry()
		ln := rawListener(t)
		b := startNode(t, "b", reg, func(c *remote.Config) { c.Peers = map[string]string{"r": ln.Addr().String()} })
		to := lockstep.PIDAt[Note]("r", "/user/x")
		if err := lockstep.Send(b.sys, to, big); err != nil {
			t.Fatalf("Send of a message within b's own limit: %v", err)
		}
		reply := lockstep.Ask(b.sys, lockstep.PIDAt[Echo]("r", "/user/echo"), bigEcho, lockstep.WithTimeout(rawWait))
		if err := lockstep.Send(b.sys, to, Note{N: 1}); err != nil {
			t.Fatal(err)
		}
		r := acceptRaw(t, ln)
		r.expect(remoting.KindHello)
		r.writeBytes(wire(control(remoting.KindWelcome), helloHex("r", "b", rawInc, 4096, "")))
		if _, f := r.expect(remoting.KindMsg); f.Seq != 1 || string(f.Payload) != `{"N":1,"Items":null}` {
			t.Errorf("first frame: seq %d %s, want seq 1 with the small message", f.Seq, f.Payload)
		}
		ctx, cancel := context.WithTimeout(context.Background(), rawWait)
		defer cancel()
		if _, err := reply.Await(ctx); !errors.Is(err, remote.ErrFrameTooLarge) || !tooLarge.MatchString(err.Error()) {
			t.Errorf("Ask with a request too large for r = %v, want %q", err, tooLarge)
		}
		if got := b.log.find("dead letter", map[string]string{"reason": "too-large-for-peer"}); len(got) != 2 {
			t.Errorf("%d dead letters too-large-for-peer, want 2: %v", len(got), got)
		}
	})
}

// The heartbeat settings of AT-REM-25.
const (
	heartbeat   = 100 * time.Millisecond
	idleTimeout = 500 * time.Millisecond
	// idleMax is the end of AT-REM-25's window for an idle close: IdleTimeout, plus one Heartbeat
	// for the tick that sees it, plus 100 ms for scheduling.
	idleMax = 700 * time.Millisecond
)

// heartbeatNode starts a node named b with AT-REM-25's heartbeat settings and edit.
func heartbeatNode(t *testing.T, edit func(*remote.Config)) *node {
	t.Helper()
	return startNode(t, "b", newRegistry(), func(c *remote.Config) {
		c.Heartbeat, c.IdleTimeout = heartbeat, idleTimeout
		if edit != nil {
			edit(c)
		}
	})
}

// idleWindow runs attempt until the idle close it measures falls in AT-REM-25's window. attempt
// returns two durations that bracket the time from the session's last received frame to its
// CLOSE idle: early, measured from an instant before the system received that frame, and late,
// from an instant after it. A close before IdleTimeout fails at once; one after idleMax is retried
// twice, since a loaded machine can delay any timer, and fails the third time.
func idleWindow(t *testing.T, attempt func(t *testing.T) (early, late time.Duration)) {
	t.Helper()
	for i := 1; ; i++ {
		early, late := attempt(t)
		if t.Failed() {
			return
		}
		if early < idleTimeout {
			t.Fatalf("CLOSE idle %s after the last frame, before IdleTimeout %s", early, idleTimeout)
		}
		if late <= idleMax {
			return
		}
		if i == 3 {
			t.Fatalf("CLOSE idle %s after the last frame, after %s, three times", late, idleMax)
		}
		t.Logf("attempt %d: CLOSE idle %s after the last frame, after %s; trying again", i, late, idleMax)
	}
}

// TestATREM25Heartbeat checks REM-090 over TCP with Heartbeat 100 ms and IdleTimeout 500 ms: a
// dialer whose session is idle writes PING, no sooner than one Heartbeat after it last received a
// frame, and a peer that answers each PING with PONG keeps the session up; an acceptor answers
// every PING with PONG; and a silent peer is closed with CLOSE idle between 500 ms and 700 ms after
// the last frame it sent, on an incoming and on an outgoing session.
func TestATREM25Heartbeat(t *testing.T) {
	t.Run("ping from an idle dialer", func(t *testing.T) {
		ln := rawListener(t)
		b := heartbeatNode(t, func(c *remote.Config) { c.Peers = map[string]string{"r": ln.Addr().String()} })
		if err := lockstep.Send(b.sys, lockstep.PIDAt[Note]("r", "/user/x"), Note{}); err != nil {
			t.Fatal(err)
		}
		r := acceptRaw(t, ln)
		r.expect(remoting.KindHello)
		welcomed := time.Now()
		r.write(*helloFrame(remoting.KindWelcome, "r", "b", ""))
		r.expect(remoting.KindMsg)
		r.expectBytes("PING", wire(control(remoting.KindPing)))
		if d := time.Since(welcomed); d < heartbeat {
			t.Errorf("PING %s after the WELCOME, before one Heartbeat", d)
		}
		// Answered, the pings go on and the session stays up for twice IdleTimeout.
		pings := 0
		for end := time.Now().Add(2 * idleTimeout); time.Now().Before(end); pings++ {
			r.write(remoting.Frame{Kind: remoting.KindPong})
			r.expect(remoting.KindPing)
		}
		if pings < 3 {
			t.Errorf("%d PINGs in %s, want one a Heartbeat", pings, 2*idleTimeout)
		}
	})

	t.Run("pong from the acceptor", func(t *testing.T) {
		b := heartbeatNode(t, nil)
		r := dialRaw(t, b.addr)
		r.handshake("r")
		for range 3 {
			r.writeBytes(wire(control(remoting.KindPing)))
			r.expectBytes("PONG", wire(control(remoting.KindPong)))
		}
	})

	t.Run("silent dialer", func(t *testing.T) {
		b := heartbeatNode(t, nil)
		idleWindow(t, func(t *testing.T) (time.Duration, time.Duration) {
			r := dialRaw(t, b.addr)
			sent := time.Now()
			r.write(*helloFrame(remoting.KindHello, "r", "b", ""))
			r.expect(remoting.KindWelcome) // b wrote it when it received the HELLO
			welcomed := time.Now()
			r.expectBytes("CLOSE idle", wire(control(remoting.KindClose), hs(remoting.CloseIdle)))
			closed := time.Now()
			r.expectEOF()
			return closed.Sub(sent), closed.Sub(welcomed)
		})
	})

	t.Run("silent acceptor", func(t *testing.T) {
		ln := rawListener(t)
		b := heartbeatNode(t, func(c *remote.Config) { c.Peers = map[string]string{"r": ln.Addr().String()} })
		idleWindow(t, func(t *testing.T) (time.Duration, time.Duration) {
			if err := lockstep.Send(b.sys, lockstep.PIDAt[Note]("r", "/user/x"), Note{}); err != nil {
				t.Fatal(err)
			}
			r := acceptRaw(t, ln)
			r.expect(remoting.KindHello)
			sent := time.Now()
			r.write(*helloFrame(remoting.KindWelcome, "r", "b", ""))
			r.expect(remoting.KindMsg) // b wrote it when it received the WELCOME
			welcomed := time.Now()
			// b pings, and r never answers.
			for {
				_, f := r.next()
				if f.Kind == remoting.KindClose {
					closed := time.Now()
					if string(f.Payload) != remoting.CloseIdle {
						t.Fatalf("CLOSE %q, want %q", f.Payload, remoting.CloseIdle)
					}
					r.expectEOF()
					return closed.Sub(sent), closed.Sub(welcomed)
				}
				if f.Kind != remoting.KindPing {
					t.Fatalf("read a %s frame, want PING or CLOSE", f.Kind)
				}
			}
		})
	})
}
