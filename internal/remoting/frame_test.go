// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/remoting"
)

// hx decodes hex digits; spaces between them are ignored.
func hx(parts ...string) []byte {
	b, err := hex.DecodeString(strings.ReplaceAll(strings.Join(parts, ""), " ", ""))
	if err != nil {
		panic(err)
	}
	return b
}

// hs returns the hex digits of s's bytes.
func hs(s string) string { return hex.EncodeToString([]byte(s)) }

// cat joins byte slices.
func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

const maxFrame = 1 << 20

// Addresses of the golden frames; each text is 16 bytes, so its length prefix is 0x10.
const (
	addrA  = "lockstep://a/u#1" // a sender on node a
	addrB  = "lockstep://b/v#2" // a target on node b
	addrQ  = "lockstep://a/q#3" // an ask address on node a
	addrL  = "lockstep://a/l#4" // a lookup reply address on node a
	pathB0 = "lockstep://b/v#0" // the path PID of addrB
)

var (
	helloAB = remoting.Hello{
		Sender: "a", Receiver: "b", Incarnation: 1, FrameLimit: 1 << 20,
		Codec: "json", Advertise: "127.0.0.1:7000",
	}
	welcomeBA = remoting.Hello{
		Sender: "b", Receiver: "a", Incarnation: 2, FrameLimit: 4096, Codec: "json",
	}
)

// golden is one frame of every kind with its bytes written out by hand: the length prefix, then
// version, kind, flags and seq, then the headers count and pairs, sender, recipient, type and
// payload. The frame-level part of AT-REM-40 (every kind's layout) and of AT-REM-04 (the raw
// handshake and MSG frame).
var golden = []struct {
	name  string
	frame remoting.Frame
	bytes []byte
}{
	{
		name:  "HELLO",
		frame: remoting.Frame{Kind: remoting.KindHello, Payload: remoting.AppendHello(nil, &helloAB)},
		bytes: hx("00000030", "01 01 0000 0000000000000000", "00", "00", "00", "00",
			"01"+hs("a"), "01"+hs("b"), "00000001", "00100000", "04"+hs("json"), "0e"+hs("127.0.0.1:7000")),
	},
	{
		name:  "WELCOME",
		frame: remoting.Frame{Kind: remoting.KindWelcome, Payload: remoting.AppendHello(nil, &welcomeBA)},
		bytes: hx("00000022", "01 02 0000 0000000000000000", "00", "00", "00", "00",
			"01"+hs("b"), "01"+hs("a"), "00000002", "00001000", "04"+hs("json"), "00"),
	},
	{
		name:  "REJECT",
		frame: remoting.Frame{Kind: remoting.KindReject, Payload: []byte(remoting.RejectVersion)},
		bytes: hx("00000017", "01 03 0000 0000000000000000", "00", "00", "00", "00", hs("version")),
	},
	{
		name:  "PING",
		frame: remoting.Frame{Kind: remoting.KindPing},
		bytes: hx("00000010", "01 04 0000 0000000000000000", "00", "00", "00", "00"),
	},
	{
		name:  "PONG",
		frame: remoting.Frame{Kind: remoting.KindPong},
		bytes: hx("00000010", "01 05 0000 0000000000000000", "00", "00", "00", "00"),
	},
	{
		name:  "CLOSE",
		frame: remoting.Frame{Kind: remoting.KindClose, Payload: []byte(remoting.CloseProtocol)},
		bytes: hx("00000018", "01 06 0000 0000000000000000", "00", "00", "00", "00", hs("protocol")),
	},
	{
		name: "MSG",
		frame: remoting.Frame{
			Kind: remoting.KindMsg, Seq: 1, Headers: []engine.Header{{Key: "k", Value: "v"}},
			Sender: addrA, Recipient: addrB, Type: "t.Ping", Payload: []byte(`{"Seq":1}`),
		},
		bytes: hx("00000043", "01 10 0000 0000000000000001", "01 01"+hs("k")+"01"+hs("v"),
			"10"+hs(addrA), "10"+hs(addrB), "06"+hs("t.Ping"), hs(`{"Seq":1}`)),
	},
	{
		name: "ASK",
		frame: remoting.Frame{
			Kind: remoting.KindAsk, Seq: 2, Sender: addrA, Recipient: addrB, Type: "t.Ping",
			Payload: remoting.AppendAsk(nil, addrQ, []byte(`{"Seq":1}`)),
		},
		bytes: hx("00000050", "01 11 0000 0000000000000002", "00",
			"10"+hs(addrA), "10"+hs(addrB), "06"+hs("t.Ping"), "10"+hs(addrQ)+hs(`{"Seq":1}`)),
	},
	{
		name: "REPLY",
		frame: remoting.Frame{
			Kind: remoting.KindReply, Seq: 3, Sender: addrB, Recipient: addrQ, Type: "t.Pong",
			Payload: []byte(`{}`),
		},
		bytes: hx("00000038", "01 12 0000 0000000000000003", "00",
			"10"+hs(addrB), "10"+hs(addrQ), "06"+hs("t.Pong"), hs(`{}`)),
	},
	{
		name: "FAIL",
		frame: remoting.Frame{
			Kind: remoting.KindFail, Seq: 4, Sender: addrB, Recipient: addrQ,
			Payload: remoting.AppendFailure(nil, &remoting.Failure{Code: 1, Message: "timed out"}),
		},
		bytes: hx("0000003a", "01 13 0000 0000000000000004", "00",
			"10"+hs(addrB), "10"+hs(addrQ), "00", "01"+hs("timed out")),
	},
	{
		name:  "WATCH",
		frame: remoting.Frame{Kind: remoting.KindWatch, Seq: 5, Sender: addrA, Recipient: addrB},
		bytes: hx("00000030", "01 14 0000 0000000000000005", "00", "10"+hs(addrA), "10"+hs(addrB), "00"),
	},
	{
		name:  "UNWATCH",
		frame: remoting.Frame{Kind: remoting.KindUnwatch, Seq: 6, Sender: addrA, Recipient: addrB},
		bytes: hx("00000030", "01 15 0000 0000000000000006", "00", "10"+hs(addrA), "10"+hs(addrB), "00"),
	},
	{
		name: "TERMINATED",
		frame: remoting.Frame{
			Kind: remoting.KindTerminated, Seq: 7, Sender: addrB, Recipient: addrA,
			Payload: remoting.AppendTerminated(nil, &remoting.Terminated{
				Kind: remoting.StopFailed, Failure: remoting.Failure{Code: 0, Message: "boom"},
			}),
		},
		bytes: hx("00000036", "01 16 0000 0000000000000007", "00",
			"10"+hs(addrB), "10"+hs(addrA), "00", "01 00"+hs("boom")),
	},
	{
		name: "LOOKUP",
		frame: remoting.Frame{
			Kind: remoting.KindLookup, Seq: 8, Sender: addrL, Recipient: pathB0, Type: "t.Ping",
		},
		bytes: hx("00000036", "01 17 0000 0000000000000008", "00",
			"10"+hs(addrL), "10"+hs(pathB0), "06"+hs("t.Ping")),
	},
	{
		name:  "LOOKUP without type",
		frame: remoting.Frame{Kind: remoting.KindLookup, Seq: 9, Sender: addrL, Recipient: pathB0},
		bytes: hx("00000030", "01 17 0000 0000000000000009", "00", "10"+hs(addrL), "10"+hs(pathB0), "00"),
	},
}

func TestFrameGoldenEveryKind(t *testing.T) {
	seen := map[remoting.Kind]bool{}
	for _, g := range golden {
		t.Run(g.name, func(t *testing.T) {
			seen[g.frame.Kind] = true
			if got := remoting.AppendFrame(nil, &g.frame); !bytes.Equal(got, g.bytes) {
				t.Errorf("AppendFrame:\n got %x\nwant %x", got, g.bytes)
			}
			got, err := remoting.ParseFrame(g.bytes, maxFrame)
			if err != nil {
				t.Fatalf("ParseFrame: %v", err)
			}
			if !reflect.DeepEqual(got, g.frame) {
				t.Errorf("ParseFrame:\n got %+v\nwant %+v", got, g.frame)
			}
		})
	}
	for k := range 256 {
		if remoting.Kind(k).Known() && !seen[remoting.Kind(k)] {
			t.Errorf("no golden frame for %s", remoting.Kind(k))
		}
	}
}

// AT-REM-04's frame-level part: the dialer's HELLO, the acceptor's WELCOME and a MSG frame as one
// byte stream, split and parsed back in order.
func TestFrameHandshakeStream(t *testing.T) {
	stream := cat(golden[0].bytes, golden[1].bytes, golden[6].bytes)
	var kinds []remoting.Kind
	for len(stream) > 0 {
		f, rest, err := remoting.SplitFrame(stream, maxFrame)
		if err != nil || f == nil {
			t.Fatalf("SplitFrame(%x) = %x, %v", stream, f, err)
		}
		fr, err := remoting.ParseFrame(f, maxFrame)
		if err != nil {
			t.Fatalf("ParseFrame: %v", err)
		}
		kinds = append(kinds, fr.Kind)
		stream = rest
	}
	want := []remoting.Kind{remoting.KindHello, remoting.KindWelcome, remoting.KindMsg}
	if !reflect.DeepEqual(kinds, want) {
		t.Errorf("kinds = %v, want %v", kinds, want)
	}
	h, err := remoting.ParseHello(golden[0].frame.Payload)
	if err != nil || h != helloAB {
		t.Errorf("ParseHello = %+v, %v; want %+v", h, err, helloAB)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	big := make([]engine.Header, remoting.MaxHeaders)
	for i := range big {
		big[i] = engine.Header{Key: strings.Repeat("k", remoting.MaxHeaderKey), Value: strings.Repeat("v", remoting.MaxHeaderValue)}
	}
	for _, f := range []remoting.Frame{
		{Kind: remoting.KindMsg, Seq: 1<<64 - 1, Sender: addrA, Recipient: addrB, Type: "t.Ping"},
		{Kind: remoting.KindMsg, Seq: 1, Headers: big, Sender: addrA, Recipient: addrB, Type: "x", Payload: []byte{0}},
		{Kind: remoting.KindReply, Seq: 2, Headers: []engine.Header{{Key: "k"}}, Payload: bytes.Repeat([]byte{0xff}, 300)},
		{Kind: remoting.KindAsk, Seq: 3, Payload: remoting.AppendAsk(nil, "", nil)},
		{Kind: remoting.KindReject, Payload: []byte(strings.Repeat("~", remoting.MaxReason))},
		{Kind: remoting.KindClose, Payload: []byte(remoting.CloseUnknownConnection)},
		{Kind: remoting.KindTerminated, Seq: 4, Payload: remoting.AppendTerminated(nil, &remoting.Terminated{Kind: remoting.StopNoActor})},
		{Kind: remoting.KindFail, Seq: 5, Payload: remoting.AppendFailure(nil, &remoting.Failure{Code: 200})},
	} {
		b := remoting.AppendFrame([]byte("prefix"), &f)
		if string(b[:6]) != "prefix" {
			t.Fatalf("AppendFrame overwrote dst: %q", b[:6])
		}
		got, err := remoting.ParseFrame(b[6:], maxFrame)
		if err != nil {
			t.Fatalf("ParseFrame(%s): %v", f.Kind, err)
		}
		if !reflect.DeepEqual(got, f) {
			t.Errorf("round trip of %s:\n got %+v\nwant %+v", f.Kind, got, f)
		}
	}
}

// frame encodes f without checking it, to build frames that break the rules.
func frame(f remoting.Frame) []byte { return remoting.AppendFrame(nil, &f) }

// withByte returns a copy of b with b[i] set to v.
func withByte(b []byte, i int, v byte) []byte {
	c := bytes.Clone(b)
	c[i] = v
	return c
}

// AT-REM-07's frame-level part: every malformed frame is a *ProtocolError naming its field.
func TestFrameViolations(t *testing.T) {
	ping := golden[3].bytes
	msg := golden[6].bytes
	headers := func(n int) []engine.Header {
		h := make([]engine.Header, n)
		for i := range h {
			h[i] = engine.Header{Key: "k", Value: "v"}
		}
		return h
	}
	msgWith := func(h []engine.Header) []byte {
		return frame(remoting.Frame{Kind: remoting.KindMsg, Seq: 1, Headers: h, Sender: addrA, Recipient: addrB, Type: "t"})
	}
	for _, tc := range []struct {
		name  string
		b     []byte
		max   int
		field remoting.Field
	}{
		{"no length prefix", hx("000000"), maxFrame, remoting.FieldLength},
		{"length below 16", hx("0000000f", "01 04 0000 0000000000000000", "00 00 00"), maxFrame, remoting.FieldLength},
		{"length 0", hx("00000000"), maxFrame, remoting.FieldLength},
		{"length above max", msg, len(msg) - 5, remoting.FieldLength},
		{"length above max uint32", hx("ffffffff"), maxFrame, remoting.FieldLength},
		{"trailing byte", cat(ping, hx("00")), maxFrame, remoting.FieldLength},
		{"truncated", ping[:len(ping)-1], maxFrame, remoting.FieldLength},
		{"version 0", withByte(ping, 4, 0), maxFrame, remoting.FieldVersion},
		{"version 2", withByte(ping, 4, 2), maxFrame, remoting.FieldVersion},
		{"kind 0", withByte(ping, 5, 0), maxFrame, remoting.FieldKind},
		{"kind 7", withByte(ping, 5, 7), maxFrame, remoting.FieldKind},
		{"kind 15", withByte(ping, 5, 15), maxFrame, remoting.FieldKind},
		{"kind 24", withByte(ping, 5, 24), maxFrame, remoting.FieldKind},
		{"kind 255", withByte(ping, 5, 255), maxFrame, remoting.FieldKind},
		{"flags high bit", withByte(ping, 6, 0x80), maxFrame, remoting.FieldFlags},
		{"flags low bit", withByte(ping, 7, 0x01), maxFrame, remoting.FieldFlags},
		{"control frame with seq", withByte(ping, 15, 1), maxFrame, remoting.FieldSeq},
		{"HELLO with seq", frame(remoting.Frame{Kind: remoting.KindHello, Seq: 1, Payload: remoting.AppendHello(nil, &helloAB)}), maxFrame, remoting.FieldSeq},
		{"data frame with seq 0", withByte(msg, 15, 0), maxFrame, remoting.FieldSeq},
		{"WATCH with seq 0", frame(remoting.Frame{Kind: remoting.KindWatch, Sender: addrA, Recipient: addrB}), maxFrame, remoting.FieldSeq},

		{"headers on PING", frame(remoting.Frame{Kind: remoting.KindPing, Headers: headers(1)}), maxFrame, remoting.FieldHeaders},
		{"headers on FAIL", frame(remoting.Frame{Kind: remoting.KindFail, Seq: 1, Headers: headers(1), Payload: []byte{1}}), maxFrame, remoting.FieldHeaders},
		{"headers on WATCH", frame(remoting.Frame{Kind: remoting.KindWatch, Seq: 1, Headers: headers(1)}), maxFrame, remoting.FieldHeaders},
		{"headers on LOOKUP", frame(remoting.Frame{Kind: remoting.KindLookup, Seq: 1, Headers: headers(1)}), maxFrame, remoting.FieldHeaders},
		{"33 headers", msgWith(headers(remoting.MaxHeaders + 1)), maxFrame, remoting.FieldHeaders},
		{"empty header key", msgWith([]engine.Header{{Key: "", Value: "v"}}), maxFrame, remoting.FieldHeaders},
		{"header key of 257 bytes", msgWith([]engine.Header{{Key: strings.Repeat("k", remoting.MaxHeaderKey+1)}}), maxFrame, remoting.FieldHeaders},
		{"header value of 8193 bytes", msgWith([]engine.Header{{Key: "k", Value: strings.Repeat("v", remoting.MaxHeaderValue+1)}}), maxFrame, remoting.FieldHeaders},
		{"header count overruns", hx("00000010", "01 10 0000 0000000000000001", "01 00 00 00"), maxFrame, remoting.FieldHeaders},
		{"header key overruns", hx("00000010", "01 10 0000 0000000000000001", "01 05 00 00"), maxFrame, remoting.FieldHeaders},
		{"header value overruns", hx("00000012", "01 10 0000 0000000000000001", "01 01 6b 7f 00 00"), maxFrame, remoting.FieldHeaders},
		{"header count overflows", hx("00000019", "01 10 0000 0000000000000001", "ffffffffffffffffff7f 00 00 00"), maxFrame, remoting.FieldHeaders},
		{"header count unterminated", hx("00000010", "01 10 0000 0000000000000001", "80 80 80 80"), maxFrame, remoting.FieldHeaders},

		{"sender on HELLO", frame(remoting.Frame{Kind: remoting.KindHello, Sender: addrA, Payload: remoting.AppendHello(nil, &helloAB)}), maxFrame, remoting.FieldSender},
		{"sender on PING", frame(remoting.Frame{Kind: remoting.KindPing, Sender: addrA}), maxFrame, remoting.FieldSender},
		{"recipient on PONG", frame(remoting.Frame{Kind: remoting.KindPong, Recipient: addrB}), maxFrame, remoting.FieldRecipient},
		{"recipient on CLOSE", frame(remoting.Frame{Kind: remoting.KindClose, Recipient: addrB, Payload: []byte("idle")}), maxFrame, remoting.FieldRecipient},
		{"type on REJECT", frame(remoting.Frame{Kind: remoting.KindReject, Type: "t", Payload: []byte("kind")}), maxFrame, remoting.FieldType},
		{"type on FAIL", frame(remoting.Frame{Kind: remoting.KindFail, Seq: 1, Type: "t", Payload: []byte{1}}), maxFrame, remoting.FieldType},
		{"type on WATCH", frame(remoting.Frame{Kind: remoting.KindWatch, Seq: 1, Type: "t"}), maxFrame, remoting.FieldType},
		{"type on UNWATCH", frame(remoting.Frame{Kind: remoting.KindUnwatch, Seq: 1, Type: "t"}), maxFrame, remoting.FieldType},
		{"type on TERMINATED", frame(remoting.Frame{Kind: remoting.KindTerminated, Seq: 1, Type: "t", Payload: []byte{0}}), maxFrame, remoting.FieldType},
		{"sender overruns", hx("00000010", "01 10 0000 0000000000000001", "00 7f 00 00"), maxFrame, remoting.FieldSender},
		{"recipient overruns", hx("00000010", "01 10 0000 0000000000000001", "00 00 02 00"), maxFrame, remoting.FieldRecipient},
		{"type overruns", hx("00000010", "01 10 0000 0000000000000001", "00 00 00 01"), maxFrame, remoting.FieldType},
		{"type missing", hx("00000010", "01 10 0000 0000000000000001", "00 00 00 80"), maxFrame, remoting.FieldType},

		{"payload on PING", cat(hx("00000011"), ping[4:], hx("00")), maxFrame, remoting.FieldPayload},
		{"payload on PONG", frame(remoting.Frame{Kind: remoting.KindPong, Payload: []byte{0}}), maxFrame, remoting.FieldPayload},
		{"payload on WATCH", frame(remoting.Frame{Kind: remoting.KindWatch, Seq: 1, Payload: []byte{0}}), maxFrame, remoting.FieldPayload},
		{"payload on UNWATCH", frame(remoting.Frame{Kind: remoting.KindUnwatch, Seq: 1, Payload: []byte{0}}), maxFrame, remoting.FieldPayload},
		{"payload on LOOKUP", frame(remoting.Frame{Kind: remoting.KindLookup, Seq: 1, Payload: []byte{0}}), maxFrame, remoting.FieldPayload},
		{"REJECT without reason", frame(remoting.Frame{Kind: remoting.KindReject}), maxFrame, remoting.FieldPayload},
		{"CLOSE reason of 65 bytes", frame(remoting.Frame{Kind: remoting.KindClose, Payload: bytes.Repeat([]byte("a"), remoting.MaxReason+1)}), maxFrame, remoting.FieldPayload},
		{"CLOSE reason not ASCII", frame(remoting.Frame{Kind: remoting.KindClose, Payload: []byte("idlé")}), maxFrame, remoting.FieldPayload},
		{"CLOSE reason with a control byte", frame(remoting.Frame{Kind: remoting.KindClose, Payload: []byte("idle\n")}), maxFrame, remoting.FieldPayload},
		{"HELLO without payload", frame(remoting.Frame{Kind: remoting.KindHello}), maxFrame, remoting.FieldPayload},
		{"WELCOME with a trailing byte", frame(remoting.Frame{Kind: remoting.KindWelcome, Payload: append(remoting.AppendHello(nil, &welcomeBA), 0)}), maxFrame, remoting.FieldPayload},
		{"ASK without payload", frame(remoting.Frame{Kind: remoting.KindAsk, Seq: 1}), maxFrame, remoting.FieldPayload},
		{"ASK address overruns", frame(remoting.Frame{Kind: remoting.KindAsk, Seq: 1, Payload: hx("05 6162")}), maxFrame, remoting.FieldPayload},
		{"FAIL without payload", frame(remoting.Frame{Kind: remoting.KindFail, Seq: 1}), maxFrame, remoting.FieldPayload},
		{"FAIL message not UTF-8", frame(remoting.Frame{Kind: remoting.KindFail, Seq: 1, Payload: hx("01 ff")}), maxFrame, remoting.FieldPayload},
		{"TERMINATED without payload", frame(remoting.Frame{Kind: remoting.KindTerminated, Seq: 1}), maxFrame, remoting.FieldPayload},
		{"TERMINATED stop kind 5", frame(remoting.Frame{Kind: remoting.KindTerminated, Seq: 1, Payload: hx("05")}), maxFrame, remoting.FieldPayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := remoting.ParseFrame(tc.b, tc.max)
			var pe *remoting.ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("ParseFrame(%x) = %+v, %v; want a *ProtocolError", tc.b, f, err)
			}
			if pe.Field != tc.field {
				t.Errorf("ParseFrame(%x): field %s, want %s (%v)", tc.b, pe.Field, tc.field, err)
			}
			if !reflect.DeepEqual(f, remoting.Frame{}) {
				t.Errorf("ParseFrame returned %+v with its error, want the zero Frame", f)
			}
		})
	}
}

func TestFrameLimitsAccepted(t *testing.T) {
	// A frame exactly at the receiver's limit is accepted, one byte under it is not.
	msg := golden[6].bytes
	if _, err := remoting.ParseFrame(msg, len(msg)-4); err != nil {
		t.Errorf("ParseFrame at the limit: %v", err)
	}
	if _, err := remoting.ParseFrame(msg, len(msg)-5); err == nil {
		t.Error("ParseFrame over the limit: no error")
	}
	// The header limits' edges are accepted.
	for _, h := range [][]engine.Header{
		{{Key: "k"}},
		{{Key: strings.Repeat("k", remoting.MaxHeaderKey), Value: strings.Repeat("v", remoting.MaxHeaderValue)}},
		make([]engine.Header, remoting.MaxHeaders),
	} {
		for i := range h {
			if h[i].Key == "" {
				h[i].Key = "k"
			}
		}
		if !remoting.ValidHeaders(h) {
			t.Errorf("ValidHeaders(%d headers) = false", len(h))
		}
		b := frame(remoting.Frame{Kind: remoting.KindMsg, Seq: 1, Headers: h})
		if _, err := remoting.ParseFrame(b, maxFrame); err != nil {
			t.Errorf("ParseFrame(%d headers): %v", len(h), err)
		}
	}
}

func TestFrameValidHeaders(t *testing.T) {
	for _, tc := range []struct {
		h    []engine.Header
		want bool
	}{
		{nil, true},
		{[]engine.Header{{Key: "k", Value: ""}}, true},
		{[]engine.Header{{Key: "", Value: "v"}}, false},
		{[]engine.Header{{Key: strings.Repeat("k", remoting.MaxHeaderKey+1)}}, false},
		{[]engine.Header{{Key: "k", Value: strings.Repeat("v", remoting.MaxHeaderValue+1)}}, false},
		{make([]engine.Header, remoting.MaxHeaders+1), false},
	} {
		for i := range tc.h {
			if tc.h[i] == (engine.Header{}) {
				tc.h[i].Key = "k"
			}
		}
		if got := remoting.ValidHeaders(tc.h); got != tc.want {
			t.Errorf("ValidHeaders(%d headers) = %t, want %t", len(tc.h), got, tc.want)
		}
	}
}

func TestFrameSplit(t *testing.T) {
	ping, msg := golden[3].bytes, golden[6].bytes
	stream := cat(ping, msg)
	for n := range len(stream) + 1 {
		f, rest, err := remoting.SplitFrame(stream[:n], maxFrame)
		if err != nil {
			t.Fatalf("SplitFrame(%d bytes): %v", n, err)
		}
		switch {
		case n < len(ping):
			if f != nil || len(rest) != n {
				t.Errorf("SplitFrame(%d bytes) = %x, %d bytes left; want nothing cut", n, f, len(rest))
			}
		default:
			if !bytes.Equal(f, ping) || !bytes.Equal(rest, stream[len(ping):n]) {
				t.Errorf("SplitFrame(%d bytes) = %x, %x; want the PING frame", n, f, rest)
			}
			if cap(f) != len(f) {
				t.Errorf("SplitFrame(%d bytes): frame capacity %d, want %d", n, cap(f), len(f))
			}
		}
	}
	// A bad length prefix fails as soon as its four bytes are there.
	for _, b := range [][]byte{hx("0000000f"), hx("00100001 00")} {
		f, rest, err := remoting.SplitFrame(b, maxFrame)
		var pe *remoting.ProtocolError
		if !errors.As(err, &pe) || pe.Field != remoting.FieldLength || f != nil || !bytes.Equal(rest, b) {
			t.Errorf("SplitFrame(%x) = %x, %x, %v; want a length error", b, f, rest, err)
		}
	}
}

func TestFrameKind(t *testing.T) {
	var names []string
	var data []bool
	for k := range 256 {
		kind := remoting.Kind(k)
		if !kind.Known() {
			if kind.Data() {
				t.Errorf("%s: Data() for an unknown kind", kind)
			}
			if want := "Kind(" + strconv.Itoa(k) + ")"; kind.String() != want {
				t.Errorf("Kind(%d).String() = %q, want %q", k, kind.String(), want)
			}
			continue
		}
		names = append(names, strconv.Itoa(k)+"="+kind.String())
		data = append(data, kind.Data())
	}
	wantNames := []string{"1=HELLO", "2=WELCOME", "3=REJECT", "4=PING", "5=PONG", "6=CLOSE",
		"16=MSG", "17=ASK", "18=REPLY", "19=FAIL", "20=WATCH", "21=UNWATCH", "22=TERMINATED", "23=LOOKUP"}
	if !reflect.DeepEqual(names, wantNames) {
		t.Errorf("kinds = %v, want %v", names, wantNames)
	}
	wantData := []bool{false, false, false, false, false, false, true, true, true, true, true, true, true, true}
	if !reflect.DeepEqual(data, wantData) {
		t.Errorf("Data() = %v, want %v", data, wantData)
	}
}

// rejectReason is the REJECT reason an acceptor picks for a malformed first frame, as
// ProtocolError's doc describes it (REM-070: version, then kind, then payload).
func rejectReason(pe *remoting.ProtocolError) string {
	switch {
	case pe.Field == remoting.FieldVersion:
		return remoting.RejectVersion
	case pe.Field == remoting.FieldLength:
		return remoting.RejectPayload
	case pe.Kind != remoting.KindHello:
		return remoting.RejectKind
	}
	return remoting.RejectPayload
}

func TestFrameProtocolErrorKind(t *testing.T) {
	hello := golden[0].bytes
	for _, tc := range []struct {
		name   string
		b      []byte
		kind   remoting.Kind
		reason string
	}{
		{"length", hx("0000000f"), 0, remoting.RejectPayload},
		{"version", withByte(hello, 4, 2), 0, remoting.RejectVersion},
		{"unknown kind", withByte(hello, 5, 7), 7, remoting.RejectKind},
		{"CLOSE with a bad reason", frame(remoting.Frame{Kind: remoting.KindClose, Payload: []byte("idle\n")}), remoting.KindClose, remoting.RejectKind},
		{"MSG with seq 0", withByte(golden[6].bytes, 15, 0), remoting.KindMsg, remoting.RejectKind},
		{"HELLO with flags", withByte(hello, 7, 1), remoting.KindHello, remoting.RejectPayload},
		{"HELLO with a sender field", frame(remoting.Frame{Kind: remoting.KindHello, Sender: addrA, Payload: golden[0].frame.Payload}), remoting.KindHello, remoting.RejectPayload},
		{"HELLO with a bad payload", frame(remoting.Frame{Kind: remoting.KindHello, Payload: hx("00")}), remoting.KindHello, remoting.RejectPayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := remoting.ParseFrame(tc.b, maxFrame)
			var pe *remoting.ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("ParseFrame(%x) = %v, want a *ProtocolError", tc.b, err)
			}
			if pe.Kind != tc.kind {
				t.Errorf("Kind = %s, want %s (%v)", pe.Kind, tc.kind, err)
			}
			if got := rejectReason(pe); got != tc.reason {
				t.Errorf("REJECT reason = %q, want %q (%v)", got, tc.reason, err)
			}
		})
	}
	// A payload parser called on its own leaves Kind 0.
	var pe *remoting.ProtocolError
	if _, err := remoting.ParseHello(nil); !errors.As(err, &pe) || pe.Kind != 0 {
		t.Errorf("ParseHello(nil) = %v, want a *ProtocolError with Kind 0", err)
	}
}

func TestFrameProtocolError(t *testing.T) {
	_, err := remoting.ParseFrame(withByte(golden[3].bytes, 4, 2), maxFrame)
	if want := "remoting: protocol error: version: version 2 is not 1"; err == nil || err.Error() != want {
		t.Errorf("error = %v, want %q", err, want)
	}
	for _, tc := range []struct {
		f    remoting.Field
		want string
	}{
		{remoting.FieldLength, "length"}, {remoting.FieldPayload, "payload"}, {0, "Field(0)"}, {99, "Field(99)"},
	} {
		if tc.f.String() != tc.want {
			t.Errorf("Field(%d).String() = %q, want %q", tc.f, tc.f.String(), tc.want)
		}
	}
}

func TestFrameAsk(t *testing.T) {
	for _, tc := range []struct {
		addr string
		body []byte
	}{{addrQ, []byte(`{"Seq":1}`)}, {addrQ, nil}, {"", []byte{0}}, {strings.Repeat("x", 200), []byte{1, 2}}} {
		p := remoting.AppendAsk(nil, tc.addr, tc.body)
		addr, body, err := remoting.ParseAsk(p)
		if err != nil || addr != tc.addr || !bytes.Equal(body, tc.body) || (len(tc.body) == 0) != (body == nil) {
			t.Errorf("ParseAsk(AppendAsk(%q, %x)) = %q, %x, %v", tc.addr, tc.body, addr, body, err)
		}
	}
	for _, p := range [][]byte{nil, hx("01"), hx("80")} {
		if _, _, err := remoting.ParseAsk(p); err == nil {
			t.Errorf("ParseAsk(%x): no error", p)
		}
	}
}

// FuzzFrameParse checks that ParseFrame never panics, fails only with a *ProtocolError, and that
// every frame it accepts encodes to bytes it parses to the same frame.
func FuzzFrameParse(f *testing.F) {
	for _, g := range golden {
		f.Add(g.bytes)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if fr, rest, err := remoting.SplitFrame(b, 1<<16); err == nil && fr != nil && len(fr)+len(rest) != len(b) {
			t.Fatalf("SplitFrame lost bytes")
		}
		fr, err := remoting.ParseFrame(b, 1<<16)
		if err != nil {
			var pe *remoting.ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("ParseFrame error %T: %v", err, err)
			}
			return
		}
		again, err := remoting.ParseFrame(remoting.AppendFrame(nil, &fr), 1<<16)
		if err != nil || !reflect.DeepEqual(again, fr) {
			t.Fatalf("re-encoded frame parses to %+v, %v; want %+v", again, err, fr)
		}
	})
}

func TestHelloRoundTrip(t *testing.T) {
	for _, h := range []remoting.Hello{
		helloAB,
		welcomeBA,
		{
			Sender: strings.Repeat("n", 64), Receiver: "B-2_x.y", Incarnation: 1<<32 - 1,
			FrameLimit: remoting.MaxFrameLimit, Codec: strings.Repeat("c", remoting.MaxCodecName),
			Advertise: strings.Repeat("h", remoting.MaxAdvertise),
		},
		{Sender: "0", Receiver: "9", Incarnation: 1, FrameLimit: remoting.MinFrameLimit, Codec: "c"},
	} {
		b := remoting.AppendHello([]byte{0xaa}, &h)
		if b[0] != 0xaa {
			t.Fatalf("AppendHello overwrote dst")
		}
		got, err := remoting.ParseHello(b[1:])
		if err != nil || got != h {
			t.Errorf("ParseHello(AppendHello(%+v)) = %+v, %v", h, got, err)
		}
	}
}

func TestHelloViolations(t *testing.T) {
	with := func(change func(h *remoting.Hello)) []byte {
		h := helloAB
		change(&h)
		return remoting.AppendHello(nil, &h)
	}
	good := remoting.AppendHello(nil, &helloAB)
	cases := []struct {
		name string
		b    []byte
	}{
		{"empty", nil},
		{"empty sender", with(func(h *remoting.Hello) { h.Sender = "" })},
		{"sender starts with a dash", with(func(h *remoting.Hello) { h.Sender = "-a" })},
		{"sender with a space", with(func(h *remoting.Hello) { h.Sender = "a b" })},
		{"sender of 65 bytes", with(func(h *remoting.Hello) { h.Sender = strings.Repeat("a", 65) })},
		{"empty receiver", with(func(h *remoting.Hello) { h.Receiver = "" })},
		{"receiver with a slash", with(func(h *remoting.Hello) { h.Receiver = "b/c" })},
		{"incarnation 0", with(func(h *remoting.Hello) { h.Incarnation = 0 })},
		{"frame limit 0", with(func(h *remoting.Hello) { h.FrameLimit = 0 })},
		{"frame limit 4095", with(func(h *remoting.Hello) { h.FrameLimit = remoting.MinFrameLimit - 1 })},
		{"frame limit 67108865", with(func(h *remoting.Hello) { h.FrameLimit = remoting.MaxFrameLimit + 1 })},
		{"empty codec", with(func(h *remoting.Hello) { h.Codec = "" })},
		{"codec of 65 bytes", with(func(h *remoting.Hello) { h.Codec = strings.Repeat("c", remoting.MaxCodecName+1) })},
		{"advertise of 256 bytes", with(func(h *remoting.Hello) { h.Advertise = strings.Repeat("h", remoting.MaxAdvertise+1) })},
		{"trailing byte", cat(good, hx("00"))},
		{"sender length overruns", hx("05 61")},
	}
	for n := range len(good) {
		cases = append(cases, struct {
			name string
			b    []byte
		}{"truncated to " + strconv.Itoa(n) + " bytes", good[:n]})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, err := remoting.ParseHello(tc.b)
			var pe *remoting.ProtocolError
			if !errors.As(err, &pe) || pe.Field != remoting.FieldPayload {
				t.Fatalf("ParseHello(%x) = %+v, %v; want a payload error", tc.b, h, err)
			}
			if h != (remoting.Hello{}) {
				t.Errorf("ParseHello returned %+v with its error", h)
			}
		})
	}
}

// A peer's long name is reported by its length, not quoted; a short invalid one is quoted.
func TestHelloErrorTextBounded(t *testing.T) {
	for _, tc := range []struct {
		h    remoting.Hello
		want string
	}{
		{remoting.Hello{Sender: strings.Repeat("a", 1<<20), Receiver: "b"}, "remoting: protocol error: payload: hello sender of 1048576 bytes is longer than a node name"},
		{remoting.Hello{Sender: "a", Receiver: strings.Repeat("-", 65)}, "remoting: protocol error: payload: hello receiver of 65 bytes is longer than a node name"},
		{remoting.Hello{Sender: strings.Repeat("-", 64), Receiver: "b"}, `remoting: protocol error: payload: hello sender "` + strings.Repeat("-", 64) + `" is not a valid node name`},
	} {
		_, err := remoting.ParseHello(remoting.AppendHello(nil, &tc.h))
		if err == nil || err.Error() != tc.want {
			t.Errorf("ParseHello error = %.200v, want %q", err, tc.want)
		}
	}
}

func TestFailAndTerminatedRoundTrip(t *testing.T) {
	// Every code byte, known or not, crosses the wire as it is.
	for c := range 256 {
		f := remoting.Failure{Code: uint8(c), Message: "message ü " + strconv.Itoa(c)}
		b := remoting.AppendFailure(nil, &f)
		if b[0] != uint8(c) || string(b[1:]) != f.Message {
			t.Fatalf("AppendFailure(%+v) = %x", f, b)
		}
		got, err := remoting.ParseFailure(b)
		if err != nil || got != f {
			t.Errorf("ParseFailure(%x) = %+v, %v; want %+v", b, got, err, f)
		}
	}
	if got, err := remoting.ParseFailure(hx("2a")); err != nil || got != (remoting.Failure{Code: 42}) {
		t.Errorf("ParseFailure(2a) = %+v, %v", got, err)
	}
	// A message that is not valid UTF-8 is repaired on the way out.
	if b := remoting.AppendFailure(nil, &remoting.Failure{Code: 3, Message: "a\xffb"}); !bytes.Equal(b, cat(hx("03"), []byte("a�b"))) {
		t.Errorf("AppendFailure with invalid UTF-8 = %x", b)
	}

	for _, tc := range []struct {
		t remoting.Terminated
		b []byte
	}{
		{remoting.Terminated{Kind: remoting.StopNormal}, hx("00")},
		{remoting.Terminated{Kind: remoting.StopFailed, Failure: remoting.Failure{Code: 7, Message: "stopped"}}, hx("01 07", hs("stopped"))},
		{remoting.Terminated{Kind: remoting.StopFailed, Failure: remoting.Failure{Code: 99}}, hx("01 63")},
		{remoting.Terminated{Kind: remoting.StopParentStopping}, hx("02")},
		{remoting.Terminated{Kind: remoting.StopShutdown}, hx("03")},
		{remoting.Terminated{Kind: remoting.StopNoActor}, hx("04")},
	} {
		if b := remoting.AppendTerminated(nil, &tc.t); !bytes.Equal(b, tc.b) {
			t.Errorf("AppendTerminated(%+v) = %x, want %x", tc.t, b, tc.b)
		}
		if got, err := remoting.ParseTerminated(tc.b); err != nil || got != tc.t {
			t.Errorf("ParseTerminated(%x) = %+v, %v; want %+v", tc.b, got, err, tc.t)
		}
	}
	// A failure is written only for StopFailed.
	shutdown := remoting.Terminated{Kind: remoting.StopShutdown, Failure: remoting.Failure{Code: 1, Message: "x"}}
	if b := remoting.AppendTerminated(nil, &shutdown); !bytes.Equal(b, hx("03")) {
		t.Errorf("AppendTerminated(shutdown with a failure) = %x, want 03", b)
	}
}

func TestFailAndTerminatedViolations(t *testing.T) {
	for _, b := range [][]byte{nil, hx("01 ff"), hx("00 c3"), hx("07 ed a0 80")} {
		f, err := remoting.ParseFailure(b)
		var pe *remoting.ProtocolError
		if !errors.As(err, &pe) || pe.Field != remoting.FieldPayload || f != (remoting.Failure{}) {
			t.Errorf("ParseFailure(%x) = %+v, %v; want a payload error", b, f, err)
		}
	}
	for _, b := range [][]byte{
		nil,            // no stop kind
		hx("05"),       // unknown stop kind
		hx("ff"),       // unknown stop kind
		hx("05 00"),    // unknown stop kind with bytes after it
		hx("01"),       // failed without a code byte
		hx("01 00 ff"), // failed with a message that is not UTF-8
		hx("00 00"),    // normal with a trailing byte
		hx("02 01"),    // parent-stopping with a trailing byte
		hx("03 00"),    // shutdown with a trailing byte
		hx("04 6162"),  // no-actor with trailing bytes
	} {
		tm, err := remoting.ParseTerminated(b)
		var pe *remoting.ProtocolError
		if !errors.As(err, &pe) || pe.Field != remoting.FieldPayload || tm != (remoting.Terminated{}) {
			t.Errorf("ParseTerminated(%x) = %+v, %v; want a payload error", b, tm, err)
		}
	}
}
