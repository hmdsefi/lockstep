// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// A raw peer is test code that speaks frames itself over a plain net.Conn to a real system
// (AT-REM-04): it writes frames byte by byte and reads the system's frames as bytes. It builds and
// parses frames with internal/remoting's frame functions, or writes them out field by field as
// golden bytes (wire).

// rawWait bounds every read of a raw peer.
const rawWait = 10 * time.Second

// rawListener listens on loopback for a raw peer: test code that speaks frames itself.
func rawListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// acceptWithin accepts a connection on ln within d, or returns nil.
func acceptWithin(t *testing.T, ln net.Listener, d time.Duration) net.Conn {
	t.Helper()
	tl, ok := ln.(*net.TCPListener)
	if !ok {
		t.Fatalf("listener %T is not TCP", ln)
	}
	if err := tl.SetDeadline(time.Now().Add(d)); err != nil {
		t.Fatal(err)
	}
	conn, err := ln.Accept()
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// readRaw reads one frame's bytes, length prefix included, from conn within rawWait. It does not
// check the frame.
func readRaw(t *testing.T, conn net.Conn) ([]byte, error) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(rawWait)); err != nil {
		t.Fatal(err)
	}
	var prefix [4]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		return nil, err
	}
	b := make([]byte, 4+int(binary.BigEndian.Uint32(prefix[:])))
	copy(b, prefix[:])
	if _, err := io.ReadFull(conn, b[4:]); err != nil {
		return nil, err
	}
	return b, nil
}

// readFrame reads one frame from conn within rawWait; a frame that breaks the protocol fails the
// test.
func readFrame(t *testing.T, conn net.Conn) (remoting.Frame, error) {
	t.Helper()
	b, err := readRaw(t, conn)
	if err != nil {
		return remoting.Frame{}, err
	}
	f, err := remoting.ParseFrame(b, remoting.MaxFrameLimit)
	if err != nil {
		t.Fatalf("frame from the node: %v", err)
	}
	return f, nil
}

// mustRead reads a frame of kind k from conn, skipping PINGs.
func mustRead(t *testing.T, conn net.Conn, k remoting.Kind) remoting.Frame {
	t.Helper()
	for {
		f, err := readFrame(t, conn)
		if err != nil {
			t.Fatalf("reading a %s frame: %v", k, err)
		}
		if f.Kind == remoting.KindPing && k != remoting.KindPing {
			continue
		}
		if f.Kind != k {
			t.Fatalf("read a %s frame, want %s", f.Kind, k)
		}
		return f
	}
}

func writeFrame(t *testing.T, conn net.Conn, f *remoting.Frame) {
	t.Helper()
	if _, err := conn.Write(remoting.AppendFrame(nil, f)); err != nil {
		t.Fatal(err)
	}
}

// helloFrame is a HELLO or WELCOME (k) from raw peer sender to receiver.
func helloFrame(k remoting.Kind, sender, receiver, advertise string) *remoting.Frame {
	return &remoting.Frame{Kind: k, Payload: remoting.AppendHello(nil, &remoting.Hello{
		Sender: sender, Receiver: receiver, Incarnation: rawInc, FrameLimit: remote.DefaultMaxFrame, Codec: "json", Advertise: advertise,
	})}
}

// rawInc is the incarnation every raw peer announces.
const rawInc = 9

// rawPeer is one connection of a raw peer: one it dialed, or one a system dialed to it.
type rawPeer struct {
	t    *testing.T
	conn net.Conn
	seq  uint64 // the sequence number of the last data frame send wrote
}

// dialRaw connects a raw peer to the system listening on addr.
func dialRaw(t *testing.T, addr string) *rawPeer {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &rawPeer{t: t, conn: conn}
}

// acceptRaw accepts the connection a system dials to ln within rawWait.
func acceptRaw(t *testing.T, ln net.Listener) *rawPeer {
	t.Helper()
	conn := acceptWithin(t, ln, rawWait)
	if conn == nil {
		t.Fatalf("no connection to %s within %s", ln.Addr(), rawWait)
	}
	return &rawPeer{t: t, conn: conn}
}

// writeBytes writes b as it is.
func (p *rawPeer) writeBytes(b []byte) {
	p.t.Helper()
	if _, err := p.conn.Write(b); err != nil {
		p.t.Fatalf("raw write: %v", err)
	}
}

// write writes f as it is, with its own sequence number.
func (p *rawPeer) write(f remoting.Frame) { p.t.Helper(); p.writeBytes(remoting.AppendFrame(nil, &f)) }

// send writes the data frame f with the connection's next sequence number.
func (p *rawPeer) send(f remoting.Frame) {
	p.t.Helper()
	p.seq++
	f.Seq = p.seq
	p.write(f)
}

// next reads the next frame, PINGs included, and returns its bytes and its parsed form. An error
// or a frame that breaks the protocol fails the test.
func (p *rawPeer) next() ([]byte, remoting.Frame) {
	p.t.Helper()
	b, err := readRaw(p.t, p.conn)
	if err != nil {
		p.t.Fatalf("raw read: %v", err)
	}
	f, err := remoting.ParseFrame(b, remoting.MaxFrameLimit)
	if err != nil {
		p.t.Fatalf("frame from the node: %v (% x)", err, b)
	}
	return b, f
}

// expect reads the next frame, skipping PINGs unless k is PING, and fails the test unless it is
// of kind k.
func (p *rawPeer) expect(k remoting.Kind) ([]byte, remoting.Frame) {
	p.t.Helper()
	for {
		b, f := p.next()
		if f.Kind == remoting.KindPing && k != remoting.KindPing {
			continue
		}
		if f.Kind != k {
			p.t.Fatalf("read a %s frame (%s), want %s", f.Kind, f.Payload, k)
		}
		return b, f
	}
}

// expectBytes reads the next frame, skipping PINGs unless want is one, and fails the test unless
// it is want, byte for byte.
func (p *rawPeer) expectBytes(what string, want []byte) remoting.Frame {
	p.t.Helper()
	b, f := p.expect(remoting.Kind(want[5]))
	sameBytes(p.t, what, b, want)
	return f
}

// expectEOF reads until the system closes the connection, then closes it. Any frame but a PING
// fails the test.
func (p *rawPeer) expectEOF() {
	p.t.Helper()
	for {
		b, err := readRaw(p.t, p.conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.t.Errorf("read %v, want EOF", err)
			}
			_ = p.conn.Close()
			return
		}
		if len(b) < 6 || remoting.Kind(b[5]) != remoting.KindPing {
			p.t.Fatalf("read % x, want EOF", b)
		}
	}
}

// expectClose reads CLOSE with reason, byte for byte, and then EOF.
func (p *rawPeer) expectClose(reason string) {
	p.t.Helper()
	p.expectBytes("CLOSE "+reason, wire(control(remoting.KindClose), hs(reason)))
	p.expectEOF()
}

// handshake writes the HELLO of sender to b and reads b's WELCOME.
func (p *rawPeer) handshake(sender string) {
	p.t.Helper()
	p.write(*helloFrame(remoting.KindHello, sender, "b", ""))
	p.expect(remoting.KindWelcome)
}

// welcome reads the HELLO a system dialed raw peer r with and answers it with r's WELCOME to
// receiver, the system's node.
func (p *rawPeer) welcome(receiver string) {
	p.t.Helper()
	_, f := p.expect(remoting.KindHello)
	if h, err := remoting.ParseHello(f.Payload); err != nil || h.Sender != receiver || h.Receiver != "r" {
		p.t.Fatalf("HELLO %+v, %v; want one from %s to r", h, err, receiver)
	}
	p.write(*helloFrame(remoting.KindWelcome, "r", receiver, ""))
}

// rawSender is the sender of the raw peers' data frames.
const rawSender = "lockstep://r/user/s#1"

// data returns a data frame of kind k from rawSender to recipient with payload, of wire name typ.
func data(k remoting.Kind, recipient, typ, payload string) remoting.Frame {
	return remoting.Frame{Kind: k, Sender: rawSender, Recipient: recipient, Type: typ, Payload: []byte(payload)}
}

// sameBytes fails the test unless got is want.
func sameBytes(t *testing.T, what string, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Errorf("%s:\n got % x\nwant % x", what, got, want)
	}
}

// Golden bytes: frames written out field by field in hex, independently of remoting.AppendFrame.

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

// hstr returns the hex of a length-prefixed text: its uvarint length, one byte for a text shorter
// than 128 bytes, then its bytes.
func hstr(s string) string {
	if len(s) >= 0x80 {
		panic("hstr: text of 128 bytes or more")
	}
	return fmt.Sprintf("%02x", len(s)) + hs(s)
}

// h32 and h64 return the big-endian hex of v.
func h32(v uint32) string { return fmt.Sprintf("%08x", v) }
func h64(v uint64) string { return fmt.Sprintf("%016x", v) }

// wire returns the frame whose L bytes are the hex parts, with its 4-byte length prefix in front.
func wire(parts ...string) []byte {
	body := hx(parts...)
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...) //nolint:gosec // test frames are small
}

// control returns the hex of the fields of a control frame of kind k up to its payload: version,
// kind, flags, sequence number 0, no headers, and empty sender, recipient and type.
func control(k remoting.Kind) string {
	return fmt.Sprintf("01 %02x 0000", byte(k)) + h64(0) + " 00 00 00 00"
}

// dataHead returns the hex of the fields of a data frame of kind k numbered seq up to its type,
// without headers.
func dataHead(k remoting.Kind, seq uint64, sender, recipient, typ string) string {
	return fmt.Sprintf("01 %02x 0000", byte(k)) + h64(seq) + " 00 " + hstr(sender) + hstr(recipient) + hstr(typ)
}

// helloHex returns the hex of a hello payload with the codec json.
func helloHex(sender, receiver string, inc, limit uint32, advertise string) string {
	return hstr(sender) + hstr(receiver) + h32(inc) + h32(limit) + hstr("json") + hstr(advertise)
}

// do runs f in the turn of the doer that receives it, so that a test sends, asks and watches from
// an actor whose address it knows.
type do struct{ f func(*lockstep.Context[do]) }

// doer runs the do messages it receives and hands every Terminated it gets to terms, if set.
type doer struct{ terms chan<- lockstep.Terminated }

func (doer) Receive(ctx *lockstep.Context[do], m do) { m.f(ctx) }

func (d doer) Signal(_ *lockstep.Context[do], sig lockstep.Signal) {
	if term, ok := sig.(lockstep.Terminated); ok && d.terms != nil {
		d.terms <- term
	}
}

// runIn runs f in a turn of the doer at pid and waits until it has run.
func runIn(t *testing.T, sys *lockstep.System, pid lockstep.PID[do], f func(*lockstep.Context[do])) {
	t.Helper()
	done := make(chan struct{})
	if err := lockstep.Send(sys, pid, do{func(ctx *lockstep.Context[do]) {
		defer close(done)
		f(ctx)
	}}); err != nil {
		t.Fatal(err)
	}
	recv(t, done)
}
