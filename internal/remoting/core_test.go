// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// fakeLink is a Link that keeps what the core does with it.
type fakeLink struct {
	t      *testing.T
	id     uint64
	node   string // Dial's node; "" for an accepted link
	adv    string // Dial's advertise
	frames []Frame
	events []string // "write <KIND>" and "close", in call order
	closed bool
	full   bool // Write refuses frames
	tries  int  // Write calls, refused ones included
}

func (l *fakeLink) ID() uint64 { return l.id }

func (l *fakeLink) Write(b []byte) bool {
	l.t.Helper()
	l.tries++
	if l.closed {
		l.t.Errorf("conn %d: Write after Close", l.id)
	}
	if l.full {
		return false
	}
	f, err := ParseFrame(b, MaxFrameLimit)
	if err != nil {
		l.t.Fatalf("conn %d: the core wrote a frame that does not parse: %v", l.id, err)
	}
	l.frames = append(l.frames, f)
	l.events = append(l.events, "write "+f.Kind.String())
	return true
}

func (l *fakeLink) Close() {
	if !l.closed {
		l.events = append(l.events, "close")
	}
	l.closed = true
}

// kinds returns the kinds of the frames written on l, with CLOSE and REJECT reasons.
func (l *fakeLink) kinds() []string {
	var out []string
	for _, f := range l.frames {
		s := f.Kind.String()
		if f.Kind == KindClose || f.Kind == KindReject {
			s += " " + string(f.Payload)
		}
		out = append(out, s)
	}
	return out
}

// fakeDriver is a Driver whose Dial returns fakeLinks numbered from 1, or errs[node].
type fakeDriver struct {
	t     *testing.T
	next  uint64
	links []*fakeLink
	errs  map[string]error
}

func (d *fakeDriver) Dial(node, advertise string) (Link, error) {
	if err := d.errs[node]; err != nil {
		return nil, err
	}
	d.next++
	l := &fakeLink{t: d.t, id: d.next, node: node, adv: advertise}
	d.links = append(d.links, l)
	return l, nil
}

// harness is one core on node "a" over a manual engine and a fake driver.
type harness struct {
	t    *testing.T
	eng  *enginetest.Engine
	drv  *fakeDriver
	core *Core
	mark int // records before this index are ignored by recs
}

func testSettings() Settings {
	return Settings{
		Registry:      codec.Default(),
		Codec:         codec.JSON{},
		MaxFrame:      1 << 20,
		OutboundQueue: 1024,
		Heartbeat:     time.Second,
		IdleTimeout:   10 * time.Second,
		DialTimeout:   5 * time.Second,
		RedialMin:     100 * time.Millisecond,
		RedialMax:     10 * time.Second,
	}
}

func testConfig(eng *enginetest.Engine, drv Driver) Config {
	b := eng.Engine()
	return Config{
		Node:        "a",
		Incarnation: 1,
		Clock:       b.Clock,
		Entropy:     b.Entropy,
		Tracer:      b.Tracer,
		Failer:      b.Failer,
		Driver:      drv,
		Settings:    testSettings(),
	}
}

// newHarness returns a started core; mod, if not nil, changes its Config first.
func newHarness(t *testing.T, mod func(*Config)) *harness {
	t.Helper()
	eng := enginetest.New("a", 7)
	drv := &fakeDriver{t: t, errs: map[string]error{}}
	cfg := testConfig(eng, drv)
	if mod != nil {
		mod(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.Start()
	h := &harness{t: t, eng: eng, drv: drv, core: c}
	h.skip()
	return h
}

// skip makes recs ignore the records so far.
func (h *harness) skip() { h.mark = len(h.eng.Records()) }

// recs returns the lockstep.remote records since the last skip as "<kind> k=v ...", then skips them.
func (h *harness) recs() []string {
	h.t.Helper()
	all := h.eng.Records()
	var out []string
	for _, r := range all[h.mark:] {
		if strings.HasPrefix(r.Kind, "lockstep.remote.") {
			out = append(out, line(r))
		}
	}
	h.mark = len(all)
	return out
}

func line(r engine.Record) string {
	parts := []string{strings.TrimPrefix(r.Kind, "lockstep.remote.")}
	for _, a := range r.Attrs {
		parts = append(parts, a.Key+"="+a.Value)
	}
	return strings.Join(parts, " ")
}

func same(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s:\n got  %q\n want %q", what, got, want)
	}
}

// frame encodes a frame.
func frame(f Frame) []byte { return AppendFrame(nil, &f) }

func ctrl(k Kind, payload string) []byte {
	var p []byte
	if payload != "" {
		p = []byte(payload)
	}
	return frame(Frame{Kind: k, Payload: p})
}

// helloPayload is a hello payload with frame limit 8192.
func helloPayload(sender, receiver string, inc uint32, codecName, adv string) []byte {
	return AppendHello(nil, &Hello{Sender: sender, Receiver: receiver, Incarnation: inc, FrameLimit: 8192, Codec: codecName, Advertise: adv})
}

func hello(sender, receiver string, inc uint32, codecName, adv string) []byte {
	return frame(Frame{Kind: KindHello, Payload: helloPayload(sender, receiver, inc, codecName, adv)})
}

func welcome(sender, receiver string, inc uint32, codecName string) []byte {
	return frame(Frame{Kind: KindWelcome, Payload: helloPayload(sender, receiver, inc, codecName, "")})
}

// dialUp connects to "b" and answers the HELLO with a WELCOME; it returns the outgoing link.
func (h *harness) dialUp() *fakeLink {
	h.t.Helper()
	if err := h.core.Connect("b"); err != nil {
		h.t.Fatalf("Connect(b): %v", err)
	}
	l := h.drv.links[len(h.drv.links)-1]
	h.core.Frame(l, welcome("b", "a", 7, "json"))
	h.skip()
	return l
}

// acceptUp accepts a link with ID id and a HELLO from "b"; it returns the incoming link.
func (h *harness) acceptUp(id uint64) *fakeLink {
	h.t.Helper()
	l := &fakeLink{t: h.t, id: id}
	h.core.Accepted(l)
	h.core.Frame(l, hello("b", "a", 9, "json", ""))
	h.skip()
	return l
}

func count(labels []string, label string) int {
	n := 0
	for _, l := range labels {
		if l == label {
			n++
		}
	}
	return n
}

// TestRemoteStart checks the start record, with listen only when set, that Start runs once, and
// that the tick runs every Heartbeat.
func TestRemoteStart(t *testing.T) {
	t.Run("record", func(t *testing.T) {
		eng := enginetest.New("a", 1)
		c, err := New(testConfig(eng, &fakeDriver{t: t}))
		if err != nil {
			t.Fatal(err)
		}
		c.Start()
		c.Start()
		var got []string
		for _, r := range eng.Records() {
			got = append(got, line(r))
		}
		same(t, "records", got, []string{"start node=a inc=1 codec=json max-frame=1048576"})
		if n := count(eng.Labels(), labelTick); n != 1 {
			t.Errorf("%d tick timers armed by two Starts, want 1", n)
		}
		eng.Advance(3 * time.Second)
		if n := count(eng.Labels(), labelTick); n != 4 {
			t.Errorf("%d tick timers after 3 heartbeats, want 4", n)
		}
		// The core keeps the armed tick's Cancel, so a shutdown can stop it.
		if c.tick == nil || !c.tick() {
			t.Error("the core does not hold the pending tick's Cancel")
		}
		eng.Advance(3 * time.Second)
		if n := count(eng.Labels(), labelTick); n != 4 {
			t.Errorf("%d tick timers after the tick was cancelled, want 4", n)
		}
	})
	t.Run("listen", func(t *testing.T) {
		eng := enginetest.New("a", 1)
		cfg := testConfig(eng, &fakeDriver{t: t})
		cfg.Incarnation = 3
		cfg.Settings.Listen = "127.0.0.1:7000"
		cfg.Settings.MaxFrame = 4096
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.Start()
		same(t, "records", []string{line(eng.Records()[0])},
			[]string{"start node=a inc=3 codec=json max-frame=4096 listen=127.0.0.1:7000"})
	})
	t.Run("no tracer", func(t *testing.T) {
		eng := enginetest.New("a", 1)
		cfg := testConfig(eng, &fakeDriver{t: t})
		cfg.Tracer = nil
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.Start()
		if err := c.Connect("b"); err != nil {
			t.Fatal(err)
		}
		if n := len(eng.Records()); n != 0 {
			t.Errorf("%d records without a tracer", n)
		}
	})
	t.Run("config errors", func(t *testing.T) {
		eng := enginetest.New("a", 1)
		for _, tc := range []struct {
			mod  func(*Config)
			want string
		}{
			{func(c *Config) { c.Node = "" }, `node name "" is not valid`},
			{func(c *Config) { c.Incarnation = 0 }, "incarnation is 0"},
			{func(c *Config) { c.Clock = nil }, "no clock"},
			{func(c *Config) { c.Entropy = nil }, "no entropy"},
			{func(c *Config) { c.Driver = nil }, "no driver"},
			{func(c *Config) { c.Settings.Codec = nil }, "no codec"},
			{func(c *Config) { c.Settings.MaxFrame = 100 }, "MaxFrame 100 out of range [4096, 67108864]"},
			{func(c *Config) { c.Settings.Heartbeat = 0 }, "Heartbeat, IdleTimeout, DialTimeout and RedialMin must be positive"},
			{func(c *Config) { c.Settings.IdleTimeout = 1500 * time.Millisecond }, "IdleTimeout 1.5s is less than twice Heartbeat 1s"},
			{func(c *Config) { c.Settings.RedialMax = time.Millisecond }, "RedialMax 1ms is less than RedialMin 100ms"},
			{func(c *Config) { c.Settings.Advertise = strings.Repeat("x", 256) }, "advertise address of 256 bytes exceeds 255"},
		} {
			cfg := testConfig(eng, &fakeDriver{t: t})
			tc.mod(&cfg)
			_, err := New(cfg)
			if err == nil || err.Error() != "remoting: invalid config: "+tc.want {
				t.Errorf("New: %v, want remoting: invalid config: %s", err, tc.want)
			}
		}
	})
}

// TestHandshakeDialer checks the dialer's side (REM-070): a lazy dial, its HELLO, the dial
// record, WELCOME and the up record.
func TestHandshakeDialer(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Settings.Advertise = "a.example:7000" })
	if len(h.drv.links) != 0 {
		t.Fatal("the core dialed before Connect")
	}
	if err := h.core.Connect("b"); err != nil {
		t.Fatal(err)
	}
	if len(h.drv.links) != 1 {
		t.Fatalf("%d dials, want 1", len(h.drv.links))
	}
	l := h.drv.links[0]
	if l.node != "b" || l.adv != "" {
		t.Errorf("Dial(%q, %q), want Dial(\"b\", \"\")", l.node, l.adv)
	}
	same(t, "records", h.recs(), []string{"dial peer=b conn=1 attempt=1"})
	same(t, "frames", l.kinds(), []string{"HELLO"})
	got, err := ParseHello(l.frames[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	want := Hello{Sender: "a", Receiver: "b", Incarnation: 1, FrameLimit: 1 << 20, Codec: "json", Advertise: "a.example:7000"}
	if got != want {
		t.Errorf("HELLO payload %+v, want %+v", got, want)
	}
	if n := count(h.eng.Labels(), labelDialTimeout); n != 1 {
		t.Errorf("%d dial-timeout timers, want 1", n)
	}
	if err := h.core.Connect("b"); err != nil || len(h.drv.links) != 1 {
		t.Errorf("second Connect: err %v, %d dials; want nil, 1", err, len(h.drv.links))
	}

	h.core.Frame(l, welcome("b", "a", 7, "json"))
	same(t, "records", h.recs(), []string{"up peer=b conn=1 dir=out peer-inc=7 max-frame=8192"})
	h.eng.Advance(5 * time.Second) // past the dial timeout: cancelled by the WELCOME
	if l.closed {
		t.Error("the session closed after its WELCOME")
	}
	if err := h.core.Connect("b"); err != nil || len(h.drv.links) != 1 {
		t.Errorf("Connect while up: err %v, %d dials; want nil, 1", err, len(h.drv.links))
	}
	if err := h.core.Connect("a"); err == nil {
		t.Error("Connect to the own node returned nil")
	}
	if err := h.core.Connect("not a name"); err == nil {
		t.Error("Connect to an invalid name returned nil")
	}
}

// TestHandshakeAcceptor checks the acceptor's side: the accept record, WELCOME, the up record, and
// that a HELLO's advertise address is used for a later dial.
func TestHandshakeAcceptor(t *testing.T) {
	h := newHarness(t, nil)
	l := &fakeLink{t: t, id: 40}
	h.core.Accepted(l)
	h.core.Accepted(l) // known: ignored
	same(t, "records", h.recs(), []string{"accept conn=40"})
	h.core.Frame(l, hello("b", "a", 9, "json", "b.example:7001"))
	same(t, "frames", l.kinds(), []string{"WELCOME"})
	got, _ := ParseHello(l.frames[0].Payload)
	want := Hello{Sender: "a", Receiver: "b", Incarnation: 1, FrameLimit: 1 << 20, Codec: "json"}
	if got != want {
		t.Errorf("WELCOME payload %+v, want %+v", got, want)
	}
	same(t, "records", h.recs(), []string{"up peer=b conn=40 dir=in peer-inc=9 max-frame=8192"})

	if err := h.core.Connect("b"); err != nil {
		t.Fatal(err)
	}
	if d := h.drv.links[0]; d.adv != "b.example:7001" {
		t.Errorf("Dial advertise %q, want the HELLO's b.example:7001", d.adv)
	}
}

// TestHandshakeRejectOrder checks REM-070's order: version, kind, payload, self, wrong-node,
// codec. Each case breaks its rule and every later one, so only the order gives its reason.
func TestHandshakeRejectOrder(t *testing.T) {
	mut := func(b []byte, i int, v byte) []byte { b[i] = v; return b }
	badHello := func() []byte { return frame(Frame{Kind: KindHello, Payload: []byte{1}}) }
	for _, tc := range []struct {
		name  string
		frame []byte
		want  string
	}{
		{"version", mut(mut(ctrl(KindPing, ""), 4, 2), 5, 99), RejectVersion},
		{"unknown kind", mut(ctrl(KindPing, ""), 5, 99), RejectKind},
		{"not hello", ctrl(KindPing, ""), RejectKind},
		{"not hello and flags", mut(welcome("b", "x", 1, "gob"), 6, 1), RejectKind},
		{"data frame", frame(Frame{Kind: KindMsg, Seq: 1}), RejectKind},
		{"payload", mut(badHello(), 6, 1), RejectPayload},
		{"bad hello payload", badHello(), RejectPayload},
		{"short frame", []byte{0, 0, 0, 3}, RejectPayload},
		{"hello with seq", frame(Frame{Kind: KindHello, Seq: 4, Payload: helloPayload("a", "x", 1, "gob", "")}), RejectPayload},
		{"self", hello("a", "x", 1, "gob", ""), RejectSelf},
		{"wrong-node", hello("b", "x", 1, "gob", ""), RejectWrongNode},
		{"codec", hello("b", "a", 1, "gob", ""), RejectCodec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			l := &fakeLink{t: t, id: 5}
			h.core.Accepted(l)
			h.core.Frame(l, tc.frame)
			same(t, "events", l.events, []string{"write REJECT", "close"})
			same(t, "frames", l.kinds(), []string{"REJECT " + tc.want})
			same(t, "records", h.recs(), []string{
				"accept conn=5",
				"reject conn=5 reason=" + tc.want + " dir=in",
				"down peer= conn=5 dir=in reason=rejected",
			})
			h.core.Frame(l, hello("b", "a", 1, "json", "")) // closed: ignored
			if len(l.frames) != 1 {
				t.Errorf("frames after the REJECT: %q", l.kinds())
			}
		})
	}
}

// TestHandshakeRejected checks a REJECT received by the dialer: reject and down records, and a
// redial with one failure.
func TestHandshakeRejected(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.core.Connect("b"); err != nil {
		t.Fatal(err)
	}
	l := h.drv.links[0]
	h.skip()
	h.core.Frame(l, ctrl(KindReject, "codec"))
	r := enginetest.New("a", 7).Engine().Entropy.Rand("lockstep/a/1/$remote")
	d := 50*time.Millisecond + time.Duration(r.Int64N(int64(50*time.Millisecond)+1))
	same(t, "records", h.recs(), []string{
		"reject conn=1 reason=codec dir=out",
		"down peer=b conn=1 dir=out reason=rejected",
		"redial peer=b failures=1 delay=" + d.String(),
	})
	same(t, "events", l.events, []string{"write HELLO", "close"})
	h.eng.Advance(d)
	if len(h.drv.links) != 2 {
		t.Fatalf("%d dials after the redial delay, want 2", len(h.drv.links))
	}
	same(t, "records", h.recs(), []string{"dial peer=b conn=2 attempt=2"})
}

// TestHandshakeDialTimeout checks that a dial without WELCOME goes down after exactly DialTimeout.
func TestHandshakeDialTimeout(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Settings.DialTimeout = 20 * time.Second })
	if err := h.core.Connect("b"); err != nil {
		t.Fatal(err)
	}
	l := h.drv.links[0]
	h.skip()
	h.eng.Advance(20*time.Second - 1) // past IdleTimeout: a dialing session is left to the dial timeout
	if l.closed {
		t.Fatalf("closed before DialTimeout: %q", l.events)
	}
	h.eng.Advance(1)
	same(t, "events", l.events, []string{"write HELLO", "close"})
	recs := h.recs()
	if len(recs) != 2 || recs[0] != "down peer=b conn=1 dir=out reason=dial-timeout" || !strings.HasPrefix(recs[1], "redial peer=b failures=1 ") {
		t.Errorf("records %q, want down dial-timeout and a redial", recs)
	}
}

// TestHandshakeDialErrors checks Connect's errors: an unknown node is returned and arms nothing;
// another dial error counts as a failure and redials.
func TestHandshakeDialErrors(t *testing.T) {
	h := newHarness(t, nil)
	h.drv.errs["c"] = fmt.Errorf("no address for c: %w", ErrUnknownNode)
	if err := h.core.Connect("c"); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("Connect(c) = %v, want ErrUnknownNode", err)
	}
	same(t, "records", h.recs(), nil)
	if n := count(h.eng.Labels(), labelRedial); n != 0 {
		t.Errorf("%d redials armed for an unknown node", n)
	}
	if _, ok := h.core.peers["c"]; ok {
		t.Error("Connect to an unknown node left a peer entry behind")
	}
	h.acceptUp(60) // b is known from its HELLO; an unknown-node dial keeps the entry
	h.drv.errs["b"] = ErrUnknownNode
	if err := h.core.Connect("b"); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("Connect(b) = %v, want ErrUnknownNode", err)
	}
	if p := h.core.peers["b"]; p == nil || p.in == nil {
		t.Error("Connect to an unknown node dropped the entry of a peer with an incoming session")
	}
	h.skip()

	h.drv.errs["b"] = errors.New("driver busy")
	if err := h.core.Connect("b"); err != nil {
		t.Errorf("Connect(b) = %v, want nil", err)
	}
	recs := h.recs()
	if len(recs) != 1 || !strings.HasPrefix(recs[0], "redial peer=b failures=1 ") {
		t.Fatalf("records %q, want one redial", recs)
	}
	delete(h.drv.errs, "b")
	h.eng.Advance(100 * time.Millisecond)
	same(t, "records", h.recs(), []string{"dial peer=b conn=1 attempt=2"})

	// A link that fails to connect.
	h.core.Closed(h.drv.links[0], errors.New("connection refused"))
	recs = h.recs()
	if len(recs) != 2 || recs[0] != "down peer=b conn=1 dir=out reason=io" || !strings.HasPrefix(recs[1], "redial peer=b failures=2 ") {
		t.Errorf("records %q, want down io and a redial with 2 failures", recs)
	}
	if !h.drv.links[0].closed {
		t.Error("the core did not Close the link the driver reported closed")
	}
	h.core.Closed(h.drv.links[0], nil) // unknown now: ignored
	same(t, "records", h.recs(), nil)
}

// TestHandshakeBeforeWelcome checks frames other than WELCOME, REJECT and CLOSE before the WELCOME.
func TestHandshakeBeforeWelcome(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
		down  string
		ev    []string
	}{
		{"pong", ctrl(KindPong, ""), "protocol", []string{"write HELLO", "write CLOSE", "close"}},
		{"garbage", []byte{0, 0, 0, 1}, "protocol", []string{"write HELLO", "write CLOSE", "close"}},
		{"close", ctrl(KindClose, "shutdown"), "closed", []string{"write HELLO", "close"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			if err := h.core.Connect("b"); err != nil {
				t.Fatal(err)
			}
			l := h.drv.links[0]
			h.skip()
			h.core.Frame(l, tc.frame)
			same(t, "events", l.events, tc.ev)
			recs := h.recs()
			if len(recs) != 2 || recs[0] != "down peer=b conn=1 dir=out reason="+tc.down || !strings.HasPrefix(recs[1], "redial peer=b failures=1 ") {
				t.Errorf("records %q, want down %s and a redial", recs, tc.down)
			}
		})
	}
}

// TestWelcomeWrongNode checks REM-075: a WELCOME from another node than the one dialed, or to
// another node than this one, takes the session down wrong-node, as a failure.
func TestWelcomeWrongNode(t *testing.T) {
	for _, tc := range []struct{ name, sender, receiver string }{
		{"sender", "c", "a"},
		{"receiver", "b", "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			if err := h.core.Connect("b"); err != nil {
				t.Fatal(err)
			}
			l := h.drv.links[0]
			h.skip()
			h.core.Frame(l, welcome(tc.sender, tc.receiver, 7, "json"))
			same(t, "events", l.events, []string{"write HELLO", "close"})
			recs := h.recs()
			if len(recs) != 2 || recs[0] != "down peer=b conn=1 dir=out reason=wrong-node" || !strings.HasPrefix(recs[1], "redial peer=b failures=1 ") {
				t.Errorf("records %q, want down wrong-node and a redial", recs)
			}
		})
	}
}

// TestWelcomeCodec checks that a WELCOME with another codec closes the session as a protocol error.
func TestWelcomeCodec(t *testing.T) {
	h := newHarness(t, nil)
	if err := h.core.Connect("b"); err != nil {
		t.Fatal(err)
	}
	l := h.drv.links[0]
	h.skip()
	h.core.Frame(l, welcome("b", "a", 7, "gob"))
	same(t, "frames", l.kinds(), []string{"HELLO", "CLOSE protocol"})
	recs := h.recs()
	if len(recs) != 2 || recs[0] != "down peer=b conn=1 dir=out reason=protocol" {
		t.Errorf("records %q, want down protocol and a redial", recs)
	}
}

// TestSessionDirections checks REM-080 and REM-085: which frames each direction accepts and writes.
func TestSessionDirections(t *testing.T) {
	t.Run("writable", func(t *testing.T) {
		for k := range Kind(30) {
			if !k.Known() {
				continue
			}
			in := k == KindWelcome || k == KindReject || k == KindPong || k == KindClose
			out := k == KindHello || k == KindPing || k == KindClose || k.Data()
			if writable(dirIn, k) != in || writable(dirOut, k) != out {
				t.Errorf("%s: writable in %v, out %v; want %v, %v", k, writable(dirIn, k), writable(dirOut, k), in, out)
			}
		}
	})
	t.Run("write guard", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.acceptUp(1)
		defer func() {
			if recover() == nil {
				t.Error("writing MSG on an incoming session did not panic")
			}
		}()
		h.core.write(h.core.links[l], &Frame{Kind: KindMsg, Seq: 1})
	})
	t.Run("outgoing refuses", func(t *testing.T) {
		for _, f := range [][]byte{
			frame(Frame{Kind: KindMsg, Seq: 1, Sender: "lockstep://b/user/x#1", Recipient: "lockstep://a/user/y#2"}),
			ctrl(KindPing, ""),
			hello("b", "a", 1, "json", ""),
			welcome("b", "a", 7, "json"),
		} {
			h := newHarness(t, nil)
			l := h.dialUp()
			h.core.Frame(l, f)
			same(t, "frames", l.kinds(), []string{"HELLO", "CLOSE protocol"})
			same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=out reason=protocol"})
		}
	})
	t.Run("incoming refuses", func(t *testing.T) {
		for _, f := range [][]byte{ctrl(KindPong, ""), hello("b", "a", 1, "json", ""), welcome("b", "a", 7, "json"), ctrl(KindReject, "kind")} {
			h := newHarness(t, nil)
			l := h.acceptUp(3)
			h.core.Frame(l, f)
			same(t, "frames", l.kinds(), []string{"WELCOME", "CLOSE protocol"})
			same(t, "records", h.recs(), []string{"down peer=b conn=3 dir=in reason=protocol"})
		}
	})
	t.Run("incoming takes data", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.acceptUp(3)
		h.core.Frame(l, frame(Frame{Kind: KindMsg, Seq: 1, Sender: "lockstep://b/user/x#1", Recipient: "lockstep://a/user/y#2"}))
		same(t, "frames", l.kinds(), []string{"WELCOME"})
		same(t, "records", h.recs(), nil)
	})
	t.Run("one outgoing session per peer", func(t *testing.T) {
		h := newHarness(t, nil)
		for range 3 {
			if err := h.core.Connect("b"); err != nil {
				t.Fatal(err)
			}
		}
		if err := h.core.Connect("c"); err != nil {
			t.Fatal(err)
		}
		h.acceptUp(50) // an incoming session from b does not count as outgoing
		if err := h.core.Connect("b"); err != nil {
			t.Fatal(err)
		}
		var dialed []string
		for _, l := range h.drv.links {
			dialed = append(dialed, l.node)
		}
		same(t, "dials", dialed, []string{"b", "c"})
	})
}

// TestSessionLoopback runs two cores against each other through an in-memory network: both dial,
// both handshakes complete, and heartbeats keep all four sessions up for 30 heartbeats.
func TestSessionLoopback(t *testing.T) {
	n := &loopNet{t: t}
	a, ea := n.add("a", 1)
	b, eb := n.add("b", 2)
	if err := a.Connect("b"); err != nil {
		t.Fatal(err)
	}
	if err := b.Connect("a"); err != nil {
		t.Fatal(err)
	}
	n.pump()
	for range 30 {
		ea.Advance(time.Second)
		eb.Advance(time.Second)
		n.pump()
	}
	want := map[string][]string{
		"a": {"dial peer=b conn=1 attempt=1", "accept conn=2", "up peer=b conn=2 dir=in peer-inc=2 max-frame=1048576",
			"up peer=b conn=1 dir=out peer-inc=2 max-frame=1048576"},
		"b": {"dial peer=a conn=2 attempt=1", "accept conn=1", "up peer=a conn=1 dir=in peer-inc=1 max-frame=1048576",
			"up peer=a conn=2 dir=out peer-inc=1 max-frame=1048576"},
	}
	for _, x := range []struct {
		name string
		e    *enginetest.Engine
	}{{"a", ea}, {"b", eb}} {
		name, e := x.name, x.e
		var got []string
		for _, r := range e.Records()[1:] {
			got = append(got, line(r))
		}
		same(t, name+" records", got, want[name])
	}
	if n.pings == 0 || n.pings != n.pongs {
		t.Errorf("%d PINGs and %d PONGs, want equal and above 0", n.pings, n.pongs)
	}
}

// loopNet connects cores in memory. Frames written are queued and delivered by pump, so no core
// is called from inside another's lock.
type loopNet struct {
	t            *testing.T
	cores        map[string]*Core
	queue        []func()
	next         uint64
	pings, pongs int
}

type loopLink struct {
	n      *loopNet
	id     uint64
	other  *loopLink
	to     *Core // the core that receives what this link writes
	closed bool
}

func (l *loopLink) ID() uint64 { return l.id }

func (l *loopLink) Write(b []byte) bool {
	if l.closed {
		return true
	}
	switch Kind(b[5]) {
	case KindPing:
		l.n.pings++
	case KindPong:
		l.n.pongs++
	}
	o, to := l.other, l.to
	l.n.queue = append(l.n.queue, func() { to.Frame(o, b) })
	return true
}

func (l *loopLink) Close() {
	if l.closed {
		return
	}
	l.closed = true
	o, to := l.other, l.to
	l.n.queue = append(l.n.queue, func() { to.Closed(o, nil) })
}

type loopDriver struct {
	n    *loopNet
	from *Core
}

func (d *loopDriver) Dial(node, _ string) (Link, error) {
	to := d.n.cores[node]
	if to == nil {
		return nil, ErrUnknownNode
	}
	d.n.next++
	local := &loopLink{n: d.n, id: d.n.next, to: to}
	remote := &loopLink{n: d.n, id: d.n.next, to: d.from, other: local}
	local.other = remote
	d.n.queue = append(d.n.queue, func() { to.Accepted(remote) })
	return local, nil
}

func (n *loopNet) add(node string, inc uint32) (*Core, *enginetest.Engine) {
	if n.cores == nil {
		n.cores = map[string]*Core{}
	}
	e := enginetest.New(node, 1)
	drv := &loopDriver{n: n}
	cfg := testConfig(e, drv)
	cfg.Node, cfg.Incarnation = node, inc
	c, err := New(cfg)
	if err != nil {
		n.t.Fatal(err)
	}
	drv.from = c
	n.cores[node] = c
	c.Start()
	return c, e
}

func (n *loopNet) pump() {
	for len(n.queue) > 0 {
		f := n.queue[0]
		n.queue = n.queue[1:]
		f()
	}
}

// TestReplacedIncoming checks REM-085: a second HELLO from a peer closes its first incoming
// session with CLOSE replaced, and a rejected one replaces nothing.
func TestReplacedIncoming(t *testing.T) {
	h := newHarness(t, nil)
	old := h.acceptUp(1)

	bad := &fakeLink{t: t, id: 2}
	h.core.Accepted(bad)
	h.core.Frame(bad, hello("b", "a", 9, "gob", ""))
	if old.closed {
		t.Fatal("a rejected HELLO replaced the session")
	}
	h.skip()

	l := &fakeLink{t: t, id: 3}
	h.core.Accepted(l)
	h.core.Frame(l, hello("b", "a", 9, "json", ""))
	same(t, "old events", old.events, []string{"write WELCOME", "write CLOSE", "close"})
	same(t, "old frames", old.kinds(), []string{"WELCOME", "CLOSE replaced"})
	same(t, "new frames", l.kinds(), []string{"WELCOME"})
	same(t, "records", h.recs(), []string{
		"accept conn=3",
		"down peer=b conn=1 dir=in reason=replaced",
		"up peer=b conn=3 dir=in peer-inc=9 max-frame=8192",
	})
	h.core.Frame(old, ctrl(KindPing, "")) // gone: ignored
	h.core.Frame(l, ctrl(KindPing, ""))
	same(t, "new frames", l.kinds(), []string{"WELCOME", "PONG"})
	if len(old.frames) != 2 {
		t.Errorf("old frames %q", old.kinds())
	}
}

// TestHeartbeatPing checks REM-090's pings: an idle dialer pings at every tick, a busy one still
// pings when it received nothing for a heartbeat, and every PING gets one PONG.
func TestHeartbeatPing(t *testing.T) {
	t.Run("idle dialer", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.dialUp()
		for i := range 5 {
			h.eng.Advance(time.Second)
			if got := len(l.frames); got != 2+i {
				t.Fatalf("after %d ticks %d frames, want %d", i+1, got, 2+i)
			}
			h.core.Frame(l, ctrl(KindPong, ""))
		}
		same(t, "frames", l.kinds(), []string{"HELLO", "PING", "PING", "PING", "PING", "PING"})
		h.eng.Advance(30 * time.Second) // no more PONGs
		if !l.closed {
			t.Error("a dialer without PONGs was not closed idle")
		}
		same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=out reason=idle"})
	})
	t.Run("busy dialer", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.dialUp()
		s := h.core.links[l]
		// A data frame half a heartbeat before every tick, and each PONG half a heartbeat after
		// its PING: the session never wrote nothing for a heartbeat, and pings every other tick.
		pings, pong := 0, false
		for i := range 30 {
			h.eng.Advance(500 * time.Millisecond)
			if pong {
				h.core.Frame(l, ctrl(KindPong, ""))
				pong = false
			}
			h.core.mu.Lock()
			h.core.write(s, &Frame{Kind: KindMsg, Seq: uint64(i + 1)})
			h.core.mu.Unlock()
			n := len(l.frames)
			h.eng.Advance(500 * time.Millisecond)
			if len(l.frames) > n && l.frames[n].Kind == KindPing {
				pings++
				pong = true
			}
		}
		if l.closed {
			t.Fatalf("a busy dialer that got PONGs went down: %q", h.recs())
		}
		if pings != 15 {
			t.Errorf("%d PINGs in 30 heartbeats, want 15 (every other tick)", pings)
		}
	})
	t.Run("pong per ping", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.acceptUp(1)
		for range 3 {
			h.core.Frame(l, ctrl(KindPing, ""))
		}
		h.eng.Advance(5 * time.Second)
		same(t, "frames", l.kinds(), []string{"WELCOME", "PONG", "PONG", "PONG"})
	})
}

// TestHeartbeatIdle checks REM-090's idle close: at the first tick IdleTimeout after the last
// received frame, with CLOSE idle, for up incoming and outgoing sessions; no redial while the peer
// has nothing waiting; and that an accepted link must HELLO within DialTimeout.
func TestHeartbeatIdle(t *testing.T) {
	t.Run("incoming", func(t *testing.T) {
		h := newHarness(t, nil)
		h.eng.Advance(500 * time.Millisecond)
		l := h.acceptUp(1)
		h.eng.Advance(3 * time.Second)
		h.core.Frame(l, ctrl(KindPing, "")) // at 3.5 s
		h.eng.Advance(10 * time.Second)     // 13.5 s: the tick at 13 s is 9.5 s after the PING
		if l.closed {
			t.Fatal("closed before IdleTimeout")
		}
		h.eng.Advance(500 * time.Millisecond) // the tick at 14 s
		same(t, "events", l.events, []string{"write WELCOME", "write PONG", "write CLOSE", "close"})
		same(t, "frames", l.kinds(), []string{"WELCOME", "PONG", "CLOSE idle"})
		same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=in reason=idle"})
		if got := h.eng.Now().Sub(enginetest.Epoch); got != 14*time.Second {
			t.Errorf("now %s", got)
		}
	})
	t.Run("outgoing", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.dialUp()
		h.eng.Advance(10*time.Second - 1)
		if l.closed {
			t.Fatal("closed before IdleTimeout")
		}
		h.eng.Advance(1)
		if got := l.kinds(); got[len(got)-1] != "CLOSE idle" || !l.closed {
			t.Errorf("frames %q, closed %v; want CLOSE idle, closed", got, l.closed)
		}
		same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=out reason=idle"})
		if len(h.drv.links) != 1 || count(h.eng.Labels(), labelRedial) != 0 {
			t.Error("the core redialed a peer with nothing waiting")
		}
	})
	t.Run("no hello", func(t *testing.T) {
		h := newHarness(t, nil)
		l := &fakeLink{t: t, id: 8}
		h.core.Accepted(l)
		h.eng.Advance(5*time.Second - 1)
		if l.closed {
			t.Fatal("closed before DialTimeout")
		}
		h.eng.Advance(1)
		same(t, "events", l.events, []string{"close"})
		same(t, "records", h.recs(), []string{"accept conn=8", "down peer= conn=8 dir=in reason=dial-timeout"})
	})
	t.Run("hello in time", func(t *testing.T) {
		h := newHarness(t, nil)
		l := &fakeLink{t: t, id: 8}
		h.core.Accepted(l)
		h.eng.Advance(5*time.Second - 1)
		h.core.Frame(l, hello("b", "a", 9, "json", ""))
		h.skip()
		for range 5 { // past the dial timeout, with the dialer pinging
			h.eng.Advance(time.Second)
			h.core.Frame(l, ctrl(KindPing, ""))
		}
		if l.closed {
			t.Errorf("an accepted session went down: %q", h.recs())
		}
	})
}

// TestSessionWritable checks back-pressure: frames a link refuses are kept, nothing more is
// written on it, and Writable writes them in order; PING is skipped and CLOSE dropped while the
// link is blocked.
func TestSessionWritable(t *testing.T) {
	t.Run("pong", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.acceptUp(1)
		l.full = true
		h.core.Frame(l, ctrl(KindPing, ""))
		l.full = false
		h.core.Frame(l, ctrl(KindPing, "")) // the first PONG is still held: this one waits too
		same(t, "frames", l.kinds(), []string{"WELCOME"})
		h.core.Writable(l)
		same(t, "frames", l.kinds(), []string{"WELCOME", "PONG", "PONG"})
		h.core.Writable(l) // nothing held: nothing written
		same(t, "frames", l.kinds(), []string{"WELCOME", "PONG", "PONG"})
	})
	t.Run("blocked dialer", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.dialUp()
		l.full = true
		h.eng.Advance(time.Second) // PING refused and held
		tries := l.tries
		h.eng.Advance(3 * time.Second) // no PING while blocked
		if l.tries != tries {
			t.Errorf("%d Write calls on a blocked link", l.tries-tries)
		}
		l.full = false
		h.core.Writable(l)
		same(t, "frames", l.kinds(), []string{"HELLO", "PING"})
		l.full = true
		h.eng.Advance(time.Second) // PING held
		h.core.Frame(l, ctrl(KindPing, ""))
		same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=out reason=protocol"})
		same(t, "events", l.events, []string{"write HELLO", "write PING", "close"})
	})
}

// TestCloseReasons checks REM-095: a received CLOSE takes the session down closed without an
// answer; a sent CLOSE is written before the link closes; Closed is down io.
func TestCloseReasons(t *testing.T) {
	t.Run("received on outgoing", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.dialUp()
		h.core.Frame(l, ctrl(KindClose, "shutdown"))
		same(t, "events", l.events, []string{"write HELLO", "close"})
		same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=out reason=closed"})
	})
	t.Run("received on incoming", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.acceptUp(4)
		h.core.Frame(l, ctrl(KindClose, "replaced"))
		same(t, "events", l.events, []string{"write WELCOME", "close"})
		same(t, "records", h.recs(), []string{"down peer=b conn=4 dir=in reason=closed"})
	})
	t.Run("protocol", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.acceptUp(4)
		h.core.Frame(l, []byte{0, 0, 0, 20, 1})
		same(t, "events", l.events, []string{"write WELCOME", "write CLOSE", "close"})
		same(t, "frames", l.kinds(), []string{"WELCOME", "CLOSE protocol"})
		same(t, "records", h.recs(), []string{"down peer=b conn=4 dir=in reason=protocol"})
	})
	t.Run("io", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.acceptUp(4)
		h.core.Closed(l, errors.New("connection reset"))
		same(t, "events", l.events, []string{"write WELCOME", "close"})
		same(t, "records", h.recs(), []string{"down peer=b conn=4 dir=in reason=io"})
	})
	t.Run("connect while up adds no work", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.dialUp()
		if err := h.core.Connect("b"); err != nil {
			t.Fatal(err)
		}
		h.core.Frame(l, ctrl(KindClose, "shutdown"))
		same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=out reason=closed"})
		if len(h.drv.links) != 1 {
			t.Errorf("%d dials, want 1", len(h.drv.links))
		}
	})
	t.Run("redial at once after an up session", func(t *testing.T) {
		h := newHarness(t, nil)
		l := h.dialUp()
		h.core.peers["b"].want = true // work waits for b, as the send path's queue will
		h.core.Frame(l, ctrl(KindClose, "shutdown"))
		same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=out reason=closed", "dial peer=b conn=2 attempt=1"})
	})
}

// TestBackoffBase checks REM-100's b = min(RedialMax, RedialMin << (f-1)), saturating.
func TestBackoffBase(t *testing.T) {
	ms := time.Millisecond
	for _, tc := range []struct {
		f      int
		lo, hi time.Duration
		want   time.Duration
	}{
		{0, 100 * ms, 10 * time.Second, 100 * ms},
		{1, 100 * ms, 10 * time.Second, 100 * ms},
		{2, 100 * ms, 10 * time.Second, 200 * ms},
		{7, 100 * ms, 10 * time.Second, 6400 * ms},
		{8, 100 * ms, 10 * time.Second, 10 * time.Second},
		{1000, 100 * ms, 10 * time.Second, 10 * time.Second},
		{64, 1, math.MaxInt64, math.MaxInt64},
		{63, 1, math.MaxInt64, 1 << 62},
		{63, 2, math.MaxInt64, math.MaxInt64},
		{5, time.Second, time.Second, time.Second},
	} {
		if got := redialBase(tc.f, tc.lo, tc.hi); got != tc.want {
			t.Errorf("redialBase(%d, %d, %d) = %d, want %d", tc.f, tc.lo, tc.hi, got, tc.want)
		}
	}
}

// TestBackoffSeededDelays checks that the redial delays of a node that never reaches its peer
// follow REM-100's formula on the stream lockstep/<node>/<inc>/$remote, one draw per redial, in
// arming order, and that the failure count resets when a session comes up.
func TestBackoffSeededDelays(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Incarnation = 3 })
	r := enginetest.New("a", 7).Engine().Entropy.Rand("lockstep/a/3/$remote")
	next := func(f int) time.Duration {
		b := int64(redialBase(f, 100*time.Millisecond, 10*time.Second))
		return time.Duration(b/2 + r.Int64N(b-b/2+1))
	}
	h.drv.errs["b"] = errors.New("refused")
	if err := h.core.Connect("b"); err != nil {
		t.Fatal(err)
	}
	for f := 1; f <= 12; f++ {
		d := next(f)
		if d < redialBase(f, 100*time.Millisecond, 10*time.Second)/2 || d > redialBase(f, 100*time.Millisecond, 10*time.Second) {
			t.Fatalf("delay %s out of [b/2, b]", d)
		}
		same(t, fmt.Sprintf("failure %d", f), h.recs(), []string{fmt.Sprintf("redial peer=b failures=%d delay=%s", f, d)})
		h.eng.Advance(d - 1)
		same(t, "before the delay", h.recs(), nil)
		h.eng.Advance(1)
	}
	if n := count(h.eng.Labels(), labelRedial); n != 13 {
		t.Errorf("%d redial timers, want 13", n)
	}

	// The 14th dial succeeds and comes up; the next failure counts from 1 again.
	delete(h.drv.errs, "b")
	h.skip()
	next(13) // the 13th redial's draw, already recorded
	h.eng.Advance(10 * time.Second)
	if len(h.drv.links) != 1 {
		t.Fatalf("%d dials, want 1", len(h.drv.links))
	}
	l := h.drv.links[0]
	same(t, "records", h.recs(), []string{"dial peer=b conn=1 attempt=14"})
	h.core.Frame(l, welcome("b", "a", 7, "json"))
	h.core.peers["b"].want = true // work waits for b, as the send path's queue will
	h.drv.errs["b"] = errors.New("refused")
	h.skip()
	h.core.Closed(l, nil) // up session lost: dial at once, which fails: failure 1
	same(t, "records", h.recs(), []string{"down peer=b conn=1 dir=out reason=io", "redial peer=b failures=1 delay=" + next(1).String()})
}
