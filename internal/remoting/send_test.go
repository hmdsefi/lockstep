// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// The errors of testErrors, standing in for package remote's.
var (
	errFrameTooLarge = errors.New("remote: frame too large")
	errUnreachable   = errors.New("remote: node unreachable")
	errPeerRestarted = errors.New("remote: peer restarted")
	errCodeFive      = errors.New("an error with code 5")
)

// testErrors is the Errors of the tests.
type testErrors struct{}

func (testErrors) Remote(node string, code uint8, msg string) error {
	return fmt.Errorf("remote: %s: %s (code %d)", node, msg, code)
}

func (testErrors) Unreachable(node string) error {
	return fmt.Errorf("%w: %s", errUnreachable, node)
}

func (testErrors) PeerRestarted(node string) error {
	return fmt.Errorf("%w: %s (%w)", errPeerRestarted, node, lockstep.ErrTerminated)
}

func (testErrors) FrameTooLarge(size, limit int) error {
	return fmt.Errorf("%w: %d bytes > max %d", errFrameTooLarge, size, limit)
}

func (testErrors) Code(err error) uint8 {
	switch {
	case errors.Is(err, errCodeFive):
		return 5
	case errors.Is(err, lockstep.ErrNotFound):
		return 3
	case errors.Is(err, lockstep.ErrWrongType):
		return 4
	}
	return 0
}

// nopHost is a Host that drops what it gets.
type nopHost struct{}

func (nopHost) Inbound(engine.Envelope) {}

// recHost is a Host that keeps what it gets, then calls on if set.
type recHost struct {
	envs []engine.Envelope
	on   func(engine.Envelope)
}

func (h *recHost) Inbound(env engine.Envelope) {
	h.envs = append(h.envs, env)
	if h.on != nil {
		h.on(env)
	}
}

// The runtime payloads of the tests are hooks.RemotePayload values themselves: useTestHooks
// replaces package lockstep's Payload and NewPayload (codec links package lockstep).
var hooksOnce sync.Once

func useTestHooks() {
	hooksOnce.Do(func() {
		hooks.Payload = func(p any) (hooks.RemotePayload, bool) {
			rp, ok := p.(hooks.RemotePayload)
			return rp, ok
		}
		hooks.NewPayload = func(p hooks.RemotePayload) any { return p }
	})
}

type ping struct{ N int }

type blob struct{ B string }

type stray struct{ X int } // not registered

func testRegistry() *codec.Registry {
	r := codec.NewRegistry()
	codec.RegisterIn[ping](r, "test.ping")
	codec.RegisterIn[blob](r, "test.blob")
	return r
}

var (
	sender   = engine.Address{Node: "a", Path: "/user/x", UID: 1}
	target   = engine.Address{Node: "b", Path: "/user/y", UID: 2}
	askAddr  = engine.Address{Node: "a", Path: "/temp/1", UID: 3}
	askAddr2 = engine.Address{Node: "a", Path: "/temp/2", UID: 4}
	bAsk     = engine.Address{Node: "b", Path: "/temp/9", UID: 5} // an ask of node b
)

func msgEnv(v any) engine.Envelope {
	return engine.Envelope{From: sender, To: target, Payload: hooks.RemotePayload{Kind: hooks.RemoteMsg, Value: v}}
}

func askEnv(v any, ask engine.Address) engine.Envelope {
	return engine.Envelope{From: sender, To: target, Payload: hooks.RemotePayload{Kind: hooks.RemoteAsk, Value: v, Ask: ask}}
}

func toNode(env engine.Envelope, node string) engine.Envelope {
	env.To.Node = node
	return env
}

// sendHarness is a harness with a recording host and dead-letter function.
type sendHarness struct {
	*harness
	host *recHost
	dead []string // "<reason> <from> -> <to> <msg>"
	log  bytes.Buffer
}

func newSendHarness(t *testing.T, mod func(*Config)) *sendHarness {
	t.Helper()
	useTestHooks()
	sh := &sendHarness{host: &recHost{}}
	sh.harness = newHarness(t, func(c *Config) {
		c.Host = sh.host
		c.DeadLetter = func(from, to engine.Address, reason string, msg any) {
			sh.dead = append(sh.dead, fmt.Sprintf("%s %s -> %s %v", reason, addrText(from), addrText(to), msg))
		}
		c.Log = slog.New(slog.NewTextHandler(&sh.log, nil))
		c.Settings.Registry = testRegistry()
		if mod != nil {
			mod(c)
		}
	})
	return sh
}

// deliver calls Deliver and fails the test on an error.
func (h *sendHarness) deliver(env engine.Envelope) {
	h.t.Helper()
	if err := h.core.Deliver(env); err != nil {
		h.t.Fatalf("Deliver: %v", err)
	}
}

// link returns the last link dialed.
func (h *sendHarness) link() *fakeLink {
	h.t.Helper()
	if len(h.drv.links) == 0 {
		h.t.Fatal("nothing dialed")
	}
	return h.drv.links[len(h.drv.links)-1]
}

// up answers the last dial with a WELCOME from "b" of incarnation 7 and frame limit 8192.
func (h *sendHarness) up() *fakeLink {
	h.t.Helper()
	l := h.link()
	h.core.Frame(l, welcome("b", "a", 7, "json"))
	return l
}

// takeDead returns the dead letters so far and forgets them.
func (h *sendHarness) takeDead() []string {
	d := h.dead
	h.dead = nil
	return d
}

// data returns the data frames written on l, as "<KIND> seq=<n>".
func data(l *fakeLink) []string {
	var out []string
	for _, f := range l.frames {
		if f.Kind.Data() {
			out = append(out, f.Kind.String()+" seq="+strconv.FormatUint(f.Seq, 10))
		}
	}
	return out
}

// lastData returns the last data frame written on l.
func lastData(t *testing.T, l *fakeLink) Frame {
	t.Helper()
	for i := len(l.frames) - 1; i >= 0; i-- {
		if l.frames[i].Kind.Data() {
			return l.frames[i]
		}
	}
	t.Fatal("no data frame written")
	return Frame{}
}

// sendRec returns the send record of f.
func sendRec(f Frame) string {
	return fmt.Sprintf("send kind=%s seq=%d from=%s to=%s type=%s bytes=%d",
		f.Kind, f.Seq, f.Sender, f.Recipient, f.Type, len(frame(f)))
}

func jsonOf(t *testing.T, v any) []byte {
	t.Helper()
	b, err := codec.JSON{}.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestDeliverKinds checks the kind mapping of REM-120 and each data frame's fields: addresses in
// their text form, the wire name, the codec bytes, the ask address of an ASK, the code and message
// of a FAIL, and a send record per frame.
func TestDeliverKinds(t *testing.T) {
	h := newSendHarness(t, nil)
	h.deliver(msgEnv(ping{1}))
	l := h.up()
	h.skip()

	fail := fmt.Errorf("it broke: %w", errCodeFive)
	envs := []engine.Envelope{
		msgEnv(ping{2}),
		askEnv(ping{3}, askAddr),
		{From: sender, To: bAsk, Payload: hooks.RemotePayload{Kind: hooks.RemoteReply, Value: ping{4}}},
		{From: sender, To: bAsk, Payload: hooks.RemotePayload{Kind: hooks.RemoteFail, Err: fail}},
	}
	for _, env := range envs {
		h.deliver(env)
	}
	same(t, "data frames", data(l), []string{"MSG seq=1", "MSG seq=2", "ASK seq=3", "REPLY seq=4", "FAIL seq=5"})
	fs := l.frames[len(l.frames)-4:]
	var want []string
	for _, f := range fs {
		want = append(want, sendRec(f))
	}
	same(t, "records", h.recs(), want)

	check := func(f Frame, kind Kind, from, to, typ string, payload []byte) {
		t.Helper()
		if f.Kind != kind || f.Sender != from || f.Recipient != to || f.Type != typ || !bytes.Equal(f.Payload, payload) {
			t.Errorf("frame %s %q -> %q type %q payload %q, want %s %q -> %q type %q payload %q",
				f.Kind, f.Sender, f.Recipient, f.Type, f.Payload, kind, from, to, typ, payload)
		}
	}
	const a, b, ask, bask = "lockstep://a/user/x#1", "lockstep://b/user/y#2", "lockstep://a/temp/1#3", "lockstep://b/temp/9#5"
	check(fs[0], KindMsg, a, b, "test.ping", jsonOf(t, ping{2}))
	check(fs[1], KindAsk, a, b, "test.ping", AppendAsk(nil, ask, jsonOf(t, ping{3})))
	check(fs[2], KindReply, a, bask, "test.ping", jsonOf(t, ping{4}))
	check(fs[3], KindFail, a, bask, "", AppendFailure(nil, &Failure{Code: 5, Message: "it broke: an error with code 5"}))

	t.Run("mapping", func(t *testing.T) {
		for rk, k := range map[hooks.RemoteKind]Kind{
			hooks.RemoteMsg: KindMsg, hooks.RemoteAsk: KindAsk, hooks.RemoteReply: KindReply, hooks.RemoteFail: KindFail,
			hooks.RemoteWatch: KindWatch, hooks.RemoteUnwatch: KindUnwatch, hooks.RemoteTerminated: KindTerminated,
			0: 0, 8: 0, 255: 0,
		} {
			if got := frameKind(rk); got != k {
				t.Errorf("frameKind(%d) = %s, want %s", rk, got, k)
			}
		}
	})

	t.Run("refused envelopes", func(t *testing.T) {
		h := newSendHarness(t, nil)
		for _, tc := range []struct {
			env  engine.Envelope
			want string
		}{
			{engine.Envelope{From: sender, To: target, Payload: "text"}, "remoting: payload of type string is not a runtime payload"},
			{engine.Envelope{From: sender, To: target, Payload: hooks.RemotePayload{Kind: hooks.RemoteWatch}}, "remoting: WATCH envelope with watch number 0"},
			{engine.Envelope{From: sender, To: target, Payload: hooks.RemotePayload{Kind: 9}}, "remoting: unknown remote envelope kind 9"},
			{engine.Envelope{From: sender, To: bAsk, Payload: hooks.RemotePayload{Kind: hooks.RemoteFail}}, "remoting: FAIL envelope without an error"},
			{toNode(msgEnv(ping{1}), "a"), `remoting: cannot send to node "a"`},
			{toNode(msgEnv(ping{1}), "B!"), `remoting: cannot send to node "B!"`},
		} {
			if err := h.core.Deliver(tc.env); err == nil || err.Error() != tc.want {
				t.Errorf("Deliver: %v, want %s", err, tc.want)
			}
		}
		if len(h.drv.links) != 0 || len(h.core.peers) != 0 || len(h.recs()) != 0 {
			t.Errorf("refused envelopes dialed %d links, left %d peers", len(h.drv.links), len(h.core.peers))
		}
	})
}

// TestDeliverEncodeErrors checks REM-110: an encode error is returned as the registry returns it,
// and nothing is queued, dialed or recorded.
func TestDeliverEncodeErrors(t *testing.T) {
	h := newSendHarness(t, nil)
	err := h.core.Deliver(msgEnv(stray{1}))
	_, _, want := h.core.set.Registry.Encode(codec.JSON{}, stray{1})
	if err == nil || !errors.Is(err, codec.ErrNotRegistered) || err.Error() != want.Error() {
		t.Errorf("Deliver(stray) = %v, want %v", err, want)
	}
	err = h.core.Deliver(askEnv(stray{1}, askAddr))
	if !errors.Is(err, codec.ErrNotRegistered) {
		t.Errorf("Deliver(ask stray) = %v, want ErrNotRegistered", err)
	}
	if len(h.drv.links) != 0 || len(h.core.peers) != 0 || len(h.recs()) != 0 || len(h.takeDead()) != 0 {
		t.Error("an encode error queued, dialed, recorded or dead-lettered something")
	}
	h.eng.Advance(time.Second)
	if len(h.host.envs) != 0 {
		t.Errorf("an encode error failed an ask: %v", h.host.envs)
	}
}

// TestDeliverHeaders checks REM-055: valid headers ride on MSG, ASK and REPLY; headers breaking the
// limits are dropped, the frame is sent without them, and one warning is logged per session.
func TestDeliverHeaders(t *testing.T) {
	h := newSendHarness(t, nil)
	hs := []engine.Header{{Key: "traceparent", Value: "00-1"}, {Key: "k", Value: ""}}
	env := msgEnv(ping{1})
	env.Headers = hs
	h.deliver(env)
	l := h.up()
	if got := lastData(t, l).Headers; !slices.Equal(got, hs) {
		t.Errorf("headers %v, want %v", got, hs)
	}
	fail := engine.Envelope{From: sender, To: bAsk, Headers: hs, Payload: hooks.RemotePayload{Kind: hooks.RemoteFail, Err: errCodeFive}}
	h.deliver(fail)
	if got := lastData(t, l).Headers; got != nil {
		t.Errorf("FAIL carries headers %v", got)
	}

	tooMany := make([]engine.Header, MaxHeaders+1)
	for i := range tooMany {
		tooMany[i] = engine.Header{Key: "k" + strconv.Itoa(i), Value: "v"}
	}
	bad := [][]engine.Header{
		tooMany,
		{{Key: "", Value: "v"}},
		{{Key: strings.Repeat("k", MaxHeaderKey+1)}},
		{{Key: "k", Value: strings.Repeat("v", MaxHeaderValue+1)}},
	}
	for _, b := range bad {
		env := askEnv(ping{2}, askAddr)
		env.Headers = b
		h.deliver(env)
		f := lastData(t, l)
		if f.Kind != KindAsk || f.Headers != nil {
			t.Errorf("%s frame with %d headers, want an ASK without headers", f.Kind, len(f.Headers))
		}
	}
	if n := strings.Count(h.log.String(), "headers dropped"); n != 1 {
		t.Errorf("%d warnings on one session, want 1:\n%s", n, h.log.String())
	}

	// A new session warns again.
	h.core.Frame(l, ctrl(KindClose, CloseShutdown))
	env = msgEnv(ping{3})
	env.Headers = tooMany
	h.deliver(env)
	h.deliver(env)
	l2 := h.up()
	if l2 == l {
		t.Fatal("no new session")
	}
	same(t, "second session", data(l2), []string{"MSG seq=1", "MSG seq=2"})
	if n := strings.Count(h.log.String(), "headers dropped"); n != 2 {
		t.Errorf("%d warnings after two sessions, want 2:\n%s", n, h.log.String())
	}
	if !strings.Contains(h.log.String(), "conn=2") || !strings.Contains(h.log.String(), "headers=33") {
		t.Errorf("warning lacks conn or headers:\n%s", h.log.String())
	}
}

// TestDeliverConfig checks the config errors of the send path's inputs.
func TestDeliverConfig(t *testing.T) {
	for _, tc := range []struct {
		mod  func(*Config)
		want string
	}{
		{func(c *Config) { c.Host = nil }, "no host"},
		{func(c *Config) { c.Errors = nil }, "no errors"},
		{func(c *Config) { c.DeadLetter = nil }, "no dead-letter function"},
		{func(c *Config) { c.Settings.Registry = nil }, "no registry"},
		{func(c *Config) { c.Settings.OutboundQueue = 0 }, "OutboundQueue 0 is less than 1"},
	} {
		h := newSendHarness(t, nil)
		cfg := testConfig(h.eng, h.drv)
		tc.mod(&cfg)
		if _, err := New(cfg); err == nil || err.Error() != "remoting: invalid config: "+tc.want {
			t.Errorf("New: %v, want remoting: invalid config: %s", err, tc.want)
		}
	}
}

// TestDeliverIncarnation checks REM-130's stamp: a frame queued before any session came up carries
// incarnation 0, and one queued later carries the peer's last known incarnation.
func TestDeliverIncarnation(t *testing.T) {
	h := newSendHarness(t, nil)
	h.deliver(msgEnv(ping{1}))
	p := h.core.peers["b"]
	if len(p.queue) != 1 || p.queue[0].inc != 0 {
		t.Fatalf("queue %v, want one frame stamped 0", p.queue)
	}
	l := h.up()
	h.core.Frame(l, ctrl(KindClose, CloseShutdown))
	h.deliver(msgEnv(ping{2}))
	if len(p.queue) != 1 || p.queue[0].inc != 7 {
		t.Fatalf("queue %v, want one frame stamped 7", p.queue)
	}
}

// TestDeliverBackPressure checks that a frame the link refuses stays queued, counted against
// OutboundQueue and without a sequence number, that nothing more is written on the link, a PING
// included, until Writable, and that Writable writes the queue in order.
func TestDeliverBackPressure(t *testing.T) {
	h := newSendHarness(t, func(c *Config) { c.Settings.OutboundQueue = 2 })
	h.deliver(msgEnv(ping{1}))
	l := h.up()
	h.skip()
	l.full = true
	h.deliver(msgEnv(ping{2}))
	tries := l.tries
	h.deliver(msgEnv(ping{3}))
	h.deliver(askEnv(ping{4}, askAddr))
	h.eng.Advance(2 * time.Second) // ticks: no PING on a held link
	if l.tries != tries {
		t.Errorf("%d writes tried on a link that refused a frame, want 0", l.tries-tries)
	}
	same(t, "dead letters", h.takeDead(), []string{"queue-full lockstep://a/user/x#1 -> lockstep://b/user/y#2 {4}"})
	l.full = false
	h.core.Writable(l)
	same(t, "data frames", data(l), []string{"MSG seq=1", "MSG seq=2", "MSG seq=3"})
	recs := h.recs()
	if len(recs) != 2 || !strings.HasPrefix(recs[0], "send kind=MSG seq=2 ") || !strings.HasPrefix(recs[1], "send kind=MSG seq=3 ") {
		t.Errorf("records %q, want sends 2 and 3", recs)
	}
	if len(h.host.envs) != 1 || !errors.Is(remoteOf(t, h.host.envs[0]).Err, errUnreachable) {
		t.Errorf("host got %v, want the ask failed unreachable", h.host.envs)
	}
	h.deliver(msgEnv(ping{5}))
	same(t, "after Writable", data(l), []string{"MSG seq=1", "MSG seq=2", "MSG seq=3", "MSG seq=4"})
}

// TestDeliverDeadLetterReentry checks that the dead-letter function runs without the core's lock,
// so it may send again.
func TestDeliverDeadLetterReentry(t *testing.T) {
	var h *sendHarness
	var errs []error
	h = newSendHarness(t, func(c *Config) {
		c.Settings.OutboundQueue = 1
		c.DeadLetter = func(from, to engine.Address, reason string, msg any) {
			h.dead = append(h.dead, reason)
			if msg == (ping{2}) {
				errs = append(errs, h.core.Deliver(msgEnv(ping{3})))
			}
		}
	})
	h.deliver(msgEnv(ping{1}))
	h.deliver(msgEnv(ping{2}))
	same(t, "dead letters", h.takeDead(), []string{"queue-full", "queue-full"})
	if len(errs) != 1 || errs[0] != nil {
		t.Errorf("Deliver from the dead-letter function: %v", errs)
	}
}

// TestOversizedOwnLimit checks REM-115: a frame above MaxFrame returns the frame-size error, and
// nothing is queued; a frame of exactly MaxFrame is sent.
func TestOversizedOwnLimit(t *testing.T) {
	h := newSendHarness(t, func(c *Config) { c.Settings.MaxFrame = MinFrameLimit })
	sizeOf := func(v any) int {
		return len(frame(Frame{Kind: KindMsg, Sender: "lockstep://a/user/x#1", Recipient: "lockstep://b/user/y#2", Type: "test.blob", Payload: jsonOf(t, v)})) - prefixLen
	}
	big := blob{strings.Repeat("x", MinFrameLimit)}
	n := sizeOf(big)
	err := h.core.Deliver(msgEnv(big))
	want := fmt.Sprintf("remote: frame too large: %d bytes > max %d", n, MinFrameLimit)
	if err == nil || !errors.Is(err, errFrameTooLarge) || err.Error() != want {
		t.Errorf("Deliver = %v, want %s", err, want)
	}
	if err := h.core.Deliver(askEnv(big, askAddr)); !errors.Is(err, errFrameTooLarge) {
		t.Errorf("Deliver(ask) = %v, want the frame-size error", err)
	}
	if len(h.drv.links) != 0 || len(h.core.peers) != 0 || len(h.takeDead()) != 0 {
		t.Error("an oversized frame was queued or dead-lettered")
	}
	exact := blob{strings.Repeat("x", len(big.B)-(n-MinFrameLimit))}
	if s := sizeOf(exact); s != MinFrameLimit {
		t.Fatalf("test frame of %d bytes, want %d", s, MinFrameLimit)
	}
	h.deliver(msgEnv(exact))
	if q := h.core.peers["b"].queue; len(q) != 1 || q[0].size() != MinFrameLimit {
		t.Errorf("a frame of exactly MaxFrame was not queued")
	}
}

// TestOversizedForPeer checks the peer's limit (REM-140): a frame above the limit in the peer's
// WELCOME is a dead letter "too-large-for-peer" and takes no sequence number, and an ask among
// them fails with the frame-size error in a later lockstep/remote/fail callback.
func TestOversizedForPeer(t *testing.T) {
	h := newSendHarness(t, nil)
	big := blob{strings.Repeat("x", 9000)} // the peer announced 8192
	h.deliver(msgEnv(ping{1}))
	h.deliver(askEnv(big, askAddr)) // queued while dialing, dropped when written
	h.deliver(msgEnv(ping{2}))
	l := h.up()
	same(t, "data frames", data(l), []string{"MSG seq=1", "MSG seq=2"})
	h.deliver(msgEnv(big))
	h.deliver(msgEnv(ping{3}))
	same(t, "data frames", data(l), []string{"MSG seq=1", "MSG seq=2", "MSG seq=3"})
	dead := h.takeDead()
	if len(dead) != 2 || !strings.HasPrefix(dead[0], "too-large-for-peer lockstep://a/user/x#1 -> lockstep://b/user/y#2 ") ||
		!strings.HasPrefix(dead[1], "too-large-for-peer ") {
		t.Errorf("dead letters %q, want two too-large-for-peer", dead)
	}
	if len(h.host.envs) != 0 {
		t.Fatal("the ask failed inside Deliver")
	}
	if count(h.eng.Labels(), labelFail) != 1 {
		t.Errorf("labels %v, want one %s", h.eng.Labels(), labelFail)
	}
	h.eng.Advance(0)
	if len(h.host.envs) != 1 {
		t.Fatalf("host got %d envelopes, want 1", len(h.host.envs))
	}
	env := h.host.envs[0]
	p := remoteOf(t, env)
	n := len(frame(Frame{Kind: KindAsk, Sender: "lockstep://a/user/x#1", Recipient: "lockstep://b/user/y#2", Type: "test.blob",
		Payload: AppendAsk(nil, "lockstep://a/temp/1#3", jsonOf(t, big))})) - prefixLen
	want := fmt.Sprintf("remote: frame too large: %d bytes > max 8192", n)
	if env.From != target || env.To != askAddr || p.Kind != hooks.RemoteFail || !errors.Is(p.Err, errFrameTooLarge) || p.Err.Error() != want {
		t.Errorf("host got %v -> %v %v, want %v -> %v FAIL %s", env.From, env.To, p, target, askAddr, want)
	}
}

// TestOutboundQueueFull checks REM-140: with OutboundQueue frames queued, a frame is a dead letter
// "queue-full"; an ask among them fails with the unreachable error in a later callback; the
// queued frames are written once the session is up.
func TestOutboundQueueFull(t *testing.T) {
	h := newSendHarness(t, func(c *Config) { c.Settings.OutboundQueue = 2 })
	h.deliver(msgEnv(ping{1}))
	h.deliver(askEnv(ping{2}, askAddr))
	h.deliver(askEnv(ping{3}, askAddr2))
	h.deliver(msgEnv(ping{4}))
	same(t, "dead letters", h.takeDead(), []string{
		"queue-full lockstep://a/user/x#1 -> lockstep://b/user/y#2 {3}",
		"queue-full lockstep://a/user/x#1 -> lockstep://b/user/y#2 {4}",
	})
	if len(h.host.envs) != 0 {
		t.Fatal("the ask failed inside Deliver")
	}
	h.eng.Advance(0)
	if len(h.host.envs) != 1 {
		t.Fatalf("host got %d envelopes, want 1", len(h.host.envs))
	}
	env := h.host.envs[0]
	p := remoteOf(t, env)
	if env.From != target || env.To != askAddr2 || p.Kind != hooks.RemoteFail || !errors.Is(p.Err, errUnreachable) || p.Err.Error() != "remote: node unreachable: b" {
		t.Errorf("host got %v -> %v %v, want the second ask failed unreachable", env.From, env.To, p)
	}
	l := h.up()
	same(t, "data frames", data(l), []string{"MSG seq=1", "ASK seq=2"})
}

// TestUnknownNode checks REM-135: when the driver knows no address for the peer, the frame is a
// dead letter "unknown-node", an ask fails with the unreachable error later, and no peer is kept;
// frames queued for a redial that finds no address are dead letters too.
func TestUnknownNode(t *testing.T) {
	h := newSendHarness(t, nil)
	h.drv.errs["c"] = fmt.Errorf("no address for c: %w", ErrUnknownNode)
	h.deliver(toNode(msgEnv(ping{1}), "c"))
	h.deliver(toNode(askEnv(ping{2}, askAddr), "c"))
	same(t, "dead letters", h.takeDead(), []string{
		"unknown-node lockstep://a/user/x#1 -> lockstep://c/user/y#2 {1}",
		"unknown-node lockstep://a/user/x#1 -> lockstep://c/user/y#2 {2}",
	})
	if len(h.core.peers) != 0 {
		t.Errorf("%d peers kept for an unknown node", len(h.core.peers))
	}
	h.eng.Advance(0)
	if len(h.host.envs) != 1 || !errors.Is(remoteOf(t, h.host.envs[0]).Err, errUnreachable) || h.host.envs[0].To != askAddr {
		t.Errorf("host got %v, want the ask failed unreachable", h.host.envs)
	}

	t.Run("on redial", func(t *testing.T) {
		h := newSendHarness(t, nil)
		h.drv.errs["b"] = errors.New("connection refused")
		h.deliver(msgEnv(ping{1}))
		h.deliver(askEnv(ping{2}, askAddr))
		if h.core.peers["b"].redial == nil {
			t.Fatal("no redial armed with frames queued")
		}
		h.drv.errs["b"] = ErrUnknownNode
		h.eng.Advance(time.Second)
		same(t, "dead letters", h.takeDead(), []string{
			"unknown-node lockstep://a/user/x#1 -> lockstep://b/user/y#2 {1}",
			"unknown-node lockstep://a/user/x#1 -> lockstep://b/user/y#2 {2}",
		})
		if len(h.host.envs) != 1 || h.host.envs[0].To != askAddr {
			t.Errorf("host got %v, want the ask failed", h.host.envs)
		}
		// Nothing waits and b never handshook: the peer is forgotten, no redial left armed.
		if p := h.core.peers["b"]; p != nil {
			t.Errorf("peer kept: redial armed %v, %d frames queued", p.redial != nil, len(p.queue))
		}
	})
}

// TestSequencePerSession checks REM-130: data frames are numbered per session from 1, and a new
// session after a down starts again at 1.
func TestSequencePerSession(t *testing.T) {
	h := newSendHarness(t, nil)
	h.deliver(msgEnv(ping{1}))
	l := h.up()
	h.deliver(msgEnv(ping{2}))
	h.deliver(askEnv(ping{3}, askAddr))
	same(t, "first session", data(l), []string{"MSG seq=1", "MSG seq=2", "ASK seq=3"})
	h.core.Frame(l, ctrl(KindClose, CloseShutdown))
	h.deliver(msgEnv(ping{4}))
	h.deliver(msgEnv(ping{5}))
	l2 := h.up()
	if l2 == l {
		t.Fatal("no new session")
	}
	same(t, "second session", data(l2), []string{"MSG seq=1", "MSG seq=2"})
	h.deliver(msgEnv(ping{6}))
	same(t, "second session", data(l2), []string{"MSG seq=1", "MSG seq=2", "MSG seq=3"})
	if got := lastData(t, l2).Payload; !bytes.Equal(got, jsonOf(t, ping{6})) {
		t.Errorf("payload %s, want ping 6", got)
	}
	// An incoming session writes no data frames.
	in := h.acceptUp(50)
	h.deliver(msgEnv(ping{7}))
	if len(data(in)) != 0 {
		t.Errorf("data frames written on an incoming session: %v", data(in))
	}
}

// TestQueuedFramesAfterUp checks that the frames delivered while the session dials are written
// in order after HELLO and the up record, and that a session that goes down keeps the unwritten
// frames for the redial.
func TestQueuedFramesAfterUp(t *testing.T) {
	h := newSendHarness(t, nil)
	h.deliver(msgEnv(ping{1}))
	h.deliver(askEnv(ping{2}, askAddr))
	h.deliver(msgEnv(ping{3}))
	l := h.link()
	same(t, "before WELCOME", l.kinds(), []string{"HELLO"})
	h.core.Frame(l, welcome("b", "a", 7, "json"))
	same(t, "after WELCOME", l.kinds(), []string{"HELLO", "MSG", "ASK", "MSG"})
	recs := h.recs()
	var kinds []string
	for _, r := range recs {
		kinds = append(kinds, strings.SplitN(r, " ", 3)[0]+" "+strings.SplitN(r, " ", 3)[1])
	}
	same(t, "records", kinds, []string{"dial peer=b", "up peer=b", "send kind=MSG", "send kind=ASK", "send kind=MSG"})
	same(t, "data frames", data(l), []string{"MSG seq=1", "ASK seq=2", "MSG seq=3"})

	t.Run("kept across a failed dial", func(t *testing.T) {
		h := newSendHarness(t, nil)
		h.deliver(msgEnv(ping{1}))
		h.deliver(msgEnv(ping{2}))
		l := h.link()
		h.core.Frame(l, ctrl(KindReject, RejectCodec))
		if p := h.core.peers["b"]; len(p.queue) != 2 || p.redial == nil {
			t.Fatalf("after a reject: %d frames queued, redial armed %v", len(p.queue), p.redial != nil)
		}
		h.eng.Advance(time.Second)
		l2 := h.up()
		if l2 == l {
			t.Fatal("no redial")
		}
		same(t, "after the redial", data(l2), []string{"MSG seq=1", "MSG seq=2"})
		if len(h.takeDead()) != 0 {
			t.Error("dead letters from a failed dial")
		}
	})
}

// remoteOf returns env's payload, which the test hooks build as a hooks.RemotePayload.
func remoteOf(t *testing.T, env engine.Envelope) hooks.RemotePayload {
	t.Helper()
	p, ok := env.Payload.(hooks.RemotePayload)
	if !ok {
		t.Fatalf("payload %T, want hooks.RemotePayload", env.Payload)
	}
	return p
}

// TestUnknownNodeOnImmediateRedial checks that when an up session goes down with frames queued and
// the immediate re-dial finds no address, the queue is dead-lettered "unknown-node", its asks fail
// later, and no redial is armed; the peer, which handshook, is kept.
func TestUnknownNodeOnImmediateRedial(t *testing.T) {
	h := newSendHarness(t, nil)
	h.deliver(msgEnv(ping{1}))
	l := h.up()
	l.full = true
	h.deliver(msgEnv(ping{2}))
	h.deliver(askEnv(ping{3}, askAddr))
	h.drv.errs["b"] = ErrUnknownNode
	h.core.Frame(l, ctrl(KindClose, CloseShutdown))
	same(t, "dead letters", h.takeDead(), []string{
		"unknown-node lockstep://a/user/x#1 -> lockstep://b/user/y#2 {2}",
		"unknown-node lockstep://a/user/x#1 -> lockstep://b/user/y#2 {3}",
	})
	p := h.core.peers["b"]
	if p == nil || p.redial != nil || len(p.queue) != 0 || p.inc != 7 {
		t.Fatalf("peer %+v, want it kept with incarnation 7, nothing queued and no redial", p)
	}
	h.eng.Advance(0)
	if len(h.host.envs) != 1 || h.host.envs[0].To != askAddr || !errors.Is(remoteOf(t, h.host.envs[0]).Err, errUnreachable) {
		t.Errorf("host got %v, want the ask failed unreachable", h.host.envs)
	}
}

// TestDeliverWritableOrder checks that Writable writes the control frames the link refused before
// the data frames queued meanwhile.
func TestDeliverWritableOrder(t *testing.T) {
	h := newSendHarness(t, nil)
	h.deliver(msgEnv(ping{1}))
	l := h.up()
	l.full = true
	h.eng.Advance(time.Second) // the tick's PING is refused and held
	h.deliver(msgEnv(ping{2}))
	h.deliver(msgEnv(ping{3}))
	p := h.core.peers["b"]
	if s := p.out; len(s.blocked) != 1 || s.stalled || len(p.queue) != 2 {
		t.Fatalf("blocked %d, stalled %v, queued %d; want a held PING and two queued frames", len(s.blocked), s.stalled, len(p.queue))
	}
	l.full = false
	n := len(l.events)
	h.core.Writable(l)
	same(t, "writes after Writable", l.events[n:], []string{"write PING", "write MSG", "write MSG"})
	same(t, "data frames", data(l), []string{"MSG seq=1", "MSG seq=2", "MSG seq=3"})
}

// TestDeliverFailTimers checks that the core keeps the Cancel of each lockstep/remote/fail timer
// until it fires, that a stopped one fails nothing, and that the fail callback may call Deliver.
func TestDeliverFailTimers(t *testing.T) {
	h := newSendHarness(t, func(c *Config) { c.Settings.OutboundQueue = 1 })
	var errs []error
	h.host.on = func(engine.Envelope) { errs = append(errs, h.core.Deliver(msgEnv(ping{9}))) }
	h.deliver(msgEnv(ping{1}))
	h.deliver(askEnv(ping{2}, askAddr))
	if len(h.core.failTimers) != 1 {
		t.Fatalf("%d fail timers kept, want 1", len(h.core.failTimers))
	}
	h.eng.Advance(0)
	if len(h.core.failTimers) != 0 || len(h.host.envs) != 1 || len(errs) != 1 || errs[0] != nil {
		t.Fatalf("after the callback: %d timers kept, host got %d, Deliver from the callback %v",
			len(h.core.failTimers), len(h.host.envs), errs)
	}
	same(t, "dead letters", h.takeDead(), []string{
		"queue-full lockstep://a/user/x#1 -> lockstep://b/user/y#2 {2}",
		"queue-full lockstep://a/user/x#1 -> lockstep://b/user/y#2 {9}",
	})

	h.deliver(askEnv(ping{3}, askAddr2))
	if len(h.core.failTimers) != 1 || !h.core.failTimers[0].cancel() {
		t.Fatal("no fail timer to stop")
	}
	h.eng.Advance(time.Second)
	if len(h.host.envs) != 1 {
		t.Errorf("a stopped fail timer failed %d asks", len(h.host.envs)-1)
	}
}

// TestDeliverBuildsAskErrorsOnlyForAsks checks that the unreachable and frame-size errors are
// built only when an ask is among the dropped frames.
func TestDeliverBuildsAskErrorsOnlyForAsks(t *testing.T) {
	ce := &countingErrors{}
	h := newSendHarness(t, func(c *Config) {
		c.Errors = ce
		c.Settings.OutboundQueue = 1
	})
	h.deliver(msgEnv(ping{1}))
	h.deliver(msgEnv(ping{2})) // queue-full, no ask
	h.up()
	h.deliver(msgEnv(blob{strings.Repeat("x", 9000)})) // too large for the peer, no ask
	if ce.built != 0 {
		t.Errorf("%d errors built for dropped frames without an ask", ce.built)
	}
	h.deliver(askEnv(blob{strings.Repeat("x", 9000)}, askAddr))
	if ce.built != 1 {
		t.Errorf("%d errors built for one dropped ask, want 1", ce.built)
	}
}

// countingErrors counts the Unreachable and FrameTooLarge errors it builds.
type countingErrors struct {
	testErrors
	built int
}

func (e *countingErrors) Unreachable(node string) error {
	e.built++
	return e.testErrors.Unreachable(node)
}

func (e *countingErrors) FrameTooLarge(size, limit int) error {
	e.built++
	return e.testErrors.FrameTooLarge(size, limit)
}

// TestDeliverForgetsIdlePeers checks that a peer that never handshook is forgotten once nothing is
// queued or waiting for it, so sends to many node names do not grow the core's peers.
func TestDeliverForgetsIdlePeers(t *testing.T) {
	h := newSendHarness(t, nil)
	for i := range 50 {
		node := "n" + strconv.Itoa(i)
		h.drv.errs[node] = ErrUnknownNode
		h.deliver(toNode(msgEnv(ping{i}), node))
	}
	if len(h.core.peers) != 0 {
		t.Errorf("%d peers kept for unknown nodes", len(h.core.peers))
	}
	// A dial that is rejected with nothing else waiting leaves no peer behind either.
	h.deliver(msgEnv(ping{1}))
	h.drv.errs["b"] = ErrUnknownNode
	h.core.Frame(h.link(), ctrl(KindReject, RejectCodec))
	h.eng.Advance(time.Minute)
	if len(h.core.peers) != 0 {
		t.Errorf("%d peers kept after the queue was dead-lettered", len(h.core.peers))
	}
}

// lockCheck is a slog handler that counts the records logged while the core's lock is held.
type lockCheck struct {
	core        *Core
	n, underMut int
}

func (*lockCheck) Enabled(context.Context, slog.Level) bool { return true }

func (h *lockCheck) Handle(context.Context, slog.Record) error {
	h.n++
	if !h.core.mu.TryLock() {
		h.underMut++
		return nil
	}
	h.core.mu.Unlock()
	return nil
}

func (h *lockCheck) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *lockCheck) WithGroup(string) slog.Handler      { return h }

// TestDeliverLogsWithoutLock checks that the core logs only after releasing its lock: the
// dropped-headers warning, a protocol error, a failed dial, a closed link and a WELCOME from the
// wrong node.
func TestDeliverLogsWithoutLock(t *testing.T) {
	lc := &lockCheck{}
	h := newSendHarness(t, func(c *Config) { c.Log = slog.New(lc) })
	lc.core = h.core

	env := msgEnv(ping{1})
	env.Headers = []engine.Header{{Key: ""}}
	h.deliver(env)
	l := h.up()
	h.core.Frame(l, []byte{0, 0, 0, 1, 9})

	h.drv.errs["c"] = errors.New("refused")
	h.deliver(toNode(msgEnv(ping{2}), "c"))

	in := h.acceptUp(60)
	h.core.Closed(in, errors.New("reset"))

	h.deliver(msgEnv(ping{3}))
	h.core.Frame(h.link(), welcome("x", "a", 7, "json"))

	if lc.n != 5 || lc.underMut != 0 {
		t.Errorf("%d log records, %d of them under the lock; want 5, none under it", lc.n, lc.underMut)
	}
}
