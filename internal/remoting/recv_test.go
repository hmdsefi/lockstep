// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// The hooks package lockstep set at init (codec links it), before any test replaced them.
var (
	realPayload    = hooks.Payload
	realNewPayload = hooks.NewPayload
	realAssignable = hooks.Assignable
	realPending    = hooks.Pending
	realLookupPath = hooks.LookupPath
)

// fakeSys stands in for a *lockstep.System in the runtime hooks that the receive path calls: it
// has the live actors of node "a", by path, and its pending asks with their R.
type fakeSys struct {
	actors map[string]fakeActor
	asks   map[engine.Address]reflect.Type
}

type fakeActor struct {
	uid uint64
	typ reflect.Type // the actor's M
}

func newFakeSys() *fakeSys {
	return &fakeSys{
		actors: map[string]fakeActor{"/user/y": {uid: 2, typ: reflect.TypeFor[ping]()}},
		asks: map[engine.Address]reflect.Type{
			askAddr:  reflect.TypeFor[ping](),
			askAddr2: reflect.TypeFor[any](),
		},
	}
}

// takes reports whether v is a t, as the mailbox's type assertion decides it.
func takes(v any, t reflect.Type) bool {
	if v == nil {
		return t.Kind() == reflect.Interface
	}
	if t.Kind() == reflect.Interface {
		return reflect.TypeOf(v).Implements(t)
	}
	return reflect.TypeOf(v) == t
}

var recvHooksOnce sync.Once

// useRecvHooks installs the test hooks: those of useTestHooks, plus Assignable, Pending and
// LookupPath answering from a *fakeSys, and package lockstep's for any other system.
func useRecvHooks() {
	useTestHooks()
	recvHooksOnce.Do(func() {
		hooks.Assignable = func(sys any, to engine.Address, v any) bool {
			s, ok := sys.(*fakeSys)
			if !ok {
				return realAssignable(sys, to, v)
			}
			if t, ok := s.asks[to]; ok {
				return takes(v, t)
			}
			a, ok := s.actors[to.Path]
			return ok && to.Node == "a" && (to.UID == 0 || to.UID == a.uid) && takes(v, a.typ)
		}
		hooks.Pending = func(sys any, ask engine.Address) bool {
			s, ok := sys.(*fakeSys)
			if !ok {
				return realPending(sys, ask)
			}
			_, ok = s.asks[ask]
			return ok
		}
		hooks.LookupPath = func(sys any, path, typeKey string) (engine.Address, error) {
			s, ok := sys.(*fakeSys)
			if !ok {
				return realLookupPath(sys, path, typeKey)
			}
			if a, ok := s.actors[path]; ok {
				return engine.Address{Node: "a", Path: path, UID: a.uid}, nil
			}
			return engine.Address{}, errors.New("not found")
		}
	})
}

// useRealPayloads makes hooks.Payload and hooks.NewPayload package lockstep's until t ends.
func useRealPayloads(t *testing.T) {
	useRecvHooks()
	p, np := hooks.Payload, hooks.NewPayload
	hooks.Payload, hooks.NewPayload = realPayload, realNewPayload
	t.Cleanup(func() { hooks.Payload, hooks.NewPayload = p, np })
}

// Addresses of the receive tests: the peer is node "b", this core node "a".
var (
	bX = engine.Address{Node: "b", Path: "/user/x", UID: 1} // a sender on b
	aY = engine.Address{Node: "a", Path: "/user/y", UID: 2} // the live actor of fakeSys, of M ping
)

// evHost is a Host and Failer that keep their calls in one list, and check that the core does not
// hold its lock while it calls them.
type evHost struct {
	t      *testing.T
	core   **Core
	envs   []engine.Envelope
	events []string // "inbound <envelope>" and "fail <error>", in call order
}

func (h *evHost) unlocked(what string) {
	if !(*h.core).mu.TryLock() {
		h.t.Errorf("%s called with the core's lock held", what)
		return
	}
	(*h.core).mu.Unlock()
}

func (h *evHost) Inbound(env engine.Envelope) {
	h.unlocked("Host.Inbound")
	h.envs = append(h.envs, env)
	h.events = append(h.events, "inbound "+envText(env))
}

func (h *evHost) Fail(err error) {
	h.unlocked("Failer.Fail")
	h.events = append(h.events, "fail "+err.Error())
}

// envText describes an envelope whose payload is a test hooks.RemotePayload:
// "<seq> <KIND> <from> -> <to> <value or error>", with " ask=<ask>" for an ASK.
func envText(env engine.Envelope) string {
	p, ok := env.Payload.(hooks.RemotePayload)
	if !ok {
		return fmt.Sprintf("%d %T", env.Seq, env.Payload)
	}
	s := fmt.Sprintf("%d %s %s -> %s ", env.Seq, frameKind(p.Kind), addrText(env.From), addrText(env.To))
	switch p.Kind {
	case hooks.RemoteFail:
		s += p.Err.Error()
	case hooks.RemoteAsk:
		s += fmt.Sprintf("%v ask=%s", p.Value, addrText(p.Ask))
	default:
		s += fmt.Sprintf("%v", p.Value)
	}
	return s
}

// recvHarness is a core on node "a" with an up incoming session from "b" (incarnation 9, conn 3).
type recvHarness struct {
	*sendHarness
	ev   *evHost
	sys  *fakeSys
	in   *fakeLink
	core *Core
}

func newRecvHarness(t *testing.T) *recvHarness {
	t.Helper()
	useRecvHooks()
	h := &recvHarness{sys: newFakeSys()}
	h.ev = &evHost{t: t, core: &h.core}
	h.sendHarness = newSendHarness(t, func(c *Config) {
		c.System = h.sys
		c.Host = h.ev
		c.Failer = h.ev
	})
	h.core = h.sendHarness.core
	h.in = h.acceptUp(3)
	return h
}

// send hands l a data frame of kind k numbered seq.
func (h *recvHarness) send(l *fakeLink, k Kind, seq uint64, from, to, typ string, payload []byte) {
	h.core.Frame(l, frame(Frame{Kind: k, Seq: seq, Sender: from, Recipient: to, Type: typ, Payload: payload}))
}

// msg hands the incoming session a MSG numbered seq from bX to to, of type test.ping or
// test.blob, with v's JSON.
func (h *recvHarness) msg(seq uint64, to string, v any) {
	h.t.Helper()
	h.send(h.in, KindMsg, seq, bX.String(), to, typeName(h.t, v), jsonOf(h.t, v))
}

func typeName(t *testing.T, v any) string {
	t.Helper()
	name, err := testRegistry().NameOf(v)
	if err != nil {
		t.Fatal(err)
	}
	return name
}

// takeEvents returns the host and Failer calls so far and forgets them.
func (h *recvHarness) takeEvents() []string {
	e := h.ev.events
	h.ev.events = nil
	return e
}

// TestSequenceRule checks REM-200 on one incoming session: the next number is accepted, a number
// above it records the gap and is accepted, and one below it is discarded late. Envelope.Seq
// counts the envelopes handed to the host per peer incarnation (REM-160), across a replaced
// session, and starts again for a new incarnation; the watch and lookup kinds take their numbers.
func TestSequenceRule(t *testing.T) {
	h := newRecvHarness(t)
	to := aY.String()
	h.msg(1, to, ping{1})
	h.msg(2, to, ping{2})
	h.msg(5, to, ping{5})
	h.msg(4, to, ping{4})
	h.msg(5, to, ping{55})
	h.msg(2, to, ping{22})
	h.send(h.in, KindWatch, 6, bX.String(), to, "", nil)
	h.send(h.in, KindLookup, 7, bX.String(), "lockstep://a/user/y#0", "", nil)
	h.core.Frame(h.in, frame(Frame{Kind: KindMsg, Seq: 8, Sender: bX.String(), Recipient: to, Type: "test.ping",
		Payload: jsonOf(t, ping{8}), Headers: []engine.Header{{Key: "k", Value: "v"}}}))
	same(t, "records", h.recs(), []string{
		"gap peer=b conn=3 want=3 got=5",
		"discard peer=b conn=3 kind=MSG seq=4 reason=late",
		"discard peer=b conn=3 kind=MSG seq=5 reason=late",
		"discard peer=b conn=3 kind=MSG seq=2 reason=late",
	})
	same(t, "host", h.takeEvents(), []string{
		"inbound 1 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {1}",
		"inbound 2 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {2}",
		"inbound 3 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {5}",
		"inbound 4 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {8}",
	})
	if got := h.ev.envs[3].Headers; !reflect.DeepEqual(got, []engine.Header{{Key: "k", Value: "v"}}) {
		t.Errorf("headers = %v, want [{k v}]", got)
	}

	// A HELLO of the same incarnation replaces the session: numbering starts at 1 on the new
	// session, and Envelope.Seq goes on.
	l2 := h.acceptUp(4)
	h.msg(1, to, ping{9}) // on the old link: discarded, see TestClosedSession
	h.recs()
	h.send(l2, KindMsg, 1, bX.String(), to, "test.ping", jsonOf(t, ping{10}))
	// A new incarnation: Envelope.Seq starts again.
	l3 := &fakeLink{t: t, id: 5}
	h.core.Accepted(l3)
	h.core.Frame(l3, hello("b", "a", 10, "json", ""))
	h.send(l3, KindMsg, 1, bX.String(), to, "test.ping", jsonOf(t, ping{11}))
	h.send(l3, KindMsg, 2, bX.String(), to, "test.ping", jsonOf(t, ping{12}))
	same(t, "host after replacement", h.takeEvents(), []string{
		"inbound 5 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {10}",
		"inbound 1 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {11}",
		"inbound 2 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {12}",
	})
}

// TestSequenceRuleLast checks that a frame numbered math.MaxUint64 is the last one a session
// takes: any data frame after it closes the session with CLOSE protocol.
func TestSequenceRuleLast(t *testing.T) {
	h := newRecvHarness(t)
	h.msg(math.MaxUint64, aY.String(), ping{1})
	h.msg(1, aY.String(), ping{2})
	same(t, "records", h.recs(), []string{
		"gap peer=b conn=3 want=1 got=18446744073709551615",
		"down peer=b conn=3 dir=in reason=protocol",
	})
	same(t, "frames", h.in.kinds(), []string{"WELCOME", "CLOSE protocol"})
	same(t, "host", h.takeEvents(), []string{"inbound 1 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {1}"})
}

// TestMisrouted checks REM-210 and REM-220: a recipient on another node, or one that is not an
// address, and an ASK whose ask address is on this node are discarded misrouted, without a
// failure; the frame still takes its sequence number.
func TestMisrouted(t *testing.T) {
	h := newRecvHarness(t)
	ask := func(askText string) []byte { return AppendAsk(nil, askText, jsonOf(t, ping{1})) }
	h.msg(1, "lockstep://c/user/y#2", ping{1})
	h.msg(2, "", ping{2})
	h.msg(3, "not an address", ping{3})
	h.send(h.in, KindAsk, 4, bX.String(), aY.String(), "test.ping", ask(addrText(askAddr)))
	h.send(h.in, KindReply, 5, bX.String(), addrText(bAsk), "test.ping", jsonOf(t, ping{5}))
	h.send(h.in, KindFail, 6, bX.String(), "lockstep://c/temp/1#3", "", AppendFailure(nil, &Failure{Code: 1, Message: "x"}))
	h.send(h.in, KindAsk, 7, bX.String(), aY.String(), "test.ping", ask(addrText(bAsk)))
	same(t, "records", h.recs(), []string{
		"discard peer=b conn=3 kind=MSG seq=1 reason=misrouted",
		"discard peer=b conn=3 kind=MSG seq=2 reason=misrouted",
		"discard peer=b conn=3 kind=MSG seq=3 reason=misrouted",
		"discard peer=b conn=3 kind=ASK seq=4 reason=misrouted",
		"discard peer=b conn=3 kind=REPLY seq=5 reason=misrouted",
		"discard peer=b conn=3 kind=FAIL seq=6 reason=misrouted",
	})
	same(t, "host", h.takeEvents(), []string{
		"inbound 1 ASK lockstep://b/user/x#1 -> lockstep://a/user/y#2 {1} ask=lockstep://b/temp/9#5",
	})
	same(t, "dead letters", h.takeDead(), nil)
}

// TestDecodeAndWrongType checks REM-230 and REM-240: a frame whose sender, ask address or codec
// bytes do not decode is discarded decode and fails the seed, and a REPLY of that kind for a
// pending ask fails the ask; a MSG or ASK whose live recipient does not take its value is
// discarded wrong-type, is a dead letter wrong-type and fails the seed, and the ASK fails back to
// its asker. A recipient that is not a live actor gets the frame, so that the runtime handles it.
// The host and the Failer run without the core's lock, in frame order. Peer text in the errors is
// quoted and cut to 256 bytes.
func TestDecodeAndWrongType(t *testing.T) {
	h := newRecvHarness(t)
	to := aY.String()
	ping1 := jsonOf(t, ping{1})
	h.send(h.in, KindMsg, 1, bX.String(), to, "test.nope", ping1)
	h.send(h.in, KindMsg, 2, "bogus", to, "test.ping", ping1)
	h.send(h.in, KindAsk, 3, bX.String(), to, "test.ping", AppendAsk(nil, "lockstep://b/temp", ping1))
	h.send(h.in, KindAsk, 4, bX.String(), to, "test.ping", AppendAsk(nil, "", ping1))
	h.send(h.in, KindReply, 5, bX.String(), addrText(askAddr), "test.ping", []byte("{"))
	h.msg(6, to, ping{6})
	h.msg(7, to, blob{"x"})
	h.send(h.in, KindAsk, 8, bX.String(), to, "test.blob", AppendAsk(nil, addrText(bAsk), jsonOf(t, blob{"y"})))
	h.msg(9, "lockstep://a/user/gone#7", blob{"z"}) // no actor at the path
	h.msg(10, "lockstep://a/user/y#9", blob{"w"})   // an earlier incarnation of /user/y
	long := strings.Repeat("é", 200)                // 400 bytes
	h.send(h.in, KindMsg, 11, bX.String(), to, long, ping1)
	h.send(h.in, KindMsg, 12, long, to, "test.ping", ping1)
	h.send(h.in, KindReply, 13, bX.String(), "lockstep://a/temp/77#99", "test.ping", []byte("{")) // not pending
	same(t, "records", h.recs(), []string{
		"discard peer=b conn=3 kind=MSG seq=1 reason=decode",
		"discard peer=b conn=3 kind=MSG seq=2 reason=decode",
		"discard peer=b conn=3 kind=ASK seq=3 reason=decode",
		"discard peer=b conn=3 kind=ASK seq=4 reason=decode",
		"discard peer=b conn=3 kind=REPLY seq=5 reason=decode",
		"discard peer=b conn=3 kind=MSG seq=7 reason=wrong-type",
		"discard peer=b conn=3 kind=ASK seq=8 reason=wrong-type",
		"dial peer=b conn=1 attempt=1", // the ASK's failure goes back to b
		"discard peer=b conn=3 kind=MSG seq=11 reason=decode",
		"discard peer=b conn=3 kind=MSG seq=12 reason=decode",
		"discard peer=b conn=3 kind=REPLY seq=13 reason=decode",
	})
	_, jsonErr := testRegistry().Decode(codec.JSON{}, "test.ping", []byte("{"))
	replyErr := `lockstep: remote: a: REPLY from b: "test.ping": ` + jsonErr.Error()
	cut := strconv.Quote(strings.Repeat("é", 128)) + "..."
	same(t, "host and failer", h.takeEvents(), []string{
		`fail lockstep: remote: a: MSG from b: "test.nope": codec: type not registered: wire name "test.nope"`,
		`fail lockstep: remote: a: MSG from b: "test.ping": sender "bogus" is not an address`,
		`fail lockstep: remote: a: ASK from b: "test.ping": ask address "lockstep://b/temp" is not an address`,
		`fail lockstep: remote: a: ASK from b: "test.ping": ask address is empty`,
		"inbound 1 FAIL lockstep://b/user/x#1 -> lockstep://a/temp/1#3 " + replyErr,
		"fail " + replyErr,
		"inbound 2 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {6}",
		`fail lockstep: remote: a: MSG from b: "test.blob": lockstep: wrong message type: the actor at "lockstep://a/user/y#2" does not take it`,
		`fail lockstep: remote: a: ASK from b: "test.blob": lockstep: wrong message type: the actor at "lockstep://a/user/y#2" does not take it`,
		"inbound 3 MSG lockstep://b/user/x#1 -> lockstep://a/user/gone#7 {z}",
		"inbound 4 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#9 {w}",
		"fail lockstep: remote: a: MSG from b: " + cut + ": codec: type not registered: wire name " + cut,
		`fail lockstep: remote: a: MSG from b: "test.ping": sender ` + cut + " is not an address",
		`fail lockstep: remote: a: REPLY from b: "test.ping": ` + jsonErr.Error(),
	})
	same(t, "dead letters", h.takeDead(), []string{
		"wrong-type lockstep://b/user/x#1 -> lockstep://a/user/y#2 {x}",
		"wrong-type lockstep://b/user/x#1 -> lockstep://a/user/y#2 {y}",
	})
	if !strings.Contains(h.log.String(), "remoting: frame not applied") {
		t.Errorf("no warning logged:\n%s", h.log.String())
	}
	// The wrong-type ASK fails back from its target to its ask address once b is up.
	l := h.up()
	f := lastData(t, l)
	fl, _ := ParseFailure(f.Payload)
	want := `lockstep: wrong message type: the actor at "lockstep://a/user/y#2" does not take it`
	if f.Kind != KindFail || f.Sender != to || f.Recipient != addrText(bAsk) || fl.Message != want {
		t.Errorf("sent %s %s -> %s %q; want FAIL %s -> %s %q", f.Kind, f.Sender, f.Recipient, fl.Message, to, addrText(bAsk), want)
	}
}

// TestDecodeAndWrongTypeQuoted checks the bounds on peer text: quoted cuts at 256 bytes, at a rune
// boundary, and cutUTF8 never ends inside a UTF-8 sequence.
func TestDecodeAndWrongTypeQuoted(t *testing.T) {
	for _, c := range []struct{ s, want string }{
		{"abc", `"abc"`},
		{strings.Repeat("a", 256), strconv.Quote(strings.Repeat("a", 256))},
		{strings.Repeat("a", 255) + "é", strconv.Quote(strings.Repeat("a", 255)) + "..."},
	} {
		if got := quoted(c.s); got != c.want {
			t.Errorf("quoted(%d bytes) = %s, want %s", len(c.s), got, c.want)
		}
	}
	for _, c := range []struct {
		s    string
		n    int
		want string
	}{{"aé", 2, "a"}, {"aé", 3, "aé"}, {"aé", 9, "aé"}, {"éé", 1, ""}, {"ab", 0, ""}} {
		if got := cutUTF8(c.s, c.n); got != c.want {
			t.Errorf("cutUTF8(%q, %d) = %q, want %q", c.s, c.n, got, c.want)
		}
	}
}

// TestDecodeAndWrongTypeFailerErrors checks that the errors the Failer gets wrap their causes.
func TestDecodeAndWrongTypeFailerErrors(t *testing.T) {
	useRecvHooks()
	sys := newFakeSys()
	h := newSendHarness(t, func(c *Config) { c.System = sys })
	l := h.acceptUp(3)
	h.core.Frame(l, frame(Frame{Kind: KindMsg, Seq: 1, Sender: bX.String(), Recipient: aY.String(), Type: "test.nope", Payload: []byte("{}")}))
	h.core.Frame(l, frame(Frame{Kind: KindMsg, Seq: 2, Sender: bX.String(), Recipient: aY.String(), Type: "test.blob", Payload: jsonOf(t, blob{})}))
	fails := h.eng.Failures()
	if len(fails) != 2 || !errors.Is(fails[0], codec.ErrNotRegistered) || !errors.Is(fails[1], lockstep.ErrWrongType) {
		t.Errorf("failures = %v, want a codec.ErrNotRegistered and a lockstep.ErrWrongType", fails)
	}
}

// TestPathAddressed checks REM-220: a recipient with uid 0 passes as it is and is checked by path:
// a live actor at the path that takes the value gets it, one that does not is wrong-type, and a
// path with no actor goes to the host.
func TestPathAddressed(t *testing.T) {
	h := newRecvHarness(t)
	h.msg(1, "lockstep://a/user/y#0", ping{1})
	h.msg(2, "lockstep://a/user/y#0", blob{"b"})
	h.msg(3, "lockstep://a/user/none#0", blob{"c"})
	h.msg(4, "lockstep://c/user/y#0", ping{4})
	h.send(h.in, KindAsk, 5, bX.String(), "lockstep://a/user/y#0", "test.ping", AppendAsk(nil, addrText(bAsk), jsonOf(t, ping{5})))
	same(t, "records", h.recs(), []string{
		"discard peer=b conn=3 kind=MSG seq=2 reason=wrong-type",
		"discard peer=b conn=3 kind=MSG seq=4 reason=misrouted",
	})
	same(t, "host", h.takeEvents(), []string{
		"inbound 1 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#0 {1}",
		`fail lockstep: remote: a: MSG from b: "test.blob": lockstep: wrong message type: the actor at "lockstep://a/user/y#0" does not take it`,
		"inbound 2 MSG lockstep://b/user/x#1 -> lockstep://a/user/none#0 {c}",
		"inbound 3 ASK lockstep://b/user/x#1 -> lockstep://a/user/y#0 {5} ask=lockstep://b/temp/9#5",
	})
	same(t, "dead letters", h.takeDead(), []string{"wrong-type lockstep://b/user/x#1 -> lockstep://a/user/y#0 {b}"})
}

// TestReplyAndFail checks REM-260 to REM-268 with fake asks: a REPLY becomes RemoteReply; one a
// pending ask's R does not take becomes RemoteFail wrapping lockstep.ErrWrongType, is discarded
// wrong-type and fails the seed; one for an ask that is not pending is not checked; a FAIL becomes
// RemoteFail with Errors.Remote; an empty REPLY is a nil answer.
func TestReplyAndFail(t *testing.T) {
	h := newRecvHarness(t)
	ask, anyAsk, gone := addrText(askAddr), addrText(askAddr2), "lockstep://a/temp/77#99"
	h.send(h.in, KindReply, 1, bX.String(), ask, "test.ping", jsonOf(t, ping{1}))
	h.send(h.in, KindReply, 2, bX.String(), ask, "test.blob", jsonOf(t, blob{"b"}))
	h.send(h.in, KindReply, 3, bX.String(), gone, "test.blob", jsonOf(t, blob{"late"}))
	h.send(h.in, KindFail, 4, bX.String(), ask, "", AppendFailure(nil, &Failure{Code: 4, Message: "no such type"}))
	h.send(h.in, KindFail, 5, bX.String(), ask, "", AppendFailure(nil, &Failure{Code: 200, Message: "unknown code"}))
	h.send(h.in, KindReply, 6, bX.String(), anyAsk, "", nil)
	h.send(h.in, KindReply, 7, bX.String(), ask, "", nil)
	h.send(h.in, KindReply, 8, bX.String(), anyAsk, "", []byte("{}"))
	wrong := `lockstep: remote: a: REPLY from b: "test.blob": lockstep: wrong message type: the ask "lockstep://a/temp/1#3" does not take it`
	wrongNil := `lockstep: remote: a: REPLY from b: lockstep: wrong message type: the ask "lockstep://a/temp/1#3" does not take it`
	undecoded := `lockstep: remote: a: REPLY from b: codec: type not registered: wire name ""`
	same(t, "records", h.recs(), []string{
		"discard peer=b conn=3 kind=REPLY seq=2 reason=wrong-type",
		"discard peer=b conn=3 kind=REPLY seq=7 reason=wrong-type",
		"discard peer=b conn=3 kind=REPLY seq=8 reason=decode",
	})
	same(t, "host and failer", h.takeEvents(), []string{
		"inbound 1 REPLY lockstep://b/user/x#1 -> lockstep://a/temp/1#3 {1}",
		"inbound 2 FAIL lockstep://b/user/x#1 -> lockstep://a/temp/1#3 " + wrong,
		"fail " + wrong,
		"inbound 3 REPLY lockstep://b/user/x#1 -> lockstep://a/temp/77#99 {late}",
		"inbound 4 FAIL lockstep://b/user/x#1 -> lockstep://a/temp/1#3 remote: b: no such type (code 4)",
		"inbound 5 FAIL lockstep://b/user/x#1 -> lockstep://a/temp/1#3 remote: b: unknown code (code 200)",
		"inbound 6 REPLY lockstep://b/user/x#1 -> lockstep://a/temp/2#4 <nil>",
		"inbound 7 FAIL lockstep://b/user/x#1 -> lockstep://a/temp/1#3 " + wrongNil,
		"fail " + wrongNil,
		"inbound 8 FAIL lockstep://b/user/x#1 -> lockstep://a/temp/2#4 " + undecoded,
		"fail " + undecoded,
	})
	if p, _ := h.ev.envs[1].Payload.(hooks.RemotePayload); !errors.Is(p.Err, lockstep.ErrWrongType) {
		t.Errorf("wrong-type reply fails with %v, which does not wrap lockstep.ErrWrongType", p.Err)
	}
	same(t, "dead letters", h.takeDead(), nil)
}

// TestReplyAndFailRuntime runs replies and failures into package lockstep's asks: the answer
// resolves the ask, a wrong-type answer fails it with lockstep.ErrWrongType and fails the seed, a
// later answer stays a late reply, a FAIL fails it with Errors.Remote's error, and an empty REPLY
// answers an ask of an interface type with nil.
func TestReplyAndFailRuntime(t *testing.T) {
	rh := newRuntimeHarness(t)
	h, sys, eng := rh.harness, rh.sys, rh.eng
	reply := func(to engine.Address, k Kind, typ string, payload []byte) {
		rh.send(k, bX.String(), addrText(to), typ, payload)
	}
	var err error
	lastAsk := func() engine.Address {
		pa := hooks.PendingAsks(sys)
		return pa[len(pa)-1].Ask
	}
	result := func(r *lockstep.Reply[ping]) (ping, error) {
		t.Helper()
		if !hooks.AskResolved(r) {
			t.Fatal("the ask is still pending")
		}
		return r.Await(context.Background())
	}

	_, r1 := lockstep.NewReply[ping](sys, lockstep.WithTimeout(time.Hour))
	a1 := lastAsk()
	reply(a1, KindReply, "test.ping", jsonOf(t, ping{4}))
	if v, err := result(r1); err != nil || v != (ping{4}) {
		t.Errorf("answer: %v, %v; want {4}, nil", v, err)
	}

	_, r2 := lockstep.NewReply[ping](sys, lockstep.WithTimeout(time.Hour))
	a2 := lastAsk()
	h.skip()
	reply(a2, KindReply, "test.blob", jsonOf(t, blob{"b"}))
	_, err = result(r2)
	want := `lockstep: remote: a: REPLY from b: "test.blob": lockstep: wrong message type: the ask "` + a2.String() + `" does not take it`
	if !errors.Is(err, lockstep.ErrWrongType) || err.Error() != want {
		t.Errorf("wrong-type answer: %v; want %q wrapping lockstep.ErrWrongType", err, want)
	}
	if f := eng.Failures(); len(f) != 1 || f[0].Error() != want {
		t.Errorf("failures = %v, want [%s]", f, want)
	}
	same(t, "records", h.recs(), []string{"discard peer=b conn=3 kind=REPLY seq=2 reason=wrong-type"})

	// A second answer to the same ask is late: it is not checked, and nothing more fails.
	mark := len(eng.Records())
	reply(a2, KindReply, "test.blob", jsonOf(t, blob{"again"}))
	if f := eng.Failures(); len(f) != 1 {
		t.Errorf("a late reply failed the seed: %v", f[1:])
	}
	if got := outcomes(eng.Records()[mark:]); !reflect.DeepEqual(got, []string{"late"}) {
		t.Errorf("lockstep.reply outcomes of the late reply = %q, want [late]", got)
	}
	same(t, "records", h.recs(), nil)

	_, r3 := lockstep.NewReply[ping](sys, lockstep.WithTimeout(time.Hour))
	reply(lastAsk(), KindFail, "", AppendFailure(nil, &Failure{Code: 3, Message: "not found"}))
	if _, err := result(r3); err == nil || err.Error() != "remote: b: not found (code 3)" {
		t.Errorf("FAIL: %v, want remote: b: not found (code 3)", err)
	}

	_, r4 := lockstep.NewReply[any](sys, lockstep.WithTimeout(time.Hour))
	reply(lastAsk(), KindReply, "", nil)
	if !hooks.AskResolved(r4) {
		t.Fatal("the ask of an interface type is still pending")
	}
	if v, err := r4.Await(context.Background()); v != nil || err != nil {
		t.Errorf("nil answer: %v, %v; want nil, nil", v, err)
	}
}

// runtimeHarness is a core on node "a" whose host is a package lockstep system on the same
// manual engine, with an up incoming session from "b" (conn 3).
type runtimeHarness struct {
	*harness
	sys *lockstep.System
	in  *fakeLink
	seq uint64 // the number of the last frame sent on in
}

// coreTransport is the system's transport: the core, once it is built.
type coreTransport struct{ core *Core }

func (t *coreTransport) Deliver(env engine.Envelope) error { return t.core.Deliver(env) }

func newRuntimeHarness(t *testing.T) *runtimeHarness {
	t.Helper()
	useRealPayloads(t)
	eng := enginetest.New("a", 1)
	bundle := eng.Engine()
	bundle.Transport = nil
	b, err := hooks.NewSystem(bundle, nil)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetHost(b.Host)
	sys, ok := b.System.(*lockstep.System)
	if !ok {
		t.Fatalf("Built.System has type %T", b.System)
	}
	tr := &coreTransport{}
	att, err := hooks.AttachTransport(sys, tr, func() {})
	if err != nil {
		t.Fatal(err)
	}
	drv := &fakeDriver{t: t, errs: map[string]error{}}
	cfg := testConfig(eng, drv)
	cfg.Host = eng // att.Host, called in engine context
	cfg.System = sys
	cfg.DeadLetter = att.DeadLetter
	cfg.Settings.Registry = testRegistry()
	core, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tr.core = core
	core.Start()
	h := &harness{t: t, eng: eng, drv: drv, core: core}
	return &runtimeHarness{harness: h, sys: sys, in: h.acceptUp(3)}
}

// send hands the incoming session the next data frame, then runs the engine until idle.
func (h *runtimeHarness) send(k Kind, from, to, typ string, payload []byte) {
	h.seq++
	h.core.Frame(h.in, frame(Frame{Kind: k, Seq: h.seq, Sender: from, Recipient: to, Type: typ, Payload: payload}))
	h.eng.RunUntilIdle()
}

// attrsOf returns the value of attribute key of every record of kind in recs.
func attrsOf(recs []engine.Record, kind, key string) []string {
	var out []string
	for _, r := range recs {
		if r.Kind != kind {
			continue
		}
		for _, a := range r.Attrs {
			if a.Key == key {
				out = append(out, a.Value)
			}
		}
	}
	return out
}

// outcomes returns the outcome attribute of every lockstep.reply record of recs.
func outcomes(recs []engine.Record) []string { return attrsOf(recs, "lockstep.reply", "outcome") }

// pingActor keeps the pings it receives.
type pingActor struct{ got *[]ping }

func (a pingActor) Receive(_ *lockstep.Context[ping], m ping) { *a.got = append(*a.got, m) }

// TestDecodeAndWrongTypeRuntime runs messages and requests into package lockstep's actors: one the
// actor takes arrives, also by path; one it does not take is a wrong-type dead letter and fails
// the seed; a request to an actor that is not alive goes to the runtime, which makes it a dead
// letter and fails it back to the asker's node with ErrTerminated.
func TestDecodeAndWrongTypeRuntime(t *testing.T) {
	h := newRuntimeHarness(t)
	var got []ping
	pid, err := lockstep.Spawn(h.sys, "y", func() lockstep.Actor[ping] { return pingActor{&got} })
	if err != nil {
		t.Fatal(err)
	}
	h.eng.RunUntilIdle()
	to, _ := pid.MarshalText()
	h.skip()
	mark := len(h.eng.Records())
	h.send(KindMsg, bX.String(), string(to), "test.ping", jsonOf(t, ping{1}))
	h.send(KindMsg, bX.String(), "lockstep://a/user/y#0", "test.ping", jsonOf(t, ping{2}))
	h.send(KindMsg, bX.String(), string(to), "test.blob", jsonOf(t, blob{"b"}))
	h.send(KindAsk, bX.String(), "lockstep://a/user/gone#0", "test.ping", AppendAsk(nil, addrText(bAsk), jsonOf(t, ping{3})))
	same(t, "records", h.recs(), []string{
		"discard peer=b conn=3 kind=MSG seq=3 reason=wrong-type",
		"dial peer=b conn=1 attempt=1", // the runtime's Fail(ErrTerminated) of the request
	})
	if !reflect.DeepEqual(got, []ping{{1}, {2}}) {
		t.Errorf("the actor received %v, want [{1} {2}]", got)
	}
	same(t, "dead letters", attrsOf(h.eng.Records()[mark:], "lockstep.deadletter", "reason"), []string{"wrong-type", "not-found"})
	if f := h.eng.Failures(); len(f) != 1 || !errors.Is(f[0], lockstep.ErrWrongType) {
		t.Errorf("failures = %v, want one wrapping lockstep.ErrWrongType", f)
	}
	// The failure goes out once the session to b is up.
	l := h.drv.links[0]
	h.core.Frame(l, welcome("b", "a", 9, "json"))
	f := lastData(t, l)
	fl, _ := ParseFailure(f.Payload)
	if f.Kind != KindFail || f.Recipient != addrText(bAsk) || !strings.Contains(fl.Message, lockstep.ErrTerminated.Error()) {
		t.Errorf("sent %s to %s with %+v, want a FAIL to %s with ErrTerminated", f.Kind, f.Recipient, fl, addrText(bAsk))
	}
}

// TestReplyEncodeFailure checks REM-266 on the sending side: a reply attempt whose answer does not
// encode, or whose REPLY frame exceeds MaxFrame, is sent as FAIL with code other and the error's
// text and fails the seed; Deliver returns nil. A nil answer is a REPLY with an empty type and
// payload. A message that does not encode is still the caller's error.
func TestReplyEncodeFailure(t *testing.T) {
	h := newSendHarness(t, func(c *Config) { c.Settings.MaxFrame = MinFrameLimit })
	from := engine.Address{Node: "a", Path: "/user/x", UID: 1}
	replyEnv := func(v any) engine.Envelope {
		return engine.Envelope{From: from, To: bAsk, Payload: hooks.RemotePayload{Kind: hooks.RemoteReply, Value: v}}
	}
	h.deliver(replyEnv(stray{1}))
	l := h.up()
	_, _, encErr := testRegistry().Encode(codec.JSON{}, stray{1})
	f := lastData(t, l)
	if f.Kind != KindFail || f.Type != "" || f.Sender != addrText(from) || f.Recipient != addrText(bAsk) {
		t.Errorf("frame = %+v, want a FAIL from %s to %s", f, from, bAsk)
	}
	if fl, _ := ParseFailure(f.Payload); fl != (Failure{Code: codeOther, Message: encErr.Error()}) {
		t.Errorf("failure = %+v, want code 0 and %q", fl, encErr)
	}

	big := blob{strings.Repeat("x", MinFrameLimit)}
	h.deliver(replyEnv(big))
	f = lastData(t, l)
	fl, _ := ParseFailure(f.Payload)
	if f.Kind != KindFail || fl.Code != codeOther || !strings.HasPrefix(fl.Message, "remote: frame too large: ") ||
		!strings.HasSuffix(fl.Message, " bytes > max 4096") {
		t.Errorf("too-large reply sent as %s %+v, want FAIL code 0 with the frame-size error", f.Kind, fl)
	}

	h.deliver(replyEnv(nil))
	f = lastData(t, l)
	if f.Kind != KindReply || f.Type != "" || f.Payload != nil {
		t.Errorf("nil answer sent as %+v, want a REPLY with an empty type and payload", f)
	}

	fails := h.eng.Failures()
	wantFails := []string{
		"lockstep: remote: a: REPLY to b: remoting.stray: " + encErr.Error(),
		"lockstep: remote: a: REPLY to b: test.blob: " + fl.Message,
	}
	var got []string
	for _, e := range fails {
		got = append(got, e.Error())
	}
	same(t, "failures", got, wantFails)
	if len(fails) == 2 && !errors.Is(fails[0], codec.ErrNotRegistered) {
		t.Errorf("failure %v does not wrap the encode error", fails[0])
	}
	same(t, "frames", data(l), []string{"FAIL seq=1", "FAIL seq=2", "REPLY seq=3"})
	same(t, "dead letters", h.takeDead(), nil)

	if err := h.core.Deliver(msgEnv(stray{1})); !errors.Is(err, codec.ErrNotRegistered) {
		t.Errorf("Deliver(MSG of an unregistered type) = %v, want the encode error", err)
	}
	if !strings.Contains(h.log.String(), "remoting: reply sent as a failure") {
		t.Errorf("no warning logged:\n%s", h.log.String())
	}
}

// TestReplyEncodeFailureFits checks that a FAIL frame always fits MaxFrame: its message is cut at
// a rune boundary, for ReplyTo.Fail and for the FAIL that replaces a reply; only addresses too
// long for any FAIL give the frame-size error.
func TestReplyEncodeFailureFits(t *testing.T) {
	h := newSendHarness(t, func(c *Config) { c.Settings.MaxFrame = MinFrameLimit })
	from := engine.Address{Node: "a", Path: "/user/x", UID: 1}
	msg := "x" + strings.Repeat("é", 3000) // 6001 bytes; the cut must not split an "é"
	h.deliver(engine.Envelope{From: from, To: bAsk, Payload: hooks.RemotePayload{Kind: hooks.RemoteFail, Err: errors.New(msg)}})
	l := h.up()
	f := lastData(t, l)
	fl, err := ParseFailure(f.Payload)
	if err != nil || len(frame(f))-prefixLen != MinFrameLimit || !strings.HasPrefix(msg, fl.Message) {
		t.Errorf("FAIL of %d bytes with a %d-byte message (%v); want %d bytes and a prefix of the message",
			len(frame(f))-prefixLen, len(fl.Message), err, MinFrameLimit)
	}

	// A reply whose answer is too large is replaced by a FAIL cut the same way.
	h.deliver(engine.Envelope{From: from, To: bAsk, Payload: hooks.RemotePayload{Kind: hooks.RemoteReply, Value: blob{strings.Repeat("y", MinFrameLimit)}}})
	if f := lastData(t, l); f.Kind != KindFail || len(frame(f))-prefixLen > MinFrameLimit {
		t.Errorf("sent %s of %d bytes, want a FAIL that fits", f.Kind, len(frame(f))-prefixLen)
	}

	// Addresses too long for even an empty failure: the frame-size error, nothing sent.
	far := engine.Address{Node: "a", Path: "/user/" + strings.Repeat("z", MinFrameLimit), UID: 1}
	for _, p := range []hooks.RemotePayload{{Kind: hooks.RemoteFail, Err: errors.New("e")}, {Kind: hooks.RemoteReply, Value: stray{1}}} {
		if err := h.core.Deliver(engine.Envelope{From: far, To: bAsk, Payload: p}); !errors.Is(err, errFrameTooLarge) {
			t.Errorf("Deliver(%s from a long path) = %v, want the frame-size error", frameKind(p.Kind), err)
		}
	}
	same(t, "frames", data(l), []string{"FAIL seq=1", "FAIL seq=2"})
}

// TestClosedSession checks REM-250: frames on the link of an incoming session the core closed are
// discarded closed-session, until the driver reports the link closed. The core remembers the last
// closed incoming session of each peer only.
func TestClosedSession(t *testing.T) {
	t.Run("replaced", func(t *testing.T) {
		h := newRecvHarness(t)
		h.msg(1, aY.String(), ping{1})
		l2 := h.acceptUp(4)
		h.msg(2, aY.String(), ping{2})
		h.core.Frame(h.in, ctrl(KindPing, ""))
		h.core.Frame(h.in, []byte{1, 2}) // does not parse: ignored
		same(t, "records", h.recs(), []string{
			"discard peer=b conn=3 kind=MSG seq=2 reason=closed-session",
			"discard peer=b conn=3 kind=PING seq=0 reason=closed-session",
		})
		h.core.Closed(h.in, nil)
		h.msg(3, aY.String(), ping{3})
		same(t, "records after Closed", h.recs(), nil)
		if len(h.core.closed) != 0 {
			t.Errorf("the core still remembers %d closed sessions", len(h.core.closed))
		}
		h.send(l2, KindMsg, 1, bX.String(), aY.String(), "test.ping", jsonOf(t, ping{4}))
		same(t, "host", h.takeEvents(), []string{
			"inbound 1 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {1}",
			"inbound 2 MSG lockstep://b/user/x#1 -> lockstep://a/user/y#2 {4}",
		})
	})
	t.Run("peer closed", func(t *testing.T) {
		h := newRecvHarness(t)
		h.core.Frame(h.in, ctrl(KindClose, CloseShutdown))
		h.recs()
		h.msg(1, aY.String(), ping{1})
		same(t, "records", h.recs(), []string{"discard peer=b conn=3 kind=MSG seq=1 reason=closed-session"})
		same(t, "host", h.takeEvents(), nil)
	})
	t.Run("last per peer", func(t *testing.T) {
		h := newRecvHarness(t)
		l2 := h.acceptUp(4)
		h.acceptUp(5)
		h.msg(1, aY.String(), ping{1})
		h.send(l2, KindMsg, 1, bX.String(), aY.String(), "test.ping", jsonOf(t, ping{2}))
		same(t, "records", h.recs(), []string{"discard peer=b conn=4 kind=MSG seq=1 reason=closed-session"})
		if len(h.core.closed) != 1 {
			t.Errorf("the core remembers %d closed sessions, want 1", len(h.core.closed))
		}
	})
	t.Run("closed by the driver", func(t *testing.T) {
		h := newRecvHarness(t)
		h.core.Closed(h.in, errors.New("reset"))
		h.recs()
		h.msg(1, aY.String(), ping{1})
		same(t, "records", h.recs(), nil)
		if len(h.core.closed) != 0 {
			t.Errorf("the core remembers %d closed sessions, want 0", len(h.core.closed))
		}
	})
}
