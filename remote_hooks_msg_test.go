// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

var (
	errNegative = errors.New("negative")
	errDown     = errors.New("node down")
)

// remoteNode is a system on a manual engine. carry moves what it hands its transport to another
// node through hooks.Payload and hooks.NewPayload, as the remoting core does with frames.
type remoteNode struct {
	eng   *enginetest.Engine
	sys   *lockstep.System
	moved int    // the envelopes of eng.Delivered that carry has moved
	seq   uint64 // the Seq of the last envelope carried to this node
}

// newRemoteNode builds a system on a manual engine for node through hooks.NewSystem.
func newRemoteNode(t *testing.T, node string) *remoteNode {
	t.Helper()
	eng := enginetest.New(node, 1)
	b, err := hooks.NewSystem(eng.Engine(), nil)
	if err != nil {
		t.Fatalf("hooks.NewSystem(%s): %v", node, err)
	}
	eng.SetHost(b.Host)
	return &remoteNode{eng: eng, sys: systemOf(t, b)}
}

// carry moves to dst every envelope n handed its transport since the last call: it reads each
// payload with hooks.Payload, builds dst's with hooks.NewPayload, numbers the envelopes from 1 per
// destination and hands them to dst's Host.Inbound in order. Then it runs dst until idle. It
// returns the envelopes it read, with the payloads hooks.Payload returned.
func (n *remoteNode) carry(t *testing.T, dst *remoteNode) ([]engine.Envelope, []hooks.RemotePayload) {
	t.Helper()
	d := n.eng.Delivered()[n.moved:]
	n.moved += len(d)
	ps := make([]hooks.RemotePayload, len(d))
	for i, env := range d {
		p, ok := hooks.Payload(env.Payload)
		if !ok {
			t.Fatalf("hooks.Payload(%T) reports no runtime payload", env.Payload)
		}
		ps[i] = p
		dst.seq++
		in := env
		in.Seq = dst.seq
		in.Payload = hooks.NewPayload(p)
		dst.eng.Inbound(in)
	}
	dst.eng.RunUntilIdle()
	return d, ps
}

// spawnAt spawns the actor factory builds at /user/<name> on n and returns its full PID.
func spawnAt[M any](t *testing.T, n *remoteNode, name string, factory func() lockstep.Actor[M], opts ...lockstep.SpawnOption) lockstep.PID[M] {
	t.Helper()
	pid, err := lockstep.Spawn(n.sys, name, factory, opts...)
	if err != nil {
		t.Fatalf("Spawn(%s): %v", name, err)
	}
	n.eng.RunUntilIdle()
	return pid
}

// collector records the ints it receives.
type collector struct{ got *[]int }

func (c collector) Receive(_ *lockstep.Context[int], m int) { *c.got = append(*c.got, m) }

// double asks for twice N.
type double struct {
	N  int
	RT lockstep.ReplyTo[int]
}

// doubler answers a double with twice N, and fails a negative N with errNegative.
type doubler struct{}

func (doubler) Receive(ctx *lockstep.Context[double], m double) {
	if m.N < 0 {
		m.RT.Fail(ctx, errNegative)
		return
	}
	m.RT.Send(ctx, 2*m.N)
}

// keeper takes requests and answers none.
type keeper struct{}

func (keeper) Receive(*lockstep.Context[double], double) {}

// askDouble asks to for twice n on behalf of from, with a timeout that never fires in a test.
func askDouble(from lockstep.Sender, to lockstep.PID[double], n int) *lockstep.Reply[int] {
	return lockstep.Ask(from, to, func(rt lockstep.ReplyTo[int]) double { return double{N: n, RT: rt} },
		lockstep.WithTimeout(time.Hour))
}

// resolved returns r's result, failing the test if r is still pending.
func resolved[R any](t *testing.T, r *lockstep.Reply[R]) (R, error) {
	t.Helper()
	if !hooks.AskResolved(r) {
		t.Fatal("the reply is still pending")
	}
	return r.Await(context.Background())
}

// attrsOf returns the attributes of every record of kind, in order.
func attrsOf(recs []engine.Record, kind string) []map[string]string {
	var out []map[string]string
	for _, r := range recs {
		if r.Kind != kind {
			continue
		}
		m := make(map[string]string, len(r.Attrs))
		for _, a := range r.Attrs {
			m[a.Key] = a.Value
		}
		out = append(out, m)
	}
	return out
}

// panicsWith fails the test unless f panics with want.
func panicsWith(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Errorf("panic = %v, want %q", got, want)
		}
	}()
	f()
}

// REM §4.3: package lockstep sets the message side of the remoting hooks in its init.
func TestRemoteHooksSet(t *testing.T) {
	for name, set := range map[string]bool{
		"Payload":    hooks.Payload != nil,
		"NewPayload": hooks.NewPayload != nil,
		"Assignable": hooks.Assignable != nil,
		"FailAsks":   hooks.FailAsks != nil,
		"Pending":    hooks.Pending != nil,
	} {
		if !set {
			t.Errorf("hooks.%s is nil", name)
		}
	}
}

// REM §4.3: hooks.Payload reads what hooks.NewPayload builds for the four message kinds, and
// reports false for a value that is not a runtime payload.
func TestRemotePayloadRoundTrip(t *testing.T) {
	ask := engine.Address{Node: "n1", Path: "/temp/1", UID: 0x42}
	for _, p := range []hooks.RemotePayload{
		{Kind: hooks.RemoteMsg, Value: 5},
		{Kind: hooks.RemoteMsg}, // a nil message of an interface M
		{Kind: hooks.RemoteAsk, Value: "q", Ask: ask},
		{Kind: hooks.RemoteReply, Value: 7},
		{Kind: hooks.RemoteReply}, // a nil answer of an interface R
		{Kind: hooks.RemoteFail, Err: errNegative},
	} {
		got, ok := hooks.Payload(hooks.NewPayload(p))
		if !ok || got != p {
			t.Errorf("Payload(NewPayload(%+v)) = %+v, %t, want it back and true", p, got, ok)
		}
	}
	for _, v := range []any{nil, 5, hooks.RemotePayload{Kind: hooks.RemoteMsg, Value: 5}} {
		if got, ok := hooks.Payload(v); ok {
			t.Errorf("Payload(%#v) = %+v, true, want false", v, got)
		}
	}
	panicsWith(t, "lockstep: hooks.NewPayload: RemoteAsk without an ask address", func() {
		hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteAsk, Value: 1})
	})
	panicsWith(t, "lockstep: hooks.NewPayload: RemoteFail with a nil Err", func() {
		hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteFail})
	})
}

// MSG-110, REM-160: a Send to another node is RemoteMsg from the sender to the target; built back
// with NewPayload it reaches the actor as an inbound message with the envelope's Seq. A path
// recipient resolves whatever the sender's message type.
func TestRemoteSendAcrossNodes(t *testing.T) {
	n1, n2 := newRemoteNode(t, "n1"), newRemoteNode(t, "n2")
	var got []int
	rec := spawnAt(t, n2, "rec", func() lockstep.Actor[int] { return collector{&got} })

	if err := lockstep.Send(n1.sys, lockstep.PIDAt[int]("n2", "/user/rec"), 5); err != nil {
		t.Fatalf("Send by path: %v", err)
	}
	// The sender's M is any, the actor's int: the core checked Assignable, so the path resolves.
	if err := lockstep.Send(n1.sys, lockstep.PIDAt[any]("n2", "/user/rec"), any(6)); err != nil {
		t.Fatalf("Send by path as any: %v", err)
	}
	if err := lockstep.Send(n1.sys, rec, 7); err != nil {
		t.Fatalf("Send by uid: %v", err)
	}
	envs, ps := n1.carry(t, n2)
	want := []hooks.RemotePayload{{Kind: hooks.RemoteMsg, Value: 5}, {Kind: hooks.RemoteMsg, Value: 6}, {Kind: hooks.RemoteMsg, Value: 7}}
	if !slices.Equal(ps, want) {
		t.Fatalf("payloads %+v, want %+v", ps, want)
	}
	user := engine.Address{Node: "n1", Path: "/user", UID: lockstep.UserGuardianUID(n1.sys)}
	path := engine.Address{Node: "n2", Path: "/user/rec"}
	full := engine.Address{Node: "n2", Path: "/user/rec", UID: rec.Address().UID()}
	for i, to := range []engine.Address{path, path, full} {
		if envs[i].From != user || envs[i].To != to {
			t.Errorf("envelope %d from %v to %v, want from %v to %v", i, envs[i].From, envs[i].To, user, to)
		}
	}
	if !slices.Equal(got, []int{5, 6, 7}) {
		t.Errorf("rec received %v, want [5 6 7]", got)
	}
	var seqs []string
	for _, a := range attrsOf(n2.eng.Records(), "lockstep.deliver") {
		if a["to"] == path.String() || a["to"] == full.String() {
			seqs = append(seqs, a["seq"])
		}
	}
	if !slices.Equal(seqs, []string{"1", "2", "3"}) {
		t.Errorf("lockstep.deliver seq values %v, want [1 2 3]", seqs)
	}
}

// MSG-300, MSG-410, MSG-420: an Ask to another node is RemoteAsk carrying the ask address; the
// target's answer comes back as RemoteReply, and its failure as RemoteFail, both to the ask
// address, and resolve the ask on the asker's node.
func TestRemoteAskAcrossNodes(t *testing.T) {
	n1, n2 := newRemoteNode(t, "n1"), newRemoteNode(t, "n2")
	d := spawnAt(t, n2, "d", func() lockstep.Actor[double] { return doubler{} })
	to := lockstep.PIDAt[double]("n2", "/user/d")

	r := askDouble(n1.sys, to, 21)
	_, ps := n1.carry(t, n2)
	if len(ps) != 1 || ps[0].Kind != hooks.RemoteAsk {
		t.Fatalf("payloads %+v, want one RemoteAsk", ps)
	}
	ask := ps[0].Ask
	if m, ok := ps[0].Value.(double); !ok || m.N != 21 {
		t.Errorf("RemoteAsk value %#v, want the double request", ps[0].Value)
	}
	if pa := hooks.PendingAsks(n1.sys); len(pa) != 1 || pa[0].Ask != ask {
		t.Errorf("pending asks %+v, want one with ask address %v", pa, ask)
	}
	if !hooks.Pending(n1.sys, ask) {
		t.Errorf("Pending(%v) = false before the answer", ask)
	}

	envs, ps := n2.carry(t, n1)
	if want := (hooks.RemotePayload{Kind: hooks.RemoteReply, Value: 42}); len(ps) != 1 || ps[0] != want {
		t.Fatalf("payloads %+v, want [%+v]", ps, want)
	}
	if replier := d.Address(); envs[0].From != (engine.Address{Node: "n2", Path: "/user/d", UID: replier.UID()}) || envs[0].To != ask {
		t.Errorf("reply envelope from %v to %v, want from %v to %v", envs[0].From, envs[0].To, replier, ask)
	}
	if v, err := resolved(t, r); v != 42 || err != nil {
		t.Errorf("Await = %d, %v, want 42, nil", v, err)
	}
	if hooks.Pending(n1.sys, ask) {
		t.Errorf("Pending(%v) = true after the answer", ask)
	}

	r = askDouble(n1.sys, to, -1)
	n1.carry(t, n2)
	_, ps = n2.carry(t, n1)
	if want := (hooks.RemotePayload{Kind: hooks.RemoteFail, Err: errNegative}); len(ps) != 1 || ps[0] != want {
		t.Fatalf("payloads %+v, want [%+v]", ps, want)
	}
	if _, err := resolved(t, r); err != errNegative { //nolint:errorlint // the very value Fail got
		t.Errorf("Await error %v, want errNegative itself", err)
	}
}

// MSG-340, MSG-345, MSG-350: the runtime's own failures of an ask from another node go back as
// RemoteFail: the request reached no live actor, the target's mailbox refused it, or the target
// stopped while it tracked the ask.
func TestRemoteAskRuntimeFailures(t *testing.T) {
	n1, n2 := newRemoteNode(t, "n1"), newRemoteNode(t, "n2")

	// failure carries what n1 sent to n2 and back, and returns the one RemoteFail that came back.
	failure := func(t *testing.T) error {
		t.Helper()
		n1.carry(t, n2)
		_, ps := n2.carry(t, n1)
		if len(ps) != 1 || ps[0].Kind != hooks.RemoteFail {
			t.Fatalf("payloads %+v, want one RemoteFail", ps)
		}
		return ps[0].Err
	}

	t.Run("absent target", func(t *testing.T) {
		r := askDouble(n1.sys, lockstep.PIDAt[double]("n2", "/user/none"), 1)
		if err := failure(t); !errors.Is(err, lockstep.ErrTerminated) {
			t.Errorf("RemoteFail error %v, want ErrTerminated", err)
		}
		if _, err := resolved(t, r); !errors.Is(err, lockstep.ErrTerminated) {
			t.Errorf("Await error %v, want ErrTerminated", err)
		}
	})

	t.Run("refused", func(t *testing.T) {
		k := spawnAt(t, n2, "full", func() lockstep.Actor[double] { return keeper{} },
			lockstep.WithMailbox(1, lockstep.FailSend))
		first := askDouble(n1.sys, k, 1)
		second := askDouble(n1.sys, k, 2) // refused: both arrive before the keeper's turn
		if err := failure(t); !errors.Is(err, lockstep.ErrMailboxFull) {
			t.Errorf("RemoteFail error %v, want ErrMailboxFull", err)
		}
		if _, err := resolved(t, second); !errors.Is(err, lockstep.ErrMailboxFull) {
			t.Errorf("Await error %v, want ErrMailboxFull", err)
		}
		if hooks.AskResolved(first) {
			t.Error("the first ask resolved, want it pending: the keeper took it")
		}
	})

	t.Run("target stopped", func(t *testing.T) {
		k := spawnAt(t, n2, "k", func() lockstep.Actor[double] { return keeper{} })
		r := askDouble(n1.sys, k, 1)
		n1.carry(t, n2)
		if err := lockstep.Stop(n2.sys, k.Address()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		n2.eng.RunUntilIdle()
		_, ps := n2.carry(t, n1)
		if len(ps) != 1 || ps[0].Kind != hooks.RemoteFail || !errors.Is(ps[0].Err, lockstep.ErrTerminated) {
			t.Fatalf("payloads %+v, want one RemoteFail with ErrTerminated", ps)
		}
		if _, err := resolved(t, r); !errors.Is(err, lockstep.ErrTerminated) {
			t.Errorf("Await error %v, want ErrTerminated", err)
		}
	})
}

// REM §4.3: for a pending ask address, Assignable checks v against the ask's R, a nil v only for
// an interface R; any address that is no pending ask of the system is false.
func TestAssignable(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	askDouble(n1.sys, lockstep.PIDAt[double]("n2", "/user/d"), 1)
	lockstep.NewReply[error](n1.sys, lockstep.WithTimeout(time.Hour))
	rtAny, _ := lockstep.NewReply[any](n1.sys, lockstep.WithTimeout(time.Hour))
	pa := hooks.PendingAsks(n1.sys)
	if len(pa) != 3 {
		t.Fatalf("%d pending asks, want 3", len(pa))
	}
	intAsk, errAsk, anyAsk := pa[0].Ask, pa[1].Ask, pa[2].Ask
	for _, c := range []struct {
		to   engine.Address
		v    any
		want bool
	}{
		{intAsk, 3, true},
		{intAsk, int64(3), false},
		{intAsk, "x", false},
		{intAsk, nil, false}, // int is no interface
		{errAsk, errNegative, true},
		{errAsk, nil, true},
		{errAsk, 3, false},
		{anyAsk, "x", true},
		{anyAsk, nil, true},
		{engine.Address{Node: "n1", Path: intAsk.Path, UID: intAsk.UID + 100}, 3, false}, // unknown uid
		{engine.Address{Node: "n2", Path: intAsk.Path, UID: intAsk.UID}, 3, false},       // another node
		{engine.Address{}, 3, false},
	} {
		if got := hooks.Assignable(n1.sys, c.to, c.v); got != c.want {
			t.Errorf("Assignable(%v, %#v) = %t, want %t", c.to, c.v, got, c.want)
		}
	}

	// A resolved ask is no pending ask.
	rtAny.Send(n1.sys, 1)
	if hooks.Assignable(n1.sys, anyAsk, 1) {
		t.Errorf("Assignable(%v) = true after the ask resolved", anyAsk)
	}
	panicsWith(t, "lockstep: hooks.Assignable: sys has type string, want a non-nil *lockstep.System", func() {
		hooks.Assignable("x", intAsk, 3)
	})
}

// REM §4.3, MSG-370: FailAsks resolves the pending asks whose target is on the node, in
// ask-counter order, and leaves every other ask, those without a target included, pending.
func TestFailAsks(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	k := spawnAt(t, n1, "k", func() lockstep.Actor[double] { return keeper{} })
	b := lockstep.PIDAt[double]("n2", "/user/b")
	a1 := askDouble(n1.sys, b, 1)
	a2 := askDouble(n1.sys, lockstep.PIDAt[double]("n3", "/user/c"), 2)
	_, a3 := lockstep.NewReply[int](n1.sys, lockstep.WithTimeout(time.Hour))
	a4 := askDouble(n1.sys, b, 4)
	a5 := askDouble(n1.sys, k, 5)
	n1.eng.RunUntilIdle()
	before := hooks.PendingAsks(n1.sys)
	if len(before) != 5 {
		t.Fatalf("%d pending asks, want 5", len(before))
	}

	hooks.FailAsks(n1.sys, "n2", errDown)
	for i, r := range []*lockstep.Reply[int]{a1, a4} {
		if _, err := resolved(t, r); err != errDown { //nolint:errorlint // the very value FailAsks got
			t.Errorf("ask %d: Await error %v, want errDown itself", i, err)
		}
	}
	var replies []string
	for _, a := range attrsOf(n1.eng.Records(), "lockstep.reply") {
		replies = append(replies, a["ask"]+" from="+a["from"]+" "+a["outcome"])
	}
	wantReplies := []string{
		before[0].Ask.String() + " from=" + before[0].Target.String() + " resolved",
		before[3].Ask.String() + " from=" + before[3].Target.String() + " resolved",
	}
	if !slices.Equal(replies, wantReplies) {
		t.Errorf("lockstep.reply records %q, want %q", replies, wantReplies)
	}

	// Asks of another node, of this node and without a target stay pending; an empty node name
	// matches none of them.
	hooks.FailAsks(n1.sys, "", errDown)
	hooks.FailAsks(n1.sys, "n9", errDown)
	var left []engine.Address
	for _, p := range hooks.PendingAsks(n1.sys) {
		left = append(left, p.Ask)
	}
	if want := []engine.Address{before[1].Ask, before[2].Ask, before[4].Ask}; !slices.Equal(left, want) {
		t.Errorf("pending asks %v, want %v", left, want)
	}
	for i, r := range []*lockstep.Reply[int]{a2, a3, a5} {
		if hooks.AskResolved(r) {
			t.Errorf("ask %d resolved, want it pending", i)
		}
	}

	panicsWith(t, "lockstep: hooks.FailAsks: nil error", func() { hooks.FailAsks(n1.sys, "n3", nil) })
	panicsWith(t, "lockstep: hooks.FailAsks: sys has type int, want a non-nil *lockstep.System", func() {
		hooks.FailAsks(1, "n3", errDown)
	})

	// A discarded system has no pending asks: FailAsks leaves the ErrStopped results alone.
	hooks.Discard(n1.sys)
	hooks.FailAsks(n1.sys, "n3", errDown)
	if _, err := resolved(t, a2); !errors.Is(err, lockstep.ErrStopped) {
		t.Errorf("Await error after Discard and FailAsks %v, want ErrStopped", err)
	}
}

// REM §4.3: Pending reports a pending ask of the system, and nothing else.
func TestPending(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	r := askDouble(n1.sys, lockstep.PIDAt[double]("n2", "/user/d"), 1)
	ask := hooks.PendingAsks(n1.sys)[0].Ask
	for _, c := range []struct {
		ask  engine.Address
		want bool
	}{
		{ask, true},
		{engine.Address{Node: "n1", Path: ask.Path, UID: ask.UID + 1}, false},
		{engine.Address{Node: "n1", Path: "/temp/9", UID: ask.UID}, false},
		{engine.Address{Node: "n2", Path: ask.Path, UID: ask.UID}, false},
		{engine.Address{}, false},
	} {
		if got := hooks.Pending(n1.sys, c.ask); got != c.want {
			t.Errorf("Pending(%v) = %t, want %t", c.ask, got, c.want)
		}
	}

	// The answer arrives through NewPayload and resolves the ask (MSG-420).
	n1.eng.Inbound(engine.Envelope{
		From:    engine.Address{Node: "n2", Path: "/user/d", UID: 7},
		To:      ask,
		Seq:     1,
		Payload: hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteReply, Value: 2}),
	})
	if v, err := resolved(t, r); v != 2 || err != nil {
		t.Errorf("Await = %d, %v, want 2, nil", v, err)
	}
	if hooks.Pending(n1.sys, ask) {
		t.Errorf("Pending(%v) = true after the answer", ask)
	}
	panicsWith(t, "lockstep: hooks.Pending: sys has type <nil>, want a non-nil *lockstep.System", func() {
		hooks.Pending(nil, ask)
	})
}
