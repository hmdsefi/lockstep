// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// AT-MSG-36 (local clauses): MSG-110, MSG-350. A path address before its actor exists is a dead
// letter for Send and resolves an Ask with ErrTerminated at once; after the spawn both reach the
// actor.
func TestATMSG36PathAddressAsk(t *testing.T) {
	sys := newLocalSystem(t)
	p := PIDAt[acctMsg]("local", "/user/acct")
	if err := Send(sys, p, acctMsg(Deposit{1})); err != nil {
		t.Errorf("Send to %v before acct exists = %v, want nil (dead letter)", p, err)
	}
	r := Ask(sys, p, getBalance, WithTimeout(time.Hour))
	if !r.resolved.Load() {
		t.Fatal("Ask through the path address before acct exists returned a pending reply")
	}
	_, err := r.Await(cancelled())
	if want := "lockstep: actor terminated: " + p.String(); !errors.Is(err, ErrTerminated) || err.Error() != want {
		t.Errorf("Await = %v, want ErrTerminated with text %q", err, want)
	}

	spawnAcct(t, sys, nil)
	if err := Send(sys, p, acctMsg(Deposit{10})); err != nil {
		t.Fatalf("Send to %v after the spawn = %v", p, err)
	}
	if v, err := Ask(sys, p, getBalance).Await(within(t, 10*time.Second)); v != 10 || err != nil {
		t.Errorf("Ask through %v after the spawn: Await = %d, %v, want 10, nil", p, v, err)
	}
}

// AT-MSG-37 (Ask clause): MSG-110, MSG-310. An Ask whose request the transport refuses returns a
// reply already resolved with the wrapped transport error; with Deliver returning nil it stays
// pending.
func TestATMSG37TransportErrorsAsk(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetDeliverError(errX)
	to := PIDAt[int]("n2", "/user/a")
	r := Ask(sys, to, func(ReplyTo[int]) int { return 1 })
	if !r.resolved.Load() {
		t.Fatal("Ask with a failing transport returned a pending reply")
	}
	_, err := r.Await(context.Background())
	const want = "lockstep: send to lockstep://n2/user/a#0: errX"
	if !errors.Is(err, errX) || err.Error() != want {
		t.Errorf("Await = %v, want an error matching errX with text %q", err, want)
	}
	if err := Send(sys, to, 1); !errors.Is(err, errX) || err.Error() != want {
		t.Errorf("Send = %v, want an error matching errX with text %q", err, want)
	}
	eng.SetDeliverError(nil)
	if err := Send(sys, to, 1); err != nil {
		t.Errorf("Send with Deliver returning nil = %v, want nil", err)
	}
	if r := Ask(sys, to, func(ReplyTo[int]) int { return 1 }); r.resolved.Load() {
		t.Error("Ask with Deliver returning nil returned a resolved reply, want pending")
	}
}

// n2Ask is an ask address allocated by a system on node n2.
var n2Ask = Address{node: "n2", path: "/temp/3", uid: 0x99}

// lastReply returns the payload of the last envelope eng delivered, which must be a reply attempt.
func lastReply(t *testing.T, eng *enginetest.Engine) (engine.Envelope, replyPayload) {
	t.Helper()
	d := eng.Delivered()
	if len(d) == 0 {
		t.Fatal("nothing was delivered")
	}
	env := d[len(d)-1]
	p, ok := env.Payload.(replyPayload)
	if !ok {
		t.Fatalf("the last delivered payload is a %T, want a reply attempt", env.Payload)
	}
	return env, p
}

// MSG-410, MSG-420: a reply attempt for another node's ask goes to Transport.Deliver, after it
// left the sender system's tracked asks, and its error is dropped; one that arrives is applied to
// the asker's table, the first resolving the ask and later ones (or one for an unknown uid) late.
func TestRemoteReplyAttempt(t *testing.T) {
	eng := enginetest.New("n1", 1)
	sys, err := newSystem(eng.Engine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetHost(host{sys})
	silent, stored := spawnSilent(t, sys)
	eng.RunUntilIdle()

	t.Run("outbound", func(t *testing.T) {
		// An ask request from n2 is tracked by silent; silent's answer leaves the tracked asks and
		// travels back through the transport.
		eng.Inbound(engine.Envelope{
			From:    remoteSender.engine(),
			To:      silent.Address().engine(),
			Payload: userPayload{msg: ReplyTo[int]{addr: n2Ask}, ask: n2Ask, typ: typeOf[any]()},
		})
		eng.RunUntilIdle()
		rt := <-stored
		c := cellOf(t, sys, silent)
		if got := sys.asks.trackedBy(c); !slices.Equal(got, []Address{n2Ask}) {
			t.Fatalf("silent tracks %v, want [%v]", got, n2Ask)
		}
		before := len(eng.Delivered())
		send(t, sys, silent, any(answer{5}))
		eng.RunUntilIdle()
		if n := len(eng.Delivered()) - before; n != 1 {
			t.Fatalf("%d envelopes delivered for the answer, want 1", n)
		}
		env, p := lastReply(t, eng)
		if env.From != silent.Address().engine() || env.To != rt.addr.engine() || env.Seq != 0 {
			t.Errorf("reply envelope from %v to %v seq %d, want from %v to %v seq 0", env.From, env.To, env.Seq, silent, rt.addr)
		}
		if p.v != 5 || p.err != nil {
			t.Errorf("reply payload %+v, want value 5 and no error", p)
		}
		if got := sys.asks.trackedBy(c); len(got) != 0 {
			t.Errorf("silent tracks %v after its answer, want none", got)
		}

		// Fail from the system, and a Deliver error that is not returned (there is no result).
		eng.SetDeliverError(errX)
		ReplyTo[int]{addr: n2Ask}.Fail(sys, errX)
		eng.SetDeliverError(nil)
		env, p = lastReply(t, eng)
		if env.From != sys.user.engine() || p.v != nil || p.err != errX { //nolint:errorlint // the very value Fail got
			t.Errorf("Fail's envelope from %v with payload %+v, want from %v with errX", env.From, p, sys.user)
		}
		if n := len(recordsOf(eng.Records(), "lockstep.reply")); n != 0 {
			t.Errorf("%d lockstep.reply records on the replier's system, want none (MSG-420 records on the asker)", n)
		}
	})

	t.Run("no transport", func(t *testing.T) {
		b := engineBundle(t, "n1")
		b.Transport = nil
		nsys, err := newSystem(b, nil)
		if err != nil {
			t.Fatal(err)
		}
		ReplyTo[int]{addr: n2Ask}.Send(nsys, 1) // a dead letter, unreachable: nothing to observe until plan 1.3
	})

	t.Run("inbound", func(t *testing.T) {
		to := PIDAt[int]("n2", "/user/a")
		r := Ask(sys, to, func(ReplyTo[int]) int { return 1 }, WithTimeout(time.Hour))
		req, ok := eng.Delivered()[len(eng.Delivered())-1].Payload.(userPayload)
		if !ok {
			t.Fatal("the request was not delivered")
		}
		ask := req.ask
		replier := Address{node: "n2", path: "/user/a", uid: 0x42}
		inbound := func(to Address, v any, err error) {
			eng.Inbound(engine.Envelope{From: replier.engine(), To: to.engine(), Payload: replyPayload{v: v, err: err}})
		}
		inbound(ask, 7, nil)
		if v, err := r.Await(context.Background()); v != 7 || err != nil {
			t.Errorf("Await = %d, %v, want 7, nil", v, err)
		}
		inbound(ask, 8, nil)
		if v, err := r.Await(context.Background()); v != 7 || err != nil {
			t.Errorf("Await after a second reply = %d, %v, want 7, nil", v, err)
		}
		unknown := Address{node: ask.node, path: ask.path, uid: ask.uid + 1}
		inbound(unknown, 9, nil)
		recs := eng.Records()
		wantResolved := []string{"ask=" + ask.String(), "from=" + replier.String(), "outcome=resolved"}
		wantLate := []string{"ask=" + ask.String(), "from=" + replier.String(), "outcome=late"}
		if got := askRecords(recs, "lockstep.reply", ask); len(got) != 2 || !slices.Equal(got[0], wantResolved) || !slices.Equal(got[1], wantLate) {
			t.Errorf("lockstep.reply records = %q, want %q then %q", got, wantResolved, wantLate)
		}
		wantUnknown := []string{"ask=" + unknown.String(), "from=" + replier.String(), "outcome=late"}
		if got := askRecords(recs, "lockstep.reply", unknown); len(got) != 1 || !slices.Equal(got[0], wantUnknown) {
			t.Errorf("lockstep.reply records for an unknown uid = %q, want one %q", got, wantUnknown)
		}

		// An error arrives unchanged.
		r2 := Ask(sys, to, func(ReplyTo[int]) int { return 2 }, WithTimeout(time.Hour))
		req2, _ := eng.Delivered()[len(eng.Delivered())-1].Payload.(userPayload)
		inbound(req2.ask, nil, errX)
		if v, err := r2.Await(context.Background()); v != 0 || err != errX { //nolint:errorlint // the very value Fail got
			t.Errorf("Await of a failed remote ask = %d, %v, want 0 and errX itself", v, err)
		}
		if n := sys.asks.len(); n != 0 {
			t.Errorf("the asker table holds %d entries, want 0", n)
		}
	})
}

// MSG-350: an ask request that arrives for an actor that is not alive, or for a path with no actor
// of the sender's type, is a dead letter, and the system sends one Fail(ErrTerminated) for the ask
// back through the transport, from the target address.
func TestInboundAskToAbsentTarget(t *testing.T) {
	eng := enginetest.New("n1", 1)
	sys, err := newSystem(eng.Engine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetHost(host{sys})
	a, log := logActor(t, sys, eng)
	targets := []Address{
		{node: "n1", path: a.Address().Path(), uid: a.Address().UID() + 100}, // stopped: a dead uid
		{node: "n1", path: "/user/none"},                                     // not-found: no actor at the path
		{node: "n1", path: a.Address().Path()},                               // not-found: an actor of another type
	}
	for i, to := range targets {
		ask := Address{node: "n2", path: "/temp/" + string(rune('1'+i)), uid: uint64(0x10 + i)}
		before := len(eng.Delivered())
		eng.Inbound(engine.Envelope{
			From:    remoteSender.engine(),
			To:      to.engine(),
			Payload: userPayload{msg: 1, ask: ask, typ: typeOf[string]()},
		})
		eng.RunUntilIdle()
		if n := len(eng.Delivered()) - before; n != 1 {
			t.Fatalf("target %v: %d envelopes delivered, want 1", to, n)
		}
		env, p := lastReply(t, eng)
		if env.From != to.engine() || env.To != ask.engine() || env.Seq != 0 {
			t.Errorf("target %v: reply envelope from %v to %v seq %d, want from the target to %v seq 0", to, env.From, env.To, env.Seq, ask)
		}
		want := "lockstep: actor terminated: " + to.String()
		if p.v != nil || !errors.Is(p.err, ErrTerminated) || p.err.Error() != want {
			t.Errorf("target %v: reply payload %+v, want ErrTerminated with text %q", to, p, want)
		}
	}
	if len(*log) != 0 {
		t.Errorf("a received %v, want nothing", *log)
	}
	if n := len(recordsOf(eng.Records(), "lockstep.reply")); n != 0 {
		t.Errorf("%d lockstep.reply records on the target's system, want none", n)
	}
}

// MSG-340: an ask request delivered twice (a duplicate inbound envelope) is tracked once, so one
// reply attempt leaves the actor tracking nothing.
func TestTrackDuplicateDelivery(t *testing.T) {
	eng := enginetest.New("n1", 1)
	sys, err := newSystem(eng.Engine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetHost(host{sys})
	silent, stored := spawnSilent(t, sys)
	eng.RunUntilIdle()
	env := engine.Envelope{
		From:    remoteSender.engine(),
		To:      silent.Address().engine(),
		Payload: userPayload{msg: ReplyTo[int]{addr: n2Ask}, ask: n2Ask, typ: typeOf[any]()},
	}
	eng.Inbound(env)
	eng.Inbound(env)
	eng.RunUntilIdle()
	<-stored
	<-stored
	c := cellOf(t, sys, silent)
	if got := sys.asks.trackedBy(c); !slices.Equal(got, []Address{n2Ask}) {
		t.Fatalf("silent tracks %v, want [%v] once", got, n2Ask)
	}
	ReplyTo[int]{addr: n2Ask}.Send(sys, 1)
	sys.asks.mu.Lock()
	l := sys.asks.byActor[c]
	sys.asks.mu.Unlock()
	if l != nil {
		t.Errorf("silent's track list survives its only ask's reply: %+v", *l)
	}
}
