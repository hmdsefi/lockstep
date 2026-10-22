// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// answerReq is a plain message that carries a ReplyTo made by NewReply; its receiver answers it.
type answerReq struct{ rt ReplyTo[int] }

// AT-MSG-35 (all but the shutdown clause): MSG-380, MSG-390. NewReply records lockstep.ask with
// to=<zero> and an empty msg; its ReplyTo travels in a plain message and resolves the reply; an
// unanswered one times out naming <zero> as the target; misuse panics.
func TestATMSG35NewReply(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	answerer, err := Spawn(sys, "answerer", actorOf[answerReq](&fnActor[answerReq]{
		receive: func(ctx *Context[answerReq], m answerReq) { m.rt.Send(ctx, 9) },
	}))
	if err != nil {
		t.Fatal(err)
	}

	rt, r := NewReply[int](sys, WithTimeout(time.Hour))
	a := rt.addr
	if a.node != "local" || a.path != "/temp/1" || a.uid == 0 {
		t.Fatalf("ask address %v, want node local, path /temp/1 and a uid", a)
	}
	if want := "lockstep://local/temp/1#" + strconv.FormatUint(a.uid, 16); a.String() != want {
		t.Errorf("ask address renders %q, want %q", a, want)
	}
	want := []string{"from=" + sys.user.String(), "to=<zero>", "ask=" + a.String(), "timeout=1h0m0s", "msg="}
	if recs := recordsOf(rec.Records(), "lockstep.ask"); len(recs) != 1 || !slices.Equal(recs[0], want) {
		t.Errorf("lockstep.ask records = %q, want one %q", recs, want)
	}
	if r.resolved.Load() {
		t.Fatal("the reply of NewReply is resolved before anyone answered")
	}
	send(t, sys, answerer, answerReq{rt})
	if v, err := r.Await(within(t, time.Second)); v != 9 || err != nil {
		t.Errorf("Await = %d, %v, want 9, nil", v, err)
	}
	resolved := []string{"ask=" + a.String(), "from=" + answerer.String(), "outcome=resolved"}
	if recs := askRecords(rec.Records(), "lockstep.reply", a); len(recs) != 1 || !slices.Equal(recs[0], resolved) {
		t.Errorf("lockstep.reply records = %q, want one %q", recs, resolved)
	}

	rt2, r2 := NewReply[int](sys, WithTimeout(50*time.Millisecond))
	v, err := r2.Await(within(t, time.Second))
	wantErr := fmt.Sprintf("lockstep: timeout: ask %s to <zero> after 50ms", rt2.addr)
	if v != 0 || !errors.Is(err, ErrTimeout) || err.Error() != wantErr {
		t.Errorf("unanswered NewReply: Await = %d, %v, want 0 and ErrTimeout with text %q", v, err, wantErr)
	}
	if rt2.addr.path != "/temp/2" {
		t.Errorf("the second NewReply has path %q, want /temp/2", rt2.addr.path)
	}
	wantRec := []string{"ask=" + rt2.addr.String(), "to=<zero>", "after=50ms"}
	if recs := askRecords(rec.Records(), "lockstep.timeout", rt2.addr); len(recs) != 1 || !slices.Equal(recs[0], wantRec) {
		t.Errorf("lockstep.timeout records = %q, want one %q", recs, wantRec)
	}

	if v := mustPanic(t, func() { NewReply[int](nil) }); v != "lockstep: nil Sender" {
		t.Errorf("NewReply[int](nil) panicked with %v", v)
	}
	if v := mustPanic(t, func() { NewReply[int](sys, nil) }); v != "lockstep: nil AskOption" {
		t.Errorf("NewReply[int](sys, nil) panicked with %v", v)
	}
	if v := mustPanic(t, func() { NewReply[int](nil, nil) }); v != "lockstep: nil Sender" {
		t.Errorf("NewReply[int](nil, nil) panicked with %v, want the nil Sender check first", v)
	}
	if n := sys.asks.len(); n != 0 {
		t.Errorf("the asker table holds %d entries after the panics, want 0", n)
	}
}

// AT-MSG-28 (NewReply clause): MSG-700, MSG-380. An invalid final timeout returns the zero ReplyTo
// and a resolved reply; nothing is allocated or recorded.
func TestATMSG28TimeoutOptionNewReply(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	before := messageRecords(rec.Records())
	rt, r := NewReply[int](sys, WithTimeout(-time.Second))
	if rt != (ReplyTo[int]{}) {
		t.Errorf("NewReply with WithTimeout(-1s) returned ReplyTo %v, want the zero ReplyTo", rt.addr)
	}
	v, err := r.Await(cancelled())
	const want = "lockstep: invalid option: WithTimeout(-1s): timeout must be positive"
	if v != 0 || !errors.Is(err, ErrInvalidOption) || err.Error() != want {
		t.Errorf("Await = %d, %v, want 0 and ErrInvalidOption with text %q", v, err, want)
	}
	if n := messageRecords(rec.Records()) - before; n != 0 {
		t.Errorf("%d records emitted, want none", n)
	}
	if n := sys.asks.len(); n != 0 {
		t.Errorf("the asker table holds %d entries, want 0", n)
	}
	// The last option wins, and the invalid call allocated no ask number.
	rt, r = NewReply[int](sys, WithTimeout(0), WithTimeout(time.Hour))
	if r.resolved.Load() || rt.addr.path != "/temp/1" {
		t.Errorf("NewReply with WithTimeout(0), WithTimeout(time.Hour): resolved %v, path %q; want pending, /temp/1",
			r.resolved.Load(), rt.addr.path)
	}
}

// AT-MSG-39 (NewReply clause): MSG-320, ENG-070. Ask and NewReply each arm one
// lockstep/ask-timeout timer; both resolve with ErrTimeout after the default 5 s.
func TestATMSG39NewReplyTimerLabel(t *testing.T) {
	sys, eng := newManualSystem(t)
	silent, _ := spawnSilent(t, sys)
	eng.RunUntilIdle()
	r1 := askSilent(sys, silent)
	_, r2 := NewReply[int](sys)
	if labels := eng.Labels(); !slices.Equal(labels, []string{askTimeoutLabel, askTimeoutLabel}) {
		t.Fatalf("Labels() = %q, want lockstep/ask-timeout twice", labels)
	}
	eng.RunUntilIdle()
	eng.Advance(5*time.Second - time.Nanosecond)
	if r2.resolved.Load() {
		t.Fatal("the NewReply resolved before its 5 s timeout")
	}
	eng.Advance(time.Nanosecond)
	for i, r := range []*Reply[int]{r1, r2} {
		if _, err := r.Await(context.Background()); !errors.Is(err, ErrTimeout) {
			t.Errorf("reply %d: Await after Advance(5s) = %v, want ErrTimeout", i+1, err)
		}
	}
}

// askPeer tells a peer actor to ask to with a one-hour timeout and to answer nothing.
type askPeer struct{ to PID[any] }

// AT-MSG-31: MSG-370. Two actors ask each other and answer nothing: the table lists both pending
// asks in counter order with their askers, targets and timers. A NewReply from the system follows
// with the system address as asker and the zero target.
func TestATMSG31PendingAsks(t *testing.T) {
	sys, eng := newManualSystem(t)
	peer := func(ctx *Context[any], m any) {
		if p, ok := m.(askPeer); ok {
			askSilent(ctx, p.to, WithTimeout(time.Hour))
		}
	}
	a, _ := spawnProbe(t, sys, "a", peer)
	b, _ := spawnProbe(t, sys, "b", peer)
	eng.RunUntilIdle()
	send(t, sys, a, any(askPeer{b}))
	eng.RunUntilIdle()
	send(t, sys, b, any(askPeer{a}))
	eng.RunUntilIdle()
	rt, _ := NewReply[int](sys)

	got := sys.pendingAsks()
	if len(got) != 3 {
		t.Fatalf("pendingAsks() lists %d asks, want 3", len(got))
	}
	want := []struct {
		path          string
		asker, target Address
	}{
		{"/temp/1", a.Address(), b.Address()},
		{"/temp/2", b.Address(), a.Address()},
		{"/temp/3", sys.user, Address{}},
	}
	for i, w := range want {
		p := got[i]
		if p.addr.path != w.path || p.asker != w.asker || p.target != w.target || p.cancel == nil {
			t.Errorf("pending ask %d = {addr %v, asker %v, target %v, timer set %v}, want {path %s, asker %v, target %v, timer set}",
				i, p.addr, p.asker, p.target, p.cancel != nil, w.path, w.asker, w.target)
		}
	}
	if got[2].addr != rt.addr {
		t.Errorf("the third pending ask is %v, want the NewReply's %v", got[2].addr, rt.addr)
	}

	// A resolved ask leaves the list; the others keep their order.
	rt.Send(sys, 1)
	if got := sys.pendingAsks(); len(got) != 2 || got[0].addr.path != "/temp/1" || got[1].addr.path != "/temp/2" {
		t.Errorf("pendingAsks() after one answer lists %d asks, want /temp/1 and /temp/2", len(got))
	}
}

// MSG-390, R47: a request that carries a NewReply's ReplyTo is an ordinary Send. Its error returns
// to the caller and leaves the reply pending until the caller answers it. (1.2-13 adds the
// ErrMailboxFull form of AT-MSG-40.)
func TestNewReplySendError(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetDeliverError(errX)
	rt, r := NewReply[int](sys, WithTimeout(time.Hour))
	err := Send(sys, PIDAt[answerReq]("n2", "/user/t"), answerReq{rt})
	if !errors.Is(err, errX) {
		t.Fatalf("Send = %v, want an error wrapping errX", err)
	}
	if r.resolved.Load() || sys.asks.len() != 1 {
		t.Fatalf("after the failed Send: resolved %v, %d table entries; want pending, 1", r.resolved.Load(), sys.asks.len())
	}
	rt.Fail(sys, err)
	if v, got := r.Await(context.Background()); v != 0 || got != err { //nolint:errorlint // the very value Fail got
		t.Errorf("Await = %d, %v, want 0 and the Send error itself", v, got)
	}
}

// fwdMsg is the message of the forwarding pair: A gets the ask, B the forwarded ReplyTo.
type fwdMsg struct{ rt ReplyTo[int] }

// AT-MSG-17 (answer clause): MSG-340, MSG-400, MSG-410. A forwards the ReplyTo of an ask it
// received to B, which answers 7: the asker gets 7 from B, and the tracked ask left A's system.
func TestATMSG17ForwardingAnswer(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	b, err := Spawn(sys, "b", actorOf[fwdMsg](&fnActor[fwdMsg]{
		receive: func(ctx *Context[fwdMsg], m fwdMsg) { m.rt.Send(ctx, 7) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	a, err := Spawn(sys, "a", actorOf[fwdMsg](&fnActor[fwdMsg]{
		receive: func(ctx *Context[fwdMsg], m fwdMsg) {
			if err := Send(ctx, b, m); err != nil {
				panic(err)
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var rt ReplyTo[int]
	r := Ask(sys, a, func(x ReplyTo[int]) fwdMsg { rt = x; return fwdMsg{x} })
	if v, err := r.Await(within(t, time.Second)); v != 7 || err != nil {
		t.Fatalf("Await = %d, %v, want 7, nil", v, err)
	}
	want := []string{"ask=" + rt.addr.String(), "from=" + b.String(), "outcome=resolved"}
	if recs := askRecords(rec.Records(), "lockstep.reply", rt.addr); len(recs) != 1 || !slices.Equal(recs[0], want) {
		t.Errorf("lockstep.reply records = %q, want one %q", recs, want)
	}
	if got := sys.asks.trackedBy(cellOf(t, sys, a)); len(got) != 0 {
		t.Errorf("a still tracks %v after b answered, want none", got)
	}
}

// MSG-420 (amended): a reply value whose dynamic type is not the ask's R resolves the ask with an
// error wrapping ErrWrongType, recorded as the resolution's err; a nil value is the zero R.
func TestReplyWrongType(t *testing.T) {
	wrongType := func(t *testing.T, r *Reply[int], ask Address, got string, recs func() [][]string) {
		t.Helper()
		v, err := r.Await(context.Background())
		want := fmt.Sprintf("lockstep: wrong message type: reply to %s: got %s, want int", ask, got)
		if v != 0 || !errors.Is(err, ErrWrongType) || err.Error() != want {
			t.Errorf("Await = %d, %v, want 0 and ErrWrongType with text %q", v, err, want)
		}
		if rs := recs(); len(rs) != 1 || !slices.Contains(rs[0], "outcome=resolved") || !slices.Contains(rs[0], "err="+want) {
			t.Errorf("lockstep.reply records = %q, want one outcome=resolved with err=%q", rs, want)
		}
	}

	t.Run("local conversion", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		rt, r := NewReply[int](sys, WithTimeout(time.Hour))
		ReplyTo[int64](rt).Send(sys, 5)
		wrongType(t, r, rt.addr, "int64", func() [][]string { return askRecords(eng.Records(), "lockstep.reply", rt.addr) })
		if n := sys.asks.len(); n != 0 {
			t.Errorf("the asker table holds %d entries, want 0", n)
		}
	})

	t.Run("remote envelope", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		eng.SetHost(host{sys})
		rt, r := NewReply[int](sys, WithTimeout(time.Hour))
		eng.Inbound(engine.Envelope{From: remoteSender.engine(), To: rt.addr.engine(), Payload: replyPayload{v: "x"}})
		wrongType(t, r, rt.addr, "string", func() [][]string { return askRecords(eng.Records(), "lockstep.reply", rt.addr) })

		rt2, r2 := NewReply[int](sys, WithTimeout(time.Hour))
		eng.Inbound(engine.Envelope{From: remoteSender.engine(), To: rt2.addr.engine(), Payload: replyPayload{}})
		if v, err := r2.Await(context.Background()); v != 0 || err != nil {
			t.Errorf("a nil remote value: Await = %d, %v, want 0, nil", v, err)
		}
	})

	t.Run("interface R", func(t *testing.T) {
		sys, _ := newManualSystem(t)
		rt, r := NewReply[acctMsg](sys, WithTimeout(time.Hour))
		ReplyTo[any](rt).Send(sys, Deposit{3})
		if v, err := r.Await(context.Background()); v != (Deposit{3}) || err != nil {
			t.Errorf("a value implementing R: Await = %v, %v, want Deposit{3}, nil", v, err)
		}
	})
}

// reentrantErr is an error whose Error method uses the asker table, as user code may.
type reentrantErr struct{ sys *System }

func (e reentrantErr) Error() string { return "pending " + strconv.Itoa(e.sys.asks.len()) }

// The lockstep.reply record describes the attempt's error (user code: Error, String) before the
// ask table's lock is taken, so that code may use the table.
func TestReplyErrorDescribedOutsideTableLock(t *testing.T) {
	sys, eng := newManualSystem(t)
	rt, r := NewReply[int](sys, WithTimeout(time.Hour))
	done := make(chan struct{})
	go func() { // a deadlocked Fail fails the test instead of hanging it
		defer close(done)
		rt.Fail(sys, reentrantErr{sys})
		rt.Fail(sys, reentrantErr{sys}) // late
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ReplyTo.Fail deadlocked: the error was described under the table's lock")
	}
	if _, err := r.Await(context.Background()); !errors.As(err, new(reentrantErr)) {
		t.Errorf("Await = %v, want the reentrant error", err)
	}
	recs := askRecords(eng.Records(), "lockstep.reply", rt.addr)
	if len(recs) != 2 || !slices.Contains(recs[0], "err=pending 1") || !slices.Contains(recs[1], "outcome=late") {
		t.Errorf("lockstep.reply records = %q, want resolved with err=pending 1, then late", recs)
	}
}
