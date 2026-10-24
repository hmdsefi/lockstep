// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// gated is an any actor that the test keeps from consuming its mailbox until it releases it: its
// Receive blocks on the message "block" (spawnGated), or the cell is held (spawnHeld). It hands
// every message it receives to got, in order, and answers a ReplyTo[int] with 7.
type gated struct {
	pid     PID[any]
	got     chan any
	gate    chan struct{}
	release func()
}

// spawnGated spawns a gated actor named name with WithMailbox(1, policy), sends it "block" and
// returns once its turn is blocked on it, so its mailbox is empty and a later message is queued.
// The actor is released when the test ends, before the system shuts down.
func spawnGated(t *testing.T, sys *System, name string, policy MailboxPolicy) *gated {
	t.Helper()
	g := newGated(t, sys, name, policy)
	var once sync.Once
	g.release = func() { once.Do(func() { close(g.gate) }) }
	t.Cleanup(g.release)
	send(t, sys, g.pid, any("block"))
	if m := recv(t, g.got); m != "block" {
		t.Fatalf("%v received %v first, want block", g.pid, m)
	}
	return g
}

// spawnHeld spawns a gated actor named name with WithMailbox(1, policy) and holds its cell (MBX
// §10), so its mailbox fills while the local engine's loop goroutine stays free to run other turns
// and timers. The actor is released when the test ends, before the system shuts down.
func spawnHeld(t *testing.T, sys *System, name string, policy MailboxPolicy) *gated {
	t.Helper()
	g := newGated(t, sys, name, policy)
	var once sync.Once
	unhold := hold(t, sys, g.pid.Address())
	g.release = func() { once.Do(unhold) }
	t.Cleanup(g.release)
	return g
}

// newGated spawns the actor of a gated value with WithMailbox(1, policy); the caller sets release.
func newGated(t *testing.T, sys *System, name string, policy MailboxPolicy) *gated {
	t.Helper()
	g := &gated{got: make(chan any, 16), gate: make(chan struct{})}
	pid, err := Spawn(sys, name, actorOf[any](&fnActor[any]{receive: func(ctx *Context[any], m any) {
		g.got <- m
		switch m := m.(type) {
		case string:
			if m == "block" {
				<-g.gate
			}
		case ReplyTo[int]:
			m.Send(ctx, 7)
		}
	}}), WithMailbox(1, policy))
	if err != nil {
		t.Fatal(err)
	}
	g.pid = pid
	return g
}

// rest releases g and returns, in order, the messages it receives (after "block") up to last, then
// "end", which rest sends once g has received last: anything g received in between shows up.
func (g *gated) rest(t *testing.T, sys *System, last any) []any {
	t.Helper()
	g.release()
	var out []any
	for {
		m := recv(t, g.got)
		out = append(out, m)
		if m == last {
			break
		}
	}
	send(t, sys, g.pid, any("end"))
	out = append(out, recv(t, g.got))
	return out
}

// sameMessages fails the test unless got equals want.
func sameMessages(t *testing.T, got []any, want ...any) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("received %v, want %v", got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("received %v, want %v", got, want)
			return
		}
	}
}

// AT-MSG-12: MSG-310, MSG-345. The target's Receive is blocked on a test channel and one message
// is queued in its mailbox of capacity 1. An ask refused by FailSend, or dropped by DropNewest,
// returns a reply already resolved with ErrMailboxFull from the target, recorded before Ask
// returns; DropNewest keeps the queued message. Under DropOldest the queued ask is resolved the
// same way before the Send that evicts it returns. The target never receives a refused or dropped
// request.
func TestATMSG12AskToFullMailbox(t *testing.T) {
	for _, policy := range []MailboxPolicy{FailSend, DropNewest} {
		t.Run(policy.String(), func(t *testing.T) {
			sys, rec := newRecordingSystem(t)
			g := spawnGated(t, sys, "t", policy)
			send(t, sys, g.pid, any("queued"))
			resolvedWith(t, rec, askTo(sys, g.pid), g.pid.Address())
			sameMessages(t, g.rest(t, sys, "queued"), "queued", "end")
		})
	}

	t.Run(DropOldest.String(), func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		g := spawnGated(t, sys, "t", DropOldest)
		queued := askTo(sys, g.pid)
		if queued.resolved.Load() {
			t.Fatal("the queued ask resolved before it was evicted")
		}
		send(t, sys, g.pid, any("later")) // evicts the ask request
		resolvedWith(t, rec, queued, g.pid.Address())
		sameMessages(t, g.rest(t, sys, "later"), "later", "end")
	})
}

// askAsync calls Ask(sys, pid, ...) with the request rt itself on a new goroutine, as a test
// goroutine that may wait, and returns the reply once Ask returns, within 10 s.
func askAsync(t *testing.T, sys *System, pid PID[any], opts ...AskOption) <-chan *Reply[int] {
	t.Helper()
	ch := make(chan *Reply[int], 1)
	go func() { ch <- Ask(sys, pid, func(rt ReplyTo[int]) any { return rt }, opts...) }()
	return ch
}

// mailboxActions returns the action attribute of each lockstep.mailbox.full record from index from
// on whose to attribute is to, in order.
func mailboxActions(recs []engine.Record, from int, to Address) []string {
	var out []string
	for _, r := range rendered(recs, from, to, "lockstep.mailbox.full") {
		for f := range strings.FieldsSeq(r) {
			if a, ok := strings.CutPrefix(f, "action="); ok {
				out = append(out, a)
			}
		}
	}
	return out
}

// AT-MSG-34 (local clause): MSG-115, MSG-330. A *System sender's Ask to a full BlockSender mailbox
// on a blocking engine waits for room. The target is held with one queued message rather than
// blocked in Receive: the local engine runs every turn and timer on one goroutine, so a blocked
// Receive would also keep the ask's timer from firing. The wait ends when the ask resolves: at its timeout the
// request is not enqueued, Ask returns the reply resolved with ErrTimeout, and the target receives
// only the message its turn blocked on and the one queued before. The wait also ends when the
// request is enqueued, and the target then answers it. An actor's Ask never waits: BlockSender acts
// as FailSend and the ask resolves with ErrMailboxFull at once.
func TestATMSG34AskBlockSender(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		g := spawnHeld(t, sys, "t", BlockSender)
		send(t, sys, g.pid, any("queued"))
		to := g.pid.Address()
		before := len(rec.Records())
		r := recv(t, askAsync(t, sys, g.pid, WithTimeout(50*time.Millisecond)))
		if !r.resolved.Load() {
			t.Fatal("Ask returned before its wait ended")
		}
		_, err := r.Await(cancelled())
		want := "lockstep: timeout: ask " + r.ask.String() + " to " + to.String() + " after 50ms"
		if !errors.Is(err, ErrTimeout) || err.Error() != want {
			t.Errorf("Await = %v, want ErrTimeout with text %q", err, want)
		}
		sameLines(t, "mailbox actions", mailboxActions(rec.Records(), before, to), []string{"blocked", "rejected"})
		if got := askRecords(rec.Records(), "lockstep.reply", r.ask); len(got) != 0 {
			t.Errorf("lockstep.reply records of the ask = %q, want none", got)
		}
		if got := sys.asks.trackedBy(cellOf(t, sys, g.pid)); len(got) != 0 {
			t.Errorf("the target tracks %v, want no ask", got)
		}
		sameMessages(t, g.rest(t, sys, "queued"), "queued", "end")
	})

	t.Run("enqueued", func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		g := spawnHeld(t, sys, "t", BlockSender)
		send(t, sys, g.pid, any("queued"))
		to := g.pid.Address()
		before := len(rec.Records())
		ch := askAsync(t, sys, g.pid, WithTimeout(time.Hour))
		waitingSenders(t, cellOf(t, sys, g.pid), 1)
		g.release()
		r := recv(t, ch)
		if v, err := r.Await(within(t, 10*time.Second)); v != 7 || err != nil {
			t.Errorf("Await = %d, %v, want 7, nil", v, err)
		}
		sameLines(t, "mailbox actions", mailboxActions(rec.Records(), before, to), []string{"blocked"})
	})

	t.Run("actor sender", func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		g := spawnHeld(t, sys, "t", BlockSender)
		send(t, sys, g.pid, any("queued"))
		w, _ := spawnProbe[any](t, sys, "w", nil)
		r, _ := inProbe(t, sys, w, func(ctx *Context[any]) any {
			return Ask(ctx, g.pid, func(rt ReplyTo[int]) any { return rt }, WithTimeout(time.Hour))
		}).(*Reply[int])
		resolvedWith(t, rec, r, g.pid.Address())
	})
}

// AT-MSG-40 (FailSend and DropOldest clauses): MSG-390. The request of a NewReply ask is an ordinary
// Send to a held target with one queued message. Under FailSend, Send's ErrMailboxFull returns to
// the caller and leaves the reply pending until the caller fails it with that error. Under
// DropOldest, the queued request that a later Send evicts is not reported: the reply stays pending
// until its timeout, and no lockstep.reply is recorded for it. (The DropNewest clause is the last
// part of TestATMBX24AsksMeetFullMailbox.)
func TestATMSG40NewReplyPolicies(t *testing.T) {
	setup := func(t *testing.T, policy MailboxPolicy) (*System, *enginetest.Recorder, PID[any]) {
		t.Helper()
		sys, rec := newRecordingSystem(t)
		pid, err := Spawn(sys, "t", idle[any](), WithMailbox(1, policy))
		if err != nil {
			t.Fatal(err)
		}
		holdToEnd(t, sys, pid.Address())
		return sys, rec, pid
	}

	t.Run(FailSend.String(), func(t *testing.T) {
		sys, _, pid := setup(t, FailSend)
		send(t, sys, pid, any(0))
		rt, r := NewReply[int](sys, WithTimeout(50*time.Millisecond))
		err := Send(sys, pid, any(answerReq{rt}))
		if !errors.Is(err, ErrMailboxFull) || err.Error() != wantFull(pid.Address()) {
			t.Fatalf("Send = %v, want ErrMailboxFull with text %q", err, wantFull(pid.Address()))
		}
		if r.isResolved() || len(sys.pendingAsks()) != 1 {
			t.Fatalf("after the refused Send: resolved %v, %d pending asks; want pending, 1", r.isResolved(), len(sys.pendingAsks()))
		}
		rt.Fail(sys, err)
		if v, got := r.Await(within(t, 10*time.Second)); v != 0 || got != err { //nolint:errorlint // the very value Fail got
			t.Errorf("Await = %d, %v, want 0 and the Send error itself", v, got)
		}
	})

	t.Run(DropOldest.String(), func(t *testing.T) {
		sys, rec, pid := setup(t, DropOldest)
		rt, r := NewReply[int](sys, WithTimeout(50*time.Millisecond))
		send(t, sys, pid, any(answerReq{rt}))
		send(t, sys, pid, any(1)) // evicts the request
		if r.isResolved() {
			t.Fatal("the NewReply ask resolved when its request was evicted")
		}
		if _, err := r.Await(within(t, 10*time.Second)); !errors.Is(err, ErrTimeout) {
			t.Errorf("Await = %v, want ErrTimeout", err)
		}
		if got := askRecords(rec.Records(), "lockstep.reply", r.ask); len(got) != 0 {
			t.Errorf("lockstep.reply records of the ask = %q, want none", got)
		}
	})
}
