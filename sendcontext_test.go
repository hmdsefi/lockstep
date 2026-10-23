// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/mailbox"
)

// sendResult is what one SendContext call on another goroutine returned.
type sendResult struct{ err error }

// sendContextAsync calls SendContext(ctx, sys, pid, msg) on a new goroutine and delivers its
// result on the returned channel.
func sendContextAsync[M any](ctx context.Context, sys *System, pid PID[M], msg M) <-chan sendResult {
	ch := make(chan sendResult, 1)
	go func() { ch <- sendResult{SendContext(ctx, sys, pid, msg)} }()
	return ch
}

// sendReturned returns the result on ch, failing the test if SendContext does not return within
// 10 s.
func sendReturned(t *testing.T, ch <-chan sendResult) error {
	t.Helper()
	select {
	case r := <-ch:
		return r.err
	case <-time.After(10 * time.Second):
		t.Fatal("SendContext did not return")
		return nil
	}
}

// waitingSenders waits up to 10 s until n senders wait on c's mailbox.
func waitingSenders[M any](t *testing.T, c *cell[M], n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for c.user.Waiting() != n {
		if time.Now().After(deadline) {
			t.Fatalf("%d senders wait on %v, want %d", c.user.Waiting(), c.addr, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// lenAttr matches the len attribute, which a record of an entry handed to a waiting sender reads
// while the released target may already be consuming its mailbox.
var lenAttr = regexp.MustCompile(` len=[0-9]+`)

// withoutLen returns lines with the len attribute removed.
func withoutLen(lines []string) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = lenAttr.ReplaceAllString(l, "")
	}
	return out
}

// blockedFull returns the lockstep.mailbox.full record of a message msg from from that meets the
// full BlockSender mailbox of capacity 1 at to, with the action action.
func blockedFull(from, to Address, action, msg string) string {
	return "lockstep.mailbox.full from=" + from.String() + " to=" + to.String() +
		" policy=block-sender action=" + action + " cause=capacity cap=1 len=1 msg=" + msg
}

// spawnCollector spawns an int actor with mailbox (capacity, policy) that hands every message it
// receives to the returned channel.
func spawnCollector(t *testing.T, sys *System, name string, capacity int, policy MailboxPolicy) (PID[int], <-chan int) {
	t.Helper()
	got := make(chan int, 16)
	pid, err := Spawn(sys, name, actorOf[int](&fnActor[int]{receive: func(_ *Context[int], m int) { got <- m }}),
		WithMailbox(capacity, policy))
	if err != nil {
		t.Fatalf("Spawn %q: %v", name, err)
	}
	return pid, got
}

// AT-MBX-12: MBX-070, MBX-090. On the local engine an outside sender to a full BlockSender mailbox
// waits: until its context ends (context.DeadlineExceeded, records blocked then rejected), or until
// the target consumes a message (nil, deliver after blocked).
func TestATMBX12BlockSenderOutsideLocal(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		tp := spawnTarget(t, sys, "t", 1, BlockSender)
		holdToEnd(t, sys, tp.Address())
		send(t, sys, tp, 1)
		before := len(rec.Records())

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := SendContext(ctx, sys, tp, 2); err != context.DeadlineExceeded { // unwrapped (MBX §7)
			t.Errorf("SendContext = %v, want context.DeadlineExceeded unwrapped", err)
		}
		to := tp.Address()
		sameLines(t, "records", rendered(rec.Records(), before, to, "lockstep.deliver", "lockstep.mailbox.full"), []string{
			blockedFull(sys.user, to, "blocked", "2"),
			blockedFull(sys.user, to, "rejected", "2"),
		})
	})

	t.Run("release", func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		tp, got := spawnCollector(t, sys, "t", 1, BlockSender)
		release := hold(t, sys, tp.Address())
		t.Cleanup(release)
		send(t, sys, tp, 1)
		before := len(rec.Records())

		start := time.Now()
		ch := sendContextAsync(context.Background(), sys, tp, 2)
		waitingSenders(t, cellOf(t, sys, tp), 1)
		time.Sleep(10*time.Millisecond - time.Since(start))
		release()
		if err := sendReturned(t, ch); err != nil {
			t.Errorf("SendContext = %v, want nil", err)
		}
		to := tp.Address()
		sameLines(t, "records", withoutLen(rendered(rec.Records(), before, to, "lockstep.deliver", "lockstep.mailbox.full")),
			withoutLen([]string{
				blockedFull(sys.user, to, "blocked", "2"),
				"lockstep.deliver from=" + sys.user.String() + " to=" + to.String() + " seq=0 len=1 msg=2",
			}))
		for _, want := range []int{1, 2} {
			if m := recv(t, got); m != want {
				t.Errorf("received %d, want %d", m, want)
			}
		}
	})
}

// MBX-090: a sender inside an actor never waits. SendContext from a Context to a full BlockSender
// mailbox behaves as FailSend, whatever its context.
func TestSendContextActorSender(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	p, _ := spawnProbe[any](t, sys, "p", nil)
	tp := spawnTarget(t, sys, "t", 1, BlockSender)
	to := tp.Address()
	holdToEnd(t, sys, to)
	send(t, sys, tp, 1)
	before := len(rec.Records())

	for i, ctx := range []context.Context{context.Background(), cancelled()} {
		err, _ := inProbe(t, sys, p, func(c *Context[any]) any { return SendContext(ctx, c, tp, 2+i) }).(error)
		if !errors.Is(err, ErrMailboxFull) || err.Error() != wantFull(to) {
			t.Errorf("SendContext from an actor = %v, want ErrMailboxFull with text %q", err, wantFull(to))
		}
	}
	sameLines(t, "records", rendered(rec.Records(), before, to, "lockstep.deliver", "lockstep.mailbox.full"), []string{
		blockedFull(p.Address(), to, "rejected", "2"),
		blockedFull(p.Address(), to, "rejected", "3"),
	})
	if n := cellOf(t, sys, tp).user.Len(); n != 1 {
		t.Errorf("the mailbox holds %d messages, want 1", n)
	}
}

// MBX §7: SendContext panics for a nil ctx, before it checks the Sender, from an outside sender
// and from an actor alike, and sends nothing.
func TestSendContextNilContext(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	p, _ := spawnProbe[any](t, sys, "p", nil)
	tp := spawnTarget(t, sys, "t", 1, BlockSender)
	const want = "lockstep: SendContext: nil context"
	var nilCtx context.Context
	before := len(rec.Records())

	if v := mustPanic(t, func() { _ = SendContext(nilCtx, sys, tp, 1) }); v != want {
		t.Errorf("SendContext(nil, sys, ...) panicked with %v, want %q", v, want)
	}
	if v := mustPanic(t, func() { _ = SendContext(nilCtx, nil, tp, 2) }); v != want {
		t.Errorf("SendContext(nil, nil, ...) panicked with %v, want %q", v, want)
	}
	v := inProbe(t, sys, p, func(c *Context[any]) (v any) {
		defer func() { v = recover() }()
		_ = SendContext(nilCtx, c, tp, 3)
		return nil
	})
	if v != want {
		t.Errorf("SendContext(nil, ctx, ...) from an actor panicked with %v, want %q", v, want)
	}
	if got := rendered(rec.Records(), before, tp.Address(), "lockstep.send", "lockstep.deliver", "lockstep.mailbox.full"); len(got) != 0 {
		t.Errorf("records after the panicking calls: %q, want none", got)
	}
}

// MBX-090, MBX-150, MBX-160: an outside sender waiting on a BlockSender mailbox whose actor stops
// is woken by the stop's close. Its message becomes a dead letter with reason stopped, like the
// queued one the stop drains, and SendContext returns nil.
func TestSendContextTargetStopsWhileWaiting(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	tp := spawnTarget(t, sys, "t", 1, BlockSender)
	to := tp.Address()
	release := hold(t, sys, to)
	defer release()
	send(t, sys, tp, 1)
	c := cellOf(t, sys, tp)
	before := len(rec.Records())

	ch := sendContextAsync(context.Background(), sys, tp, 2)
	waitingSenders(t, c, 1)
	if err := Stop(sys, to); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	release() // the stop runs before any user message, so the waiter never gets a slot
	if err := sendReturned(t, ch); err != nil {
		t.Errorf("SendContext to an actor that stopped while the sender waited = %v, want nil", err)
	}
	if n := c.user.Waiting(); n != 0 {
		t.Errorf("%d senders still wait, want 0", n)
	}
	from := sys.user.String()
	full := rendered(rec.Records(), before, to, "lockstep.mailbox.full")
	sameLines(t, "mailbox.full records", full, []string{blockedFull(sys.user, to, "blocked", "2")})
	// The drain (msg 1) and the woken sender (msg 2) record on different goroutines.
	dead := rendered(rec.Records(), before, to, "lockstep.deadletter")
	slices.Sort(dead)
	sameLines(t, "dead letters", dead, []string{
		"lockstep.deadletter from=" + from + " to=" + to.String() + " reason=stopped msg=1",
		"lockstep.deadletter from=" + from + " to=" + to.String() + " reason=stopped msg=2",
	})
}

// MBX-070, MBX §7: a context cancelled while the sender waits makes SendContext return
// context.Canceled unwrapped, with the message out of the line and not queued; the records are
// blocked then rejected.
func TestSendContextCancelledWhileWaiting(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	tp := spawnTarget(t, sys, "t", 1, BlockSender)
	to := tp.Address()
	holdToEnd(t, sys, to)
	send(t, sys, tp, 1)
	c := cellOf(t, sys, tp)
	before := len(rec.Records())

	ctx, cancel := context.WithCancel(context.Background())
	ch := sendContextAsync(ctx, sys, tp, 2)
	waitingSenders(t, c, 1)
	if n := sys.waiting.len(); n != 1 {
		t.Errorf("the system lists %d mailboxes with waiting senders, want 1", n)
	}
	cancel()
	if err := sendReturned(t, ch); err != context.Canceled { // unwrapped (MBX §7)
		t.Errorf("SendContext = %v, want context.Canceled unwrapped", err)
	}
	if n, w := c.user.Len(), c.user.Waiting(); n != 1 || w != 0 {
		t.Errorf("the mailbox holds %d messages with %d senders waiting, want 1 and 0", n, w)
	}
	if n := sys.waiting.len(); n != 0 {
		t.Errorf("the system lists %d mailboxes with waiting senders, want none", n)
	}
	sameLines(t, "records", rendered(rec.Records(), before, to, "lockstep.deliver", "lockstep.mailbox.full"), []string{
		blockedFull(sys.user, to, "blocked", "2"),
		blockedFull(sys.user, to, "rejected", "2"),
	})
}

// MBX-070, MBX-090: a context that has ended before the send waits for nothing. With room the
// message is enqueued; on a full mailbox SendContext returns ctx.Err() at once, recorded as a
// rejection without a wait.
func TestSendContextDoneBeforeWaiting(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	tp := spawnTarget(t, sys, "t", 1, BlockSender)
	to := tp.Address()
	holdToEnd(t, sys, to)
	before := len(rec.Records())

	if err := SendContext(cancelled(), sys, tp, 1); err != nil {
		t.Errorf("SendContext with room = %v, want nil", err)
	}
	if err := SendContext(cancelled(), sys, tp, 2); err != context.Canceled { // unwrapped (MBX §7)
		t.Errorf("SendContext to a full mailbox = %v, want context.Canceled unwrapped", err)
	}
	sameLines(t, "records", rendered(rec.Records(), before, to, "lockstep.deliver", "lockstep.mailbox.full"), []string{
		"lockstep.deliver from=" + sys.user.String() + " to=" + to.String() + " seq=0 len=1 msg=1",
		blockedFull(sys.user, to, "rejected", "2"),
	})
}

// MBX-120: an inbound envelope counts as an actor sender, so on a blocking engine a full
// BlockSender mailbox refuses it without waiting.
func TestInboundBlockSenderNeverWaits(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	tp := spawnTarget(t, sys, "t", 1, BlockSender)
	to := tp.Address()
	holdToEnd(t, sys, to)
	send(t, sys, tp, 1)
	before := len(rec.Records())

	host{sys}.Inbound(inbound(to, 4, userPayload{msg: 2}))
	sameLines(t, "records", rendered(rec.Records(), before, to, "lockstep.deliver", "lockstep.mailbox.full"), []string{
		blockedFull(remoteSender, to, "rejected", "2"),
	})
}

// MBX-075: the system lists the mailboxes that outside senders wait on, and rechecking them hands
// a slot to a waiter that is admitted now.
func TestSystemRechecksWaitingMailboxes(t *testing.T) {
	sys, _ := newManualSystem(t)
	q := mailbox.New[entry](2, mailbox.BlockSender)
	var admitted atomic.Bool
	sys.waiting.add(q)
	ch := make(chan mailbox.Outcome, 1)
	go func() {
		out, _, _ := q.OfferWait(context.Background(), entry{msg: 1, kind: entrySent}, admitted.Load)
		ch <- out
	}()
	deadline := time.Now().Add(10 * time.Second)
	for q.Waiting() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the sender does not wait")
		}
		time.Sleep(time.Millisecond)
	}
	sys.waiting.recheck()
	if n := q.SentLen(); n != 0 {
		t.Fatalf("recheck while not admitted enqueued %d entries, want 0", n)
	}
	admitted.Store(true)
	sys.waiting.recheck()
	select {
	case out := <-ch:
		if out != mailbox.Enqueued {
			t.Errorf("OfferWait = %v, want Enqueued", out)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("recheck handed no slot to the admitted waiter")
	}
	sys.waiting.remove(q)
	if n := sys.waiting.len(); n != 0 {
		t.Errorf("the system lists %d mailboxes, want none", n)
	}
}

// MBX §4.1, MBX §8: for a path address (PIDAt) the error of a full mailbox and the to attribute of
// lockstep.deliver and lockstep.mailbox.full name the address as the sender named it.
func TestPathAddressInRecordsAndErrors(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	tp := spawnTarget(t, sys, "t", 1, FailSend)
	holdToEnd(t, sys, tp.Address())
	at := PIDAt[int](sys.Node(), "/user/t")
	to := at.Address()
	before := len(rec.Records())

	if err := Send(sys, at, 1); err != nil {
		t.Fatalf("Send to the path address = %v", err)
	}
	if err := Send(sys, at, 2); !errors.Is(err, ErrMailboxFull) || err.Error() != wantFull(to) {
		t.Errorf("Send to the full mailbox = %v, want ErrMailboxFull with text %q", err, wantFull(to))
	}
	from := sys.user.String()
	sameLines(t, "records", rendered(rec.Records(), before, to, "lockstep.deliver", "lockstep.mailbox.full"), []string{
		"lockstep.deliver from=" + from + " to=" + to.String() + " seq=0 len=1 msg=1",
		"lockstep.mailbox.full from=" + from + " to=" + to.String() + " policy=fail-send action=rejected" +
			" cause=capacity cap=1 len=1 msg=2",
	})
}

// MSG-345: askRefused fails only an ask that a mailbox's actor tracks. An ask that no actor
// tracks (here a NewReply ask; in a race, one the target's stop has failed already) is left alone.
func TestAskRefusedUntrackedAsk(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	tp := spawnTarget(t, sys, "t", 1, FailSend)
	rt, r := NewReply[int](sys, WithTimeout(time.Hour))
	sys.askRefused(rt.addr, tp.Address(), mailboxFullError(tp.Address()))
	if r.resolved.Load() {
		t.Error("askRefused resolved an ask that no actor tracks")
	}
	for _, rr := range rec.Records() {
		if rr.Kind == "lockstep.reply" {
			t.Errorf("unexpected record %s", render(rr))
		}
	}
}
