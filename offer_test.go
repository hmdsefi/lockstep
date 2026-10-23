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

// render returns rec as "kind key=value key=value", for comparing records as text.
func render(rec engine.Record) string {
	parts := []string{rec.Kind}
	for _, a := range rec.Attrs {
		parts = append(parts, a.Key+"="+a.Value)
	}
	return strings.Join(parts, " ")
}

// rendered returns the records of recs from index from on whose kind is one of kinds and whose to
// attribute is to, rendered, in order.
func rendered(recs []engine.Record, from int, to Address, kinds ...string) []string {
	var out []string
	for _, r := range recs[from:] {
		isKind := false
		for _, k := range kinds {
			isKind = isKind || r.Kind == k
		}
		if !isKind {
			continue
		}
		for _, a := range r.Attrs {
			if a.Key == "to" && a.Value == to.String() {
				out = append(out, render(r))
			}
		}
	}
	return out
}

// renderKind returns the records of recs of kind, rendered, in order.
func renderKind(recs []engine.Record, kind string) []string {
	var out []string
	for _, r := range recs {
		if r.Kind == kind {
			out = append(out, render(r))
		}
	}
	return out
}

// sameLines fails the test unless got equals want.
func sameLines(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s:\n got %q\nwant %q", what, got, want)
	}
}

// holdToEnd holds the cell at addr and releases it when the test ends, before the system shuts
// down, so the shutdown can stop it.
func holdToEnd(t *testing.T, sys *System, addr Address) {
	t.Helper()
	t.Cleanup(hold(t, sys, addr))
}

// wantFull returns the text of the error of a full mailbox at addr.
func wantFull(addr Address) string { return "lockstep: mailbox full: " + addr.String() }

// spawnTarget spawns an idle int actor with a mailbox of the given capacity and policy.
func spawnTarget(t *testing.T, sys *System, name string, capacity int, policy MailboxPolicy) PID[int] {
	t.Helper()
	pid, err := Spawn(sys, name, idle[int](), WithMailbox(capacity, policy))
	if err != nil {
		t.Fatalf("Spawn %q: %v", name, err)
	}
	return pid
}

// AT-MBX-11: MBX-090, MBX-092, MBX-150, MBX §8. One subtest per row of the send result table
// that neither waits nor sheds. The target has capacity 1 and is held; the records are the ones
// naming it, in order.
func TestATMBX11SendResultTable(t *testing.T) {
	type row struct {
		name   string
		policy MailboxPolicy
		full   bool // the mailbox holds one message
		actor  bool // the sender is an actor
		closed bool // the target has stopped
		err    bool // Send returns ErrMailboxFull
		recs   func(from, to string, policy string) []string
	}
	full := func(action, policy, from, to, dropped string) string {
		s := "lockstep.mailbox.full from=" + from + " to=" + to + " policy=" + policy + " action=" + action +
			" cause=capacity cap=1 len=1 msg=2"
		if dropped != "" {
			s += " dropped=" + dropped
		}
		return s
	}
	deliver := func(from, to string) string {
		return "lockstep.deliver from=" + from + " to=" + to + " seq=0 len=1 msg=2"
	}
	rows := []row{
		{name: "room", policy: FailSend, recs: func(from, to, _ string) []string {
			return []string{deliver(from, to)}
		}},
		{name: "FailSend full", policy: FailSend, full: true, err: true, recs: func(from, to, p string) []string {
			return []string{full("rejected", p, from, to, "")}
		}},
		{name: "DropNewest full", policy: DropNewest, full: true, recs: func(from, to, p string) []string {
			return []string{full("dropped-newest", p, from, to, "")}
		}},
		{name: "DropOldest full", policy: DropOldest, full: true, recs: func(from, to, p string) []string {
			return []string{full("dropped-oldest", p, from, to, "1"), deliver(from, to)}
		}},
		{name: "BlockSender full, actor sender", policy: BlockSender, full: true, actor: true, err: true,
			recs: func(from, to, p string) []string { return []string{full("rejected", p, from, to, "")} }},
		{name: "closed", policy: FailSend, closed: true, recs: func(from, to, _ string) []string {
			return []string{"lockstep.deadletter from=" + from + " to=" + to + " reason=stopped msg=2"}
		}},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			sys, rec := newRecordingSystem(t)
			w, wp := spawnProbe[any](t, sys, "w", nil)
			p, _ := spawnProbe[any](t, sys, "p", nil)
			tp := spawnTarget(t, sys, "t", 1, r.policy)
			to := tp.Address()
			switch {
			case r.closed:
				stopAndWait(t, sys, w, wp, to)
			default:
				holdToEnd(t, sys, to)
				if r.full {
					send(t, sys, tp, 1)
				}
			}
			from := sys.user
			before := len(rec.Records())
			var err error
			if r.actor {
				from = p.Address()
				err, _ = inProbe(t, sys, p, func(ctx *Context[any]) any { return Send(ctx, tp, 2) }).(error)
			} else {
				err = Send(sys, tp, 2)
			}
			switch {
			case !r.err && err != nil:
				t.Errorf("Send = %v, want nil", err)
			case r.err && (!errors.Is(err, ErrMailboxFull) || err.Error() != wantFull(to)):
				t.Errorf("Send = %v, want ErrMailboxFull with text %q", err, wantFull(to))
			}
			sameLines(t, "records", rendered(rec.Records(), before, to, "lockstep.deliver", "lockstep.mailbox.full", "lockstep.deadletter"),
				r.recs(from.String(), to.String(), r.policy.String()))
		})
	}

	// Eight goroutines send to one held actor: the outcomes add up, whatever the interleaving.
	t.Run("concurrent senders", func(t *testing.T) {
		const senders, each, capacity = 8, 50, 100
		for _, policy := range []MailboxPolicy{FailSend, DropNewest, DropOldest} {
			sys, rec := newRecordingSystem(t)
			tp := spawnTarget(t, sys, "t", capacity, policy)
			holdToEnd(t, sys, tp.Address())
			var wg sync.WaitGroup
			var mu sync.Mutex
			var ok, refused int
			for range senders {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range each {
						err := Send(sys, tp, i)
						mu.Lock()
						switch {
						case err == nil:
							ok++
						case errors.Is(err, ErrMailboxFull):
							refused++
						default:
							t.Errorf("Send = %v", err)
						}
						mu.Unlock()
					}
				}()
			}
			wg.Wait()
			count := func(kind, action string) int {
				n := 0
				for _, r := range rec.Records() {
					if r.Kind == kind && (action == "" || strings.Contains(render(r), " action="+action+" ")) {
						n++
					}
				}
				return n
			}
			total := senders * each
			wantOK, wantRefused := total, 0
			if policy == FailSend {
				wantOK, wantRefused = capacity, total-capacity
			}
			if ok != wantOK || refused != wantRefused {
				t.Errorf("%v: %d sends returned nil and %d ErrMailboxFull, want %d and %d", policy, ok, refused, wantOK, wantRefused)
			}
			wantDeliver := capacity
			if policy == DropOldest {
				wantDeliver = total
			}
			if n := count("lockstep.deliver", ""); n != wantDeliver {
				t.Errorf("%v: %d lockstep.deliver records, want %d", policy, n, wantDeliver)
			}
			if n := count("lockstep.mailbox.full", ""); n != total-capacity {
				t.Errorf("%v: %d lockstep.mailbox.full records, want %d", policy, n, total-capacity)
			}
			if got := cellOf(t, sys, tp).user.Len(); got != capacity {
				t.Errorf("%v: the mailbox holds %d messages, want %d", policy, got, capacity)
			}
		}
	})
}

// AT-MBX-20 (local part): MBX-150. A message to a stopped actor, to a path with no actor and to
// another node on an engine without a Transport is a dead letter with the reason of its case,
// the sender first, and Send returns nil.
func TestATMBX20DeadLetters(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	w, wp := spawnProbe[any](t, sys, "w", nil)
	a := spawnTarget(t, sys, "a", 1, FailSend)
	stopAndWait(t, sys, w, wp, a.Address())
	before := len(rec.Records())

	none := PIDAt[int](sys.Node(), "/user/none")
	other := PIDAt[int]("other", "/user/x")
	for i, to := range []PID[int]{a, none, other} {
		if err := Send(sys, to, i+1); err != nil {
			t.Errorf("Send to %v = %v, want nil", to, err)
		}
	}
	var got []string
	for _, r := range rec.Records()[before:] {
		if r.Kind == "lockstep.deadletter" {
			got = append(got, render(r))
		}
	}
	from := " from=" + sys.user.String()
	sameLines(t, "dead letters", got, []string{
		"lockstep.deadletter" + from + " to=" + a.Address().String() + " reason=stopped msg=1",
		"lockstep.deadletter" + from + " to=" + none.Address().String() + " reason=not-found msg=2",
		"lockstep.deadletter" + from + " to=" + other.Address().String() + " reason=unreachable msg=3",
	})
}

// askTo asks target with a request that carries the ask's ReplyTo, with a timeout of an hour.
func askTo(sys *System, target PID[any]) *Reply[int] {
	return Ask(sys, target, func(rt ReplyTo[int]) any { return rt }, WithTimeout(time.Hour))
}

// resolvedWith fails the test unless r is resolved already, with the error of a full mailbox at
// target, and the ask has exactly one lockstep.reply record, outcome resolved, from target: one
// reply attempt, hence one askRefused call.
func resolvedWith(t *testing.T, rec interface{ Records() []engine.Record }, r *Reply[int], target Address) {
	t.Helper()
	if !r.resolved.Load() {
		t.Errorf("the ask to %v is still pending", target)
		return
	}
	_, err := r.Await(cancelled())
	if !errors.Is(err, ErrMailboxFull) || err.Error() != wantFull(target) {
		t.Errorf("Await = %v, want ErrMailboxFull with text %q", err, wantFull(target))
	}
	want := "lockstep.reply ask=" + r.ask.String() + " from=" + target.String() + " outcome=resolved err=" + err.Error()
	var got []string
	for _, rr := range rec.Records() {
		if rr.Kind == "lockstep.reply" && strings.Contains(render(rr), "ask="+r.ask.String()+" ") {
			got = append(got, render(rr))
		}
	}
	sameLines(t, "reply records of the ask", got, []string{want})
}

// AT-MBX-24 (cases one and two): MBX-092, MSG-310, MSG-345, MSG-390. An ask whose request a full
// mailbox refuses or drops resolves with ErrMailboxFull before the call that offered it returns,
// from the target, once, and never times out; a NewReply request that DropNewest drops is not
// reported and the reply times out.
func TestATMBX24AsksMeetFullMailbox(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	spawnAny := func(name string, policy MailboxPolicy) PID[any] {
		pid, err := Spawn(sys, name, idle[any](), WithMailbox(1, policy))
		if err != nil {
			t.Fatal(err)
		}
		holdToEnd(t, sys, pid.Address())
		return pid
	}
	t1, t2, t3 := spawnAny("t1", FailSend), spawnAny("t2", DropNewest), spawnAny("t3", DropOldest)
	send(t, sys, t1, any(0))
	send(t, sys, t2, any(0))
	queued := askTo(sys, t3) // t3's one queued entry is this ask request

	resolvedWith(t, rec, askTo(sys, t1), t1.Address())
	resolvedWith(t, rec, askTo(sys, t2), t2.Address())

	if queued.resolved.Load() {
		t.Fatal("the queued ask request resolved before it was evicted")
	}
	send(t, sys, t3, any(1)) // evicts the ask request
	resolvedWith(t, rec, queued, t3.Address())
	for _, pid := range []PID[any]{t1, t2, t3} {
		if got := sys.asks.trackedBy(cellOf(t, sys, pid)); len(got) != 0 {
			t.Errorf("%v tracks %v, want no ask", pid, got)
		}
	}
	for _, r := range rec.Records() {
		if r.Kind == "lockstep.timeout" {
			t.Errorf("unexpected record %s", render(r))
		}
	}

	// The request of a NewReply ask carries no ask address: the drop is silent.
	rt, r := NewReply[int](sys, WithTimeout(50*time.Millisecond))
	if err := Send(sys, t2, any(rt)); err != nil {
		t.Fatalf("Send of a NewReply request to a DropNewest mailbox = %v, want nil", err)
	}
	if r.resolved.Load() {
		t.Error("the NewReply ask resolved when its request was dropped")
	}
	if _, err := r.Await(within(t, 10*time.Second)); !errors.Is(err, ErrTimeout) {
		t.Errorf("Await = %v, want ErrTimeout", err)
	}
	for _, rr := range rec.Records() {
		if rr.Kind == "lockstep.reply" && strings.Contains(render(rr), "ask="+r.ask.String()+" ") {
			t.Errorf("unexpected record %s", render(rr))
		}
	}
}

// MBX-092, ENG-150: the interposer holds the request of an ask; when the engine offers it later
// the mailbox is full, and the ask resolves with ErrMailboxFull at that point.
func TestRefusedAskRequestOfferedByInterposer(t *testing.T) {
	eng, sys, fake := interposedSystem(t, func(msg any) bool { _, ok := msg.(ReplyTo[int]); return ok })
	pid, err := Spawn(sys, "t", idle[any](), WithMailbox(1, FailSend))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	holdToEnd(t, sys, pid.Address())
	send(t, sys, pid, any(0))
	r := askTo(sys, pid)
	if r.resolved.Load() {
		t.Fatal("the ask resolved before its held request was offered")
	}
	fake.kept()
	resolvedWith(t, eng, r, pid.Address())
}

// MBX-095: the offer to a local mailbox happens inside Send, from a system and from an actor,
// before any turn runs.
func TestMBX095OfferInsideSend(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got []int
	b, err := Spawn(sys, "b", actorOf[int](&fnActor[int]{receive: func(_ *Context[int], m int) { got = append(got, m) }}))
	if err != nil {
		t.Fatal(err)
	}
	cb := cellOf(t, sys, b)
	queuedInSend := -1
	a, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{receive: func(ctx *Context[int], m int) {
		if err := Send(ctx, b, m); err != nil {
			t.Errorf("Send from an actor: %v", err)
		}
		queuedInSend = cb.user.Len()
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle() // both started

	send(t, sys, a, 10)
	if !eng.Step() { // a's turn only: b has not run
		t.Fatal("no turn ran")
	}
	if queuedInSend != 1 || len(got) != 0 {
		t.Errorf("during Send from an actor: %d queued, %d received; want 1, 0", queuedInSend, len(got))
	}
	eng.RunUntilIdle()
	send(t, sys, b, 20)
	if n := cb.user.Len(); n != 1 || len(got) != 1 {
		t.Errorf("after Send from a system: %d queued, %d received since; want 1, 0", n, len(got)-1)
	}
	eng.RunUntilIdle()
	if len(got) != 2 || got[0] != 10 || got[1] != 20 {
		t.Errorf("received %v, want [10 20]", got)
	}
}

// inbound returns the envelope the transport hands to Host.Inbound: a user message from
// remoteSender to to with the sequence number seq.
func inbound(to Address, seq uint64, p userPayload) engine.Envelope {
	return engine.Envelope{From: remoteSender.engine(), To: to.engine(), Seq: seq, Payload: p}
}

// AT-MBX-18 (the local engine's part), AT-MSG-38: MBX-120, MBX-092. Inbound envelopes are offered
// in call order with seq = env.Seq. Under DropNewest the third is a recorded drop; under
// BlockSender the refusal is a rejection, never a wait. An inbound ask request the mailbox
// refuses or drops fails back to its asker once.
func TestMBX120InboundOutcomes(t *testing.T) {
	setup := func(t *testing.T, capacity int, policy MailboxPolicy) (*System, *enginetest.Engine, PID[any]) {
		t.Helper()
		sys, eng := newManualSystem(t)
		eng.SetHost(host{sys})
		pid, err := Spawn(sys, "r", idle[any](), WithMailbox(capacity, policy))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		holdToEnd(t, sys, pid.Address())
		return sys, eng, pid
	}
	from := remoteSender.String()
	for _, tc := range []struct {
		policy MailboxPolicy
		action string
	}{{DropNewest, "dropped-newest"}, {BlockSender, "rejected"}, {FailSend, "rejected"}} {
		t.Run(tc.policy.String(), func(t *testing.T) {
			sys, eng, pid := setup(t, 2, tc.policy)
			to := pid.Address()
			for seq := uint64(1); seq <= 3; seq++ {
				eng.Inbound(inbound(to, seq, userPayload{msg: int(seq) * 10}))
			}
			sameLines(t, "records", rendered(eng.Records(), 0, to, "lockstep.deliver", "lockstep.mailbox.full"), []string{
				"lockstep.deliver from=" + from + " to=" + to.String() + " seq=1 len=1 msg=10",
				"lockstep.deliver from=" + from + " to=" + to.String() + " seq=2 len=2 msg=20",
				"lockstep.mailbox.full from=" + from + " to=" + to.String() + " policy=" + tc.policy.String() +
					" action=" + tc.action + " cause=capacity cap=2 len=2 msg=30",
			})
			if got := cellOf(t, sys, pid).user.Len(); got != 2 {
				t.Errorf("the mailbox holds %d messages, want 2", got)
			}
		})
	}

	// An ask request from n2: the reply attempt travels back from the target to the ask address.
	ask := Address{node: "n2", path: "/temp/5", uid: 99}
	request := userPayload{msg: ReplyTo[int]{addr: ask}, ask: ask, typ: typeOf[any]()}
	for _, policy := range []MailboxPolicy{FailSend, DropNewest, BlockSender} {
		t.Run("ask refused under "+policy.String(), func(t *testing.T) {
			sys, eng, pid := setup(t, 1, policy)
			to := pid.Address()
			eng.Inbound(inbound(to, 1, userPayload{msg: 0}))
			eng.Inbound(inbound(to, 2, request))
			expectRefusalReply(t, eng, to, ask)
			if got := sys.asks.trackedBy(cellOf(t, sys, pid)); len(got) != 0 {
				t.Errorf("the target tracks %v, want no ask", got)
			}
		})
	}
	t.Run("ask evicted under DropOldest", func(t *testing.T) {
		_, eng, pid := setup(t, 1, DropOldest)
		to := pid.Address()
		eng.Inbound(inbound(to, 1, request))
		if n := len(eng.Delivered()); n != 0 {
			t.Fatalf("%d envelopes delivered before the eviction, want 0", n)
		}
		eng.Inbound(inbound(to, 2, userPayload{msg: 0}))
		expectRefusalReply(t, eng, to, ask)
	})
}

// expectRefusalReply fails the test unless eng delivered exactly one envelope, a reply attempt
// from target to ask carrying the error of a full mailbox at target.
func expectRefusalReply(t *testing.T, eng *enginetest.Engine, target, ask Address) {
	t.Helper()
	d := eng.Delivered()
	if len(d) != 1 {
		t.Fatalf("%d envelopes delivered, want one reply attempt", len(d))
	}
	p, ok := d[0].Payload.(replyPayload)
	if !ok || d[0].From != target.engine() || d[0].To != ask.engine() {
		t.Fatalf("delivered %+v, want a reply attempt from %v to %v", d[0], target, ask)
	}
	if !errors.Is(p.err, ErrMailboxFull) || p.err.Error() != wantFull(target) || p.v != nil {
		t.Errorf("reply attempt carries %v and %v, want no value and ErrMailboxFull with text %q", p.v, p.err, wantFull(target))
	}
}

// MBX-090: on an engine that cannot make a caller wait (Blocking is false), a *System sender to
// a full BlockSender mailbox gets ErrMailboxFull at once, and the record is a rejection.
func TestMBX090BlockSenderWithoutBlocking(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid := spawnTarget(t, sys, "t", 1, BlockSender)
	holdToEnd(t, sys, pid.Address())
	send(t, sys, pid, 1)
	to := pid.Address()
	err := Send(sys, pid, 2)
	if !errors.Is(err, ErrMailboxFull) || err.Error() != wantFull(to) {
		t.Errorf("Send = %v, want ErrMailboxFull with text %q", err, wantFull(to))
	}
	from := sys.user.String()
	sameLines(t, "records", rendered(eng.Records(), 0, to, "lockstep.deliver", "lockstep.mailbox.full"), []string{
		"lockstep.deliver from=" + from + " to=" + to.String() + " seq=0 len=1 msg=1",
		"lockstep.mailbox.full from=" + from + " to=" + to.String() + " policy=block-sender action=rejected" +
			" cause=capacity cap=1 len=1 msg=2",
	})
}

// MBX-150, MBX §8: the dead letter of a stop's drain names the entry's sender and the stopped
// actor.
func TestDrainDeadLetterNamesSender(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid := spawnTarget(t, sys, "t", 4, FailSend)
	eng.RunUntilIdle()
	release := hold(t, sys, pid.Address())
	send(t, sys, pid, 1)
	send(t, sys, pid, 2)
	if err := Stop(sys, pid.Address()); err != nil {
		t.Fatal(err)
	}
	release()
	eng.RunUntilIdle()
	from := sys.user.String()
	to := pid.Address().String()
	var got []string
	for _, r := range eng.Records() {
		if r.Kind == "lockstep.deadletter" {
			got = append(got, render(r))
		}
	}
	sameLines(t, "dead letters", got, []string{
		"lockstep.deadletter from=" + from + " to=" + to + " reason=stopped msg=1",
		"lockstep.deadletter from=" + from + " to=" + to + " reason=stopped msg=2",
	})
}
