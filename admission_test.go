// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/engine/local"
)

// shedPressure is the admission error of a system that sheds because of memory pressure.
var shedPressure error = &engine.AdmissionError{Source: engine.SourcePressure}

// pressure returns the change of the pressure source to on, with the combined state shedding.
func pressure(on, shedding bool) engine.AdmissionChange {
	return engine.AdmissionChange{Source: engine.SourcePressure, On: on, Shedding: shedding}
}

// shedText returns OVL-120's text for a message to to shed for reason.
func shedText(to Address, reason string) string {
	return "lockstep: send to " + to.String() + ": overloaded (" + reason + "), mailbox treated as full"
}

// wantShed fails the test unless err is OVL-120's error for a message to to shed under pressure.
func wantShed(t *testing.T, what string, err error, to Address) {
	t.Helper()
	if !errors.Is(err, ErrOverloaded) || !errors.Is(err, ErrMailboxFull) || err.Error() != shedText(to, "pressure") {
		t.Errorf("%s = %v, want ErrOverloaded and ErrMailboxFull with text %q", what, err, shedText(to, "pressure"))
	}
}

// shedRecord returns the lockstep.overload event=shed record of a message from from to to shed
// under pressure by a mailbox with policy, which dropped what dropped says.
func shedRecord(from, to Address, policy MailboxPolicy, dropped string) string {
	return "lockstep.overload event=shed from=" + from.String() + " to=" + to.String() +
		" reason=pressure policy=" + policy.String() + " dropped=" + dropped
}

// admissionDouble is an engine.Admission for a local engine whose answers the test sets, and whose
// changes the test reports through the onChange the system bound, on the test's goroutine.
type admissionDouble struct {
	mu       sync.Mutex
	sendErr  error
	onChange func(engine.AdmissionChange)
}

func (*admissionDouble) AdmitSpawn(bool) error { return nil }

func (a *admissionDouble) AdmitSend() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sendErr
}

func (a *admissionDouble) Bind(onChange func(engine.AdmissionChange), _ func() engine.Usage) {
	a.onChange = onChange
}

// refuseSends makes AdmitSend return err from now on.
func (a *admissionDouble) refuseSends(err error) {
	a.mu.Lock()
	a.sendErr = err
	a.mu.Unlock()
}

// change reports c to the system.
func (a *admissionDouble) change(c engine.AdmissionChange) { a.onChange(c) }

// newAdmissionSystem returns a system on a local engine whose tracer is a new recorder and whose
// Admission is a double. The system shuts down when the test ends.
func newAdmissionSystem(t *testing.T) (*System, *enginetest.Recorder, *admissionDouble) {
	t.Helper()
	rec := enginetest.NewRecorder()
	le, err := local.New(local.Config{Node: "local", Tracer: rec})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	b := le.Engine()
	adm := &admissionDouble{}
	b.Admission = adm
	sys, err := newSystem(b, nil)
	if err != nil {
		le.Stop()
		t.Fatalf("newSystem on a local engine: %v", err)
	}
	shutdownAtEnd(t, sys)
	return sys, rec, adm
}

// offerRecords returns the records of recs from index from on that are lockstep.overload records,
// or lockstep.deliver and lockstep.mailbox.full records naming to, rendered without len, in order.
func offerRecords(recs []engine.Record, from int, to Address) []string {
	var out []string
	for _, r := range recs[from:] {
		switch r.Kind {
		case "lockstep.overload":
			out = append(out, render(r))
		case "lockstep.deliver", "lockstep.mailbox.full":
			if s := render(r); strings.Contains(s, " to="+to.String()+" ") {
				out = append(out, s)
			}
		}
	}
	return withoutLen(out)
}

// waitRecorded waits up to 10 s until rec holds, from index from on, a record that renders as want
// once len is removed. A waiting sender writes its waiting record after it joins the line and
// without the mailbox's lock, so Waiting() can count it before the record exists; a test that
// writes a record of its own next waits for the sender's first.
func waitRecorded(t *testing.T, rec *enginetest.Recorder, from int, want string) {
	t.Helper()
	want = lenAttr.ReplaceAllString(want, "")
	deadline := time.Now().Add(10 * time.Second)
	for {
		recs := rec.Records()
		for _, r := range recs[from:] {
			if lenAttr.ReplaceAllString(render(r), "") == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no record %q after 10 s", want)
		}
		time.Sleep(time.Millisecond)
	}
}

// signalLog is an any actor that logs the strings and Balances it receives and the Terminated
// signals it gets, and passes every message to receive, if set.
type signalLog struct {
	log     *[]string
	receive func(ctx *Context[any], m any)
}

func (a *signalLog) Receive(ctx *Context[any], m any) {
	switch m := m.(type) {
	case string:
		*a.log = append(*a.log, m)
	case Balance:
		*a.log = append(*a.log, "Receive("+m.String()+")")
	}
	if a.receive != nil {
		a.receive(ctx, m)
	}
}

func (a *signalLog) Signal(_ *Context[any], sig Signal) {
	if _, ok := sig.(Terminated); ok {
		*a.log = append(*a.log, "Terminated")
	}
}

// OVL-100, OVL-160, OVL §8: while shedding, a normal spawn is refused after the cap check with
// ErrOverloaded and OVL-100's text, and recorded as event=refuse; a spawn from a rebuild context
// and a system actor (reserve spawns) are admitted, and a refused spawn takes no slot.
func TestOVL100SpawnRefusedWhileShedding(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var fromStarted, fromReceive error
	startedSpawns := 0
	p, err := Spawn(sys, "p", actorOf[any](&fnActor[any]{
		started: func(ctx *Context[any]) {
			if ctx.inc >= 2 {
				startedSpawns++
				_, fromStarted = Spawn(ctx, "k", idle[any]())
			}
		},
		receive: func(ctx *Context[any], msg any) {
			switch msg {
			case "spawn":
				_, fromReceive = Spawn(ctx, "r", idle[any]())
			case "panic":
				panic("panic")
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	eng.SetAdmission(shedPressure, nil)

	_, err = Spawn(sys, "a", idle[any]())
	if want := "lockstep: spawn in /user: overloaded (pressure)"; err == nil || err.Error() != want || !errors.Is(err, ErrOverloaded) {
		t.Errorf("Spawn from the system = %v, want ErrOverloaded with text %q", err, want)
	}
	send(t, sys, p, any("spawn"))
	eng.RunUntilIdle()
	if want := "lockstep: spawn in /user/p: overloaded (pressure)"; fromReceive == nil || fromReceive.Error() != want || !errors.Is(fromReceive, ErrOverloaded) {
		t.Errorf("Spawn from Receive = %v, want ErrOverloaded with text %q", fromReceive, want)
	}
	send(t, sys, p, any("panic"))
	eng.RunUntilIdle()
	if startedSpawns != 1 || fromStarted != nil {
		t.Errorf("spawn from the restarted Started: %d calls, err %v; want 1 call, admitted", startedSpawns, fromStarted)
	}
	if _, err := spawnSystem(sys, "s", idle[any]()); err != nil {
		t.Errorf("system actor spawn while shedding: %v", err)
	}
	if got := sys.liveActors(); got != 3 { // p, k, s
		t.Errorf("live %d, want 3", got)
	}
	sameRecords(t, recordsOf(eng.Records(), "lockstep.overload"), "lockstep.overload",
		[]string{"event=refuse", "parent=/user", "reason=pressure"},
		[]string{"event=refuse", "parent=/user/p", "reason=pressure"},
	)
}

// OVL-110, MBX-035: while AdmitSend refuses, a reply to the system, a piped reply, a timer
// message, a signal, a stop and a send to a system actor are delivered and never shed; a user
// message to a user actor is.
func TestOVL110ExemptMessages(t *testing.T) {
	sys, eng := newManualSystem(t)
	acct := spawnAcct(t, sys, nil)
	c, err := Spawn(sys, "c", idle[any]())
	if err != nil {
		t.Fatal(err)
	}
	var log []string
	a, err := Spawn(sys, "a", actorOf[any](&signalLog{log: &log, receive: func(ctx *Context[any], m any) {
		if m == "go" {
			Pipe(ctx, Ask(ctx, acct, getBalance), toBalance)
			ctx.After(time.Second, any("tick"))
			if err := Watch(ctx, c.Address()); err != nil {
				t.Errorf("Watch: %v", err)
			}
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	var sysGot []int
	s, err := spawnSystem(sys, "s", actorOf[int](&fnActor[int]{receive: func(_ *Context[int], m int) { sysGot = append(sysGot, m) }}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	release := hold(t, sys, acct.Address())
	send(t, sys, a, any("go"))
	r := Ask(sys, acct, getBalance, WithTimeout(time.Hour))
	eng.RunUntilIdle() // a asks; both requests wait in acct's mailbox

	eng.SetAdmission(nil, shedPressure)
	if err := Stop(sys, c.Address()); err != nil {
		t.Errorf("Stop: %v", err)
	}
	if err := Send(sys, s, 7); err != nil {
		t.Errorf("Send to a system actor = %v, want nil", err)
	}
	release()
	eng.RunUntilIdle()
	eng.Advance(time.Second)
	eng.RunUntilIdle()

	slices.Sort(log) // the order of a signal and user messages is not the point here
	if want := []string{"Receive(Balance{0, <nil>})", "Terminated", "go", "tick"}; !slices.Equal(log, want) {
		t.Errorf("a's log (sorted) = %q, want %q", log, want)
	}
	if v, err := r.Await(cancelled()); v != 0 || err != nil {
		t.Errorf("the system's ask = %d, %v, want 0, nil", v, err)
	}
	if !slices.Equal(sysGot, []int{7}) {
		t.Errorf("the system actor got %v, want [7]", sysGot)
	}

	// A user message to a user actor is shed: AdmitSend did refuse.
	wantShed(t, "Send to a user actor", Send(sys, a, any("x")), a.Address())
	sameLines(t, "overload records", renderKind(eng.Records(), "lockstep.overload"), []string{
		shedRecord(sys.user, a.Address(), FailSend, "new"),
	})
}

// AT-OVL-13 on the manual engine: OVL-110, OVL-120, MBX-090. While shedding, each policy acts as
// if the mailbox were full, from an actor sender and from the system: FailSend refuses with
// OVL-120's error, DropNewest drops the message, DropOldest drops the queued message and
// enqueues the new one, and BlockSender acts as FailSend without waiting. Each shed is recorded
// as event=shed instead of lockstep.mailbox.full; the entry DropOldest enqueues as delivered.
func TestOVL120ShedPolicies(t *testing.T) {
	for _, fromActor := range []bool{false, true} {
		name := "system sender"
		if fromActor {
			name = "actor sender"
		}
		t.Run(name, func(t *testing.T) {
			sys, eng := newManualSystem(t)
			sender, err := spawnSystem(sys, "sender", actorOf[any](&funcActor{}))
			if err != nil {
				t.Fatal(err)
			}
			policies := []MailboxPolicy{FailSend, DropNewest, DropOldest, BlockSender}
			targets := make([]PID[int], len(policies))
			got := make([]<-chan int, len(policies))
			for i, p := range policies {
				targets[i], got[i] = spawnCollector(t, sys, p.String(), 4, p)
			}
			eng.RunUntilIdle()
			var releases []func()
			for _, tp := range targets {
				releases = append(releases, hold(t, sys, tp.Address()))
			}
			send(t, sys, targets[2], 1) // DropOldest's one queued message
			before := len(eng.Records())

			eng.SetAdmission(nil, shedPressure)
			errs := make([]error, len(targets))
			from := sys.user
			if fromActor {
				from = sender.Address()
				run(t, sys, eng, sender, func(ctx *Context[any]) {
					for i, tp := range targets {
						errs[i] = Send(ctx, tp, 2)
					}
				})
			} else {
				for i, tp := range targets {
					errs[i] = Send(sys, tp, 2)
				}
			}
			eng.SetAdmission(nil, nil)
			for _, release := range releases {
				release()
			}
			eng.RunUntilIdle()

			wantShed(t, "Send to FailSend", errs[0], targets[0].Address())
			for _, i := range []int{1, 2} {
				if errs[i] != nil {
					t.Errorf("Send to %v = %v, want nil", policies[i], errs[i])
				}
			}
			wantShed(t, "Send to BlockSender", errs[3], targets[3].Address())
			wantGot := [][]int{nil, nil, {2}, nil}
			for i := range targets {
				var received []int
				for len(got[i]) > 0 {
					received = append(received, <-got[i])
				}
				if !slices.Equal(received, wantGot[i]) {
					t.Errorf("%v target received %v, want %v", policies[i], received, wantGot[i])
				}
			}
			dropped := []string{"new", "new", "oldest", "new"}
			for i, tp := range targets {
				to := tp.Address()
				want := []string{shedRecord(from, to, policies[i], dropped[i])}
				if policies[i] == DropOldest {
					want = append(want, "lockstep.deliver from="+from.String()+" to="+to.String()+" seq=0 len=1 msg=2")
				}
				sameLines(t, policies[i].String()+" records",
					rendered(eng.Records(), before, to, "lockstep.overload", "lockstep.deliver", "lockstep.mailbox.full"), want)
			}
		})
	}
}

// AT-OVL-16 on the manual engine: OVL-130, MBX-092. While shedding, an Ask whose request a
// FailSend mailbox refuses, or a DropNewest mailbox drops, resolves before Ask returns with
// OVL-120's error, never with ErrTimeout; so does a queued ask request that DropOldest evicts for
// a shed message.
func TestOVL130AsksResolveAtOnce(t *testing.T) {
	sys, eng := newManualSystem(t)
	spawnAny := func(name string, capacity int, policy MailboxPolicy) PID[any] {
		pid, err := Spawn(sys, name, idle[any](), WithMailbox(capacity, policy))
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}
	t1, t2, t3 := spawnAny("t1", 4, FailSend), spawnAny("t2", 4, DropNewest), spawnAny("t3", 4, DropOldest)
	eng.RunUntilIdle()
	queued := askTo(sys, t3)
	eng.SetAdmission(nil, shedPressure)

	resolved := func(what string, r *Reply[int], target Address) {
		t.Helper()
		if !r.resolved.Load() {
			t.Errorf("%s is still pending", what)
			return
		}
		_, err := r.Await(cancelled())
		if errors.Is(err, ErrTimeout) {
			t.Errorf("%s = %v, want no ErrTimeout", what, err)
		}
		wantShed(t, what, err, target)
	}
	resolved("ask to FailSend", askTo(sys, t1), t1.Address())
	resolved("ask to DropNewest", askTo(sys, t2), t2.Address())
	if queued.resolved.Load() {
		t.Fatal("the queued ask request resolved before it was evicted")
	}
	if err := Send(sys, t3, any(1)); err != nil {
		t.Errorf("Send to DropOldest = %v, want nil", err)
	}
	resolved("evicted ask", queued, t3.Address())
	for _, pid := range []PID[any]{t1, t2, t3} {
		if got := sys.asks.trackedBy(cellOf(t, sys, pid)); len(got) != 0 {
			t.Errorf("%v tracks %v, want no ask", pid, got)
		}
	}
}

// AT-OVL-17 on the manual engine: OVL-140, MBX-092, MBX-120. A message arriving while shedding is
// admitted on arrival: under FailSend and BlockSender it is dropped, recorded as event=shed with
// the policy and dropped=new; a shed ask request fails back to its asker with OVL-120's error.
func TestOVL140InboundShedIsDropped(t *testing.T) {
	for _, policy := range []MailboxPolicy{FailSend, BlockSender} {
		t.Run(policy.String(), func(t *testing.T) {
			sys, eng := newManualSystem(t)
			eng.SetHost(host{sys})
			pid, err := Spawn(sys, "r", idle[any](), WithMailbox(2, policy))
			if err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle()
			to := pid.Address()
			eng.SetAdmission(nil, shedPressure)
			eng.Inbound(inbound(to, 1, userPayload{msg: 10}))
			if got := cellOf(t, sys, pid).user.Len(); got != 0 {
				t.Errorf("the mailbox holds %d messages, want 0", got)
			}
			sameLines(t, "records", rendered(eng.Records(), 0, to, "lockstep.overload", "lockstep.deliver", "lockstep.mailbox.full"),
				[]string{shedRecord(remoteSender, to, policy, "new")})

			ask := Address{node: "n2", path: "/temp/5", uid: 99}
			eng.Inbound(inbound(to, 2, userPayload{msg: ReplyTo[int]{addr: ask}, ask: ask, typ: typeOf[any]()}))
			d := eng.Delivered()
			if len(d) != 1 {
				t.Fatalf("%d envelopes delivered, want one reply attempt", len(d))
			}
			p, ok := d[0].Payload.(replyPayload)
			if !ok || d[0].From != to.engine() || d[0].To != ask.engine() {
				t.Fatalf("delivered %+v, want a reply attempt from %v to %v", d[0], to, ask)
			}
			wantShed(t, "the reply attempt's error", p.err, to)
		})
	}
}

// OVL-150, OVL §8: every source change is recorded as event=on or event=off with the change's
// values and the combined state after it; the system sheds from the first source on to the last
// one off. The usage the system bound reports its live actors.
func TestOVL150ChangeRecords(t *testing.T) {
	sys, eng := newManualSystem(t)
	if _, err := Spawn(sys, "a", idle[any]()); err != nil {
		t.Fatal(err)
	}
	if got := eng.Usage(); got != (engine.Usage{Actors: 1}) {
		t.Errorf("Usage() = %+v, want {Actors: 1}", got)
	}
	budget := func(on bool, live uint64, shedding bool) engine.AdmissionChange {
		return engine.AdmissionChange{Source: engine.SourceBudget, On: on, Live: live, High: 1000, Low: 800, Shedding: shedding}
	}
	changes := []engine.AdmissionChange{pressure(true, true), budget(true, 900, true), pressure(false, true), budget(false, 700, false)}
	var shedding []bool
	for _, c := range changes {
		eng.ChangeAdmission(c)
		shedding = append(shedding, sys.shedding())
	}
	if want := []bool{true, true, true, false}; !slices.Equal(shedding, want) {
		t.Errorf("shedding after each change %v, want %v", shedding, want)
	}
	sameRecords(t, recordsOf(eng.Records(), "lockstep.overload"), "lockstep.overload",
		[]string{"event=on", "reason=pressure", "live=0", "high=0", "low=0", "shedding=true"},
		[]string{"event=on", "reason=budget", "live=900", "high=1000", "low=800", "shedding=true"},
		[]string{"event=off", "reason=pressure", "live=0", "high=0", "low=0", "shedding=true"},
		[]string{"event=off", "reason=budget", "live=700", "high=1000", "low=800", "shedding=false"},
	)
}

// AT-OVL-46's Phase 1 part: OVL-120, OVL-150, MBX-075. An outside sender to a BlockSender mailbox
// with room waits while shedding, recorded as event=shed dropped=none; the change to admitting
// rechecks the mailbox and its message is delivered. A wait whose context ends first returns the
// context's error with no further record. A sender whose context has ended before it could wait
// gets the context's error, and its shed message is recorded as event=shed dropped=new.
func TestOVL150RecheckReleasesWaiter(t *testing.T) {
	t.Run("released", func(t *testing.T) {
		sys, rec, adm := newAdmissionSystem(t)
		tp, got := spawnCollector(t, sys, "t", 1, BlockSender)
		to := tp.Address()
		adm.change(pressure(true, true))
		adm.refuseSends(shedPressure)
		before := len(rec.Records())
		ch := sendContextAsync(context.Background(), sys, tp, 1)
		waitingSenders(t, cellOf(t, sys, tp), 1)
		waitRecorded(t, rec, before, shedRecord(sys.user, to, BlockSender, "none"))
		adm.refuseSends(nil)
		adm.change(pressure(false, false))
		if err := sendReturned(t, ch); err != nil {
			t.Errorf("SendContext = %v, want nil", err)
		}
		if m := recv(t, got); m != 1 {
			t.Errorf("the target received %d, want 1", m)
		}
		sameLines(t, "records", offerRecords(rec.Records(), before, to), withoutLen([]string{
			shedRecord(sys.user, to, BlockSender, "none"),
			"lockstep.overload event=off reason=pressure live=0 high=0 low=0 shedding=false",
			"lockstep.deliver from=" + sys.user.String() + " to=" + to.String() + " seq=0 len=1 msg=1",
		}))
	})

	t.Run("deadline", func(t *testing.T) {
		sys, rec, adm := newAdmissionSystem(t)
		tp := spawnTarget(t, sys, "t", 1, BlockSender)
		to := tp.Address()
		adm.change(pressure(true, true))
		adm.refuseSends(shedPressure)
		before := len(rec.Records())
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := SendContext(ctx, sys, tp, 1); err != context.DeadlineExceeded {
			t.Errorf("SendContext = %v, want context.DeadlineExceeded unwrapped", err)
		}
		sameLines(t, "records", offerRecords(rec.Records(), before, to), withoutLen([]string{
			shedRecord(sys.user, to, BlockSender, "none"),
		}))
		if n := cellOf(t, sys, tp).user.Waiting(); n != 0 {
			t.Errorf("%d senders wait, want 0", n)
		}
	})

	t.Run("ended", func(t *testing.T) {
		sys, rec, adm := newAdmissionSystem(t)
		tp := spawnTarget(t, sys, "t", 1, BlockSender)
		to := tp.Address()
		adm.change(pressure(true, true))
		adm.refuseSends(shedPressure)
		before := len(rec.Records())
		if err := SendContext(cancelled(), sys, tp, 1); err != context.Canceled {
			t.Errorf("SendContext = %v, want context.Canceled unwrapped", err)
		}
		sameLines(t, "records", offerRecords(rec.Records(), before, to), []string{
			shedRecord(sys.user, to, BlockSender, "new"),
		})
		if n := cellOf(t, sys, tp).user.Len(); n != 0 {
			t.Errorf("the mailbox holds %d messages, want 0", n)
		}
	})
}

// AT-MBX-11, the waiting row's states that need admission: MBX-070, MBX-075, MBX-090. An outside
// sender on a blocking engine to a BlockSender mailbox waits when its message is shed (event=shed
// dropped=none) or when other senders wait although there is room (mailbox.full action blocked),
// and each message is delivered once it is admitted and has room.
func TestATMBX11WaitingRowAdmission(t *testing.T) {
	t.Run("waiters", func(t *testing.T) {
		sys, rec, adm := newAdmissionSystem(t)
		tp, got := spawnCollector(t, sys, "t", 1, BlockSender)
		to := tp.Address()
		release := hold(t, sys, to)
		t.Cleanup(release)
		adm.change(pressure(true, true))
		adm.refuseSends(shedPressure)
		before := len(rec.Records())
		from := sys.user.String()
		blocked := "lockstep.mailbox.full from=" + from + " to=" + to.String() + " policy=block-sender action=blocked" +
			" cause=capacity cap=1 len=0 msg=2"
		ch1 := sendContextAsync(context.Background(), sys, tp, 1)
		waitingSenders(t, cellOf(t, sys, tp), 1)
		waitRecorded(t, rec, before, shedRecord(sys.user, to, BlockSender, "none"))
		adm.refuseSends(nil)
		ch2 := sendContextAsync(context.Background(), sys, tp, 2) // admitted, but behind a waiter
		waitingSenders(t, cellOf(t, sys, tp), 2)
		waitRecorded(t, rec, before, blocked)
		adm.change(pressure(false, false))
		if err := sendReturned(t, ch1); err != nil {
			t.Errorf("first SendContext = %v, want nil", err)
		}
		release()
		if err := sendReturned(t, ch2); err != nil {
			t.Errorf("second SendContext = %v, want nil", err)
		}
		if a, b := recv(t, got), recv(t, got); a != 1 || b != 2 {
			t.Errorf("the target received %d, %d, want 1, 2", a, b)
		}
		sameLines(t, "records", offerRecords(rec.Records(), before, to), withoutLen([]string{
			shedRecord(sys.user, to, BlockSender, "none"),
			blocked,
			"lockstep.overload event=off reason=pressure live=0 high=0 low=0 shedding=false",
			"lockstep.deliver from=" + from + " to=" + to.String() + " seq=0 len=1 msg=1",
			"lockstep.deliver from=" + from + " to=" + to.String() + " seq=0 len=1 msg=2",
		}))
	})

	t.Run("shed", func(t *testing.T) {
		sys, rec, adm := newAdmissionSystem(t)
		tp, got := spawnCollector(t, sys, "t", 1, BlockSender)
		to := tp.Address()
		release := hold(t, sys, to)
		t.Cleanup(release)
		send(t, sys, tp, 1)
		adm.change(pressure(true, true))
		adm.refuseSends(shedPressure)
		before := len(rec.Records())
		ch := sendContextAsync(context.Background(), sys, tp, 2)
		waitingSenders(t, cellOf(t, sys, tp), 1)
		waitRecorded(t, rec, before, shedRecord(sys.user, to, BlockSender, "none"))
		adm.refuseSends(nil)
		adm.change(pressure(false, false)) // admitting, but no room yet
		waitingSenders(t, cellOf(t, sys, tp), 1)
		release()
		if err := sendReturned(t, ch); err != nil {
			t.Errorf("SendContext = %v, want nil", err)
		}
		if a, b := recv(t, got), recv(t, got); a != 1 || b != 2 {
			t.Errorf("the target received %d, %d, want 1, 2", a, b)
		}
		sameLines(t, "records", offerRecords(rec.Records(), before, to), withoutLen([]string{
			shedRecord(sys.user, to, BlockSender, "none"),
			"lockstep.overload event=off reason=pressure live=0 high=0 low=0 shedding=false",
			"lockstep.deliver from=" + sys.user.String() + " to=" + to.String() + " seq=0 len=1 msg=2",
		}))
	})
}

// MBX-035, OVL-110: a piped reply and a timer message reach an actor whose FailSend mailbox is
// full while the system sheds; neither is refused, dropped or recorded as shed or full.
func TestMBX035SelfEntriesNeverShed(t *testing.T) {
	sys, eng := newManualSystem(t)
	acct := spawnAcct(t, sys, nil)
	var log []string
	a, err := Spawn(sys, "a", actorOf[any](&signalLog{log: &log, receive: func(ctx *Context[any], m any) {
		if m == "go" {
			Pipe(ctx, Ask(ctx, acct, getBalance), toBalance)
			ctx.After(time.Second, any("tick"))
		}
	}}), WithMailbox(1, FailSend))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	releaseAcct := hold(t, sys, acct.Address())
	send(t, sys, a, any("go"))
	eng.RunUntilIdle()
	releaseA := hold(t, sys, a.Address())
	send(t, sys, a, any("fill")) // a's mailbox is full
	eng.SetAdmission(nil, shedPressure)

	releaseAcct()
	eng.RunUntilIdle()
	eng.Advance(time.Second)
	if got := cellOf(t, sys, a).user.Len(); got != 3 {
		t.Errorf("a's mailbox holds %d entries, want 3", got)
	}
	releaseA()
	eng.RunUntilIdle()
	if want := []string{"go", "fill", "Receive(Balance{0, <nil>})", "tick"}; !slices.Equal(log, want) {
		t.Errorf("a's log = %q, want %q", log, want)
	}
	if got := rendered(eng.Records(), 0, a.Address(), "lockstep.overload", "lockstep.mailbox.full"); len(got) != 0 {
		t.Errorf("records naming a: %q, want none", got)
	}
}
