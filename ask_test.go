// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// GetBalance asks RFC-0002's account actor for its balance.
type GetBalance struct{ Reply ReplyTo[int64] }

func (GetBalance) acctMsg() {}

// getBalance is the builder of a balance ask.
func getBalance(rt ReplyTo[int64]) acctMsg { return GetBalance{rt} }

// spawnAcct spawns RFC-0002's account actor as "acct": Deposit adds to the balance and GetBalance
// answers it. got, if not nil, receives every GetBalance before it is answered.
func spawnAcct(t *testing.T, sys *System, got chan<- GetBalance) PID[acctMsg] {
	t.Helper()
	var balance int64
	pid, err := Spawn(sys, "acct", actorOf[acctMsg](&fnActor[acctMsg]{
		receive: func(ctx *Context[acctMsg], m acctMsg) {
			switch m := m.(type) {
			case Deposit:
				balance += int64(m.Amount)
			case GetBalance:
				if got != nil {
					got <- m
				}
				m.Reply.Send(ctx, balance)
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// answer tells silent to answer every ReplyTo it holds with v.
type answer struct{ v int }

// spawnSilent spawns MSG §10's silent actor as "silent": a probe that keeps every ReplyTo[int] it
// receives, also sends it to the returned channel (capacity 64), and answers nothing until the
// test sends it an answer.
func spawnSilent(t *testing.T, sys *System) (PID[any], <-chan ReplyTo[int]) {
	t.Helper()
	stored := make(chan ReplyTo[int], 64)
	var held []ReplyTo[int]
	pid, _ := spawnProbe(t, sys, "silent", func(ctx *Context[any], m any) {
		switch m := m.(type) {
		case ReplyTo[int]:
			held = append(held, m)
			stored <- m
		case answer:
			for _, rt := range held {
				rt.Send(ctx, m.v)
			}
			held = nil
		}
	})
	return pid, stored
}

// askSilent asks silent from from; the request is the ask's ReplyTo itself.
func askSilent(from Sender, silent PID[any], opts ...AskOption) *Reply[int] {
	return Ask(from, silent, func(rt ReplyTo[int]) any { return rt }, opts...)
}

// within returns a context that ends after d.
func within(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// cancelled returns a context that has already ended.
func cancelled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// recv returns the next value from ch, failing the test if none arrives within 10 s.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("no %T within 10 s", *new(T))
		panic("unreachable")
	}
}

// askRecords returns the attributes of recs' records of kind whose ask attribute is ask.
func askRecords(recs []engine.Record, kind string, ask Address) [][]string {
	var out [][]string
	for _, a := range recordsOf(recs, kind) {
		if slices.Contains(a, "ask="+ask.String()) {
			out = append(out, a)
		}
	}
	return out
}

// recordIndex returns the index of the first record in recs of kind that has every one of attrs
// (key=value), or -1.
func recordIndex(recs []engine.Record, kind string, attrs ...string) int {
	return slices.IndexFunc(recs, func(r engine.Record) bool {
		if r.Kind != kind {
			return false
		}
		have := recordsOf([]engine.Record{r}, kind)[0]
		for _, a := range attrs {
			if !slices.Contains(have, a) {
				return false
			}
		}
		return true
	})
}

// messageRecords returns the number of MSG §8 records in recs: those of sends, asks and replies.
// (A local engine's turns, which record lockstep.turn, run concurrently with the test.)
func messageRecords(recs []engine.Record) int {
	n := 0
	for _, r := range recs {
		switch r.Kind {
		case "lockstep.send", "lockstep.ask", "lockstep.reply", "lockstep.timeout":
			n++
		}
	}
	return n
}

// AT-MSG-01: MSG-110, MSG-300, MSG-420. Two deposits, then an ask for the balance.
func TestATMSG01SendAndAsk(t *testing.T) {
	sys := newLocalSystem(t)
	acct := spawnAcct(t, sys, nil)
	send(t, sys, acct, acctMsg(Deposit{10}))
	send(t, sys, acct, acctMsg(Deposit{20}))
	v, err := Ask(sys, acct, getBalance).Await(within(t, 10*time.Second))
	if v != 30 || err != nil {
		t.Errorf("Await = %d, %v, want 30, nil", v, err)
	}
}

// counterMsg is the message type of the concurrent-send counter.
type counterMsg interface{ counterMsg() }

// Inc adds one to the counter.
type Inc struct{}

// GetCount asks the counter for its count.
type GetCount struct{ Reply ReplyTo[int] }

func (Inc) counterMsg()      {}
func (GetCount) counterMsg() {}

// AT-MSG-08: MSG-120. Concurrent sends from a System are all delivered; run with -race.
func TestATMSG08ConcurrentSends(t *testing.T) {
	sys := newLocalSystem(t)
	n := 0
	counter, err := Spawn(sys, "counter", actorOf[counterMsg](&fnActor[counterMsg]{
		receive: func(ctx *Context[counterMsg], m counterMsg) {
			switch m := m.(type) {
			case Inc:
				n++
			case GetCount:
				m.Reply.Send(ctx, n)
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				if err := Send(sys, counter, counterMsg(Inc{})); err != nil {
					t.Errorf("Send: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
	v, err := Ask(sys, counter, func(rt ReplyTo[int]) counterMsg { return GetCount{rt} }).
		Await(within(t, 10*time.Second))
	if v != 8000 || err != nil {
		t.Errorf("count = %d, %v, want 8000, nil", v, err)
	}
}

// AT-MSG-09: MSG-300, MSG-320. The builder runs once, its ReplyTo travels in the message, and
// lockstep.ask names the ask address /temp/1, then /temp/2 with a greater uid.
func TestATMSG09AskBuilderAndRecord(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	got := make(chan GetBalance, 2)
	acct := spawnAcct(t, sys, got)
	calls := 0
	var stored ReplyTo[int64]
	build := func(rt ReplyTo[int64]) acctMsg {
		calls++
		stored = rt
		return GetBalance{rt}
	}
	r := Ask(sys, acct, build)
	if calls != 1 {
		t.Fatalf("build called %d times by the time Ask returned, want 1", calls)
	}
	if m := recv(t, got); m.Reply != stored {
		t.Errorf("acct received ReplyTo %v, want the builder's %v", m.Reply.addr, stored.addr)
	}
	first := stored.addr
	if first.node != "local" || first.path != "/temp/1" || first.uid == 0 {
		t.Errorf("first ask address %v, want node local, path /temp/1 and a uid", first)
	}
	if want := "lockstep://local/temp/1#" + strconv.FormatUint(first.uid, 16); first.String() != want {
		t.Errorf("first ask address renders %q, want %q", first, want)
	}
	want := []string{
		"from=" + sys.user.String(), "to=" + acct.String(), "ask=" + first.String(), "timeout=5s",
		"msg=" + rec.Describe(GetBalance{stored}),
	}
	if recs := recordsOf(rec.Records(), "lockstep.ask"); len(recs) != 1 || !slices.Equal(recs[0], want) {
		t.Errorf("lockstep.ask records = %q, want one %q", recs, want)
	}
	if v, err := r.Await(within(t, 10*time.Second)); v != 0 || err != nil {
		t.Errorf("Await = %d, %v, want 0, nil", v, err)
	}

	Ask(sys, acct, build)
	second := stored.addr
	if calls != 2 || second.path != "/temp/2" || second.uid <= first.uid {
		t.Errorf("second ask address %v after %d builder calls, want path /temp/2 and a uid above %#x",
			second, calls, first.uid)
	}
	recv(t, got)
}

// AT-MSG-10: MSG-300 step 5. A panicking builder propagates its panic out of Ask, leaves no table
// entry, no timer and no record, and consumes its ask number.
func TestATMSG10BuilderPanic(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	acct := spawnAcct(t, sys, nil)
	v := mustPanic(t, func() { Ask(sys, acct, func(ReplyTo[int64]) acctMsg { panic("boom") }) })
	if v != "boom" {
		t.Errorf("Ask panicked with %v, want boom", v)
	}
	if n := sys.asks.len(); n != 0 {
		t.Errorf("the asker table holds %d entries after the panic, want 0", n)
	}
	if recs := recordsOf(rec.Records(), "lockstep.ask"); len(recs) != 0 {
		t.Errorf("lockstep.ask records %q after the panic, want none", recs)
	}
	var next ReplyTo[int64]
	r := Ask(sys, acct, func(rt ReplyTo[int64]) acctMsg { next = rt; return GetBalance{rt} })
	if next.addr.path != "/temp/2" {
		t.Errorf("the next ask has path %q, want /temp/2", next.addr.path)
	}
	if _, err := r.Await(within(t, 10*time.Second)); err != nil {
		t.Errorf("the next ask: %v", err)
	}

	// No timer was started: the manual engine saw no AfterFunc.
	msys, eng := newManualSystem(t)
	macct := spawnAcct(t, msys, nil)
	eng.RunUntilIdle()
	mustPanic(t, func() { Ask(msys, macct, func(ReplyTo[int64]) acctMsg { panic("boom") }) })
	if labels := eng.Labels(); len(labels) != 0 {
		t.Errorf("timer labels %q after a builder panic, want none", labels)
	}
}

// AT-MSG-11 (zero PID clause): MSG-300 step 3. An ask to the zero PID is resolved with ErrNotFound
// before build runs; nothing is allocated or recorded.
func TestATMSG11AskToZeroPID(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	before := len(rec.Records())
	called := false
	r := Ask(sys, PID[acctMsg]{}, func(rt ReplyTo[int64]) acctMsg { called = true; return GetBalance{rt} })
	v, err := r.Await(cancelled())
	if v != 0 || !errors.Is(err, ErrNotFound) || err.Error() != "lockstep: not found: <zero>" {
		t.Errorf("Await = %d, %v, want 0 and ErrNotFound with text %q", v, err, "lockstep: not found: <zero>")
	}
	if called {
		t.Error("build was called")
	}
	if n := len(rec.Records()); n != before {
		t.Errorf("%d records emitted, want none", n-before)
	}
	if n := sys.asks.len(); n != 0 {
		t.Errorf("the asker table holds %d entries, want 0", n)
	}
	silent, stored := spawnSilent(t, sys)
	askSilent(sys, silent)
	if rt := recv(t, stored); rt.addr.path != "/temp/1" {
		t.Errorf("the next ask has path %q, want /temp/1 (no number was allocated)", rt.addr.path)
	}
}

// AT-MSG-14: MSG-320, MSG-420, MSG-430. An ask times out with the §7 text and lockstep.timeout; a
// later answer is late and changes nothing.
func TestATMSG14TimeoutAndLateAnswer(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	silent, stored := spawnSilent(t, sys)
	r := askSilent(sys, silent, WithTimeout(50*time.Millisecond))
	rt := recv(t, stored)
	v, err := r.Await(within(t, 10*time.Second))
	want := fmt.Sprintf("lockstep: timeout: ask %s to %s after 50ms", rt.addr, silent)
	if v != 0 || !errors.Is(err, ErrTimeout) || err.Error() != want {
		t.Fatalf("Await = %d, %v, want 0 and ErrTimeout with text %q", v, err, want)
	}
	wantRec := []string{"ask=" + rt.addr.String(), "to=" + silent.String(), "after=50ms"}
	if recs := askRecords(rec.Records(), "lockstep.timeout", rt.addr); len(recs) != 1 || !slices.Equal(recs[0], wantRec) {
		t.Errorf("lockstep.timeout records = %q, want one %q", recs, wantRec)
	}
	if recs := askRecords(rec.Records(), "lockstep.reply", rt.addr); len(recs) != 0 {
		t.Errorf("lockstep.reply records %q for a timeout, want none", recs)
	}

	inProbe(t, sys, silent, func(ctx *Context[any]) any { rt.Send(ctx, 1); return nil })
	all := rec.Records()
	late := []string{"ask=" + rt.addr.String(), "from=" + silent.String(), "outcome=late"}
	if recs := askRecords(all, "lockstep.reply", rt.addr); len(recs) != 1 || !slices.Equal(recs[0], late) {
		t.Errorf("lockstep.reply records after the late answer = %q, want one %q", recs, late)
	}
	timeoutAt := recordIndex(all, "lockstep.timeout", "ask="+rt.addr.String())
	lateAt := recordIndex(all, "lockstep.reply", "ask="+rt.addr.String(), "outcome=late")
	if timeoutAt < 0 || lateAt < timeoutAt {
		t.Errorf("lockstep.reply outcome=late at %d does not follow lockstep.timeout at %d", lateAt, timeoutAt)
	}
	if v2, err2 := r.Await(cancelled()); v2 != 0 || err2 != err {
		t.Errorf("Await after the late answer = %d, %v, want 0 and the same %v", v2, err2, err)
	}
}

// replyTwice is the message of the exactly-once replier: answer rt twice, first with err (or 1
// if err is nil), then with a value.
type replyTwice struct {
	rt  ReplyTo[int]
	err error
}

// AT-MSG-19: MSG-420, MSG-430. The first reply attempt wins; the second is late.
func TestATMSG19ExactlyOnce(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	done := make(chan struct{}, 2)
	a, err := Spawn(sys, "a", actorOf[replyTwice](&fnActor[replyTwice]{
		receive: func(ctx *Context[replyTwice], m replyTwice) {
			if m.err == nil {
				m.rt.Send(ctx, 1)
				m.rt.Send(ctx, 2)
			} else {
				m.rt.Fail(ctx, m.err)
				m.rt.Send(ctx, 3)
			}
			done <- struct{}{}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	outcomes := func(ask Address) []string {
		var out []string
		for _, attrs := range askRecords(rec.Records(), "lockstep.reply", ask) {
			for _, kv := range attrs {
				if o, ok := strings.CutPrefix(kv, "outcome="); ok {
					out = append(out, o)
				}
			}
		}
		return out
	}

	var rt1 ReplyTo[int]
	r1 := Ask(sys, a, func(rt ReplyTo[int]) replyTwice { rt1 = rt; return replyTwice{rt: rt} })
	recv(t, done)
	if v, err := r1.Await(within(t, 10*time.Second)); v != 1 || err != nil {
		t.Errorf("Await after Send(1), Send(2) = %d, %v, want 1, nil", v, err)
	}
	if got := outcomes(rt1.addr); !slices.Equal(got, []string{"resolved", "late"}) {
		t.Errorf("lockstep.reply outcomes %q, want [resolved late]", got)
	}

	e := errors.New("e")
	var rt2 ReplyTo[int]
	r2 := Ask(sys, a, func(rt ReplyTo[int]) replyTwice { rt2 = rt; return replyTwice{rt: rt, err: e} })
	recv(t, done)
	if v, err := r2.Await(within(t, 10*time.Second)); v != 0 || err != e { //nolint:errorlint // the very value Fail got
		t.Errorf("Await after Fail(e), Send(3) = %d, %v, want 0 and e itself", v, err)
	}
	if got := outcomes(rt2.addr); !slices.Equal(got, []string{"resolved", "late"}) {
		t.Errorf("lockstep.reply outcomes %q, want [resolved late]", got)
	}
	want := []string{"ask=" + rt2.addr.String(), "from=" + a.String(), "outcome=resolved", "err=" + rec.Describe(e)}
	if recs := askRecords(rec.Records(), "lockstep.reply", rt2.addr); len(recs) == 0 || !slices.Equal(recs[0], want) {
		t.Errorf("lockstep.reply records %q, want the first to be %q", recs, want)
	}
}

// AT-MSG-20: MSG-400, MSG-410. A zero ReplyTo records outcome=zero and does nothing else; misuse
// panics; ReplyTo values compare with ==.
func TestATMSG20ReplyToMisuse(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	before := len(rec.Records())
	ReplyTo[int]{}.Send(sys, 1)
	got := rec.Records()[before:]
	want := []string{"ask=<zero>", "from=" + sys.user.String(), "outcome=zero"}
	if len(got) != 1 || got[0].Kind != "lockstep.reply" || !slices.Equal(recordsOf(got, "lockstep.reply")[0], want) {
		t.Errorf("records of a zero ReplyTo's Send = %+v, want one lockstep.reply %q", got, want)
	}
	if n := sys.asks.len(); n != 0 {
		t.Errorf("the asker table holds %d entries, want 0", n)
	}

	silent, stored := spawnSilent(t, sys)
	askSilent(sys, silent, WithTimeout(time.Hour))
	askSilent(sys, silent, WithTimeout(time.Hour))
	rt1, rt2 := recv(t, stored), recv(t, stored)
	for name, rt := range map[string]ReplyTo[int]{"zero": {}, "live": rt1} {
		if v := mustPanic(t, func() { rt.Fail(sys, nil) }); v != "lockstep: ReplyTo.Fail with nil error" {
			t.Errorf("Fail(sys, nil) on a %s ReplyTo panicked with %v", name, v)
		}
		if v := mustPanic(t, func() { rt.Send(nil, 1) }); v != "lockstep: nil Sender" {
			t.Errorf("Send(nil, 1) on a %s ReplyTo panicked with %v", name, v)
		}
		if v := mustPanic(t, func() { rt.Fail(nil, errX) }); v != "lockstep: nil Sender" {
			t.Errorf("Fail(nil, err) on a %s ReplyTo panicked with %v", name, v)
		}
	}
	if n := sys.asks.len(); n != 2 {
		t.Errorf("the asker table holds %d entries after the panics, want the 2 asks", n)
	}
	copied, zero1, zero2 := rt1, ReplyTo[int]{}, ReplyTo[int]{}
	if copied != rt1 || rt1 == rt2 || zero1 != zero2 || rt1 == zero1 {
		t.Error("ReplyTo values do not compare by their ask address")
	}
}

// AT-MSG-28 (Ask clauses): MSG-700, MSG-300. WithTimeout never panics; an invalid final timeout
// resolves the reply before anything is built or recorded; the last option wins; nil options and
// builders panic.
func TestATMSG28TimeoutOptionAsk(t *testing.T) {
	_, _ = WithTimeout(0), WithTimeout(-time.Second)
	sys, rec := newRecordingSystem(t)
	acct := spawnAcct(t, sys, nil)
	before := messageRecords(rec.Records())
	called := false
	r := Ask(sys, acct, func(rt ReplyTo[int64]) acctMsg { called = true; return GetBalance{rt} }, WithTimeout(0))
	_, err := r.Await(cancelled())
	const want = "lockstep: invalid option: WithTimeout(0s): timeout must be positive"
	if !errors.Is(err, ErrInvalidOption) || err.Error() != want {
		t.Errorf("Ask with WithTimeout(0): Await = %v, want ErrInvalidOption with text %q", err, want)
	}
	if called || messageRecords(rec.Records()) != before {
		t.Errorf("build called: %v; %d records emitted; want neither", called, messageRecords(rec.Records())-before)
	}
	_, err = Ask(sys, acct, getBalance, WithTimeout(time.Hour), WithTimeout(-time.Second)).Await(cancelled())
	if want := "lockstep: invalid option: WithTimeout(-1s): timeout must be positive"; err == nil || err.Error() != want {
		t.Errorf("Ask with a last WithTimeout(-1s): Await = %v, want %q", err, want)
	}

	silent, stored := spawnSilent(t, sys)
	start := time.Now()
	r50 := askSilent(sys, silent, WithTimeout(time.Hour), WithTimeout(50*time.Millisecond))
	rt50 := recv(t, stored)
	if _, err := r50.Await(within(t, 10*time.Second)); !errors.Is(err, ErrTimeout) {
		t.Errorf("Ask with a last WithTimeout(50ms): Await = %v after %v, want ErrTimeout", err, time.Since(start))
	}
	if recs := askRecords(rec.Records(), "lockstep.ask", rt50.addr); len(recs) != 1 || !slices.Contains(recs[0], "timeout=50ms") {
		t.Errorf("lockstep.ask records %q, want one with timeout=50ms", recs)
	}
	rHour := askSilent(sys, silent, WithTimeout(0), WithTimeout(time.Hour))
	rtHour := recv(t, stored)
	if rHour.resolved.Load() {
		t.Error("Ask with WithTimeout(0), WithTimeout(time.Hour) is resolved, want pending")
	}
	if recs := askRecords(rec.Records(), "lockstep.ask", rtHour.addr); len(recs) != 1 || !slices.Contains(recs[0], "timeout=1h0m0s") {
		t.Errorf("lockstep.ask records %q, want one with timeout=1h0m0s", recs)
	}

	if v := mustPanic(t, func() { Ask(sys, acct, getBalance, nil) }); v != "lockstep: nil AskOption" {
		t.Errorf("Ask with a nil option panicked with %v", v)
	}
	p := PIDAt[int]("local", "/user/p")
	if v := mustPanic(t, func() { Ask[int, int](sys, p, nil) }); v != "lockstep: Ask with nil build function" {
		t.Errorf("Ask with a nil build panicked with %v", v)
	}
	if v := mustPanic(t, func() { Ask[int, int](nil, p, nil, nil) }); v != "lockstep: nil Sender" {
		t.Errorf("Ask with a nil Sender panicked with %v, want the nil Sender check first", v)
	}
}

// AT-MSG-39: MSG-320, ENG-070. The timeout timer is an AfterFunc labelled lockstep/ask-timeout
// on the asker's engine clock.
func TestATMSG39AskTimerLabel(t *testing.T) {
	sys, eng := newManualSystem(t)
	silent, _ := spawnSilent(t, sys)
	eng.RunUntilIdle()
	r := askSilent(sys, silent)
	if labels := eng.Labels(); !slices.Equal(labels, []string{"lockstep/ask-timeout"}) {
		t.Fatalf("Labels() = %q, want [lockstep/ask-timeout]", labels)
	}
	eng.RunUntilIdle()
	eng.Advance(5*time.Second - time.Nanosecond)
	if r.resolved.Load() {
		t.Fatal("the ask resolved before its 5 s timeout")
	}
	eng.Advance(time.Nanosecond)
	if _, err := r.Await(context.Background()); !errors.Is(err, ErrTimeout) {
		t.Errorf("Await after Advance(5s) = %v, want ErrTimeout", err)
	}
}

// MSG-310: every dead letter of the request on the asker's system resolves the ask with
// ErrTerminated before Ask returns, and a routing error with that error; a hand-off to the
// transport leaves it pending. Each immediate resolution records lockstep.reply from the target
// and cancels the timer.
func TestAskRoutingOutcomes(t *testing.T) {
	sys, eng := newManualSystem(t)
	a, _ := logActor(t, sys, eng)
	terminated := func(t *testing.T, s *System, to PID[int], recs func() []engine.Record) {
		t.Helper()
		var rt ReplyTo[int]
		r := Ask(s, to, func(x ReplyTo[int]) int { rt = x; return 1 })
		if !r.resolved.Load() {
			t.Fatalf("Ask to %v returned a pending reply, want it resolved", to)
		}
		_, err := r.Await(context.Background())
		want := "lockstep: actor terminated: " + to.String()
		if !errors.Is(err, ErrTerminated) || err.Error() != want {
			t.Errorf("Ask to %v: Await = %v, want ErrTerminated with text %q", to, err, want)
		}
		wantRec := []string{"ask=" + rt.addr.String(), "from=" + to.String(), "outcome=resolved", "err=" + err.Error()}
		if got := askRecords(recs(), "lockstep.reply", rt.addr); len(got) != 1 || !slices.Equal(got[0], wantRec) {
			t.Errorf("lockstep.reply records %q, want one %q", got, wantRec)
		}
		if n := s.asks.len(); n != 0 {
			t.Errorf("the asker table holds %d entries, want 0", n)
		}
	}
	t.Run("not-found", func(t *testing.T) {
		terminated(t, sys, PIDAt[int]("local", "/user/none"), eng.Records)
		terminated(t, sys, PIDAt[int]("local", a.Address().Path()+"/x"), eng.Records)
	})
	t.Run("stopped", func(t *testing.T) {
		gone := PID[int]{addr: Address{node: "local", path: a.Address().Path(), uid: a.Address().UID() + 100}}
		terminated(t, sys, gone, eng.Records)
	})
	t.Run("unreachable", func(t *testing.T) {
		b := engineBundle(t, "local")
		b.Transport = nil
		nsys, err := newSystem(b, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec, ok := b.Tracer.(*enginetest.Recorder)
		if !ok {
			t.Fatalf("the manual bundle's tracer is a %T", b.Tracer)
		}
		terminated(t, nsys, PIDAt[int]("n2", "/user/a"), rec.Records)
	})
	t.Run("transport error", func(t *testing.T) {
		eng.SetDeliverError(errX)
		defer eng.SetDeliverError(nil)
		to := PIDAt[int]("n2", "/user/a")
		r := Ask(sys, to, func(ReplyTo[int]) int { return 1 })
		_, err := r.Await(context.Background())
		if !errors.Is(err, errX) || err.Error() != "lockstep: send to "+to.String()+": errX" {
			t.Errorf("Ask with a failing transport: Await = %v, want the wrapped errX", err)
		}
	})
	t.Run("handed", func(t *testing.T) {
		to := PIDAt[int]("n2", "/user/a")
		var rt ReplyTo[int]
		r := Ask(sys, to, func(x ReplyTo[int]) int { rt = x; return 7 })
		if r.resolved.Load() {
			t.Fatal("an ask handed to the transport is resolved, want pending")
		}
		d := eng.Delivered()
		p, ok := d[len(d)-1].Payload.(userPayload)
		if !ok || p.ask != rt.addr || p.msg != 7 || p.typ != typeOf[int]() {
			t.Errorf("delivered payload %+v, want the request carrying ask %v", d[len(d)-1].Payload, rt.addr)
		}
	})
	// No immediate resolution left its timer behind: only the handed ask times out.
	before := len(recordsOf(eng.Records(), "lockstep.timeout"))
	eng.Advance(5 * time.Second)
	if n := len(recordsOf(eng.Records(), "lockstep.timeout")) - before; n != 1 {
		t.Errorf("%d asks timed out, want 1 (the handed one)", n)
	}
}

// ENG-150, MSG-310: a request the interposer holds leaves the ask pending; when the engine offers
// it, an enqueue tracks the ask and a dead letter resolves it with ErrTerminated.
func TestAskHeld(t *testing.T) {
	eng, sys, fake := interposedSystem(t, func(msg any) bool { _, ok := msg.(ReplyTo[int]); return ok })
	silent, stored := spawnSilent(t, sys)
	eng.RunUntilIdle()

	r := askSilent(sys, silent)
	if r.resolved.Load() || len(stored) != 0 {
		t.Fatal("a held ask is resolved or delivered")
	}
	fake.kept()
	eng.RunUntilIdle()
	rt := <-stored
	if got := sys.asks.trackedBy(cellOf(t, sys, silent)); !slices.Equal(got, []Address{rt.addr}) {
		t.Errorf("silent tracks %v after the offer, want [%v]", got, rt.addr)
	}
	send(t, sys, silent, any(answer{4}))
	eng.RunUntilIdle()
	if v, err := r.Await(context.Background()); v != 4 || err != nil {
		t.Errorf("Await = %d, %v, want 4, nil", v, err)
	}

	to := PIDAt[any]("local", "/user/later")
	r = askSilent(sys, to, WithTimeout(time.Hour))
	if r.resolved.Load() {
		t.Fatal("a held ask to an absent actor is resolved before the offer")
	}
	fake.kept()
	_, err := r.Await(context.Background())
	if want := "lockstep: actor terminated: " + to.String(); !errors.Is(err, ErrTerminated) || err.Error() != want {
		t.Errorf("Await after the offer = %v, want ErrTerminated with text %q", err, want)
	}
}

// MSG-340, MSG-410: an enqueued request makes the ask tracked by its target, in tracking order; a
// reply attempt passing through the system removes it, whoever answers.
func TestAskTracking(t *testing.T) {
	sys, eng := newManualSystem(t)
	silent, stored := spawnSilent(t, sys)
	fwd, _ := spawnProbe[any](t, sys, "fwd", nil)
	eng.RunUntilIdle()
	c := cellOf(t, sys, silent)

	r1 := askSilent(sys, silent, WithTimeout(time.Hour))
	r2 := askSilent(sys, silent, WithTimeout(time.Hour))
	eng.RunUntilIdle()
	rt1, rt2 := <-stored, <-stored
	if got := sys.asks.trackedBy(c); !slices.Equal(got, []Address{rt1.addr, rt2.addr}) {
		t.Fatalf("silent tracks %v, want [%v %v]", got, rt1.addr, rt2.addr)
	}
	// Another actor answers the second ask (a forwarded ReplyTo).
	inProbe(t, sys, fwd, func(ctx *Context[any]) any { rt2.Send(ctx, 2); return nil })
	if got := sys.asks.trackedBy(c); !slices.Equal(got, []Address{rt1.addr}) {
		t.Errorf("silent tracks %v after fwd answered, want [%v]", got, rt1.addr)
	}
	rt1.Send(sys, 1)
	if got := sys.asks.trackedBy(c); len(got) != 0 {
		t.Errorf("silent tracks %v after both answers, want none", got)
	}
	v1, _ := r1.Await(context.Background())
	v2, _ := r2.Await(context.Background())
	if v1 != 1 || v2 != 2 {
		t.Errorf("answers %d, %d, want 1, 2", v1, v2)
	}
	// Many tracked asks answered in turn leave nothing behind.
	for i := range 100 {
		r := askSilent(sys, silent)
		eng.RunUntilIdle()
		(<-stored).Send(sys, i)
		if v, err := r.Await(context.Background()); v != i || err != nil {
			t.Fatalf("ask %d: Await = %d, %v", i, v, err)
		}
	}
	if got := sys.asks.trackedBy(c); len(got) != 0 {
		t.Errorf("silent tracks %v, want none", got)
	}
}

// MSG-430 under -race: answers from other goroutines race the timeouts; each ask resolves exactly
// once, every Await sees that one result, and the records agree.
func TestAskResolutionRace(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	silent, stored := spawnSilent(t, sys)
	const n = 200
	replies := make([]*Reply[int], n)
	rts := make([]ReplyTo[int], n)
	var wg sync.WaitGroup
	for i := range n {
		// Timeouts from 1 µs to 1.8 ms: some fire before the answers, some race them.
		replies[i] = askSilent(sys, silent, WithTimeout(time.Duration(i%10)*200*time.Microsecond+time.Microsecond))
		rts[i] = recv(t, stored)
		wg.Go(func() { rts[i].Send(sys, i) })
		wg.Go(func() { rts[i].Fail(sys, errX) })
	}
	wg.Wait()
	recs := rec.Records()
	timedOut := 0
	for i, r := range replies {
		v, err := r.Await(within(t, 10*time.Second))
		for range 3 {
			if v2, err2 := r.Await(cancelled()); v2 != v || err2 != err { //nolint:errorlint // the same value
				t.Fatalf("ask %d: Await gave %d, %v, then %d, %v", i, v, err, v2, err2)
			}
		}
		resolved, late := 0, 0
		for _, a := range askRecords(recs, "lockstep.reply", rts[i].addr) {
			switch {
			case slices.Contains(a, "outcome=resolved"):
				resolved++
			case slices.Contains(a, "outcome=late"):
				late++
			}
		}
		timeouts := len(askRecords(recs, "lockstep.timeout", rts[i].addr))
		switch {
		case errors.Is(err, ErrTimeout):
			timedOut++
			if timeouts != 1 || resolved != 0 || late != 2 {
				t.Errorf("ask %d timed out with %d timeout, %d resolved and %d late records", i, timeouts, resolved, late)
			}
		case err == nil && v == i, errors.Is(err, errX) && v == 0:
			if timeouts != 0 || resolved != 1 || late != 1 {
				t.Errorf("ask %d answered with %d timeout, %d resolved and %d late records", i, timeouts, resolved, late)
			}
		default:
			t.Errorf("ask %d: Await = %d, %v", i, v, err)
		}
	}
	t.Logf("%d of %d asks timed out, the others were answered", timedOut, n)
}
