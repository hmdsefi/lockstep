// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// Deposit is the account message of the send tests (RFC-0002's acct).
type Deposit struct{ Amount int }

// acctMsg is an interface message type: its zero value is nil (AT-MSG-03).
type acctMsg interface{ acctMsg() }

func (Deposit) acctMsg() {}

// errX is the transport error of the send tests.
var errX = errors.New("errX")

// typeOf returns the reflect.Type of T, the type routeRequest.typ holds.
func typeOf[T any]() reflect.Type { return reflect.TypeFor[T]() }

// interposeCall is one call of fakeInterposer.
type interposeCall struct {
	from, to engine.Address
	msg      any
}

// fakeInterposer keeps offer of the messages hold selects and lets every other one through.
type fakeInterposer struct {
	hold  func(msg any) bool
	calls []interposeCall
	kept  func()
}

func (f *fakeInterposer) Interpose(from, to engine.Address, msg any, offer func()) bool {
	f.calls = append(f.calls, interposeCall{from, to, msg})
	if f.hold(msg) {
		f.kept = offer
		return true
	}
	return false
}

// callsTo returns the calls whose target is a.
func (f *fakeInterposer) callsTo(a engine.Address) []interposeCall {
	var out []interposeCall
	for _, c := range f.calls {
		if c.to == a {
			out = append(out, c)
		}
	}
	return out
}

// interposedSystem returns a manual system whose engine has a fakeInterposer that holds the
// messages hold selects.
func interposedSystem(t *testing.T, hold func(msg any) bool) (*enginetest.Engine, *System, *fakeInterposer) {
	t.Helper()
	eng := enginetest.New("local", 1)
	b := eng.Engine()
	fake := &fakeInterposer{hold: hold}
	b.Interposer = fake
	sys, err := newSystem(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	return eng, sys, fake
}

// AT-ENG-26: ENG-080, MSG-110. A non-local target goes to Transport.Deliver with Seq 0; its error
// comes back wrapped; with no Transport the message is a dead letter and Send returns nil.
func TestATENG26DeliverError(t *testing.T) {
	sys, eng := newManualSystem(t)
	p := PIDAt[int]("n2", "/user/a")
	if err := Send(sys, p, 1); err != nil {
		t.Fatalf("Send: %v", err)
	}
	got := eng.Delivered()
	if len(got) != 1 || got[0].To.Node != "n2" || got[0].Seq != 0 {
		t.Fatalf("Delivered() = %+v, want one envelope to n2 with Seq 0", got)
	}
	if got[0].From != sys.user.engine() || got[0].To != p.Address().engine() {
		t.Errorf("envelope from %v to %v, want %v to %v", got[0].From, got[0].To, sys.user, p)
	}

	e := errors.New("e")
	eng.SetDeliverError(e)
	if err := Send(sys, p, 2); !errors.Is(err, e) {
		t.Errorf("Send with a failing transport = %v, want an error matching e", err)
	}
	if n := len(eng.Delivered()); n != 2 {
		t.Errorf("%d envelopes delivered, want 2 (Deliver was called for the failing one)", n)
	}

	b := engineBundle(t, "local")
	b.Transport = nil
	nsys, err := newSystem(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Send(nsys, p, 3); err != nil {
		t.Errorf("Send without a transport = %v, want nil", err)
	}
	o := nsys.route(routeRequest{from: nsys.user, to: p.Address(), msg: 3})
	if o.kind != routeDead || o.reason != reasonUnreachable {
		t.Errorf("route without a transport = %+v, want a dead letter %q", o, reasonUnreachable)
	}
}

// AT-ENG-29: ENG-150. The interposer sees each local send before the offer; a held message is
// offered later without a second call.
func TestATENG29Interposer(t *testing.T) {
	eng, sys, fake := interposedSystem(t, func(msg any) bool { return msg == 1 })
	a, log := logActor(t, sys, eng)
	var sendErrs []error
	s, err := Spawn(sys, "s", actorOf[string](&fnActor[string]{
		receive: func(ctx *Context[string], _ string) {
			sendErrs = append(sendErrs, Send(ctx, a, 1), Send(ctx, a, 2))
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, s, "go")
	eng.RunUntilIdle()

	if len(sendErrs) != 2 || sendErrs[0] != nil || sendErrs[1] != nil {
		t.Errorf("Send errors = %v, want nil, nil", sendErrs)
	}
	if !slices.Equal(*log, []int{2}) {
		t.Fatalf("log %v, want [2]", *log)
	}
	calls := fake.callsTo(a.Address().engine())
	if len(calls) != 2 || calls[0].from != s.Address().engine() || calls[1].from != s.Address().engine() ||
		calls[0].msg != 1 || calls[1].msg != 2 {
		t.Errorf("interposer calls to A = %+v, want two from S with messages 1 and 2", calls)
	}

	sys.eng.Clock.AfterFunc(0, "lockstep/timer", fake.kept)
	eng.Advance(0)
	if !slices.Equal(*log, []int{2, 1}) {
		t.Errorf("log after the offer %v, want [2 1]", *log)
	}
	if n := len(fake.callsTo(a.Address().engine())); n != 2 {
		t.Errorf("%d interposer calls to A after the offer, want 2 (offer does not interpose)", n)
	}

	// Without an interposer the messages arrive in send order.
	nsys, neng := newManualSystem(t)
	na, nlog := logActor(t, nsys, neng)
	send(t, nsys, na, 1)
	send(t, nsys, na, 2)
	neng.RunUntilIdle()
	if !slices.Equal(*nlog, []int{1, 2}) {
		t.Errorf("log without an interposer %v, want [1 2]", *nlog)
	}
}

// AT-MSG-02: MSG-010, MSG-030. The receiver gets the very pointer, slice and map that were sent.
func TestATMSG02NoCopyLocal(t *testing.T) {
	type T struct{ n int }
	sys, eng := newManualSystem(t)
	var got []any
	pid, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{
		receive: func(_ *Context[any], m any) { got = append(got, m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	ptr := &T{1}
	sl := make([]int, 2, 4)
	mp := map[string]int{"k": 1}
	send(t, sys, pid, any(ptr))
	send(t, sys, pid, any(sl))
	send(t, sys, pid, any(mp))
	eng.RunUntilIdle()
	if len(got) != 3 {
		t.Fatalf("received %d messages, want 3", len(got))
	}
	if p, ok := got[0].(*T); !ok || p != ptr {
		t.Errorf("received pointer %v, want %p", got[0], ptr)
	}
	if s, ok := got[1].([]int); !ok || &s[0] != &sl[0] || len(s) != 2 || cap(s) != 4 {
		t.Errorf("received slice %v does not share the sent backing array", got[1])
	}
	if m, ok := got[2].(map[string]int); !ok {
		t.Errorf("received %T, want the map", got[2])
	} else {
		m["w"] = 2
		if mp["w"] != 2 {
			t.Error("a write through the received map is not visible in the sender's map")
		}
	}
}

// AT-MSG-03: MSG-020. A nil interface message is delivered as nil.
func TestATMSG03NilInterfaceMessage(t *testing.T) {
	sys, eng := newManualSystem(t)
	var received []acctMsg
	pid, err := Spawn(sys, "acct", actorOf[acctMsg](&fnActor[acctMsg]{
		receive: func(_ *Context[acctMsg], m acctMsg) { received = append(received, m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := Send(sys, pid, acctMsg(nil)); err != nil {
		t.Fatalf("Send of a nil message = %v, want nil", err)
	}
	eng.RunUntilIdle()
	if len(received) != 1 || received[0] != nil {
		t.Errorf("received %v, want one nil message", received)
	}
}

// AT-MSG-06: MSG-110, §8. lockstep.send carries the sender, the target and the described message.
func TestATMSG06SendRecord(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	acct, err := Spawn(sys, "acct", idle[Deposit]())
	if err != nil {
		t.Fatal(err)
	}
	if err := Send(sys, acct, Deposit{5}); err != nil {
		t.Fatal(err)
	}
	want := func(from Address) []string {
		return []string{"from=" + from.String(), "to=" + acct.String(), "msg=" + rec.Describe(Deposit{5})}
	}
	got := recordsOf(rec.Records(), "lockstep.send")
	if len(got) != 1 || !slices.Equal(got[0], want(sys.user)) {
		t.Fatalf("lockstep.send records after the system's send = %q, want one %q", got, want(sys.user))
	}

	// An ad hoc actor p sends from inside its Receive.
	done := make(chan struct{})
	p, err := Spawn(sys, "p", actorOf[int](&fnActor[int]{
		receive: func(ctx *Context[int], _ int) {
			if err := Send(ctx, acct, Deposit{5}); err != nil {
				t.Errorf("Send from p: %v", err)
			}
			close(done)
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := Send(sys, p, 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("p did not run within 1 s")
	}
	got = recordsOf(rec.Records(), "lockstep.send")
	if len(got) != 3 || !slices.Equal(got[2], want(p.Address())) {
		t.Errorf("lockstep.send records = %q, want the third to be %q", got, want(p.Address()))
	}
}

// AT-MSG-04: MSG-100 (TestATMSG04SendFromStoppedSystem has the stopped check). A zero PID is ErrNotFound; a nil Sender or
// a Context outside its turn panics, before the zero PID is looked at.
func TestATMSG04SendChecks(t *testing.T) {
	sys, eng := newManualSystem(t)
	err := Send(sys, PID[acctMsg]{}, acctMsg(Deposit{}))
	if !errors.Is(err, ErrNotFound) || err.Error() != "lockstep: not found: <zero>" {
		t.Errorf("Send to the zero PID = %v, want ErrNotFound with text %q", err, "lockstep: not found: <zero>")
	}
	for name, from := range map[string]Sender{
		"nil interface": nil,
		"nil *System":   (*System)(nil),
		"nil *Context":  (*Context[int])(nil),
	} {
		if v := mustPanic(t, func() { _ = Send(from, PID[int]{}, 1) }); v != "lockstep: nil Sender" {
			t.Errorf("Send from a %s panicked with %v, want %q", name, v, "lockstep: nil Sender")
		}
	}
	if v := mustPanic(t, func() { _ = Send[int](nil, PIDAt[int]("local", "/user/a"), 1) }); v != "lockstep: nil Sender" {
		t.Errorf("Send[int](nil, p, 1) panicked with %v", v)
	}

	var ctx *Context[int]
	a, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{started: func(c *Context[int]) { ctx = c }}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	want := "lockstep: Context of " + a.String() + " used outside its turn"
	if v := mustPanic(t, func() { _ = Send(ctx, PID[int]{}, 1) }); v != want {
		t.Errorf("Send with a Context outside its turn panicked with %v, want %q", v, want)
	}
}

// AT-ACT-34 (Send clauses), ACT-440, MSG-110: a path address names whichever actor of type M is
// registered at its path when it is used; any other target is a dead letter, and Send returns nil.
func TestSendPathAddress(t *testing.T) {
	sys, eng := newManualSystem(t)
	nrec := len(eng.Records())
	p := PIDAt[int]("local", "/user/a")
	if len(eng.Records()) != nrec || p.IsZero() || p.Address().UID() != 0 {
		t.Error("PIDAt emitted a record, is zero, or has a uid")
	}
	deadNotFound := func(to PID[int], msg int) {
		t.Helper()
		if err := Send(sys, to, msg); err != nil {
			t.Errorf("Send(%v, %d) = %v, want nil", to, msg, err)
		}
		o := sys.route(routeRequest{from: sys.user, to: to.Address(), typ: typeOf[int](), msg: msg})
		if o.kind != routeDead || o.reason != reasonNotFound {
			t.Errorf("route to %v = %+v, want a dead letter %q", to, o, reasonNotFound)
		}
	}
	// No actor yet.
	deadNotFound(p, 1)
	eng.RunUntilIdle()

	// An actor of another type at /user/b.
	var strs []string
	b, err := Spawn(sys, "b", actorOf[string](&fnActor[string]{
		receive: func(_ *Context[string], m string) { strs = append(strs, m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	deadNotFound(PIDAt[int]("local", "/user/b"), 4)
	send(t, sys, b, "x")
	eng.RunUntilIdle()
	if !slices.Equal(strs, []string{"x"}) {
		t.Errorf("the string actor received %v, want only [x]", strs)
	}

	// An int actor at /user/a: the path address reaches it.
	a, log := logActor(t, sys, eng)
	if p == a {
		t.Error("the path PID equals the PID Spawn returned")
	}
	if err := Send(sys, p, 2); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if !slices.Equal(*log, []int{2}) {
		t.Errorf("log %v, want [2]: the earlier dead letters were not delivered, the path send was", *log)
	}
	o := sys.route(routeRequest{from: sys.user, to: p.Address(), typ: typeOf[int](), msg: 3})
	if o.kind != routeEnqueued || o.cell != actorCell(cellOf(t, sys, a)) {
		t.Errorf("route to the int actor = %+v, want it enqueued at the actor's cell", o)
	}
}

// MSG-110, MBX-150: a full address that is not registered is a dead letter with reason stopped;
// another node's address goes to the transport.
func TestRouteOutcomes(t *testing.T) {
	sys, eng := newManualSystem(t)
	a, log := logActor(t, sys, eng)
	gone := Address{node: "local", path: a.Address().Path(), uid: a.Address().UID() + 100}
	nowhere := Address{node: "local", path: "/user/zz", uid: 99}
	for name, to := range map[string]Address{"unknown uid": gone, "unregistered path": nowhere} {
		o := sys.route(routeRequest{from: sys.user, to: to, typ: typeOf[int](), msg: 1})
		if o.kind != routeDead || o.reason != reasonStopped || o.cell != nil || o.err != nil {
			t.Errorf("route to an %s = %+v, want a dead letter %q", name, o, reasonStopped)
		}
		if err := Send(sys, PID[int]{addr: to}, 1); err != nil {
			t.Errorf("Send to an %s = %v, want nil", name, err)
		}
	}
	eng.RunUntilIdle()
	if len(*log) != 0 {
		t.Errorf("a dead letter reached the live actor: %v", *log)
	}
	o := sys.route(routeRequest{from: sys.user, to: PIDAt[int]("n2", "/user/a").Address(), msg: 1})
	if o.kind != routeHanded || o.err != nil {
		t.Errorf("route to another node = %+v, want it handed to the transport", o)
	}
	eng.SetDeliverError(errX)
	o = sys.route(routeRequest{from: sys.user, to: PIDAt[int]("n2", "/user/a").Address(), msg: 1})
	if o.kind != routeFailed || !errors.Is(o.err, errX) {
		t.Errorf("route to another node with a failing transport = %+v, want routeFailed wrapping errX", o)
	}
}

// ENG-150: a message the interposer holds is routeHeld, and the dead-letter decision is made when
// the engine offers it.
func TestRouteHeld(t *testing.T) {
	eng, sys, fake := interposedSystem(t, func(any) bool { return true })
	a, log := logActor(t, sys, eng)
	o := sys.route(routeRequest{from: sys.user, to: a.Address(), typ: typeOf[int](), msg: 1})
	if o.kind != routeHeld || o.cell != nil {
		t.Fatalf("route of a held message = %+v, want routeHeld", o)
	}
	fake.kept()
	eng.RunUntilIdle()
	if !slices.Equal(*log, []int{1}) {
		t.Errorf("log %v, want [1] after the offer", *log)
	}
}

// AT-MSG-37: MSG-110. A transport error comes back wrapped with the target address.
func TestATMSG37TransportErrorsSend(t *testing.T) {
	sys, eng := newManualSystem(t)
	p := PIDAt[int]("n2", "/user/a")
	eng.SetDeliverError(errX)
	err := Send(sys, p, 1)
	if !errors.Is(err, errX) {
		t.Fatalf("Send = %v, want an error matching errX", err)
	}
	if want := "lockstep: send to lockstep://n2/user/a#0: errX"; err.Error() != want {
		t.Errorf("error text %q, want %q", err, want)
	}
	eng.SetDeliverError(nil)
	if err := Send(sys, p, 2); err != nil {
		t.Errorf("Send with Deliver returning nil = %v, want nil", err)
	}
}

// AT-ACT-18 (the system's sends): ACT-340. Messages sent right after Spawn wait for Started.
func TestATACT18MessagesBeforeStart(t *testing.T) {
	sys := newLocalSystem(t)
	got := make(chan string, 4)
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		started: func(*Context[int]) { got <- "started" },
		receive: func(_ *Context[int], m int) { got <- strconv.Itoa(m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	for m := 1; m <= 3; m++ {
		if err := Send(sys, pid, m); err != nil {
			t.Fatal(err)
		}
	}
	var seq []string
	timeout := time.After(time.Second)
	for len(seq) < 4 {
		select {
		case s := <-got:
			seq = append(seq, s)
		case <-timeout:
			t.Fatalf("received %v within 1 s, want 4 events", seq)
		}
	}
	if want := []string{"started", "1", "2", "3"}; !slices.Equal(seq, want) {
		t.Errorf("events %v, want %v", seq, want)
	}
}
