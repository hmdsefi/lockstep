// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/internal/supervise"
)

// closer is an io.Closer that logs its name in log when closed and then returns err or, when
// panics is set, panics.
type closer struct {
	name   string
	log    *[]string
	err    error
	panics bool
}

func (c *closer) Close() error {
	*c.log = append(*c.log, c.name)
	if c.panics {
		panic("close " + c.name)
	}
	return c.err
}

// errC is the error r2 of AT-SUP-14 returns from Close.
var errC = errors.New("errC")

// systemMsgs empties c's system queue, which a hold keeps c from processing, and returns its
// messages in queue order.
func systemMsgs(c *cell[any]) []sysMsg {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []sysMsg
	for {
		m, ok := c.sysq.Pop()
		if !ok {
			return out
		}
		out = append(out, m)
	}
}

// closeRecords returns the lockstep.close records of recs as "index type err" strings, in record
// order, for the actor and incarnation they name.
func closeRecords(t *testing.T, recs [][]string, actor string, inc int) []string {
	t.Helper()
	var out []string
	for _, r := range recs {
		if len(r) != 5 || r[0] != "actor="+actor || r[1] != "inc="+strconv.Itoa(inc) {
			t.Fatalf("lockstep.close record %q: want attributes actor, inc, index, type, err of %s inc %d", r, actor, inc)
		}
		out = append(out, strings.TrimPrefix(r[2], "index=")+" "+strings.TrimPrefix(r[3], "type=")+" "+
			strings.TrimPrefix(r[4], "err="))
	}
	return out
}

// AT-SUP-14: SUP-120. The actor owns r1, r2 and r3 in Started and r4 in Stopped; r2's Close returns
// errC and r3's panics. The stop closes r4, r3, r2, r1, each once, and records them with indexes
// 3, 2, 1, 0 before lockstep.stop.
func TestATSUP14OwnOrderAndErrors(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs, closed []string
	r1 := &closer{name: "r1", log: &closed}
	r2 := &closer{name: "r2", log: &closed, err: errC}
	r3 := &closer{name: "r3", log: &closed, panics: true}
	r4 := &closer{name: "r4", log: &closed}
	p, err := Spawn(sys, "p", actorOf[any](&supProbe{
		obs: &obs,
		onStart: func(ctx *Context[any]) {
			ctx.Own(r1)
			ctx.Own(r2)
			ctx.Own(r3)
		},
		onStop: func(ctx *Context[any], _ StopReason) { ctx.Own(r4) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if len(closed) != 0 {
		t.Fatalf("closed %q before the stop", closed)
	}
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	if want := []string{"r4", "r3", "r2", "r1"}; !slices.Equal(closed, want) {
		t.Errorf("close order %q, want %q", closed, want)
	}
	ty := reflect.TypeOf(r1).String()
	got := closeRecords(t, recordsOf(eng.Records(), "lockstep.close"), p.String(), 1)
	if len(got) != 4 {
		t.Fatalf("lockstep.close records %q, want four", got)
	}
	if got[0] != "3 "+ty+" " || got[1] != "2 "+ty+" lockstep: panic: close r3" || got[2] != "1 "+ty+" errC" ||
		got[3] != "0 "+ty+" " {
		t.Errorf("lockstep.close records %q", got)
	}
	// Every close precedes the stop record.
	last := indexOf(eng.Records(), "lockstep.close", "index=0")
	stop := indexOf(eng.Records(), "lockstep.stop")
	if last < 0 || stop < 0 || last > stop {
		t.Errorf("lockstep.close index=0 at %d, lockstep.stop at %d; want the closes first", last, stop)
	}
	if n := len(recordsOf(eng.Records(), "lockstep.fail")); n != 0 {
		t.Errorf("%d lockstep.fail records, want none: a failing Close is not a failure", n)
	}
}

// SUP-120: a resource owned twice is closed twice, once per registration, and the list is empty
// when the stop is done.
func TestSUP120OwnTwiceClosesTwice(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs, closed []string
	r := &closer{name: "r", log: &closed}
	p, err := Spawn(sys, "p", actorOf[any](&supProbe{
		obs:     &obs,
		onStart: func(ctx *Context[any]) { ctx.Own(r); ctx.Own(r) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, p)
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if want := []string{"r", "r"}; !slices.Equal(closed, want) {
		t.Errorf("closed %q, want %q", closed, want)
	}
	if len(c.ctx.owned) != 0 {
		t.Errorf("%d resources still listed after the stop, want none", len(c.ctx.owned))
	}
}

// SUP-120: an actor without a Stopped hook still closes its owned resources at the stop.
func TestSUP120CloseWithoutStopper(t *testing.T) {
	sys, eng := newManualSystem(t)
	var closed []string
	r1 := &closer{name: "r1", log: &closed}
	r2 := &closer{name: "r2", log: &closed}
	p, err := Spawn(sys, "p", actorOf[any](&fnActor[any]{started: func(ctx *Context[any]) {
		ctx.Own(r1)
		ctx.Own(r2)
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if want := []string{"r2", "r1"}; !slices.Equal(closed, want) {
		t.Errorf("closed %q, want %q", closed, want)
	}
}

// AT-SUP-14, SUP-120, SUP-140: Own(nil) panics with lockstep: Own called with nil resource,
// recovered as a failure of the handler's source; the text is the panic value's.
func TestATSUP14OwnNil(t *testing.T) {
	const text = "lockstep: Own called with nil resource"
	sys, eng := newManualSystem(t)
	var obs []string
	p := &supProbe{obs: &obs}
	p.receive = func(ctx *Context[any], _ any) { ctx.Own(nil) }
	parent, kids := supervised(t, sys, eng, actorOf[any](p))
	send(t, sys, kids[0], any("own"))
	eng.RunUntilIdle()

	want := [][]string{{"actor=" + kids[0].String(), "inc=1", "source=receive", "err=lockstep: panic: " + text}}
	if got := recordsOf(eng.Records(), "lockstep.fail"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.fail records:\n got %q\nwant %q", got, want)
	}
	n := failedNotices(parent)
	var pe *PanicError
	if len(n) != 1 || !errors.As(n[0].err, &pe) || pe.Value != text {
		t.Errorf("failed notices %+v, want one with a *PanicError of %q", n, text)
	}
}

// SUP-110: a panic in Stopped, and separately a Fail in it, is a cleanup failure. The stop goes on:
// the owned resources close, lockstep.stop is recorded and the actor reaches Stopped. One
// lockstep.fail with source stopped is recorded and one cleanupFailed notice reaches the
// supervisor, which has no failed notice for it.
func TestSUP110PanicInStoppedContinuesStop(t *testing.T) {
	tests := []struct {
		name string
		stop func(*Context[any], StopReason)
		err  string
		as   func(error) bool
	}{
		{
			name: "panic",
			stop: func(*Context[any], StopReason) { panic("stopped boom") },
			err:  "lockstep: panic: stopped boom",
			as: func(err error) bool {
				var pe *PanicError
				return errors.As(err, &pe) && pe.Value == "stopped boom"
			},
		},
		{
			name: "fail",
			stop: func(ctx *Context[any], _ StopReason) { ctx.Fail(errProbe) },
			err:  "probe failed",
			as:   func(err error) bool { return err == errProbe },
		},
		{
			name: "own nil",
			stop: func(ctx *Context[any], _ StopReason) { ctx.Own(nil) },
			err:  "lockstep: panic: lockstep: Own called with nil resource",
			as: func(err error) bool {
				var pe *PanicError
				return errors.As(err, &pe) && pe.Value == "lockstep: Own called with nil resource"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sys, eng := newManualSystem(t)
			var obs, closed []string
			r1 := &closer{name: "r1", log: &closed}
			r2 := &closer{name: "r2", log: &closed}
			p := &supProbe{obs: &obs, onStop: tt.stop}
			p.onStart = func(ctx *Context[any]) { ctx.Own(r1); ctx.Own(r2) }
			parent, kids := supervised(t, sys, eng, actorOf[any](p))
			child := cellOf(t, sys, kids[0])
			if err := Stop(sys, kids[0].Address()); err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle()

			if want := []string{"r2", "r1"}; !slices.Equal(closed, want) {
				t.Errorf("closed %q, want %q", closed, want)
			}
			wantStop := [][]string{{"actor=" + kids[0].String(), "inc=1", "reason=normal", "err="}}
			if got := recordsOf(eng.Records(), "lockstep.stop"); !slices.EqualFunc(got, wantStop, slices.Equal) {
				t.Errorf("lockstep.stop records %q, want %q", got, wantStop)
			}
			wantFail := [][]string{{"actor=" + kids[0].String(), "inc=1", "source=stopped", "err=" + tt.err}}
			if got := recordsOf(eng.Records(), "lockstep.fail"); !slices.EqualFunc(got, wantFail, slices.Equal) {
				t.Errorf("lockstep.fail records:\n got %q\nwant %q", got, wantFail)
			}
			if st, _ := child.lifecycle(); st != supervise.Stopped {
				t.Errorf("state %v, want stopped", st)
			}

			var notices []cleanupFailed
			var kinds []string
			for _, m := range systemMsgs(parent) {
				kinds = append(kinds, fmt.Sprintf("%T", m))
				if n, ok := m.(cleanupFailed); ok {
					notices = append(notices, n)
				}
			}
			// The cleanup notice precedes the stopped notice and no failed notice is sent.
			if want := []string{"lockstep.cleanupFailed", "lockstep.stopped"}; !slices.Equal(kinds, want) {
				t.Errorf("parent's system messages %q, want %q", kinds, want)
			}
			if len(notices) != 1 {
				t.Fatalf("cleanupFailed notices %+v, want one", notices)
			}
			if n := notices[0]; n.child != kids[0].Address() || n.inc != 1 || !tt.as(n.err) {
				t.Errorf("notice %+v, want child %v, inc 1 and the failure %q", n, kids[0], tt.err)
			}
		})
	}
}

// SUP-100: Self, Send and Ask work inside Stopped, Spawn returns ErrStopped, ctx.Stop does nothing
// and Own registers a resource that is closed with the others (AT-SUP-12 without timers).
func TestSUP100ContextInsideStopped(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obsO, obsP, closed []string
	o, err := Spawn(sys, "o", actorOf[any](&supProbe{obs: &obsO}))
	if err != nil {
		t.Fatal(err)
	}
	var spawnErr error
	var self PID[any]
	var reply *Reply[string]
	r := &closer{name: "r", log: &closed}
	p, err := Spawn(sys, "p", actorOf[any](&supProbe{
		obs: &obsP,
		onStop: func(ctx *Context[any], _ StopReason) {
			self = ctx.Self()
			if err := Send(ctx, o, any("from stopped")); err != nil {
				panic(err)
			}
			reply = Ask(ctx, o, func(rt ReplyTo[string]) any { return rt })
			_, spawnErr = Spawn(ctx, "x", idle[any]())
			ctx.Stop()
			ctx.Own(r)
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	if self != p {
		t.Errorf("Self inside Stopped = %v, want %v", self, p)
	}
	if len(obsO) != 3 || obsO[1] != "from stopped" {
		t.Errorf("the observer received %q, want started, the Send and the Ask request", obsO)
	}
	if reply == nil {
		t.Error("Ask inside Stopped returned nil")
	}
	if !errors.Is(spawnErr, ErrStopped) {
		t.Errorf("Spawn inside Stopped = %v, want ErrStopped", spawnErr)
	}
	if want := []string{"r"}; !slices.Equal(closed, want) {
		t.Errorf("closed %q, want %q", closed, want)
	}
	if got, want := stopRecordsOf(eng.Records()), []string{p.String() + " normal"}; !slices.Equal(got, want) {
		t.Errorf("lockstep.stop records %q, want %q", got, want)
	}
	if want := []string{"started /user/p 1", "stopped /user/p normal"}; !slices.Equal(obsP, want) {
		t.Errorf("observations %q, want %q: Stopped runs once", obsP, want)
	}
}
