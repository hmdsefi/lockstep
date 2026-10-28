// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine/local"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// ping is the message of the OBS log event tests.
type ping struct{}

// obsActor is an actor of ping messages built from optional functions.
type obsActor struct {
	started func(ctx *lockstep.Context[ping])
	receive func(ctx *lockstep.Context[ping], m ping)
	stopped func(ctx *lockstep.Context[ping], r lockstep.StopReason)
}

func (a *obsActor) Started(ctx *lockstep.Context[ping]) {
	if a.started != nil {
		a.started(ctx)
	}
}

func (a *obsActor) Receive(ctx *lockstep.Context[ping], m ping) {
	if a.receive != nil {
		a.receive(ctx, m)
	}
}

func (a *obsActor) Stopped(ctx *lockstep.Context[ping], r lockstep.StopReason) {
	if a.stopped != nil {
		a.stopped(ctx, r)
	}
}

// obsOf returns a factory that returns a every time.
func obsOf(a *obsActor) func() lockstep.Actor[ping] {
	return func() lockstep.Actor[ping] { return a }
}

// panicky returns a factory of actors whose Receive panics with "boom".
func panicky() func() lockstep.Actor[ping] {
	return obsOf(&obsActor{receive: func(*lockstep.Context[ping], ping) { panic("boom") }})
}

// obsEffect is an Effect named name whose Run is run.
type obsEffect struct {
	name string
	run  func(ctx context.Context) (int, error)
}

func (e obsEffect) Name() string                         { return e.name }
func (e obsEffect) Run(ctx context.Context) (int, error) { return e.run(ctx) }

// errCloser is an io.Closer whose Close returns err.
type errCloser struct{ err error }

func (c errCloser) Close() error { return c.err }

// spawnObs spawns a on sys as name and fails the test on an error.
func spawnObs(t *testing.T, sys *lockstep.System, name string, factory func() lockstep.Actor[ping],
	opts ...lockstep.SpawnOption,
) lockstep.PID[ping] {
	t.Helper()
	pid, err := lockstep.Spawn(sys, name, factory, opts...)
	if err != nil {
		t.Fatalf("Spawn %s: %v", name, err)
	}
	return pid
}

// waitRecords polls h until it holds at least n records with message msg, then returns them; it
// fails the test after 5 s.
func waitRecords(t *testing.T, h *recHandler, msg string, n int) []recRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		recs := h.withMsg(msg)
		if len(recs) >= n {
			return recs
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d records %q after 5 s, want %d; all records: %v", len(recs), msg, n, h.records())
		}
		time.Sleep(time.Millisecond)
	}
}

// checkRecord fails the test unless rec has level and exactly the attribute keys keys, in order.
func checkRecord(t *testing.T, rec recRecord, level slog.Level, keys ...string) {
	t.Helper()
	if rec.level != level {
		t.Errorf("%q at %v, want %v", rec.msg, rec.level, level)
	}
	if got := rec.keys(); !slices.Equal(got, keys) {
		t.Errorf("%q has keys %v, want %v", rec.msg, got, keys)
	}
}

// attrOf returns the attribute key of rec, failing the test if there is none.
func attrOf(t *testing.T, rec recRecord, key string) slog.Value {
	t.Helper()
	for _, a := range rec.attrs {
		if a.Key == key {
			return a.Value
		}
	}
	t.Fatalf("%q has no attribute %s: %v", rec.msg, key, rec.attrs)
	return slog.Value{}
}

// errText returns the text of v, an error attribute, or fails the test.
func errText(t *testing.T, v slog.Value) string {
	t.Helper()
	e, ok := v.Any().(error)
	if v.Kind() != slog.KindAny || !ok || e == nil {
		t.Fatalf("attribute %v (%v) is not an error", v, v.Kind())
	}
	return e.Error()
}

// actorAttrs returns the actor attributes of OBS-020 for pid on node.
func actorAttrs(node string, pid lockstep.PID[ping]) []slog.Attr {
	return []slog.Attr{
		slog.String("node", node),
		slog.String("path", pid.Address().Path()),
		slog.Uint64("uid", pid.Address().UID()),
	}
}

// checkAttrs fails the test unless the attributes of rec start with want, kinds included.
func checkAttrs(t *testing.T, rec recRecord, want ...slog.Attr) {
	t.Helper()
	if len(rec.attrs) < len(want) {
		t.Fatalf("%q has attributes %v, want them to start with %v", rec.msg, rec.attrs, want)
	}
	for i, w := range want {
		if a := rec.attrs[i]; !a.Equal(w) || a.Value.Kind() != w.Value.Kind() {
			t.Errorf("%q attribute %d = %v (%v), want %v (%v)", rec.msg, i, a, a.Value.Kind(), w, w.Value.Kind())
		}
	}
}

// stoppedPID spawns an actor named name on sys that stops in Started, waits until its stop has
// completed and returns its PID: every Send to it is a dead letter with reason stopped.
func stoppedPID(t *testing.T, sys *lockstep.System, name string) lockstep.PID[ping] {
	t.Helper()
	pid := spawnObs(t, sys, name, obsOf(&obsActor{started: func(ctx *lockstep.Context[ping]) { ctx.Stop() }}))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := lockstep.Lookup[ping](sys, pid.Address().Path()); errors.Is(err, lockstep.ErrNotFound) {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("%v did not stop within 5 s", pid)
		}
		time.Sleep(time.Millisecond)
	}
}

// AT-OBS-01: OBS-010.
func TestATOBS01DefaultLogger(t *testing.T) {
	a, ha := newRecLogger(slog.LevelInfo)
	b, hb := newRecLogger(slog.LevelDebug)
	setDefault(t, a)
	sys := newProdSystem(t)
	slog.SetDefault(b)

	pid := spawnObs(t, sys, "a", panicky())
	if err := lockstep.Send(sys, pid, ping{}); err != nil {
		t.Fatal(err)
	}
	waitRecords(t, ha, "actor failed", 1)
	if recs := hb.records(); len(recs) != 0 {
		t.Errorf("the default logger set after New got %d records, want 0: %v", len(recs), recs)
	}
}

// AT-OBS-02: OBS-020, OBS-060.
func TestATOBS02AttributeOrder(t *testing.T) {
	l, h := newRecLogger(slog.LevelInfo)
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l))
	pid := spawnObs(t, sys, "a", panicky())
	if err := lockstep.Send(sys, pid, ping{}); err != nil {
		t.Fatal(err)
	}
	waitRecords(t, h, "actor restarted", 1)

	var got []recRecord
	for _, r := range h.records() {
		if r.msg == "actor failed" || r.msg == "actor restarted" {
			got = append(got, r)
		}
	}
	if len(got) != 2 || got[0].msg != "actor failed" || got[1].msg != "actor restarted" {
		t.Fatalf("records %v, want actor failed then actor restarted", got)
	}
	want := actorAttrs("n1", pid)
	failed := got[0]
	checkRecord(t, failed, slog.LevelError, "node", "path", "uid", "err", "directive", "panic")
	checkAttrs(t, failed, want...)
	if s := errText(t, attrOf(t, failed, "err")); s != "lockstep: panic: boom" {
		t.Errorf("actor failed err = %q, want lockstep: panic: boom", s)
	}
	var pe *lockstep.PanicError
	if e, _ := attrOf(t, failed, "err").Any().(error); !errors.As(e, &pe) || len(pe.Stack) == 0 {
		t.Errorf("actor failed err = %#v, want the *PanicError with its stack", e)
	}
	if d := attrOf(t, failed, "directive"); d.Kind() != slog.KindString || d.String() != "restart" {
		t.Errorf("actor failed directive = %v (%v), want the string restart", d, d.Kind())
	}
	if p := attrOf(t, failed, "panic"); p.Kind() != slog.KindBool || !p.Bool() {
		t.Errorf("actor failed panic = %v (%v), want true", p, p.Kind())
	}

	restarted := got[1]
	checkRecord(t, restarted, slog.LevelInfo, "node", "path", "uid", "err")
	checkAttrs(t, restarted, want...)
	if s := errText(t, attrOf(t, restarted, "err")); s != "lockstep: panic: boom" {
		t.Errorf("actor restarted err = %q, want the cause lockstep: panic: boom", s)
	}

	t.Run("Fail", func(t *testing.T) {
		pid := spawnObs(t, sys, "b", obsOf(&obsActor{receive: func(ctx *lockstep.Context[ping], _ ping) {
			ctx.Fail(errors.New("bad"))
		}}))
		if err := lockstep.Send(sys, pid, ping{}); err != nil {
			t.Fatal(err)
		}
		var rec recRecord
		deadline := time.Now().Add(5 * time.Second)
		for rec.msg == "" {
			for _, r := range h.withMsg("actor failed") {
				if attrOf(t, r, "path").String() == "/user/b" {
					rec = r
				}
			}
			if rec.msg == "" && time.Now().After(deadline) {
				t.Fatal("no actor failed record for /user/b after 5 s")
			}
			time.Sleep(time.Millisecond)
		}
		if s := errText(t, attrOf(t, rec, "err")); s != "bad" {
			t.Errorf("actor failed err = %q, want bad", s)
		}
		if p := attrOf(t, rec, "panic"); p.Bool() {
			t.Error("actor failed panic = true after ctx.Fail, want false")
		}
	})
}

// held spawns an actor named name on sys with the given mailbox and holds it: it runs no turn,
// so its mailbox fills, until the test ends (MBX §10's hold, OBS §10's pauseTurns). The hold
// ends before the system shuts down.
func held(t *testing.T, sys *lockstep.System, name string, capacity int, policy lockstep.MailboxPolicy) lockstep.PID[ping] {
	t.Helper()
	pid := spawnObs(t, sys, name, obsOf(&obsActor{}), lockstep.WithMailbox(capacity, policy))
	release := lockstep.Hold(sys, pid.Address())
	t.Cleanup(release) // runs before newProdSystem's Shutdown
	return pid
}

// debugRecords returns h's Debug records.
func debugRecords(h *recHandler) []recRecord {
	var out []recRecord
	for _, r := range h.records() {
		if r.level == slog.LevelDebug {
			out = append(out, r)
		}
	}
	return out
}

// lateAsk is the message of the late-reply clause of AT-OBS-05: the actor keeps rt, and answers it
// when answer is set.
type lateAsk struct {
	rt     lockstep.ReplyTo[int]
	answer bool
}

// AT-OBS-05: OBS-060, OBS-070.
func TestATOBS05DebugEvents(t *testing.T) {
	l, h := newRecLogger(slog.LevelDebug)
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l))

	// expect checks that the Debug records since the last call are exactly want: the message,
	// the actor or system attributes and the event's own attributes, in order.
	seen := 0
	expect := func(t *testing.T, want ...recRecord) {
		t.Helper()
		var recs []recRecord
		deadline := time.Now().Add(5 * time.Second)
		for {
			recs = debugRecords(h)[seen:]
			if len(recs) >= len(want) || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		seen += len(recs)
		if len(recs) != len(want) {
			t.Fatalf("Debug records %v, want %d records %v", recs, len(want), want)
		}
		for i, w := range want {
			if recs[i].msg != w.msg {
				t.Errorf("Debug record %d is %q, want %q", i, recs[i].msg, w.msg)
				continue
			}
			checkRecord(t, recs[i], slog.LevelDebug, keysOf(w.attrs)...)
			checkAttrs(t, recs[i], w.attrs...)
		}
	}
	rec := func(msg string, attrs ...slog.Attr) recRecord { return recRecord{msg: msg, attrs: attrs} }

	t.Run("fail-send", func(t *testing.T) {
		c := held(t, sys, "c", 1, lockstep.FailSend)
		if err := lockstep.Send(sys, c, ping{}); err != nil {
			t.Fatal(err)
		}
		if err := lockstep.Send(sys, c, ping{}); !errors.Is(err, lockstep.ErrMailboxFull) {
			t.Fatalf("the second Send = %v, want ErrMailboxFull", err)
		}
		expect(t, rec("mailbox full", append(actorAttrs("n1", c), slog.String("policy", "fail-send"))...))
	})

	t.Run("drop-newest", func(t *testing.T) {
		d := held(t, sys, "d", 1, lockstep.DropNewest)
		for range 2 {
			if err := lockstep.Send(sys, d, ping{}); err != nil {
				t.Fatal(err)
			}
		}
		expect(t,
			rec("mailbox full", append(actorAttrs("n1", d), slog.String("policy", "drop-newest"))...),
			rec("message dropped", append(actorAttrs("n1", d), slog.String("reason", "drop-newest"))...),
		)
	})

	t.Run("drop-oldest", func(t *testing.T) {
		o := held(t, sys, "o", 1, lockstep.DropOldest)
		for range 2 {
			if err := lockstep.Send(sys, o, ping{}); err != nil {
				t.Fatal(err)
			}
		}
		expect(t,
			rec("mailbox full", append(actorAttrs("n1", o), slog.String("policy", "drop-oldest"))...),
			rec("message dropped", append(actorAttrs("n1", o), slog.String("reason", "drop-oldest"))...),
		)
	})

	t.Run("dead letter", func(t *testing.T) {
		gone := stoppedPID(t, sys, "gone")
		if err := lockstep.Send(sys, gone, ping{}); err != nil {
			t.Fatal(err)
		}
		expect(t, rec("dead letter",
			slog.String("node", "n1"),
			slog.String("to", gone.Address().String()),
			slog.String("reason", "stopped"),
			slog.String("type", "lockstep_test.ping"),
		))
	})

	t.Run("late reply", func(t *testing.T) {
		got := make(chan struct{}, 1)
		var kept lockstep.ReplyTo[int]
		late := lockstep.PIDAt[lateAsk]("n1", "/user/late")
		_, err := lockstep.Spawn(sys, "late", func() lockstep.Actor[lateAsk] {
			return &lateActor{receive: func(ctx *lockstep.Context[lateAsk], m lateAsk) {
				if m.answer {
					kept.Send(ctx, 1)
					return
				}
				kept = m.rt
				got <- struct{}{}
			}}
		})
		if err != nil {
			t.Fatal(err)
		}
		r := lockstep.Ask(sys, late, func(rt lockstep.ReplyTo[int]) lateAsk { return lateAsk{rt: rt} },
			lockstep.WithTimeout(10*time.Millisecond))
		recv(t, got)
		if _, err := r.Await(context.Background()); !errors.Is(err, lockstep.ErrTimeout) {
			t.Fatalf("Await = %v, want ErrTimeout", err)
		}
		n := len(h.withMsg("message dropped"))
		if err := lockstep.Send(sys, late, lateAsk{answer: true}); err != nil {
			t.Fatal(err)
		}
		dropped := waitRecords(t, h, "message dropped", n+1)[n]
		// The subject of a late reply is the ask address, /temp/<n>, the one address the reply
		// names: the ask has resolved, so no actor waits for it.
		path := attrOf(t, dropped, "path").String()
		if !strings.HasPrefix(path, "/temp/") {
			t.Errorf("late reply path = %q, want the ask address /temp/<n>", path)
		}
		expect(t, rec("message dropped",
			slog.String("node", "n1"),
			slog.String("path", path),
			slog.Uint64("uid", attrOf(t, dropped, "uid").Uint64()),
			slog.String("reason", "late-reply"),
		))
	})

	t.Run("cancelled effect", func(t *testing.T) {
		started := make(chan struct{})
		fx := obsEffect{name: "waits", run: func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}}
		e := spawnObs(t, sys, "e", obsOf(&obsActor{started: func(ctx *lockstep.Context[ping]) {
			lockstep.Do(ctx, fx, func(int, error) ping { return ping{} })
			close(started)
		}}))
		recv(t, started)
		if err := lockstep.Stop(sys, e.Address()); err != nil {
			t.Fatal(err)
		}
		expect(t, rec("message dropped", append(actorAttrs("n1", e), slog.String("reason", "cancelled-effect"))...))
	})
}

// lateActor is an actor of lateAsk messages whose Receive is receive.
type lateActor struct {
	receive func(ctx *lockstep.Context[lateAsk], m lateAsk)
}

func (a *lateActor) Receive(ctx *lockstep.Context[lateAsk], m lateAsk) { a.receive(ctx, m) }

// keysOf returns the keys of attrs in order.
func keysOf(attrs []slog.Attr) []string {
	ks := make([]string, len(attrs))
	for i, a := range attrs {
		ks[i] = a.Key
	}
	return ks
}

// enabledCounter is a handler at Info that counts its Enabled calls and fails the test when it
// is asked to handle a Debug record (AT-OBS-06).
type enabledCounter struct {
	t       *testing.T
	enabled atomic.Int64
}

func (h *enabledCounter) Enabled(_ context.Context, l slog.Level) bool {
	h.enabled.Add(1)
	return l >= slog.LevelInfo
}

func (h *enabledCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level < slog.LevelInfo {
		h.t.Errorf("Handle called for the Debug record %q", r.Message)
	}
	return nil
}

func (h *enabledCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *enabledCounter) WithGroup(string) slog.Handler      { return h }

// AT-OBS-06: OBS-070.
func TestATOBS06DisabledDebugCostsNothing(t *testing.T) {
	h := &enabledCounter{t: t}
	sys := newProdSystem(t, lockstep.WithLogger(slog.New(h)))
	pid := stoppedPID(t, sys, "gone")
	before := h.enabled.Load()
	for range 1000 {
		if err := lockstep.Send(sys, pid, ping{}); err != nil {
			t.Fatal(err)
		}
	}
	if n := h.enabled.Load() - before; n < 1000 {
		t.Errorf("1,000 dead letters made %d Enabled calls, want at least 1,000", n)
	}

	if lockstep.RaceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	send := func() {
		if err := lockstep.Send(sys, pid, ping{}); err != nil {
			t.Fatal(err)
		}
	}
	logged := testing.AllocsPerRun(1000, send)
	lockstep.SetLogOff(sys, true)
	off := testing.AllocsPerRun(1000, send)
	lockstep.SetLogOff(sys, false)
	if logged != off {
		t.Errorf("a dead letter allocates %v times with a logger at Info, %v without the logging call", logged, off)
	}
}

// AT-OBS-14 (all subtests but stuck and goroutines, which the overload tests cover): OBS-060.
func TestATOBS14RemainingEvents(t *testing.T) {
	actorKeys := func(keys ...string) []string { return append([]string{"node", "path", "uid"}, keys...) }
	only := func(t *testing.T, h *recHandler, msg string) recRecord {
		t.Helper()
		recs := waitRecords(t, h, msg, 1)
		if len(recs) != 1 {
			t.Fatalf("%d records %q, want 1: %v", len(recs), msg, recs)
		}
		return recs[0]
	}

	t.Run("intensity", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelDebug)
		sys := newProdSystem(t, lockstep.WithLogger(l))
		// The parent restarts its child at once: with the default backoff, which doubles from
		// 10 ms, ten restarts would take about 10 s.
		s := lockstep.OneForOne(func(error) lockstep.Directive { return lockstep.DirectiveRestart })
		s.MinBackoff, s.MaxBackoff = 0, 0
		child := make(chan lockstep.PID[ping], 1)
		spawnObs(t, sys, "p", obsOf(&obsActor{started: func(ctx *lockstep.Context[ping]) {
			c, err := lockstep.Spawn(ctx, "c", panicky())
			if err != nil {
				panic(err)
			}
			child <- c
		}}), lockstep.WithStrategy(s))
		c := recv(t, child)
		for range 11 {
			if err := lockstep.Send(sys, c, ping{}); err != nil {
				t.Fatal(err)
			}
		}
		waitRecords(t, h, "actor failed", 12) // the child's 11 failures, then the parent's escalation
		rec := only(t, h, "restart intensity exceeded")
		checkRecord(t, rec, slog.LevelError, actorKeys("max_restarts", "window")...)
		checkAttrs(t, rec, actorAttrs(sys.Node(), c)...)
		if v := attrOf(t, rec, "max_restarts"); v.Kind() != slog.KindInt64 || v.Int64() != 10 {
			t.Errorf("max_restarts = %v (%v), want 10", v, v.Kind())
		}
		if v := attrOf(t, rec, "window"); v.Kind() != slog.KindDuration || v.Duration() != time.Minute {
			t.Errorf("window = %v (%v), want 1m0s", v, v.Kind())
		}
		// The child's failures: ten restarts, then the escalation that intensity makes of the
		// eleventh; the parent fails with the child's error, which is no panic of its own.
		var directives []string
		var parentPanic slog.Value
		for _, r := range h.withMsg("actor failed") {
			switch attrOf(t, r, "path").String() {
			case c.Address().Path():
				directives = append(directives, attrOf(t, r, "directive").String())
			case "/user/p":
				parentPanic = attrOf(t, r, "panic")
			}
		}
		want := append(slices.Repeat([]string{"restart"}, 10), "escalate")
		if !slices.Equal(directives, want) {
			t.Errorf("the child's actor failed directives = %v, want %v", directives, want)
		}
		if parentPanic.Kind() != slog.KindBool || parentPanic.Bool() {
			t.Errorf("the parent's actor failed panic = %v, want false", parentPanic)
		}
	})

	t.Run("stopped-panic", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelDebug)
		sys := newProdSystem(t, lockstep.WithLogger(l))
		pid := spawnObs(t, sys, "a", obsOf(&obsActor{
			started: func(ctx *lockstep.Context[ping]) { ctx.Stop() },
			stopped: func(*lockstep.Context[ping], lockstep.StopReason) { panic("s") },
		}))
		rec := only(t, h, "stopped hook panicked")
		checkRecord(t, rec, slog.LevelError, actorKeys("panic", "stack")...)
		checkAttrs(t, rec, actorAttrs(sys.Node(), pid)...)
		if v := attrOf(t, rec, "panic"); v.Kind() != slog.KindString || v.String() != "s" {
			t.Errorf("panic = %v (%v), want the string s", v, v.Kind())
		}
		if v := attrOf(t, rec, "stack"); v.Kind() != slog.KindString || v.String() == "" {
			t.Errorf("stack = %q (%v), want a non-empty string", v, v.Kind())
		}
	})

	t.Run("close-failed", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelDebug)
		sys := newProdSystem(t, lockstep.WithLogger(l))
		pid := spawnObs(t, sys, "a", obsOf(&obsActor{started: func(ctx *lockstep.Context[ping]) {
			ctx.Own(errCloser{err: errors.New("c")})
			ctx.Stop()
		}}))
		rec := only(t, h, "resource close failed")
		checkRecord(t, rec, slog.LevelWarn, actorKeys("err", "index")...)
		checkAttrs(t, rec, actorAttrs(sys.Node(), pid)...)
		if s := errText(t, attrOf(t, rec, "err")); s != "c" {
			t.Errorf("err = %q, want c", s)
		}
		if v := attrOf(t, rec, "index"); v.Kind() != slog.KindInt64 || v.Int64() != 0 {
			t.Errorf("index = %v (%v), want 0", v, v.Kind())
		}
	})

	t.Run("effect-panic", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelDebug)
		sys := newProdSystem(t, lockstep.WithLogger(l))
		fx := obsEffect{name: "fx", run: func(context.Context) (int, error) { panic("fx boom") }}
		pid := spawnObs(t, sys, "a", obsOf(&obsActor{started: func(ctx *lockstep.Context[ping]) {
			lockstep.Do(ctx, fx, func(int, error) ping { return ping{} })
		}}))
		rec := only(t, h, "effect panicked")
		checkRecord(t, rec, slog.LevelError, actorKeys("effect", "panic", "stack")...)
		checkAttrs(t, rec, actorAttrs(sys.Node(), pid)...)
		if v := attrOf(t, rec, "effect"); v.Kind() != slog.KindString || v.String() != "fx" {
			t.Errorf("effect = %v (%v), want the string fx", v, v.Kind())
		}
		if v := attrOf(t, rec, "panic"); v.Kind() != slog.KindString || v.String() != "fx boom" {
			t.Errorf("panic = %v (%v), want the string fx boom", v, v.Kind())
		}
		if v := attrOf(t, rec, "stack"); v.Kind() != slog.KindString || v.String() == "" {
			t.Errorf("stack = %q (%v), want a non-empty string", v, v.Kind())
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelDebug)
		sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l))
		spawnObs(t, sys, "a", obsOf(&obsActor{}))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sys.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		var got []recRecord
		for _, r := range h.records() {
			if strings.HasPrefix(r.msg, "shutdown ") {
				got = append(got, r)
			}
		}
		if len(got) != 2 || got[0].msg != "shutdown started" || got[1].msg != "shutdown finished" {
			t.Fatalf("records %v, want shutdown started then shutdown finished", got)
		}
		checkRecord(t, got[0], slog.LevelInfo, "node", "actors")
		checkRecord(t, got[1], slog.LevelInfo, "node", "elapsed")
		checkAttrs(t, got[0], slog.String("node", "n1"), slog.Int("actors", 1))
		checkAttrs(t, got[1], slog.String("node", "n1"))
		if v := attrOf(t, got[1], "elapsed"); v.Kind() != slog.KindDuration || v.Duration() < 0 {
			t.Errorf("elapsed = %v (%v), want a duration of at least 0", v, v.Kind())
		}
	})

	t.Run("shutdown-incomplete", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelDebug)
		sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l))
		// The effect ignores the cancellation of its context, and the actor's Stopped hook waits
		// for it to return: the shutdown cannot complete before release is closed.
		release, running, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		fx := obsEffect{name: "ignores-cancel", run: func(context.Context) (int, error) {
			close(running)
			<-release
			close(returned)
			return 0, nil
		}}
		spawnObs(t, sys, "a", obsOf(&obsActor{
			started: func(ctx *lockstep.Context[ping]) { lockstep.Do(ctx, fx, func(int, error) ping { return ping{} }) },
			stopped: func(*lockstep.Context[ping], lockstep.StopReason) { <-returned },
		}))
		recv(t, running)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := sys.Shutdown(ctx)
		close(release)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Shutdown with a cancelled context = %v, want context.Canceled", err)
		}
		rec := only(t, h, "shutdown incomplete")
		checkRecord(t, rec, slog.LevelError, "node", "err")
		checkAttrs(t, rec, slog.String("node", "n1"))
		if e, _ := attrOf(t, rec, "err").Any().(error); !errors.Is(e, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", e)
		}
	})
}

// startedHook is a slog handler that calls hook when it handles the "shutdown started" record and
// drops every record.
type startedHook struct{ hook func() }

func (h startedHook) Enabled(context.Context, slog.Level) bool { return true }

func (h startedHook) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "shutdown started" {
		h.hook()
	}
	return nil
}

func (h startedHook) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h startedHook) WithGroup(string) slog.Handler      { return h }

// A handler that panics or calls runtime.Goexit while it handles "shutdown started" does not keep
// the shutdown from completing (OBS-060, ACT-060): the first Shutdown still schedules the sweep,
// so a later Shutdown waits for the actors to stop and returns nil.
func TestShutdownCompletesAfterStartedHandlerExits(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit func()
	}{
		{"panic", func() { panic("handler boom") }},
		{"goexit", runtime.Goexit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sys := newProdSystem(t, lockstep.WithLogger(slog.New(startedHook{hook: tc.exit})))
			spawnObs(t, sys, "a", obsOf(&obsActor{}))
			exited := make(chan struct{})
			go func() {
				defer close(exited)
				defer func() { _ = recover() }()
				_ = sys.Shutdown(context.Background())
				t.Error("Shutdown returned, want the handler to end the call")
			}()
			recv(t, exited)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := sys.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown after the handler ended the first call = %v, want nil", err)
			}
		})
	}
}

// OBS-155: a system built on a given bundle, as simulation builds its node systems, emits no
// runtime log event, neither through slog's default logger nor through a WithLogger logger. The
// bundle is a local engine's, which blocks like production's, so the system has a logger.
func TestNoEventsOnEngineBuiltSystem(t *testing.T) {
	def, hdef := newRecLogger(slog.LevelDebug)
	setDefault(t, def)
	given, hgiven := newRecLogger(slog.LevelDebug)
	for i, opts := range [][]lockstep.Option{nil, {lockstep.WithLogger(given)}} {
		le, err := local.New(local.Config{Node: "n1"})
		if err != nil {
			t.Fatal(err)
		}
		built, err := hooks.NewSystem(le.Engine(), opts)
		if err != nil {
			le.Stop()
			t.Fatal(err)
		}
		sys := systemOf(t, built)
		failed := make(chan struct{}, 3)
		pid := spawnObs(t, sys, "a"+strconv.Itoa(i), obsOf(&obsActor{
			started: func(ctx *lockstep.Context[ping]) {
				lockstep.Do(ctx, obsEffect{name: "fx", run: func(context.Context) (int, error) { panic("fx") }},
					func(int, error) ping { return ping{} })
			},
			receive: func(*lockstep.Context[ping], ping) {
				failed <- struct{}{}
				panic("boom")
			},
		}), lockstep.WithMailbox(1, lockstep.DropNewest))
		for range 3 {
			if err := lockstep.Send(sys, pid, ping{}); err != nil {
				t.Fatal(err)
			}
		}
		recv(t, failed)
		if err := lockstep.Send(sys, lockstep.PIDAt[ping]("n1", "/user/none"), ping{}); err != nil { // a dead letter
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = sys.Shutdown(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	if recs := hdef.records(); len(recs) != 0 {
		t.Errorf("slog's default logger got %d records, want 0: %v", len(recs), recs)
	}
	if recs := hgiven.records(); len(recs) != 0 {
		t.Errorf("the WithLogger logger got %d records, want 0: %v", len(recs), recs)
	}
}
