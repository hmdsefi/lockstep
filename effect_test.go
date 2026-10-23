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

// testFetch is EFF §10's effect: the simulation handler, never Run, produces its result.
type testFetch struct{ Sym string }

func (testFetch) Name() string { return "prices.fetch" }

func (testFetch) Run(context.Context) (int64, error) { panic("Run called") }

// namedEffect is an effect with any name, whose Run returns 5.
type namedEffect struct{ name string }

func (e namedEffect) Name() string { return e.name }

func (namedEffect) Run(context.Context) (int64, error) { return 5, nil }

// fetched is the message that the done function of the effect tests returns.
type fetched struct {
	v   int64
	err error
}

// filler is a message that only occupies a mailbox slot.
type filler struct{}

// effectActor is an any actor that runs the func(*Context[any]) it receives, keeps every other
// message in order and calls stopped, if set, from Stopped.
type effectActor struct {
	got     []any
	stopped func(ctx *Context[any], r StopReason)
}

func (a *effectActor) Receive(ctx *Context[any], m any) {
	if f, ok := m.(func(*Context[any])); ok {
		f(ctx)
		return
	}
	a.got = append(a.got, m)
}

func (a *effectActor) Stopped(ctx *Context[any], r StopReason) {
	if a.stopped != nil {
		a.stopped(ctx, r)
	}
}

// spawnEffectActor spawns an effectActor named "q" and runs the engine until it has started.
func spawnEffectActor(t *testing.T, sys *System, eng *enginetest.Engine, opts ...SpawnOption) (PID[any], *effectActor) {
	t.Helper()
	a := &effectActor{}
	pid, err := Spawn(sys, "q", actorOf[any](a), opts...)
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	return pid, a
}

// doFetch starts a testFetch effect for sym whose done returns a fetched.
func doFetch(ctx *Context[any], sym string, opts ...EffectOption) EffectID {
	return Do(ctx, testFetch{sym}, func(v int64, err error) any { return fetched{v, err} }, opts...)
}

// fetchedAt returns the i-th message that a received, which must be a fetched.
func fetchedAt(t *testing.T, a *effectActor, i int) fetched {
	t.Helper()
	f, ok := a.got[i].(fetched)
	if !ok {
		t.Fatalf("message %d is %T, want fetched", i, a.got[i])
	}
	return f
}

// effectDones returns the lockstep.effect.done records of recs as maps of their attributes.
func effectDones(recs []engine.Record) []map[string]string {
	return effectRecords(recs, "lockstep.effect.done")
}

// effectRecords returns the records of kind in recs as maps from attribute key to value.
func effectRecords(recs []engine.Record, kind string) []map[string]string {
	var out []map[string]string
	for _, r := range recs {
		if r.Kind != kind {
			continue
		}
		m := make(map[string]string, len(r.Attrs))
		for _, a := range r.Attrs {
			m[a.Key] = a.Value
		}
		out = append(out, m)
	}
	return out
}

// failErrs returns the err attributes of the lockstep.fail records of recs.
func failErrs(recs []engine.Record) []string {
	var out []string
	for _, m := range effectRecords(recs, "lockstep.fail") {
		out = append(out, m["err"])
	}
	return out
}

// EFF-010: Do panics with the messages of EFF-010, in order, before it starts anything, and inside
// a handler the panic fails the actor (SUP-140).
func TestEFF010DoPanics(t *testing.T) {
	ok := func(int64, error) any { return nil }
	long := strings.Repeat("a", 65)
	cases := []struct {
		name string
		call func(ctx *Context[any])
		want string
	}{
		{"nil effect", func(ctx *Context[any]) { Do[int64](ctx, nil, ok) }, "lockstep: Do: nil effect"},
		{"nil effect before nil done", func(ctx *Context[any]) { Do[int64](ctx, nil, nil, nil) }, "lockstep: Do: nil effect"},
		{"empty name", func(ctx *Context[any]) { Do(ctx, namedEffect{""}, ok) }, `lockstep: Do: invalid effect name ""`},
		{"space", func(ctx *Context[any]) { Do(ctx, namedEffect{"a b"}, ok) }, `lockstep: Do: invalid effect name "a b"`},
		{"leading dot", func(ctx *Context[any]) { Do(ctx, namedEffect{".a"}, ok) }, `lockstep: Do: invalid effect name ".a"`},
		{"leading dash", func(ctx *Context[any]) { Do(ctx, namedEffect{"-a"}, ok) }, `lockstep: Do: invalid effect name "-a"`},
		{"65 characters", func(ctx *Context[any]) { Do(ctx, namedEffect{long}, ok) }, `lockstep: Do: invalid effect name "` + long + `"`},
		{"non-ASCII", func(ctx *Context[any]) { Do(ctx, namedEffect{"é"}, ok) }, `lockstep: Do: invalid effect name "é"`},
		{"name before nil done", func(ctx *Context[any]) { Do[int64](ctx, namedEffect{"a b"}, nil, nil) }, `lockstep: Do: invalid effect name "a b"`},
		{"nil done", func(ctx *Context[any]) { Do[int64, any](ctx, namedEffect{"a"}, nil) }, "lockstep: Do: nil done"},
		{"nil done before nil option", func(ctx *Context[any]) { Do[int64, any](ctx, namedEffect{"a"}, nil, nil) }, "lockstep: Do: nil done"},
		{"nil option", func(ctx *Context[any]) { Do(ctx, namedEffect{"a"}, ok, nil) }, "lockstep: Do: nil option"},
		{"nil option before bad timeout", func(ctx *Context[any]) {
			Do(ctx, namedEffect{"a"}, ok, WithEffectTimeout(0), nil)
		}, "lockstep: Do: nil option"},
		{"zero timeout", func(ctx *Context[any]) {
			Do(ctx, namedEffect{"a"}, ok, WithEffectTimeout(0))
		}, "lockstep: Do: effect timeout must be positive, got 0s"},
		{"negative timeout", func(ctx *Context[any]) {
			Do(ctx, namedEffect{"a"}, ok, WithEffectTimeout(-time.Second))
		}, "lockstep: Do: effect timeout must be positive, got -1s"},
		{"bad timeout overridden by a later one", func(ctx *Context[any]) {
			Do(ctx, namedEffect{"a"}, ok, WithEffectTimeout(0), WithEffectTimeout(time.Second))
		}, "lockstep: Do: effect timeout must be positive, got 0s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sys, eng := newManualSystem(t)
			pid, _ := spawnEffectActor(t, sys, eng)
			before := len(eng.Records())
			inTurn(t, sys, eng, pid, tc.call)
			errs := failErrs(eng.Records()[before:])
			if want := "lockstep: panic: " + tc.want; !slices.Equal(errs, []string{want}) {
				t.Errorf("lockstep.fail errs = %q, want [%q]", errs, want)
			}
			if n := len(eng.Pending()); n != 0 {
				t.Errorf("%d effects pending after the panic, want none", n)
			}
			if n := len(effectRecords(eng.Records(), "lockstep.effect.start")); n != 0 {
				t.Errorf("%d lockstep.effect.start records after the panic, want none", n)
			}
		})
	}

	t.Run("valid names", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, _ := spawnEffectActor(t, sys, eng)
		names := []string{"a", "9", "A.b_c-d", strings.Repeat("a", 64), "lockstep.journal.append"}
		var ids []EffectID
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			for _, n := range names {
				ids = append(ids, Do(ctx, namedEffect{n}, func(int64, error) any { return nil }))
			}
		})
		if want := []EffectID{1, 2, 3, 4, 5}; !slices.Equal(ids, want) {
			t.Errorf("IDs = %v, want %v", ids, want)
		}
		if errs := failErrs(eng.Records()); len(errs) != 0 {
			t.Errorf("a valid name failed the actor: %q", errs)
		}
	})

	t.Run("outside a turn", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, _ := spawnEffectActor(t, sys, eng)
		var ctx *Context[any]
		inTurn(t, sys, eng, pid, func(c *Context[any]) { ctx = c })
		defer func() {
			if r := recover(); r == nil {
				t.Error("Do outside a turn did not panic")
			}
		}()
		doFetch(ctx, "X")
	})
}

// EFF-020: a valid Do assigns ID 1 and the next IDs, records the start, arms the timeout timer and
// starts the effect through the engine with the request's fields.
func TestEFF020StartsEffect(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, _ := spawnEffectActor(t, sys, eng)
	labelsBefore := len(eng.Labels())
	var ids [2]EffectID
	e := testFetch{"X"}
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		ids[0] = Do(ctx, e, func(v int64, err error) any { return fetched{v, err} })
		ids[1] = doFetch(ctx, "Y", WithEffectTimeout(time.Second))
	})
	if ids != [2]EffectID{1, 2} {
		t.Errorf("IDs = %v, want [1 2]", ids)
	}
	pending := eng.Pending()
	if len(pending) != 2 {
		t.Fatalf("Pending() holds %d requests, want 2", len(pending))
	}
	req := pending[0]
	if req.ID != 1 || req.Name != "prices.fetch" || req.Actor != pid.Address().engine() || req.Value != any(e) {
		t.Errorf("request = {ID %d, Name %q, Actor %v, Value %v}, want {1, prices.fetch, %v, %v}",
			req.ID, req.Name, req.Actor, req.Value, pid.Address().engine(), e)
	}
	if labels := eng.Labels()[labelsBefore:]; !slices.Equal(labels, []string{"lockstep/effect-timeout", "lockstep/effect-timeout"}) {
		t.Errorf("labels = %q, want one lockstep/effect-timeout per effect", labels)
	}
	starts := effectRecords(eng.Records(), "lockstep.effect.start")
	if len(starts) != 2 {
		t.Fatalf("%d lockstep.effect.start records, want 2", len(starts))
	}
	want := map[string]string{"actor": pid.Address().String(), "inc": "1", "effect": "1", "name": "prices.fetch", "timeout": "30s"}
	for k, v := range want {
		if starts[0][k] != v {
			t.Errorf("start[0][%q] = %q, want %q", k, starts[0][k], v)
		}
	}
	if starts[1]["effect"] != "2" || starts[1]["timeout"] != "1s" {
		t.Errorf("start[1] = %v, want effect 2 and timeout 1s", starts[1])
	}
	// The attribute order of EFF §8.
	for _, r := range eng.Records() {
		if r.Kind != "lockstep.effect.start" {
			continue
		}
		var keys []string
		for _, a := range r.Attrs {
			keys = append(keys, a.Key)
		}
		if !slices.Equal(keys, []string{"actor", "inc", "effect", "name", "timeout"}) {
			t.Errorf("start attributes = %q", keys)
		}
	}

	t.Run("Run calls the effect's Run", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, _ := spawnEffectActor(t, sys, eng)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
			Do(ctx, namedEffect{"n"}, func(int64, error) any { return nil })
		})
		v, err := eng.Pending()[0].Run(context.Background())
		if v != any(int64(5)) || err != nil {
			t.Errorf("Run() = %v, %v, want 5, nil", v, err)
		}
	})
}

// EFF-030: Do while the incarnation is stopping or restarting returns 0, starts nothing and
// records effect=0 outcome=refused; the counter is untouched.
func TestEFF030RefusedWhileStopping(t *testing.T) {
	check := func(t *testing.T, sys *System, eng *enginetest.Engine, id *EffectID, dones []map[string]string) {
		t.Helper()
		if *id != 0 {
			t.Errorf("Do while ending returned %d, want 0", *id)
		}
		if n := len(eng.Pending()); n != 0 {
			t.Errorf("%d effects pending, want none", n)
		}
		if n := len(effectRecords(eng.Records(), "lockstep.effect.start")); n != 0 {
			t.Errorf("%d start records, want none", n)
		}
		if len(dones) != 1 {
			t.Fatalf("%d lockstep.effect.done records, want 1", len(dones))
		}
		d := dones[0]
		if d["effect"] != "0" || d["outcome"] != "refused" || d["name"] != "prices.fetch" || d["err"] != "" {
			t.Errorf("done record = %v, want effect=0 name=prices.fetch outcome=refused err=\"\"", d)
		}
		if keys := recordKeys(eng.Records(), "lockstep.effect.done"); !slices.Equal(keys, []string{"actor", "inc", "effect", "name", "outcome", "err"}) {
			t.Errorf("done attributes = %q", keys)
		}
		// The refused call took no ID: the next Do of the system gets 1.
		other, _ := Spawn(sys, "other", actorOf[any](&effectActor{}))
		eng.RunUntilIdle()
		var next EffectID
		inTurn(t, sys, eng, other, func(ctx *Context[any]) { next = doFetch(ctx, "X") })
		if next != 1 {
			t.Errorf("next Do returned %d, want 1", next)
		}
	}

	t.Run("Stopped after Stop", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var id EffectID = 99
		a := &effectActor{}
		a.stopped = func(ctx *Context[any], _ StopReason) { id = doFetch(ctx, "X") }
		pid, err := Spawn(sys, "q", actorOf[any](a))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		if err := Stop(sys, pid.Address()); err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		check(t, sys, eng, &id, effectDones(eng.Records()))
	})

	t.Run("Stopped after a failure", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var id EffectID = 99
		a := &effectActor{}
		a.stopped = func(ctx *Context[any], r StopReason) {
			if r.Kind == StopRestarting {
				id = doFetch(ctx, "X")
			}
		}
		pid, err := Spawn(sys, "q", actorOf[any](a))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.Fail(errors.New("boom")) })
		eng.RunUntilIdle()
		eng.Advance(time.Hour)
		eng.RunUntilIdle()
		check(t, sys, eng, &id, effectDones(eng.Records()))
	})
}

// recordKeys returns the attribute keys of the first record of kind in recs.
func recordKeys(recs []engine.Record, kind string) []string {
	for _, r := range recs {
		if r.Kind == kind {
			var keys []string
			for _, a := range r.Attrs {
				keys = append(keys, a.Key)
			}
			return keys
		}
	}
	return nil
}

// EFF-040: the timeout is DefaultEffectTimeout unless WithEffectTimeout is given; of several
// options the last one wins.
func TestEFF040LastTimeoutWins(t *testing.T) {
	if DefaultEffectTimeout != 30*time.Second {
		t.Fatalf("DefaultEffectTimeout = %v, want 30s", DefaultEffectTimeout)
	}
	sys, eng := newManualSystem(t)
	a := &effectActor{}
	pid, err := Spawn(sys, "q", actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		doFetch(ctx, "X", WithEffectTimeout(time.Hour), WithEffectTimeout(2*time.Second))
	})
	eng.Advance(time.Second)
	eng.RunUntilIdle()
	if len(a.got) != 0 {
		t.Fatalf("a result arrived after 1s: %v", a.got)
	}
	eng.Advance(time.Second)
	eng.RunUntilIdle()
	if len(a.got) != 1 || !errors.Is(fetchedAt(t, a, 0).err, ErrTimeout) {
		t.Errorf("after 2s got %v, want one ErrTimeout", a.got)
	}
	if starts := effectRecords(eng.Records(), "lockstep.effect.start"); starts[0]["timeout"] != "2s" {
		t.Errorf("start timeout = %q, want 2s", starts[0]["timeout"])
	}
}

// EFF-060, EFF-050: the engine's result becomes one result entry whose done runs in a later turn;
// the outcome follows the error, and a late result or a timeout adds nothing.
func TestEFF060DeliversResult(t *testing.T) {
	type tc struct {
		name    string
		v       any
		err     error
		want    fetched
		outcome string
		errAttr string
	}
	errDown := errors.New("down")
	cases := []tc{
		{"ok", int64(7), nil, fetched{7, nil}, "ok", ""},
		{"nil value is the zero value", nil, nil, fetched{0, nil}, "ok", ""},
		{"error", nil, errDown, fetched{0, errDown}, "error", "down"},
		{"value with error", int64(3), errDown, fetched{3, errDown}, "error", "down"},
		{"overloaded", nil, engine.ErrOverloaded, fetched{0, ErrOverloaded}, "overloaded", "lockstep: overloaded"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sys, eng := newManualSystem(t)
			pid, a := spawnEffectActor(t, sys, eng)
			inTurn(t, sys, eng, pid, func(ctx *Context[any]) { doFetch(ctx, "X") })
			before := len(eng.Records())
			if !eng.CompleteEffect(1, c.v, c.err) {
				t.Fatal("CompleteEffect(1) = false, want true")
			}
			if len(a.got) != 0 {
				t.Fatalf("done ran inside the engine's callback: %v", a.got)
			}
			eng.RunUntilIdle()
			if len(a.got) != 1 || a.got[0] != any(c.want) {
				t.Fatalf("got %v, want [%v]", a.got, c.want)
			}
			dones := effectDones(eng.Records()[before:])
			if len(dones) != 1 {
				t.Fatalf("%d done records, want 1", len(dones))
			}
			d := dones[0]
			if d["outcome"] != c.outcome || d["err"] != c.errAttr || d["effect"] != "1" || d["name"] != "prices.fetch" || d["inc"] != "1" {
				t.Errorf("done record = %v, want outcome=%s err=%q effect=1", d, c.outcome, c.errAttr)
			}
			if errors.Is(c.err, engine.ErrOverloaded) {
				if err := fetchedAt(t, a, 0).err; !errors.Is(err, ErrOverloaded) {
					t.Errorf("err = %v, want ErrOverloaded", err)
				}
			}
			// The timeout timer was cancelled, and a second completion is ignored.
			eng.Advance(DefaultEffectTimeout)
			eng.RunUntilIdle()
			if len(a.got) != 1 || len(effectDones(eng.Records())) != 1 {
				t.Errorf("after the timeout time: %d results, %d done records, want 1 and 1", len(a.got), len(effectDones(eng.Records())))
			}
			if eng.CompleteEffect(1, int64(1), nil) {
				t.Error("CompleteEffect(1) after the outcome = true, want false")
			}
		})
	}

	t.Run("the table is empty once the outcome is delivered", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, a := spawnEffectActor(t, sys, eng)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { doFetch(ctx, "X") })
		eng.Advance(DefaultEffectTimeout)
		eng.RunUntilIdle()
		cl := cellOf(t, sys, pid)
		cl.emu.Lock()
		n := len(cl.effects)
		cl.emu.Unlock()
		if n != 0 {
			t.Errorf("%d effects pending in the cell after the timeout, want 0", n)
		}
		if len(a.got) != 1 || len(effectDones(eng.Records())) != 1 {
			t.Errorf("got %v and %d done records, want one of each", a.got, len(effectDones(eng.Records())))
		}
	})
}

// EFF-070: a value of another type than T is misuse: done is not called, Failer.Fail gets the
// error and the record says outcome=error.
func TestEFF070WrongValueType(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, a := spawnEffectActor(t, sys, eng)
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { doFetch(ctx, "X") })
	before := len(eng.Records())
	eng.CompleteEffect(1, "100", nil)
	eng.RunUntilIdle()
	want := `lockstep: effect "prices.fetch": engine returned string, want int64`
	f := eng.Failures()
	if len(f) != 1 || f[0].Error() != want {
		t.Errorf("Failures() = %v, want one error %q", f, want)
	}
	if len(a.got) != 0 {
		t.Errorf("done ran: %v", a.got)
	}
	dones := effectDones(eng.Records()[before:])
	if len(dones) != 1 || dones[0]["outcome"] != "error" || dones[0]["err"] != want {
		t.Errorf("done records = %v, want one outcome=error err=%q", dones, want)
	}
	eng.Advance(DefaultEffectTimeout)
	eng.RunUntilIdle()
	if len(a.got) != 0 || len(effectDones(eng.Records())) != 1 {
		t.Errorf("the timeout ran after the misuse: got %v, %d done records", a.got, len(effectDones(eng.Records())))
	}
}

// EFF-080, MBX-035: a result reaches an actor whose mailbox is full, with no mailbox.full record,
// and results arrive in the order their outcomes were delivered.
func TestEFF080ResultEntries(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, a := spawnEffectActor(t, sys, eng, WithMailbox(1, FailSend))
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		doFetch(ctx, "A")
		doFetch(ctx, "B")
	})
	release := hold(t, sys, pid.Address())
	if err := Send(sys, pid, any(filler{})); err != nil {
		t.Fatalf("Send(filler): %v", err)
	}
	if err := Send(sys, pid, any(filler{})); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("second Send = %v, want ErrMailboxFull (the mailbox is full)", err)
	}
	fullBefore := len(effectRecords(eng.Records(), "lockstep.mailbox.full"))
	eng.CompleteEffect(2, int64(20), nil)
	eng.CompleteEffect(1, int64(10), nil)
	if n := len(effectRecords(eng.Records(), "lockstep.mailbox.full")); n != fullBefore {
		t.Errorf("results added %d mailbox.full records, want none", n-fullBefore)
	}
	release()
	eng.RunUntilIdle()
	want := []any{filler{}, fetched{20, nil}, fetched{10, nil}}
	if !slices.Equal(a.got, want) {
		t.Errorf("Receive got %v, want %v", a.got, want)
	}
}

// EFF-080: done is a guarded handler: a panic in it fails the actor with source receive.
func TestEFF080DonePanics(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, _ := spawnEffectActor(t, sys, eng)
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) {
		Do(ctx, testFetch{"X"}, func(int64, error) any { panic("done boom") })
	})
	before := len(eng.Records())
	eng.CompleteEffect(1, int64(1), nil)
	eng.RunUntilIdle()
	fails := effectRecords(eng.Records()[before:], "lockstep.fail")
	if len(fails) != 1 || fails[0]["source"] != "receive" || fails[0]["err"] != "lockstep: panic: done boom" {
		t.Errorf("lockstep.fail records = %v, want one source=receive for the panic", fails)
	}
}

// effectsSpy wraps the engine's Effects: it logs the engine Cancel calls together with the number
// of lockstep.effect.done records at the time of the call.
type effectsSpy struct {
	engine.Effects
	recs   func() []engine.Record
	cancel []int
}

func (s *effectsSpy) Start(req engine.EffectRequest, done func(v any, err error)) engine.Cancel {
	c := s.Effects.Start(req, done)
	return func() bool {
		s.cancel = append(s.cancel, len(effectDones(s.recs())))
		return c()
	}
}

// EFF-090: when the timer fires the runtime calls the engine's Cancel, then delivers ErrTimeout
// with outcome=timeout; a later result is ignored.
func TestEFF090Timeout(t *testing.T) {
	eng := enginetest.New("local", 1)
	b := eng.Engine()
	spy := &effectsSpy{Effects: b.Effects, recs: eng.Records}
	b.Effects = spy
	sys, err := newSystem(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := &effectActor{}
	pid, err := Spawn(sys, "q", actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	inTurn(t, sys, eng, pid, func(ctx *Context[any]) { doFetch(ctx, "X") })
	eng.Advance(DefaultEffectTimeout - time.Nanosecond)
	eng.RunUntilIdle()
	if len(eng.Pending()) != 1 || len(a.got) != 0 {
		t.Fatalf("the effect resolved before its timeout: pending %d, got %v", len(eng.Pending()), a.got)
	}
	eng.Advance(time.Nanosecond)
	if n := len(eng.Pending()); n != 0 {
		t.Errorf("%d requests pending after the timeout, want 0", n)
	}
	if !slices.Equal(spy.cancel, []int{0}) {
		t.Errorf("engine Cancel calls (with the done records so far) = %v, want [0]: Cancel before the record", spy.cancel)
	}
	eng.RunUntilIdle()
	if len(a.got) != 1 {
		t.Fatalf("got %v, want one result", a.got)
	}
	f := fetchedAt(t, a, 0)
	if f.v != 0 || !errors.Is(f.err, ErrTimeout) {
		t.Errorf("result = %v, want zero value and ErrTimeout", f)
	}
	dones := effectDones(eng.Records())
	if len(dones) != 1 || dones[0]["outcome"] != "timeout" || dones[0]["err"] != ErrTimeout.Error() {
		t.Errorf("done records = %v, want one outcome=timeout", dones)
	}
	if eng.CompleteEffect(1, int64(1), nil) {
		t.Error("CompleteEffect(1) after the timeout = true, want false")
	}
	eng.RunUntilIdle()
	if len(a.got) != 1 {
		t.Errorf("a result arrived after the timeout: %v", a.got)
	}
}

// slowEffect is an effect whose Run waits for its context and returns.
type slowEffect struct{}

func (slowEffect) Name() string { return "slow" }

func (slowEffect) Run(ctx context.Context) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// EFF-050, EFF-090: on the local engine, with timeouts that race the engine's result and the
// Start call itself, every effect ends in exactly one outcome and one record.
func TestEFF050ExactlyOneOutcome(t *testing.T) {
	rec := enginetest.NewRecorder()
	le, err := local.New(local.Config{Node: "local", Tracer: rec})
	if err != nil {
		t.Fatal(err)
	}
	sys, err := newSystem(le.Engine(), nil)
	if err != nil {
		le.Stop()
		t.Fatal(err)
	}
	shutdownAtEnd(t, sys)

	const n = 200
	var mu sync.Mutex
	var results []int64
	all := make(chan struct{})
	a := &effectActor{}
	pid, err := Spawn(sys, "q", actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	inProbeStart := func(i int) func(ctx *Context[any]) {
		return func(ctx *Context[any]) {
			timeout := time.Duration(1+i%3) * time.Nanosecond
			Do(ctx, slowEffect{}, func(v int64, err error) any {
				mu.Lock()
				defer mu.Unlock()
				results = append(results, int64(i))
				if len(results) == n {
					close(all)
				}
				return nil
			}, WithEffectTimeout(timeout))
		}
	}
	for i := range n {
		if err := Send(sys, pid, any(inProbeStart(i))); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-all:
	case <-time.After(10 * time.Second):
		mu.Lock()
		got := len(results)
		mu.Unlock()
		t.Fatalf("only %d of %d effects delivered an outcome", got, n)
	}
	// A moment for a stray second outcome to show up.
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	got := slices.Clone(results)
	mu.Unlock()
	slices.Sort(got)
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("results = %v, want each of 0..%d once", got, n-1)
		}
	}
	dones := effectDones(rec.Records())
	if len(dones) != n {
		t.Errorf("%d done records, want %d", len(dones), n)
	}
	seen := map[string]bool{}
	for _, d := range dones {
		if seen[d["effect"]] {
			t.Errorf("effect %s has two done records", d["effect"])
		}
		seen[d["effect"]] = true
		if d["outcome"] != "timeout" {
			t.Errorf("effect %s outcome = %s, want timeout", d["effect"], d["outcome"])
		}
	}
	cl := cellOf(t, sys, pid)
	cl.emu.Lock()
	left := len(cl.effects)
	cl.emu.Unlock()
	if left != 0 {
		t.Errorf("%d effects still pending in the cell", left)
	}
}

// EFF-080, MBX-170: a result of an incarnation that has ended is never delivered: it is dropped
// as a Self entry of an ended incarnation, whether it was in the mailbox when the incarnation
// ended or arrived after.
func TestEFF080ResultOfEndedIncarnation(t *testing.T) {
	restart := func(t *testing.T, sys *System, eng *enginetest.Engine, pid PID[any]) {
		t.Helper()
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { ctx.Fail(errors.New("boom")) })
		eng.Advance(time.Second)
		eng.RunUntilIdle()
		if _, inc := cellOf(t, sys, pid).lifecycle(); inc != 2 {
			t.Fatalf("incarnation = %d, want 2", inc)
		}
	}
	drops := func(eng *enginetest.Engine) []map[string]string {
		var out []map[string]string
		for _, d := range effectRecords(eng.Records(), "lockstep.drop") {
			if d["reason"] == "incarnation-ended" {
				out = append(out, d)
			}
		}
		return out
	}

	t.Run("arrives after the restart", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, a := spawnEffectActor(t, sys, eng)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { doFetch(ctx, "X") })
		restart(t, sys, eng, pid)
		eng.CompleteEffect(1, int64(7), nil)
		eng.RunUntilIdle()
		if len(a.got) != 0 {
			t.Errorf("the new incarnation got %v", a.got)
		}
		d := drops(eng)
		if len(d) != 1 || d[0]["msg"] != "7" || d[0]["inc"] != "1" || d[0]["to"] != pid.Address().String() {
			t.Errorf("drop records = %v, want one for the value 7 of incarnation 1", d)
		}
	})

	t.Run("in the mailbox when the incarnation ends", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		pid, a := spawnEffectActor(t, sys, eng)
		inTurn(t, sys, eng, pid, func(ctx *Context[any]) { doFetch(ctx, "X") })
		release := hold(t, sys, pid.Address())
		send(t, sys, pid, any(func(ctx *Context[any]) { ctx.Fail(errors.New("boom")) }))
		eng.CompleteEffect(1, nil, errors.New("down"))
		release()
		eng.RunUntilIdle()
		eng.Advance(time.Second)
		eng.RunUntilIdle()
		if len(a.got) != 0 {
			t.Errorf("the new incarnation got %v", a.got)
		}
		d := drops(eng)
		if len(d) != 1 || d[0]["msg"] != "down" || d[0]["inc"] != "1" {
			t.Errorf("drop records = %v, want one for the error of incarnation 1", d)
		}
		if n := len(effectDones(eng.Records())); n != 1 {
			t.Errorf("%d done records, want 1", n)
		}
	})
}
