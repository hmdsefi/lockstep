// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package enginetest_test

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// fake is a fake actor (spec §10): an engine.Slot field, an Address, and a RunTurn that records its
// budget and returns a scripted more.
type fake struct {
	slot    engine.Slot
	addr    engine.Address
	name    string
	order   *[]string // shared run log; each turn appends name
	script  []bool    // more for turn i; false once the script runs out
	always  bool      // return true on every turn (overrides script)
	budgets []int
	turns   int
	onTurn  func()
	inTurn  func() bool
	seenIn  []bool
}

func newFake(name string, order *[]string, script ...bool) *fake {
	return &fake{name: name, order: order, script: script, addr: engine.Address{Node: "n", Path: "/" + name, UID: 1}}
}

func (f *fake) Address() engine.Address { return f.addr }
func (f *fake) Slot() *engine.Slot      { return &f.slot }
func (f *fake) RunTurn(budget int) bool {
	f.budgets = append(f.budgets, budget)
	if f.inTurn != nil {
		f.seenIn = append(f.seenIn, f.inTurn())
	}
	if f.order != nil {
		*f.order = append(*f.order, f.name)
	}
	if f.onTurn != nil {
		f.onTurn()
	}
	i := f.turns
	f.turns++
	if f.always {
		return true
	}
	return i < len(f.script) && f.script[i]
}

// panicText runs f and returns the text of its panic, or "" if it did not panic.
func panicText(f func()) (text string) {
	defer func() {
		if p := recover(); p != nil {
			text = fmt.Sprint(p)
		}
	}()
	f()
	return ""
}

func wantPanic(t *testing.T, want string, f func()) {
	t.Helper()
	if got := panicText(f); got != want {
		t.Errorf("panic = %q, want %q", got, want)
	}
}

func wantOrder(t *testing.T, got *[]string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(*got, want) {
		t.Errorf("order = %v, want %v", *got, want)
	}
}

// fnv1a64 is FNV-1a 64 over s (ENG-540).
func fnv1a64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// AT-ENG-40, ENG-050, ENG-510
func TestATENG40ManualTurns(t *testing.T) {
	t.Run("a", func(t *testing.T) {
		m := enginetest.New("n", 1)
		b := m.Engine()
		var order []string
		a, bb := newFake("A", &order), newFake("B", &order)
		a.inTurn, bb.inTurn = b.Scheduler.InTurn, b.Scheduler.InTurn

		if b.Scheduler.InTurn() {
			t.Error("InTurn() = true before any turn")
		}
		if m.Step() {
			t.Error("Step() on an empty queue = true")
		}
		b.Scheduler.Ready(a)
		b.Scheduler.Ready(bb)
		b.Scheduler.Ready(a) // deduplicated by the Slot
		first, second := m.Step(), m.Step()
		if !first || !second {
			t.Fatalf("Step() = %v, %v, want two turns", first, second)
		}
		if m.Step() {
			t.Error("third Step() = true, want false: the second Ready(A) was deduplicated")
		}
		wantOrder(t, &order, "A", "B")
		if !reflect.DeepEqual(a.budgets, []int{1}) || !reflect.DeepEqual(bb.budgets, []int{1}) {
			t.Errorf("budgets = %v, %v, want [1], [1]", a.budgets, bb.budgets)
		}
		if !reflect.DeepEqual(a.seenIn, []bool{true}) {
			t.Errorf("InTurn inside RunTurn = %v, want [true]", a.seenIn)
		}
		if b.Scheduler.InTurn() {
			t.Error("InTurn() = true after the turns")
		}
		if a.slot.State() != engine.SlotIdle {
			t.Errorf("A's slot = %v, want idle", a.slot.State())
		}

		m.SetBudget(4)
		b.Scheduler.Ready(a)
		m.Step()
		if !reflect.DeepEqual(a.budgets, []int{1, 4}) {
			t.Errorf("budgets after SetBudget(4) = %v, want [1 4]", a.budgets)
		}
		wantPanic(t, "enginetest: SetBudget(0) with n < 1", func() { m.SetBudget(0) })
	})

	t.Run("b", func(t *testing.T) {
		m := enginetest.New("n", 1)
		b := m.Engine()
		var order []string
		a, bb := newFake("A", &order, true, true), newFake("B", &order)
		b.Scheduler.Ready(a)
		b.Scheduler.Ready(bb)
		if got := m.RunUntilIdle(); got != 4 {
			t.Errorf("RunUntilIdle() = %d, want 4", got)
		}
		wantOrder(t, &order, "A", "B", "A", "A")
		if got := m.RunUntilIdle(); got != 0 {
			t.Errorf("RunUntilIdle() on an idle engine = %d, want 0", got)
		}
	})
}

// AT-ENG-41, ENG-520
func TestATENG41ManualTimers(t *testing.T) {
	m := enginetest.New("n", 1)
	b := m.Engine()
	var log []string
	var order []string
	x := newFake("X", &order)
	at := func(name string) {
		log = append(log, fmt.Sprintf("%s@%v", name, m.Now().Sub(enginetest.Epoch)))
		if !b.Scheduler.InTurn() {
			t.Errorf("%s: InTurn() = false inside a timer function", name)
		}
	}
	f1 := func() { at("f1") }
	f3 := func() { at("f3") }
	f4 := func() { at("f4") }
	f5 := func() { at("f5") }
	f2 := func() {
		at("f2")
		b.Clock.AfterFunc(time.Millisecond, "e", f5)
		b.Scheduler.Ready(x)
	}

	if !m.Now().Equal(enginetest.Epoch) {
		t.Fatalf("Now() = %v, want Epoch %v", m.Now(), enginetest.Epoch)
	}
	if !b.Clock.Now().Equal(enginetest.Epoch) {
		t.Fatalf("Clock.Now() = %v, want Epoch", b.Clock.Now())
	}
	b.Clock.AfterFunc(20*time.Millisecond, "a", f1)
	b.Clock.AfterFunc(10*time.Millisecond, "b", f2)
	b.Clock.AfterFunc(10*time.Millisecond, "c", f3)
	b.Clock.AfterFunc(0, "d", f4)
	if len(log) != 0 {
		t.Fatalf("functions ran during AfterFunc: %v", log)
	}
	if got, want := m.Labels(), []string{"a", "b", "c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Labels() = %v, want %v", got, want)
	}

	m.Advance(0)
	if want := []string{"f4@0s"}; !reflect.DeepEqual(log, want) {
		t.Errorf("after Advance(0): log = %v, want %v", log, want)
	}

	// f2 arms f5 for +11ms and readies X, whose turn runs before f3.
	var seq []string
	x.onTurn = func() { seq = append(seq, fmt.Sprintf("X@%v|%d", m.Now().Sub(enginetest.Epoch), len(log))) }
	m.Advance(15 * time.Millisecond)
	if want := []string{"f4@0s", "f2@10ms", "f3@10ms", "f5@11ms"}; !reflect.DeepEqual(log, want) {
		t.Errorf("after Advance(15ms): log = %v, want %v", log, want)
	}
	if want := []string{"X@10ms|2"}; !reflect.DeepEqual(seq, want) {
		t.Errorf("X's turn = %v, want %v (after f2, before f3)", seq, want)
	}
	if got, want := m.Now(), enginetest.Epoch.Add(15*time.Millisecond); !got.Equal(want) {
		t.Errorf("Now() = %v, want %v", got, want)
	}
	if got, want := m.Labels(), []string{"a", "b", "c", "d", "e"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Labels() = %v, want %v", got, want)
	}

	m.Advance(5 * time.Millisecond)
	if got := log[len(log)-1]; got != "f1@20ms" {
		t.Errorf("last = %q, want f1@20ms", got)
	}
	if len(log) != 5 {
		t.Errorf("log = %v, want 5 entries", log)
	}

	wantPanic(t, "enginetest: Advance(-1ns) with d < 0", func() { m.Advance(-1) })
}

// ENG-070, ENG-520: a negative d fires as soon as possible; Cancel is exactly-once.
func TestManualTimerCancelAndNegative(t *testing.T) {
	m := enginetest.New("n", 1)
	b := m.Engine()
	ran := 0
	cancel := b.Clock.AfterFunc(time.Second, "x", func() { ran++ })
	if !cancel() {
		t.Error("first Cancel() = false, want true")
	}
	if cancel() {
		t.Error("second Cancel() = true, want false")
	}
	m.Advance(time.Hour)
	if ran != 0 {
		t.Errorf("cancelled timer ran %d times", ran)
	}

	fired := b.Clock.AfterFunc(-time.Hour, "neg", func() { ran++ })
	m.Advance(0)
	if ran != 1 {
		t.Errorf("negative-d timer ran %d times after Advance(0), want 1", ran)
	}
	if fired() {
		t.Error("Cancel() of a fired timer = true, want false")
	}
}

// AT-ENG-42, ENG-530, ENG-090
func TestATENG42ManualEffects(t *testing.T) {
	m := enginetest.New("n", 1)
	b := m.Engine()
	type result struct {
		v   any
		err error
	}
	var got1, got2 []result
	var in2 []bool
	run := func(context.Context) (any, error) {
		t.Error("Run was called")
		return nil, nil
	}
	r1 := engine.EffectRequest{ID: 1, Name: "one", Run: run}
	r2 := engine.EffectRequest{ID: 2, Name: "two", Run: run}
	c1 := b.Effects.Start(r1, func(v any, err error) { got1 = append(got1, result{v, err}) })
	b.Effects.Start(r2, func(v any, err error) {
		in2 = append(in2, b.Scheduler.InTurn())
		got2 = append(got2, result{v, err})
	})

	pend := m.Pending()
	if len(pend) != 2 || pend[0].ID != 1 || pend[1].ID != 2 || pend[0].Name != "one" {
		t.Fatalf("Pending() = %+v, want IDs [1 2] in start order", pend)
	}
	if len(got1)+len(got2) != 0 {
		t.Fatal("a done function ran during Start")
	}

	if !m.CompleteEffect(2, "x", nil) {
		t.Error("CompleteEffect(2) = false, want true")
	}
	if !reflect.DeepEqual(got2, []result{{"x", nil}}) {
		t.Errorf("done(2) calls = %v, want one (x, nil)", got2)
	}
	if !reflect.DeepEqual(in2, []bool{true}) {
		t.Errorf("InTurn inside done = %v, want [true]", in2)
	}
	if b.Scheduler.InTurn() {
		t.Error("InTurn() = true after CompleteEffect")
	}
	if m.CompleteEffect(2, "y", nil) {
		t.Error("second CompleteEffect(2) = true, want false")
	}
	if len(got2) != 1 {
		t.Errorf("done(2) ran %d times, want 1", len(got2))
	}

	if !c1() {
		t.Error("Cancel() of a pending effect = false, want true")
	}
	if c1() {
		t.Error("second Cancel() = true, want false")
	}
	if len(m.Pending()) != 0 {
		t.Errorf("Pending() = %+v, want empty", m.Pending())
	}
	if m.CompleteEffect(1, nil, nil) {
		t.Error("CompleteEffect(1) after Cancel = true, want false")
	}
	if len(got1) != 0 {
		t.Error("done(1) ran after Cancel")
	}

	// Cancel after completion reports false.
	var done3 int
	c3 := b.Effects.Start(engine.EffectRequest{ID: 3, Run: run}, func(any, error) { done3++ })
	boom := errors.New("boom")
	if !m.CompleteEffect(3, nil, boom) || done3 != 1 {
		t.Errorf("CompleteEffect(3) = false or done ran %d times", done3)
	}
	if c3() {
		t.Error("Cancel() after completion = true, want false")
	}
}

// AT-ENG-43, ENG-100, ENG-540
func TestATENG43ManualEntropy(t *testing.T) {
	first3 := func(r *rand.Rand) [3]uint64 { return [3]uint64{r.Uint64(), r.Uint64(), r.Uint64()} }
	m := enginetest.New("n", 7)
	e := m.Engine().Entropy
	r := e.Rand("lockstep/a")
	if r2 := e.Rand("lockstep/a"); r2 != r {
		t.Error("Rand(lockstep/a) twice returned different pointers")
	}
	ref := rand.New(rand.NewPCG(7, fnv1a64("lockstep/a"))) //nolint:gosec // G404: the reference stream
	if got, want := first3(r), first3(ref); got != want {
		t.Errorf("first three Uint64 = %v, want %v", got, want)
	}
	if e.Rand("lockstep/b") == r {
		t.Error("different labels returned the same stream")
	}

	again := first3(enginetest.New("n", 7).Engine().Entropy.Rand("lockstep/a"))
	refAgain := rand.New(rand.NewPCG(7, fnv1a64("lockstep/a"))) //nolint:gosec // G404: the reference stream
	if want := first3(refAgain); again != want {
		t.Errorf("second engine with seed 7 = %v, want %v", again, want)
	}
	other := first3(enginetest.New("n", 8).Engine().Entropy.Rand("lockstep/a"))
	if other == again {
		t.Error("seed 8 gave the same values as seed 7")
	}

	wantPanic(t, `enginetest: Entropy label "node/x" does not start with "lockstep/"`, func() { e.Rand("node/x") })
	wantPanic(t, `enginetest: Entropy label "" does not start with "lockstep/"`, func() { e.Rand("") })
}

// AT-ENG-45, ENG-510
func TestATENG45QuiescenceBound(t *testing.T) {
	m := enginetest.New("n", 1)
	a := newFake("A", nil)
	a.always = true
	m.Engine().Scheduler.Ready(a)
	wantPanic(t, "enginetest: no quiescence after 1000000 turns", func() { m.RunUntilIdle() })
	if a.turns != enginetest.MaxTurns {
		t.Errorf("turns = %d, want %d", a.turns, enginetest.MaxTurns)
	}
}

type hostFake struct {
	got  []engine.Envelope
	in   []bool
	turn func() bool
}

func (h *hostFake) Inbound(env engine.Envelope) {
	h.got = append(h.got, env)
	h.in = append(h.in, h.turn())
}

// AT-ENG-46, ENG-500, ENG-560
func TestATENG46ManualBundle(t *testing.T) {
	m := enginetest.New("node-7", 1)
	b := m.Engine()

	if err := b.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	if b.Node != "node-7" || b.Epoch != 1 || b.Blocking || b.Interposer != nil || b.Stop == nil || b.Transport == nil {
		t.Errorf("bundle = Node %q Epoch %d Blocking %v Interposer %v Stop-nil %v Transport-nil %v",
			b.Node, b.Epoch, b.Blocking, b.Interposer, b.Stop == nil, b.Transport == nil)
	}
	if _, ok := b.Scheduler.(engine.LocalScheduler); ok {
		t.Error("Scheduler implements LocalScheduler, want it not to")
	}
	if !b.Tracer.Enabled() {
		t.Error("Tracer.Enabled() = false")
	}

	t.Run("transport", func(t *testing.T) {
		env := engine.Envelope{From: engine.Address{Node: "a"}, To: engine.Address{Node: "b"}, Payload: 1}
		if err := b.Transport.Deliver(env); err != nil {
			t.Errorf("Deliver() = %v, want nil", err)
		}
		e := errors.New("not registered")
		m.SetDeliverError(e)
		if err := b.Transport.Deliver(env); err != e {
			t.Errorf("Deliver() after SetDeliverError = %v, want %v", err, e)
		}
		if got := m.Delivered(); len(got) != 2 || !reflect.DeepEqual(got[0], env) {
			t.Errorf("Delivered() = %v, want both envelopes in order", got)
		}
		m.SetDeliverError(nil)
		if err := b.Transport.Deliver(env); err != nil {
			t.Errorf("Deliver() after SetDeliverError(nil) = %v", err)
		}
	})

	t.Run("inbound", func(t *testing.T) {
		wantPanic(t, "enginetest: Inbound with no Host set", func() { m.Inbound(engine.Envelope{}) })
		h := &hostFake{turn: b.Scheduler.InTurn}
		m.SetHost(h)
		env := engine.Envelope{To: engine.Address{Node: "node-7", Path: "/x"}, Payload: "p"}
		m.Inbound(env)
		if len(h.got) != 1 || !reflect.DeepEqual(h.got[0], env) || !reflect.DeepEqual(h.in, []bool{true}) {
			t.Errorf("Host.Inbound calls = %v, InTurn = %v, want one call with env, InTurn true", h.got, h.in)
		}
		if b.Scheduler.InTurn() {
			t.Error("InTurn() = true after Inbound")
		}
	})

	t.Run("admission", func(t *testing.T) {
		ae := &engine.AdmissionError{Source: engine.SourcePressure}
		if b.Admission.AdmitSend() != nil || b.Admission.AdmitSpawn(false) != nil {
			t.Fatal("admission refuses by default")
		}
		m.SetAdmission(nil, ae)
		if err := b.Admission.AdmitSend(); err != error(ae) {
			t.Errorf("AdmitSend() = %v, want %v", err, ae)
		}
		if err := b.Admission.AdmitSpawn(false); err != nil {
			t.Errorf("AdmitSpawn(false) = %v, want nil", err)
		}
		m.SetAdmission(ae, nil)
		if err := b.Admission.AdmitSpawn(true); err != error(ae) {
			t.Errorf("AdmitSpawn(true) = %v, want %v", err, ae)
		}

		const notBound = "enginetest: Admission.Bind not called"
		wantPanic(t, notBound, func() { m.ChangeAdmission(engine.AdmissionChange{}) })
		wantPanic(t, notBound, func() { m.Usage() })

		var changes []engine.AdmissionChange
		var in []bool
		onChange := func(c engine.AdmissionChange) {
			changes = append(changes, c)
			in = append(in, b.Scheduler.InTurn())
		}
		usageCalls := 0
		usage := func() engine.Usage { usageCalls++; return engine.Usage{Actors: 3, Bytes: 99} }
		b.Admission.Bind(onChange, usage)

		c := engine.AdmissionChange{Source: engine.SourceBudget, On: true, Shedding: true}
		m.ChangeAdmission(c)
		if !reflect.DeepEqual(changes, []engine.AdmissionChange{c}) || !reflect.DeepEqual(in, []bool{true}) {
			t.Errorf("onChange calls = %v, InTurn = %v", changes, in)
		}
		if b.Scheduler.InTurn() {
			t.Error("InTurn() = true after ChangeAdmission")
		}
		if got := m.Usage(); got != (engine.Usage{Actors: 3, Bytes: 99}) || usageCalls != 1 {
			t.Errorf("Usage() = %v after %d calls", got, usageCalls)
		}
		wantPanic(t, "enginetest: Admission.Bind called twice", func() { b.Admission.Bind(onChange, usage) })
	})

	t.Run("failer", func(t *testing.T) {
		e1, e2 := errors.New("e1"), errors.New("e2")
		b.Failer.Fail(e1)
		b.Failer.Fail(e2)
		if got := m.Failures(); len(got) != 2 || got[0] != e1 || got[1] != e2 {
			t.Errorf("Failures() = %v, want [e1 e2]", got)
		}
	})

	t.Run("tracer", func(t *testing.T) {
		b.Tracer.Record("lockstep.x", engine.Attr{Key: "k", Value: "v"})
		want := []engine.Record{{Kind: "lockstep.x", Attrs: []engine.Attr{{Key: "k", Value: "v"}}}}
		if got := m.Records(); !reflect.DeepEqual(got, want) {
			t.Errorf("Records() = %v, want %v", got, want)
		}
		if got := b.Tracer.Describe(int8(-3)); got != "-3" {
			t.Errorf("Describe(int8(-3)) = %q", got)
		}
	})

	t.Run("waiter", func(t *testing.T) {
		wm := enginetest.New("n", 1)
		wb := wm.Engine()
		done := make(chan struct{})
		a := newFake("A", nil)
		a.onTurn = func() { close(done) }
		wb.Scheduler.Ready(a)
		if err := wb.Waiter.Wait(context.Background(), done); err != nil {
			t.Errorf("Wait() with done closed by a turn = %v, want nil", err)
		}
		if a.turns != 1 {
			t.Errorf("Wait ran %d turns, want 1 (RunUntilIdle)", a.turns)
		}
		open := make(chan struct{})
		if err := wb.Waiter.Wait(context.Background(), open); err != enginetest.ErrWouldBlock {
			t.Errorf("Wait() with done open = %v, want ErrWouldBlock", err)
		}
		if got, want := enginetest.ErrWouldBlock.Error(), "enginetest: wait would block; advance the clock or complete an effect"; got != want {
			t.Errorf("ErrWouldBlock = %q, want %q", got, want)
		}
		var nilDone <-chan struct{}
		if err := wb.Waiter.Wait(context.Background(), nilDone); err != enginetest.ErrWouldBlock {
			t.Errorf("Wait() with a nil channel = %v, want ErrWouldBlock", err)
		}
	})

	t.Run("stop", func(t *testing.T) {
		sm := enginetest.New("n", 1)
		sb := sm.Engine()
		var order []string
		ran := 0
		sb.Clock.AfterFunc(time.Second, "t", func() { ran++ })
		if sm.Stopped() {
			t.Fatal("Stopped() = true before Stop")
		}
		sb.Stop()
		sb.Stop() // idempotent
		if !sm.Stopped() {
			t.Fatal("Stopped() = false after Stop")
		}
		a := newFake("A", &order)
		sb.Scheduler.Ready(a)
		if sm.Step() {
			t.Error("Step() = true after Stop")
		}
		if got := sm.RunUntilIdle(); got != 0 || len(order) != 0 {
			t.Errorf("RunUntilIdle() = %d, order %v after Stop", got, order)
		}
		cancel := sb.Clock.AfterFunc(time.Millisecond, "late", func() { ran++ })
		if cancel() {
			t.Error("Cancel() of an AfterFunc after Stop = true, want false")
		}
		if got := sm.Labels(); !reflect.DeepEqual(got, []string{"t"}) {
			t.Errorf("Labels() = %v, want [t]: AfterFunc after Stop records nothing", got)
		}
		sm.Advance(time.Second)
		if ran != 0 {
			t.Errorf("timers ran %d times after Stop", ran)
		}
		if got, want := sm.Now(), enginetest.Epoch.Add(time.Second); !got.Equal(want) {
			t.Errorf("Now() = %v, want %v", got, want)
		}
	})
}

// ENG-510: Advance's RunUntilIdle runs between timers and Step re-queues behind queued turns.
func TestManualRequeueBehindQueued(t *testing.T) {
	m := enginetest.New("n", 1)
	b := m.Engine()
	var order []string
	a := newFake("A", &order, true, false)
	c := newFake("C", &order)
	b.Scheduler.Ready(a)
	b.Scheduler.Ready(c)
	m.RunUntilIdle()
	wantOrder(t, &order, "A", "C", "A")
}

// hostFunc is an engine.Host that calls itself.
type hostFunc func(engine.Envelope)

func (h hostFunc) Inbound(env engine.Envelope) { h(env) }

// ENG-135: Wait, Step, RunUntilIdle and Advance panic when called from engine context (inside a
// turn, timer function, effect completion, Inbound or onChange), and the engine works normally
// once the callback returns.
func TestManualDriveFromEngineContext(t *testing.T) {
	calls := []struct {
		method string
		call   func(m *enginetest.Engine, b engine.Engine)
	}{
		{"Wait", func(_ *enginetest.Engine, b engine.Engine) { _ = b.Waiter.Wait(context.Background(), nil) }},
		{"Step", func(m *enginetest.Engine, _ engine.Engine) { m.Step() }},
		{"RunUntilIdle", func(m *enginetest.Engine, _ engine.Engine) { m.RunUntilIdle() }},
		{"Advance", func(m *enginetest.Engine, _ engine.Engine) { m.Advance(time.Second) }},
	}
	// Each enter runs fn once, as engine context of m.
	contexts := []struct {
		name  string
		enter func(m *enginetest.Engine, b engine.Engine, fn func())
	}{
		{"turn", func(m *enginetest.Engine, b engine.Engine, fn func()) {
			a := newFake("A", nil)
			a.onTurn = fn
			b.Scheduler.Ready(a)
			m.Step()
		}},
		{"timer", func(m *enginetest.Engine, b engine.Engine, fn func()) {
			b.Clock.AfterFunc(time.Millisecond, "t", fn)
			m.Advance(time.Millisecond)
		}},
		{"effect", func(m *enginetest.Engine, b engine.Engine, fn func()) {
			run := func(context.Context) (any, error) { return nil, nil }
			b.Effects.Start(engine.EffectRequest{ID: 1, Name: "e", Run: run}, func(any, error) { fn() })
			m.CompleteEffect(1, nil, nil)
		}},
		{"inbound", func(m *enginetest.Engine, _ engine.Engine, fn func()) {
			m.SetHost(hostFunc(func(engine.Envelope) { fn() }))
			m.Inbound(engine.Envelope{})
		}},
		{"onChange", func(m *enginetest.Engine, b engine.Engine, fn func()) {
			b.Admission.Bind(func(engine.AdmissionChange) { fn() }, func() engine.Usage { return engine.Usage{} })
			m.ChangeAdmission(engine.AdmissionChange{})
		}},
	}
	for _, c := range contexts {
		for _, call := range calls {
			t.Run(c.name+"/"+call.method, func(t *testing.T) {
				m := enginetest.New("n", 1)
				b := m.Engine()
				ran := false
				var got string
				c.enter(m, b, func() {
					ran = true
					got = panicText(func() { call.call(m, b) })
				})
				if !ran {
					t.Fatal("the callback did not run")
				}
				if want := "enginetest: " + call.method + " called from engine context"; got != want {
					t.Errorf("panic = %q, want %q", got, want)
				}

				// Outside engine context the same calls work.
				if b.Scheduler.InTurn() {
					t.Error("InTurn() = true after the callback")
				}
				order := []string{}
				b.Scheduler.Ready(newFake("B", &order))
				if n := m.RunUntilIdle(); n != 1 {
					t.Errorf("RunUntilIdle() = %d, want 1", n)
				}
				fired := false
				b.Clock.AfterFunc(time.Second, "t", func() { fired = true })
				m.Advance(time.Second)
				if !fired {
					t.Error("Advance did not fire the timer")
				}
				done := make(chan struct{})
				close(done)
				if err := b.Waiter.Wait(context.Background(), done); err != nil {
					t.Errorf("Wait(closed) = %v, want nil", err)
				}
				if err := b.Waiter.Wait(context.Background(), make(chan struct{})); !errors.Is(err, enginetest.ErrWouldBlock) {
					t.Errorf("Wait(open) = %v, want ErrWouldBlock", err)
				}
			})
		}
	}
}
