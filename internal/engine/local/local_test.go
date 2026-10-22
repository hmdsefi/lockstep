// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package local_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/engine/local"
)

// wait bounds every wait for the loop in these tests.
const wait = 5 * time.Second

// fake is a fake actor (spec §10): an engine.Slot field, an Address, and a RunTurn that hands its
// budget to turn and returns turn's more.
type fake struct {
	slot engine.Slot
	addr engine.Address
	turn func(budget int) bool
}

func newFake(name string, turn func(budget int) bool) *fake {
	return &fake{addr: engine.Address{Node: "a", Path: "/" + name, UID: 1}, turn: turn}
}

func (f *fake) Address() engine.Address { return f.addr }
func (f *fake) Slot() *engine.Slot      { return &f.slot }
func (f *fake) RunTurn(budget int) bool { return f.turn(budget) }

// start returns a running engine for cfg that the test stops when it ends.
func start(t *testing.T, cfg local.Config) (*local.Engine, engine.Engine) {
	t.Helper()
	e, err := local.New(cfg)
	if err != nil {
		t.Fatalf("New(%+v): %v", cfg, err)
	}
	t.Cleanup(e.Stop)
	return e, e.Engine()
}

// waitFor polls cond until it holds, and fails the test after wait.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// recv receives from ch, and fails the test after wait.
func recv[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(wait):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
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

// gate is a fake actor whose turn blocks the loop until release is closed, so a test can queue
// tasks in a known order behind it.
type gate struct {
	*fake
	running chan struct{}
	release chan struct{}
}

func newGate() *gate {
	g := &gate{running: make(chan struct{}), release: make(chan struct{})}
	g.fake = newFake("gate", func(int) bool {
		close(g.running)
		<-g.release
		return false
	})
	return g
}

// hold runs g's turn on b's loop and returns once the loop is blocked in it.
func (g *gate) hold(t *testing.T, b engine.Engine) {
	t.Helper()
	b.Scheduler.Ready(g)
	recv(t, "the gate turn", g.running)
	if b.Scheduler.InTurn() {
		t.Error("InTurn() = true on the test goroutine while the loop runs a turn")
	}
}

// AT-ENG-30, ENG-400
func TestATENG30LocalNew(t *testing.T) {
	for _, tc := range []struct {
		cfg  local.Config
		want string
	}{
		{local.Config{}, "lockstep: local engine: empty node name"},
		{local.Config{Node: "a", Throughput: -1}, "lockstep: local engine: throughput -1 < 0"},
	} {
		e, err := local.New(tc.cfg)
		if err == nil || err.Error() != tc.want {
			t.Errorf("New(%+v) error = %v, want %q", tc.cfg, err, tc.want)
		}
		if e != nil {
			t.Errorf("New(%+v) = %v, want nil", tc.cfg, e)
			e.Stop()
		}
	}
	if local.DefaultThroughput != 32 || local.EffectWorkers != 64 || local.EffectQueue != 1024 {
		t.Errorf("constants = %d, %d, %d, want 32, 64, 1024",
			local.DefaultThroughput, local.EffectWorkers, local.EffectQueue)
	}
	for _, tc := range []struct{ throughput, want int }{{0, 32}, {5, 5}} {
		_, b := start(t, local.Config{Node: "a", Throughput: tc.throughput})
		budgets := make(chan int, 1)
		b.Scheduler.Ready(newFake("x", func(budget int) bool { budgets <- budget; return false }))
		if got := recv(t, "the first turn", budgets); got != tc.want {
			t.Errorf("Throughput %d: budget = %d, want %d", tc.throughput, got, tc.want)
		}
	}
}

// AT-ENG-31, ENG-050, ENG-410
func TestATENG31LocalFairness(t *testing.T) {
	e, b := start(t, local.Config{Node: "a"})
	var order []string // appended on the loop only; read after Stop
	done := make(chan struct{}, 2)
	actor := func(name string) *fake {
		turns := 0
		return newFake(name, func(int) bool {
			order = append(order, name)
			turns++
			if turns == 4 {
				done <- struct{}{}
			}
			return turns <= 3
		})
	}
	a, bb := actor("A"), actor("B")
	g := newGate()
	g.hold(t, b)
	b.Scheduler.Ready(a)
	b.Scheduler.Ready(bb)
	close(g.release)
	recv(t, "A's last turn", done)
	recv(t, "B's last turn", done)
	e.Stop()
	if got, want := fmt.Sprint(order), "[A B A B A B A B]"; got != want {
		t.Errorf("turn order = %s, want %s", got, want)
	}
}

// AT-ENG-32, ENG-050, ENG-060, ENG-410
func TestATENG32LocalConcurrency(t *testing.T) {
	const goroutines, calls, actors = 16, 1000, 4
	e, b := start(t, local.Config{Node: "a"})
	var (
		counter int // shared by every turn without synchronization: turns run on one goroutine
		seq     atomic.Uint64
		fakes   [actors]*fake
		ready   [actors]atomic.Uint64 // seq of the last Ready of each actor
		began   [actors]atomic.Uint64 // seq of the last turn start of each actor
	)
	for i := range fakes {
		fakes[i] = newFake(fmt.Sprint("x", i), func(int) bool {
			began[i].Store(seq.Add(1))
			counter++
			return false
		})
	}
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for n := range calls {
				i := (g + n) % actors
				s := seq.Add(1)
				for {
					old := ready[i].Load()
					if old >= s || ready[i].CompareAndSwap(old, s) {
						break
					}
				}
				b.Scheduler.Ready(fakes[i])
			}
		})
	}
	wg.Wait()
	for i := range fakes {
		waitFor(t, fmt.Sprintf("a turn of x%d after its last Ready", i), func() bool {
			return began[i].Load() > ready[i].Load()
		})
	}
	e.Stop()
	if counter < actors || counter > goroutines*calls {
		t.Errorf("turns = %d, want between %d and %d", counter, actors, goroutines*calls)
	}
}

// AT-ENG-33, ENG-055, ENG-070, ENG-420
func TestATENG33LocalTimersAndInTurn(t *testing.T) {
	e, b := start(t, local.Config{Node: "a"})
	counter := 0 // incremented by a timer function and by a turn, without synchronization
	inTurn := make(chan bool, 1)
	cancel := b.Clock.AfterFunc(10*time.Millisecond, "lockstep/timer", func() {
		counter++
		inTurn <- b.Scheduler.InTurn()
	})
	if !recv(t, "the timer", inTurn) {
		t.Error("InTurn() = false inside a timer function")
	}
	if b.Scheduler.InTurn() {
		t.Error("InTurn() = true on the test goroutine")
	}
	if cancel() {
		t.Error("Cancel after the timer ran = true")
	}
	turned := make(chan bool, 1)
	b.Scheduler.Ready(newFake("x", func(int) bool { counter++; turned <- b.Scheduler.InTurn(); return false }))
	if !recv(t, "the turn", turned) {
		t.Error("InTurn() = false inside a turn")
	}

	var ran atomic.Bool
	cancel = b.Clock.AfterFunc(time.Hour, "lockstep/timer", func() { ran.Store(true) })
	if !cancel() {
		t.Error("first Cancel of a pending timer = false")
	}
	if cancel() {
		t.Error("second Cancel = true")
	}

	// A timer whose runtime timer fired but whose task has not started yet: Cancel still wins.
	g := newGate()
	g.hold(t, b)
	cancel = b.Clock.AfterFunc(0, "lockstep/timer", func() { ran.Store(true) })
	time.Sleep(20 * time.Millisecond) // the runtime timer fires and queues the task behind the gate
	if !cancel() {
		t.Error("Cancel of a timer whose task is queued = false")
	}
	close(g.release)

	// Arm AfterFunc(-1) in engine context and set the flag after it returns: fn runs on the loop,
	// so it can only run once this turn has ended, never inside the AfterFunc call.
	var armed atomic.Bool
	fired := make(chan bool, 1)
	b.Scheduler.Ready(newFake("arm", func(int) bool {
		b.Clock.AfterFunc(-1, "lockstep/timer", func() { fired <- armed.Load() })
		armed.Store(true)
		return false
	}))
	if !recv(t, "the AfterFunc(-1) timer", fired) {
		t.Error("AfterFunc(-1) ran fn before AfterFunc returned")
	}

	stopPanic := make(chan string, 1)
	b.Clock.AfterFunc(0, "lockstep/timer", func() { stopPanic <- panicText(e.Stop) })
	if got, want := recv(t, "Stop inside a timer", stopPanic), "lockstep: local engine: Stop called from engine context"; got != want {
		t.Errorf("Stop inside a timer: panic = %q, want %q", got, want)
	}

	time.Sleep(20 * time.Millisecond)
	e.Stop()
	if ran.Load() {
		t.Error("a cancelled timer ran")
	}
	if counter != 2 {
		t.Errorf("counter = %d, want 2", counter)
	}
}

// AT-ENG-35, ENG-100, ENG-440
func TestATENG35LocalEntropy(t *testing.T) {
	_, b := start(t, local.Config{Node: "a"})
	r1, r2 := b.Entropy.Rand("lockstep/a"), b.Entropy.Rand("lockstep/a")
	if r1 == nil || r2 == nil {
		t.Fatal("Rand returned nil")
	}
	if r1 == r2 {
		t.Error("two Rand calls returned the same pointer")
	}
	if r1.Uint64() == r2.Uint64() {
		t.Error("two streams start with the same Uint64")
	}
	got := panicText(func() { b.Entropy.Rand("kernel/x") })
	if want := `local: Entropy label "kernel/x" does not start with "lockstep/"`; got != want {
		t.Errorf("Rand(\"kernel/x\") panic = %q, want %q", got, want)
	}
}

// AT-ENG-36, ENG-140, ENG-460
func TestATENG36LocalStop(t *testing.T) {
	t.Run("after", func(t *testing.T) {
		e, b := start(t, local.Config{Node: "a"})
		var called atomic.Int32
		ctxDone := make(chan (<-chan struct{}), 1)
		returned := make(chan struct{})
		b.Effects.Start(engine.EffectRequest{ID: 1, Name: "block", Run: func(ctx context.Context) (any, error) {
			defer close(returned)
			ctxDone <- ctx.Done()
			<-ctx.Done()
			return nil, ctx.Err()
		}}, func(any, error) { called.Add(1) })
		running := recv(t, "the effect to start", ctxDone)

		b.Clock.AfterFunc(20*time.Millisecond, "lockstep/timer", func() { called.Add(1) }) // armed just before Stop
		e.Stop()
		e.Stop() // idempotent
		select {
		case <-running:
		default:
			t.Error("a running effect's context is not done after Stop")
		}
		recv(t, "the effect to return", returned)

		b.Scheduler.Ready(newFake("x", func(int) bool { called.Add(1); return false }))
		if b.Clock.AfterFunc(0, "lockstep/timer", func() { called.Add(1) })() {
			t.Error("Cancel of an AfterFunc after Stop = true")
		}
		c := b.Effects.Start(engine.EffectRequest{ID: 2, Name: "late", Run: func(context.Context) (any, error) {
			called.Add(1)
			return nil, nil
		}}, func(any, error) { called.Add(1) })
		if c() {
			t.Error("Cancel of a Start after Stop = true")
		}
		b.Clock.AfterFunc(0, "lockstep/timer", func() { called.Add(1) })
		time.Sleep(50 * time.Millisecond)
		if n := called.Load(); n != 0 {
			t.Errorf("%d callbacks ran after Stop", n)
		}
	})

	t.Run("waits for the running turn", func(t *testing.T) {
		e, b := start(t, local.Config{Node: "a"})
		began := make(chan struct{})
		var finished atomic.Bool
		b.Scheduler.Ready(newFake("x", func(int) bool {
			close(began)
			time.Sleep(20 * time.Millisecond)
			finished.Store(true)
			return true // would re-queue; Stop discards it
		}))
		recv(t, "the turn", began)
		e.Stop()
		if !finished.Load() {
			t.Error("Stop returned before the running turn did")
		}
	})

	t.Run("discards queued tasks", func(t *testing.T) {
		e, b := start(t, local.Config{Node: "a"})
		g := newGate()
		g.hold(t, b)
		var ran atomic.Bool
		b.Scheduler.Ready(newFake("x", func(int) bool { ran.Store(true); return false }))
		stopped := make(chan struct{})
		go func() { e.Stop(); close(stopped) }()
		// Once Stop stopped accepting tasks, AfterFunc returns a Cancel that reports false.
		waitFor(t, "Stop to stop accepting tasks", func() bool {
			return !b.Clock.AfterFunc(time.Hour, "lockstep/timer", func() {})()
		})
		close(g.release)
		recv(t, "Stop", stopped)
		if ran.Load() {
			t.Error("a turn queued before Stop ran")
		}
	})
}

// AT-ENG-37, AT-ENG-44 (local clause), ENG-130, ENG-135, ENG-450
func TestATENG37LocalBundle(t *testing.T) {
	e, b := start(t, local.Config{Node: "a"})
	if b.Node != "a" || b.Epoch == 0 || !b.Blocking || b.Transport != nil || b.Interposer != nil {
		t.Errorf("bundle Node %q, Epoch %d, Blocking %v, Transport %v, Interposer %v; want a, non-zero, true, nil, nil",
			b.Node, b.Epoch, b.Blocking, b.Transport, b.Interposer)
	}
	if err := b.Validate(); err != nil {
		t.Errorf("Validate() = %v", err)
	}
	if again := e.Engine(); again.Epoch != b.Epoch || again.Scheduler != b.Scheduler || again.Tracer != b.Tracer {
		t.Error("two Engine() calls differ")
	}
	if b.Stop == nil {
		t.Error("Stop is nil")
	}
	if _, ok := b.Scheduler.(engine.LocalScheduler); ok {
		t.Error("the Scheduler implements LocalScheduler")
	}
	if err := b.Admission.AdmitSpawn(true); err != nil {
		t.Errorf("AdmitSpawn(true) = %v", err)
	}
	if err := b.Admission.AdmitSpawn(false); err != nil {
		t.Errorf("AdmitSpawn(false) = %v", err)
	}
	if err := b.Admission.AdmitSend(); err != nil {
		t.Errorf("AdmitSend() = %v", err)
	}
	b.Admission.Bind(func(engine.AdmissionChange) { t.Error("Bind called onChange") },
		func() engine.Usage { t.Error("Bind called usage"); return engine.Usage{} })
	b.Failer.Fail(errors.New("x"))
	if b.Tracer.Enabled() {
		t.Error("Tracer.Enabled() = true with no tracer")
	}
	b.Tracer.Record("lockstep.x", engine.Attr{Key: "k", Value: "v"})
	if got := b.Tracer.Describe(3); got != "int" {
		t.Errorf("Describe(3) = %q, want \"int\"", got)
	}
	if now := b.Clock.Now(); time.Since(now) > time.Minute || time.Since(now) < -time.Minute {
		t.Errorf("Clock.Now() = %v, not wall time", now)
	}

	done := make(chan struct{})
	close(done)
	if err := b.Waiter.Wait(context.Background(), done); err != nil {
		t.Errorf("Wait after close(done) = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Waiter.Wait(ctx, make(chan struct{})); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait after cancel() = %v, want context.Canceled", err)
	}

	// AT-ENG-44's local clause: a local engine built with a Recorder records through it.
	rec := enginetest.NewRecorder()
	_, rb := start(t, local.Config{Node: "a", Tracer: rec})
	if !rb.Tracer.Enabled() {
		t.Error("Tracer.Enabled() = false with a Recorder")
	}
	turned := make(chan struct{})
	rb.Scheduler.Ready(newFake("x", func(int) bool {
		rb.Tracer.Record("lockstep.turn", engine.Attr{Key: "actor", Value: "x"})
		close(turned)
		return false
	}))
	recv(t, "the turn", turned)
	got := rec.Records()
	if len(got) != 1 || got[0].Kind != "lockstep.turn" || fmt.Sprint(got[0].Attrs) != "[{actor x}]" {
		t.Errorf("Records() = %v, want [{lockstep.turn [{actor x}]}]", got)
	}
}
