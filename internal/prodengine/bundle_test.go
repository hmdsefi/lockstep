// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// fakeAdmission is an engine.Admission that refuses nothing and counts Bind calls.
type fakeAdmission struct{ binds int }

func (*fakeAdmission) AdmitSpawn(bool) error { return nil }
func (*fakeAdmission) AdmitSend() error      { return nil }
func (f *fakeAdmission) Bind(func(engine.AdmissionChange), func() engine.Usage) {
	f.binds++
}

// TestATSCH23EntropyTracerAdmission covers SCH-240, SCH-250 and SCH-260 (AT-SCH-23).
func TestATSCH23EntropyTracerAdmission(t *testing.T) {
	b := newEngine(t, Config{Node: "n", Workers: 1}).Engine()

	r1, r2 := b.Entropy.Rand("lockstep/x"), b.Entropy.Rand("lockstep/x")
	if r1 == nil || r2 == nil {
		t.Fatal("Rand(\"lockstep/x\") returned nil")
	}
	if v1, v2 := r1.Uint64(), r2.Uint64(); v1 == v2 {
		t.Errorf("two Rand(\"lockstep/x\") streams start with the same value %d, want different", v1)
	}
	const want = `prodengine: Entropy label "x" does not start with "lockstep/"`
	if got := panicText(func() { b.Entropy.Rand("x") }); got != want {
		t.Errorf("Rand(\"x\") panicked with %q, want %q", got, want)
	}

	if b.Tracer.Enabled() {
		t.Error("Tracer.Enabled() = true, want false")
	}
	b.Tracer.Record("lockstep.test", engine.Attr{Key: "k", Value: "v"})
	b.Tracer.Record("")
	if got := b.Tracer.Describe(3); got != "int" {
		t.Errorf("Tracer.Describe(3) = %q, want \"int\"", got)
	}

	for name, err := range map[string]error{
		"AdmitSpawn(false)": b.Admission.AdmitSpawn(false),
		"AdmitSpawn(true)":  b.Admission.AdmitSpawn(true),
		"AdmitSend()":       b.Admission.AdmitSend(),
	} {
		if err != nil {
			t.Errorf("with nil Config.Admission, %s = %v, want nil", name, err)
		}
	}
	b.Admission.Bind(func(engine.AdmissionChange) { t.Error("admit-all reported a change") },
		func() engine.Usage { return engine.Usage{} })
	if err := b.Admission.AdmitSend(); err != nil {
		t.Errorf("AdmitSend() after Bind = %v, want nil", err)
	}

	fake := &fakeAdmission{}
	fb := newEngine(t, Config{Node: "n", Workers: 1, Admission: fake}).Engine()
	if got, ok := fb.Admission.(*fakeAdmission); !ok || got != fake {
		t.Errorf("Admission = %#v, want the Config's fake %p", fb.Admission, fake)
	}
	if fake.binds != 0 {
		t.Errorf("the engine called Bind %d times, want 0", fake.binds)
	}
}

// TestATSCH26Bundle covers SCH-115 and SCH-260 (AT-SCH-26).
func TestATSCH26Bundle(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2})
	b := e.Engine()
	if err := b.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
	if b.Node != "n" {
		t.Errorf("Node = %q, want \"n\"", b.Node)
	}
	if b.Epoch == 0 {
		t.Error("Epoch = 0, want non-zero")
	}
	if !b.Blocking {
		t.Error("Blocking = false, want true")
	}
	if b.Transport != nil {
		t.Errorf("Transport = %#v, want nil", b.Transport)
	}
	if b.Interposer != nil {
		t.Errorf("Interposer = %#v, want nil", b.Interposer)
	}
	if _, ok := b.Scheduler.(engine.LocalScheduler); !ok {
		t.Errorf("Scheduler %T does not implement engine.LocalScheduler", b.Scheduler)
	}

	b2 := e.Engine()
	if b2.Epoch != b.Epoch {
		t.Errorf("a second Engine() has Epoch %d, want %d", b2.Epoch, b.Epoch)
	}
	if b2.Node != b.Node || b2.Blocking != b.Blocking || b2.Scheduler != b.Scheduler ||
		b2.Clock != b.Clock || b2.Effects != b.Effects || b2.Entropy != b.Entropy ||
		b2.Tracer != b.Tracer || b2.Admission != b.Admission || b2.Failer != b.Failer ||
		b2.Waiter != b.Waiter || b2.Transport != nil || b2.Interposer != nil || b2.Stop == nil {
		t.Errorf("a second Engine() = %+v, want the same values as %+v", b2, b)
	}
	if other := newEngine(t, Config{Node: "n", Workers: 1}).Engine(); other.Scheduler == b.Scheduler {
		t.Error("two engines share one Scheduler")
	}

	done := make(chan struct{})
	close(done)
	if err := b.Waiter.Wait(context.Background(), done); err != nil {
		t.Errorf("Wait after close(done) = %v, want nil", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Waiter.Wait(ctx, make(chan struct{})); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait after cancel() = %v, want context.Canceled", err)
	}

	b.Failer.Fail(errors.New("misuse"))

	b.Stop()
	if !e.s.closing.Load() {
		t.Error("the bundle's Stop did not stop the engine")
	}
	a := newFake(t, 0)
	a.add()
	b.Scheduler.Ready(a)
	time.Sleep(100 * time.Millisecond)
	if n := a.turns.Load(); n != 0 {
		t.Errorf("Ready after the bundle's Stop ran %d turns, want 0", n)
	}
}
