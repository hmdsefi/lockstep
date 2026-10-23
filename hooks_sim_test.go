// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// askMsg is a request that carries the ReplyTo of its ask; the targets here never answer it.
type askMsg struct{ rt lockstep.ReplyTo[int] }

// askTrigger tells an asker actor to ask to; sys-side code reads the Reply it started.
type askTrigger struct{ to lockstep.PID[askMsg] }

// asker starts one ask per askTrigger it receives and keeps the replies in order.
type asker struct{ replies []*lockstep.Reply[int] }

func (a *asker) Receive(ctx *lockstep.Context[askTrigger], m askTrigger) {
	a.replies = append(a.replies, lockstep.Ask(ctx, m.to, func(rt lockstep.ReplyTo[int]) askMsg {
		return askMsg{rt: rt}
	}))
}

// silent holds the ReplyTo of the first request it gets and never answers.
type silent struct{ held []lockstep.ReplyTo[int] }

func (s *silent) Receive(_ *lockstep.Context[askMsg], m askMsg) { s.held = append(s.held, m.rt) }

// simSystem builds a system on node n1 with hooks.NewSystem on a manual engine, as sim does.
func simSystem(t *testing.T) (*lockstep.System, *enginetest.Engine) {
	t.Helper()
	eng := enginetest.New("n1", 1)
	b, err := hooks.NewSystem(eng.Engine(), nil)
	if err != nil {
		t.Fatalf("hooks.NewSystem: %v", err)
	}
	return systemOf(t, b), eng
}

// mustPanic fails the test unless fn panics.
func mustPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s did not panic", what)
		}
	}()
	fn()
}

// AT-SIM-32: SIM §5.9, MSG-360, MSG-370, SIM-200.
func TestATSIM32Hooks(t *testing.T) {
	if hooks.Discard == nil || hooks.AskResolved == nil || hooks.PendingAsks == nil {
		t.Fatalf("hooks unset after init: Discard=%t AskResolved=%t PendingAsks=%t",
			hooks.Discard != nil, hooks.AskResolved != nil, hooks.PendingAsks != nil)
	}
	sys, eng := simSystem(t)
	a := &asker{}
	ap, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[askTrigger] { return a })
	if err != nil {
		t.Fatal(err)
	}
	b := &silent{}
	bp, err := lockstep.Spawn(sys, "b", func() lockstep.Actor[askMsg] { return b })
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if got := hooks.PendingAsks(sys); got != nil {
		t.Errorf("PendingAsks before any ask = %v, want nil", got)
	}
	if err := lockstep.Send(sys, ap, askTrigger{to: bp}); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if len(a.replies) != 1 {
		t.Fatalf("a started %d asks, want 1", len(a.replies))
	}
	r := a.replies[0]

	got := hooks.PendingAsks(sys)
	if len(got) != 1 {
		t.Fatalf("PendingAsks = %v, want one entry", got)
	}
	if got[0].Asker.String() != ap.Address().String() || got[0].Target.String() != bp.Address().String() || !got[0].ByActor {
		t.Errorf("PendingAsks[0] = %+v, want Asker %v, Target %v, ByActor true", got[0], ap, bp)
	}
	if got[0].Ask.Node != "n1" || got[0].Ask.Path != "/temp/1" {
		t.Errorf("PendingAsks[0].Ask = %v, want /temp/1 on n1", got[0].Ask)
	}
	if hooks.AskResolved(r) {
		t.Error("AskResolved is true before Discard")
	}

	hooks.Discard(sys)
	if !hooks.AskResolved(r) {
		t.Error("AskResolved is false after Discard")
	}
	if v, err := r.Await(context.Background()); v != 0 || !errors.Is(err, lockstep.ErrStopped) {
		t.Errorf("Await after Discard = %d, %v, want 0 and ErrStopped", v, err)
	}
	if got := hooks.PendingAsks(sys); got != nil {
		t.Errorf("PendingAsks after Discard = %v, want nil", got)
	}

	mustPanic(t, "AskResolved(3)", func() { hooks.AskResolved(3) })
	mustPanic(t, "AskResolved(nil)", func() { hooks.AskResolved(nil) })
	mustPanic(t, "AskResolved of a nil *Reply", func() { hooks.AskResolved((*lockstep.Reply[int])(nil)) })
	mustPanic(t, "PendingAsks(3)", func() { hooks.PendingAsks(3) })
	mustPanic(t, "Discard(3)", func() { hooks.Discard(3) })
	mustPanic(t, "Discard(nil)", func() { hooks.Discard(nil) })
}

// MSG-360, MSG-370: asks resolve in ask-counter order and PendingAsks lists them in that order;
// a *System asker is not an actor.
func TestDiscardResolvesInCounterOrder(t *testing.T) {
	sys, eng := simSystem(t)
	a := &asker{}
	ap, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[askTrigger] { return a })
	if err != nil {
		t.Fatal(err)
	}
	bp, err := lockstep.Spawn(sys, "b", func() lockstep.Actor[askMsg] { return &silent{} })
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	send := func(rt lockstep.ReplyTo[int]) askMsg { return askMsg{rt: rt} }
	r1 := lockstep.Ask(sys, bp, send, lockstep.WithTimeout(time.Hour)) // /temp/1, asked by sys
	for range 2 {
		if err := lockstep.Send(sys, ap, askTrigger{to: bp}); err != nil { // /temp/2 and /temp/3, by a
			t.Fatal(err)
		}
	}
	eng.RunUntilIdle()
	_, r4 := lockstep.NewReply[int](sys) // /temp/4, no target
	if len(a.replies) != 2 {
		t.Fatalf("a started %d asks, want 2", len(a.replies))
	}

	pas := hooks.PendingAsks(sys)
	var paths []string
	var byActor []bool
	for _, p := range pas {
		paths = append(paths, p.Ask.Path)
		byActor = append(byActor, p.ByActor)
	}
	if want := []string{"/temp/1", "/temp/2", "/temp/3", "/temp/4"}; !slices.Equal(paths, want) {
		t.Fatalf("PendingAsks paths = %v, want %v", paths, want)
	}
	if want := []bool{false, true, true, false}; !slices.Equal(byActor, want) {
		t.Errorf("ByActor = %v, want %v", byActor, want)
	}
	if pas[0].Target.String() != bp.Address().String() || pas[0].Asker.Path != "/user" {
		t.Errorf("ask 1 = %+v, want target %v and the user guardian as asker", pas[0], bp)
	}
	if pas[3].Target != (engine.Address{}) {
		t.Errorf("NewReply target = %v, want the zero Address", pas[3].Target)
	}

	before := len(eng.Records())
	hooks.Discard(sys)
	var order []string
	for _, rec := range eng.Records()[before:] {
		if rec.Kind != "lockstep.reply" {
			t.Errorf("Discard recorded %q, want only lockstep.reply", rec.Kind)
			continue
		}
		for _, at := range rec.Attrs {
			if at.Key == "ask" {
				order = append(order, at.Value)
			}
		}
	}
	if len(order) != 4 {
		t.Fatalf("Discard resolved %d asks (%v), want 4", len(order), order)
	}
	for i, p := range pas {
		if order[i] != p.Ask.String() {
			t.Errorf("resolution %d is %s, want %s", i, order[i], p.Ask)
		}
	}
	for i, r := range []*lockstep.Reply[int]{r1, a.replies[0], a.replies[1], r4} {
		if !hooks.AskResolved(r) {
			t.Errorf("ask %d is unresolved after Discard", i+1)
		}
		if _, err := r.Await(context.Background()); !errors.Is(err, lockstep.ErrStopped) {
			t.Errorf("ask %d: err = %v, want ErrStopped", i+1, err)
		}
	}

	// The timers were cancelled, a late reply is dropped and a second Discard changes nothing.
	eng.Advance(2 * time.Hour)
	hooks.Discard(sys)
	if n := len(eng.Records()); n != before+4 {
		t.Errorf("%d records after the timers, a second Discard: want %d", n, before+4)
	}
	for _, rec := range eng.Records() {
		if rec.Kind == "lockstep.timeout" {
			t.Error("a timeout fired for an ask that Discard resolved")
		}
	}
}

// resource is an io.Closer that counts its Close calls.
type resource struct{ closed int }

func (r *resource) Close() error { r.closed++; return nil }

// lifecycleActor owns a resource and counts its Stopped calls.
type lifecycleActor struct {
	res     *resource
	stopped *int
}

func (l *lifecycleActor) Started(ctx *lockstep.Context[int])                  { ctx.Own(l.res) }
func (l *lifecycleActor) Receive(*lockstep.Context[int], int)                 {}
func (l *lifecycleActor) Stopped(*lockstep.Context[int], lockstep.StopReason) { *l.stopped++ }

// watcher counts the signals it receives.
type watcher struct {
	target  lockstep.Address
	signals *int
}

func (w *watcher) Started(ctx *lockstep.Context[int])             { _ = lockstep.Watch(ctx, w.target) }
func (w *watcher) Receive(*lockstep.Context[int], int)            {}
func (w *watcher) Signal(*lockstep.Context[int], lockstep.Signal) { *w.signals++ }

// SUP-360, SIM-110: a discarded system runs no Stopped, closes nothing, pushes no Terminated and
// records no lockstep.stop; every entry point that takes the system as sender, parent or subject
// then fails or does nothing.
func TestDiscardRunsNothing(t *testing.T) {
	sys, eng := simSystem(t)
	var stopped, signals int
	res := &resource{}
	tp, err := lockstep.Spawn(sys, "t", func() lockstep.Actor[int] { return &lifecycleActor{res: res, stopped: &stopped} })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockstep.Spawn(sys, "w", func() lockstep.Actor[int] {
		return &watcher{target: tp.Address(), signals: &signals}
	}); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := lockstep.Send(sys, tp, 1); err != nil {
		t.Fatalf("Send before Discard: %v", err)
	}
	eng.RunUntilIdle()
	before := len(eng.Records())

	hooks.Discard(sys)
	eng.RunUntilIdle()
	eng.Advance(time.Hour)

	if err := lockstep.Send(sys, tp, 2); !errors.Is(err, lockstep.ErrStopped) {
		t.Errorf("Send after Discard: err = %v, want ErrStopped", err)
	}
	if err := lockstep.SendContext(context.Background(), sys, tp, 3); !errors.Is(err, lockstep.ErrStopped) {
		t.Errorf("SendContext after Discard: err = %v, want ErrStopped", err)
	}
	r := lockstep.Ask(sys, tp, func(rt lockstep.ReplyTo[int]) int { return 4 })
	if !hooks.AskResolved(r) {
		t.Error("Ask after Discard returned a pending reply")
	}
	if _, err := r.Await(context.Background()); !errors.Is(err, lockstep.ErrStopped) {
		t.Errorf("Ask after Discard: err = %v, want ErrStopped", err)
	}
	if _, err := lockstep.Spawn(sys, "late", func() lockstep.Actor[int] { return &lifecycleActor{res: &resource{}, stopped: &stopped} }); !errors.Is(err, lockstep.ErrStopped) {
		t.Errorf("Spawn after Discard: err = %v, want ErrStopped", err)
	}
	if err := sys.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown after Discard = %v, want nil", err)
	}
	eng.RunUntilIdle()

	if stopped != 0 {
		t.Errorf("Stopped ran %d times, want 0", stopped)
	}
	if res.closed != 0 {
		t.Errorf("the owned resource was closed %d times, want 0", res.closed)
	}
	if signals != 0 {
		t.Errorf("the watcher received %d signals, want 0", signals)
	}
	for _, rec := range eng.Records()[before:] {
		switch rec.Kind {
		case "lockstep.stop", "lockstep.shutdown", "lockstep.send":
			t.Errorf("record %q after Discard", rec.Kind)
		case "lockstep.spawn":
			// Only the refusal of the late Spawn, which carries a result.
			if !slices.ContainsFunc(rec.Attrs, func(a engine.Attr) bool { return a.Key == "result" }) {
				t.Errorf("a spawn that was not refused: %+v", rec)
			}
		}
	}
	if eng.Stopped() {
		t.Error("Discard stopped the engine, which is the caller's to do")
	}
}

// MSG-430: an ask resolves exactly once when Discard races with replies; each lockstep.reply
// record names one ask once.
func TestDiscardRacesReplies(t *testing.T) {
	sys, eng := simSystem(t)
	b := &silent{}
	bp, err := lockstep.Spawn(sys, "b", func() lockstep.Actor[askMsg] { return b })
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	const n = 50
	replies := make([]*lockstep.Reply[int], n)
	for i := range replies {
		replies[i] = lockstep.Ask(sys, bp, func(rt lockstep.ReplyTo[int]) askMsg { return askMsg{rt: rt} }, lockstep.WithTimeout(time.Hour))
	}
	eng.RunUntilIdle()
	if len(b.held) != n {
		t.Fatalf("b holds %d ReplyTos, want %d", len(b.held), n)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, rt := range b.held {
			rt.Send(sys, 7)
		}
	}()
	hooks.Discard(sys)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the replying goroutine did not finish")
	}

	seen := make(map[string]int)
	for _, rec := range eng.Records() {
		if rec.Kind != "lockstep.reply" {
			continue
		}
		for _, at := range rec.Attrs {
			if at.Key == "ask" {
				seen[at.Value]++
			}
		}
	}
	for i, r := range replies {
		if !hooks.AskResolved(r) {
			t.Fatalf("ask %d is unresolved", i+1)
		}
		v, err := r.Await(context.Background())
		if (err == nil) != (v == 7) || (err != nil && !errors.Is(err, lockstep.ErrStopped)) {
			t.Errorf("ask %d: Await = %d, %v, want 7 or ErrStopped", i+1, v, err)
		}
	}
	for ask, count := range seen {
		if count != 1 {
			t.Errorf("%d lockstep.reply records for %s, want 1", count, ask)
		}
	}
	if len(seen) != n {
		t.Errorf("%d asks have a lockstep.reply record, want %d", len(seen), n)
	}
}
