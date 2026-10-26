// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// The tests below drive ActorControl.StartInternal (PER-240). The manual engine never calls Run,
// so its tests complete an effect with CompleteEffect or time it out with Advance; the local
// engine runs Run on its effect workers.

// internalName is the name of the internal effects of these tests.
const internalName = "test.internal"

// iMsg is the user message of the internal effect tests.
type iMsg struct{ n int }

// doneCall is one call of an internal effect's Done.
type doneCall struct {
	ctx      any
	v        any
	err      error
	received int // user messages Receive had handled when Done ran
}

// internalActor starts an internal effect from its Started or Stopped hook, or from onUser, and
// records what its Done and Receive see. Its fields are written in its turns; a test on the local
// engine reads Done's calls from doneCh instead.
type internalActor struct {
	pause   bool                               // Started pauses user delivery
	inStart bool                               // Started starts the effect
	inStop  bool                               // Stopped starts the effect
	resume  bool                               // Done resumes user delivery
	run     func(context.Context) (any, error) // the effect's Run; nil returns (nil, nil)
	onUser  func(*Context[iMsg], iMsg)         // runs in Receive, after got
	doneCh  chan doneCall                      // if set, gets every Done call

	ctx    *Context[iMsg] // the Context of the incarnation
	cancel func()         // StartInternal's result
	got    []int          // the messages Receive handled
	dones  []doneCall     // Done's calls
}

func (a *internalActor) effect() hooks.InternalEffect {
	run := a.run
	if run == nil {
		run = func(context.Context) (any, error) { return nil, nil }
	}
	return hooks.InternalEffect{
		Name: internalName,
		Run:  run,
		Done: func(ctx any, v any, err error) {
			d := doneCall{ctx: ctx, v: v, err: err, received: len(a.got)}
			a.dones = append(a.dones, d)
			if a.doneCh != nil {
				a.doneCh <- d
			}
			if a.resume {
				hooks.Control(ctx).PauseUser(false)
			}
		},
	}
}

func (a *internalActor) Started(ctx *Context[iMsg]) {
	a.ctx = ctx
	if a.pause {
		hooks.Control(ctx).PauseUser(true)
	}
	if a.inStart {
		a.cancel = hooks.Control(ctx).StartInternal(a.effect())
	}
}

func (a *internalActor) Receive(ctx *Context[iMsg], m iMsg) {
	a.got = append(a.got, m.n)
	if a.onUser != nil {
		a.onUser(ctx, m)
	}
}

func (a *internalActor) Stopped(ctx *Context[iMsg], _ StopReason) {
	if a.inStop {
		a.cancel = hooks.Control(ctx).StartInternal(a.effect())
	}
}

// manualInternal returns a manual system with a spawned at /user/a and run until idle.
func manualInternal(t *testing.T, a *internalActor, opts ...SpawnOption) (*System, *enginetest.Engine, PID[iMsg]) {
	t.Helper()
	sys, eng := newManualSystem(t)
	pid, err := Spawn(sys, "a", actorOf[iMsg](a), opts...)
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	return sys, eng, pid
}

// onlyPending returns the one pending effect of eng, failing otherwise.
func onlyPending(t *testing.T, eng *enginetest.Engine) engine.EffectRequest {
	t.Helper()
	p := eng.Pending()
	if len(p) != 1 {
		t.Fatalf("%d pending effects, want 1", len(p))
	}
	return p[0]
}

// intEffect is the Do effect of the shared ID check; the manual engine never runs it.
type intEffect struct{}

func (intEffect) Name() string                     { return "test.do" }
func (intEffect) Run(context.Context) (int, error) { return 0, nil }

// PER-240, AT-PER-25: an internal effect behaves as Do except that its completion is a system
// message: it runs Done in a later turn, with that turn's Context, while user delivery is paused
// and while the mailbox is full; a resume from Done hands the actor its queued messages. It times
// out with ErrTimeout, a panic in Run reaches Done as the engine's error, it shares Do's ID
// counter, its cancel cancels it as CancelEffect does, and it is refused while the incarnation
// stops (EFF-030).
func TestPER240InternalEffectLocal(t *testing.T) {
	t.Run("paused and full", func(t *testing.T) {
		a := &internalActor{pause: true, inStart: true, resume: true}
		sys, eng, pid := manualInternal(t, a, WithMailbox(1, FailSend))
		req := onlyPending(t, eng)
		if ie, ok := req.Value.(hooks.InternalEffect); !ok || req.Name != internalName || ie.Name != internalName || ie.Inline {
			t.Fatalf("pending request %+v, want the internal effect %q", req, internalName)
		}
		send(t, sys, pid, iMsg{1})
		if err := Send(sys, pid, iMsg{2}); !errors.Is(err, ErrMailboxFull) {
			t.Fatalf("Send 2: %v, want ErrMailboxFull", err)
		}
		eng.RunUntilIdle()
		if len(a.got) != 0 || len(a.dones) != 0 {
			t.Fatalf("before the completion: Receive got %v, Done ran %d times; want nothing", a.got, len(a.dones))
		}

		if !eng.CompleteEffect(req.ID, 42, nil) {
			t.Fatal("CompleteEffect reported false")
		}
		eng.RunUntilIdle()
		if len(a.dones) != 1 {
			t.Fatalf("Done ran %d times, want 1", len(a.dones))
		}
		d := a.dones[0]
		if ctx, ok := d.ctx.(*Context[iMsg]); !ok || ctx != a.ctx {
			t.Errorf("Done's ctx is %T %p, want the incarnation's *Context[iMsg] %p", d.ctx, d.ctx, a.ctx)
		}
		if d.v != 42 || d.err != nil || d.received != 0 {
			t.Errorf("Done got (%v, %v) after %d messages, want (42, nil) after 0", d.v, d.err, d.received)
		}
		if !slices.Equal(a.got, []int{1}) {
			t.Errorf("after the resume in Done, Receive got %v, want [1]", a.got)
		}
		dones := effectDones(eng.Records())
		if len(dones) != 1 || dones[0]["effect"] != "1" || dones[0]["name"] != internalName || dones[0]["outcome"] != "ok" {
			t.Errorf("lockstep.effect.done records %v, want one for effect 1 %q with outcome ok", dones, internalName)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		a := &internalActor{inStart: true}
		_, eng, _ := manualInternal(t, a)
		req := onlyPending(t, eng) // the manual engine never runs it: Run outlives any timeout
		eng.Advance(DefaultEffectTimeout)
		if len(a.dones) != 1 || a.dones[0].v != nil || !errors.Is(a.dones[0].err, ErrTimeout) {
			t.Fatalf("Done calls %+v, want one with (nil, ErrTimeout)", a.dones)
		}
		if len(eng.Pending()) != 0 || eng.CompleteEffect(req.ID, 1, nil) {
			t.Error("the timed-out effect is still pending in the engine")
		}
		eng.RunUntilIdle()
		if len(a.dones) != 1 {
			t.Errorf("Done ran %d times, want 1", len(a.dones))
		}
		if dones := effectDones(eng.Records()); len(dones) != 1 || dones[0]["outcome"] != "timeout" {
			t.Errorf("lockstep.effect.done records %v, want one with outcome timeout", dones)
		}
	})

	t.Run("run panics", func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		a := &internalActor{
			inStart: true,
			run:     func(context.Context) (any, error) { panic("disk on fire") },
			doneCh:  make(chan doneCall, 1),
		}
		if _, err := Spawn(sys, "a", actorOf[iMsg](a)); err != nil {
			t.Fatal(err)
		}
		var d doneCall
		select {
		case d = <-a.doneCh:
		case <-time.After(10 * time.Second):
			t.Fatal("Done did not run within 10s")
		}
		want := `lockstep: effect "test.internal" panicked: disk on fire`
		if _, ok := d.ctx.(*Context[iMsg]); !ok || d.v != nil || d.err == nil || d.err.Error() != want {
			t.Fatalf("Done got (%T, %v, %v), want (*Context[iMsg], nil, %q)", d.ctx, d.v, d.err, want)
		}
		if dones := effectDones(rec.Records()); len(dones) != 1 || dones[0]["outcome"] != "error" || dones[0]["err"] != want {
			t.Errorf("lockstep.effect.done records %v, want one with outcome error and err %q", dones, want)
		}
	})

	t.Run("shared ids", func(t *testing.T) {
		a := &internalActor{}
		a.onUser = func(ctx *Context[iMsg], _ iMsg) {
			Do(ctx, intEffect{}, func(int, error) iMsg { return iMsg{} })
			a.cancel = hooks.Control(ctx).StartInternal(a.effect())
			Do(ctx, intEffect{}, func(int, error) iMsg { return iMsg{} })
		}
		sys, eng, pid := manualInternal(t, a)
		send(t, sys, pid, iMsg{1})
		eng.RunUntilIdle()
		var ids []uint64
		var names []string
		for _, r := range eng.Pending() {
			ids, names = append(ids, r.ID), append(names, r.Name)
		}
		if !slices.Equal(ids, []uint64{1, 2, 3}) || !slices.Equal(names, []string{"test.do", internalName, "test.do"}) {
			t.Errorf("pending effects %v %q, want IDs [1 2 3] for test.do, %s, test.do", ids, names, internalName)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		a := &internalActor{inStart: true}
		a.onUser = func(*Context[iMsg], iMsg) { a.cancel(); a.cancel() }
		sys, eng, pid := manualInternal(t, a)
		req := onlyPending(t, eng)
		send(t, sys, pid, iMsg{1})
		eng.RunUntilIdle()
		if len(eng.Pending()) != 0 || eng.CompleteEffect(req.ID, 1, nil) {
			t.Error("the cancelled effect is still pending in the engine")
		}
		eng.Advance(2 * DefaultEffectTimeout)
		if len(a.dones) != 0 {
			t.Errorf("Done ran %d times for a cancelled effect, want 0", len(a.dones))
		}
		if dones := effectDones(eng.Records()); len(dones) != 1 || dones[0]["outcome"] != "cancelled" || dones[0]["name"] != internalName {
			t.Errorf("lockstep.effect.done records %v, want one cancelled for %q", dones, internalName)
		}
	})

	t.Run("stopping", func(t *testing.T) {
		a := &internalActor{inStop: true}
		sys, eng, pid := manualInternal(t, a)
		if err := Stop(sys, pid.Address()); err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		if a.cancel == nil {
			t.Fatal("StartInternal from Stopped returned a nil cancel")
		}
		a.cancel() // a no-op
		if len(eng.Pending()) != 0 || len(a.dones) != 0 {
			t.Errorf("StartInternal from Stopped started an effect (%d pending, %d Done calls)", len(eng.Pending()), len(a.dones))
		}
		if dones := effectDones(eng.Records()); len(dones) != 1 || dones[0]["effect"] != "0" || dones[0]["outcome"] != "refused" || dones[0]["name"] != internalName {
			t.Errorf("lockstep.effect.done records %v, want one refused for effect 0 %q", dones, internalName)
		}
	})
}

// parentOf is a supervisor whose Started spawns child at c and whose deciding function returns
// directive, after appending each failure to decided.
type parentOf struct {
	child     *internalActor
	directive Directive
	decided   []error
	pid       PID[iMsg] // the child's PID
}

func (p *parentOf) Started(ctx *Context[iMsg]) {
	pid, err := Spawn(ctx, "c", actorOf[iMsg](p.child))
	if err != nil {
		panic(err)
	}
	p.pid = pid
}

func (*parentOf) Receive(*Context[iMsg], iMsg) {}

// PER-240, PER-160, SUP-160: an internal effect's outcome that reaches its incarnation while it
// is Suspended waits: after a resume, Done runs once in the same incarnation; after a restart, the
// outcome is dropped and Done never runs.
func TestPER240InternalEffectSuspended(t *testing.T) {
	for _, tc := range []struct {
		name      string
		directive Directive
		dones     int
		starts    int
	}{
		{"resume", DirectiveResume, 1, 1},
		{"restart", DirectiveRestart, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sys, eng := newManualSystem(t)
			starts := 0
			child := &internalActor{inStart: true}
			p := &parentOf{child: child, directive: tc.directive}
			strategy := OneForOne(func(err error) Directive {
				p.decided = append(p.decided, err)
				return p.directive
			})
			if _, err := Spawn(sys, "p", actorOf[iMsg](p), WithStrategy(strategy)); err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle()
			req := onlyPending(t, eng)
			first := child.ctx
			if first == nil {
				t.Fatal("the child did not start")
			}
			starts++

			// The child fails; its turn suspends it and queues the parent's decision.
			if err := hooks.InjectFail(sys, p.pid.Address().engine(), errors.New("boom")); err != nil {
				t.Fatal(err)
			}
			if !eng.Step() {
				t.Fatal("no turn ran for the injected failure")
			}
			// The outcome reaches the Suspended child before the parent decides.
			if !eng.CompleteEffect(req.ID, 42, nil) {
				t.Fatal("CompleteEffect reported false")
			}
			eng.RunUntilIdle()
			eng.Advance(time.Second) // past the restart's backoff

			if len(p.decided) != 1 {
				t.Fatalf("the parent decided %d times, want 1", len(p.decided))
			}
			if child.ctx != first {
				starts++
			}
			if len(child.dones) != tc.dones || starts != tc.starts {
				t.Fatalf("Done ran %d times with %d incarnations, want %d and %d", len(child.dones), starts, tc.dones, tc.starts)
			}
			if tc.dones == 1 {
				d := child.dones[0]
				if d.ctx != any(first) || d.v != 42 || d.err != nil {
					t.Errorf("Done got (%p, %v, %v), want the first incarnation's Context, 42 and nil", d.ctx, d.v, d.err)
				}
				return
			}
			var drops []map[string]string
			for _, r := range effectRecords(eng.Records(), "lockstep.drop") {
				if r["to"] == p.pid.String() {
					drops = append(drops, r)
				}
			}
			if len(drops) != 1 || drops[0]["reason"] != "incarnation-ended" || drops[0]["inc"] != "1" || drops[0]["msg"] != "42" {
				t.Errorf("lockstep.drop records of the child %v, want one incarnation-ended for inc 1 with msg 42", drops)
			}
		})
	}
}
