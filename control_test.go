// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// pauseMsg is message n of sender from in TestPauseUserProduction.
type pauseMsg struct{ from, n int }

// pauseResult is what pauser reports after its last message.
type pauseResult struct {
	whilePaused int // messages Receive got while the incarnation was paused
	outOfOrder  int // messages that did not follow their sender's previous one
}

// pauser pauses its user delivery in Started and after every every-th message, and resumes it in
// Signal. It reports each pause on paused, and its result on done after total messages. Its
// fields other than the channels are turn-owned.
type pauser struct {
	total, every int
	paused       chan struct{} // capacity 1: a resumer waits on it
	done         chan pauseResult

	isPaused bool
	received int
	last     map[int]int
	res      pauseResult
}

func (p *pauser) pause(ctx *Context[pauseMsg]) {
	hooks.Control(ctx).PauseUser(true)
	p.isPaused = true
	select {
	case p.paused <- struct{}{}:
	default:
	}
}

func (p *pauser) Started(ctx *Context[pauseMsg]) {
	p.last = map[int]int{}
	p.pause(ctx)
}

func (p *pauser) Receive(ctx *Context[pauseMsg], m pauseMsg) {
	if p.isPaused {
		p.res.whilePaused++
	}
	if m.n != p.last[m.from]+1 {
		p.res.outOfOrder++
	}
	p.last[m.from] = m.n
	p.received++
	if p.received == p.total {
		p.done <- p.res
		return
	}
	if p.received%p.every == 0 {
		p.pause(ctx)
	}
}

func (p *pauser) Signal(ctx *Context[pauseMsg], _ Signal) {
	hooks.Control(ctx).PauseUser(false)
	p.isPaused = false
}

// MBX-190, PER-250 on the production engine of New: senders on several goroutines fill the
// mailbox of an actor that pauses its user delivery again and again, while a resumer resumes it
// through hooks.Signal after every pause, and the actor's turns move between workers. No message
// is handled while paused, each sender's messages arrive in order, and all of them arrive. Run it
// under -race: the paused flag is read and written only in the actor's turns.
func TestPauseUserProduction(t *testing.T) {
	const senders, per, every = 4, 500, 7
	sys := newLocalSystem(t, WithWorkers(4))
	p := &pauser{total: senders * per, every: every, paused: make(chan struct{}, 1), done: make(chan pauseResult, 1)}
	pid, err := Spawn(sys, "pauser", actorOf[pauseMsg](p), WithMailbox(senders*per, FailSend))
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	defer wg.Wait()
	defer close(stop)
	wg.Add(1)
	go func() { // the resumer
		defer wg.Done()
		for {
			select {
			case <-p.paused:
				if !hooks.Signal(sys, pid.addr.engine(), Terminated{}) {
					t.Error("hooks.Signal to the live pauser reported false")
					return
				}
			case <-stop:
				return
			}
		}
	}()
	errs := make(chan error, senders)
	for from := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 1; n <= per; n++ {
				if err := Send(sys, pid, pauseMsg{from: from, n: n}); err != nil {
					errs <- fmt.Errorf("sender %d, message %d: %w", from, n, err)
					return
				}
			}
		}()
	}

	select {
	case res := <-p.done:
		if res != (pauseResult{}) {
			t.Errorf("%d messages handled while paused, %d out of order; want 0 and 0", res.whilePaused, res.outOfOrder)
		}
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(30 * time.Second):
		t.Fatal("the pauser did not handle every message within 30s")
	}
}

// PER §4.3: the misuse panics of hooks.Control, hooks.Signal and ActorControl.
func TestControlMisuse(t *testing.T) {
	sys := newLocalSystem(t, WithWorkers(1))
	pid, _ := spawnProbe[any](t, sys, "probe", nil)

	if got, want := mustPanic(t, func() { hooks.Control(3) }), "lockstep: hooks.Control: ctx has type int, want a *lockstep.Context"; got != want {
		t.Errorf("Control(3) panicked with %v, want %q", got, want)
	}
	if got, want := mustPanic(t, func() { hooks.Control((*Context[int])(nil)) }), "lockstep: nil Context"; got != want {
		t.Errorf("Control(nil *Context) panicked with %v, want %q", got, want)
	}
	if got, want := mustPanic(t, func() { hooks.Signal(sys, pid.addr.engine(), "x") }), "lockstep: hooks.Signal: sig has type string, want a lockstep.Signal"; got != want {
		t.Errorf("Signal of a string panicked with %v, want %q", got, want)
	}
	if got, want := mustPanic(t, func() { hooks.Signal(3, pid.addr.engine(), Terminated{}) }), "lockstep: hooks.Signal: sys has type int, want a non-nil *lockstep.System"; got != want {
		t.Errorf("Signal on an int panicked with %v, want %q", got, want)
	}
	if hooks.Signal(sys, engine.Address{}, Terminated{}) || hooks.Signal(sys, sys.user.engine(), Terminated{}) {
		t.Error("hooks.Signal to the zero address or the user guardian reported true")
	}

	var kept *Context[any]
	v := inProbe(t, sys, pid, func(ctx *Context[any]) any {
		kept = ctx
		ctl := hooks.Control(ctx)
		var got []any
		for _, f := range []func(){
			func() { ctl.StartInternal(hooks.InternalEffect{Name: "x"}) },
			func() { ctl.Record("lockstep.persist.test", "a") },
		} {
			got = append(got, recoverOf(f))
		}
		return got
	})
	panics, _ := v.([]any)
	want := []any{"lockstep: StartInternal: not implemented", `lockstep: ActorControl.Record("lockstep.persist.test"): odd number of key-value arguments`}
	if fmt.Sprint(panics) != fmt.Sprint(want) {
		t.Errorf("in a turn: panics %q, want %q", panics, want)
	}
	ctl := hooks.Control(kept)
	if got, want := mustPanic(t, func() { ctl.PauseUser(true) }), "lockstep: Context of "+pid.String()+" used outside its turn"; got != want {
		t.Errorf("PauseUser outside a turn panicked with %v, want %q", got, want)
	}
	if ctl.Address() != pid.String() || ctl.TraceEnabled() != sys.eng.Tracer.Enabled() {
		t.Errorf("outside a turn: Address() = %q, TraceEnabled() = %v", ctl.Address(), ctl.TraceEnabled())
	}
}

// recoverOf calls f and returns what it panicked with, or nil.
func recoverOf(f func()) (v any) {
	defer func() { v = recover() }()
	f()
	return nil
}

// PER §4.3: the closed-queue branch of hooks.Signal. The final stop removes the actor from the
// registry before it closes the queues (SUP-060 steps 4 and 5), so hooks.Signal reaches a closed
// cell only when it resolved the cell just before that removal, in a race with the stop; no
// deterministic schedule produces it. The test checks the branch on the cell itself: after the
// final stop, offerSystem takes nothing and reports false, and hooks.Signal reports false too.
func TestHooksSignalClosedCell(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, _ := spawnProbe[any](t, sys, "gone", nil)
	eng.RunUntilIdle()
	c := cellOf(t, sys, pid)
	if err := Stop(sys, pid.Address()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	eng.RunUntilIdle()
	if st, _ := c.lifecycle(); st != supervise.Stopped {
		t.Fatalf("the probe is %v after its stop, want stopped", st)
	}

	if hooks.Signal(sys, pid.addr.engine(), Terminated{}) {
		t.Error("hooks.Signal to the stopped probe reported true")
	}
	if c.offerSystem(signalMsg{sig: Terminated{}, direct: true}, nil) {
		t.Error("offerSystem on the closed cell reported true")
	}
	if n := c.sysq.Len(); n != 0 {
		t.Errorf("the closed system queue holds %d messages, want 0", n)
	}
}
