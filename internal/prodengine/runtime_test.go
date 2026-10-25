// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/prodengine"
)

// TestATSCH15TimersCancelledByShutdown covers SCH-140 and SCH-210 (AT-SCH-15): the runtime cancels
// an actor's After and Every timers and its pending ask's timeout before it stops the engine, so
// no timer is pending when Shutdown calls the bundle's Stop.
func TestATSCH15TimersCancelledByShutdown(t *testing.T) {
	e := prodengine.New(prodengine.Config{Node: "n", Workers: 2})
	t.Cleanup(e.Stop)
	b := e.Engine()
	var atStop atomic.Int64
	atStop.Store(-1)
	b.Stop = func() {
		atStop.Store(int64(e.PendingTimers())) // before the engine's own Stop stops any timer
		e.Stop()
	}
	built, err := hooks.NewSystem(b, nil)
	if err != nil {
		t.Fatalf("hooks.NewSystem: %v", err)
	}
	sys, ok := built.System.(*lockstep.System)
	if !ok {
		t.Fatalf("hooks.NewSystem built %T, want *lockstep.System", built.System)
	}

	silent, err := lockstep.Spawn(sys, "silent", func() lockstep.Actor[string] { return silentActor{} })
	if err != nil {
		t.Fatal(err)
	}
	armed := make(chan struct{})
	a, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[string] {
		return &armer{silent: silent, armed: armed}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := lockstep.Send(sys, a, "arm"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-armed:
	case <-time.After(10 * time.Second):
		t.Fatal("the actor did not arm its timers within 10 s")
	}
	// After, Every and the ask's timeout.
	if n := e.PendingTimers(); n < 3 {
		t.Fatalf("PendingTimers() = %d before Shutdown, want at least 3", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	if n := atStop.Load(); n != 0 {
		t.Errorf("PendingTimers() = %d when Shutdown called the bundle's Stop, want 0", n)
	}
	if n := e.PendingTimers(); n != 0 {
		t.Errorf("PendingTimers() = %d after Shutdown, want 0", n)
	}
}

// silentActor never answers.
type silentActor struct{}

func (silentActor) Receive(*lockstep.Context[string], string) {}

// armer, on "arm", sets an After and an Every timer of an hour, asks silent with the default
// timeout and then closes armed.
type armer struct {
	silent lockstep.PID[string]
	armed  chan struct{}
}

func (a *armer) Receive(ctx *lockstep.Context[string], msg string) {
	if msg != "arm" {
		return
	}
	ctx.After(time.Hour, "after")
	ctx.Every(time.Hour, "every")
	r := lockstep.Ask(ctx, a.silent, func(lockstep.ReplyTo[int]) string { return "q" })
	lockstep.Pipe(ctx, r, func(_ int, err error) string {
		if errors.Is(err, lockstep.ErrStopped) {
			return "stopped"
		}
		return "answered"
	})
	close(a.armed)
}
