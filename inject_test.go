// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/hooks"
)

// SFT-040: an inject command processed in Running fails the incarnation with source injected, as
// SUP-150 says for a failure in Receive, and consumes no user message: the message queued before
// the turn reaches the next incarnation.
func TestSFT040InjectInRunning(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got []int
	pid, _ := spawnProbe(t, sys, "p", func(_ *Context[int], m int) { got = append(got, m) })
	eng.RunUntilIdle()

	send(t, sys, pid, 7)
	errInjected := errors.New("injected")
	if err := hooks.InjectFail(sys, pid.Address().engine(), errInjected); err != nil {
		t.Fatalf("InjectFail = %v, want nil", err)
	}
	eng.RunUntilIdle()
	eng.Advance(time.Second) // the restart's backoff
	eng.RunUntilIdle()

	fails := recordsOf(eng.Records(), "lockstep.fail")
	if len(fails) != 1 {
		t.Fatalf("%d lockstep.fail records, want 1", len(fails))
	}
	if attrOf(fails[0], "source") != "injected" || attrOf(fails[0], "err") != "injected" ||
		attrOf(fails[0], "actor") != pid.Address().String() || attrOf(fails[0], "inc") != "1" {
		t.Errorf("lockstep.fail %v, want source=injected err=injected inc=1 of %s", fails[0], pid.Address())
	}
	if !slices.Equal(got, []int{7}) {
		t.Errorf("the actor received %v, want [7] delivered after the restart", got)
	}
	starts := recordsOf(eng.Records(), "lockstep.start")
	if len(starts) != 2 {
		t.Errorf("%d lockstep.start records, want 2: one restart", len(starts))
	}
}

// SFT-040: an inject command processed in any state but Running is discarded with lockstep.drop
// reason not-running and no failure.
func TestSFT040InjectWhileSuspended(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, _ := spawnProbe[int](t, sys, "p", nil)
	eng.RunUntilIdle()

	for range 2 {
		if err := hooks.InjectFail(sys, pid.Address().engine(), errors.New("x")); err != nil {
			t.Fatalf("InjectFail = %v, want nil", err)
		}
	}
	eng.RunUntilIdle()

	if n := len(recordsOf(eng.Records(), "lockstep.fail")); n != 1 {
		t.Errorf("%d lockstep.fail records, want 1", n)
	}
	drops := recordsOf(eng.Records(), "lockstep.drop")
	if len(drops) != 1 {
		t.Fatalf("%d lockstep.drop records, want 1", len(drops))
	}
	if attrOf(drops[0], "to") != pid.Address().String() || attrOf(drops[0], "reason") != "not-running" || attrOf(drops[0], "msg") != "inject" {
		t.Errorf("lockstep.drop %v, want to=%s reason=not-running msg=inject", drops[0], pid.Address())
	}
}

// SFT-030: hooks.InjectFail returns an error wrapping ErrNotFound for an address that names no live
// actor of the system, a guardian, the zero address and another node's address included, and
// panics for a nil error or a value that is not a system.
func TestSFT030InjectFailHookErrors(t *testing.T) {
	sys, eng := newManualSystem(t)
	pid, _ := spawnProbe[int](t, sys, "p", nil)
	eng.RunUntilIdle()
	if err := Stop(sys, pid.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	for _, addr := range []Address{
		pid.Address(), // stopped
		{},
		sys.user,
		{node: "other", path: "/user/p", uid: 1},
		PIDAt[int](sys.node, "/user/none").Address(),
	} {
		err := hooks.InjectFail(sys, addr.engine(), errors.New("x"))
		if want := "lockstep: not found: " + addr.String(); !errors.Is(err, ErrNotFound) || err.Error() != want {
			t.Errorf("InjectFail(%v) = %v, want ErrNotFound with text %q", addr, err, want)
		}
	}

	live, _ := spawnProbe[int](t, sys, "q", nil)
	eng.RunUntilIdle()
	for _, c := range []struct {
		name string
		fn   func()
	}{
		{"nil error", func() { _ = hooks.InjectFail(sys, live.Address().engine(), nil) }},
		{"not System", func() { _ = hooks.InjectFail("sys", live.Address().engine(), errors.New("x")) }},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", c.name)
				}
			}()
			c.fn()
		}()
	}
}
