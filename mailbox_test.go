// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"slices"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/mailbox"
)

// spawnInt spawns an idle actor named name on sys with opts and returns its cell.
func spawnInt(t *testing.T, sys *System, name string, opts ...SpawnOption) *cell[int] {
	t.Helper()
	pid, err := Spawn(sys, name, idle[int](), opts...)
	if err != nil {
		t.Fatalf("Spawn %q: %v", name, err)
	}
	return cellOf(t, sys, pid)
}

// AT-MBX-01: MBX-010. The capacity and policy of a cell's mailbox come from its WithMailbox, else
// the system's WithDefaultMailbox, else 1024 and FailSend; the last option of each kind wins.
func TestATMBX01Defaults(t *testing.T) {
	check := func(t *testing.T, c *cell[int], capacity int, policy mailbox.Policy) {
		t.Helper()
		if got := c.user.Capacity(); got != capacity {
			t.Errorf("Capacity() = %d, want %d", got, capacity)
		}
		if got := c.user.Policy(); got != policy {
			t.Errorf("Policy() = %v, want %v", got, policy)
		}
	}

	t.Run("defaults", func(t *testing.T) {
		sys, _ := newManualSystem(t)
		check(t, spawnInt(t, sys, "a"), 1024, mailbox.FailSend)
		check(t, spawnInt(t, sys, "b", WithMailbox(8, DropOldest)), 8, mailbox.DropOldest)
	})
	t.Run("local system", func(t *testing.T) {
		sys := newLocalSystem(t)
		check(t, spawnInt(t, sys, "a"), mailbox.DefaultCapacity, mailbox.FailSend)
		check(t, spawnInt(t, sys, "b", WithMailbox(8, DropOldest)), 8, mailbox.DropOldest)
	})
	t.Run("WithDefaultMailbox", func(t *testing.T) {
		sys := newLocalSystem(t, WithDefaultMailbox(4, DropNewest))
		check(t, spawnInt(t, sys, "a"), 4, mailbox.DropNewest)
		check(t, spawnInt(t, sys, "b", WithMailbox(2, BlockSender)), 2, mailbox.BlockSender)
	})
	t.Run("the last option of each kind wins", func(t *testing.T) {
		sys := newLocalSystem(t,
			WithDefaultMailbox(2, FailSend), WithDefaultMailbox(4, DropNewest))
		check(t, spawnInt(t, sys, "a"), 4, mailbox.DropNewest)
		check(t, spawnInt(t, sys, "b", WithMailbox(2, FailSend), WithMailbox(8, DropOldest)),
			8, mailbox.DropOldest)
	})
	t.Run("options on the manual and recording systems", func(t *testing.T) {
		sys, _ := newManualSystem(t, WithDefaultMailbox(5, DropOldest))
		check(t, spawnInt(t, sys, "a"), 5, mailbox.DropOldest)
		sys, _ = newRecordingSystem(t, WithDefaultMailbox(6, DropNewest))
		check(t, spawnInt(t, sys, "a"), 6, mailbox.DropNewest)
	})
}

// AT-MBX-03: MBX-020. The option constructors never panic; the invalid value is reported where
// the option is applied, with the texts of MBX §7.
func TestATMBX03OptionValidation(t *testing.T) {
	var spawnOpts [2]SpawnOption
	var sysOpt Option
	mustNotPanic(t, func() {
		spawnOpts[0] = WithMailbox(0, FailSend)
		spawnOpts[1] = WithMailbox(1, MailboxPolicy(9))
		sysOpt = WithDefaultMailbox(-1, FailSend)
	})

	sys, eng := newManualSystem(t)
	for i, want := range []string{
		"lockstep: invalid option: mailbox capacity must be at least 1, got 0",
		"lockstep: invalid option: unknown mailbox policy MailboxPolicy(9)",
	} {
		name := "x" + string(rune('0'+i))
		pid, err := Spawn(sys, name, idle[int](), spawnOpts[i])
		if !pid.IsZero() || !errors.Is(err, ErrInvalidOption) || err == nil || err.Error() != want {
			t.Errorf("Spawn with option %d = %v, %v; want the zero PID and %q", i, pid, err, want)
		}
		if _, _, ok := sys.reg.Lookup("/user/" + name); ok {
			t.Errorf("Spawn with option %d registered /user/%s", i, name)
		}
	}
	if n := len(recordsOf(eng.Records(), "lockstep.spawn")); n != 2 {
		t.Errorf("%d lockstep.spawn records after two refused spawns, want 2", n)
	}

	// An invalid capacity is reported before an unknown policy. The first invalid option in
	// argument order is reported even when a later valid WithMailbox follows (ACT-300 step 3a).
	pid, err := Spawn(sys, "z", idle[int](), WithMailbox(-1, FailSend), WithMailbox(3, FailSend))
	if want := "lockstep: invalid option: mailbox capacity must be at least 1, got -1"; !pid.IsZero() || err == nil || err.Error() != want {
		t.Errorf("Spawn with a bad then a good WithMailbox = %v, %v; want the zero PID and %q", pid, err, want)
	}
	if _, _, ok := sys.reg.Lookup("/user/z"); ok {
		t.Error("Spawn with a bad then a good WithMailbox registered /user/z")
	}
	v0 := mustPanic(t, func() { New(WithDefaultMailbox(0, FailSend), WithDefaultMailbox(4, DropNewest)) })
	if err, _ := v0.(error); err == nil || !errors.Is(err, ErrInvalidOption) ||
		err.Error() != "lockstep: invalid option: mailbox capacity must be at least 1, got 0" {
		t.Errorf("New(bad, good WithDefaultMailbox) panicked with %#v, want the first one's error", v0)
	}
	_, err = Spawn(sys, "y", idle[int](), WithMailbox(-3, MailboxPolicy(9)))
	if want := "lockstep: invalid option: mailbox capacity must be at least 1, got -3"; err == nil || err.Error() != want {
		t.Errorf("Spawn with capacity -3 and policy 9 = %v, want %q", err, want)
	}

	v := mustPanic(t, func() { New(sysOpt) })
	err, ok := v.(error)
	if !ok || !errors.Is(err, ErrInvalidOption) ||
		err.Error() != "lockstep: invalid option: mailbox capacity must be at least 1, got -1" {
		t.Errorf("New(WithDefaultMailbox(-1, FailSend)) panicked with %#v", v)
	}
	v = mustPanic(t, func() { New(WithDefaultMailbox(1, MailboxPolicy(4))) })
	if err, ok := v.(error); !ok || !errors.Is(err, ErrInvalidOption) ||
		err.Error() != "lockstep: invalid option: unknown mailbox policy MailboxPolicy(4)" {
		t.Errorf("New(WithDefaultMailbox(1, 4)) panicked with %#v", v)
	}

	// The first invalid setting in argument order is the one reported (ACT-010).
	v = mustPanic(t, func() { New(WithDefaultMailbox(0, FailSend), WithNodeName("-bad")) })
	if err, _ := v.(error); err == nil || err.Error() != "lockstep: invalid option: mailbox capacity must be at least 1, got 0" {
		t.Errorf("New(bad mailbox, bad node) panicked with %#v, want the mailbox error", v)
	}
	v = mustPanic(t, func() { New(WithNodeName("-bad"), WithDefaultMailbox(0, FailSend)) })
	if err, _ := v.(error); err == nil || !errors.Is(err, ErrInvalidOption) ||
		err.Error() == "lockstep: invalid option: mailbox capacity must be at least 1, got 0" {
		t.Errorf("New(bad node, bad mailbox) panicked with %#v, want the node error", v)
	}

	// ACT-015: the constructor behind hooks.NewSystem returns the error and never panics, and a
	// failed call leaves the engine's admission unbound.
	var built hooks.Built
	mustNotPanic(t, func() {
		built, err = hooks.NewSystem(enginetest.New("n1", 1).Engine(), []Option{WithDefaultMailbox(0, FailSend)})
	})
	if want := "lockstep: invalid option: mailbox capacity must be at least 1, got 0"; !errors.Is(err, ErrInvalidOption) ||
		err == nil || err.Error() != want || built.System != nil {
		t.Errorf("hooks.NewSystem with WithDefaultMailbox(0, FailSend) = %v, %v; want %q", built, err, want)
	}

	for p, want := range [...]string{"fail-send", "drop-newest", "drop-oldest", "block-sender"} {
		if got := MailboxPolicy(p).String(); got != want {
			t.Errorf("MailboxPolicy(%d).String() = %q, want %q", p, got, want)
		}
	}
	if FailSend.String() != "fail-send" || DropNewest.String() != "drop-newest" ||
		DropOldest.String() != "drop-oldest" || BlockSender.String() != "block-sender" {
		t.Error("a policy constant has the wrong String")
	}
	if got := MailboxPolicy(9).String(); got != "MailboxPolicy(9)" {
		t.Errorf("MailboxPolicy(9).String() = %q, want %q", got, "MailboxPolicy(9)")
	}
}

// mustNotPanic runs f and fails the test if it panics.
func mustNotPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if v := recover(); v != nil {
			t.Fatalf("panicked with %v", v)
		}
	}()
	f()
}

// MBX §10, "hold": a held cell's mailbox fills, its turns process nothing, and the release lets the
// next run deliver every message in order, including the begin that was queued before the hold.
func TestHoldDefersTurns(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got []int
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(_ *Context[int], m int) { got = append(got, m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	// Held before its first turn: even begin waits.
	release := hold(t, sys, pid.Address())
	for m := 1; m <= 3; m++ {
		send(t, sys, pid, m)
	}
	eng.RunUntilIdle()
	c := cellOf(t, sys, pid)
	if c.state != cellStarting || len(got) != 0 || c.user.Len() != 3 {
		t.Fatalf("held: state %v, received %v, queued %d; want starting, none, 3", c.state, got, c.user.Len())
	}
	if c.RunTurn(1) {
		t.Error("RunTurn of a held cell returned true")
	}

	release()
	release() // a second release changes nothing
	eng.RunUntilIdle()
	if want := []int{1, 2, 3}; !slices.Equal(got, want) || c.user.Len() != 0 {
		t.Errorf("after the release: received %v, queued %d; want %v, 0", got, c.user.Len(), want)
	}

	// Held again after the start: further messages wait, and the release schedules the cell once.
	release = hold(t, sys, pid.Address())
	send(t, sys, pid, 4)
	send(t, sys, pid, 5)
	eng.RunUntilIdle()
	if want := []int{1, 2, 3}; !slices.Equal(got, want) || c.user.Len() != 2 {
		t.Errorf("held again: received %v, queued %d; want %v, 2", got, c.user.Len(), want)
	}
	release()
	eng.RunUntilIdle()
	if want := []int{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Errorf("after the second release: received %v, want %v", got, want)
	}

	// A release with nothing to do schedules nothing.
	release = hold(t, sys, pid.Address())
	release()
	eng.RunUntilIdle()
	if want := []int{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Errorf("after an idle hold: received %v, want %v", got, want)
	}
}

// The Hold of the external tests holds the same cell as hold, and refuses an address nothing is
// registered at.
func TestHoldExported(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got []int
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		receive: func(_ *Context[int], m int) { got = append(got, m) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	release := Hold(sys, pid.Address())
	send(t, sys, pid, 1)
	eng.RunUntilIdle()
	if len(got) != 0 {
		t.Fatalf("a held cell received %v", got)
	}
	release()
	eng.RunUntilIdle()
	if want := []int{1}; !slices.Equal(got, want) {
		t.Errorf("after the release: received %v, want %v", got, want)
	}

	v := mustPanic(t, func() { Hold(sys, Address{node: "local", path: "/user/none", uid: 7}) })
	if want := "lockstep: Hold: lockstep://local/user/none#7 is not a registered actor"; v != want {
		t.Errorf("Hold of an unknown address panicked with %v, want %q", v, want)
	}
}
