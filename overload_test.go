// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// fillUser spawns n idle actors named f<i> under /user and returns their PIDs.
func fillUser(t *testing.T, sys *System, n int) []PID[any] {
	t.Helper()
	pids := make([]PID[any], 0, n)
	for i := range n {
		pid, err := Spawn(sys, "f"+strconv.Itoa(i), idle[any]())
		if err != nil {
			t.Fatalf("filling spawn %d of %d: %v", i+1, n, err)
		}
		pids = append(pids, pid)
	}
	return pids
}

// spawnUntilRefused spawns idle actors with generated names from parent until a spawn fails, and
// returns how many succeeded and the error. It gives up after 1000 spawns with errNoRefusal.
func spawnUntilRefused(parent Spawner) (int, error) {
	for n := range 1000 {
		if _, err := Spawn(parent, "", idle[any]()); err != nil {
			return n, err
		}
	}
	return 1000, errNoRefusal
}

// errNoRefusal is spawnUntilRefused's error when every spawn succeeded.
var errNoRefusal = errors.New("no spawn refused after 1000")

// capText is OVL-050's text for a spawn under parentPath refused by the cap.
func capText(parentPath string, live, limit, reserve int) string {
	return "lockstep: spawn in " + parentPath + ": actor cap reached (live " + strconv.Itoa(live) +
		", limit " + strconv.Itoa(limit) + ", reserve " + strconv.Itoa(reserve) + ")"
}

// wantCapErr fails the test unless err matches ErrActorCap with OVL-050's text.
func wantCapErr(t *testing.T, what string, err error, parentPath string, live, limit, reserve int) {
	t.Helper()
	if !errors.Is(err, ErrActorCap) {
		t.Errorf("%s: err %v, want ErrActorCap", what, err)
		return
	}
	if want := capText(parentPath, live, limit, reserve); err.Error() != want {
		t.Errorf("%s: text %q, want %q", what, err.Error(), want)
	}
}

// funcActor runs every func(*Context[any]) message in its Receive and calls stopped, if set, from
// its Stopped hook.
type funcActor struct {
	stopped func(ctx *Context[any])
}

func (*funcActor) Receive(ctx *Context[any], msg any) {
	if f, ok := msg.(func(*Context[any])); ok {
		f(ctx)
	}
}

func (a *funcActor) Stopped(ctx *Context[any], _ StopReason) {
	if a.stopped != nil {
		a.stopped(ctx)
	}
}

// run sends f to pid, which must be a funcActor, and runs the manual engine until idle.
func run(t *testing.T, sys *System, eng *enginetest.Engine, pid PID[any], f func(ctx *Context[any])) {
	t.Helper()
	send(t, sys, pid, any(f))
	eng.RunUntilIdle()
}

// AT-OVL-01: OVL-010. An invalid WithMaxActors makes New panic and hooks.NewSystem return the
// error, both wrapping ErrInvalidOption with OVL-010's detail.
func TestATOVL01InvalidMaxActors(t *testing.T) {
	for _, n := range []int{0, -3} {
		detail := "WithMaxActors: n must be at least 1 (got " + strconv.Itoa(n) + ")"
		err := mustPanicError(t, func() { New(WithMaxActors(n)) })
		if !errors.Is(err, ErrInvalidOption) || !strings.HasSuffix(err.Error(), detail) {
			t.Errorf("New(WithMaxActors(%d)) panicked with %q, want ErrInvalidOption ending with %q", n, err, detail)
		}
	}
	_, err := hooks.NewSystem(enginetest.New("n1", 1).Engine(), []Option{WithMaxActors(0)})
	detail := "WithMaxActors: n must be at least 1 (got 0)"
	if !errors.Is(err, ErrInvalidOption) || !strings.HasSuffix(err.Error(), detail) {
		t.Errorf("hooks.NewSystem with WithMaxActors(0): err %v, want ErrInvalidOption ending with %q", err, detail)
	}
}

// OVL-010, OVL-350, OVL §7: every option detail, from New and from hooks.NewSystem; the first
// invalid option in argument order wins; valid values build a system.
func TestOVL010OptionDetails(t *testing.T) {
	for _, tc := range []struct {
		opts   []Option
		detail string
	}{
		{[]Option{WithMaxActors(0)}, "WithMaxActors: n must be at least 1 (got 0)"},
		{[]Option{WithAdmission(1000, 1000)}, "WithAdmission: need 0 < low < high (got low=1000 high=1000)"},
		{[]Option{WithAdmission(5, 0)}, "WithAdmission: need 0 < low < high (got low=0 high=5)"},
		{[]Option{WithAdmission(0, 5)}, "WithAdmission: need 0 < low < high (got low=5 high=0)"},
		{[]Option{WithAdmission(10, 20)}, "WithAdmission: need 0 < low < high (got low=20 high=10)"},
		{[]Option{WithWatchdog(-time.Second)}, "WithWatchdog: d must be >= 0 (got -1s)"},
		{[]Option{WithGoroutineWarning(-1)}, "WithGoroutineWarning: bound must be >= 0 (got -1)"},
		{[]Option{WithWatchdog(-time.Millisecond), WithMaxActors(0)}, "WithWatchdog: d must be >= 0 (got -1ms)"},
		{[]Option{WithMaxActors(-1), WithMaxActors(5)}, "WithMaxActors: n must be at least 1 (got -1)"},
	} {
		want := "lockstep: invalid option: " + tc.detail
		err := mustPanicError(t, func() { New(tc.opts...) })
		if !errors.Is(err, ErrInvalidOption) || err.Error() != want {
			t.Errorf("New panicked with %q, want ErrInvalidOption %q", err, want)
		}
		_, err = hooks.NewSystem(enginetest.New("n1", 1).Engine(), tc.opts)
		if !errors.Is(err, ErrInvalidOption) || err == nil || err.Error() != want {
			t.Errorf("hooks.NewSystem: err %v, want ErrInvalidOption %q", err, want)
		}
	}
	for _, opts := range [][]Option{
		{WithAdmission(0, 0)},
		{WithAdmission(2000, 1000)},
		{WithMaxActors(1)},
		{WithWatchdog(0), WithGoroutineWarning(0), WithMemoryEstimates()},
		{WithWatchdog(time.Millisecond), WithGoroutineWarning(1)},
	} {
		mustNotPanic(t, func() { newLocalSystem(t, opts...) })
		if _, err := hooks.NewSystem(enginetest.New("n1", 1).Engine(), opts); err != nil {
			t.Errorf("hooks.NewSystem with valid options: %v", err)
		}
	}
}

// AT-OVL-02: OVL-010, OVL-030. Limit 10 keeps a reserve of 1: nine normal spawns succeed and the
// tenth is refused. The usage reported to admission counts the live actors (OVL-020).
func TestATOVL02CapWithReserve(t *testing.T) {
	sys, eng := newManualSystem(t, WithMaxActors(10))
	n, err := spawnUntilRefused(sys)
	if n != 9 {
		t.Errorf("%d spawns succeeded, want 9", n)
	}
	wantCapErr(t, "10th spawn", err, "/user", 9, 10, 1)
	if got := eng.Usage(); got != (engine.Usage{Actors: 9}) {
		t.Errorf("Usage() = %+v, want {Actors: 9}", got)
	}
}

// AT-OVL-03: OVL-010. Limits 1 and 150 have reserves 0 and 1.
func TestATOVL03SmallAndLargeLimits(t *testing.T) {
	for _, tc := range []struct{ limit, admitted, reserve int }{{1, 1, 0}, {150, 149, 1}} {
		sys, _ := newManualSystem(t, WithMaxActors(tc.limit))
		n, err := spawnUntilRefused(sys)
		if n != tc.admitted {
			t.Errorf("limit %d: %d spawns succeeded, want %d", tc.limit, n, tc.admitted)
		}
		wantCapErr(t, "limit "+strconv.Itoa(tc.limit), err, "/user", tc.admitted, tc.limit, tc.reserve)
	}
}

// AT-OVL-04: OVL-020. A stopping actor keeps its slot until it is removed from the registry,
// after its Stopped hook has returned.
func TestATOVL04SlotFreedAtRemoval(t *testing.T) {
	sys, eng := newManualSystem(t, WithMaxActors(10))
	fillUser(t, sys, 8)
	var during error
	s, err := Spawn(sys, "s", actorOf[any](&funcActor{stopped: func(*Context[any]) {
		_, during = Spawn(sys, "x", idle[any]())
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if err := Stop(sys, s.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	wantCapErr(t, "spawn from s's Stopped", during, "/user", 9, 10, 1)
	if _, err := Spawn(sys, "x", idle[any]()); err != nil {
		t.Errorf("spawn after s's removal: %v", err)
	}
	if got := sys.liveActors(); got != 9 {
		t.Errorf("live %d, want 9", got)
	}
}

// AT-OVL-05: OVL-020, OVL-160. An in-place restart with the cap full neither takes a slot nor is
// refused.
func TestATOVL05RestartNotRefused(t *testing.T) {
	sys, eng := newManualSystem(t, WithMaxActors(10), WithGuardianStrategy(r0()))
	fill := fillUser(t, sys, 8)
	var obs []string
	c := spawnR(t, sys, "c", &rProbe{obs: &obs})
	if _, err := spawnSystem(sys, "s1", idle[any]()); err != nil { // the tenth, a reserve spawn
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, c, any("panic"))
	eng.RunUntilIdle()
	if !slices.Contains(obs, "started /user/c 2") {
		t.Fatalf("observations %q lack started /user/c 2", obs)
	}
	if i := indexOf(eng.Records(), "lockstep.overload", "event=cap"); i >= 0 {
		t.Errorf("the restart emitted an event=cap record: %v", eng.Records()[i])
	}
	if got := sys.liveActors(); got != 10 {
		t.Errorf("live %d after the restart, want 10", got)
	}
	_, err := Spawn(sys, "n", idle[any]())
	wantCapErr(t, "normal spawn after the restart", err, "/user", 10, 10, 1)

	if err := Stop(sys, fill[0].Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if _, err := spawnSystem(sys, "s2", idle[any]()); err != nil {
		t.Errorf("reserve spawn after one removal: %v", err)
	}
	_, err = spawnSystem(sys, "s3", idle[any]())
	wantCapErr(t, "second reserve spawn after one removal", err, "/system", 10, 10, 1)
}

// AT-OVL-06: OVL-030. A spawn from the Started of incarnation 2 is a reserve spawn; the same spawn
// from Receive is a normal one.
func TestATOVL06RebuildContextUsesReserve(t *testing.T) {
	sys, eng := newManualSystem(t, WithMaxActors(10), WithGuardianStrategy(r0()))
	fillUser(t, sys, 8)
	var fromStarted, fromReceive error
	startedSpawns := 0
	p, err := Spawn(sys, "p", actorOf[any](&fnActor[any]{
		started: func(ctx *Context[any]) {
			if ctx.inc >= 2 {
				startedSpawns++
				_, fromStarted = Spawn(ctx, "k", idle[any]())
			}
		},
		receive: func(ctx *Context[any], msg any) {
			switch msg {
			case "spawn":
				_, fromReceive = Spawn(ctx, "k", idle[any]())
			case "panic":
				panic("panic")
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, p, any("spawn"))
	eng.RunUntilIdle()
	wantCapErr(t, "spawn from Receive", fromReceive, "/user/p", 9, 10, 1)
	send(t, sys, p, any("panic"))
	eng.RunUntilIdle()
	if startedSpawns != 1 || fromStarted != nil {
		t.Errorf("spawn from the restarted Started: %d calls, err %v; want 1 call, admitted", startedSpawns, fromStarted)
	}
	if got := sys.liveActors(); got != 10 {
		t.Errorf("live %d, want 10", got)
	}
}

// AT-OVL-07: OVL-030. The first Started of an actor spawned from a rebuild context is a rebuild
// context too; its later Receive is not.
func TestATOVL07RebuildContextIsInherited(t *testing.T) {
	sys, eng := newManualSystem(t, WithMaxActors(200), WithGuardianStrategy(r0()))
	fillUser(t, sys, 197)
	var errC, errG, fromReceive error
	var c PID[any]
	child := &fnActor[any]{
		started: func(ctx *Context[any]) { _, errG = Spawn(ctx, "g", idle[any]()) },
		receive: func(ctx *Context[any], _ any) { _, fromReceive = Spawn(ctx, "h", idle[any]()) },
	}
	p, err := Spawn(sys, "p", actorOf[any](&fnActor[any]{
		started: func(ctx *Context[any]) {
			if ctx.inc >= 2 {
				c, errC = Spawn(ctx, "c", actorOf[any](child))
			}
		},
		receive: func(*Context[any], any) { panic("panic") },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, p, any("panic"))
	eng.RunUntilIdle()
	if errC != nil || errG != nil {
		t.Fatalf("spawns of the rebuild: C %v, G %v; want both admitted", errC, errG)
	}
	if got := sys.liveActors(); got != 200 {
		t.Errorf("live %d, want 200", got)
	}
	send(t, sys, c, any("spawn"))
	eng.RunUntilIdle()
	wantCapErr(t, "spawn from C's Receive", fromReceive, "/user/p/c", 200, 200, 2)
}

// AT-OVL-08: OVL-030. A system actor is a reserve spawn.
func TestATOVL08SystemActorUsesReserve(t *testing.T) {
	sys, _ := newManualSystem(t, WithMaxActors(10))
	fillUser(t, sys, 9)
	_, err := Spawn(sys, "n", idle[any]())
	wantCapErr(t, "normal spawn at limit-reserve", err, "/user", 9, 10, 1)
	if _, err := spawnSystem(sys, "s", idle[any]()); err != nil {
		t.Errorf("system actor spawn at limit-reserve: %v", err)
	}
	if got := sys.liveActors(); got != 10 {
		t.Errorf("live %d, want 10", got)
	}
}

// AT-OVL-09: OVL-040. Spawn checks a stopping parent, then a taken name, then the cap, then
// admission. A refused spawn takes no slot, advances no generated-name counter and runs no
// factory.
func TestATOVL09CheckOrder(t *testing.T) {
	sys, eng := newManualSystem(t, WithMaxActors(10))
	refusedCalls := 0
	refused := func() Actor[any] { refusedCalls++; return &fnActor[any]{} }
	var fromStopped error
	pa := &funcActor{}
	p, err := Spawn(sys, "p", actorOf[any](pa))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	run(t, sys, eng, p, func(ctx *Context[any]) {
		if _, err := Spawn(ctx, "a", idle[any]()); err != nil {
			t.Errorf("Spawn a: %v", err)
		}
	})
	fill := fillUser(t, sys, 7) // p, a and 7: live 9 = limit - reserve

	var errName, errCap, errCapAdmission error
	errAdmit := errors.New("admission says no")
	run(t, sys, eng, p, func(ctx *Context[any]) {
		_, errName = Spawn(ctx, "a", refused)
		_, errCap = Spawn(ctx, "", refused)
		eng.SetAdmission(errAdmit, nil)
		_, errCapAdmission = Spawn(ctx, "", refused)
		eng.SetAdmission(nil, nil)
	})
	if !errors.Is(errName, ErrNameTaken) {
		t.Errorf("Spawn(ctx, a) with the cap full: %v, want ErrNameTaken", errName)
	}
	wantCapErr(t, "Spawn(ctx, \"\") with the cap full", errCap, "/user/p", 9, 10, 1)
	wantCapErr(t, "Spawn(ctx, \"\") with the cap full and admission refusing", errCapAdmission, "/user/p", 9, 10, 1)
	if got := sys.liveActors(); got != 9 {
		t.Errorf("live %d after the refusals, want 9", got)
	}

	if err := Stop(sys, fill[0].Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	var generated PID[any]
	run(t, sys, eng, p, func(ctx *Context[any]) {
		var err error
		if generated, err = Spawn(ctx, "", idle[any]()); err != nil {
			t.Errorf("Spawn(ctx, \"\") after a removal: %v", err)
		}
	})
	if got := generated.Address().Path(); got != "/user/p/$1" {
		t.Errorf("generated child %s, want /user/p/$1", got)
	}

	// A stopping parent: its children stop first, so the Stopped hook fills the cap again before
	// it spawns.
	pa.stopped = func(ctx *Context[any]) {
		if _, err := spawnUntilRefused(sys); !errors.Is(err, ErrActorCap) {
			t.Errorf("filling from p's Stopped: %v, want ErrActorCap", err)
		}
		_, fromStopped = Spawn(ctx, "", refused)
	}
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if !errors.Is(fromStopped, ErrStopped) {
		t.Errorf("Spawn from the stopping parent with the cap full: %v, want ErrStopped", fromStopped)
	}
	if refusedCalls != 0 {
		t.Errorf("a refused spawn's factory ran %d times, want 0", refusedCalls)
	}
}

// recAdmission is an engine.Admission that records the reserve argument of each AdmitSpawn call
// and refuses with err while it is set.
type recAdmission struct {
	engine.Admission
	reserves []bool
	err      error
}

func (a *recAdmission) AdmitSpawn(reserve bool) error {
	a.reserves = append(a.reserves, reserve)
	return a.err
}

// OVL-040, OVL-100: admission is asked after the cap, with the spawn's reserve flag, and a spawn
// it refuses gives its slot back; a spawn the cap refuses never reaches admission.
func TestOVL040AdmissionAfterCap(t *testing.T) {
	b := enginetest.New("local", 1).Engine()
	adm := &recAdmission{Admission: b.Admission}
	b.Admission = adm
	sys, err := newSystem(b, []Option{WithMaxActors(4)}) // reserve 1: three normal spawns
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Spawn(sys, "a", idle[any]()); err != nil {
		t.Fatal(err)
	}
	if _, err := spawnSystem(sys, "s", idle[any]()); err != nil {
		t.Fatal(err)
	}
	adm.err = errors.New("admission says no")
	if _, err := Spawn(sys, "b", idle[any]()); !errors.Is(err, adm.err) {
		t.Errorf("spawn refused by admission: %v, want %v", err, adm.err)
	}
	if got := sys.liveActors(); got != 2 {
		t.Errorf("live %d after an admission refusal, want 2", got)
	}
	adm.err = nil
	if _, err := Spawn(sys, "b", idle[any]()); err != nil {
		t.Errorf("spawn after admission admits again: %v", err)
	}
	if _, err := Spawn(sys, "c", idle[any]()); !errors.Is(err, ErrActorCap) {
		t.Errorf("normal spawn at limit-reserve: %v, want ErrActorCap", err)
	}
	if want := []bool{false, true, false, false}; !slices.Equal(adm.reserves, want) {
		t.Errorf("AdmitSpawn reserve arguments %v, want %v", adm.reserves, want)
	}
}

// OVL-050, OVL §8: the refused spawn's text and its one lockstep.overload record.
func TestOVL050CapRecord(t *testing.T) {
	sys, rec := newRecordingSystem(t, WithMaxActors(2))
	if _, err := Spawn(sys, "", idle[any]()); err != nil {
		t.Fatal(err)
	}
	_, err := Spawn(sys, "", idle[any]())
	if want := "lockstep: spawn in /user: actor cap reached (live 1, limit 2, reserve 1)"; err == nil || err.Error() != want {
		t.Errorf("second spawn: %v, want %q", err, want)
	}
	if !errors.Is(err, ErrActorCap) {
		t.Errorf("second spawn: %v does not match ErrActorCap", err)
	}
	if _, err := spawnSystem(sys, "s", idle[any]()); err != nil {
		t.Fatal(err)
	}
	_, err = spawnSystem(sys, "t", idle[any]())
	wantCapErr(t, "reserve spawn at the limit", err, "/system", 2, 2, 1)
	sameRecords(t, recordsOf(rec.Records(), "lockstep.overload"), "lockstep.overload",
		[]string{"event=cap", "parent=/user", "live=1", "limit=2", "reserve=1", "reserved=false"},
		[]string{"event=cap", "parent=/system", "live=2", "limit=2", "reserve=1", "reserved=true"},
	)
}
