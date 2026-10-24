// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/mailbox"
)

// AT-ACT-01: ACT-010, ACT-020, ACT-340.
func TestATACT01DefaultsAndPaths(t *testing.T) {
	sys := newLocalSystem(t)
	type result struct {
		pid PID[int]
		err error
	}
	child := make(chan result, 1)
	p, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		started: func(ctx *Context[int]) {
			b, err := Spawn(ctx, "b", idle[int]())
			child <- result{b, err}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	a := p.Address()
	if a.Node() != "local" || a.Path() != "/user/a" || a.UID() == 0 || a.UID()>>32 == 0 {
		t.Errorf("a = %v: want node local, path /user/a, a uid with a non-zero epoch", a)
	}
	select {
	case r := <-child:
		if r.err != nil || r.pid.Address().Path() != "/user/a/b" || r.pid.Address().Node() != "local" {
			t.Errorf("Spawn of b from a's Started = %v, %v; want /user/a/b", r.pid, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a did not start within 10 s")
	}
}

// AT-ACT-13 (the Spawn clauses): ACT-200, ACT-300.
func TestATACT13Spawner(t *testing.T) {
	sys, _ := newManualSystem(t)
	f := idle[int]()
	var c *Context[int]
	var s *System
	cases := []struct {
		name string
		call func()
		want string
	}{
		{"nil *Context", func() { _, _ = Spawn[int](c, "a", f) }, "lockstep: nil Spawner"},
		{"nil Spawner", func() { _, _ = Spawn[int](nil, "a", f) }, "lockstep: nil Spawner"},
		{"nil *System", func() { _, _ = Spawn[int](s, "a", f) }, "lockstep: nil Spawner"},
		{"nil factory", func() { _, _ = Spawn[int](sys, "a", nil) }, "lockstep: Spawn with nil factory"},
		{"nil SpawnOption", func() { _, _ = Spawn[int](sys, "a", f, nil) }, "lockstep: nil SpawnOption"},
		// Options are applied before step 1, so a nil option panics first.
		{"nil option and parent", func() { _, _ = Spawn[int](nil, "a", f, nil) }, "lockstep: nil SpawnOption"},
	}
	for _, tc := range cases {
		if v := mustPanic(t, tc.call); v != tc.want {
			t.Errorf("%s: panic %v, want %q", tc.name, v, tc.want)
		}
	}
	if n := sys.reg.Len(); n != 0 {
		t.Errorf("%d actors registered after panicking spawns", n)
	}
}

// AT-ACT-15: ACT-310.
func TestATACT15NameRules(t *testing.T) {
	sys, _ := newManualSystem(t)
	for _, name := range []string{"a", "o-17", "A.b_c%2Fd-e", strings.Repeat("x", 768)} {
		p, err := Spawn(sys, name, idle[int]())
		if err != nil || p.Address().Path() != "/user/"+name {
			t.Errorf("Spawn(%.20q) = %v, %v; want /user/<name>", name, p, err)
		}
	}
	for _, name := range []string{".", "..", "a/b", "a b", "$1", "é", "a~b", "a:b", "a@b", "a+b", strings.Repeat("y", 769)} {
		p, err := Spawn(sys, name, idle[int]())
		if !errors.Is(err, ErrInvalidName) || !p.IsZero() {
			t.Errorf("Spawn(%.20q) = %v, %v; want ErrInvalidName", name, p, err)
			continue
		}
		if want := fmt.Sprintf("lockstep: invalid name: %q", name); err.Error() != want {
			t.Errorf("Spawn(%.20q): message %.60q, want %.60q", name, err.Error(), want)
		}
	}
}

// AT-ACT-16: ACT-320. Generated names under the system and under an actor, across a restart of
// the actor (on a system without a cap), and with the actor cap (on a second system).
func TestATACT16GeneratedNamesCapAndRestart(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	for i := 1; i <= 3; i++ {
		p, err := Spawn(sys, "", idle[int]())
		if want := fmt.Sprintf("/user/$%d", i); err != nil || p.Address().Path() != want {
			t.Errorf("generated spawn %d = %v, %v; want %s", i, p, err, want)
		}
	}
	// Every incarnation of a spawns one generated child from Started.
	var children []PID[int]
	a, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		started: func(ctx *Context[int]) {
			child, err := Spawn(ctx, "", idle[int]())
			if err != nil {
				t.Errorf("generated child of a: %v", err)
			}
			children = append(children, child)
		},
		receive: func(ctx *Context[int], _ int) { ctx.Fail(errProbe) },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, a, 1) // a restarts; its child is stopped first
	eng.RunUntilIdle()
	var paths []string
	for _, c := range children {
		paths = append(paths, c.Address().Path())
	}
	if want := []string{"/user/a/$1", "/user/a/$2"}; !slices.Equal(paths, want) {
		t.Errorf("generated children of a across a restart = %v, want %v", paths, want)
	}

	// With the cap (reserve 1), a normal spawn is admitted only while fewer than 2 actors are live.
	// The refused spawn consumes no number; the slot of $1 comes back when its stop completes.
	capped, capEng := newManualSystem(t, WithMaxActors(3))
	first, err := Spawn(capped, "", idle[int]())
	if err != nil || first.Address().Path() != "/user/$1" {
		t.Fatalf("first spawn under the cap = %v, %v; want /user/$1", first, err)
	}
	if p, err := Spawn(capped, "", idle[int]()); err != nil || p.Address().Path() != "/user/$2" {
		t.Fatalf("second spawn under the cap = %v, %v; want /user/$2", p, err)
	}
	if p, err := Spawn(capped, "", idle[int]()); !errors.Is(err, ErrActorCap) || !p.IsZero() {
		t.Fatalf("third spawn under the cap = %v, %v; want ErrActorCap", p, err)
	}
	if err := Stop(capped, first.Address()); err != nil {
		t.Fatal(err)
	}
	capEng.RunUntilIdle()
	if p, err := Spawn(capped, "", idle[int]()); err != nil || p.Address().Path() != "/user/$3" {
		t.Errorf("spawn after $1 stopped = %v, %v; want /user/$3", p, err)
	}
}

// AT-ACT-19: ACT-360, ACT-040. Run with -race.
func TestATACT19ConcurrentSpawns(t *testing.T) {
	sys := newLocalSystem(t)
	const workers = 64
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = Spawn(sys, "same", idle[int]())
		}()
	}
	wg.Wait()
	ok, taken := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrNameTaken):
			taken++
		default:
			t.Errorf("Spawn(same): %v", err)
		}
	}
	if ok != 1 || taken != workers-1 {
		t.Errorf("%d successes and %d ErrNameTaken, want 1 and %d", ok, taken, workers-1)
	}

	const each = 100
	pids := make([][]PID[int], workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				p, err := Spawn(sys, "", idle[int]())
				if err != nil {
					t.Errorf("generated spawn: %v", err)
					return
				}
				pids[i] = append(pids[i], p)
			}
		}()
	}
	wg.Wait()
	paths := make(map[string]bool)
	uids := make(map[uint64]bool)
	for _, ps := range pids {
		for _, p := range ps {
			paths[p.Address().Path()] = true
			uids[p.Address().UID()] = true
		}
	}
	if len(paths) != workers*each || len(uids) != workers*each {
		t.Fatalf("%d distinct paths and %d distinct uids, want %d", len(paths), len(uids), workers*each)
	}
	for n := 1; n <= workers*each; n++ {
		if p := fmt.Sprintf("/user/$%d", n); !paths[p] {
			t.Fatalf("no actor at %s", p)
		}
	}
}

// ACT-100, ACT-340, SUP-030: Spawn does not call the factory; the start turn does, once, and
// Started runs before the first user message, even one queued before the start.
func TestSpawnDoesNotCallFactory(t *testing.T) {
	sys, eng := newManualSystem(t)
	calls := 0
	var seen []string
	pid, err := Spawn(sys, "a", func() Actor[int] {
		calls++
		return &fnActor[int]{
			started: func(*Context[int]) { seen = append(seen, "started") },
			receive: func(_ *Context[int], m int) { seen = append(seen, fmt.Sprint(m)) },
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("factory called %d times inside Spawn", calls)
	}
	send(t, sys, pid, 1)
	send(t, sys, pid, 2)
	eng.RunUntilIdle()
	if calls != 1 {
		t.Errorf("factory called %d times after the start, want 1", calls)
	}
	if want := []string{"started", "1", "2"}; !slices.Equal(seen, want) {
		t.Errorf("handler order %v, want %v", seen, want)
	}
	if c := cellOf(t, sys, pid); c.inc != 1 {
		t.Errorf("incarnation %d, want 1", c.inc)
	}

	// A sender can reach a cell between its registration and the push of begin (ENG-350 lets it
	// call Ready): that turn processes nothing, and the message waits for the start.
	raw := newCell(sys, Address{}, func() Actor[int] { calls++; return &fnActor[int]{} }, sys.mbx, OneForOne(nil))
	raw.user.Offer(entry{msg: 1, kind: entrySent}, mailbox.Sent, false)
	if more := raw.RunTurn(1); more || calls != 1 || raw.user.Len() != 1 {
		t.Errorf("turn before begin: more %v, factory calls %d, queued %d; want false, 1, 1", more, calls, raw.user.Len())
	}
}

// ACT §8: lockstep.spawn on success and on every refusal of ACT-300 steps 3 to 6.
func TestSpawnRecord(t *testing.T) {
	sys, eng := newManualSystem(t)
	user := "parent=" + sys.user.String()
	pa, err := Spawn(sys, "a", idle[int]())
	if err != nil {
		t.Fatal(err)
	}
	pg, err := Spawn(sys, "", idle[string]())
	if err != nil {
		t.Fatal(err)
	}
	_, errTaken := Spawn(sys, "a", idle[int]())
	_, errName := Spawn(sys, "a/b", idle[int]())
	eng.SetAdmission(shedPressure, nil)
	_, errAdmit := Spawn(sys, "c", idle[int]())
	if !errors.Is(errAdmit, ErrOverloaded) || !errors.Is(errAdmit, shedPressure) {
		t.Errorf("Spawn refused by admission: err %v, want ErrOverloaded wrapping %v", errAdmit, shedPressure)
	}
	if !errors.Is(errTaken, ErrNameTaken) {
		t.Errorf("second Spawn of a: %v", errTaken)
	} else if want := `lockstep: name taken: "a" under ` + sys.user.String(); errTaken.Error() != want {
		t.Errorf("second Spawn of a: message %q, want %q", errTaken.Error(), want)
	}

	got := recordsOf(eng.Records(), "lockstep.spawn")
	want := [][]string{
		{user, "addr=" + pa.String(), "name=a", "type=int", "result=ok"},
		{user, "addr=" + pg.String(), "name=$1", "type=string", "result=ok"},
		{user, "name=a", "type=int", "result=" + errTaken.Error()},
		{user, "name=a/b", "type=int", "result=" + errName.Error()},
		{user, "name=c", "type=int", "result=lockstep: spawn in /user: overloaded (pressure)"},
	}
	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.spawn records:\n got %q\nwant %q", got, want)
	}
	if n := sys.reg.Len(); n != 2 {
		t.Errorf("%d actors registered, want 2", n)
	}
}

// AT-ACT-14: ACT-300. Steps 3, 3a, 4, 5 and 6 in that order; ErrStopped names the parent.
func TestATACT14SpawnCheckOrder(t *testing.T) {
	sys, eng := newManualSystem(t)
	if _, err := Spawn(sys, "a", idle[int]()); err != nil {
		t.Fatal(err)
	}
	// Step 5 before step 6: a taken name is reported even when admission would refuse.
	eng.SetAdmission(ErrOverloaded, nil)
	if _, err := Spawn(sys, "a", idle[int]()); !errors.Is(err, ErrNameTaken) {
		t.Errorf("taken name with admission refusing: %v, want ErrNameTaken", err)
	}
	if _, err := Spawn(sys, "b", idle[int]()); !errors.Is(err, ErrOverloaded) {
		t.Errorf("free name with admission refusing: %v, want ErrOverloaded", err)
	}
	eng.SetAdmission(nil, nil)

	errBad := fmt.Errorf("%w: bad", ErrInvalidOption)
	bad := SpawnOption(func(c *spawnConfig) { c.fail(errBad) })
	worse := SpawnOption(func(c *spawnConfig) { c.fail(errors.New("not the first")) })
	sys.closing.Store(true)

	if _, err := Spawn(sys, "a b", idle[int](), bad); !errors.Is(err, ErrInvalidName) {
		t.Errorf("invalid name and option while closing: %v, want ErrInvalidName", err)
	}
	if _, err := Spawn(sys, "a", idle[int](), bad, worse); err != errBad { //nolint:errorlint // the option's own error
		t.Errorf("invalid options while closing: %v, want the first option's error", err)
	}
	_, err := Spawn(sys, "a", idle[int]())
	if want := "lockstep: stopped: " + sys.user.String(); !errors.Is(err, ErrStopped) || err.Error() != want {
		t.Errorf("Spawn while closing: %v, want %q", err, want)
	}

	// The same order on a system whose Shutdown is blocked in a Stopped hook, and with the actor cap
	// reached.
	t.Run("during a real shutdown", func(t *testing.T) {
		live := newLocalSystem(t)
		entered, release := make(chan struct{}, 1), make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(release) }) }) // runs before the system's shutdown
		if _, err := Spawn(live, "x", actorOf[any](&lifeActor[any]{stopped: func(*Context[any], StopReason) {
			entered <- struct{}{}
			<-release
		}})); err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { result <- live.Shutdown(context.Background()) }()
		recv(t, entered)

		if _, err := Spawn(live, "bad/name", idle[int]()); !errors.Is(err, ErrInvalidName) {
			t.Errorf("invalid name during the shutdown: %v, want ErrInvalidName", err)
		}
		_, err := Spawn(live, "y", idle[int](), WithStrategy(Strategy{}))
		if want := "lockstep: invalid option: Strategy must be built with OneForOne or AllForOne"; !errors.Is(err, ErrInvalidOption) || err.Error() != want {
			t.Errorf("invalid strategy during the shutdown: %v, want ErrInvalidOption with text %q", err, want)
		}
		if _, err := Spawn(live, "y", idle[int]()); !isStoppedErr(live, err) {
			t.Errorf("Spawn during the shutdown = %v, want ErrStopped with text %q", err, stoppedText(live))
		}
		once.Do(func() { close(release) })
		if err := recv(t, result); err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
	})

	t.Run("actor cap reached", func(t *testing.T) {
		capped, _ := newManualSystem(t, WithMaxActors(1))
		if _, err := Spawn(capped, "a", idle[int]()); err != nil {
			t.Fatal(err)
		}
		if _, err := Spawn(capped, "a", idle[int]()); !errors.Is(err, ErrNameTaken) {
			t.Errorf("taken name at the cap: %v, want ErrNameTaken (step 5 before 6)", err)
		}
		if _, err := Spawn(capped, "b", idle[int]()); !errors.Is(err, ErrActorCap) {
			t.Errorf("free name at the cap: %v, want ErrActorCap", err)
		}
	})
}

// ACT-030: the internal spawn function places children under /system, with their own counter.
func TestSpawnSystem(t *testing.T) {
	sys, eng := newManualSystem(t)
	p, err := spawnSystem(sys, "s", idle[int]())
	if err != nil || p.Address().Path() != "/system/s" {
		t.Errorf("spawnSystem(s) = %v, %v; want /system/s", p, err)
	}
	g, err := spawnSystem(sys, "", idle[int]())
	if err != nil || g.Address().Path() != "/system/$1" {
		t.Errorf("spawnSystem(\"\") = %v, %v; want /system/$1", g, err)
	}
	eng.RunUntilIdle()
	if c := cellOf(t, sys, p); c.Address() != p.Address().engine() || c.parent != sys.system {
		t.Errorf("cell address %v, parent %v", c.Address(), c.parent)
	}
	if v := mustPanic(t, func() { _, _ = spawnSystem[int](sys, "x", nil) }); v != "lockstep: Spawn with nil factory" {
		t.Errorf("spawnSystem with nil factory panicked with %v", v)
	}
}
