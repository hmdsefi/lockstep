// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// allForOne is AllForOne(nil) with MinBackoff d.
func allForOne(d time.Duration) Strategy {
	s := AllForOne(nil)
	s.MinBackoff = d
	return s
}

// spawnGroup spawns, under the user guardian, a supervisor "s" with strategy st and the children
// c1, c2 and c3, in that order, all rProbes observing into obs. It runs eng until idle.
func spawnGroup(t *testing.T, sys *System, eng *enginetest.Engine, st Strategy, obs *[]string) (s PID[any], c [3]PID[any]) {
	t.Helper()
	s = spawnR(t, sys, "s", &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
		for i, name := range []string{"c1", "c2", "c3"} {
			c[i] = spawnR(t, ctx, name, &rProbe{obs: obs})
		}
	}}, WithStrategy(st))
	eng.RunUntilIdle()
	return s, c
}

// withPrefix returns the elements of obs that start with prefix.
func withPrefix(obs []string, prefix string) []string {
	var out []string
	for _, o := range obs {
		if strings.HasPrefix(o, prefix) {
			out = append(out, o)
		}
	}
	return out
}

// attrOf returns the value of the attribute key of a record's attributes.
func attrOf(rec []string, key string) string {
	for _, a := range rec {
		if v, ok := strings.CutPrefix(a, key+"="); ok {
			return v
		}
	}
	return ""
}

// AT-SUP-26: SUP-260. C2 fails with e under AllForOne: C3, C2 and C1 stop with Restarting(e), in
// that order; then all three start incarnation 2; one lockstep.directive has scope all-for-one.
// Variant DirectiveStop: the three stop in order C3, C2, C1 with Failed(e) and none restarts.
func TestATSUP26AllForOneRestartOrder(t *testing.T) {
	t.Run("restart", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var obs []string
		s, c := spawnGroup(t, sys, eng, allForOne(0), &obs)
		send(t, sys, c[1], any("fail"))
		eng.RunUntilIdle()

		sameObs(t, withPrefix(obs, "stopped "), []string{
			"stopped /user/s/c3 restarting: errX",
			"stopped /user/s/c2 restarting: errX",
			"stopped /user/s/c1 restarting: errX",
		})
		started := withPrefix(obs, "started ")
		slices.Sort(started)
		sameObs(t, started, []string{
			"started /user/s/c1 1", "started /user/s/c1 2",
			"started /user/s/c2 1", "started /user/s/c2 2",
			"started /user/s/c3 1", "started /user/s/c3 2",
		})
		recs := eng.Records()
		var restarted []string
		for _, r := range recordsOf(recs, "lockstep.restart") {
			restarted = append(restarted, attrOf(r, "actor"))
		}
		sameObs(t, restarted, []string{c[2].String(), c[1].String(), c[0].String()})
		want := directiveAttrs(s.Address(), c[1].Address(), 1, "restart", "errX", 1, 0, false, false)
		want[3] = "scope=all-for-one"
		sameRecords(t, recordsOf(recs, "lockstep.directive"), "lockstep.directive", want)
	})
	t.Run("stop", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var obs []string
		s, c := spawnGroup(t, sys, eng, func() Strategy {
			st := allForOne(0)
			st.Decider = func(error) Directive { return DirectiveStop }
			return st
		}(), &obs)
		send(t, sys, c[1], any("fail"))
		eng.RunUntilIdle()

		sameObs(t, withPrefix(obs, "stopped "), []string{
			"stopped /user/s/c3 failed: errX",
			"stopped /user/s/c2 failed: errX",
			"stopped /user/s/c1 failed: errX",
		})
		if n := len(withPrefix(obs, "started ")); n != 3 {
			t.Errorf("%d started observations, want only the three of incarnation 1: %q", n, obs)
		}
		recs := eng.Records()
		var stopped []string
		for _, r := range recordsOf(recs, "lockstep.stop") {
			stopped = append(stopped, attrOf(r, "actor")+" "+attrOf(r, "reason"))
		}
		sameObs(t, stopped, []string{c[2].String() + " failed", c[1].String() + " failed", c[0].String() + " failed"})
		want := directiveAttrs(s.Address(), c[1].Address(), 1, "stop", "errX", 0, 0, false, false)
		want[3] = "scope=all-for-one"
		sameRecords(t, recordsOf(recs, "lockstep.directive"), "lockstep.directive", want)
	})
}

// SUP-260: a supervisor that stops during the group's delay cancels the timer, so no begin is
// pushed when the delay ends, even though the stop is still in progress then and the children it
// has not reached are still in Restarting. No child starts incarnation 2, and the stop stops every
// child, those in Restarting included, without a second Stopped call.
func TestSUP260GroupRestartCancelledByStop(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	s, c := spawnGroup(t, sys, eng, allForOne(time.Second), &obs)
	send(t, sys, c[1], any("fail"))
	eng.RunUntilIdle()
	eng.Advance(100 * time.Millisecond) // the delay is running; every child is Restarting
	for i, k := range c {
		if st, _ := cellOf(t, sys, k).lifecycle(); st.String() != "restarting" {
			t.Fatalf("c%d is %v, want restarting", i+1, st)
		}
	}
	release := Hold(sys, c[2].Address()) // the stop waits for C3, the first child it stops
	if err := Stop(sys, s.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	eng.Advance(5 * time.Second) // the delay ends while C1 and C2 still wait for their stop
	release()
	eng.RunUntilIdle()

	if got := withPrefix(obs, "started "); len(got) != 3 {
		t.Errorf("started observations %q, want only incarnation 1's", got)
	}
	if got := len(withPrefix(obs, "stopped ")); got != 3 {
		t.Errorf("%d Stopped calls, want the 3 of the restart: %q", got, obs)
	}
	recs := eng.Records()
	if got := recordsOf(recs, "lockstep.start"); len(got) != 4 {
		t.Errorf("lockstep.start records %q, want the 4 of incarnation 1", got)
	}
	sameRecords(t, recordsOf(recs, "lockstep.stop"), "lockstep.stop",
		[]string{"actor=" + c[2].String(), "inc=1", "reason=parent-stopping", "err="},
		[]string{"actor=" + c[1].String(), "inc=1", "reason=parent-stopping", "err="},
		[]string{"actor=" + c[0].String(), "inc=1", "reason=parent-stopping", "err="},
		[]string{"actor=" + s.String(), "inc=1", "reason=normal", "err="},
	)
}

// SUP-260: a supervisor that stops while it waits for a child's ended pushes no further end: the
// children it did not reach are stopped by its stop with ParentStopping, never restarted.
func TestSUP260StopDuringGroupRestart(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	s, c := spawnGroup(t, sys, eng, allForOne(0), &obs)
	release := Hold(sys, c[2].Address()) // C3 does not answer end, so the supervisor waits for it
	send(t, sys, c[1], any("fail"))
	eng.RunUntilIdle()
	if err := Stop(sys, s.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	release()
	eng.RunUntilIdle()

	sameObs(t, withPrefix(obs, "stopped "), []string{
		"stopped /user/s/c3 restarting: errX", // its end was queued before the supervisor's stop
		"stopped /user/s/c2 parent-stopping",
		"stopped /user/s/c1 parent-stopping",
	})
	if got := withPrefix(obs, "started "); len(got) != 3 {
		t.Errorf("started observations %q, want only incarnation 1's", got)
	}
}

// SUP-260: failures of different children count toward one window.
func TestSUP260GroupIntensity(t *testing.T) {
	sys, eng := newManualSystem(t)
	st := allForOne(0)
	st.MaxRestarts, st.Window = 2, time.Minute
	var obs []string
	s, c := spawnGroup(t, sys, eng, st, &obs)
	for _, k := range c {
		send(t, sys, k, any("fail"))
		eng.RunUntilIdle()
	}

	var recs [][]string // the supervisor's; the user guardian then restarts it
	for _, r := range recordsOf(eng.Records(), "lockstep.directive") {
		if attrOf(r, "supervisor") == s.Address().String() {
			recs = append(recs, r)
		}
	}
	if len(recs) != 3 {
		t.Fatalf("lockstep.directive records %q, want 3", recs)
	}
	for i, want := range []struct{ directive, restarts, limit string }{
		{"restart", "1", "false"}, {"restart", "2", "false"}, {"escalate", "2", "true"},
	} {
		got := recs[i]
		if attrOf(got, "directive") != want.directive || attrOf(got, "restarts") != want.restarts ||
			attrOf(got, "limit") != want.limit || attrOf(got, "scope") != "all-for-one" ||
			attrOf(got, "child") != c[i].String() {
			t.Errorf("record %d = %q, want directive %s restarts %s limit %s of %v", i, got, want.directive,
				want.restarts, want.limit, c[i])
		}
	}
}

// SUP-260: DirectiveResume resumes the failed child only; the others are left alone.
func TestSUP260ResumeFailedChildOnly(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	st := allForOne(0)
	st.Decider = func(error) Directive { return DirectiveResume }
	_, c := spawnGroup(t, sys, eng, st, &obs)
	send(t, sys, c[1], any("fail"))
	eng.RunUntilIdle()

	if got := withPrefix(obs, "stopped "); len(got) != 0 {
		t.Errorf("Stopped calls %q, want none", got)
	}
	for i, k := range c {
		if st, inc := cellOf(t, sys, k).lifecycle(); st.String() != "running" || inc != 1 {
			t.Errorf("c%d is %v in incarnation %d, want running in 1", i+1, st, inc)
		}
	}
}

// SUP-260, SUP-170: a failure of a child the group restart has yet to end is held until the group
// restart has pushed its commands; by then the notice is stale, so it is dropped and counts
// neither as a directive nor toward intensity.
func TestSUP260FailureDuringGroupRestartIsHeld(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	_, c := spawnGroup(t, sys, eng, allForOne(0), &obs)
	release := Hold(sys, c[2].Address())
	send(t, sys, c[1], any("fail"))
	eng.RunUntilIdle() // the supervisor waits for C3's ended
	send(t, sys, c[0], any("fail"))
	eng.RunUntilIdle() // C1 is Suspended; its notice reached the supervisor
	release()
	eng.RunUntilIdle()

	if n := len(recordsOf(eng.Records(), "lockstep.directive")); n != 1 {
		t.Errorf("%d lockstep.directive records, want 1", n)
	}
	if indexOf(eng.Records(), "lockstep.drop", "reason=stale-failure") < 0 {
		t.Errorf("no lockstep.drop record for C1's stale failure: %q", recordsOf(eng.Records(), "lockstep.drop"))
	}
	for i, k := range c {
		if st, inc := cellOf(t, sys, k).lifecycle(); st.String() != "running" || inc != 2 {
			t.Errorf("c%d is %v in incarnation %d, want running in 2", i+1, st, inc)
		}
	}
}

// spawnsOnDemand is an rProbe that spawns the child "c4" when it receives "spawn".
type spawnsOnDemand struct {
	*rProbe
	t   *testing.T
	obs *[]string
	c4  *PID[any]
}

func (p spawnsOnDemand) Receive(ctx *Context[any], msg any) {
	if msg == "spawn" {
		*p.c4 = spawnR(p.t, ctx, "c4", &rProbe{obs: p.obs})
		return
	}
	p.rProbe.Receive(ctx, msg)
}

// SUP-260: a child spawned while the group waits out its delay and fails restarts alone, on its own
// delay; the first group's begin still comes at the end of the first delay.
func TestSUP260RestartsOverlap(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	var c [3]PID[any]
	var c4 PID[any]
	s, err := Spawn(sys, "s", actorOf[any](spawnsOnDemand{
		rProbe: &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
			for i, name := range []string{"c1", "c2", "c3"} {
				c[i] = spawnR(t, ctx, name, &rProbe{obs: &obs})
			}
		}},
		t: t, obs: &obs, c4: &c4,
	}), WithStrategy(allForOne(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, c[1], any("fail"))
	eng.RunUntilIdle() // the group waits out one second
	send(t, sys, s, any("spawn"))
	eng.RunUntilIdle()
	send(t, sys, c4, any("fail"))
	eng.RunUntilIdle() // C4's own restart: second in the window, so two seconds
	eng.Advance(time.Second)

	for i, k := range c {
		if _, inc := cellOf(t, sys, k).lifecycle(); inc != 2 {
			t.Errorf("c%d inc = %d after the first delay, want 2", i+1, inc)
		}
	}
	if st, _ := cellOf(t, sys, c4).lifecycle(); st.String() != "restarting" {
		t.Errorf("c4 is %v after one second, want restarting for another second", st)
	}
	eng.Advance(2 * time.Second)
	if st, inc := cellOf(t, sys, c4).lifecycle(); st.String() != "running" || inc != 2 {
		t.Errorf("c4 is %v in incarnation %d, want running in 2", st, inc)
	}
}

// SUP-260, SUP-300: the guardians supervise with the same code, so an AllForOne guardian strategy
// restarts every top-level actor, in reverse spawn order; a shutdown during the delay pushes no
// begin.
func TestSUP260GuardianAllForOne(t *testing.T) {
	t.Run("restart", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithGuardianStrategy(allForOne(0)))
		var obs []string
		p1 := spawnR(t, sys, "p1", &rProbe{obs: &obs})
		spawnR(t, sys, "p2", &rProbe{obs: &obs})
		eng.RunUntilIdle()
		send(t, sys, p1, any("fail"))
		eng.RunUntilIdle()
		sameObs(t, withPrefix(obs, "stopped "), []string{
			"stopped /user/p2 restarting: errX",
			"stopped /user/p1 restarting: errX",
		})
		if got := len(withPrefix(obs, "started ")); got != 4 {
			t.Errorf("%d started observations, want 4: %q", got, obs)
		}
	})
	t.Run("shutdown during the delay", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithGuardianStrategy(allForOne(time.Second)))
		var obs []string
		p1 := spawnR(t, sys, "p1", &rProbe{obs: &obs})
		spawnR(t, sys, "p2", &rProbe{obs: &obs})
		eng.RunUntilIdle()
		send(t, sys, p1, any("fail"))
		eng.RunUntilIdle()
		if err := sys.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		eng.Advance(2 * time.Second)
		if got := len(withPrefix(obs, "started ")); got != 2 {
			t.Errorf("%d started observations, want only incarnation 1's: %q", got, obs)
		}
		if !eng.Stopped() {
			t.Error("the shutdown did not complete")
		}
	})
}

// SUP-260: a supervisor that stops cancels every group delay timer, also one due at the very
// instant at which another group restart is decided. The manual engine fires timers due together in
// the order they were armed, running every turn between them. Here timer A (group {C1}) fires and
// C1 fails again in Started; the supervisor decides a third group restart, arming its timer at
// that instant, while timer B (group {C4}) is still pending; then a test timer stops the supervisor
// while it waits for C4, which it has asked to stop, and B fires. B must push no begin.
func TestSUP260StopCancelsTimersDueTogether(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	var c1, c4 PID[any]
	st := allForOne(time.Second)
	st.MaxBackoff = time.Second // every delay is one second
	s, err := Spawn(sys, "s", actorOf[any](spawnsOnDemand{
		rProbe: &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
			c1 = spawnR(t, ctx, "c1", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
				if ctx.inc == 2 {
					panic("again")
				}
			}})
		}},
		t: t, obs: &obs, c4: &c4,
	}), WithStrategy(st))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, c1, any("fail"))
	eng.RunUntilIdle() // timer A, due at 1s
	sys.eng.Clock.AfterFunc(time.Second, "test", func() {
		if err := Stop(sys, s.Address()); err != nil {
			t.Error(err)
		}
	})
	send(t, sys, s, any("spawn"))
	eng.RunUntilIdle()
	send(t, sys, c4, any("fail"))
	eng.RunUntilIdle() // timer B, due at 1s, armed after A and the test timer
	release := Hold(sys, c4.Address())
	eng.Advance(time.Second)

	if n := cellOf(t, sys, c4).sysq.Len(); n != 1 {
		t.Errorf("C4's system queue holds %d messages, want only the stop request", n)
	}
	release()
	eng.RunUntilIdle()
	if got := withPrefix(obs, "started /user/s/c4"); len(got) != 1 {
		t.Errorf("C4 started observations %q, want only incarnation 1's", got)
	}
}
