// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/supervise"
)

// rProbe is SUP §10's Probe for the restart tests. It observes "started <path> <inc>" before its
// onStart hook runs, "stopped <path> <reason>" before its onStop hook, "<path> <inc> got <msg>"
// for every message, and "<path> terminated <addr path> <reason>" for every Terminated. It
// panics with "panic" on "panic", calls ctx.Fail(errX) on "fail" and ctx.Stop() on "stop". Every
// incarnation is the same value, so the hooks see every incarnation.
type rProbe struct {
	obs      *[]string
	onStart  func(ctx *Context[any])
	onStop   func(ctx *Context[any], r StopReason)
	onSignal func(ctx *Context[any], sig Signal)
}

func (p *rProbe) Started(ctx *Context[any]) {
	*p.obs = append(*p.obs, "started "+ctx.Self().Address().Path()+" "+strconv.FormatUint(uint64(ctx.inc), 10))
	if p.onStart != nil {
		p.onStart(ctx)
	}
}

func (p *rProbe) Stopped(ctx *Context[any], r StopReason) {
	*p.obs = append(*p.obs, "stopped "+ctx.Self().Address().Path()+" "+r.String())
	if p.onStop != nil {
		p.onStop(ctx, r)
	}
}

func (p *rProbe) Signal(ctx *Context[any], sig Signal) {
	if t, ok := sig.(Terminated); ok {
		*p.obs = append(*p.obs, ctx.Self().Address().Path()+" terminated "+t.Addr.Path()+" "+t.Reason.String())
	}
	if p.onSignal != nil {
		p.onSignal(ctx, sig)
	}
}

func (p *rProbe) Receive(ctx *Context[any], msg any) {
	s, _ := msg.(string)
	*p.obs = append(*p.obs, ctx.Self().Address().Path()+" "+strconv.FormatUint(uint64(ctx.inc), 10)+" got "+s)
	switch s {
	case "panic":
		panic("panic")
	case "fail":
		ctx.Fail(errX)
	case "stop":
		ctx.Stop()
	}
}

// r0 is SUP §10's strategy R0: OneForOne(nil) with MinBackoff 0.
func r0() Strategy {
	s := OneForOne(nil)
	s.MinBackoff = 0
	return s
}

// withBackoff returns OneForOne(nil) with MinBackoff d.
func withBackoff(d time.Duration) Strategy {
	s := OneForOne(nil)
	s.MinBackoff = d
	return s
}

// spawnR spawns an rProbe named name under parent with opts and fails the test on an error.
func spawnR(t *testing.T, parent Spawner, name string, p *rProbe, opts ...SpawnOption) PID[any] {
	t.Helper()
	pid, err := Spawn(parent, name, actorOf[any](p), opts...)
	if err != nil {
		t.Fatalf("Spawn %q: %v", name, err)
	}
	return pid
}

// sameObs fails the test unless obs equals want.
func sameObs(t *testing.T, obs, want []string) {
	t.Helper()
	if !slices.Equal(obs, want) {
		t.Errorf("observations:\n got %q\nwant %q", obs, want)
	}
}

// directiveRecord returns the attributes of lockstep.directive that supervisor sup records for a
// failure of child's incarnation inc with error text err.
func directiveRecord(sup, child Address, inc int, directive, err string, restarts int, delay time.Duration, limit bool) []string {
	return directiveAttrs(sup, child, inc, directive, err, restarts, delay, limit, false)
}

// directiveAttrs is directiveRecord with the escalated attribute of SUP-300.
func directiveAttrs(sup, child Address, inc int, directive, err string, restarts int, delay time.Duration,
	limit, escalated bool,
) []string {
	return []string{
		"supervisor=" + sup.String(), "child=" + child.String(), "inc=" + strconv.Itoa(inc),
		"scope=one-for-one", "directive=" + directive, "err=" + err, "restarts=" + strconv.Itoa(restarts),
		"delay=" + delay.String(), "limit=" + strconv.FormatBool(limit), "escalated=" + strconv.FormatBool(escalated),
	}
}

// sameRecords fails the test unless the records of kind in recs have the attributes want.
func sameRecords(t *testing.T, recs [][]string, kind string, want ...[]string) {
	t.Helper()
	if !slices.EqualFunc(recs, want, slices.Equal) {
		t.Errorf("%s records:\n got %q\nwant %q", kind, recs, want)
	}
}

// AT-SUP-04: SUP-040, SUP-180. A factory that returns nil fails with source factory and inc 0;
// the user guardian restarts the cell (R0), which calls no Stopped, and the second factory call
// starts incarnation 1. Variant: a decider's DirectiveResume for a factory failure is applied as a
// restart.
func TestATSUP04FactoryFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		decider func(error) Directive
	}{
		{"nil decider", nil},
		{"resume applied as restart", func(error) Directive { return DirectiveResume }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := r0()
			s.Decider = tc.decider
			sys, eng := newManualSystem(t, WithGuardianStrategy(s))
			var obs []string
			calls := 0
			p, err := Spawn(sys, "p", func() Actor[any] {
				calls++
				if calls == 1 {
					return nil
				}
				return &rProbe{obs: &obs}
			})
			if err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle()

			const nilErr = "lockstep: factory returned nil actor"
			recs := eng.Records()
			sameRecords(t, recordsOf(recs, "lockstep.fail"), "lockstep.fail",
				[]string{"actor=" + p.String(), "inc=0", "source=factory", "err=" + nilErr})
			sameRecords(t, recordsOf(recs, "lockstep.directive"), "lockstep.directive",
				directiveRecord(sys.user, p.Address(), 0, "restart", nilErr, 1, 0, false))
			sameRecords(t, recordsOf(recs, "lockstep.restart"), "lockstep.restart",
				[]string{"actor=" + p.String(), "inc=0", "err=" + nilErr})
			sameObs(t, obs, []string{"started /user/p 1"})
			if calls != 2 {
				t.Errorf("factory calls = %d, want 2", calls)
			}
		})
	}
}

// AT-SUP-05: SUP-040, SUP-190. A Started panic on incarnation 1 ends that incarnation with
// Stopped(Restarting(err)), and incarnation 2 starts.
func TestATSUP05StartedFailure(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var obs []string
	p := spawnR(t, sys, "p", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
		if ctx.inc == 1 {
			panic("boom")
		}
	}})
	eng.RunUntilIdle()
	sameObs(t, obs, []string{"started /user/p 1", "stopped /user/p restarting: lockstep: panic: boom", "started /user/p 2"})
	sameRecords(t, recordsOf(eng.Records(), "lockstep.restart"), "lockstep.restart",
		[]string{"actor=" + p.String(), "inc=1", "err=lockstep: panic: boom"})
}

// AT-SUP-11: SUP-100. A panic, a Fail and a stop end three incarnations, each with exactly one
// Stopped call.
func TestATSUP11StoppedExactlyOnce(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var obs []string
	p := spawnR(t, sys, "p", &rProbe{obs: &obs})
	send(t, sys, p, any("panic"))
	send(t, sys, p, any("fail"))
	eng.RunUntilIdle()
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	var stops, starts []string
	for _, o := range obs {
		switch {
		case strings.HasPrefix(o, "stopped "):
			stops = append(stops, o)
		case strings.HasPrefix(o, "started "):
			starts = append(starts, o)
		}
	}
	sameObs(t, stops, []string{
		"stopped /user/p restarting: lockstep: panic: panic",
		"stopped /user/p restarting: errX",
		"stopped /user/p normal",
	})
	sameObs(t, starts, []string{"started /user/p 1", "started /user/p 2", "started /user/p 3"})
}

// AT-SUP-16: SUP-150. The message whose Receive failed is consumed: the next incarnation receives
// the following message and never the failing one.
func TestATSUP16FailingMessageNotRedelivered(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var obs []string
	p := spawnR(t, sys, "p", &rProbe{obs: &obs})
	send(t, sys, p, any("panic"))
	send(t, sys, p, any("a"))
	eng.RunUntilIdle()
	sameObs(t, obs, []string{
		"started /user/p 1",
		"/user/p 1 got panic",
		"stopped /user/p restarting: lockstep: panic: panic",
		"started /user/p 2",
		"/user/p 2 got a",
	})
}

// AT-SUP-18: SUP-170. The failed notice of a child that has stopped since is stale: the
// supervisor drops it with lockstep.drop reason stale-failure and records no directive.
func TestATSUP18StaleFailureNotice(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	var c PID[any]
	s := spawnR(t, sys, "s", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
		c = spawnR(t, ctx, "c", &rProbe{obs: &obs})
	}}, WithStrategy(r0()))
	eng.RunUntilIdle()
	release := Hold(sys, s.Address())
	send(t, sys, c, any("panic"))
	eng.RunUntilIdle() // C is Suspended; its notice waits in S's system queue
	if err := Stop(sys, c.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	release()
	eng.RunUntilIdle()

	recs := eng.Records()
	n := failed{child: c.Address(), inc: 1}
	sameRecords(t, recordsOf(recs, "lockstep.drop"), "lockstep.drop",
		[]string{"to=" + s.String(), "reason=stale-failure", "msg=" + sys.eng.Tracer.Describe(n)})
	if d := recordsOf(recs, "lockstep.directive"); len(d) != 0 {
		t.Errorf("lockstep.directive records %q, want none", d)
	}
	if !slices.Contains(obs, "stopped /user/s/c normal") {
		t.Errorf("observations %q lack stopped /user/s/c normal", obs)
	}
}

// AT-SUP-20: SUP-190, MBX-010. A restart keeps the PID, the registry entry and the mailbox with
// its sent entries; the old incarnation's child is stopped first, and the new incarnation spawns a
// new one.
func TestATSUP20RestartKeepsPIDAndMailbox(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var obs []string
	var selves []PID[any]
	c := spawnR(t, sys, "c", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
		selves = append(selves, ctx.Self())
		spawnR(t, ctx, "g", &rProbe{obs: &obs})
	}}, WithMailbox(10, FailSend))
	send(t, sys, c, any("panic"))
	send(t, sys, c, any("a"))
	send(t, sys, c, any("b"))

	cc := cellOf(t, sys, c)
	sawRestarting := false
	for eng.Step() {
		if st, _ := cc.lifecycle(); st == supervise.Restarting && !sawRestarting {
			sawRestarting = true
			got, err := Lookup[any](sys, "/user/c")
			if err != nil || got != c {
				t.Errorf("Lookup during the restart = %v, %v; want %v", got, err, c)
			}
		}
	}
	if !sawRestarting {
		t.Fatal("the cell never was Restarting")
	}
	// C's own observations in order; G's turns interleave with C's.
	sameObs(t, without(obs, "/user/c/g"), []string{
		"started /user/c 1",
		"/user/c 1 got panic",
		"stopped /user/c restarting: lockstep: panic: panic",
		"started /user/c 2",
		"/user/c 2 got a",
		"/user/c 2 got b",
	})
	gStop := slices.Index(obs, "stopped /user/c/g parent-stopping")
	if cStop := slices.Index(obs, "stopped /user/c restarting: lockstep: panic: panic"); gStop < 0 || gStop > cStop {
		t.Errorf("observations %q: want stopped /user/c/g parent-stopping before C's Stopped", obs)
	}
	var gStarts []int
	for i, o := range obs {
		if o == "started /user/c/g 1" {
			gStarts = append(gStarts, i)
		}
	}
	if len(gStarts) != 2 || gStarts[1] < slices.Index(obs, "started /user/c 2") {
		t.Errorf("observations %q: want a new G started by incarnation 2", obs)
	}
	if len(selves) != 2 || selves[0] != c || selves[1] != c {
		t.Errorf("ctx.Self in Started = %v, want %v twice", selves, c)
	}
	if _, capacity, policy := cc.mailboxState(); capacity != 10 || policy != FailSend {
		t.Errorf("mailbox after the restart: capacity %d, policy %v; want 10, fail-send", capacity, policy)
	}
}

// AT-SUP-25: SUP-250. Under OneForOne a child's failure restarts that child only.
func TestATSUP25OneForOneIsolation(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs1, obs2 []string
	var c1 PID[any]
	spawnR(t, sys, "s", &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
		c1 = spawnR(t, ctx, "c1", &rProbe{obs: &obs1})
		spawnR(t, ctx, "c2", &rProbe{obs: &obs2})
	}}, WithStrategy(r0()))
	eng.RunUntilIdle()
	send(t, sys, c1, any("panic"))
	eng.RunUntilIdle()
	sameObs(t, obs2, []string{"started /user/s/c2 1"})
	if _, inc := cellOf(t, sys, c1).lifecycle(); inc != 2 {
		t.Errorf("C1 inc = %d, want 2", inc)
	}
}

// AT-SUP-28: SUP-280, SUP-190. Three failures in Started in a row restart the actor after 10ms,
// 20ms and 40ms of virtual time, measured from the end of the previous incarnation.
func TestATSUP28BackoffVirtualTime(t *testing.T) {
	sys, eng := newManualSystem(t) // the user guardian's OneForOne(nil): MinBackoff 10ms
	var obs []string
	var starts, ends []time.Time
	p := spawnR(t, sys, "p", &rProbe{
		obs: &obs,
		onStart: func(ctx *Context[any]) {
			starts = append(starts, eng.Now())
			if ctx.inc <= 3 {
				panic("boom")
			}
		},
		onStop: func(*Context[any], StopReason) { ends = append(ends, eng.Now()) },
	})
	eng.RunUntilIdle()
	eng.Advance(time.Second)

	if len(starts) != 4 || len(ends) != 3 {
		t.Fatalf("%d starts and %d ends, want 4 and 3: %q", len(starts), len(ends), obs)
	}
	for i, want := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond} {
		if got := starts[i+1].Sub(ends[i]); got != want {
			t.Errorf("started %d at end of incarnation %d + %v, want + %v", i+2, i+1, got, want)
		}
	}
	const boom = "lockstep: panic: boom"
	sameRecords(t, recordsOf(eng.Records(), "lockstep.directive"), "lockstep.directive",
		directiveRecord(sys.user, p.Address(), 1, "restart", boom, 1, 10*time.Millisecond, false),
		directiveRecord(sys.user, p.Address(), 2, "restart", boom, 2, 20*time.Millisecond, false),
		directiveRecord(sys.user, p.Address(), 3, "restart", boom, 3, 40*time.Millisecond, false),
	)
}

// AT-SUP-29: SUP-290. A stop during the backoff stops the actor at once, with no second Stopped
// call; a message sent during the delay is accepted and then dead-lettered by the stop; the
// pending begin is cancelled.
func TestATSUP29StopDuringBackoff(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(withBackoff(time.Second)))
	var obs []string
	p := spawnR(t, sys, "p", &rProbe{obs: &obs})
	send(t, sys, p, any("panic"))
	eng.RunUntilIdle()
	eng.Advance(100 * time.Millisecond)
	send(t, sys, p, any("late"))
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	eng.Advance(2 * time.Second)

	sameObs(t, obs, []string{"started /user/p 1", "/user/p 1 got panic", "stopped /user/p restarting: lockstep: panic: panic"})
	recs := eng.Records()
	sameRecords(t, recordsOf(recs, "lockstep.stop"), "lockstep.stop",
		[]string{"actor=" + p.String(), "inc=1", "reason=normal", "err="})
	if indexOf(recs, "lockstep.deadletter", "to="+p.String(), "reason=stopped", "msg="+sys.eng.Tracer.Describe("late")) < 0 {
		t.Errorf("no dead letter for the message sent during the delay: %q", recordsOf(recs, "lockstep.deadletter"))
	}
	if starts := recordsOf(recs, "lockstep.start"); len(starts) != 1 {
		t.Errorf("lockstep.start records %q, want only incarnation 1's", starts)
	}
}

// AT-SUP-32: SUP-330. Restarts produce no Terminated; the final stop gives each watcher exactly
// one, in watch-registration order, after lockstep.stop; the parent, not watching, gets none.
func TestATSUP32TerminatedOnceNotOnRestart(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	var c PID[any]
	spawnR(t, sys, "p", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
		c = spawnR(t, ctx, "c", &rProbe{obs: new([]string)})
	}}, WithStrategy(r0()))
	eng.RunUntilIdle()
	afterStop := func(ctx *Context[any], _ Signal) {
		if indexOf(eng.Records(), "lockstep.stop", "actor="+c.String()) < 0 {
			obs = append(obs, ctx.Self().Address().Path()+" got Terminated before lockstep.stop")
		}
	}
	watch := func(ctx *Context[any]) {
		if err := Watch(ctx, c.Address()); err != nil {
			panic(err)
		}
	}
	spawnR(t, sys, "w1", &rProbe{obs: &obs, onStart: watch, onSignal: afterStop})
	eng.RunUntilIdle()
	spawnR(t, sys, "w2", &rProbe{obs: &obs, onStart: watch, onSignal: afterStop})
	eng.RunUntilIdle()
	for range 2 {
		send(t, sys, c, any("panic"))
		eng.RunUntilIdle()
	}
	if _, inc := cellOf(t, sys, c).lifecycle(); inc != 3 {
		t.Fatalf("C inc = %d after two restarts, want 3", inc)
	}
	if err := Stop(sys, c.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	var terms []string
	for _, o := range obs {
		if strings.Contains(o, "Terminated") || strings.Contains(o, " terminated ") {
			terms = append(terms, o)
		}
	}
	sameObs(t, terms, []string{"/user/w1 terminated /user/p/c normal", "/user/w2 terminated /user/p/c normal"})
}

// AT-SUP-33: SUP-340. Watches belong to the incarnation: W watches X by uid and by path in
// incarnation 1, both registry entries go when that incarnation ends, and incarnation 2 gets no
// Terminated when X stops.
func TestATSUP33WatchesBelongToIncarnation(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var obs []string
	x := spawnR(t, sys, "x", &rProbe{obs: new([]string)})
	eng.RunUntilIdle()
	w := spawnR(t, sys, "w", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
		if ctx.inc != 1 {
			return
		}
		for _, a := range []Address{x.Address(), PIDAt[any]("local", "/user/x").Address()} {
			if err := Watch(ctx, a); err != nil {
				panic(err)
			}
		}
	}})
	eng.RunUntilIdle()
	if got := sys.reg.Watchers("/user/x"); len(got) != 2 || got[0].Key.UID == 0 || got[1].Key.UID != 0 {
		t.Fatalf("watch entries of X %+v, want W's uid key, then its path key", got)
	}
	send(t, sys, w, any("panic"))
	eng.RunUntilIdle()
	if _, inc := cellOf(t, sys, w).lifecycle(); inc != 2 {
		t.Fatalf("W inc = %d, want 2", inc)
	}
	if got := sys.reg.Watchers("/user/x"); len(got) != 0 {
		t.Errorf("watch entries of X after W's restart %+v, want none", got)
	}
	if err := Stop(sys, x.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	for _, o := range obs {
		if strings.Contains(o, " terminated ") {
			t.Errorf("W observed %q after its restart", o)
		}
	}
}

// AT-SUP-39: SUP-190, ENG-070. The backoff is one AfterFunc with label lockstep/backoff; the next
// incarnation starts only once the manual clock has advanced by the delay.
func TestATSUP39BackoffTimerLabel(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(withBackoff(time.Second)))
	var obs []string
	c := spawnR(t, sys, "c", &rProbe{obs: &obs})
	send(t, sys, c, any("panic"))
	eng.RunUntilIdle()
	if n := countOf(eng.Labels(), "lockstep/backoff"); n != 1 {
		t.Errorf("Labels() %q hold lockstep/backoff %d times, want once", eng.Labels(), n)
	}
	if slices.Contains(obs, "started /user/c 2") {
		t.Fatal("started /user/c 2 before the clock advanced")
	}
	eng.Advance(time.Second)
	if !slices.Contains(obs, "started /user/c 2") {
		t.Errorf("observations %q lack started /user/c 2 after Advance", obs)
	}
}

// without returns the elements of obs that do not mention path.
func without(obs []string, path string) []string {
	var out []string
	for _, o := range obs {
		if !strings.Contains(o, path) {
			out = append(out, o)
		}
	}
	return out
}

// countOf returns how many elements of s equal v.
func countOf(s []string, v string) int {
	n := 0
	for _, x := range s {
		if x == v {
			n++
		}
	}
	return n
}

// AT-MBX-02: MBX-010. The mailbox keeps its capacity and policy across a restart.
func TestATMBX02CapacitySurvivesRestart(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	p := spawnR(t, sys, "p", &rProbe{obs: new([]string)}, WithMailbox(3, FailSend))
	send(t, sys, p, any("panic"))
	eng.RunUntilIdle()
	if indexOf(eng.Records(), "lockstep.start", "actor="+p.String(), "inc=2") < 0 {
		t.Fatal("no lockstep.start with inc 2")
	}
	if _, capacity, policy := cellOf(t, sys, p).mailboxState(); capacity != 3 || policy != FailSend {
		t.Errorf("capacity %d, policy %v; want 3, fail-send", capacity, policy)
	}
}

// SUP-300: the user guardian supervises top-level actors with the WithGuardianStrategy strategy,
// decider and backoff included; the system guardian supervises system actors with
// OneForOne(nil), whose backoff is 10ms.
func TestSUP300GuardianSupervision(t *testing.T) {
	var decided []string
	s := withBackoff(50 * time.Millisecond)
	s.Decider = func(err error) Directive {
		decided = append(decided, err.Error())
		return DirectiveRestart
	}
	sys, eng := newManualSystem(t, WithGuardianStrategy(s))
	var obs []string
	u := spawnR(t, sys, "u", &rProbe{obs: &obs})
	y, err := spawnSystem(sys, "y", actorOf[any](&rProbe{obs: &obs}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, u, any("panic"))
	send(t, sys, y, any("fail"))
	eng.RunUntilIdle()

	eng.Advance(10 * time.Millisecond)
	if !slices.Contains(obs, "started /system/y 2") || slices.Contains(obs, "started /user/u 2") {
		t.Errorf("after 10ms: observations %q; want /system/y restarted and /user/u not yet", obs)
	}
	eng.Advance(40 * time.Millisecond)
	if !slices.Contains(obs, "started /user/u 2") {
		t.Errorf("after 50ms: observations %q lack started /user/u 2", obs)
	}
	if want := []string{"lockstep: panic: panic"}; !slices.Equal(decided, want) {
		t.Errorf("the user guardian's decider got %q, want %q", decided, want)
	}
	sameRecords(t, recordsOf(eng.Records(), "lockstep.directive"), "lockstep.directive",
		directiveRecord(sys.user, u.Address(), 1, "restart", "lockstep: panic: panic", 1, 50*time.Millisecond, false),
		directiveRecord(sys.system, y.Address(), 1, "restart", "errX", 1, 10*time.Millisecond, false),
	)
}

// SUP-180, SUP-270, SUP-300: a restart beyond MaxRestarts in the window becomes DirectiveEscalate,
// which the user guardian applies as a stop: recorded with directive stop, limit true, escalated
// true and the count already in the window.
func TestSUP180RestartOverLimitEscalates(t *testing.T) {
	s := r0()
	s.MaxRestarts = 1
	sys, eng := newManualSystem(t, WithGuardianStrategy(s))
	p := spawnR(t, sys, "p", &rProbe{obs: new([]string)})
	for range 2 {
		send(t, sys, p, any("fail"))
		eng.RunUntilIdle()
	}
	sameRecords(t, recordsOf(eng.Records(), "lockstep.directive"), "lockstep.directive",
		directiveRecord(sys.user, p.Address(), 1, "restart", "errX", 1, 0, false),
		directiveAttrs(sys.user, p.Address(), 2, "stop", "errX", 1, 0, true, true),
	)
}

// MBX-170, MSG-620: a piped reply that a later incarnation pops is recorded only as
// lockstep.pipe.drop; any other Self entry of an ended incarnation as lockstep.drop reason
// incarnation-ended. Neither is delivered.
func TestMBX170StalePipedReply(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var received []any
	a, err := Spawn(sys, "a", actorOf[any](&fnActor[any]{receive: func(ctx *Context[any], m any) {
		received = append(received, m)
		switch m {
		case "pipe":
			rt, r := NewReply[int](ctx)
			Pipe(ctx, r, func(v int, _ error) any { return v })
			rt.Send(ctx, 1) // the Self entry of this incarnation is queued now (MSG-610)
			panic("boom")
		case "self":
			ctx.cell.offerSelf(entry{msg: "timer", kind: entrySelf, inc: ctx.inc})
			panic("boom")
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, a, any("pipe"))
	eng.RunUntilIdle()

	recs := eng.Records()
	if n := len(recordsOf(recs, "lockstep.pipe.drop")); n != 1 {
		t.Errorf("%d lockstep.pipe.drop records, want 1", n)
	}
	if d := recordsOf(recs, "lockstep.drop"); len(d) != 0 {
		t.Errorf("lockstep.drop records %q, want none for a piped reply", d)
	}

	send(t, sys, a, any("self"))
	eng.RunUntilIdle()
	sameRecords(t, recordsOf(eng.Records(), "lockstep.drop"), "lockstep.drop",
		[]string{"to=" + a.String(), "reason=incarnation-ended", "inc=2", "msg=" + sys.eng.Tracer.Describe("timer")})
	if want := []any{"pipe", "self"}; !slices.Equal(received, want) {
		t.Errorf("received %v, want %v: no stale Self entry is delivered", received, want)
	}
	if _, inc := cellOf(t, sys, a).lifecycle(); inc != 3 {
		t.Errorf("inc = %d, want 3", inc)
	}
}

// SUP-100, SUP-040: when the factory fails for the incarnation after a restart, the ended
// incarnation's Context is gone, so a later stop calls no second Stopped and closes nothing again.
func TestSUP100FactoryFailureAfterRestartStopsOnce(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(withBackoff(time.Second)))
	var obs, closed []string
	calls := 0
	p, err := Spawn(sys, "p", func() Actor[any] {
		calls++
		if calls == 2 {
			return nil
		}
		return &rProbe{obs: &obs, onStart: func(ctx *Context[any]) { ctx.Own(&closer{name: "r", log: &closed}) }}
	})
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, p, any("panic"))
	eng.RunUntilIdle()
	eng.Advance(time.Second) // begin: the factory returns nil, and the restart waits 2s
	if calls != 2 {
		t.Fatalf("factory calls = %d, want 2", calls)
	}
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	eng.Advance(5 * time.Second)

	sameObs(t, obs, []string{"started /user/p 1", "/user/p 1 got panic", "stopped /user/p restarting: lockstep: panic: panic"})
	sameObs(t, closed, []string{"r"})
	recs := eng.Records()
	sameRecords(t, recordsOf(recs, "lockstep.restart"), "lockstep.restart",
		[]string{"actor=" + p.String(), "inc=1", "err=lockstep: panic: panic"},
		[]string{"actor=" + p.String(), "inc=1", "err=lockstep: factory returned nil actor"},
	)
	sameRecords(t, recordsOf(recs, "lockstep.stop"), "lockstep.stop",
		[]string{"actor=" + p.String(), "inc=1", "reason=normal", "err="})
	if calls != 2 {
		t.Errorf("factory calls = %d, want 2", calls)
	}
}

// SUP-060: a stop request processed while the cell stops its children for a restart replaces the
// restart: the remaining steps run with the stop's reason, Stopped gets it, and no incarnation
// starts.
func TestSUP060StopReplacesRestart(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var obs []string
	var g PID[any]
	c := spawnR(t, sys, "c", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
		g = spawnR(t, ctx, "g", &rProbe{obs: &obs})
	}})
	eng.RunUntilIdle()
	send(t, sys, c, any("panic"))
	cc := cellOf(t, sys, c)
	for {
		if st, _ := cc.lifecycle(); st == supervise.Restarting {
			break
		}
		if !eng.Step() {
			t.Fatal("C never was Restarting")
		}
	}
	if err := Stop(sys, c.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	sameObs(t, obs, []string{
		"started /user/c 1",
		"started /user/c/g 1",
		"/user/c 1 got panic",
		"stopped /user/c/g parent-stopping",
		"stopped /user/c normal",
	})
	recs := eng.Records()
	if r := recordsOf(recs, "lockstep.restart"); len(r) != 0 {
		t.Errorf("lockstep.restart records %q, want none", r)
	}
	sameRecords(t, recordsOf(recs, "lockstep.stop"), "lockstep.stop",
		[]string{"actor=" + g.String(), "inc=1", "reason=parent-stopping", "err="},
		[]string{"actor=" + c.String(), "inc=1", "reason=normal", "err="},
	)
}

// SUP-300, ACT-050: once a shutdown has started, the guardians discard failed notices, as a
// stopping supervisor does, and the sweep stops the suspended child.
func TestSUP300NoRestartDuringShutdown(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
	var obs []string
	p := spawnR(t, sys, "p", &rProbe{obs: &obs})
	eng.RunUntilIdle()
	send(t, sys, p, any("panic"))
	if err := sys.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	sameObs(t, obs, []string{"started /user/p 1", "/user/p 1 got panic", "stopped /user/p shutdown"})
	if d := recordsOf(eng.Records(), "lockstep.directive"); len(d) != 0 {
		t.Errorf("lockstep.directive records %q, want none", d)
	}
}

// SUP-350, SUP-290: an actor in a restart delay when the shutdown starts is stopped without a
// Stopped call, and its pending begin never runs.
func TestSUP350ShutdownDuringBackoff(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(withBackoff(time.Second)))
	var obs []string
	p := spawnR(t, sys, "p", &rProbe{obs: &obs})
	send(t, sys, p, any("panic"))
	eng.RunUntilIdle()
	if err := sys.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	eng.Advance(2 * time.Second)
	sameObs(t, obs, []string{"started /user/p 1", "/user/p 1 got panic", "stopped /user/p restarting: lockstep: panic: panic"})
	sameRecords(t, recordsOf(eng.Records(), "lockstep.stop"), "lockstep.stop",
		[]string{"actor=" + p.String(), "inc=1", "reason=shutdown", "err="})
	if !eng.Stopped() {
		t.Error("the shutdown did not complete")
	}
}
