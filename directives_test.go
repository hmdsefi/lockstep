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

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// deciding returns R0 whose decider returns d for every error.
func deciding(d Directive) Strategy {
	s := r0()
	s.Decider = func(error) Directive { return d }
	return s
}

// childFailedObs returns an onSignal hook that appends each ChildFailed to obs as
// "childfailed <child path> <directive> cleanup=<cleanup> <err>".
func childFailedObs(obs *[]string) func(*Context[any], Signal) {
	return func(_ *Context[any], sig Signal) {
		if cf, ok := sig.(ChildFailed); ok {
			err := "<nil>"
			if cf.Err != nil {
				err = cf.Err.Error()
			}
			*obs = append(*obs, "childfailed "+cf.Child.Path()+" "+cf.Directive.String()+
				" cleanup="+strconv.FormatBool(cf.Cleanup)+" "+err)
		}
	}
}

// escalatedRecord returns the attributes of the lockstep.directive record of a guardian that
// applies an escalation as a stop (SUP-300).
func escalatedRecord(sup, child Address, inc int, err string, restarts int, limit bool) []string {
	return directiveAttrs(sup, child, inc, "stop", err, restarts, 0, limit, true)
}

// matching returns the elements of obs that contain sub.
func matching(obs []string, sub string) []string {
	var out []string
	for _, o := range obs {
		if strings.Contains(o, sub) {
			out = append(out, o)
		}
	}
	return out
}

// spawnSupervised spawns, under the user guardian, a supervisor S named "s" with strategy st and
// child "c", both rProbes observing into obs; S's signal hook is onSignal. It runs eng until idle
// and returns both PIDs.
func spawnSupervised(t *testing.T, sys *System, eng *enginetest.Engine, st Strategy, obs *[]string,
	onSignal func(*Context[any], Signal),
) (s, c PID[any]) {
	t.Helper()
	s = spawnR(t, sys, "s", &rProbe{obs: obs, onSignal: onSignal, onStart: func(ctx *Context[any]) {
		c = spawnR(t, ctx, "c", &rProbe{obs: obs})
	}}, WithStrategy(st))
	eng.RunUntilIdle()
	return s, c
}

// AT-SUP-13: SUP-110, SUP-230. A panic in the child's Stopped during a restart is recorded with
// source stopped, the owned resources are still closed in reverse order and the child restarts.
// The supervisor's Signal gets the restart's ChildFailed in the deciding turn, then
// ChildFailed{Cleanup: true}; the cleanup failure is not decided.
func TestATSUP13PanicInStopped(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs, sobs, closed []string
	var c PID[any]
	spawnR(t, sys, "s", &rProbe{obs: new([]string), onSignal: childFailedObs(&sobs), onStart: func(ctx *Context[any]) {
		c = spawnR(t, ctx, "c", &rProbe{
			obs: &obs,
			onStart: func(ctx *Context[any]) {
				ctx.Own(&closer{name: "r1", log: &closed})
				ctx.Own(&closer{name: "r2", log: &closed})
			},
			onStop: func(*Context[any], StopReason) { panic("cleanup") },
		})
	}}, WithStrategy(r0()))
	eng.RunUntilIdle()
	send(t, sys, c, any("panic"))
	eng.RunUntilIdle()

	recs := eng.Records()
	if indexOf(recs, "lockstep.fail", "actor="+c.String(), "inc=1", "source=stopped", "err=lockstep: panic: cleanup") < 0 {
		t.Errorf("no lockstep.fail with source stopped: %q", recordsOf(recs, "lockstep.fail"))
	}
	sameObs(t, closed, []string{"r2", "r1"})
	if !slices.Contains(obs, "started /user/s/c 2") {
		t.Errorf("observations %q lack started /user/s/c 2", obs)
	}
	sameObs(t, sobs, []string{
		"childfailed /user/s/c restart cleanup=false lockstep: panic: panic",
		"childfailed /user/s/c Directive(0) cleanup=true lockstep: panic: cleanup",
	})
	if d := recordsOf(recs, "lockstep.directive"); len(d) != 1 {
		t.Errorf("lockstep.directive records %q, want one", d)
	}
}

// AT-SUP-17: SUP-160, SUP-320. A Suspended child keeps the Terminated that reaches it. After a
// resume it gets it; when the restart ends its incarnation instead, the kept signal is recorded as
// lockstep.drop reason not-running and never delivered.
func TestATSUP17SuspendedKeepsSignals(t *testing.T) {
	for _, d := range []Directive{DirectiveResume, DirectiveRestart} {
		t.Run(d.String(), func(t *testing.T) {
			sys, eng := newManualSystem(t)
			var obs []string
			w := spawnR(t, sys, "w", &rProbe{obs: new([]string)})
			eng.RunUntilIdle()
			var c PID[any]
			s := spawnR(t, sys, "s", &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
				c = spawnR(t, ctx, "c", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
					if ctx.inc == 1 {
						if err := Watch(ctx, w.Address()); err != nil {
							panic(err)
						}
					}
				}})
			}}, WithStrategy(deciding(d)))
			eng.RunUntilIdle()
			release := Hold(sys, s.Address())
			send(t, sys, c, any("panic"))
			eng.RunUntilIdle() // C is Suspended; its notice waits in S's system queue
			if err := Stop(sys, w.Address()); err != nil {
				t.Fatal(err)
			}
			eng.RunUntilIdle() // W's Terminated reaches C, which keeps it
			if st, _ := cellOf(t, sys, c).lifecycle(); st != supervise.Suspended {
				t.Fatalf("C is %v, want suspended", st)
			}
			if got := matching(obs, " terminated "); len(got) != 0 {
				t.Fatalf("C observed %q while suspended", got)
			}
			release()
			eng.RunUntilIdle()

			terms := matching(obs, " terminated ")
			drops := recordsOf(eng.Records(), "lockstep.drop")
			term := Terminated{Addr: w.Address(), Reason: Normal}
			if d == DirectiveResume {
				sameObs(t, terms, []string{"/user/s/c terminated /user/w normal"})
				sameRecords(t, drops, "lockstep.drop")
				if _, inc := cellOf(t, sys, c).lifecycle(); inc != 1 {
					t.Errorf("C inc = %d, want 1", inc)
				}
				return
			}
			sameObs(t, terms, nil)
			sameRecords(t, drops, "lockstep.drop",
				[]string{"to=" + c.String(), "reason=not-running", "msg=" + sys.eng.Tracer.Describe(term)})
		})
	}
}

// SUP-160, ACT-630: when a restart ends a Suspended incarnation, a kept Terminated whose watch the
// incarnation had cancelled is skipped without a record, as a delivery would skip it; a kept
// Terminated of a current watch is recorded as lockstep.drop reason not-running.
func TestDropKeptSkipsStaleTerminated(t *testing.T) {
	sys, eng := newManualSystem(t)
	cancelled := PIDAt[any]("local", "/user/gone1").Address()
	current := PIDAt[any]("local", "/user/gone2").Address()
	var c PID[any]
	spawnR(t, sys, "s", &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
		c = spawnR(t, ctx, "c", &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
			if ctx.inc != 1 {
				return
			}
			// Neither address is alive: each Watch queues a Terminated with reason NoActor.
			for _, a := range []Address{cancelled, current} {
				if err := Watch(ctx, a); err != nil {
					panic(err)
				}
			}
			Unwatch(ctx, cancelled)
			panic("boom") // C is Suspended before it pops the two Terminated signals
		}})
	}}, WithStrategy(deciding(DirectiveRestart)))
	eng.RunUntilIdle()

	if _, inc := cellOf(t, sys, c).lifecycle(); inc != 2 {
		t.Fatalf("C inc = %d, want 2 after the restart", inc)
	}
	term := Terminated{Addr: current, Reason: NoActor}
	sameRecords(t, recordsOf(eng.Records(), "lockstep.drop"), "lockstep.drop",
		[]string{"to=" + c.String(), "reason=not-running", "msg=" + sys.eng.Tracer.Describe(term)})
}

// SUP-160, SUP-210: a Suspended supervisor keeps its children's failed and ended notices and
// handles them in arrival order after its resume. S decides C2's restart and pushes end, then
// escalates C1's failure; while S is Suspended, C2's ended notice and C3's failed notice arrive.
// Once G resumes S, S resumes C1, starts C2's backoff and decides C3's failure: C2 and C3 restart.
func TestSUP160SuspendedKeepsChildNotices(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	var s, c1, c2, c3 PID[any]
	st := withBackoff(time.Second)
	st.Decider = func(err error) Directive {
		if errors.Is(err, errX) {
			return DirectiveRestart
		}
		return DirectiveEscalate
	}
	g := spawnR(t, sys, "g", &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
		s = spawnR(t, ctx, "s", &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
			c1 = spawnR(t, ctx, "c1", &rProbe{obs: &obs})
			c2 = spawnR(t, ctx, "c2", &rProbe{obs: &obs})
			c3 = spawnR(t, ctx, "c3", &rProbe{obs: &obs})
		}}, WithStrategy(st))
	}}, WithStrategy(deciding(DirectiveResume)))
	eng.RunUntilIdle()

	releaseG := Hold(sys, g.Address())
	releaseS := Hold(sys, s.Address())
	send(t, sys, c2, any("fail"))
	send(t, sys, c1, any("panic"))
	eng.RunUntilIdle() // both notices wait in S's system queue, C2's first
	releaseS()
	eng.RunUntilIdle() // S restarts C2, escalates C1 and keeps C2's ended notice
	send(t, sys, c3, any("fail"))
	eng.RunUntilIdle() // S keeps C3's failed notice
	if st, _ := cellOf(t, sys, s).lifecycle(); st != supervise.Suspended {
		t.Fatalf("S is %v, want suspended", st)
	}
	if st, _ := cellOf(t, sys, c2).lifecycle(); st != supervise.Restarting {
		t.Fatalf("C2 is %v, want restarting", st)
	}
	if n := countOf(eng.Labels(), "lockstep/backoff"); n != 0 {
		t.Fatalf("a backoff timer was armed while S was suspended: %q", eng.Labels())
	}
	releaseG()
	eng.RunUntilIdle()
	eng.Advance(time.Second)

	for _, k := range []struct {
		pid  PID[any]
		want uint32
	}{{s, 1}, {c1, 1}, {c2, 2}, {c3, 2}} {
		if st, inc := cellOf(t, sys, k.pid).lifecycle(); st != supervise.Running || inc != k.want {
			t.Errorf("%v is %v at inc %d, want running at inc %d", k.pid, st, inc, k.want)
		}
	}
	if slices.Contains(obs, "stopped /user/g/s/c1 restarting: lockstep: panic: panic") {
		t.Errorf("observations %q: C1 was restarted, want it resumed", obs)
	}
	recs := eng.Records()
	if indexOf(recs, "lockstep.fail", "actor="+s.String(), "source=escalate") < 0 {
		t.Errorf("no lockstep.fail for S with source escalate: %q", recordsOf(recs, "lockstep.fail"))
	}
	sameRecords(t, recordsOf(recs, "lockstep.directive"), "lockstep.directive",
		directiveRecord(s.Address(), c2.Address(), 1, "restart", "errX", 1, time.Second, false),
		directiveRecord(s.Address(), c1.Address(), 1, "escalate", "lockstep: panic: panic", 0, 0, false),
		directiveRecord(g.Address(), s.Address(), 1, "resume", "lockstep: panic: panic", 0, 0, false),
		directiveRecord(s.Address(), c3.Address(), 1, "restart", "errX", 1, time.Second, false),
	)
}

// AT-SUP-19: SUP-180. A decider that returns a value outside DirectiveRestart to DirectiveResume,
// or panics, fails the supervisor with source decider. The child stays Suspended until the
// supervisor's own supervisor resumes the supervisor, which then resumes the child.
func TestATSUP19DeciderResults(t *testing.T) {
	for _, tc := range []struct {
		name    string
		decider func(error) Directive
		err     string
	}{
		{"Directive(0)", func(error) Directive { return 0 }, "lockstep: invalid directive Directive(0)"},
		{"Directive(9)", func(error) Directive { return 9 }, "lockstep: invalid directive Directive(9)"},
		{"panic", func(error) Directive { panic("decider") }, "lockstep: panic: decider"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c PID[any]
			var sys *System
			var childAtDecision supervise.State
			var decided error
			g := r0()
			g.Decider = func(err error) Directive {
				childAtDecision, _ = cellOf(t, sys, c).lifecycle()
				decided = err
				return DirectiveResume
			}
			sys, eng := newManualSystem(t, WithGuardianStrategy(g))
			var obs []string
			st := r0()
			st.Decider = tc.decider
			s, c2 := spawnSupervised(t, sys, eng, st, &obs, nil)
			c = c2
			send(t, sys, c, any("panic"))
			eng.RunUntilIdle()

			sameRecords(t, recordsOf(eng.Records(), "lockstep.fail"), "lockstep.fail",
				[]string{"actor=" + c.String(), "inc=1", "source=receive", "err=lockstep: panic: panic"},
				[]string{"actor=" + s.String(), "inc=1", "source=decider", "err=" + tc.err})
			if decided == nil || decided.Error() != tc.err {
				t.Errorf("the guardian decided error %v, want %q", decided, tc.err)
			}
			var perr *PanicError
			if isPanic := errors.As(decided, &perr); isPanic != (tc.name == "panic") {
				t.Errorf("the decider failure %v is a *PanicError: %v", decided, isPanic)
			}
			if childAtDecision != supervise.Suspended {
				t.Errorf("C was %v while S's failure was decided, want suspended", childAtDecision)
			}
			if d := recordsOf(eng.Records(), "lockstep.directive"); len(d) != 1 {
				t.Errorf("lockstep.directive records %q, want only the guardian's", d)
			}
			send(t, sys, c, any("x"))
			eng.RunUntilIdle()
			if !slices.Contains(obs, "/user/s/c 1 got x") || len(matching(obs, "stopped ")) != 0 {
				t.Errorf("observations %q: want C resumed at inc 1, nothing stopped", obs)
			}
		})
	}
}

// AT-SUP-21: SUP-200, SUP §8. DirectiveStop stops the child for good with Failed(err): Stopped
// and the watchers' Terminated get that reason, and lockstep.stop records reason failed with the
// error; a normal stop records reason normal and an empty err.
func TestATSUP21StopDirective(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(deciding(DirectiveStop)))
	var obs []string
	var terms []Terminated
	c := spawnR(t, sys, "c", &rProbe{obs: &obs})
	n := spawnR(t, sys, "n", &rProbe{obs: &obs})
	eng.RunUntilIdle()
	spawnR(t, sys, "w", &rProbe{
		obs: new([]string),
		onStart: func(ctx *Context[any]) {
			if err := Watch(ctx, c.Address()); err != nil {
				panic(err)
			}
		},
		onSignal: func(_ *Context[any], sig Signal) {
			if tm, ok := sig.(Terminated); ok {
				terms = append(terms, tm)
			}
		},
	})
	eng.RunUntilIdle()
	send(t, sys, c, any("panic"))
	eng.RunUntilIdle()
	if err := Stop(sys, n.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	if !slices.Contains(obs, "stopped /user/c failed: lockstep: panic: panic") {
		t.Errorf("observations %q lack stopped /user/c failed: lockstep: panic: panic", obs)
	}
	if len(terms) != 1 || terms[0].Addr != c.Address() || terms[0].Reason.Kind != StopFailed {
		t.Fatalf("Terminated signals %+v, want one for C with kind failed", terms)
	}
	var perr *PanicError
	if !errors.As(terms[0].Reason.Err, &perr) {
		t.Fatalf("Terminated reason error %v is not a *PanicError", terms[0].Reason.Err)
	}
	recs := eng.Records()
	sameRecords(t, recordsOf(recs, "lockstep.stop"), "lockstep.stop",
		[]string{"actor=" + c.String(), "inc=1", "reason=failed", "err=" + sys.eng.Tracer.Describe(terms[0].Reason.Err)},
		[]string{"actor=" + n.String(), "inc=1", "reason=normal", "err="})
	sameRecords(t, recordsOf(recs, "lockstep.directive"), "lockstep.directive",
		directiveRecord(sys.user, c.Address(), 1, "stop", "lockstep: panic: panic", 0, 0, false))
}

// counter is AT-SUP-22's actor: it counts the messages it receives that are not "panic", panics
// on "panic" and reports its count on "count". A restart would give a new counter.
type counter struct {
	n       int
	counts  *[]int
	stopped *int
}

func (a *counter) Receive(_ *Context[any], msg any) {
	switch msg {
	case "panic":
		panic("panic")
	case "count":
		*a.counts = append(*a.counts, a.n)
	default:
		a.n++
	}
}

func (a *counter) Stopped(*Context[any], StopReason) { *a.stopped++ }

// AT-SUP-22: SUP-210. DirectiveResume keeps the incarnation and its state: the failing message is
// lost, nothing is stopped, inc stays 1, and resumes are not counted, so 100 of them never
// escalate.
func TestATSUP22Resume(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(deciding(DirectiveResume)))
	var counts []int
	stopped := 0
	c, err := Spawn(sys, "c", func() Actor[any] { return &counter{counts: &counts, stopped: &stopped} })
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"a", "panic", "b", "count"} {
		send(t, sys, c, any(m))
		eng.RunUntilIdle()
	}
	for range 100 {
		send(t, sys, c, any("panic"))
		eng.RunUntilIdle()
	}
	send(t, sys, c, any("count"))
	eng.RunUntilIdle()

	if !slices.Equal(counts, []int{2, 2}) {
		t.Errorf("counts %v, want [2 2]", counts)
	}
	if stopped != 0 {
		t.Errorf("Stopped called %d times, want 0", stopped)
	}
	if st, inc := cellOf(t, sys, c).lifecycle(); st != supervise.Running || inc != 1 {
		t.Errorf("C is %v at inc %d, want running at inc 1", st, inc)
	}
	ds := recordsOf(eng.Records(), "lockstep.directive")
	if len(ds) != 101 {
		t.Fatalf("%d lockstep.directive records, want 101", len(ds))
	}
	want := directiveRecord(sys.user, c.Address(), 1, "resume", "lockstep: panic: panic", 0, 0, false)
	for i, d := range ds {
		if !slices.Equal(d, want) {
			t.Fatalf("lockstep.directive %d = %q, want %q", i, d, want)
		}
	}
}

// AT-SUP-23: SUP-220. DirectiveEscalate fails the supervisor P with the child's error and source
// escalate. When P's supervisor restarts P, C is stopped with it; when it resumes P, P resumes C.
func TestATSUP23Escalate(t *testing.T) {
	for _, d := range []Directive{DirectiveRestart, DirectiveResume} {
		t.Run(d.String(), func(t *testing.T) {
			sys, eng := newManualSystem(t)
			var obs []string
			var p, c PID[any]
			spawnR(t, sys, "g", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
				p = spawnR(t, ctx, "p", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
					c = spawnR(t, ctx, "c", &rProbe{obs: &obs})
				}}, WithStrategy(deciding(DirectiveEscalate)))
			}}, WithStrategy(deciding(d)))
			eng.RunUntilIdle()
			c1 := c
			send(t, sys, c1, any("panic"))
			eng.RunUntilIdle()

			const perr = "lockstep: panic: panic"
			recs := eng.Records()
			if indexOf(recs, "lockstep.fail", "actor="+p.String(), "inc=1", "source=escalate", "err="+perr) < 0 {
				t.Errorf("no lockstep.fail for P with source escalate: %q", recordsOf(recs, "lockstep.fail"))
			}
			stops := matching(obs, "stopped ")
			if d == DirectiveResume {
				sameObs(t, stops, nil)
				for _, pid := range []PID[any]{p, c1} {
					if st, inc := cellOf(t, sys, pid).lifecycle(); st != supervise.Running || inc != 1 {
						t.Errorf("%v is %v at inc %d, want running at inc 1", pid, st, inc)
					}
				}
				send(t, sys, c1, any("x"))
				eng.RunUntilIdle()
				if !slices.Contains(obs, "/user/g/p/c 1 got x") {
					t.Errorf("observations %q: C did not resume", obs)
				}
				return
			}
			sameObs(t, stops, []string{"stopped /user/g/p/c parent-stopping", "stopped /user/g/p restarting: " + perr})
			if i := slices.Index(obs, "started /user/g/p 2"); i < slices.Index(obs, "stopped /user/g/p restarting: "+perr) {
				t.Errorf("observations %q: want started /user/g/p 2 after P's Stopped", obs)
			}
		})
	}
}

// AT-SUP-24: SUP-230. After pushing the command of a restart, a stop or a resume, the supervisor
// delivers ChildFailed with the applied directive to its own Signal, in the same turn; for an
// escalation it delivers nothing.
func TestATSUP24ChildFailedAfterDirective(t *testing.T) {
	for _, d := range []Directive{DirectiveRestart, DirectiveStop, DirectiveResume, DirectiveEscalate} {
		t.Run(d.String(), func(t *testing.T) {
			sys, eng := newManualSystem(t)
			var sobs []string
			var c PID[any]
			queued := -1
			obsCF := childFailedObs(&sobs)
			_, c = spawnSupervised(t, sys, eng, deciding(d), new([]string), func(ctx *Context[any], sig Signal) {
				if _, ok := sig.(ChildFailed); ok {
					queued = cellOf(t, sys, c).sysq.Len() // the command waits in C's system queue
				}
				obsCF(ctx, sig)
			})
			send(t, sys, c, any("panic"))
			eng.RunUntilIdle()
			if d == DirectiveEscalate {
				sameObs(t, sobs, nil)
				return
			}
			sameObs(t, sobs, []string{"childfailed /user/s/c " + d.String() + " cleanup=false lockstep: panic: panic"})
			if queued != 1 {
				t.Errorf("C's system queue held %d messages when ChildFailed was delivered, want the command", queued)
			}
		})
	}
}

// AT-SUP-27, runtime part: SUP-270, SUP-180. With MaxRestarts 2 the third failure in the window
// is not restarted: S records directive escalate with limit true and fails with source escalate.
func TestATSUP27IntensityRuntime(t *testing.T) {
	sys, eng := newManualSystem(t)
	st := r0()
	st.MaxRestarts = 2
	st.Window = time.Minute
	s, c := spawnSupervised(t, sys, eng, st, new([]string), nil)
	for range 3 {
		send(t, sys, c, any("fail"))
		eng.RunUntilIdle()
	}
	recs := eng.Records()
	ds := recordsOf(recs, "lockstep.directive")
	if len(ds) < 3 {
		t.Fatalf("lockstep.directive records %q, want at least 3", ds)
	}
	sameRecords(t, ds[:3], "lockstep.directive",
		directiveRecord(s.Address(), c.Address(), 1, "restart", "errX", 1, 0, false),
		directiveRecord(s.Address(), c.Address(), 2, "restart", "errX", 2, 0, false),
		directiveRecord(s.Address(), c.Address(), 3, "escalate", "errX", 2, 0, true),
	)
	if indexOf(recs, "lockstep.fail", "actor="+s.String(), "source=escalate", "err=errX") < 0 {
		t.Errorf("no lockstep.fail for S with source escalate: %q", recordsOf(recs, "lockstep.fail"))
	}
}

// SUP §8: lockstep.directive's restarts is the count in the window for every directive: a stop,
// resume or escalation from the decider records the restarts already counted, without counting
// one.
func TestSUP180DirectiveRecordsCountInWindow(t *testing.T) {
	sys, eng := newManualSystem(t)
	st := r0()
	st.Decider = func(err error) Directive {
		if errors.Is(err, errX) {
			return DirectiveRestart
		}
		return DirectiveResume
	}
	s, c := spawnSupervised(t, sys, eng, st, new([]string), nil)
	for _, m := range []string{"fail", "fail", "panic"} {
		send(t, sys, c, any(m))
		eng.RunUntilIdle()
	}
	sameRecords(t, recordsOf(eng.Records(), "lockstep.directive"), "lockstep.directive",
		directiveRecord(s.Address(), c.Address(), 1, "restart", "errX", 1, 0, false),
		directiveRecord(s.Address(), c.Address(), 2, "restart", "errX", 2, 0, false),
		directiveRecord(s.Address(), c.Address(), 3, "resume", "lockstep: panic: panic", 2, 0, false),
	)
}

// AT-SUP-30: SUP-300. On a system built by New, a guardian applies DirectiveEscalate as
// DirectiveStop: the top-level actor stops with Failed(err), and the system keeps running.
func TestATSUP30GuardianEscalation(t *testing.T) {
	sys := newLocalSystem(t, WithGuardianStrategy(OneForOne(func(error) Directive { return DirectiveEscalate })))
	reasons := make(chan string, 1)
	p := spawnR(t, sys, "p", &rProbe{obs: new([]string), onStop: func(_ *Context[any], r StopReason) { reasons <- r.String() }})
	send(t, sys, p, any("panic"))
	select {
	case r := <-reasons:
		if r != "failed: lockstep: panic: panic" {
			t.Errorf("P stopped with %q, want failed: lockstep: panic: panic", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("P did not stop within 10s")
	}
	q, _ := spawnProbe[any](t, sys, "q", nil)
	if got := inProbe(t, sys, q, func(ctx *Context[any]) any { return ctx.Self() }); got != q {
		t.Errorf("the new top-level actor answered %v, want %v", got, q)
	}
}

// SUP-300, SUP §8: a guardian's escalation, from its decider or from restart intensity, is applied
// as a stop and recorded as directive stop with escalated true.
func TestSUP300GuardianEscalationRecord(t *testing.T) {
	t.Run("decider", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithGuardianStrategy(deciding(DirectiveEscalate)))
		var obs []string
		p := spawnR(t, sys, "p", &rProbe{obs: &obs})
		send(t, sys, p, any("panic"))
		eng.RunUntilIdle()
		sameRecords(t, recordsOf(eng.Records(), "lockstep.directive"), "lockstep.directive",
			escalatedRecord(sys.user, p.Address(), 1, "lockstep: panic: panic", 0, false))
		sameObs(t, matching(obs, "stopped "), []string{"stopped /user/p failed: lockstep: panic: panic"})
	})
	t.Run("intensity", func(t *testing.T) {
		s := r0()
		s.MaxRestarts = 1
		sys, eng := newManualSystem(t, WithGuardianStrategy(s))
		var obs []string
		p := spawnR(t, sys, "p", &rProbe{obs: &obs})
		for range 2 {
			send(t, sys, p, any("fail"))
			eng.RunUntilIdle()
		}
		sameRecords(t, recordsOf(eng.Records(), "lockstep.directive"), "lockstep.directive",
			directiveRecord(sys.user, p.Address(), 1, "restart", "errX", 1, 0, false),
			escalatedRecord(sys.user, p.Address(), 2, "errX", 1, true))
		sameObs(t, matching(obs, "stopped "), []string{"stopped /user/p restarting: errX", "stopped /user/p failed: errX"})
	})
	t.Run("decider failure", func(t *testing.T) {
		s := r0()
		s.Decider = func(error) Directive { panic("decider") }
		sys, eng := newManualSystem(t, WithGuardianStrategy(s))
		var obs []string
		p := spawnR(t, sys, "p", &rProbe{obs: &obs})
		send(t, sys, p, any("fail"))
		eng.RunUntilIdle()
		recs := eng.Records()
		if indexOf(recs, "lockstep.fail", "actor="+sys.user.String(), "inc=0", "source=decider", "err=lockstep: panic: decider") < 0 {
			t.Errorf("no lockstep.fail for the user guardian with source decider: %q", recordsOf(recs, "lockstep.fail"))
		}
		sameRecords(t, recordsOf(recs, "lockstep.directive"), "lockstep.directive",
			escalatedRecord(sys.user, p.Address(), 1, "errX", 0, false))
		sameObs(t, matching(obs, "stopped "), []string{"stopped /user/p failed: errX"})
	})
}

// SUP-320, SUP-110: a ChildFailed for a cleanup failure that reaches a cell outside Running is
// recorded as lockstep.drop reason not-running and not delivered: here C is Restarting, or
// Stopping, while it stops its child G, whose Stopped panics.
func TestSUP320SignalOutsideRunning(t *testing.T) {
	for _, tc := range []struct {
		name string
		act  func(t *testing.T, sys *System, c PID[any])
	}{
		{"restarting", func(t *testing.T, sys *System, c PID[any]) { send(t, sys, c, any("panic")) }},
		{"stopping", func(t *testing.T, sys *System, c PID[any]) {
			if err := Stop(sys, c.Address()); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sys, eng := newManualSystem(t, WithGuardianStrategy(r0()))
			var cobs []string
			var g PID[any]
			var gerr error
			c := spawnR(t, sys, "c", &rProbe{obs: new([]string), onSignal: childFailedObs(&cobs), onStart: func(ctx *Context[any]) {
				if ctx.inc == 1 {
					g = spawnR(t, ctx, "g", &rProbe{obs: new([]string), onStop: func(ctx *Context[any], _ StopReason) {
						gerr = errors.New("cleanup")
						ctx.Fail(gerr)
					}})
				}
			}})
			eng.RunUntilIdle()
			tc.act(t, sys, c)
			eng.RunUntilIdle()
			sameObs(t, cobs, nil)
			cf := ChildFailed{Child: g.Address(), Err: gerr, Cleanup: true}
			sameRecords(t, recordsOf(eng.Records(), "lockstep.drop"), "lockstep.drop",
				[]string{"to=" + c.String(), "reason=not-running", "msg=" + sys.eng.Tracer.Describe(cf)})
		})
	}
}

// SUP-320, MSG-810: a ChildFailed for a cleanup failure that reaches a running supervisor is
// delivered to its Signal.
func TestSUP110ChildFailedCleanupDelivered(t *testing.T) {
	sys, eng := newManualSystem(t)
	var sobs []string
	var c PID[any]
	spawnR(t, sys, "s", &rProbe{obs: new([]string), onSignal: childFailedObs(&sobs), onStart: func(ctx *Context[any]) {
		c = spawnR(t, ctx, "c", &rProbe{obs: new([]string), onStop: func(ctx *Context[any], _ StopReason) { ctx.Fail(errX) }})
	}})
	eng.RunUntilIdle()
	if err := Stop(sys, c.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	sameObs(t, sobs, []string{"childfailed /user/s/c Directive(0) cleanup=true errX"})
}

// SUP-350, SUP-290: a system actor whose guardian backoff ends while the shutdown stops the user
// guardian's children gets no new incarnation; the sweep then stops it with no Stopped call.
func TestSUP350GuardianBackoffEndsDuringShutdown(t *testing.T) {
	sys, eng := newManualSystem(t)
	var obs []string
	u := spawnR(t, sys, "u", &rProbe{obs: &obs})
	y, err := spawnSystem(sys, "y", actorOf[any](&rProbe{obs: &obs}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, y, any("panic"))
	eng.RunUntilIdle() // Y waits 10ms for its begin
	release := Hold(sys, u.Address())
	if err := sys.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle() // the sweep waits for U, which is held
	eng.Advance(10 * time.Millisecond)
	if slices.Contains(obs, "started /system/y 2") {
		t.Fatalf("observations %q: Y restarted during the shutdown", obs)
	}
	release()
	eng.RunUntilIdle()
	sameObs(t, matching(obs, "/system/y"), []string{
		"started /system/y 1",
		"/system/y 1 got panic",
		"stopped /system/y restarting: lockstep: panic: panic",
	})
	if !eng.Stopped() {
		t.Error("the shutdown did not complete")
	}
}

// SUP-220, SUP-180: when a supervisor resumes, a child whose escalated failure happened in Started
// has no initialized state to resume, so the supervisor restarts it instead, as Normalize turns a
// decider's DirectiveResume for that source into DirectiveRestart.
func TestSUP220ResumedSupervisorRestartsStartedFailure(t *testing.T) {
	sys, eng := newManualSystem(t, WithGuardianStrategy(deciding(DirectiveResume)))
	var obs []string
	var c PID[any]
	s := spawnR(t, sys, "s", &rProbe{obs: new([]string), onStart: func(ctx *Context[any]) {
		c = spawnR(t, ctx, "c", &rProbe{obs: &obs, onStart: func(ctx *Context[any]) {
			if ctx.inc == 1 {
				panic("boom")
			}
		}})
	}}, WithStrategy(deciding(DirectiveEscalate)))
	eng.RunUntilIdle()

	if st, inc := cellOf(t, sys, c).lifecycle(); st != supervise.Running || inc != 2 {
		t.Errorf("C is %v at inc %d, want running at inc 2", st, inc)
	}
	const boom = "lockstep: panic: boom"
	sameObs(t, obs, []string{"started /user/s/c 1", "stopped /user/s/c restarting: " + boom, "started /user/s/c 2"})
	sameRecords(t, recordsOf(eng.Records(), "lockstep.directive"), "lockstep.directive",
		directiveRecord(s.Address(), c.Address(), 1, "escalate", boom, 0, 0, false),
		directiveRecord(sys.user, s.Address(), 1, "resume", boom, 0, 0, false),
		directiveRecord(s.Address(), c.Address(), 1, "restart", boom, 1, 0, false),
	)
}
