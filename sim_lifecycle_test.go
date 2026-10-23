// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// refusedReq is a request whose ReplyTo the target never gets to use.
type refusedReq struct{ rt lockstep.ReplyTo[int] }

// refusedStep is the message of the asking actor: the first one, with to set, starts the ask, and
// the piped resolution, with done set, carries its error.
type refusedStep struct {
	to   lockstep.PID[refusedReq]
	done bool
	err  error
}

// AT-MBX-24 (third case), MBX-092: an actor on n1 asks a held, full DropNewest mailbox on n2; the
// ask resolves with ErrMailboxFull as soon as the failure travels back, long before its timeout.
func TestATMBX24RefusedAskAcrossNodes(t *testing.T) {
	const askTimeout = 5 * time.Second
	var err error
	var resolved bool
	var started, at time.Duration
	var t2Addr string
	sim.Simulate(t, simOpts(10), func(s *sim.Sim) {
		err, resolved, started, at, t2Addr = nil, false, 0, 0, ""
		w := s.World()
		n1 := s.Node("n1", func(sys *lockstep.System) {
			simSpawn(sys, "a", &simActor[refusedStep]{receive: func(ctx *lockstep.Context[refusedStep], m refusedStep) {
				if m.done {
					resolved, err, at = true, m.err, time.Duration(w.Sim.Now())
					return
				}
				started = time.Duration(w.Sim.Now())
				r := lockstep.Ask(ctx, m.to, func(rt lockstep.ReplyTo[int]) refusedReq { return refusedReq{rt: rt} },
					lockstep.WithTimeout(askTimeout))
				lockstep.Pipe(ctx, r, func(_ int, err error) refusedStep { return refusedStep{done: true, err: err} })
			}})
		})
		n2 := s.Node("n2", func(sys *lockstep.System) {
			tp := simSpawn(sys, "t2", &simActor[refusedReq]{}, lockstep.WithMailbox(1, lockstep.DropNewest))
			t2Addr = tp.Address().String()
			_ = lockstep.Hold(sys, tp.Address())
			if err := lockstep.Send(sys, tp, refusedReq{}); err != nil { // fills the mailbox
				panic(err)
			}
		})
		w.RunFor(10 * time.Millisecond)
		a := lockstep.PIDAt[refusedStep]("n1", "/user/a")
		if e := sim.Send(s, "n1", a, refusedStep{to: lockstep.PIDAt[refusedReq]("n2", "/user/t2")}); e != nil {
			w.T().Fatal(e)
		}
		w.RunFor(askTimeout + time.Second)

		tt := w.T()
		if !resolved {
			tt.Fatalf("the ask never resolved")
		}
		// The ask names t2 by path, so the error names it without a uid.
		want := mailboxFullText(lockstep.PIDAt[refusedReq]("n2", "/user/t2").Address().String())
		if !errors.Is(err, lockstep.ErrMailboxFull) || err.Error() != want {
			tt.Errorf("ask resolved with %v, want an error wrapping ErrMailboxFull with text %q (t2 is %s)", err, want, t2Addr)
		}
		if took := at - started; took <= 0 || took >= 100*time.Millisecond {
			tt.Errorf("the ask resolved %v after it started, want a round trip of a few milliseconds, not its %v timeout", took, askTimeout)
		}
		if got := simRecords(w, n1, "lockstep.timeout"); len(got) != 0 {
			tt.Errorf("%d lockstep.timeout records on n1, want none", len(got))
		}
		if got := simRecords(w, n2, "lockstep.mailbox.full"); len(got) != 1 || simAttr(got[0], "action") != "dropped-newest" {
			tt.Errorf("mailbox.full records on n2 %v, want one dropped-newest", got)
		}
	})
}

// supGroup is the children of the AllForOne scenario, in spawn order.
var supGroup = []string{"c1", "c2", "c3"}

// errFail is the error C2 fails with.
var errFail = errors.New("e")

// supCmd tells a child to fail.
type supCmd struct{}

// allForOneStrategy returns AllForOne with MinBackoff 0 and a decider that always returns d.
func allForOneStrategy(d lockstep.Directive) lockstep.Strategy {
	st := lockstep.AllForOne(func(error) lockstep.Directive { return d })
	st.MinBackoff = 0
	return st
}

// determinismOpts returns the options of AT-SUP-37: 20 seeds, each run twice with the traces
// compared.
func determinismOpts() sim.Options {
	opts := simOpts(20)
	opts.CheckDeterminism = true
	return opts
}

// checkRunTwice fails t unless every one of the seeds that ran, 20 of them, ran its body twice.
func checkRunTwice(t *testing.T, runs map[uint64]int) {
	t.Helper()
	if len(runs) != 20 {
		t.Errorf("%d seeds ran, want 20", len(runs))
	}
	for seed, n := range runs {
		if n != 2 {
			t.Errorf("seed %#x ran %d times, want 2 (the run and its determinism check)", seed, n)
		}
	}
}

// AT-SUP-37, with the scenarios of AT-SUP-26 (SUP-260) and AT-SUP-28 (SUP-280, SUP-190) as they
// run in simulation. Every seed runs twice and faultline compares the trace hashes.
func TestATSUP37Determinism(t *testing.T) {
	// AT-SUP-26: C2 fails under AllForOne; C3, C2 and C1 stop in that order and, for a restart,
	// start again.
	for _, tc := range []struct {
		name      string
		directive lockstep.Directive
		verb      string
		stopped   []string
		started   int // Started calls of each of c1, c2 and c3
	}{
		{"all-for-one restart", lockstep.DirectiveRestart, "restart", []string{
			"/user/s/c3 restarting: e", "/user/s/c2 restarting: e", "/user/s/c1 restarting: e",
		}, 2},
		{"all-for-one stop", lockstep.DirectiveStop, "stop", []string{
			"/user/s/c3 failed: e", "/user/s/c2 failed: e", "/user/s/c1 failed: e",
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runs := make(map[uint64]int)
			sim.Simulate(t, determinismOpts(), func(s *sim.Sim) {
				runs[s.World().Seed()]++
				var stopped, starts []string
				n1 := s.Node("n1", func(sys *lockstep.System) {
					simSpawn(sys, "s", &simActor[supCmd]{started: func(ctx *lockstep.Context[supCmd]) {
						for _, name := range supGroup {
							simSpawn(ctx, name, &simActor[supCmd]{
								started: func(ctx *lockstep.Context[supCmd]) { starts = append(starts, ctx.Self().Address().Path()) },
								receive: func(ctx *lockstep.Context[supCmd], _ supCmd) { ctx.Fail(errFail) },
								stopped: func(ctx *lockstep.Context[supCmd], reason lockstep.StopReason) {
									stopped = append(stopped, ctx.Self().Address().Path()+" "+reason.String())
								},
							})
						}
					}}, lockstep.WithStrategy(allForOneStrategy(tc.directive)))
				})
				w := s.World()
				w.RunFor(100 * time.Millisecond)
				if err := sim.Send(s, "n1", lockstep.PIDAt[supCmd]("n1", "/user/s/c2"), supCmd{}); err != nil {
					w.T().Fatal(err)
				}
				w.RunFor(time.Second)

				tt := w.T()
				if !slices.Equal(stopped, tc.stopped) {
					tt.Errorf("Stopped observations %q, want %q", stopped, tc.stopped)
				}
				if len(starts) != len(supGroup)*tc.started {
					tt.Errorf("%d Started calls %q, want %d", len(starts), starts, len(supGroup)*tc.started)
				}
				for _, name := range supGroup {
					path := "/user/s/" + name
					n := 0
					for _, p := range starts {
						if p == path {
							n++
						}
					}
					if n != tc.started {
						tt.Errorf("%s started %d times, want %d", path, n, tc.started)
					}
				}
				dirs := simRecords(w, n1, "lockstep.directive")
				if len(dirs) != 1 || simAttr(dirs[0], "scope") != "all-for-one" || simAttr(dirs[0], "directive") != tc.verb {
					tt.Errorf("lockstep.directive records %v, want one with scope=all-for-one directive=%s", dirs, tc.verb)
				}
			})
			checkRunTwice(t, runs)
		})
	}

	// AT-SUP-28: C fails in Started three times in a row; each new incarnation starts 10ms, 20ms
	// and 40ms of virtual time after the previous one ended.
	t.Run("backoff", func(t *testing.T) {
		runs := make(map[uint64]int)
		sim.Simulate(t, determinismOpts(), func(s *sim.Sim) {
			runs[s.World().Seed()]++
			var starts, ends []time.Time
			s.Node("n1", func(sys *lockstep.System) {
				simSpawn(sys, "c", &simActor[supCmd]{
					started: func(ctx *lockstep.Context[supCmd]) {
						starts = append(starts, ctx.Now())
						if len(starts) <= 3 {
							panic("boom")
						}
					},
					stopped: func(ctx *lockstep.Context[supCmd], _ lockstep.StopReason) { ends = append(ends, ctx.Now()) },
				})
			})
			w := s.World()
			w.RunFor(time.Second)

			tt := w.T()
			if len(starts) != 4 || len(ends) != 3 {
				tt.Fatalf("%d starts and %d ends, want 4 and 3", len(starts), len(ends))
			}
			for i, want := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond} {
				if got := starts[i+1].Sub(ends[i]); got != want {
					tt.Errorf("incarnation %d started %v after incarnation %d ended, want %v", i+2, got, i+1, want)
				}
			}
		})
		checkRunTwice(t, runs)
	})
}
