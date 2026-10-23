// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// tick is the message of the timers under test.
type tick struct{}

// AT-TMR-01, TMR-010: ctx.Now reads the node's virtual clock: kernel.Epoch plus the virtual time,
// the same value for the whole of a handler, and in UTC.
func TestATTMR01NowInSimulation(t *testing.T) {
	sim.Simulate(t, simOpts(3), func(s *sim.Sim) {
		var startedAt kernel.Time
		var first, second, third time.Time
		s.Node("n1", func(sys *lockstep.System) {
			simSpawn(sys, "a", &simActor[tick]{
				started: func(ctx *lockstep.Context[tick]) {
					startedAt, first = s.World().Sim.Now(), ctx.Now()
					ctx.After(1500*time.Millisecond, tick{})
				},
				receive: func(ctx *lockstep.Context[tick], _ tick) { second, third = ctx.Now(), ctx.Now() },
			})
		})
		s.World().RunFor(2 * time.Second)

		tt := s.World().T()
		if startedAt != 0 {
			tt.Errorf("Started ran at virtual time %v, want 0 for a node booted at time 0", startedAt)
		}
		if want := kernel.Epoch.Add(time.Duration(startedAt)); !first.Equal(want) {
			tt.Errorf("Now in Started = %v, want kernel.Epoch plus the virtual time: %v", first, want)
		}
		if !second.Equal(third) {
			tt.Errorf("the two observations in the tick handler differ: %v and %v", second, third)
		}
		if got := second.Sub(first); got != 1500*time.Millisecond {
			tt.Errorf("tick observed %v after Started, want 1.5s", got)
		}
		if first.Location() != time.UTC || second.Location() != time.UTC {
			tt.Errorf("Location() is %v and %v, want UTC", first.Location(), second.Location())
		}
	})
}

// AT-TMR-02, TMR-010, TMR-020: a clock jump moves what Now reads, not when a timer fires, and
// neither does clock drift.
func TestATTMR02ClockJumpAndDrift(t *testing.T) {
	for _, tc := range []struct {
		name string
		act  func(n *kernel.Node) // at virtual time 1 s
		want func(nodeNow time.Time) time.Time
	}{
		// The jump moves Now to Epoch plus 10 s of virtual time plus the 5 s jump.
		{"jump", func(n *kernel.Node) { n.JumpClock(5 * time.Second) },
			func(time.Time) time.Time { return kernel.Epoch.Add(15 * time.Second) }},
		// The drift makes Now differ from Epoch plus the virtual time; the engine reads the node's clock.
		{"drift", func(n *kernel.Node) { n.SetDrift(100000) },
			func(nodeNow time.Time) time.Time { return nodeNow }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := simOpts(3)
			opts.Duration = 20 * time.Second
			sim.Simulate(t, opts, func(s *sim.Sim) {
				w := s.World()
				var firedAt kernel.Time
				var observed, nodeNow time.Time
				var handled int
				var n1 *kernel.Node
				n1 = s.Node("n1", func(sys *lockstep.System) {
					handled = 0
					simSpawn(sys, "a", &simActor[tick]{
						started: func(ctx *lockstep.Context[tick]) { ctx.After(10*time.Second, tick{}) },
						receive: func(ctx *lockstep.Context[tick], _ tick) {
							handled++
							firedAt, observed, nodeNow = w.Sim.Now(), ctx.Now(), n1.Now()
						},
					})
				})
				w.RunFor(time.Second)
				tc.act(n1)
				w.RunFor(10 * time.Second)

				tt := w.T()
				if handled != 1 {
					tt.Fatalf("tick handled %d times, want once", handled)
				}
				if firedAt != kernel.Time(10*time.Second) {
					tt.Errorf("tick handled at virtual time %v, want 10s", firedAt)
				}
				if want := tc.want(nodeNow); !observed.Equal(want) {
					tt.Errorf("tick observed Now %v, want %v", observed, want)
				}
				if tc.name == "drift" && observed.Equal(kernel.Epoch.Add(10*time.Second)) {
					tt.Errorf("tick observed the undrifted clock %v", observed)
				}
			})
		})
	}
}

// draw tells an actor to draw from its random stream; fail makes it panic.
type draw struct{ fail bool }

// drawer returns an actor that appends a draw of ctx.Rand to *draws, tagged with the actor's
// path, for every draw message, and panics for a fail message.
func drawer(draws *[]drawn) *simActor[draw] {
	return &simActor[draw]{receive: func(ctx *lockstep.Context[draw], m draw) {
		if m.fail {
			panic("fail")
		}
		*draws = append(*draws, drawn{ctx.Self().Address().Path(), ctx.Rand().Uint64()})
	}}
}

// drawn is one observed draw.
type drawn struct {
	path  string
	value uint64
}

// values returns the values of the draws at path, in order.
func values(draws []drawn, path string) []uint64 {
	var out []uint64
	for _, d := range draws {
		if d.path == path {
			out = append(out, d.value)
		}
	}
	return out
}

// sendDraw sends m to the actor at path on n1 and runs the simulation for 100 ms.
func sendDraw(s *sim.Sim, path string, m draw) {
	if err := sim.Send(s, "n1", lockstep.PIDAt[draw]("n1", path), m); err != nil {
		s.World().T().Fatal(err)
	}
	s.World().RunFor(100 * time.Millisecond)
}

// AT-TMR-13, TMR-110: ctx.Rand is the kernel's stream for lockstep/<node>/<epoch>/<path>/<inc>;
// a restart of the actor moves to inc 2, a child has its own path, and a restart of the node
// moves every stream to epoch 2.
func TestATTMR13StreamLabel(t *testing.T) {
	sim.Simulate(t, simOpts(10), func(s *sim.Sim) {
		var draws []drawn
		s.Node("n1", func(sys *lockstep.System) {
			simSpawn(sys, "r", &simActor[draw]{
				started: func(ctx *lockstep.Context[draw]) {
					simSpawn(ctx, "c", &simActor[draw]{started: func(ctx *lockstep.Context[draw]) {
						draws = append(draws, drawn{ctx.Self().Address().Path(), ctx.Rand().Uint64()})
					}})
				},
				receive: drawer(&draws).receive,
			})
		})
		w := s.World()
		n1 := w.Sim.Lookup("n1")
		w.RunFor(10 * time.Millisecond)
		sendDraw(s, "/user/r", draw{})
		sendDraw(s, "/user/r", draw{})
		sendDraw(s, "/user/r", draw{fail: true}) // r restarts after its backoff of 10ms
		sendDraw(s, "/user/r", draw{})
		n1.Crash()
		w.RunFor(10 * time.Millisecond)
		n1.Restart()
		w.RunFor(10 * time.Millisecond)
		sendDraw(s, "/user/r", draw{})

		tt := w.T()
		r1 := simExpectStream(w, "lockstep/n1/1/user/r/1")
		r2 := simExpectStream(w, "lockstep/n1/1/user/r/2")
		c1 := simExpectStream(w, "lockstep/n1/1/user/r/c/1")
		e2 := simExpectStream(w, "lockstep/n1/2/user/r/1")
		check := func(what string, got []uint64, want ...uint64) {
			if !slices.Equal(got, want) {
				tt.Errorf("%s drew %v, want %v", what, got, want)
			}
		}
		// r: two draws in inc 1, one in inc 2, then one in inc 1 of node incarnation 2.
		a, b := r1.next(), r1.next()
		check("r", values(draws, "/user/r"), a, b, r2.next(), e2.next())
		// c, a child of r, draws in each Started: the new cell that r's restart spawns at the same
		// path and in the same node incarnation continues the stream (TMR-140); the node restart
		// moves it to epoch 2.
		cs := simExpectStream(w, "lockstep/n1/2/user/r/c/1")
		check("c", values(draws, "/user/r/c"), c1.next(), c1.next(), cs.next())
	})
}

// drawModes are the four runs of AT-TMR-16.
type drawMode int

const (
	drawBase      drawMode = iota // the actor draws twice
	drawUnrelated                 // as drawBase, with an unrelated actor spawned before it
	drawRespawn                   // as drawBase, then the actor stops and is spawned again, and draws
	drawRestart                   // as drawBase, then the node restarts and spawns the actor again, and draws
)

// tmr16Seed is the seed of the four runs of AT-TMR-16.
const tmr16Seed = 0x5eed

// runDraws runs the scenario of mode with tmr16Seed and returns the actor's draws, in order, together
// with the values of the streams the draws are expected from: the stream of node epoch 1, which
// the first two draws and a respawn use, and the first draw of node epoch 2.
func runDraws(t *testing.T, mode drawMode) (got []uint64, epoch1 [3]uint64, epoch2 uint64) {
	t.Helper()
	sim.SimulateSeeds(t, simOpts(1), func(s *sim.Sim) {
		var draws []drawn
		s.Node("n1", func(sys *lockstep.System) {
			if mode == drawUnrelated {
				simSpawn(sys, "u", &simActor[draw]{})
			}
			simSpawn(sys, "a", drawer(&draws))
		})
		w := s.World()
		n1 := w.Sim.Lookup("n1")
		w.RunFor(10 * time.Millisecond)
		sendDraw(s, "/user/a", draw{})
		sendDraw(s, "/user/a", draw{})
		switch mode {
		case drawRespawn:
			if err := lockstep.Stop(s.System("n1"), lockstep.PIDAt[draw]("n1", "/user/a").Address()); err != nil {
				w.T().Fatal(err)
			}
			w.RunFor(10 * time.Millisecond)
			simSpawn(s.System("n1"), "a", drawer(&draws))
			w.RunFor(10 * time.Millisecond)
			sendDraw(s, "/user/a", draw{})
		case drawRestart:
			n1.Crash()
			w.RunFor(10 * time.Millisecond)
			n1.Restart()
			w.RunFor(10 * time.Millisecond)
			sendDraw(s, "/user/a", draw{})
		}
		got = values(draws, "/user/a")
		first := simExpectStream(w, "lockstep/n1/1/user/a/1")
		for i := range epoch1 {
			epoch1[i] = first.next()
		}
		epoch2 = simExpectStream(w, "lockstep/n1/2/user/a/1").next()
	}, tmr16Seed)
	return got, epoch1, epoch2
}

// AT-TMR-16, TMR-140: a stream depends on where the actor sits, not on uids: an unrelated actor
// spawned first changes nothing; an actor stopped and spawned again under the same name in one
// node incarnation continues the stream; after a node restart the stream is the one of epoch 2.
func TestATTMR16LabelStability(t *testing.T) {
	var base, unrelated, respawn, restart []uint64
	var epoch1 [3]uint64
	var epoch2 uint64
	t.Run("first run", func(t *testing.T) { base, epoch1, epoch2 = runDraws(t, drawBase) })
	t.Run("unrelated actor", func(t *testing.T) { unrelated, _, _ = runDraws(t, drawUnrelated) })
	t.Run("respawn", func(t *testing.T) { respawn, _, _ = runDraws(t, drawRespawn) })
	t.Run("node restart", func(t *testing.T) { restart, _, _ = runDraws(t, drawRestart) })
	if t.Failed() {
		return
	}

	if want := epoch1[:2]; !slices.Equal(base, want) {
		t.Errorf("first run drew %v, want the first two draws of lockstep/n1/1/user/a/1: %v", base, want)
	}
	if !slices.Equal(unrelated, base) {
		t.Errorf("with an unrelated actor spawned first, drew %v, want %v as without", unrelated, base)
	}
	if want := epoch1[:]; !slices.Equal(respawn, want) {
		t.Errorf("after a respawn drew %v, want the stream to continue: %v", respawn, want)
	}
	if len(restart) != 3 || restart[2] != epoch2 {
		t.Errorf("after a node restart drew %v, want the first draw of epoch 2: %d", restart, epoch2)
	}
	if len(restart) == 3 && len(respawn) == 3 && restart[2] == respawn[2] {
		t.Errorf("the draw after a node restart equals the one after a respawn (%d), want the streams to differ", restart[2])
	}
}

// who is the sender of a simultaneous timer's tick.
type who string

// AT-TMR-17, §6: timers due at the same instant fire in an order the seed picks. Two actors arm
// a 10ms timer in Started and send their tick to an observer when it fires; across 64 seeds both
// orders occur, and a rerun of a seed gives the same order and trace.
func TestATTMR17SimultaneousTimers(t *testing.T) {
	opts := simOpts(64)
	opts.CheckDeterminism = true
	orders := make(map[uint64]string) // seed -> the order the observer saw
	reruns := make(map[uint64]int)
	sim.Simulate(t, opts, func(s *sim.Sim) {
		reruns[s.World().Seed()]++
		var seen []who
		s.Node("n1", func(sys *lockstep.System) {
			o := simSpawn(sys, "o", &simActor[who]{receive: func(_ *lockstep.Context[who], m who) { seen = append(seen, m) }})
			for _, name := range []who{"a", "b"} {
				simSpawn(sys, string(name), &simActor[tick]{
					started: func(ctx *lockstep.Context[tick]) { ctx.After(10*time.Millisecond, tick{}) },
					receive: func(ctx *lockstep.Context[tick], _ tick) {
						if err := lockstep.Send(ctx, o, name); err != nil {
							panic(err)
						}
					},
				})
			}
		})
		s.World().RunFor(time.Second)

		tt := s.World().T()
		if len(seen) != 2 {
			tt.Fatalf("observer saw %v, want both ticks", seen)
		}
		order := string(seen[0]) + string(seen[1])
		if prev, ok := orders[s.World().Seed()]; ok && prev != order {
			tt.Errorf("seed %#x: a rerun saw %s, the first run saw %s", s.World().Seed(), order, prev)
		}
		orders[s.World().Seed()] = order
	})
	counts := map[string]int{}
	for _, o := range orders {
		counts[o]++
	}
	if len(orders) != 64 || counts["ab"] == 0 || counts["ba"] == 0 {
		t.Errorf("%d seeds, orders seen %v, want 64 seeds with both ab and ba", len(orders), counts)
	}
	for seed, n := range reruns {
		if n != 2 {
			t.Errorf("seed %#x ran %d times, want 2 (the run and its rerun)", seed, n)
		}
	}
}
