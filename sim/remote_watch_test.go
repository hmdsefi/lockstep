// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// ACT-602, ACT-605, SIM-160: a cross-node watch in simulation crosses as WATCH and TERMINATED
// frames between the nodes' remoting cores. The watcher on n1 gets Terminated with the stop reason
// when the actor on n2 stops, and at once with NoActor for an address on n2 that is not alive.
func TestRemoteWatchAcrossSimNodes(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var target lockstep.PID[int]
		s.Node("n2", func(sys *lockstep.System) { target = spawn(sys, "b", &actor[int]{}) })
		w.RunFor(time.Millisecond)

		byPath := lockstep.PIDAt[int]("n2", "/user/b").Address()
		none := lockstep.PIDAt[int]("n2", "/user/none").Address()
		var live, dead []lockstep.Terminated
		s.Node("n1", func(sys *lockstep.System) {
			for _, wt := range []struct {
				name string
				w    *watcher
			}{
				{"live", &watcher{target: byPath, got: &live}},
				{"dead", &watcher{target: none, got: &dead}},
			} {
				if _, err := lockstep.Spawn(sys, wt.name, func() lockstep.Actor[Tick] { return wt.w }); err != nil {
					panic(err)
				}
			}
		})
		w.RunFor(time.Second)
		if want := []lockstep.Terminated{{Addr: none, Reason: lockstep.NoActor}}; !slices.Equal(dead, want) {
			tt.Errorf("the watcher of %v got %v, want %v", none, dead, want)
		}
		if len(live) != 0 {
			tt.Errorf("the watcher of %v got %v before the stop, want nothing", byPath, live)
		}

		if err := lockstep.Stop(s.System("n2"), target.Address()); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)
		if want := []lockstep.Terminated{{Addr: byPath, Reason: lockstep.Normal}}; !slices.Equal(live, want) {
			tt.Errorf("the watcher of %v got %v, want %v", byPath, live, want)
		}
	})
}

// WatchCmd tells a remoteWatcher to Watch each of Watch, then Unwatch each of Unwatch, in one
// turn; then, when Halt is set, to send Seq{} to Halt's path, and with Stop to stop itself. It
// never leaves its node.
type WatchCmd struct {
	Watch, Unwatch []lockstep.Address
	Halt           lockstep.Address
	Stop           bool
}

// remoteWatcher carries out WatchCmds and keeps the Terminated signals it gets in *got.
type remoteWatcher struct{ got *[]lockstep.Terminated }

func (w remoteWatcher) Receive(ctx *lockstep.Context[WatchCmd], m WatchCmd) {
	for _, a := range m.Watch {
		if err := lockstep.Watch(ctx, a); err != nil {
			panic(err)
		}
	}
	for _, a := range m.Unwatch {
		lockstep.Unwatch(ctx, a)
	}
	if !m.Halt.IsZero() {
		if err := lockstep.Send(ctx, lockstep.PIDAt[Seq](m.Halt.Node(), m.Halt.Path()), Seq{}); err != nil {
			panic(err)
		}
	}
	if m.Stop {
		ctx.Stop()
	}
}

func (w remoteWatcher) Signal(_ *lockstep.Context[WatchCmd], sig lockstep.Signal) {
	if t, ok := sig.(lockstep.Terminated); ok {
		*w.got = append(*w.got, t)
	}
}

// spawnWatcher spawns a remoteWatcher at /user/w on sys and returns its PID.
func spawnWatcher(sys *lockstep.System, got *[]lockstep.Terminated) lockstep.PID[WatchCmd] {
	pid, err := lockstep.Spawn(sys, "w", func() lockstep.Actor[WatchCmd] { return remoteWatcher{got: got} })
	if err != nil {
		panic(err)
	}
	return pid
}

// halter returns an actor that stops at its first Seq.
func halter() *actor[Seq] {
	return &actor[Seq]{receive: func(ctx *lockstep.Context[Seq], _ Seq) { ctx.Stop() }}
}

// command sends m to the watcher at pid on node a and runs the world for a second.
func command(s *sim.Sim, pid lockstep.PID[WatchCmd], m WatchCmd) {
	if err := sim.Send(s, "a", pid, m); err != nil {
		s.World().T().Fatalf("Send %+v: %v", m, err)
	}
	s.World().RunFor(time.Second)
}

// sends returns, in order, the remote send records of kind on node, each as "<from> <to>".
func sends(s *sim.Sim, recs []kernel.Record, node, kind string) []string {
	var out []string
	for _, r := range ofKind(recs, "lockstep.remote.send") {
		if nodeName(s, r.Node) == node && attr(r, "kind") == kind {
			out = append(out, attr(r, "from")+" "+attr(r, "to"))
		}
	}
	return out
}

// AT-REM-12 (defined here: the spec gives it one line), REM-280, REM-282: remote death watch. A
// watcher on a gets Terminated with the reason of each actor it watches on b: Normal for a stop,
// and Failed with a *remote.Error from b of code other carrying the failure's text for an actor
// that its supervisor stopped after a failure. After Unwatch, a stop of the watched actor gives
// nothing, and b sends no TERMINATED for it.
func TestATREM12RemoteWatch(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []lockstep.Terminated
		var wpid lockstep.PID[WatchCmd]
		s.Node("a", func(sys *lockstep.System) { wpid = spawnWatcher(sys, &got) })
		var bnk, other lockstep.PID[BankMsg]
		var acct lockstep.PID[Seq]
		s.Node("b", func(sys *lockstep.System) {
			bnk = spawn(sys, "bank", bank())
			other = spawn(sys, "other", bank())
			stop := lockstep.OneForOne(func(error) lockstep.Directive { return lockstep.DirectiveStop })
			spawn(sys, "sup", &actor[Seq]{started: func(ctx *lockstep.Context[Seq]) {
				acct = spawn(ctx, "acct", &actor[Seq]{})
			}}, lockstep.WithStrategy(stop))
		})
		w.RunFor(time.Millisecond)

		command(s, wpid, WatchCmd{Watch: []lockstep.Address{bnk.Address(), acct.Address(), other.Address()}})
		command(s, wpid, WatchCmd{Unwatch: []lockstep.Address{other.Address()}})
		if len(got) != 0 {
			tt.Fatalf("Terminated before any stop: %v", got)
		}
		for _, a := range []lockstep.Address{bnk.Address(), other.Address()} {
			if err := lockstep.Stop(s.System("b"), a); err != nil {
				tt.Fatal(err)
			}
		}
		if err := s.InjectFail(acct.Address(), errBoom); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)

		if len(got) != 2 {
			tt.Fatalf("Terminated %v, want two: the bank's and the account's", got)
		}
		slices.SortFunc(got, func(x, y lockstep.Terminated) int { return strings.Compare(x.Addr.String(), y.Addr.String()) })
		if got[0] != (lockstep.Terminated{Addr: bnk.Address(), Reason: lockstep.Normal}) {
			tt.Errorf("the bank's Terminated %+v, want normal", got[0])
		}
		if got[1].Addr != acct.Address() || got[1].Reason.Kind != lockstep.StopFailed {
			tt.Errorf("the account's Terminated %+v, want failed", got[1])
		}
		remoteErr(tt, "the account's failure", got[1].Reason.Err, "b", remote.CodeOther, "boom", nil)

		recs := w.Sim.Records()
		wa := wpid.Address().String()
		if got, want := sends(s, recs, "a", "UNWATCH"), []string{wa + " " + other.String()}; !slices.Equal(got, want) {
			tt.Errorf("a's UNWATCH frames %q, want %q", got, want)
		}
		terms := sends(s, recs, "b", "TERMINATED")
		slices.Sort(terms)
		if want := []string{bnk.String() + " " + wa, acct.String() + " " + wa}; !slices.Equal(terms, want) {
			tt.Errorf("b's TERMINATED frames %q, want %q: none for the unwatched actor", terms, want)
		}
	})
}

// AT-REM-14 (simulation half; defined here), REM-280, ACT-602: a watch of an actor that is not
// alive on b, by a path nobody holds or by the uid of a stopped actor, gives Terminated with
// reason no-actor; a watch followed, in the same turn, by a message that makes the watched actor
// stop gives Terminated normal, since WATCH and the message cross on one session in order.
func TestATREM14WatchNotAlive(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []lockstep.Terminated
		var wpid lockstep.PID[WatchCmd]
		s.Node("a", func(sys *lockstep.System) { wpid = spawnWatcher(sys, &got) })
		var gone, h lockstep.PID[Seq]
		s.Node("b", func(sys *lockstep.System) {
			gone = spawn(sys, "gone", halter())
			h = spawn(sys, "h", halter())
		})
		w.RunFor(time.Millisecond)
		if err := lockstep.Stop(s.System("b"), gone.Address()); err != nil {
			tt.Fatal(err)
		}
		none := lockstep.PIDAt[Seq]("b", "/user/none").Address()

		command(s, wpid, WatchCmd{Watch: []lockstep.Address{none, gone.Address()}})
		want := []lockstep.Terminated{{Addr: none, Reason: lockstep.NoActor}, {Addr: gone.Address(), Reason: lockstep.NoActor}}
		if !slices.Equal(got, want) {
			tt.Errorf("watches of actors that are not alive: %v, want %v", got, want)
		}
		got = nil
		command(s, wpid, WatchCmd{Watch: []lockstep.Address{h.Address()}, Halt: h.Address()})
		if want := []lockstep.Terminated{{Addr: h.Address(), Reason: lockstep.Normal}}; !slices.Equal(got, want) {
			tt.Errorf("watch across a stop: %v, want %v", got, want)
		}
	})
}

// AT-ACT-36 (defined here: actors.md gives it no text; AT-ACT-37 is its one-node half), ACT-602,
// ACT-630, ACT-640, ACT-660, REM-282: remote death watch end to end between a and b. A second
// Watch of the same address does nothing: one lockstep.watch record with alive=remote and one
// WATCH frame; the watched actor's stop gives one Terminated. Unwatch records lockstep.unwatch and
// sends UNWATCH; a Watch of the same address after its Unwatch sends a new WATCH, and the actor's
// stop then gives one Terminated. The end of the watcher's incarnation, by a restart after a
// failure or by a stop, sends UNWATCH for each remote address it still watches, in insertion
// order, and records no lockstep.unwatch; b then sends no TERMINATED when those actors stop.
func TestATACT36RemoteWatchEndToEnd(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []lockstep.Terminated
		var wpid lockstep.PID[WatchCmd]
		s.Node("a", func(sys *lockstep.System) { wpid = spawnWatcher(sys, &got) })
		var p, q, r, u lockstep.PID[Seq]
		s.Node("b", func(sys *lockstep.System) {
			p, q, r, u = spawn(sys, "p", halter()), spawn(sys, "q", halter()), spawn(sys, "r", halter()), spawn(sys, "u", halter())
		})
		w.RunFor(time.Millisecond)
		wa := wpid.Address().String()

		command(s, wpid, WatchCmd{Watch: []lockstep.Address{p.Address(), p.Address()}})
		command(s, wpid, WatchCmd{Halt: p.Address()})
		if want := []lockstep.Terminated{{Addr: p.Address(), Reason: lockstep.Normal}}; !slices.Equal(got, want) {
			tt.Errorf("after p stopped: %v, want %v", got, want)
		}

		got = nil
		command(s, wpid, WatchCmd{Watch: []lockstep.Address{q.Address()}})
		command(s, wpid, WatchCmd{Unwatch: []lockstep.Address{q.Address()}})
		command(s, wpid, WatchCmd{Watch: []lockstep.Address{u.Address()}})
		command(s, wpid, WatchCmd{Unwatch: []lockstep.Address{u.Address()}})
		command(s, wpid, WatchCmd{Watch: []lockstep.Address{u.Address()}})
		command(s, wpid, WatchCmd{Halt: u.Address()})
		if want := []lockstep.Terminated{{Addr: u.Address(), Reason: lockstep.Normal}}; !slices.Equal(got, want) {
			tt.Errorf("after u stopped: %v, want %v", got, want)
		}

		got = nil
		command(s, wpid, WatchCmd{Watch: []lockstep.Address{r.Address()}})
		if err := s.InjectFail(wpid.Address(), errBoom); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)
		command(s, wpid, WatchCmd{Watch: []lockstep.Address{q.Address()}, Stop: true})
		for _, x := range []lockstep.PID[Seq]{q, r} {
			if err := lockstep.Stop(s.System("b"), x.Address()); err != nil {
				tt.Fatal(err)
			}
		}
		w.RunFor(time.Second)
		if len(got) != 0 {
			tt.Errorf("Terminated after the watcher restarted and stopped: %v", got)
		}

		recs := w.Sim.Records()
		var watchRecs, unwatchRecs []string
		for _, rec := range recs {
			if (rec.Kind != "lockstep.watch" && rec.Kind != "lockstep.unwatch") || nodeName(s, rec.Node) != "a" {
				continue
			}
			switch rec.Kind {
			case "lockstep.watch":
				watchRecs = append(watchRecs, attr(rec, "addr")+" "+attr(rec, "alive"))
			case "lockstep.unwatch":
				unwatchRecs = append(unwatchRecs, attr(rec, "addr"))
			}
		}
		remote := func(pids ...lockstep.PID[Seq]) []string {
			var out []string
			for _, x := range pids {
				out = append(out, x.String()+" remote")
			}
			return out
		}
		if want := remote(p, q, u, u, r, q); !slices.Equal(watchRecs, want) {
			tt.Errorf("a's lockstep.watch records %q, want %q", watchRecs, want)
		}
		if want := []string{q.String(), u.String()}; !slices.Equal(unwatchRecs, want) {
			tt.Errorf("a's lockstep.unwatch records %q, want %q: none at the incarnation's end", unwatchRecs, want)
		}
		from := func(pids ...lockstep.PID[Seq]) []string {
			var out []string
			for _, x := range pids {
				out = append(out, wa+" "+x.String())
			}
			return out
		}
		if got, want := sends(s, recs, "a", "WATCH"), from(p, q, u, u, r, q); !slices.Equal(got, want) {
			tt.Errorf("a's WATCH frames %q, want %q", got, want)
		}
		if got, want := sends(s, recs, "a", "UNWATCH"), from(q, u, r, q); !slices.Equal(got, want) {
			tt.Errorf("a's UNWATCH frames %q, want %q", got, want)
		}
		if got, want := sends(s, recs, "b", "TERMINATED"), []string{p.String() + " " + wa, u.String() + " " + wa}; !slices.Equal(got, want) {
			tt.Errorf("b's TERMINATED frames %q, want %q", got, want)
		}
	})
}
