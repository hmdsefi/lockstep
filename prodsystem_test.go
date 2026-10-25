// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/prodengine"
)

// prodShutdownBound bounds the Shutdown that ends a test on the production engine. It is the bound
// of shutdownAtEnd, so a stress test with many actors under -race on a busy machine still fits.
const prodShutdownBound = 30 * time.Second

// newProdSystem returns a system built through the ACT-015 constructor with opts on the bundle of
// a new production engine for cfg, and that engine. The system shuts down, and the engine stops,
// when the test ends.
func newProdSystem(t *testing.T, cfg prodengine.Config, opts ...Option) (*System, *prodengine.Engine) {
	t.Helper()
	return newProdSystemWith(t, cfg, nil, opts...)
}

// newProdSystemWith is newProdSystem with edit, when non-nil, applied to the bundle before the
// system is built on it, so a test can replace a field such as Tracer or Effects.
func newProdSystemWith(t *testing.T, cfg prodengine.Config, edit func(*engine.Engine), opts ...Option) (*System, *prodengine.Engine) {
	t.Helper()
	pe := prodengine.New(cfg)
	eng := pe.Engine()
	if edit != nil {
		edit(&eng)
	}
	sys, err := newSystem(eng, opts)
	if err != nil {
		pe.Stop()
		t.Fatalf("newSystem on a production engine: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), prodShutdownBound)
		defer cancel()
		if err := sys.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown at the end of the test: %v", err)
		}
		// Shutdown has stopped the engine unless it failed or edit replaced the bundle's Stop. Stop
		// waits for every worker, so a worker stuck in a turn would hang the test: bound the wait.
		stopped := make(chan struct{})
		go func() {
			pe.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(prodShutdownBound):
			t.Errorf("prodengine Stop: a worker did not exit within %v", prodShutdownBound)
		}
	})
	return sys, pe
}

// quietProbe is a probe that drops every signal, so the stops and failures of many children never
// fill a signal channel and block its turns. It serves as the parent of a stress test's actors.
type quietProbe struct{ probe[any] }

// Signal drops sig.
func (*quietProbe) Signal(*Context[any], Signal) {}

// spawnQuiet spawns a quietProbe named name at the top level.
func spawnQuiet(t *testing.T, sys *System, name string) PID[any] {
	t.Helper()
	pid, err := Spawn(sys, name, actorOf[any](&quietProbe{}))
	if err != nil {
		t.Fatalf("Spawn parent %q: %v", name, err)
	}
	return pid
}

// spawnUnder spawns an actor named name with factory under parent (a probe or quietProbe), during a
// turn of parent, and returns its PID. Unlike inProbe it does not fail the test, so any goroutine
// may call it.
func spawnUnder[M any](sys *System, parent PID[any], name string, factory func() Actor[M]) (PID[M], error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := Ask(sys, parent, func(rt ReplyTo[any]) any {
		return Query[any]{fn: func(ctx *Context[any]) any {
			pid, err := Spawn(ctx, name, factory)
			if err != nil {
				return err
			}
			return pid
		}, reply: rt}
	}, WithTimeout(10*time.Second))
	v, err := r.Await(ctx)
	if err != nil {
		return PID[M]{}, err
	}
	switch v := v.(type) {
	case PID[M]:
		return v, nil
	case error:
		return PID[M]{}, v
	default:
		return PID[M]{}, fmt.Errorf("spawnUnder: parent answered %T", v)
	}
}

// meetResult is what an actor of AT-SCH-04 records: its name and whether it saw the other actor's
// turn running at the same time.
type meetResult struct {
	name    string
	reached bool
}

// meetUp runs AT-SCH-04 on a production engine with workers workers and returns the two records
// in the order the actors recorded them.
func meetUp(t *testing.T, workers int) []meetResult {
	t.Helper()
	sys, _ := newProdSystem(t, prodengine.Config{Node: "local", Workers: workers})
	var met atomic.Int32
	results := make(chan meetResult, 2)
	meet := func(name string) func() Actor[int] {
		return func() Actor[int] {
			first := true // turn-owned
			return &fnActor[int]{receive: func(*Context[int], int) {
				if !first {
					return
				}
				first = false
				met.Add(1)
				//lockstep:allow LS100 test needs two concurrent turns
				for start := time.Now(); met.Load() < 2 && time.Since(start) < 5*time.Second; {
					runtime.Gosched()
				}
				results <- meetResult{name: name, reached: met.Load() == 2}
			}}
		}
	}
	a, err := Spawn(sys, "a", meet("a"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Spawn(sys, "b", meet("b"))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, a, 1)
	send(t, sys, b, 1)
	out := make([]meetResult, 0, 2)
	for range 2 {
		select {
		case r := <-results:
			out = append(out, r)
		case <-time.After(20 * time.Second):
			t.Fatalf("%d of 2 actors recorded within 20 s", len(out))
		}
	}
	return out
}

// AT-SCH-04: SCH-020, SCH-060, SCH-115, ENG-060. Turns of different actors run in parallel on
// different workers: with two workers both actors see the other's turn; with one worker the first
// actor to run spins alone until its 5 s bound.
func TestATSCH04ParallelTurns(t *testing.T) {
	t.Run("two workers", func(t *testing.T) {
		for _, r := range meetUp(t, 2) {
			if !r.reached {
				t.Errorf("actor %s recorded false, want true: two workers run both turns at once", r.name)
			}
		}
	})
	t.Run("one worker", func(t *testing.T) {
		got := meetUp(t, 1)
		if got[0].reached {
			t.Errorf("first actor to run (%s) recorded true, want false: one worker runs one turn at a time", got[0].name)
		}
		if !got[1].reached {
			t.Errorf("second actor to run (%s) recorded false, want true: the first had counted already", got[1].name)
		}
	})
}

// stressMsg is the message of AT-SCH-21's actors.
type stressMsg struct {
	kind stressKind
	hops int   // stressToken: the hops left, this one included
	err  error // stressEffectDone: the effect's error
}

// stressKind says what a stressMsg asks its actor to do.
type stressKind int

const (
	stressToken      stressKind = iota // count the message; pass it on to the next actor while hops > 1
	stressEffect                       // run an effect that returns at once
	stressEffectDone                   // the effect's result
	stressTimers                       // arm a short, a long and a periodic timer
	stressTick                         // a timer fired
)

// stressCounts are the counters AT-SCH-21's actors share.
type stressCounts struct {
	tokens  atomic.Int64 // stressToken messages received
	effects atomic.Int64 // effect results received
	ticks   atomic.Int64 // timer messages received
}

// stressActor returns the factory of AT-SCH-21's actor i of len(*peers).
func stressActor(t *testing.T, i int, peers *[]PID[stressMsg], counts *stressCounts) func() Actor[stressMsg] {
	return func() Actor[stressMsg] {
		return &fnActor[stressMsg]{receive: func(ctx *Context[stressMsg], m stressMsg) {
			switch m.kind {
			case stressToken:
				counts.tokens.Add(1)
				if m.hops > 1 {
					next := (*peers)[(i+1)%len(*peers)]
					if err := Send(ctx, next, stressMsg{kind: stressToken, hops: m.hops - 1}); err != nil {
						t.Errorf("actor %d: Send to %v: %v", i, next, err)
					}
				}
			case stressEffect:
				eff := funcEffect{name: "stress", run: func(context.Context) (int64, error) { return 0, nil }}
				Do(ctx, eff, func(_ int64, err error) stressMsg { return stressMsg{kind: stressEffectDone, err: err} })
			case stressEffectDone:
				if m.err != nil {
					t.Errorf("actor %d: effect failed: %v", i, m.err)
				}
				counts.effects.Add(1)
			case stressTimers:
				ctx.After(time.Millisecond, stressMsg{kind: stressTick})
				ctx.After(time.Hour, stressMsg{kind: stressTick}) // still pending at the shutdown
				ctx.Every(5*time.Millisecond, stressMsg{kind: stressTick})
			case stressTick:
				counts.ticks.Add(1)
			}
		}}
	}
}

// AT-SCH-21: SCH-210, SCH-220. On four workers, 100 actors under one parent exchange 10,000
// messages, run 50 effects and set timers; Shutdown within 5 s returns nil and no goroutine is
// left over.
func TestATSCH21ShutdownLeavesNothing(t *testing.T) {
	const actors, messages, effects = 100, 10_000, 50
	sys, _ := newProdSystem(t, prodengine.Config{Node: "local", Workers: 4})
	parent := spawnQuiet(t, sys, "parent")
	var counts stressCounts
	peers := make([]PID[stressMsg], actors)
	inProbe(t, sys, parent, func(ctx *Context[any]) any {
		for i := range peers {
			pid, err := Spawn(ctx, fmt.Sprintf("c%d", i), stressActor(t, i, &peers, &counts))
			if err != nil {
				t.Errorf("Spawn c%d: %v", i, err)
				continue
			}
			peers[i] = pid
		}
		return nil
	})
	if t.Failed() {
		t.FailNow()
	}
	for i, pid := range peers {
		send(t, sys, pid, stressMsg{kind: stressToken, hops: messages / actors})
		send(t, sys, pid, stressMsg{kind: stressTimers})
		if i < effects {
			send(t, sys, pid, stressMsg{kind: stressEffect})
		}
	}
	waitUntil(t, "every token, effect result and first tick", func() bool {
		return counts.tokens.Load() == messages && counts.effects.Load() == effects && counts.ticks.Load() >= actors
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	if n := counts.tokens.Load(); n != messages {
		t.Errorf("%d tokens received, want %d", n, messages)
	}
	if n := counts.effects.Load(); n != effects {
		t.Errorf("%d effect results received, want %d", n, effects)
	}
	goleak.VerifyNone(t)
}

// errSibling is the failure of TestParallelSpawnStopWatch's failing siblings.
var errSibling = errors.New("sibling fails")

// TestParallelSpawnStopWatch runs AT-ACT-27's watch race 1,000 times on four workers (ENG-060,
// ACT-600): four watchers each watch the target, and send it a message, while a test goroutine
// stops it; meanwhile two goroutines keep spawning siblings, one under the target's parent and one
// under a second parent, making every other one fail (its parent restarts it), and stopping them.
// The tracer records, so trace records and dead letters come from parallel turns too. Every watcher
// receives exactly one Terminated per target, with reason Normal or NoActor.
func TestParallelSpawnStopWatch(t *testing.T) {
	const iterations, watchers, churners = 1000, 4, 2
	sys, _ := newProdSystemWith(t, prodengine.Config{Node: "local", Workers: 4},
		func(e *engine.Engine) { e.Tracer = enginetest.NewRecorder() })
	parents := make([]PID[any], churners)
	for k := range parents {
		parents[k] = spawnQuiet(t, sys, fmt.Sprintf("parent%d", k))
	}
	ws := make([]PID[any], watchers)
	wps := make([]*probe[any], watchers)
	for k := range ws {
		ws[k], wps[k] = spawnProbe[any](t, sys, fmt.Sprintf("w%d", k), func(ctx *Context[any], m any) {
			if p, ok := m.(PID[int]); ok {
				if err := Watch(ctx, p.Address()); err != nil {
					t.Errorf("Watch(%v): %v", p, err)
				}
				if err := Send(ctx, p, 1); err != nil { // delivered, or a dead letter once p stopped
					t.Errorf("Send to %v: %v", p, err)
				}
			}
		})
	}
	failing := func() Actor[int] {
		return &fnActor[int]{receive: func(ctx *Context[int], _ int) { ctx.Fail(errSibling) }}
	}

	stop := make(chan struct{})
	var churn sync.WaitGroup
	var siblings atomic.Int64
	for k := range churners {
		churn.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := siblings.Add(1)
				name := fmt.Sprintf("s%d", n)
				factory := idle[int]()
				if n%2 == 0 {
					factory = failing
				}
				pid, err := spawnUnder(sys, parents[k], name, factory)
				if err != nil {
					t.Errorf("spawn sibling %s: %v", name, err)
					return
				}
				if err := Send(sys, pid, 1); err != nil {
					t.Errorf("Send to sibling %s: %v", name, err)
					return
				}
				if err := Stop(sys, pid.Address()); err != nil {
					t.Errorf("Stop sibling %s: %v", name, err)
					return
				}
			}
		})
	}
	defer func() {
		close(stop)
		churn.Wait()
	}()

	for i := range iterations {
		tp, err := spawnUnder(sys, parents[0], fmt.Sprintf("t%d", i), idle[int]())
		if err != nil {
			t.Fatalf("iteration %d: spawn target: %v", i, err)
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			if err := Stop(sys, tp.Address()); err != nil {
				t.Errorf("iteration %d: Stop: %v", i, err)
			}
		})
		for _, w := range ws {
			wg.Go(func() {
				if err := Send(sys, w, any(tp)); err != nil {
					t.Errorf("iteration %d: Send to %v: %v", i, w, err)
				}
			})
		}
		wg.Wait()
		for k, wp := range wps {
			term := awaitTerminated(t, wp, tp.Address())
			if term.Reason != Normal && term.Reason != NoActor {
				t.Fatalf("iteration %d, watcher %d: Terminated %v, want reason Normal or NoActor", i, k, term)
			}
		}
	}
	for _, wp := range wps {
		noSignal(t, wp, 100*time.Millisecond)
	}
	if siblings.Load() < churners {
		t.Errorf("%d siblings spawned during the watch races, want at least %d", siblings.Load(), churners)
	}
}

// askMsg is the message of TestParallelAsks's actors.
type askMsg struct {
	kind  askKind
	from  int          // askQuery: the asking actor
	seq   int          // askQuery, askAnswer: the ask's number at its asker
	reply ReplyTo[int] // askQuery
	value int          // askAnswer: the reply
	err   error        // askAnswer: the ask's error
}

// askKind says what an askMsg asks its actor to do.
type askKind int

const (
	askStart  askKind = iota // ask the peers
	askQuery                 // answer from*asksPerActor + seq
	askAnswer                // a piped reply
)

// asksPerActor is the number of asks each actor of TestParallelAsks makes.
const asksPerActor = 100

// TestParallelAsks: ENG-060, MSG-600, MSG-620. On four workers, 64 actors under one parent each
// make 100 asks at once, round-robin over the 63 other actors, and pipe the replies back to
// themselves; every reply arrives exactly once, with the right value.
func TestParallelAsks(t *testing.T) {
	const actors = 64
	sys, _ := newProdSystem(t, prodengine.Config{Node: "local", Workers: 4})
	parent := spawnQuiet(t, sys, "parent")
	var got [actors][asksPerActor]atomic.Int32
	var total atomic.Int64
	peers := make([]PID[askMsg], actors)
	factory := func(i int) func() Actor[askMsg] {
		return func() Actor[askMsg] {
			return &fnActor[askMsg]{receive: func(ctx *Context[askMsg], m askMsg) {
				switch m.kind {
				case askStart:
					for j := range asksPerActor {
						peer := peers[(i+1+j%(actors-1))%actors] // never i itself
						r := Ask(ctx, peer, func(rt ReplyTo[int]) askMsg {
							return askMsg{kind: askQuery, from: i, seq: j, reply: rt}
						}, WithTimeout(30*time.Second))
						Pipe(ctx, r, func(v int, err error) askMsg {
							return askMsg{kind: askAnswer, seq: j, value: v, err: err}
						})
					}
				case askQuery:
					m.reply.Send(ctx, m.from*asksPerActor+m.seq)
				case askAnswer:
					if m.err != nil {
						t.Errorf("actor %d, ask %d: %v", i, m.seq, m.err)
					} else if want := i*asksPerActor + m.seq; m.value != want {
						t.Errorf("actor %d, ask %d: reply %d, want %d", i, m.seq, m.value, want)
					}
					got[i][m.seq].Add(1)
					total.Add(1)
				}
			}}
		}
	}
	inProbe(t, sys, parent, func(ctx *Context[any]) any {
		for i := range peers {
			pid, err := Spawn(ctx, fmt.Sprintf("a%d", i), factory(i))
			if err != nil {
				t.Errorf("Spawn a%d: %v", i, err)
				continue
			}
			peers[i] = pid
		}
		return nil
	})
	if t.Failed() {
		t.FailNow()
	}
	for _, pid := range peers {
		send(t, sys, pid, askMsg{kind: askStart})
	}
	waitUntil(t, "every piped reply", func() bool { return total.Load() >= actors*asksPerActor })
	time.Sleep(50 * time.Millisecond) // a duplicate reply would arrive within this
	if n := total.Load(); n != actors*asksPerActor {
		t.Errorf("%d replies arrived, want %d", n, actors*asksPerActor)
	}
	for i := range got {
		for j := range got[i] {
			if n := got[i][j].Load(); n != 1 {
				t.Errorf("actor %d, ask %d: reply arrived %d times, want once", i, j, n)
			}
		}
	}
}
