// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/persist"
	"github.com/hmdsefi/lockstep/persist/filestore"
	"github.com/hmdsefi/lockstep/sim"
)

// The counter fixture and the simulation setup of PER §10: an EventSourced counter whose
// commands reply with the sum once their events are committed, spawned by a parent that restarts
// it after every failure and keeps the errors.

// CounterCmd is the counter's command type. It is sealed: the types below implement it.
type CounterCmd interface{ counterCmd() }

// Add adds N and replies with the new sum.
type Add struct {
	N       int64
	ReplyTo lockstep.ReplyTo[int64]
}

// AddMany adds 1 K times, in one batch of K events, and replies with the new sum.
type AddMany struct {
	K       int
	ReplyTo lockstep.ReplyTo[int64]
}

// Get replies with the sum at once and persists nothing.
type Get struct{ ReplyTo lockstep.ReplyTo[int64] }

// SnapNow requests a snapshot.
type SnapNow struct{}

// Boom panics in Handle.
type Boom struct{}

// AddThenStop adds N and stops the counter.
type AddThenStop struct{ N int64 }

// FailWith adds N and fails the counter with Err.
type FailWith struct {
	N   int64
	Err error
}

func (Add) counterCmd()         {}
func (AddMany) counterCmd()     {}
func (Get) counterCmd()         {}
func (SnapNow) counterCmd()     {}
func (Boom) counterCmd()        {}
func (AddThenStop) counterCmd() {}
func (FailWith) counterCmd()    {}

// Added is the counter's event. One Added{1} encodes to the 7 bytes {"N":1}.
type Added struct{ N int64 }

// CounterState is the counter's state.
type CounterState struct{ Sum, Count int64 }

// counterCtx is the persist context of the counter.
type counterCtx = persist.Context[CounterCmd, Added, CounterState]

// counterRun runs the counter fixture on n1 for one seed and observes it across node boots and
// actor incarnations.
type counterRun struct {
	s      *sim.Sim
	config func(cfg *persist.Config[Added, CounterState]) // edits the default Config; nil: none
	pid    lockstep.PID[CounterCmd]                       // the counter of the latest boot

	boots     int                // runs of n1's setup
	factories int                // calls of the counter's factory
	errs      []error            // the errors the parent's decider saw, in order
	recs      []persist.Recovery // the Recovery of each Recovered call, in order
	log       []string           // the counter's handler calls, in order
	handleAt  []int              // len(Records()) at each Handle call
	thenSeqs  []uint64           // LastSeq() in each Then callback
}

// newCounterRun returns a run of the counter fixture in s.
func newCounterRun(s *sim.Sim) *counterRun { return &counterRun{s: s} }

// node adds n1. Its setup builds a filestore.Store on n1's Volume and spawns /user/parent, whose
// OneForOne decider appends each error to r.errs and restarts the child. The parent spawns the
// counter as /user/parent/counter with persistence ID c-1, and r.pid follows the latest boot.
func (r *counterRun) node() *kernel.Node {
	return r.s.Node("n1", func(sys *lockstep.System) {
		r.boots++
		store := filestore.New(r.s.Volume("n1"))
		cfg := persist.Config[Added, CounterState]{
			Journal:   store,
			Events:    persist.JSON[Added](),
			Snapshots: store,
			State:     persist.JSON[CounterState](),
		}
		if r.config != nil {
			r.config(&cfg)
		}
		decider := func(err error) lockstep.Directive {
			r.errs = append(r.errs, err)
			return lockstep.DirectiveRestart
		}
		spawn(sys, "parent", &actor[PMsg]{started: func(ctx *lockstep.Context[PMsg]) {
			pid, err := persist.Spawn(ctx, "counter", "c-1", r.newCounter, cfg)
			if err != nil {
				panic(err)
			}
			r.pid = pid
		}}, lockstep.WithStrategy(lockstep.OneForOne(decider)))
	})
}

// newCounter is the counter's factory.
func (r *counterRun) newCounter() persist.EventSourced[CounterCmd, Added, CounterState] {
	r.factories++
	return &pcounter{r: r}
}

// ask asks the counter with build from n1's System, fails the test on an error, runs 1 ms of
// virtual time after the reply, and returns the reply.
func (r *counterRun) ask(tt testing.TB, build func(lockstep.ReplyTo[int64]) CounterCmd) int64 {
	tt.Helper()
	v, err := sim.Ask(r.s, "n1", r.pid, build)
	if err != nil {
		tt.Fatalf("Ask: %v", err)
	}
	r.s.World().RunFor(time.Millisecond)
	return v
}

// addCmd, addManyCmd and getCmd build the counter's asks.
func addCmd(n int64) func(lockstep.ReplyTo[int64]) CounterCmd {
	return func(rt lockstep.ReplyTo[int64]) CounterCmd { return Add{N: n, ReplyTo: rt} }
}

func addManyCmd(k int) func(lockstep.ReplyTo[int64]) CounterCmd {
	return func(rt lockstep.ReplyTo[int64]) CounterCmd { return AddMany{K: k, ReplyTo: rt} }
}

func getCmd(rt lockstep.ReplyTo[int64]) CounterCmd { return Get{ReplyTo: rt} }

// pcounter is the counter of PER §10: an EventSourced[CounterCmd, Added, CounterState] that also
// implements Recoverer and SignalReceiver. It logs its handler calls to r.
type pcounter struct{ r *counterRun }

func (c *pcounter) Initial() CounterState { return CounterState{} }

func (c *pcounter) Handle(ctx *counterCtx, state CounterState, cmd CounterCmd) []Added {
	r := c.r
	r.handleAt = append(r.handleAt, len(r.s.World().Sim.Records()))
	r.log = append(r.log, fmt.Sprintf("handle %T", cmd))
	reply := func(rt lockstep.ReplyTo[int64]) func(*counterCtx, CounterState) {
		return func(ctx *counterCtx, state CounterState) {
			r.thenSeqs = append(r.thenSeqs, ctx.LastSeq())
			r.log = append(r.log, fmt.Sprintf("reply %d", state.Sum))
			rt.Send(ctx, state.Sum)
		}
	}
	switch m := cmd.(type) {
	case Add:
		ctx.Then(reply(m.ReplyTo))
		return []Added{{N: m.N}}
	case AddMany:
		ctx.Then(reply(m.ReplyTo))
		events := make([]Added, m.K)
		for i := range events {
			events[i] = Added{N: 1}
		}
		return events
	case Get:
		r.log = append(r.log, fmt.Sprintf("get %d", state.Sum))
		m.ReplyTo.Send(ctx, state.Sum)
	case SnapNow:
		ctx.Snapshot()
	case Boom:
		panic("boom")
	case AddThenStop:
		ctx.Stop()
		return []Added{{N: m.N}}
	case FailWith:
		ctx.Fail(m.Err)
		return []Added{{N: m.N}}
	}
	return nil
}

func (c *pcounter) Apply(state CounterState, event Added) CounterState {
	state.Sum += event.N
	state.Count++
	return state
}

func (c *pcounter) Recovered(_ *counterCtx, state CounterState, rec persist.Recovery) {
	c.r.recs = append(c.r.recs, rec)
	c.r.log = append(c.r.log, fmt.Sprintf("recovered %d", state.Sum))
}

func (c *pcounter) Signal(_ *counterCtx, state CounterState, _ lockstep.Signal) {
	c.r.log = append(c.r.log, fmt.Sprintf("signal %d", state.Sum))
}

// effectStarts returns the lockstep.effect.start records of effects named name, in order.
func effectStarts(recs []kernel.Record, name string) []kernel.Record {
	var out []kernel.Record
	for _, r := range ofKind(recs, "lockstep.effect.start") {
		if attr(r, "name") == name {
			out = append(out, r)
		}
	}
	return out
}

// isKind reports whether r has kind.
func isKind(kind string) func(r kernel.Record) bool {
	return func(r kernel.Record) bool { return r.Kind == kind }
}
