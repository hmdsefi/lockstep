// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// EffectPanicError is passed to done when an effect's Run panics.
type EffectPanicError struct {
	Name  string // EffectRequest.Name
	Value any    // value passed to panic
	Stack []byte // runtime/debug.Stack() of the effect goroutine at recovery
}

// Error returns fmt.Sprintf("lockstep: effect %q panicked: %v", Name, Value), the text of ENG-430.
func (e *EffectPanicError) Error() string {
	return fmt.Sprintf("lockstep: effect %q panicked: %v", e.Name, e.Value)
}

// States of a job. Exactly one CompareAndSwap away from jobPending wins, so Cancel returns true
// at most once and only while done has not started (SCH-180, SCH §11).
const (
	jobPending   uint32 = iota // started, queued or refused; the done callback has not started
	jobStarted                 // the done callback started
	jobCancelled               // Cancel won; done never runs
)

// job is one Effects.Start call.
type job struct {
	state  atomic.Uint32
	req    engine.EffectRequest
	done   func(v any, err error)
	cancel context.CancelFunc // cancels the context of Run; set under pool.mu when Run is about to start
}

// pool is the effect pool (SCH §5.4). One mutex guards the queue, the running count and the
// closing flag, so SCH-150's decision is atomic.
type pool struct {
	mu      sync.Mutex
	running int    // live effect goroutines; at most Config.EffectWorkers
	queue   []*job // FIFO; at most Config.EffectQueue
	closing bool
}

// stop marks the pool closing and drops the queued jobs: their Run and done never run. Running
// jobs are not touched here; Stop cancels engineCtx, which ends their contexts (SCH-200 step 2).
func (p *pool) stop() {
	p.mu.Lock()
	p.closing = true
	clear(p.queue)
	p.queue = nil
	p.mu.Unlock()
}

// effects is the bundle's Effects (SCH-150 to SCH-190, ENG-430).
type effects struct{ e *Engine }

var _ engine.Effects = effects{}

// effects returns the engine's Effects.
func (e *Engine) effects() engine.Effects { return effects{e} }

// Start runs req on an effect goroutine, queues it, or refuses it with engine.ErrOverloaded
// (SCH-150).
func (x effects) Start(req engine.EffectRequest, done func(v any, err error)) engine.Cancel {
	e := x.e
	p := &e.fx
	j := &job{req: req, done: done}
	p.mu.Lock()
	switch {
	case p.closing:
		p.mu.Unlock()
		return cancelNothing
	case p.running < e.cfg.EffectWorkers:
		p.running++
		ctx := e.begin(j)
		p.mu.Unlock()
		go e.effectLoop(j, ctx)
	case len(p.queue) < e.cfg.EffectQueue:
		p.queue = append(p.queue, j)
		p.mu.Unlock()
	default:
		p.mu.Unlock()
		e.s.pushCallback(func() { e.runDone(j, nil, engine.ErrOverloaded) })
	}
	return e.cancelJob(j)
}

// begin gives j the context of its Run (SCH-170). The caller holds e.fx.mu.
func (e *Engine) begin(j *job) context.Context {
	ctx, cancel := context.WithCancel(e.engineCtx)
	j.cancel = cancel
	return ctx
}

// cancelJob returns j's Cancel (SCH-180): it wins only while done has not started, and then
// cancels the context of Run or removes j from the queue.
func (e *Engine) cancelJob(j *job) engine.Cancel {
	return func() bool {
		if !j.state.CompareAndSwap(jobPending, jobCancelled) {
			return false
		}
		p := &e.fx
		p.mu.Lock()
		cancel := j.cancel
		if cancel == nil {
			if i := slices.Index(p.queue, j); i >= 0 {
				p.queue = slices.Delete(p.queue, i, i+1)
			}
		}
		p.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return true
	}
}

// effectLoop is the effect goroutine (SCH-160): it runs j, then the head of the queue, and exits
// when the queue is empty or the engine is closing. After each Run it pushes the callback that
// calls done, unless Cancel won (SCH-170).
func (e *Engine) effectLoop(j *job, ctx context.Context) {
	for j != nil {
		v, err := callRun(j, ctx)
		j.cancel()
		e.finish(j, v, err)
		j, ctx = e.nextJob()
	}
}

// finish pushes the callback that calls j's done with Run's result, unless Cancel won. It is a
// function of its own so that the callback captures this j, not the loop's variable.
func (e *Engine) finish(j *job, v any, err error) {
	if j.state.Load() == jobPending {
		e.s.pushCallback(func() { e.runDone(j, v, err) })
	}
}

// nextJob takes the oldest queued job that Cancel has not won and gives it its context. When
// none is left, or the pool is closing, it retires the calling effect goroutine and returns nil.
func (e *Engine) nextJob() (*job, context.Context) {
	p := &e.fx
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.closing && len(p.queue) > 0 {
		j := p.queue[0]
		p.queue[0] = nil
		p.queue = p.queue[1:]
		if j.state.Load() == jobPending {
			return j, e.begin(j)
		}
	}
	p.running--
	return nil, nil
}

// callRun calls j's Run and turns a panic into an *EffectPanicError (SCH-170).
func callRun(j *job, ctx context.Context) (v any, err error) {
	defer func() {
		if r := recover(); r != nil {
			v, err = nil, &EffectPanicError{Name: j.req.Name, Value: r, Stack: debug.Stack()}
		}
	}()
	return j.req.Run(ctx)
}

// runDone is the callback of j, run by a worker. It calls done unless Cancel won, or unless the
// engine is closing (SCH-190).
func (e *Engine) runDone(j *job, v any, err error) {
	if !j.state.CompareAndSwap(jobPending, jobStarted) {
		return
	}
	if e.s.closing.Load() {
		return
	}
	j.done(v, err)
}
