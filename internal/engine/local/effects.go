// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package local

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// effect is one Effects.Start call.
type effect struct {
	state  atomic.Uint32 // pending, started (done's task started) or cancelled
	req    engine.EffectRequest
	done   func(v any, err error)
	ctx    context.Context    // set under Engine.mu when Run starts; nil while waiting
	cancel context.CancelFunc // cancels ctx; nil while waiting
}

// pool is the effect pool: at most EffectWorkers effects run, at most EffectQueue wait. Its
// fields are guarded by Engine.mu.
type pool struct {
	running map[*effect]struct{} // effects whose Run is in progress
	waiting []*effect            // FIFO
	stopped bool
}

// stop drops the waiting effects and returns them and the running ones; the caller cancels them
// outside the lock.
func (p *pool) stop() (running, waiting []*effect) {
	p.stopped = true
	for f := range p.running { // order does not matter: each context is cancelled once
		running = append(running, f)
	}
	waiting = p.waiting
	p.running, p.waiting = nil, nil
	return running, waiting
}

// effects is the bundle's Effects (ENG-430).
type effects struct{ e *Engine }

// Start runs req on a new goroutine, queues it, or refuses it with ErrOverloaded.
func (x effects) Start(req engine.EffectRequest, done func(v any, err error)) engine.Cancel {
	e := x.e
	f := &effect{req: req, done: done}
	e.mu.Lock()
	p := &e.fx
	switch {
	case e.stopped:
		e.mu.Unlock()
		return cancelNothing
	case len(p.running) < EffectWorkers:
		e.begin(f)
		go e.work(f)
	case len(p.waiting) < EffectQueue:
		p.waiting = append(p.waiting, f)
	default:
		e.mu.Unlock()
		e.post(task{fn: func() { e.complete(f, nil, engine.ErrOverloaded) }})
		return e.cancelEffect(f)
	}
	e.mu.Unlock()
	return e.cancelEffect(f)
}

// begin marks f running and gives it its context. The caller holds e.mu.
func (e *Engine) begin(f *effect) {
	if e.fx.running == nil {
		e.fx.running = make(map[*effect]struct{})
	}
	e.fx.running[f] = struct{}{}
	f.ctx, f.cancel = context.WithCancel(context.Background())
}

// cancelEffect returns f's Cancel: it wins only while done's task has not started, and then
// cancels Run's context or removes f from the wait queue.
func (e *Engine) cancelEffect(f *effect) engine.Cancel {
	return func() bool {
		if !f.state.CompareAndSwap(pending, cancelled) {
			return false
		}
		e.mu.Lock()
		cancel := f.cancel
		if cancel == nil {
			if i := slices.Index(e.fx.waiting, f); i >= 0 {
				e.fx.waiting = slices.Delete(e.fx.waiting, i, i+1)
			}
		}
		e.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return true
	}
}

// work runs f on its own goroutine and queues the task that calls its done, unless f was
// cancelled. It then starts the oldest waiting effect on a new goroutine (ENG-430 step 2), so no
// goroutine state (LockOSThread, pprof labels) carries over from one Run to the next.
func (e *Engine) work(f *effect) {
	v, err := call(f)
	f.cancel()
	if f.state.Load() == pending {
		e.post(task{fn: func() { e.complete(f, v, err) }})
	}
	if g := e.next(f); g != nil {
		go e.work(g)
	}
}

// next retires prev and returns the oldest waiting effect that is not cancelled, now marked
// running, or nil when none waits or the engine stopped.
func (e *Engine) next(prev *effect) *effect {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := &e.fx
	delete(p.running, prev)
	for !p.stopped && len(p.waiting) > 0 {
		f := p.waiting[0]
		p.waiting[0] = nil
		p.waiting = p.waiting[1:]
		if f.state.Load() == pending {
			e.begin(f)
			return f
		}
	}
	return nil
}

// call runs f's Run with its context and turns a panic into an error (ENG-430).
func call(f *effect) (v any, err error) {
	defer func() {
		if p := recover(); p != nil {
			v, err = nil, fmt.Errorf("lockstep: effect %q panicked: %v", f.req.Name, p)
		}
	}()
	return f.req.Run(f.ctx)
}

// complete runs on the loop: it calls done unless Cancel won first.
func (e *Engine) complete(f *effect, v any, err error) {
	if f.state.CompareAndSwap(pending, started) {
		f.done(v, err)
	}
}
