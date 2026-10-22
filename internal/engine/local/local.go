// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package local is the single-goroutine engine behind lockstep.New in Phase 1 (architecture
// §12.3): one loop goroutine runs every turn, timer function and effect completion.
package local

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
)

const (
	DefaultThroughput = 32   // turn budget when Config.Throughput is 0
	EffectWorkers     = 64   // effects running at once
	EffectQueue       = 1024 // effects waiting for a worker
)

// Config configures New.
type Config struct {
	Node       string        // node name; non-empty
	Throughput int           // turn budget; 0 means DefaultThroughput; negative is an error
	Tracer     engine.Tracer // nil: tracing disabled; must be safe for concurrent use
}

// task is one unit of work for the loop: a turn of actor, or else a call of fn.
type task struct {
	actor engine.ActorRef
	fn    func()
}

// Engine is a running local engine.
type Engine struct {
	budget int
	bundle engine.Engine

	mu      sync.Mutex
	tasks   []task              // FIFO queue of tasks; the loop takes the whole slice at once
	stopped bool                // Stop stopped accepting tasks
	timers  map[*timer]struct{} // runtime timers armed and not yet fired or cancelled
	fx      pool                // the effect pool (effects.go)

	halted  atomic.Bool   // mirrors stopped for the loop, which checks it between tasks
	wake    chan struct{} // 1-buffered: a token means the loop should look at tasks again
	exited  chan struct{} // closed when the loop goroutine returns
	loopID  atomic.Uint64 // goroutine ID of the loop
	running atomic.Bool   // the loop is running a task

	stopOnce sync.Once
}

// New validates cfg and starts the engine's loop goroutine.
func New(cfg Config) (*Engine, error) {
	if cfg.Node == "" {
		return nil, errors.New("lockstep: local engine: empty node name")
	}
	if cfg.Throughput < 0 {
		return nil, fmt.Errorf("lockstep: local engine: throughput %d < 0", cfg.Throughput)
	}
	e := &Engine{
		budget: cfg.Throughput,
		timers: make(map[*timer]struct{}),
		wake:   make(chan struct{}, 1),
		exited: make(chan struct{}),
	}
	if e.budget == 0 {
		e.budget = DefaultThroughput
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = noTracer{}
	}
	e.bundle = engine.Engine{
		Node:      cfg.Node,
		Epoch:     newEpoch(),
		Blocking:  true,
		Scheduler: scheduler{e},
		Clock:     clock{e},
		Effects:   effects{e},
		Entropy:   entropy{},
		Tracer:    tracer,
		Admission: admitAll{},
		Failer:    noFailer{},
		Waiter:    waiter{},
		Stop:      e.Stop,
	}
	ready := make(chan struct{})
	go e.loop(ready)
	<-ready // the loop's ID is known before any InTurn call
	return e, nil
}

// newEpoch returns a non-zero uint32 read with crypto/rand (ENG-450).
func newEpoch() uint32 {
	var b [4]byte
	for {
		_, _ = rand.Read(b[:]) // never fails (crypto/rand.Read, Go 1.24+)
		if v := binary.LittleEndian.Uint32(b[:]); v != 0 {
			return v
		}
	}
}

// Engine returns the bundle for one system. Its Stop field is e.Stop.
func (e *Engine) Engine() engine.Engine {
	return e.bundle
}

// Stop stops the engine (ENG-460). It panics when called from engine context.
func (e *Engine) Stop() {
	if e.inTurn() {
		panic("lockstep: local engine: Stop called from engine context")
	}
	e.stopOnce.Do(e.stop)
}

// stop is Stop's first call. Once it returns, the loop goroutine has exited and nothing calls back.
func (e *Engine) stop() {
	e.mu.Lock()
	e.stopped = true
	e.halted.Store(true)
	clear(e.tasks)
	e.tasks = nil
	timers := e.timers
	e.timers = nil
	running, waiting := e.fx.stop()
	e.mu.Unlock()

	for t := range timers { // order does not matter: each timer is stopped once
		if t.state.CompareAndSwap(pending, cancelled) {
			t.rt.Stop()
		}
	}
	for _, f := range waiting {
		f.state.CompareAndSwap(pending, cancelled)
	}
	for _, f := range running {
		f.state.CompareAndSwap(pending, cancelled)
		f.cancel()
	}
	e.notify()
	<-e.exited
}

// post appends t to the task queue and wakes the loop. After Stop it does nothing. It never
// blocks.
func (e *Engine) post(t task) {
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return
	}
	e.tasks = append(e.tasks, t)
	e.mu.Unlock()
	e.notify()
}

// notify leaves a token in wake unless one is there already.
func (e *Engine) notify() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// loop runs tasks one at a time in queue order until Stop.
func (e *Engine) loop(ready chan<- struct{}) {
	defer close(e.exited)
	e.loopID.Store(goroutineID())
	close(ready)
	var batch []task
	for {
		e.mu.Lock()
		batch, e.tasks = e.tasks, batch[:0]
		stopped := e.stopped
		e.mu.Unlock()
		if stopped {
			return
		}
		if len(batch) == 0 {
			<-e.wake
			continue
		}
		for i := range batch {
			if e.halted.Load() {
				break // Stop discards the rest
			}
			e.run(batch[i])
		}
		clear(batch)
	}
}

// run runs one task as engine context. A turn follows ENG-050 step 2; a re-queued turn goes
// behind the tasks already queued.
func (e *Engine) run(t task) {
	e.running.Store(true)
	if a := t.actor; a != nil {
		slot := a.Slot()
		slot.Begin()
		more := a.RunTurn(e.budget)
		if slot.End(more) {
			e.post(task{actor: a})
		}
	} else {
		t.fn()
	}
	e.running.Store(false)
}

// inTurn reports whether the caller is the loop goroutine running a task (ENG-055, ENG-410).
func (e *Engine) inTurn() bool {
	return e.running.Load() && goroutineID() == e.loopID.Load()
}

// goroutineID returns the calling goroutine's ID: the number after "goroutine " in the first line
// of its stack trace, or 0 if that cannot be parsed (no goroutine has ID 0).
func goroutineID() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b, ok := bytes.CutPrefix(b, []byte("goroutine "))
	if !ok {
		return 0
	}
	if i := bytes.IndexByte(b, ' '); i >= 0 {
		b = b[:i]
	}
	id, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// scheduler is the bundle's Scheduler (ENG-050, ENG-410). It is not a LocalScheduler.
type scheduler struct{ e *Engine }

// Ready queues one turn of a if its Slot was idle. After Stop it does nothing.
func (s scheduler) Ready(a engine.ActorRef) {
	if s.e.halted.Load() {
		return
	}
	if a.Slot().Ready() {
		s.e.post(task{actor: a})
	}
}

// InTurn reports whether the caller is the loop goroutine running a task.
func (s scheduler) InTurn() bool { return s.e.inTurn() }

// noTracer is the bundle's Tracer when Config.Tracer is nil (ENG-450).
type noTracer struct{}

func (noTracer) Enabled() bool                 { return false }
func (noTracer) Record(string, ...engine.Attr) {}
func (noTracer) Describe(v any) string         { return fmt.Sprintf("%T", v) }

// admitAll is the bundle's Admission: it admits everything and ignores Bind (ENG-125, ENG-450).
type admitAll struct{}

func (admitAll) AdmitSpawn(bool) error                                  { return nil }
func (admitAll) AdmitSend() error                                       { return nil }
func (admitAll) Bind(func(engine.AdmissionChange), func() engine.Usage) {}

// noFailer is the bundle's Failer: it ignores misuse (ENG-130).
type noFailer struct{}

func (noFailer) Fail(error) {}

// waiter is the bundle's Waiter (ENG-135).
type waiter struct{}

// Wait returns nil once done is closed, or ctx.Err() if ctx ends first.
func (waiter) Wait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
