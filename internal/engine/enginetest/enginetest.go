// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package enginetest is a deterministic manual engine and a recording tracer for unit tests of the
// runtime. Nothing runs until the test calls Step, RunUntilIdle, Advance, CompleteEffect, Inbound
// or the Waiter, and everything runs on the calling goroutine. Import it only from _test.go files.
package enginetest

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Epoch is the manual clock's start: time.Unix(0, 0).UTC().
var Epoch = time.Unix(0, 0).UTC()

// MaxTurns bounds RunUntilIdle.
const MaxTurns = 1_000_000

// ErrWouldBlock is returned by the manual Waiter when done is still open after RunUntilIdle.
var ErrWouldBlock = errors.New("enginetest: wait would block; advance the clock or complete an effect")

// timer is one pending AfterFunc call.
type timer struct {
	due time.Time
	seq uint64
	fn  func()
}

// pendingEffect is one effect started and neither completed nor cancelled.
type pendingEffect struct {
	req  engine.EffectRequest
	done func(v any, err error)
}

// Engine is a manual engine. It is not safe for concurrent use.
type Engine struct {
	node   string
	seed   uint64
	budget int
	host   engine.Host

	runq []engine.ActorRef // FIFO of queued turns
	busy int               // depth of engine-context callbacks the engine is running (InTurn)

	now      time.Time
	timers   []*timer // pending timers, in creation order
	timerSeq uint64
	labels   []string

	pending []*pendingEffect // in start order

	rands map[string]*rand.Rand

	rec *Recorder

	failures   []error
	delivered  []engine.Envelope
	deliverErr error

	spawnErr, sendErr error
	onChange          func(engine.AdmissionChange)
	usage             func() engine.Usage

	stopped bool
}

// New returns a manual engine for node with entropy seed seed, uid epoch 1 and budget 1.
func New(node string, seed uint64) *Engine {
	return &Engine{
		node:   node,
		seed:   seed,
		budget: 1,
		now:    Epoch,
		rands:  make(map[string]*rand.Rand),
		rec:    NewRecorder(),
	}
}

// Engine returns the bundle. Blocking is false; Stop is the engine's stop function.
func (e *Engine) Engine() engine.Engine {
	return engine.Engine{
		Node:      e.node,
		Epoch:     1,
		Blocking:  false,
		Scheduler: scheduler{e},
		Clock:     clock{e},
		Transport: transport{e},
		Effects:   effects{e},
		Entropy:   entropy{e},
		Tracer:    e.rec,
		Admission: admission{e},
		Failer:    failer{e},
		Waiter:    waiter{e},
		Stop:      e.stop,
	}
}

// SetBudget sets the budget passed to RunTurn. It panics if n < 1.
func (e *Engine) SetBudget(n int) {
	if n < 1 {
		panic("enginetest: SetBudget(" + strconv.Itoa(n) + ") with n < 1")
	}
	e.budget = n
}

// SetHost sets the Host that Inbound calls.
func (e *Engine) SetHost(h engine.Host) {
	e.host = h
}

// run calls fn as engine context: InTurn is true while it runs.
func (e *Engine) run(fn func()) {
	e.busy++
	defer func() { e.busy-- }()
	fn()
}

// Step runs the first queued turn and reports whether there was one.
func (e *Engine) Step() bool {
	if e.stopped || len(e.runq) == 0 {
		return false
	}
	a := e.runq[0]
	e.runq[0] = nil
	e.runq = e.runq[1:]
	e.run(func() {
		slot := a.Slot()
		slot.Begin()
		more := a.RunTurn(e.budget)
		if slot.End(more) {
			e.runq = append(e.runq, a)
		}
	})
	return true
}

// RunUntilIdle runs Step until no turn is queued and returns the number of turns run.
func (e *Engine) RunUntilIdle() int {
	n := 0
	for e.Step() {
		n++
		if n >= MaxTurns {
			panic("enginetest: no quiescence after " + strconv.Itoa(MaxTurns) + " turns")
		}
	}
	return n
}

// Advance moves the clock forward by d, firing due timers in order (ENG-520).
func (e *Engine) Advance(d time.Duration) {
	if d < 0 {
		panic("enginetest: Advance(" + d.String() + ") with d < 0")
	}
	target := e.now.Add(d)
	for !e.stopped {
		t := e.nextDue(target)
		if t == nil {
			break
		}
		e.removeTimer(t)
		e.now = t.due
		e.run(t.fn)
		e.RunUntilIdle()
	}
	e.now = target
}

// nextDue returns the pending timer with the smallest (due, seq) that is due by target, or nil.
func (e *Engine) nextDue(target time.Time) *timer {
	var best *timer
	for _, t := range e.timers {
		if t.due.After(target) {
			continue
		}
		if best == nil || t.due.Before(best.due) || (t.due.Equal(best.due) && t.seq < best.seq) {
			best = t
		}
	}
	return best
}

// removeTimer removes t from the pending timers and reports whether it was there.
func (e *Engine) removeTimer(t *timer) bool {
	for i, p := range e.timers {
		if p == t {
			e.timers = slices.Delete(e.timers, i, i+1)
			return true
		}
	}
	return false
}

// Now returns the manual clock's time.
func (e *Engine) Now() time.Time {
	return e.now
}

// Labels returns the labels passed to AfterFunc, in call order.
func (e *Engine) Labels() []string {
	return append([]string(nil), e.labels...)
}

// Records returns the records traced so far, in order.
func (e *Engine) Records() []engine.Record {
	return e.rec.Records()
}

// Failures returns the errors passed to Fail, in order.
func (e *Engine) Failures() []error {
	return append([]error(nil), e.failures...)
}

// Delivered returns the envelopes passed to Transport.Deliver, in order.
func (e *Engine) Delivered() []engine.Envelope {
	return append([]engine.Envelope(nil), e.delivered...)
}

// SetDeliverError sets the error Transport.Deliver returns from now on (nil initially).
func (e *Engine) SetDeliverError(err error) {
	e.deliverErr = err
}

// Inbound calls the Host's Inbound with env as engine context. It panics if no Host is set.
func (e *Engine) Inbound(env engine.Envelope) {
	if e.host == nil {
		panic("enginetest: Inbound with no Host set")
	}
	e.run(func() { e.host.Inbound(env) })
}

// Pending returns the effects started and neither completed nor cancelled, in start order.
func (e *Engine) Pending() []engine.EffectRequest {
	out := make([]engine.EffectRequest, len(e.pending))
	for i, p := range e.pending {
		out[i] = p.req
	}
	return out
}

// removeEffect removes p from the pending effects and reports whether it was there.
func (e *Engine) removeEffect(p *pendingEffect) bool {
	for i, q := range e.pending {
		if q == p {
			e.pending = slices.Delete(e.pending, i, i+1)
			return true
		}
	}
	return false
}

// CompleteEffect calls the done function of pending effect id with (v, err), as engine context,
// and reports whether id was pending.
func (e *Engine) CompleteEffect(id uint64, v any, err error) bool {
	for _, p := range e.pending {
		if p.req.ID == id {
			e.removeEffect(p)
			e.run(func() { p.done(v, err) })
			return true
		}
	}
	return false
}

// SetAdmission sets the errors that AdmitSpawn and AdmitSend return from now on.
func (e *Engine) SetAdmission(spawn, send error) {
	e.spawnErr, e.sendErr = spawn, send
}

// ChangeAdmission calls the onChange registered with Admission.Bind with c, as engine context.
// It panics if Bind has not been called.
func (e *Engine) ChangeAdmission(c engine.AdmissionChange) {
	e.mustBeBound()
	e.run(func() { e.onChange(c) })
}

// Usage calls the usage function registered with Admission.Bind and returns its result.
// It panics if Bind has not been called.
func (e *Engine) Usage() engine.Usage {
	e.mustBeBound()
	return e.usage()
}

func (e *Engine) mustBeBound() {
	if e.onChange == nil {
		panic("enginetest: Admission.Bind not called")
	}
}

// Stopped reports whether the bundle's Stop has been called.
func (e *Engine) Stopped() bool {
	return e.stopped
}

func (e *Engine) stop() {
	e.stopped = true
}

// scheduler is the bundle's Scheduler (ENG-050, ENG-055). It is not a LocalScheduler.
type scheduler struct{ e *Engine }

func (s scheduler) Ready(a engine.ActorRef) {
	if s.e.stopped {
		return
	}
	if a.Slot().Ready() {
		s.e.runq = append(s.e.runq, a)
	}
}

func (s scheduler) InTurn() bool { return s.e.busy > 0 }

// clock is the bundle's Clock (ENG-520).
type clock struct{ e *Engine }

func (c clock) Now() time.Time { return c.e.now }

func (c clock) AfterFunc(d time.Duration, label string, fn func()) engine.Cancel {
	e := c.e
	if e.stopped {
		return func() bool { return false }
	}
	e.labels = append(e.labels, label)
	e.timerSeq++
	t := &timer{due: e.now.Add(max(d, 0)), seq: e.timerSeq, fn: fn}
	e.timers = append(e.timers, t)
	return func() bool { return e.removeTimer(t) }
}

// transport is the bundle's Transport (ENG-560).
type transport struct{ e *Engine }

func (t transport) Deliver(env engine.Envelope) error {
	t.e.delivered = append(t.e.delivered, env)
	return t.e.deliverErr
}

// effects is the bundle's Effects (ENG-530). It never calls Run.
type effects struct{ e *Engine }

func (f effects) Start(req engine.EffectRequest, done func(v any, err error)) engine.Cancel {
	e := f.e
	p := &pendingEffect{req: req, done: done}
	e.pending = append(e.pending, p)
	return func() bool { return e.removeEffect(p) }
}

// entropy is the bundle's Entropy (ENG-540).
type entropy struct{ e *Engine }

func (n entropy) Rand(label string) *rand.Rand {
	if !strings.HasPrefix(label, "lockstep/") {
		panic(fmt.Sprintf("enginetest: Entropy label %q does not start with \"lockstep/\"", label))
	}
	if r, ok := n.e.rands[label]; ok {
		return r
	}
	r := rand.New(rand.NewPCG(n.e.seed, fnv1a64(label))) //nolint:gosec // G404: a seeded deterministic stream is the point
	n.e.rands[label] = r
	return r
}

// fnv1a64 is FNV-1a 64 over the bytes of s.
func fnv1a64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// admission is the bundle's Admission (ENG-560).
type admission struct{ e *Engine }

func (a admission) AdmitSpawn(bool) error { return a.e.spawnErr }
func (a admission) AdmitSend() error      { return a.e.sendErr }

func (a admission) Bind(onChange func(engine.AdmissionChange), usage func() engine.Usage) {
	if a.e.onChange != nil {
		panic("enginetest: Admission.Bind called twice")
	}
	a.e.onChange, a.e.usage = onChange, usage
}

// failer is the bundle's Failer.
type failer struct{ e *Engine }

func (f failer) Fail(err error) { f.e.failures = append(f.e.failures, err) }

// waiter is the bundle's Waiter (ENG-135).
type waiter struct{ e *Engine }

// Wait runs the engine until idle and returns nil if done is then closed, else ErrWouldBlock.
func (w waiter) Wait(_ context.Context, done <-chan struct{}) error {
	w.e.RunUntilIdle()
	if closed(done) {
		return nil
	}
	return ErrWouldBlock
}

// closed reports whether done is closed, without blocking and without a select statement (DL005):
// reflect.Select with a default case takes the default when done is open (or nil).
func closed(done <-chan struct{}) bool {
	chosen, _, _ := reflect.Select([]reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)},
		{Dir: reflect.SelectDefault},
	})
	return chosen == 0
}
