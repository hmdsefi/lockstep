// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// States of a timer. Exactly one CompareAndSwap away from timerPending wins, so Cancel returns
// true at most once and only while the callback has not started (SCH-130, SCH §11).
const (
	timerPending   uint32 = iota // armed or fired; the callback has not started
	timerStarted                 // the callback started
	timerCancelled               // Cancel (or Stop) won; the function never runs
)

// timer is one AfterFunc call.
type timer struct {
	state atomic.Uint32
	rt    *time.Timer // set under Engine.tmu before anything else reads it
	fn    func()
}

// clock is the bundle's Clock (SCH-120, SCH-130).
type clock struct{ e *Engine }

var _ engine.Clock = clock{}

// clock returns the engine's Clock.
func (e *Engine) clock() engine.Clock { return clock{e} }

// Now returns the wall time (SCH-120).
func (clock) Now() time.Time { return time.Now() }

// AfterFunc arms a runtime timer that, once d has passed, pushes a callback running fn in
// engine context (SCH-130). label is ignored. While the engine is closing it arms nothing and
// the returned Cancel reports false.
func (c clock) AfterFunc(d time.Duration, _ string, fn func()) engine.Cancel {
	e := c.e
	t := &timer{fn: fn}
	e.tmu.Lock()
	if e.s.closing.Load() {
		e.tmu.Unlock()
		return cancelNothing
	}
	e.timers[t] = struct{}{}
	t.rt = time.AfterFunc(max(d, 0), func() { e.fire(t) })
	e.tmu.Unlock()
	return func() bool {
		if !t.state.CompareAndSwap(timerPending, timerCancelled) {
			return false
		}
		t.rt.Stop()
		e.removeTimer(t)
		return true
	}
}

// fire runs on the goroutine of the runtime timer: it pushes the callback that runs t.fn. A
// timer that fires while the engine is closing runs nothing (SCH-130).
func (e *Engine) fire(t *timer) {
	if e.s.closing.Load() {
		return
	}
	e.s.pushCallback(func() { e.runTimer(t) })
}

// runTimer is the callback of t, run by a worker. It starts t.fn unless Cancel won, or unless
// the engine is closing.
func (e *Engine) runTimer(t *timer) {
	if !t.state.CompareAndSwap(timerPending, timerStarted) {
		return
	}
	e.removeTimer(t)
	if e.s.closing.Load() {
		return
	}
	t.fn()
}

// stopTimers stops every pending timer (SCH-200 step 2): it wins each timer's state from
// pending to cancelled, stops its runtime timer and empties the pending set, so the pending count
// falls to 0 (SCH-140). The caller marked the engine closing first, so an AfterFunc either
// registered its timer before stopTimers locks tmu and is stopped here, or sees the mark and arms
// nothing. A timer whose callback already started, or whose Cancel already won, leaves the set
// here too: runTimer checks the mark after its state change, and a callback that passed that
// check before the mark runs on a worker, which Stop waits for.
func (e *Engine) stopTimers() {
	e.tmu.Lock()
	for t := range e.timers {
		if t.state.CompareAndSwap(timerPending, timerCancelled) {
			t.rt.Stop()
		}
	}
	clear(e.timers)
	e.tmu.Unlock()
}

// removeTimer drops t from the pending set; the pending count falls by one (SCH-140).
func (e *Engine) removeTimer(t *timer) {
	e.tmu.Lock()
	delete(e.timers, t)
	e.tmu.Unlock()
}

// pendingTimers returns the number of pending timers: armed and neither started nor cancelled,
// ask deadlines included (SCH-140). Tests use it.
func (e *Engine) pendingTimers() int {
	e.tmu.Lock()
	n := len(e.timers)
	e.tmu.Unlock()
	return n + e.dl.pending()
}

// cancelNothing is the Cancel of a timer that was never armed.
func cancelNothing() bool { return false }
