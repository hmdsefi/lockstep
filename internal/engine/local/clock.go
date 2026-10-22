// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package local

import (
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// States of a timer or an effect. Exactly one CompareAndSwap away from pending wins, so Cancel
// returns true at most once and only while the callback has not started (ENG-420, ENG-430).
const (
	pending   uint32 = iota // armed or running; the callback has not started
	started                 // the callback's task started
	cancelled               // Cancel or Stop won; the callback never runs
)

// timer is one AfterFunc call.
type timer struct {
	state atomic.Uint32
	rt    *time.Timer // set under Engine.mu before anything reads it
	fn    func()
}

// clock is the bundle's Clock (ENG-420).
type clock struct{ e *Engine }

// Now returns the wall time.
func (clock) Now() time.Time { return time.Now() }

// AfterFunc arms a runtime timer that queues a task running fn once d has passed. After Stop it
// does nothing and its Cancel returns false.
func (c clock) AfterFunc(d time.Duration, _ string, fn func()) engine.Cancel {
	e := c.e
	t := &timer{fn: fn}
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return cancelNothing
	}
	e.timers[t] = struct{}{}
	t.rt = time.AfterFunc(max(d, 0), func() { e.fire(t) })
	e.mu.Unlock()
	return func() bool {
		if !t.state.CompareAndSwap(pending, cancelled) {
			return false
		}
		t.rt.Stop()
		e.mu.Lock()
		delete(e.timers, t) // no-op after Stop, which set the map to nil
		e.mu.Unlock()
		return true
	}
}

// fire runs on the runtime timer's goroutine: it queues the task that runs t.fn on the loop.
func (e *Engine) fire(t *timer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return
	}
	delete(e.timers, t)
	e.tasks = append(e.tasks, task{fn: func() {
		if t.state.CompareAndSwap(pending, started) {
			t.fn()
		}
	}})
	e.notify() // non-blocking, so holding mu is fine
}

// cancelNothing is the Cancel of a callback that was never armed.
func cancelNothing() bool { return false }
