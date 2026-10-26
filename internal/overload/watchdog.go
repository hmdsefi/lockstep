// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

const (
	// minPollPeriod is the shortest period of the watchdog's polls (OVL-500).
	minPollPeriod = 10 * time.Millisecond
	// stackLimit is the longest stack section a stuck report carries before it is truncated
	// (OVL-510).
	stackLimit = 64 << 10
	// truncatedMark follows a stack section cut at stackLimit (OVL-510).
	truncatedMark = "...(truncated)"
)

// stackHook, when set, replaces runtime.Stack (OVL §10).
var stackHook atomic.Pointer[func([]byte, bool) int]

// stackDump calls the test hook if one is set and runtime.Stack otherwise.
func stackDump(buf []byte, all bool) int {
	if h := stackHook.Load(); h != nil {
		return (*h)(buf, all)
	}
	return runtime.Stack(buf, all)
}

// Turn is one turn in progress as the watchdog sees it. The runtime fills it from SCH's
// prodengine.RunningTurn (SCH-270), field by field; internal/overload does not import
// internal/prodengine, and internal/prodengine does not import internal/overload (R38).
type Turn struct {
	Worker int             // worker index
	GoID   uint64          // goroutine ID of the worker
	Actor  engine.ActorRef // actor whose turn is running
	Start  time.Time       // when the turn started
}

// Watchdog polls the turns in progress and reports those running past a timeout (production).
type Watchdog struct {
	timeout time.Duration
	running func(dst []Turn) []Turn
	onStuck func(t Turn, elapsed time.Duration, stack string)

	mu      sync.Mutex // guards started and stopped
	started bool
	stopped bool
	stop    chan struct{} // closed by the first Stop after Start
	done    chan struct{} // closed when the polling goroutine has exited
}

// NewWatchdog returns a stopped watchdog. running appends the turns in progress to dst and
// returns the extended slice; the runtime passes an adapter over (*prodengine.Engine).RunningTurns.
// onStuck is called on the watchdog goroutine, once per stuck turn (OVL-500).
func NewWatchdog(timeout time.Duration, running func(dst []Turn) []Turn, onStuck func(t Turn, elapsed time.Duration, stack string)) *Watchdog {
	return &Watchdog{
		timeout: timeout,
		running: running,
		onStuck: onStuck,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Start starts the polling goroutine (OVL-700). Calling it again, or after Stop, does nothing.
func (w *Watchdog) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.stopped {
		return
	}
	w.started = true
	go w.run()
}

// Stop stops polling and returns after the goroutine exited, waiting for an onStuck call in
// progress. It may be called more than once, and without Start.
func (w *Watchdog) Stop() {
	w.mu.Lock()
	if !w.stopped {
		w.stopped = true
		if w.started {
			close(w.stop)
		}
	}
	started := w.started
	w.mu.Unlock()
	if started {
		<-w.done
	}
}

// pollPeriod returns the period of the polls for timeout d: max(d/5, 10ms) (OVL-500).
func pollPeriod(d time.Duration) time.Duration {
	return max(d/5, minPollPeriod)
}

// run is the polling goroutine (OVL-500, OVL §11). turns is the one buffer every poll reuses;
// reported holds, per worker, the Start of the turn reported last, while that turn still runs.
func (w *Watchdog) run() {
	defer close(w.done)
	tick := time.NewTicker(pollPeriod(w.timeout))
	defer tick.Stop()
	var turns []Turn
	reported := make(map[int]time.Time)
	for {
		select {
		case <-w.stop:
			return
		case <-tick.C:
		}
		turns = w.poll(turns[:0], reported)
	}
}

// poll reads the turns in progress into turns, reports each stuck turn not reported yet, drops
// the reported turns that have ended, and returns the buffer with its actor references cleared.
// A turn is identified by its (Worker, Start) pair; a worker runs one turn at a time, so the
// reported set holds at most one pair per worker.
func (w *Watchdog) poll(turns []Turn, reported map[int]time.Time) []Turn {
	turns = w.running(turns)
	for worker, start := range reported {
		if !runs(turns, worker, start) {
			delete(reported, worker)
		}
	}
	for _, t := range turns {
		if start, ok := reported[t.Worker]; ok && start.Equal(t.Start) {
			continue
		}
		elapsed := time.Since(t.Start)
		if elapsed < w.timeout {
			continue
		}
		reported[t.Worker] = t.Start
		w.onStuck(t, elapsed, GoroutineStack(t.GoID))
	}
	clear(turns) // keep no actor alive until the next poll
	return turns
}

// runs reports whether turns holds the turn (worker, start).
func runs(turns []Turn, worker int, start time.Time) bool {
	for _, t := range turns {
		if t.Worker == worker && t.Start.Equal(start) {
			return true
		}
	}
	return false
}

// GoroutineStack returns the section of runtime.Stack(buf, true) for the goroutine whose ID is
// goid: from its "goroutine <goid> [" header to the end of its last line. A section longer than
// 64 KiB is cut after the last line end within its first 64 KiB and followed by "...(truncated)"
// (OVL-510). It returns "" when no such goroutine exists. It stops the world for the dump.
func GoroutineStack(goid uint64) string {
	buf := make([]byte, stackLimit)
	for {
		n := stackDump(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	return stackSection(buf, goid)
}

// stackSection cuts the section of goroutine goid out of dump, the output of runtime.Stack with
// all set: sections are separated by a blank line and each starts with "goroutine <id> [".
func stackSection(dump []byte, goid uint64) string {
	head := []byte("goroutine " + strconv.FormatUint(goid, 10) + " [")
	start := -1
	for i := 0; i < len(dump); {
		j := bytes.Index(dump[i:], head)
		if j < 0 {
			break
		}
		if k := i + j; k == 0 || dump[k-1] == '\n' {
			start = k
			break
		}
		i += j + 1
	}
	if start < 0 {
		return ""
	}
	sec := dump[start:]
	if end := bytes.Index(sec, []byte("\n\n")); end >= 0 {
		sec = sec[:end]
	}
	sec = bytes.TrimSuffix(sec, []byte("\n"))
	if len(sec) <= stackLimit {
		return string(sec)
	}
	cut := stackLimit
	if nl := bytes.LastIndexByte(sec[:stackLimit], '\n'); nl >= 0 {
		cut = nl + 1
	}
	return string(sec[:cut]) + truncatedMark
}
