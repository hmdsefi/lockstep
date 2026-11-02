// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestATSCH24RunningTurns covers SCH-270 (AT-SCH-24): a turn in progress is reported with its
// worker, the worker's goroutine ID and its start time, and is gone once it ends.
func TestATSCH24RunningTurns(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2})
	a := newFake(t, 0)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock) // runs before the engine's Stop, which waits for the turn
	var entered atomic.Bool
	a.onTurn = func(*fakeActor, int) {
		entered.Store(true)
		<-release
	}
	a.add()
	e.s.Ready(a)
	waitFor(t, time.Second, "the blocking turn", entered.Load)

	rts := e.RunningTurns(nil)
	if len(rts) != 1 {
		t.Fatalf("RunningTurns(nil) = %v while one turn blocks, want one entry", rts)
	}
	rt := rts[0]
	if rt.Actor != a {
		t.Errorf("RunningTurns: Actor = %v, want the blocked fake", rt.Actor)
	}
	if rt.Worker < 0 || rt.Worker >= 2 {
		t.Errorf("RunningTurns: Worker = %d, want in [0, 2)", rt.Worker)
	}
	if !bytes.Contains(allStacks(), fmt.Appendf(nil, "goroutine %d [", rt.GoID)) {
		t.Errorf("RunningTurns: GoID %d is not a goroutine in runtime.Stack", rt.GoID)
	}
	if d := time.Since(rt.Start); d < 0 || d > time.Second {
		t.Errorf("RunningTurns: Start is %v ago, want within the last second", d)
	}

	unblock()
	waitFor(t, time.Second, "an empty RunningTurns after the turn", func() bool {
		return len(e.RunningTurns(nil)) == 0
	})
}

// TestRunningTurnsStartIsStable covers SCH-270 and OVL-500's use of it: the watchdog identifies a
// turn by its (Worker, Start) pair and measures it with time.Since, so two snapshots of one turn
// must report equal starts that carry a monotonic reading.
func TestRunningTurnsStartIsStable(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 1})
	a := newFake(t, 0)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock) // runs before the engine's Stop, which waits for the turn
	var entered atomic.Bool
	a.onTurn = func(*fakeActor, int) {
		entered.Store(true)
		<-release
	}
	before := time.Now()
	a.add()
	e.s.Ready(a)
	waitFor(t, time.Second, "the blocking turn", entered.Load)

	first, second := e.RunningTurns(nil), e.RunningTurns(nil)
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("RunningTurns = %v, then %v, while one turn blocks; want one entry each", first, second)
	}
	start := first[0].Start
	if !start.Equal(second[0].Start) {
		t.Errorf("two snapshots of one turn: Start %v, then %v; want equal", start, second[0].Start)
	}
	if !strings.Contains(start.String(), " m=") { // String prints a monotonic reading as m=±<s>
		t.Errorf("RunningTurns: Start %v has no monotonic reading", start)
	}
	if start.Before(before) || time.Since(start) > time.Second {
		t.Errorf("RunningTurns: Start %v, want between %v and now", start, before)
	}
}

// TestRunningTurnsSkipsCallbacks covers SCH-270: a worker running a callback is not reported.
func TestRunningTurnsSkipsCallbacks(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 2})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock) // runs before the engine's Stop, which waits for the callback
	var entered atomic.Bool
	e.s.pushCallback(func() {
		entered.Store(true)
		<-release
	})
	waitFor(t, time.Second, "the blocking callback", entered.Load)
	rts := e.RunningTurns(nil)
	unblock()
	if len(rts) != 0 {
		t.Errorf("RunningTurns(nil) = %v while only a callback runs, want empty", rts)
	}
}

// TestRunningTurnsAppends checks that RunningTurns appends to dst.
func TestRunningTurnsAppends(t *testing.T) {
	e := newEngine(t, Config{Node: "n", Workers: 1})
	dst := []RunningTurn{{Worker: 7}}
	if got := e.RunningTurns(dst); len(got) != 1 || got[0].Worker != 7 {
		t.Errorf("RunningTurns(dst) with no turn = %v, want dst unchanged", got)
	}
}

// allStacks returns the stack traces of every goroutine.
func allStacks() []byte {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
