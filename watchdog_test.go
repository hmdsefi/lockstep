// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"log"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep/internal/overload"
)

// stuckMsg is the message of OBS-060's "Stuck turn" event (OVL-510).
const stuckMsg = "turn exceeds watchdog timeout"

// watchdogCreator is the "created by" line of the watchdog's goroutine in a stack dump. Its top
// frame names the wrapper of the go statement until the goroutine first runs.
const watchdogCreator = "created by github.com/hmdsefi/lockstep/internal/overload.(*Watchdog).Start "

// restoreStdLog restores the standard log package's output and flags when the test ends.
// slog.SetDefault with a handler of its own redirects them, and restoring slog's default logger
// does not undo that. Call it before captureDefaultLogger, so it runs after that restore.
func restoreStdLog(t *testing.T) {
	t.Helper()
	w, f := log.Writer(), log.Flags()
	t.Cleanup(func() {
		log.SetOutput(w)
		log.SetFlags(f)
	})
}

// stuckRecords returns the records of recs with the stuck-turn message.
func stuckRecords(recs []slogRecord) []slogRecord {
	return slices.DeleteFunc(recs, func(r slogRecord) bool { return r.msg != stuckMsg })
}

// goroutineStacks returns the stack of every goroutine, from runtime.Stack, by its header line
// "goroutine <id> [<state>]:" without the state.
func goroutineStacks() map[string]string {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	stacks := make(map[string]string)
	for _, sec := range strings.Split(strings.TrimSpace(string(buf)), "\n\n") {
		id, _, _ := strings.Cut(sec, " [")
		stacks[id] = sec
	}
	return stacks
}

// startedSince returns the stacks of the goroutines that did not exist in before, leaving out
// the short-lived ones that run a time.AfterFunc callback: the production engine's timers fire
// on them.
func startedSince(before map[string]string) []string {
	var started []string
	for id, stack := range goroutineStacks() {
		if _, ok := before[id]; !ok && !strings.Contains(stack, "created by time.goFunc") {
			started = append(started, stack)
		}
	}
	return started
}

// watchdogRunning reports whether a watchdog goroutine runs.
func watchdogRunning() bool {
	for _, stack := range goroutineStacks() {
		if strings.Contains(stack, watchdogCreator) {
			return true
		}
	}
	return false
}

// AT-OVL-37: OVL-500, OVL-510. With WithWatchdog(100ms), a turn blocked for 300 ms is logged
// once through the system's logger at Error, with the actor's node, path and uid, its running
// time rounded down to milliseconds, the timeout, and the stack of the worker running it. That
// internal/prodengine does not import internal/overload (R38) is TestImportBoundaries' DET-080
// row for internal/prodengine.
func TestATOVL37StuckTurnReported(t *testing.T) {
	verifyNoneAtEnd(t)
	restoreStdLog(t)
	records := captureDefaultLogger(t)
	sys := newLocalSystem(t, WithNodeName("n1"), WithWatchdog(100*time.Millisecond))
	entered, release := make(chan struct{}, 1), make(chan struct{})
	t.Cleanup(func() { close(release) }) // before the shutdown, which waits for the turn
	pid, err := Spawn(sys, "stuck", actorOf[int](&fnActor[int]{receive: func(*Context[int], int) {
		entered <- struct{}{}
		<-release
	}}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, pid, 1)
	recv(t, entered)
	began := time.Now()
	waitUntil(t, "a stuck-turn record", func() bool { return len(stuckRecords(records())) > 0 })
	time.Sleep(time.Until(began.Add(300 * time.Millisecond)))

	got := stuckRecords(records())
	if len(got) != 1 {
		t.Fatalf("%d stuck-turn records, want 1: %+v", len(got), got)
	}
	rec := got[0]
	if rec.level != slog.LevelError {
		t.Errorf("level = %v, want ERROR", rec.level)
	}
	keys := make([]string, len(rec.attrs))
	for i, a := range rec.attrs {
		keys[i] = a.Key
	}
	if want := []string{"node", "path", "uid", "elapsed", "timeout", "stack"}; !slices.Equal(keys, want) {
		t.Fatalf("attributes %v, want %v", keys, want)
	}
	want := []slog.Attr{slog.String("node", "n1"), slog.String("path", "/user/stuck"), slog.Uint64("uid", pid.Address().uid)}
	if !slices.EqualFunc(rec.attrs[:3], want, slog.Attr.Equal) {
		t.Errorf("actor attributes %v, want %v", rec.attrs[:3], want)
	}
	elapsed, timeout, stack := rec.attrs[3].Value, rec.attrs[4].Value, rec.attrs[5].Value
	if elapsed.Kind() != slog.KindDuration || elapsed.Duration() < 100*time.Millisecond || elapsed.Duration()%time.Millisecond != 0 {
		t.Errorf("elapsed = %v (%v), want a duration of at least 100ms in whole milliseconds", elapsed, elapsed.Kind())
	}
	if !timeout.Equal(slog.DurationValue(100 * time.Millisecond)) {
		t.Errorf("timeout = %v, want 100ms", timeout)
	}
	if s := stack.String(); stack.Kind() != slog.KindString || !strings.HasPrefix(s, "goroutine ") ||
		!strings.Contains(s, "TestATOVL37StuckTurnReported") {
		t.Errorf("stack is not the worker's, running this test's actor:\n%s", s)
	}
}

// AT-OVL-41: OVL-500. WithWatchdog(0) turns the watchdog off: no watchdog goroutine runs, a turn
// that blocks 300 ms is not reported, and the actor is not failed.
func TestATOVL41WatchdogOff(t *testing.T) {
	verifyNoneAtEnd(t)
	restoreStdLog(t)
	records := captureDefaultLogger(t)
	sys := newLocalSystem(t, WithWatchdog(0))
	if sys.prodOvl.watchdog != nil || watchdogRunning() {
		t.Fatal("a watchdog runs with WithWatchdog(0)")
	}
	var builds atomic.Int32
	done := make(chan int, 2)
	pid, err := Spawn(sys, "slow", func() Actor[int] {
		builds.Add(1)
		return &fnActor[int]{receive: func(_ *Context[int], n int) {
			if n == 1 {
				time.Sleep(300 * time.Millisecond)
			}
			done <- n
		}}
	})
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, pid, 1)
	send(t, sys, pid, 2)
	for want := 1; want <= 2; want++ {
		if n := recv(t, done); n != want {
			t.Fatalf("the actor handled %d, want %d", n, want)
		}
	}
	if n := builds.Load(); n != 1 {
		t.Errorf("the actor was built %d times, want once: it failed", n)
	}
	if got := stuckRecords(records()); len(got) != 0 {
		t.Errorf("stuck-turn records %+v, want none", got)
	}
}

// AT-OVL-44: OVL-700. New with every overload feature on starts one goroutine besides its
// workers, the watchdog's; after Shutdown no goroutine is left.
func TestATOVL44OneGoroutine(t *testing.T) {
	const workers = 2
	before := goroutineStacks()
	sys := New(WithWorkers(workers), WithAdmission(1<<62, 1<<61), WithMemoryEstimates(),
		WithWatchdog(time.Second), WithGoroutineWarning(DefaultGoroutineWarning))
	shutdownAtEnd(t, sys)

	var started []string
	waitUntil(t, "the workers and the watchdog started", func() bool {
		started = startedSince(before)
		return len(started) == workers+1
	})
	n := 0
	for _, stack := range started {
		if strings.Contains(stack, watchdogCreator) {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d of the %d goroutines New started run the watchdog, want 1:\n%s", n, len(started), strings.Join(started, "\n\n"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	goleak.VerifyNone(t)
}

// OVL-700: when an actor shuts the system down, the shutdown completes in engine context and the
// engine stops on a goroutine of its own; the watchdog has exited once the engine has stopped.
func TestOVL700WatchdogStopsInEngineContext(t *testing.T) {
	sys := newLocalSystem(t, WithWatchdog(time.Second))
	waitUntil(t, "the watchdog started", watchdogRunning)
	returned := make(chan error, 1)
	closer, err := Spawn(sys, "closer", actorOf[int](&fnActor[int]{receive: func(*Context[int], int) {
		returned <- sys.Shutdown(context.Background())
	}}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, closer, 1)
	if err := recv(t, returned); err != nil {
		t.Fatalf("Shutdown during a turn = %v, want nil", err)
	}
	waitUntil(t, "engine stopped", sys.shut.engStopped.Load)
	goleak.VerifyNone(t)
}

// OVL-510: a stuck turn of the guardians, which are not a cell, is logged with the user
// guardian's address, and the running time is rounded down to milliseconds.
func TestStuckTurnOfGuardians(t *testing.T) {
	restoreStdLog(t)
	records := captureDefaultLogger(t)
	sys := newLocalSystem(t, WithNodeName("n1"), WithWatchdog(0))
	turn := overload.Turn{Worker: 0, GoID: 1, Actor: &sys.guardians, Start: time.Now()}
	sys.onStuck(turn, 1999*time.Microsecond, time.Second, "goroutine 1 [select]:")

	got := stuckRecords(records())
	if len(got) != 1 {
		t.Fatalf("%d stuck-turn records, want 1: %+v", len(got), got)
	}
	want := []slog.Attr{
		slog.String("node", "n1"), slog.String("path", "/user"), slog.Uint64("uid", sys.user.uid),
		slog.Duration("elapsed", time.Millisecond), slog.Duration("timeout", time.Second),
		slog.String("stack", "goroutine 1 [select]:"),
	}
	if got[0].level != slog.LevelError || !slices.EqualFunc(got[0].attrs, want, slog.Attr.Equal) {
		t.Errorf("record %v %v, want ERROR %v", got[0].level, got[0].attrs, want)
	}
}
