// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
)

// The counters of OBS-120 that the overload events feed.
const (
	shedTotal        = "lockstep_shed_total"
	fullTotal        = "lockstep_mailbox_full_total"
	dropsTotal       = "lockstep_drops_total"
	transitionsTotal = "lockstep_admission_transitions_total"
	stuckTurnsTotal  = "lockstep_stuck_turns_total"
)

// checkSeries fails the test unless, of the series called name that rec registered, the one whose
// label has the value v reads want[v], and every other one reads 0. It fails the test too if rec
// registered no series called name with that label, or if want names a value the series have no
// such label for.
func checkSeries(t *testing.T, rec *recMetrics, name, label string, want map[string]uint64) {
	t.Helper()
	values := labelValues(rec, name, label)
	if len(values) == 0 {
		t.Fatalf("no series %s with label %s", name, label)
	}
	for v := range want {
		if !slices.Contains(values, v) {
			t.Fatalf("%s has no series with %s=%q, only %q", name, label, v, values)
		}
	}
	for _, v := range values {
		if got := rec.count(t, name, label, v); got != want[v] {
			t.Errorf("%s{%s=%q} = %d, want %d", name, label, v, got, want[v])
		}
	}
}

// waitSeries polls the counter name with the labels kv of rec until it reads at least want, and
// fails the test if it does not within 10 s.
func waitSeries(t *testing.T, rec *recMetrics, want uint64, name string, kv ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := rec.count(t, name, kv...)
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %v = %d after 10 s, want %d", name, kv, got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// onlyRecord returns the one record of h with message msg, or fails the test.
func onlyRecord(t *testing.T, h *recHandler, msg string) recRecord {
	t.Helper()
	recs := h.withMsg(msg)
	if len(recs) != 1 {
		t.Fatalf("%d records %q, want 1: %v", len(recs), msg, recs)
	}
	return recs[0]
}

// checkShedRecord fails the test unless rec is the Debug record message shed for the actor pid
// on node n1, with policy and reason.
func checkShedRecord(t *testing.T, rec recRecord, pid lockstep.PID[ping], policy, reason string) {
	t.Helper()
	checkRecord(t, rec, slog.LevelDebug, "node", "path", "uid", "policy", "reason")
	checkAttrs(t, rec, append(actorAttrs("n1", pid), slog.String("policy", policy), slog.String("reason", reason))...)
}

// AT-OBS-12, which also verifies the transitions clause of AT-OVL-32: OBS-060, OBS-120, OBS-130.
// The memory source turns on and off through the sampler's read seam. A message sent in between to
// an actor with an empty FailSend mailbox is shed: it counts as shed and not as a full mailbox.
func TestATOBS12AdmissionEvents(t *testing.T) {
	var live atomic.Uint64
	live.Store(900) // under the low mark: New's first sample leaves admission on
	fakeHeap(t, &live)
	l, h := newRecLogger(slog.LevelDebug)
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l), lockstep.WithMetrics(rec),
		lockstep.WithAdmission(2000, 1000))
	got := make(chan ping, 2)
	pid := spawnObs(t, sys, "a", obsOf(&obsActor{receive: func(_ *lockstep.Context[ping], m ping) { got <- m }}),
		lockstep.WithMailbox(2, lockstep.FailSend))

	checkSeries(t, rec, transitionsTotal, "to", nil)
	if n := rec.gauge(t, "lockstep_admission_refusing"); n != 0 {
		t.Fatalf("lockstep_admission_refusing = %d before the first transition, want 0", n)
	}

	// Refusing: the sampler's next tick (100 ms) reads a live heap over the high mark.
	live.Store(2100)
	refusing := waitRecords(t, h, "admission refusing", 1)[0]
	checkRecord(t, refusing, slog.LevelWarn, "node", "heap_live_bytes", "high_mark_bytes")
	checkAttrs(t, refusing, slog.String("node", "n1"), slog.Uint64("heap_live_bytes", 2100), slog.Uint64("high_mark_bytes", 2000))
	checkSeries(t, rec, transitionsTotal, "to", map[string]uint64{"refusing": 1})
	if n := rec.gauge(t, "lockstep_admission_refusing"); n != 1 {
		t.Errorf("lockstep_admission_refusing = %d while refusing, want 1", n)
	}

	// The shed message: an error that matches ErrOverloaded, one shed and no full mailbox.
	err := lockstep.Send(sys, pid, ping{})
	if !errors.Is(err, lockstep.ErrOverloaded) || !errors.Is(err, lockstep.ErrMailboxFull) {
		t.Fatalf("Send while refusing = %v, want an error matching ErrOverloaded and ErrMailboxFull", err)
	}
	checkSeries(t, rec, shedTotal, "policy", map[string]uint64{"fail-send": 1})
	checkSeries(t, rec, fullTotal, "policy", nil)
	checkSeries(t, rec, dropsTotal, "reason", nil)
	checkShedRecord(t, onlyRecord(t, h, "message shed"), pid, "fail-send", "memory")
	if recs := h.withMsg("mailbox full"); len(recs) != 0 {
		t.Errorf("a shed message logged mailbox full: %v", recs)
	}
	if recs := h.withMsg("message dropped"); len(recs) != 0 {
		t.Errorf("a shed message logged message dropped: %v", recs)
	}

	// Admitting again: the live heap falls under the low mark.
	live.Store(900)
	resumed := waitRecords(t, h, "admission resumed", 1)[0]
	checkRecord(t, resumed, slog.LevelInfo, "node", "heap_live_bytes", "low_mark_bytes")
	checkAttrs(t, resumed, slog.String("node", "n1"), slog.Uint64("heap_live_bytes", 900), slog.Uint64("low_mark_bytes", 1000))
	checkSeries(t, rec, transitionsTotal, "to", map[string]uint64{"refusing": 1, "admitting": 1})
	if n := rec.gauge(t, "lockstep_admission_refusing"); n != 0 {
		t.Errorf("lockstep_admission_refusing = %d after the resume, want 0", n)
	}

	// The same send is delivered now, and counts nowhere.
	if err := lockstep.Send(sys, pid, ping{}); err != nil {
		t.Fatalf("Send after the resume = %v", err)
	}
	recv(t, got)
	checkSeries(t, rec, shedTotal, "policy", map[string]uint64{"fail-send": 1})
	checkSeries(t, rec, fullTotal, "policy", nil)
	if n := len(h.withMsg("message shed")); n != 1 {
		t.Errorf("%d records message shed after the resume, want 1", n)
	}
	if n := len(h.withMsg("admission refusing")) + len(h.withMsg("admission resumed")); n != 2 {
		t.Errorf("%d admission records, want 2", n)
	}
}

// OBS-060, OBS-120, OVL-120: a BlockSender actor's sender that waits because the system sheds is
// counted once, as shed, when it starts to wait. It is not a full mailbox, and its delivery after
// the resume counts nowhere.
func TestBlockSenderShedCountedOnce(t *testing.T) {
	var live atomic.Uint64
	live.Store(900)
	fakeHeap(t, &live)
	l, h := newRecLogger(slog.LevelDebug)
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l), lockstep.WithMetrics(rec),
		lockstep.WithAdmission(2000, 1000))
	got := make(chan ping, 1)
	pid := spawnObs(t, sys, "a", obsOf(&obsActor{receive: func(_ *lockstep.Context[ping], m ping) { got <- m }}),
		lockstep.WithMailbox(2, lockstep.BlockSender))

	live.Store(2100)
	waitRecords(t, h, "admission refusing", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sent := make(chan error, 1)
	go func() { sent <- lockstep.SendContext(ctx, sys, pid, ping{}) }()

	checkShedRecord(t, waitRecords(t, h, "message shed", 1)[0], pid, "block-sender", "memory")
	checkSeries(t, rec, shedTotal, "policy", map[string]uint64{"block-sender": 1})
	checkSeries(t, rec, fullTotal, "policy", nil)
	select {
	case err := <-sent:
		t.Fatalf("SendContext returned %v while the system sheds, want it to wait", err)
	case <-time.After(50 * time.Millisecond):
	}

	live.Store(900)
	if err := recv(t, sent); err != nil {
		t.Fatalf("SendContext after the resume = %v, want nil", err)
	}
	recv(t, got)
	checkSeries(t, rec, shedTotal, "policy", map[string]uint64{"block-sender": 1})
	checkSeries(t, rec, fullTotal, "policy", nil)
	if n := len(h.withMsg("message shed")); n != 1 {
		t.Errorf("%d records message shed, want 1: the wait is one message", n)
	}
	if recs := h.withMsg("mailbox full"); len(recs) != 0 {
		t.Errorf("a shed wait logged mailbox full: %v", recs)
	}
}

// OBS-120: a BlockSender send that finds the mailbox full counts once, when it first has to wait,
// and not again when it is enqueued.
func TestBlockSenderFullCountedOnce(t *testing.T) {
	l, h := newRecLogger(slog.LevelDebug)
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l), lockstep.WithMetrics(rec))
	got := make(chan ping, 2)
	pid := spawnObs(t, sys, "a", obsOf(&obsActor{receive: func(_ *lockstep.Context[ping], m ping) { got <- m }}),
		lockstep.WithMailbox(1, lockstep.BlockSender))
	release := lockstep.Hold(sys, pid.Address())
	t.Cleanup(release) // runs before newProdSystem's Shutdown

	if err := lockstep.Send(sys, pid, ping{}); err != nil {
		t.Fatalf("Send to an empty mailbox = %v", err)
	}
	checkSeries(t, rec, fullTotal, "policy", nil)
	sent := make(chan error, 1)
	go func() { sent <- lockstep.Send(sys, pid, ping{}) }()
	waitSeries(t, rec, 1, fullTotal, "policy", "block-sender")
	select {
	case err := <-sent:
		t.Fatalf("Send to a full BlockSender mailbox returned %v, want it to wait", err)
	case <-time.After(50 * time.Millisecond):
	}

	release() // the actor consumes the first message, which makes room for the second
	if err := recv(t, sent); err != nil {
		t.Fatalf("Send after the room came = %v, want nil", err)
	}
	recv(t, got)
	recv(t, got)
	checkSeries(t, rec, fullTotal, "policy", map[string]uint64{"block-sender": 1})
	checkSeries(t, rec, shedTotal, "policy", nil)
	if recs := h.withMsg("mailbox full"); len(recs) != 1 {
		t.Errorf("%d records mailbox full, want 1: %v", len(recs), recs)
	}
}

// AT-OBS-14, subtests stuck and goroutines, which also verifies the counter clause of AT-OVL-37:
// OBS-060, OBS-120.
func TestATOBS14OverloadEvents(t *testing.T) {
	t.Run("stuck", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelDebug)
		rec := &recMetrics{}
		sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l), lockstep.WithMetrics(rec),
			lockstep.WithWatchdog(50*time.Millisecond))
		stuckTurns := rec.find(t, stuckTurnsTotal)
		done := make(chan struct{})
		pid := spawnObs(t, sys, "a", obsOf(&obsActor{receive: func(*lockstep.Context[ping], ping) {
			// The turn spins 200 ms. It goes on, up to 10 s, until the watchdog has counted it, so a
			// machine that is too busy to poll within 200 ms does not end the turn unseen.
			start := time.Now()
			for time.Since(start) < 200*time.Millisecond || (stuckTurns.n.Load() == 0 && time.Since(start) < 10*time.Second) {
				runtime.Gosched()
			}
			close(done)
		}}))
		if err := lockstep.Send(sys, pid, ping{}); err != nil {
			t.Fatal(err)
		}
		recv(t, done)
		waitRecords(t, h, "turn exceeds watchdog timeout", 1)
		time.Sleep(50 * time.Millisecond) // the watchdog reports a turn once: wait for a late second report

		stuck := onlyRecord(t, h, "turn exceeds watchdog timeout")
		checkRecord(t, stuck, slog.LevelError, "node", "path", "uid", "elapsed", "timeout", "stack")
		checkAttrs(t, stuck, actorAttrs("n1", pid)...)
		if v := attrOf(t, stuck, "elapsed"); v.Kind() != slog.KindDuration || v.Duration() < 50*time.Millisecond || v.Duration()%time.Millisecond != 0 {
			t.Errorf("elapsed = %v (%v), want a duration of at least 50ms in whole milliseconds", v, v.Kind())
		}
		if v := attrOf(t, stuck, "timeout"); v.Kind() != slog.KindDuration || v.Duration() != 50*time.Millisecond {
			t.Errorf("timeout = %v (%v), want 50ms", v, v.Kind())
		}
		if v := attrOf(t, stuck, "stack"); v.Kind() != slog.KindString || !strings.HasPrefix(v.String(), "goroutine ") ||
			!strings.Contains(v.String(), "TestATOBS14OverloadEvents") {
			t.Errorf("stack is not the worker's, running this test's actor:\n%v", v)
		}
		if got := rec.count(t, stuckTurnsTotal); got != 1 {
			t.Errorf("%s = %d, want 1", stuckTurnsTotal, got)
		}
	})

	t.Run("goroutines", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelDebug)
		rec := &recMetrics{}
		// New starts the two workers and the watchdog: three goroutines below the bound of ten.
		bound := runtime.NumGoroutine() + 10
		newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l), lockstep.WithMetrics(rec),
			lockstep.WithWorkers(2), lockstep.WithGoroutineWarning(bound))
		const started = 20
		stop := make(chan struct{})
		var wg sync.WaitGroup
		defer func() { // the goroutines end with the test
			close(stop)
			wg.Wait()
		}()
		for range started {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-stop
			}()
		}
		waitRecords(t, h, "goroutine count above bound", 1) // the monitor's sample, 1 s after New
		time.Sleep(1200 * time.Millisecond)                 // a second sample finds the monitor disarmed

		warn := onlyRecord(t, h, "goroutine count above bound")
		checkRecord(t, warn, slog.LevelWarn, "node", "goroutines", "bound", "summary")
		checkAttrs(t, warn, slog.String("node", "n1"))
		if v := attrOf(t, warn, "goroutines"); v.Kind() != slog.KindInt64 || v.Int64() < int64(bound) {
			t.Errorf("goroutines = %v (%v), want an int of at least the bound %d", v, v.Kind(), bound)
		}
		if v := attrOf(t, warn, "bound"); v.Kind() != slog.KindInt64 || v.Int64() != int64(bound) {
			t.Errorf("bound = %v (%v), want %d", v, v.Kind(), bound)
		}
		// The summary credits each goroutine to its first frame's function: the test's own 20 goroutines
		// wait in one function, whatever else the process runs.
		summary := attrOf(t, warn, "summary")
		want := strconv.Itoa(started) + " "
		found := false
		for item := range strings.SplitSeq(summary.String(), "; ") {
			if fn, ok := strings.CutPrefix(item, want); ok && strings.Contains(fn, "TestATOBS14OverloadEvents") {
				found = true
			}
		}
		if summary.Kind() != slog.KindString || !found {
			t.Errorf("summary = %q, want an item %q followed by this test's function", summary, want)
		}
		if n := rec.gauge(t, "lockstep_goroutines"); n < int64(bound) {
			t.Errorf("lockstep_goroutines = %d, want at least the bound %d", n, bound)
		}
	})
}

// OBS-120, OVL-520: a message refused because its target is stuck is counted as shed, with the
// reason stuck, and is not a full mailbox or a drop.
func TestShedToStuckActor(t *testing.T) {
	l, h := newRecLogger(slog.LevelDebug)
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l), lockstep.WithMetrics(rec),
		lockstep.WithWatchdog(50*time.Millisecond))
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release) }) // before the shutdown, which waits for the turn
	var blocked atomic.Bool
	pid := spawnObs(t, sys, "a", obsOf(&obsActor{receive: func(*lockstep.Context[ping], ping) {
		if blocked.CompareAndSwap(false, true) { // later messages, queued before the mark, do not block
			close(entered)
			<-release
		}
	}}), lockstep.WithMailbox(1024, lockstep.FailSend)) // room for the sends until the mark
	if err := lockstep.Send(sys, pid, ping{}); err != nil {
		t.Fatal(err)
	}
	recv(t, entered)
	waitSeries(t, rec, 1, stuckTurnsTotal)

	// The watchdog counts and logs the turn, then marks the actor stuck: a send in between is
	// queued, and the next is refused.
	var err error
	deadline := time.Now().Add(10 * time.Second)
	for err = lockstep.Send(sys, pid, ping{}); err == nil; err = lockstep.Send(sys, pid, ping{}) {
		if time.Now().After(deadline) {
			t.Fatal("no send was refused within 10 s of the stuck turn")
		}
		time.Sleep(time.Millisecond)
	}
	if want := "lockstep: send to " + pid.Address().String() + ": actor stuck, mailbox treated as full"; err.Error() != want {
		t.Fatalf("Send to a stuck actor = %q, want %q", err, want)
	}
	if !errors.Is(err, lockstep.ErrMailboxFull) || errors.Is(err, lockstep.ErrOverloaded) {
		t.Errorf("the error matches ErrMailboxFull %v and ErrOverloaded %v, want true and false",
			errors.Is(err, lockstep.ErrMailboxFull), errors.Is(err, lockstep.ErrOverloaded))
	}
	checkSeries(t, rec, shedTotal, "policy", map[string]uint64{"fail-send": 1})
	checkSeries(t, rec, fullTotal, "policy", nil)
	checkSeries(t, rec, dropsTotal, "reason", nil)
	checkShedRecord(t, onlyRecord(t, h, "message shed"), pid, "fail-send", "stuck")
	if recs := h.withMsg("mailbox full"); len(recs) != 0 {
		t.Errorf("a message to a stuck actor logged mailbox full: %v", recs)
	}
}
