// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package hollywood

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthdm/hollywood/actor"

	"github.com/hmdsefi/lockstep/bench/harness"
)

// closeWait is how long the tests give Close, which itself waits without a deadline, before they
// call it stuck.
const closeWait = time.Minute

// start starts a system of the adapter and closes it when the test ends, unless the test closed
// it.
func start(t *testing.T) *system {
	t.Helper()
	hs, err := Lib{}.Start()
	if err != nil {
		t.Fatal(err)
	}
	s, ok := hs.(*system)
	if !ok {
		t.Fatalf("Start returned a %T", hs)
	}
	t.Cleanup(func() { _ = closeWithin(s, closeWait) })
	return s
}

// closeWithin closes s and returns an error if Close has not returned after d.
func closeWithin(s *system, d time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		return fmt.Errorf("did not return within %s", d)
	}
}

// barrier is an event no actor of hollywood or the adapter broadcasts.
type barrier struct{}

// drainEvents returns once the event stream of e has handled every event broadcast before the
// call. The event stream is an actor that handles its events in the order they arrive and logs
// each before it hands it on, so a subscriber that has received a barrier broadcast now has seen
// every earlier event logged.
func drainEvents(t *testing.T, e *actor.Engine) {
	t.Helper()
	got := make(chan struct{}, 1)
	sub := e.SpawnFunc(func(c *actor.Context) {
		if _, ok := c.Message().(barrier); ok {
			got <- struct{}{}
		}
	}, "barrier")
	e.Subscribe(sub)
	e.BroadcastEvent(barrier{})
	select {
	case <-got:
	case <-time.After(closeWait):
		t.Fatal("the event stream did not hand on a barrier event")
	}
}

// runAll runs every workload once on s with the sizes of BEN-130, and spawns a second ping-pong
// workload on the same system, so the actors of both need distinct IDs.
func runAll(t *testing.T, s *system) {
	t.Helper()
	run := func(w harness.Workload, err error, n int) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		delivered, err := w.Run(n)
		if err != nil {
			t.Fatal(err)
		}
		if delivered != n {
			t.Fatalf("delivered %d of %d", delivered, n)
		}
	}
	w, err := s.PingPong(2)
	run(w, err, 1001)
	w, err = s.PingPong(3)
	run(w, err, 1001)
	w, err = s.FanOut(3)
	run(w, err, 1537)
	s.PrepareIdle(100)
	if err := s.SpawnIdle(); err != nil {
		t.Fatal(err)
	}
	rec := harness.NewRecorder(time.Now(), 200)
	e, err := s.Echo(rec)
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if err := e.Send(); err != nil {
			t.Fatal(err)
		}
		if err := e.Ask(); err != nil {
			t.Fatal(err)
		}
	}
	if got := rec.Len(); got != 100 {
		t.Fatalf("the echo recorder holds %d samples, want 100", got)
	}
}

// infoLog is a slog handler that accepts every level and keeps the records of level Info and above.
type infoLog struct {
	mu     sync.Mutex
	logged []string
}

func (h *infoLog) Enabled(context.Context, slog.Level) bool { return true }

func (h *infoLog) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelInfo {
		h.mu.Lock()
		h.logged = append(h.logged, r.Level.String()+" "+r.Message)
		h.mu.Unlock()
	}
	return nil
}

func (h *infoLog) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *infoLog) WithGroup(string) slog.Handler      { return h }

func (h *infoLog) records() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.logged...)
}

// captureLog makes h the handler of slog's default logger until the test ends, and then restores
// the logger and the log package. SetDefault also points the log package at the handler, and
// setting the previous logger back does not undo that when it is slog's own default.
func captureLog(t *testing.T, h slog.Handler) {
	t.Helper()
	prev, prevWriter, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	})
}

// TestCaptureLogRestores checks that captureLog leaves the log package as it found it.
func TestCaptureLogRestores(t *testing.T) {
	writer, flags := log.Writer(), log.Flags()
	t.Run("capture", func(t *testing.T) { captureLog(t, &infoLog{}) })
	if got := log.Writer(); got != writer {
		t.Errorf("log writes to %v after the capture, want %v", got, writer)
	}
	if got := log.Flags(); got != flags {
		t.Errorf("log flags are %d after the capture, want %d", got, flags)
	}
}

// TestNoLogAtInfo checks BEN-030's claim that hollywood logs nothing at Info or above in these
// workloads: the go command merges a test binary's standard error into its standard output, so a
// log line would land inside a benchmark's result line. hollywood logs through slog's default
// logger, so the test replaces it with a handler that enables every level. It checks every event
// the engine broadcast until Close returned: it waits for the event stream, which handles events
// after the actors that broadcast them have moved on, to log them all. Output that hollywood
// writes outside slog, or after the test, is not covered.
func TestNoLogAtInfo(t *testing.T) {
	h := &infoLog{}
	captureLog(t, h)

	slog.Warn("probe")
	if got := h.records(); len(got) != 1 || got[0] != "WARN probe" {
		t.Fatalf("the handler recorded %q for a probe record, want [WARN probe]", got)
	}

	s := start(t)
	runAll(t, s)
	if err := closeWithin(s, closeWait); err != nil {
		t.Fatalf("Close: %v", err)
	}
	drainEvents(t, s.engine)
	if got := h.records(); len(got) != 1 {
		t.Errorf("hollywood logged %d records at Info or above, the first %q", len(got)-1, got[1])
	}
}

// TestCloseStopsEveryActor checks that Close stops every actor the system spawned, whichever
// workload spawned it: before Close each is registered with the engine, after Close none is. It
// also checks that no two actors share an ID, since the engine does not spawn an actor whose ID is
// taken.
func TestCloseStopsEveryActor(t *testing.T) {
	s := start(t)
	runAll(t, s)

	// 2 and 3 pairs of ping-pong actors, a producer and 3 consumers, 100 idle actors and the echo
	// actor.
	if want := 2*2 + 3*2 + 1 + 3 + 100 + 1; len(s.pids) != want {
		t.Fatalf("the system spawned %d actors, want %d", len(s.pids), want)
	}
	seen := make(map[string]bool)
	for _, pid := range s.pids {
		if seen[pid.ID] {
			t.Errorf("two actors have the ID %s", pid.ID)
		}
		seen[pid.ID] = true
		if !registered(s, pid.ID) {
			t.Errorf("actor %s is not registered before Close", pid.ID)
		}
	}
	if err := closeWithin(s, closeWait); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, pid := range s.pids {
		if registered(s, pid.ID) {
			t.Errorf("actor %s is still registered after Close", pid.ID)
		}
	}
}

// registered reports whether the engine of s has an actor with the PID ID id, which has the form
// kind/id.
func registered(s *system, id string) bool {
	kind, name, ok := strings.Cut(id, "/")
	return ok && s.engine.Registry.GetPID(kind, name) != nil
}

// TestSpawnIdleBeforePrepareIdle checks that SpawnIdle without PrepareIdle returns an error.
func TestSpawnIdleBeforePrepareIdle(t *testing.T) {
	s := start(t)
	if err := s.SpawnIdle(); err == nil {
		t.Error("SpawnIdle before PrepareIdle returned nil")
	}
}

// TestFanOutNeedsConsumers checks that FanOut with no consumer returns an error: hollywood's
// inbox of size 4*0 divides by zero on its first message.
func TestFanOutNeedsConsumers(t *testing.T) {
	s := start(t)
	if _, err := s.FanOut(0); err == nil {
		t.Error("FanOut(0) returned nil")
	}
}
