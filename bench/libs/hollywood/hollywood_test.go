// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package hollywood

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/bench/harness"
)

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
	t.Cleanup(func() { _ = s.Close() })
	return s
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

// TestNoLogAtInfo checks BEN-030's claim that hollywood logs nothing at Info or above in these
// workloads: the go command merges a test binary's standard error into its standard output, so a
// log line would land inside a benchmark's result line. hollywood logs through slog's default
// logger, so the test replaces it with a handler that enables every level.
func TestNoLogAtInfo(t *testing.T) {
	h := &infoLog{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })

	slog.Warn("probe")
	if got := h.records(); len(got) != 1 || got[0] != "WARN probe" {
		t.Fatalf("the handler recorded %q for a probe record, want [WARN probe]", got)
	}

	s := start(t)
	runAll(t, s)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
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
	if err := s.Close(); err != nil {
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
