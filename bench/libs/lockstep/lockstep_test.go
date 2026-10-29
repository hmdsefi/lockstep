// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep"
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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = s.sys.Shutdown(ctx)
	})
	return s
}

// runAll runs every workload once on s with the sizes of BEN-130.
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
	w, err = s.FanOut(3)
	run(w, err, 1537)
	rec := harness.NewRecorder(time.Now(), 200)
	w, err = s.AskPipe(rec)
	run(w, err, 100)
	s.PrepareIdle(1000)
	if err := s.SpawnIdle(); err != nil {
		t.Fatal(err)
	}
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
}

// TestATBEN13LeakCaught covers AT-BEN-13: when Close skips Shutdown, goleak finds the system's
// goroutines, so TestLockstepNoLeak would fail; when Close shuts the system down, it finds none.
func TestATBEN13LeakCaught(t *testing.T) {
	t.Run("Shutdown skipped", func(t *testing.T) {
		before := goleak.IgnoreCurrent()
		s := start(t)
		s.skipShutdown = true
		runAll(t, s)
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		err := goleak.Find(before)
		if err == nil {
			t.Fatal("goleak.Find found no goroutines after a Close that skipped Shutdown")
		}
		// The leak includes the workers of lockstep's production engine.
		if !strings.Contains(err.Error(), "github.com/hmdsefi/lockstep/internal/prodengine.") {
			t.Errorf("goleak.Find reports no worker of the production engine:\n%v", err)
		}
		// Shut the system down here, so its goroutines do not outlive the test.
		if err := s.sys.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if err := goleak.Find(before); err != nil {
			t.Errorf("goleak.Find after Shutdown: %v", err)
		}
	})
	t.Run("Shutdown", func(t *testing.T) {
		before := goleak.IgnoreCurrent()
		s := start(t)
		runAll(t, s)
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := goleak.Find(before); err != nil {
			t.Errorf("goleak.Find after Close: %v", err)
		}
	})
}

// TestPrepareIdleActorCap checks BEN-030's actor cap: PrepareIdle keeps the started system, built
// with lockstep's default cap, for up to 900,000 actors, and above that replaces it with a system
// whose cap is 2,000,000, shutting the started one down. It spawns no actor of that size.
func TestPrepareIdleActorCap(t *testing.T) {
	t.Run("n=900000", func(t *testing.T) {
		s := start(t)
		started := s.sys
		s.PrepareIdle(900_000)
		if s.sys != started {
			t.Error("PrepareIdle(900000) replaced the system")
		}
		if s.maxActors != 0 {
			t.Errorf("maxActors = %d, want 0 (lockstep's default)", s.maxActors)
		}
	})
	t.Run("n=900001", func(t *testing.T) {
		s := start(t)
		started := s.sys
		s.PrepareIdle(900_001)
		if s.sys == started {
			t.Fatal("PrepareIdle(900001) kept the started system")
		}
		if s.maxActors != 2_000_000 {
			t.Errorf("maxActors = %d, want 2000000", s.maxActors)
		}
		_, err := lockstep.Spawn(started, "", func() lockstep.Actor[harness.Hello] { return &idler{} })
		if !errors.Is(err, lockstep.ErrStopped) {
			t.Errorf("Spawn on the replaced system: %v, want ErrStopped", err)
		}
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}
