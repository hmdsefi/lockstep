// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bench

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep/bench/harness"
	"github.com/hmdsefi/lockstep/bench/libs"
)

// TestLockstepNoLeak runs every workload on the lockstep adapter, closing each system, and fails
// if a goroutine started meanwhile is still running (BEN-210). Goroutines that already run when
// it starts, such as those another library left behind, are not checked. It is declared before
// TestWorkloads, so in the default order no other library has run in this binary yet.
func TestLockstepNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	runWorkloads(t, findLib(t, "lockstep"))
}

// TestWorkloads runs every workload on every library with small sizes and checks what the
// adapters report: the messages delivered, the samples recorded and the error of Close (BEN-130,
// AT-BEN-03).
func TestWorkloads(t *testing.T) {
	for _, lib := range libs.Libs() {
		t.Run("lib="+lib.Name(), func(t *testing.T) {
			runWorkloads(t, lib)
		})
	}
}

// askPiper is a system that runs the ask-with-pipe workload of BEN-090, which only lockstep has.
type askPiper interface {
	AskPipe(rec *harness.Recorder) (harness.Workload, error)
}

// workloads are BEN-130's workloads and sizes, plus two runs with fewer messages than actors. The
// sizes exercise the edges: two pairs share an odd n unevenly, so one pair's quota ends on a Ping
// that gets no Pong; with 3 consumers and n = 1,537 one consumer gets 513 items, more than its 512
// credits, with a last ack window of one item; and with n = 2, one pair or consumer gets nothing,
// as most do in a benchmark run with b.N = 1, and Run must still return.
var workloads = []struct {
	name string
	only string // the one library that runs the workload, or "" for every library
	run  func(t *testing.T, lib string, sys harness.System)
}{
	{name: "PingPong/pairs=1", run: pingPong(1, 1001)},
	{name: "PingPong/pairs=2", run: pingPong(2, 1001)},
	{name: "PingPong/pairs=3/n=2", run: pingPong(3, 2)},
	{name: "FanOut/consumers=3", run: fanOut(3, 1537)},
	{name: "FanOut/consumers=3/n=2", run: fanOut(3, 2)},
	{name: "IdleActors/n=1000", run: idleActors(1000)},
	{name: "SendLatency", run: sendLatency(100)},
	{name: "AskLatency/callers=1", run: askLatency(1, 100)},
	{name: "AskLatency/callers=4", run: askLatency(4, 100)},
	{name: "AskPipe", only: "lockstep", run: askPipe(100)},
}

// findLib returns the library of Libs named name.
func findLib(t *testing.T, name string) harness.Lib {
	t.Helper()
	for _, lib := range libs.Libs() {
		if lib.Name() == name {
			return lib
		}
	}
	t.Fatalf("libs.Libs() has no library %q", name)
	return nil
}

// runWorkloads runs each workload of lib as a subtest, on a system of its own, and checks that
// Close returns nil. A subtest that fails early still closes its system.
func runWorkloads(t *testing.T, lib harness.Lib) {
	for _, w := range workloads {
		if w.only != "" && w.only != lib.Name() {
			continue
		}
		t.Run(w.name, func(t *testing.T) {
			sys, err := lib.Start()
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = sys.Close()
				}
			})
			w.run(t, lib.Name(), sys)
			closed = true
			if err := sys.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
}

func pingPong(pairs, n int) func(*testing.T, string, harness.System) {
	return func(t *testing.T, lib string, sys harness.System) {
		w, err := sys.PingPong(pairs)
		if err != nil {
			t.Fatal(err)
		}
		checkRun(t, lib, w, n)
	}
}

func fanOut(consumers, n int) func(*testing.T, string, harness.System) {
	return func(t *testing.T, lib string, sys harness.System) {
		w, err := sys.FanOut(consumers)
		if err != nil {
			t.Fatal(err)
		}
		checkRun(t, lib, w, n)
	}
}

func idleActors(n int) func(*testing.T, string, harness.System) {
	return func(t *testing.T, _ string, sys harness.System) {
		sys.PrepareIdle(n)
		if err := sys.SpawnIdle(); err != nil {
			t.Fatal(err)
		}
	}
}

// sendLatency sends n stamps; the echo actor records one sample per stamp.
func sendLatency(n int) func(*testing.T, string, harness.System) {
	return func(t *testing.T, _ string, sys harness.System) {
		rec := harness.NewRecorder(time.Now(), n)
		e, err := sys.Echo(rec)
		if err != nil {
			t.Fatal(err)
		}
		for range n {
			if err := e.Send(); err != nil {
				t.Fatal(err)
			}
		}
		checkSamples(t, "echo recorder", rec, n)
	}
}

// askLatency makes n asks from callers goroutines, each timing its asks with a recorder of its
// own, as BenchmarkAskLatency does; the echo actor records nothing for an ask.
func askLatency(callers, n int) func(*testing.T, string, harness.System) {
	return func(t *testing.T, _ string, sys harness.System) {
		base := time.Now()
		echoRec := harness.NewRecorder(base, 0)
		e, err := sys.Echo(echoRec)
		if err != nil {
			t.Fatal(err)
		}
		recs := make([]*harness.Recorder, callers)
		var wg sync.WaitGroup
		for i := range callers {
			rec := harness.NewRecorder(base, n)
			recs[i] = rec
			asks := n / callers
			if i < n%callers {
				asks++
			}
			wg.Go(func() {
				for range asks {
					start := rec.Now()
					if err := e.Ask(); err != nil {
						t.Errorf("Ask: %v", err)
						return
					}
					rec.Add(rec.Now() - start)
				}
			})
		}
		wg.Wait()
		all := harness.NewRecorder(base, n)
		for _, rec := range recs {
			all.Merge(rec)
		}
		checkSamples(t, "caller recorders", all, n)
		checkSamples(t, "echo recorder", echoRec, 0)
	}
}

// askPipe runs n round trips of BEN-090; the asking actor records one sample per round trip.
func askPipe(n int) func(*testing.T, string, harness.System) {
	return func(t *testing.T, lib string, sys harness.System) {
		p, ok := sys.(askPiper)
		if !ok {
			t.Fatalf("%s: %T has no AskPipe method", lib, sys)
		}
		rec := harness.NewRecorder(time.Now(), n)
		w, err := p.AskPipe(rec)
		if err != nil {
			t.Fatal(err)
		}
		checkRun(t, lib, w, n)
		checkSamples(t, "round-trip recorder", rec, n)
	}
}

// checkRun runs n messages on w and checks that all n were delivered.
func checkRun(t *testing.T, lib string, w harness.Workload, n int) {
	t.Helper()
	delivered, err := w.Run(n)
	if err != nil {
		t.Fatal(err)
	}
	if delivered != n {
		t.Errorf("%s: delivered %d of %d", lib, delivered, n)
	}
}

// checkSamples checks that rec holds exactly want samples.
func checkSamples(t *testing.T, name string, rec *harness.Recorder, want int) {
	t.Helper()
	if got := rec.Len(); got != want {
		t.Errorf("%s holds %d samples, want %d", name, got, want)
	}
}
