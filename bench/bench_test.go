// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bench

import (
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/bench/harness"
	"github.com/hmdsefi/lockstep/bench/libs"
)

// The benchmarks of BEN-040 to BEN-110. A workload's benchmark has one sub-benchmark per library,
// lib=<name> in the order of libs.Libs, and inside it one per parameter value, so
// -bench 'PingPong/lib=lockstep' selects one library. The parameter value procs stands for
// runtime.GOMAXPROCS(0), so the names do not depend on the machine.
//
// Every benchmark reports its allocations and fails unless the actors deliver every message it
// sends. It starts its system and spawns its actors before it resets the timer, and stops the
// timer before it closes the system; BenchmarkIdleActors, which times the spawn, starts a system in
// each iteration with the timer stopped. The custom units are BEN-120's.

// largeEnv names the variable that, set to 1, adds n=1000000 to BenchmarkIdleActors. CI does not set
// it, and TestATBEN05Names removes it from its child's environment: at that size some libraries
// need tens of gigabytes of memory, and a full run takes hours.
const largeEnv = "LOCKSTEP_BENCH_LARGE"

// BenchmarkPingPong measures messages between the two actors of each of pairs pairs (BEN-040). One
// op is one message, a Ping or a Pong; msgs/s is the messages delivered per second.
func BenchmarkPingPong(b *testing.B) {
	eachLib(b, func(b *testing.B, lib harness.Lib) {
		eachParam(b, "pairs", oneAndProcs(), func(b *testing.B, pairs int) {
			benchMessages(b, lib, func(sys harness.System) (harness.Workload, error) {
				return sys.PingPong(pairs)
			})
		})
	})
}

// BenchmarkFanOut measures one producer sending items to consumers consumers with credit flow
// control (BEN-050). One op is one item; the consumers' acks are not counted. msgs/s is the items
// delivered per second.
func BenchmarkFanOut(b *testing.B) {
	eachLib(b, func(b *testing.B, lib harness.Lib) {
		eachParam(b, "consumers", values(16, 1024), func(b *testing.B, consumers int) {
			benchMessages(b, lib, func(sys harness.System) (harness.Workload, error) {
				return sys.FanOut(consumers)
			})
		})
	})
}

// BenchmarkIdleActors measures spawning n actors that each handle one Hello, and what they hold
// once idle (BEN-060). One op spawns the n actors.
func BenchmarkIdleActors(b *testing.B) {
	sizes := []int{10_000, 100_000}
	if os.Getenv(largeEnv) == "1" {
		sizes = append(sizes, 1_000_000)
	}
	eachLib(b, func(b *testing.B, lib harness.Lib) {
		eachParam(b, "n", values(sizes...), func(b *testing.B, n int) {
			benchIdle(b, lib, n)
		})
	})
}

// BenchmarkSendLatency measures how long a message sent from outside the system takes to reach an
// otherwise idle echo actor (BEN-070). One op is one send, after which the sender waits for the
// echo actor to record the latency.
func BenchmarkSendLatency(b *testing.B) {
	eachLib(b, benchSendLatency)
}

// BenchmarkAskLatency measures the round trip of an ask from outside the system to the echo actor
// (BEN-080), from one caller and from runtime.GOMAXPROCS(0) callers at once. One op is one ask.
func BenchmarkAskLatency(b *testing.B) {
	eachLib(b, func(b *testing.B, lib harness.Lib) {
		b.Run("callers=1", func(b *testing.B) { benchAskLatency(b, lib) })
		b.Run("callers=procs", func(b *testing.B) { benchAskLatencyParallel(b, lib) })
	})
}

// BenchmarkAskPipe measures the round trips of lockstep's ask with pipe (BEN-090): an actor asks
// another, pipes the answer into its own mailbox, and asks again on each answer, timing each round
// trip. One op is one round trip. Only lockstep runs it; the other libraries have no equivalent in
// their non-blocking APIs.
func BenchmarkAskPipe(b *testing.B) {
	lib := findLib(b, "lockstep")
	b.Run("lib="+lib.Name(), func(b *testing.B) {
		b.ReportAllocs()
		withSystem(b, lib, func(sys harness.System) {
			rec := harness.NewRecorder(time.Now(), b.N)
			w, err := asAskPiper(b, lib.Name(), sys).AskPipe(rec)
			if err != nil {
				b.Fatal(err)
			}
			runTimed(b, lib, w)
			reportLatency(b, lib, rec)
		})
	})
}

// BenchmarkCalibrate measures copying 65,536 fixed ints into a scratch slice and sorting them
// (BEN-110). It runs no actor library: the time budgets of the lockstep-only results are ratios to
// it, so a slower or faster runner moves both sides of the ratio.
func BenchmarkCalibrate(b *testing.B) {
	b.ReportAllocs()
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // fixed benchmark input
	input := make([]int, 65_536)
	for i := range input {
		input[i] = rng.Int()
	}
	scratch := make([]int, len(input))
	b.ResetTimer()
	for range b.N {
		copy(scratch, input)
		slices.Sort(scratch)
	}
}

// param is one value of a benchmark parameter: the value as the benchmark's name shows it, and the
// value the workload uses.
type param struct {
	name  string
	value int
}

// values returns the parameter values vs, each named by its decimal form.
func values(vs ...int) []param {
	ps := make([]param, len(vs))
	for i, v := range vs {
		ps[i] = param{name: strconv.Itoa(v), value: v}
	}
	return ps
}

// oneAndProcs returns the parameter values 1 and procs, runtime.GOMAXPROCS(0).
func oneAndProcs() []param {
	return []param{{name: "1", value: 1}, {name: "procs", value: runtime.GOMAXPROCS(0)}}
}

// eachLib runs bench in one sub-benchmark lib=<name> per library, in the order of libs.Libs.
func eachLib(b *testing.B, bench func(b *testing.B, lib harness.Lib)) {
	for _, lib := range libs.Libs() {
		b.Run("lib="+lib.Name(), func(b *testing.B) { bench(b, lib) })
	}
}

// eachParam runs bench in one sub-benchmark <key>=<name> per parameter value of ps.
func eachParam(b *testing.B, key string, ps []param, bench func(b *testing.B, value int)) {
	for _, p := range ps {
		b.Run(key+"="+p.name, func(b *testing.B) { bench(b, p.value) })
	}
}

// withSystem starts a system of lib and runs bench on it, then stops the timer and closes the
// system. If bench fails the benchmark, the system is closed all the same.
func withSystem(b *testing.B, lib harness.Lib, bench func(sys harness.System)) {
	sys, err := lib.Start()
	if err != nil {
		b.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = sys.Close() // the benchmark has failed already, with the error that matters
		}
	}()
	bench(sys)
	b.StopTimer()
	closed = true
	if err := sys.Close(); err != nil {
		b.Fatal(err)
	}
}

// benchMessages runs b.N messages of the workload spawn spawns on a system of lib, in one Run, and
// reports msgs/s, the messages delivered per second.
func benchMessages(b *testing.B, lib harness.Lib, spawn func(harness.System) (harness.Workload, error)) {
	b.ReportAllocs()
	withSystem(b, lib, func(sys harness.System) {
		w, err := spawn(sys)
		if err != nil {
			b.Fatal(err)
		}
		runTimed(b, lib, w)
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})
}

// runTimed resets the timer, runs b.N messages of w, a workload of lib, and stops the timer. It
// fails b unless the actors delivered all b.N.
func runTimed(b *testing.B, lib harness.Lib, w harness.Workload) {
	b.Helper()
	b.ResetTimer()
	delivered, err := w.Run(b.N)
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	checkDelivered(b, lib, delivered, b.N)
}

// checkDelivered fails b unless the actors of lib delivered all n messages.
func checkDelivered(b *testing.B, lib harness.Lib, delivered, n int) {
	b.Helper()
	if delivered != n {
		b.Fatalf("%s: delivered %d of %d", lib.Name(), delivered, n)
	}
}

// benchIdle measures n idle actors of lib (BEN-060). Each iteration starts a system and spawns
// the actors with the timer running; with the timer stopped, it measures the heap and goroutines
// the actors hold, and closes the system. It reports, averaged over the iterations, the bytes of
// heap per actor (an integer per iteration), the goroutines per actor, and the spawn time per
// actor.
func benchIdle(b *testing.B, lib harness.Lib, n int) {
	b.ReportAllocs()
	b.StopTimer()
	b.ResetTimer()
	var heapPerActor int64         // summed over the iterations
	var goroutinesPerActor float64 // summed over the iterations
	for range b.N {
		withSystem(b, lib, func(sys harness.System) {
			sys.PrepareIdle(n)
			h0, g0 := settle()
			b.StartTimer()
			err := sys.SpawnIdle()
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			h1, g1 := settle()
			// The adapter holds the actors' PIDs, so keeping it alive until here keeps them alive
			// through both measurements.
			runtime.KeepAlive(sys)
			heapPerActor += (h1 - h0) / int64(n)
			goroutinesPerActor += float64(g1-g0) / float64(n)
		})
	}
	iterations := float64(b.N)
	b.ReportMetric(float64(heapPerActor)/iterations, "B/actor")
	b.ReportMetric(goroutinesPerActor/iterations, "goroutines/actor")
	b.ReportMetric(float64(b.Elapsed())/iterations/float64(n), "ns/actor")
}

// settle collects garbage twice, so that the heap holds only what is reachable, and returns the
// bytes allocated on the heap and the number of goroutines.
func settle() (heap int64, goroutines int) {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc), runtime.NumGoroutine() //nolint:gosec // a heap size is far below math.MaxInt64
}

// benchSendLatency sends b.N stamps to an echo actor of lib from one goroutine, each after the
// echo actor recorded the latency of the one before, and reports the median and 99th percentile
// latency (BEN-070).
func benchSendLatency(b *testing.B, lib harness.Lib) {
	b.ReportAllocs()
	withSystem(b, lib, func(sys harness.System) {
		rec := harness.NewRecorder(time.Now(), b.N)
		e, err := sys.Echo(rec)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for range b.N {
			if err := e.Send(); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportLatency(b, lib, rec) // the echo actor records one sample per stamp it receives
	})
}

// benchAskLatency makes b.N asks to an echo actor of lib from one goroutine, timing each, and
// reports the median and 99th percentile round trip (BEN-080).
func benchAskLatency(b *testing.B, lib harness.Lib) {
	b.ReportAllocs()
	withSystem(b, lib, func(sys harness.System) {
		base := time.Now()
		e, err := sys.Echo(harness.NewRecorder(base, 0)) // an ask records nothing in the echo actor
		if err != nil {
			b.Fatal(err)
		}
		rec := harness.NewRecorder(base, b.N)
		b.ResetTimer()
		for range b.N {
			start := rec.Now()
			if err := e.Ask(); err != nil {
				b.Fatal(err)
			}
			rec.Add(rec.Now() - start)
		}
		b.StopTimer()
		reportLatency(b, lib, rec)
	})
}

// benchAskLatencyParallel makes b.N asks in all to an echo actor of lib from
// runtime.GOMAXPROCS(0) goroutines at once (BEN-080). Each goroutine times its asks with a
// recorder of its own; the samples are merged after the timer stops.
func benchAskLatencyParallel(b *testing.B, lib harness.Lib) {
	b.ReportAllocs()
	withSystem(b, lib, func(sys harness.System) {
		base := time.Now()
		e, err := sys.Echo(harness.NewRecorder(base, 0))
		if err != nil {
			b.Fatal(err)
		}
		// With parallelism 1, RunParallel runs exactly GOMAXPROCS goroutines. Any one of them may
		// make all b.N asks, so each recorder has room for b.N samples.
		b.SetParallelism(1)
		recs := make([]*harness.Recorder, runtime.GOMAXPROCS(0))
		for i := range recs {
			recs[i] = harness.NewRecorder(base, b.N)
		}
		var next atomic.Int64 // the index of the next goroutine's recorder
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rec := recs[next.Add(1)-1]
			for pb.Next() {
				start := rec.Now()
				if err := e.Ask(); err != nil {
					b.Error(err) // reportLatency then fails the benchmark: this ask has no sample
					return
				}
				rec.Add(rec.Now() - start)
			}
		})
		b.StopTimer()
		all := harness.NewRecorder(base, b.N)
		for _, rec := range recs {
			all.Merge(rec)
		}
		reportLatency(b, lib, all)
	})
}

// reportLatency fails b unless rec holds b.N samples, one per message delivered, and reports their
// median and 99th percentile as p50-ns and p99-ns.
func reportLatency(b *testing.B, lib harness.Lib, rec *harness.Recorder) {
	b.Helper()
	checkDelivered(b, lib, rec.Len(), b.N)
	b.ReportMetric(float64(rec.Percentile(50)), "p50-ns")
	b.ReportMetric(float64(rec.Percentile(99)), "p99-ns")
}
