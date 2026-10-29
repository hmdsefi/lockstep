// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bench

import (
	"cmp"
	"maps"
	"runtime"
	"slices"
	"strings"
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
// AT-BEN-03). It also checks what each closed system leaves running (checkLeftovers).
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

// asAskPiper returns sys, a system of lib, as an askPiper, and fails tb if it has no AskPipe
// method.
func asAskPiper(tb testing.TB, lib string, sys harness.System) askPiper {
	tb.Helper()
	p, ok := sys.(askPiper)
	if !ok {
		tb.Fatalf("%s: %T has no AskPipe method", lib, sys)
	}
	return p
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
func findLib(tb testing.TB, name string) harness.Lib {
	tb.Helper()
	for _, lib := range libs.Libs() {
		if lib.Name() == name {
			return lib
		}
	}
	tb.Fatalf("libs.Libs() has no library %q", name)
	return nil
}

// runWorkloads runs each workload of lib as a subtest, on a system of its own, and checks that
// Close returns nil and leaves nothing running that it should have stopped (checkLeftovers). A
// subtest that fails early still closes its system.
func runWorkloads(t *testing.T, lib harness.Lib) {
	for _, w := range workloads {
		if w.only != "" && w.only != lib.Name() {
			continue
		}
		t.Run(w.name, func(t *testing.T) {
			before := goroutineStacks()
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
				t.Fatalf("Close: %v", err)
			}
			checkLeftovers(t, lib.Name(), before)
		})
	}
}

// goaktDrain is the function of the goroutine GoAkt v4.6.3 starts in Start and leaves running
// after Stop unless remoting is enabled, blocked on a channel that Stop does not close.
const goaktDrain = "github.com/tochemey/goakt/v4/actor.(*actorSystem).drainCoalescedFailures("

// checkLeftovers fails t if goroutines that were not running when before was taken still run once
// the system of lib is closed, except that a GoAkt system may leave one goroutine blocked in
// goaktDrain. It retries for a while first, since a closed system's goroutines may still be
// exiting.
//
// GoAkt's goroutine keeps the stopped system reachable, so every later benchmark in the same test
// binary would run with that system's heap; the bench job runs GoAkt's benchmarks in a process of
// their own for that reason (BEN-030, BEN-220). A library version that leaves anything else
// running fails here instead of silently changing what later benchmarks measure.
func checkLeftovers(t *testing.T, lib string, before map[string]string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for wait := time.Millisecond; ; wait = min(2*wait, 100*time.Millisecond) {
		left := leftovers(lib, before, goroutineStacks())
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("%s: Close left %d goroutines running:\n\n%s", lib, len(left), strings.Join(left, "\n\n"))
			return
		}
		time.Sleep(wait)
	}
}

// leftovers returns the stacks of the goroutines of after that before does not hold, in goroutine
// ID order, without the one goroutine a system of lib may leave: for GoAkt, one blocked in a
// channel receive in goaktDrain.
func leftovers(lib string, before, after map[string]string) []string {
	var left []string
	allowed := lib == "goakt"
	for _, id := range slices.SortedFunc(maps.Keys(after), compareIDs) {
		if _, ok := before[id]; ok {
			continue
		}
		stack := after[id]
		if allowed && strings.HasPrefix(stack, "goroutine "+id+" [chan receive") && strings.Contains(stack, "\n"+goaktDrain) {
			allowed = false
			continue
		}
		left = append(left, stack)
	}
	return left
}

// TestLeftovers checks which new goroutines leftovers reports: any for lockstep, protoactor and
// hollywood, and for GoAkt all but one blocked in a channel receive in goaktDrain.
func TestLeftovers(t *testing.T) {
	stack := func(id, state, fn string) string {
		return "goroutine " + id + " [" + state + "]:\n" + fn + "0xc000123456)\n\t/x/y.go:1 +0x1"
	}
	const other = "github.com/example/lib.(*system).loop("
	var (
		old     = stack("1", "running", "testing.tRunner(")
		drain   = stack("12", "chan receive", goaktDrain)
		drain2  = stack("13", "chan receive", goaktDrain)
		running = stack("14", "runnable", goaktDrain)
		loop    = stack("100", "select", other)
	)
	before := map[string]string{"1": old}
	for _, c := range []struct {
		lib   string
		after []string
		want  []string
	}{
		{"lockstep", []string{old}, nil},
		{"lockstep", []string{old, loop}, []string{loop}},
		{"lockstep", []string{old, drain}, []string{drain}},
		{"goakt", []string{old}, nil},
		{"goakt", []string{old, drain}, nil},
		{"goakt", []string{old, loop, drain2, drain}, []string{drain2, loop}},
		{"goakt", []string{old, running}, []string{running}},
		{"protoactor", []string{old, drain}, []string{drain}},
	} {
		after := map[string]string{}
		for _, g := range c.after {
			after[strings.Fields(g)[1]] = g
		}
		if got := leftovers(c.lib, before, after); !slices.Equal(got, c.want) {
			t.Errorf("leftovers(%s, %q) = %q, want %q", c.lib, c.after, got, c.want)
		}
	}
}

// compareIDs orders goroutine IDs numerically.
func compareIDs(a, b string) int {
	return cmp.Or(cmp.Compare(len(a), len(b)), strings.Compare(a, b))
}

// goroutineStacks returns the stack of every goroutine, keyed by goroutine ID. Each stack starts
// with a line such as "goroutine 7 [chan receive]:".
func goroutineStacks() map[string]string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	stacks := map[string]string{}
	for _, stack := range strings.Split(string(buf), "\n\n") {
		fields := strings.Fields(stack)
		if len(fields) >= 2 && fields[0] == "goroutine" {
			stacks[fields[1]] = stack
		}
	}
	return stacks
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
		rec := harness.NewRecorder(time.Now(), n)
		w, err := asAskPiper(t, lib, sys).AskPipe(rec)
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
