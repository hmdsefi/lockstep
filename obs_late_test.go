// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// The late series AT-OBS-17 registers, with their help texts (OBS-150's table).
const (
	framesSent     = "lockstep_remote_frames_sent_total"
	framesSentHelp = "Remoting frames sent."
	sessions       = "lockstep_remote_sessions"
	sessionsHelp   = "Open remoting sessions."
)

// panicText calls fn and returns the value it panicked with as text, failing the test if fn
// returns.
func panicText(t *testing.T, what string, fn func()) (text string) {
	t.Helper()
	defer func() {
		v := recover()
		if v == nil {
			t.Errorf("%s did not panic", what)
		}
		text = fmt.Sprint(v)
	}()
	fn()
	return ""
}

// checkPanic fails the test unless fn panics with want.
func checkPanic(t *testing.T, what, want string, fn func()) {
	t.Helper()
	if got := panicText(t, what, fn); got != want {
		t.Errorf("%s panicked with %q, want %q", what, got, want)
	}
}

// unusedGauge returns a gauge function that fails the test if anything calls it.
func unusedGauge(t *testing.T) func() int64 {
	return func() int64 {
		t.Error("lockstep called a gauge function it should not have registered")
		return 0
	}
}

// sameLabels reports whether s carries the labels kv, as name and value pairs in turn.
func sameLabels(s *recSeries, kv ...string) bool {
	var got []string
	for _, l := range s.labels {
		got = append(got, l.Name, l.Value)
	}
	return slices.Equal(got, kv)
}

// AT-OBS-17: OBS-080, OBS-150.
func TestATOBS17LateRegistration(t *testing.T) {
	if hooks.MetricCounter == nil || hooks.MetricGauge == nil {
		t.Fatal("package lockstep left hooks.MetricCounter or hooks.MetricGauge nil")
	}
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))
	atNew := len(rec.registered())

	add := hooks.MetricCounter(sys, framesSent, framesSentHelp)
	if add == nil {
		t.Fatal("MetricCounter returned nil on a system with metrics")
	}
	late := rec.registered()[atNew:]
	if len(late) != 1 {
		t.Fatalf("MetricCounter registered %d series, want 1", len(late))
	}
	if s := late[0]; s.gauge || s.name != framesSent || s.help != framesSentHelp || !sameLabels(s, "node", "n1") {
		t.Errorf("MetricCounter registered %s (gauge %v, help %q), want the counter %s{node=\"n1\"} with help %q",
			s, s.gauge, s.help, framesSent, framesSentHelp)
	}
	add(3)
	if got := rec.count(t, framesSent); got != 3 {
		t.Errorf("after add(3): %s = %d, want 3", framesSent, got)
	}
	again := hooks.MetricCounter(sys, framesSent, framesSentHelp)
	if got := len(rec.registered()); got != atNew+1 {
		t.Errorf("a second identical MetricCounter call registered %d more series, want none", got-atNew-1)
	}
	if again == nil {
		t.Fatal("a second identical MetricCounter call returned nil")
	}
	again(2)
	if got := rec.count(t, framesSent); got != 5 {
		t.Errorf("after add(3) and the second call's add(2): %s = %d, want 5 (the same counter)", framesSent, got)
	}

	hooks.MetricGauge(sys, sessions, sessionsHelp, func() int64 { return 2 })
	late = rec.registered()[atNew+1:]
	if len(late) != 1 {
		t.Fatalf("MetricGauge registered %d series, want 1", len(late))
	}
	if s := late[0]; !s.gauge || s.name != sessions || s.help != sessionsHelp || !sameLabels(s, "node", "n1") {
		t.Errorf("MetricGauge registered %s (gauge %v, help %q), want the gauge %s{node=\"n1\"} with help %q",
			s, s.gauge, s.help, sessions, sessionsHelp)
	}
	if got := rec.gauge(t, sessions); got != 2 {
		t.Errorf("%s = %d, want 2", sessions, got)
	}
	hooks.MetricGauge(sys, sessions, sessionsHelp, unusedGauge(t))
	if got := len(rec.registered()); got != atNew+2 {
		t.Errorf("a second identical MetricGauge call registered %d more series, want none", got-atNew-2)
	}

	checkPanic(t, "MetricCounter of lockstep_other_total",
		"lockstep: metrics: lockstep_other_total is not a late series",
		func() { hooks.MetricCounter(sys, "lockstep_other_total", "x") })
	checkPanic(t, "MetricCounter with the label list \"peer\"",
		"lockstep: metrics: "+framesSent+": odd label list",
		func() { hooks.MetricCounter(sys, framesSent, framesSentHelp, "peer") })
	checkPanic(t, "MetricGauge of lockstep_other",
		"lockstep: metrics: lockstep_other is not a late series",
		func() { hooks.MetricGauge(sys, "lockstep_other", "x", unusedGauge(t)) })
	checkPanic(t, "MetricGauge with the label list \"peer\"",
		"lockstep: metrics: "+sessions+": odd label list",
		func() { hooks.MetricGauge(sys, sessions, sessionsHelp, unusedGauge(t), "peer") })
	if got := len(rec.registered()); got != atNew+2 {
		t.Errorf("the calls that panicked registered %d series, want none", got-atNew-2)
	}

	off := newProdSystem(t)
	if add := hooks.MetricCounter(off, framesSent, framesSentHelp); add != nil {
		t.Error("MetricCounter on a system without metrics returned a function, want nil")
	}
	hooks.MetricGauge(off, sessions, sessionsHelp, unusedGauge(t))
	// The name and labels are checked first, whether the system has metrics or not.
	checkPanic(t, "MetricCounter of lockstep_other_total without metrics",
		"lockstep: metrics: lockstep_other_total is not a late series",
		func() { hooks.MetricCounter(off, "lockstep_other_total", "x") })
	checkPanic(t, "MetricGauge with the label list \"peer\" without metrics",
		"lockstep: metrics: "+sessions+": odd label list",
		func() { hooks.MetricGauge(off, sessions, sessionsHelp, unusedGauge(t), "peer") })

	shutdownNow(t, sys)
	if got := rec.gauge(t, sessions); got != 0 {
		t.Errorf("after Shutdown: %s = %d, want 0", sessions, got)
	}
	if add := hooks.MetricCounter(sys, framesSent, framesSentHelp); add != nil {
		t.Errorf("after Shutdown: MetricCounter of the registered %s returned a function, want nil", framesSent)
	}
	if add := hooks.MetricCounter(sys, "lockstep_remote_gaps_total", "Remoting sequence gaps detected."); add != nil {
		t.Error("after Shutdown: MetricCounter of a new series returned a function, want nil")
	}
	hooks.MetricGauge(sys, sessions, sessionsHelp, unusedGauge(t), "peer", "n3")
	if got := len(rec.registered()); got != atNew+2 {
		t.Errorf("after Shutdown: rec saw %d registrations, want none", got-atNew-2)
	}
}

// A late series carries node and then the given labels, in order; each label list is its own
// series, registered once (OBS-150).
func TestLateRegistrationLabels(t *testing.T) {
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))
	atNew := len(rec.registered())

	peer2 := hooks.MetricCounter(sys, framesSent, framesSentHelp, "peer", "n2", "link", "a")
	peer3 := hooks.MetricCounter(sys, framesSent, framesSentHelp, "peer", "n3", "link", "a")
	plain := hooks.MetricCounter(sys, framesSent, framesSentHelp)
	hooks.MetricGauge(sys, sessions, sessionsHelp, func() int64 { return 4 }, "peer", "n2")
	hooks.MetricGauge(sys, sessions, sessionsHelp, unusedGauge(t), "peer", "n2")
	hooks.MetricGauge(sys, sessions, sessionsHelp, func() int64 { return 5 }, "peer", "n3")
	if again := hooks.MetricCounter(sys, framesSent, framesSentHelp, "peer", "n2", "link", "a"); again == nil {
		t.Fatal("a second MetricCounter call for peer n2 returned nil")
	} else {
		again(1)
	}
	peer2(1)
	peer3(5)
	plain(7)

	late := rec.registered()[atNew:]
	want := []string{
		framesSent + `{node="n1",peer="n2",link="a"}`,
		framesSent + `{node="n1",peer="n3",link="a"}`,
		framesSent + `{node="n1"}`,
		sessions + `{node="n1",peer="n2"}`,
		sessions + `{node="n1",peer="n3"}`,
	}
	var got []string
	for _, s := range late {
		got = append(got, s.String())
	}
	if !slices.Equal(got, want) {
		t.Fatalf("late series:\n got %q\nwant %q", got, want)
	}
	for i, n := range []uint64{2, 5, 7} {
		if v := late[i].n.Load(); v != n {
			t.Errorf("%s = %d, want %d", late[i], v, n)
		}
	}
	for i, n := range []int64{4, 5} {
		if v := late[3+i].fn(); v != n {
			t.Errorf("%s = %d, want %d", late[3+i], v, n)
		}
	}
}

// A late series is registered with the kind its table row gives it.
func TestLateRegistrationKind(t *testing.T) {
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithMetrics(rec))
	atNew := len(rec.registered())
	checkPanic(t, "MetricCounter of a late gauge",
		"lockstep: metrics: "+sessions+" is a late gauge, not a counter",
		func() { hooks.MetricCounter(sys, sessions, sessionsHelp) })
	checkPanic(t, "MetricGauge of a late counter",
		"lockstep: metrics: "+framesSent+" is a late counter, not a gauge",
		func() { hooks.MetricGauge(sys, framesSent, framesSentHelp, unusedGauge(t)) })
	if got := len(rec.registered()); got != atNew {
		t.Errorf("the calls that panicked registered %d series, want none", got-atNew)
	}
}

// Both hooks panic for a sys that is not a *System, as the other hooks do.
func TestLateRegistrationSysType(t *testing.T) {
	checkPanic(t, "MetricCounter of an int",
		"lockstep: hooks.MetricCounter: sys has type int, want a non-nil *lockstep.System",
		func() { hooks.MetricCounter(3, framesSent, framesSentHelp) })
	checkPanic(t, "MetricGauge of a nil *System",
		"lockstep: hooks.MetricGauge: sys has type *lockstep.System, want a non-nil *lockstep.System",
		func() { hooks.MetricGauge((*lockstep.System)(nil), sessions, sessionsHelp, unusedGauge(t)) })
}

// A system built through hooks.NewSystem, as simulation builds its systems, has metrics off even
// with WithMetrics among its options, so neither hook calls a backend there (OBS-150, OBS-155).
func TestLateRegistrationEngineBuilt(t *testing.T) {
	rec := &recMetrics{}
	sys, _ := manualSystem(t, lockstep.WithMetrics(rec))
	if add := hooks.MetricCounter(sys, framesSent, framesSentHelp); add != nil {
		t.Error("MetricCounter on an engine-built system returned a function, want nil")
	}
	hooks.MetricGauge(sys, sessions, sessionsHelp, unusedGauge(t))
	if got := rec.registered(); len(got) != 0 {
		t.Errorf("rec saw %d registrations, want none", len(got))
	}
}

// From the moment Shutdown starts, while actors are still stopping, MetricCounter returns nil and
// MetricGauge does nothing; a gauge registered before reads 0 once Shutdown has returned
// (OBS-150).
func TestLateRegistrationDuringShutdown(t *testing.T) {
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithMetrics(rec), lockstep.WithLogger(slog.New(slog.DiscardHandler)))
	hooks.MetricGauge(sys, sessions, sessionsHelp, func() int64 { return 2 })
	held := spawnObs(t, sys, "held", obsOf(&obsActor{}))
	release := lockstep.Hold(sys, held.Address())
	defer release()
	registered := len(rec.registered())

	// The held actor cannot stop, so the shutdown starts and stays unfinished.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := sys.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown returned nil with an actor held")
	}
	if add := hooks.MetricCounter(sys, framesSent, framesSentHelp); add != nil {
		t.Error("MetricCounter after Shutdown started returned a function, want nil")
	}
	hooks.MetricGauge(sys, sessions, sessionsHelp, unusedGauge(t), "peer", "n2")
	if got := len(rec.registered()); got != registered {
		t.Errorf("after Shutdown started: rec saw %d registrations, want none", got-registered)
	}

	release()
	shutdownNow(t, sys)
	if got := rec.gauge(t, sessions); got != 0 {
		t.Errorf("after Shutdown: %s = %d, want 0", sessions, got)
	}
}

// Late registrations race Shutdown while the backend polls every gauge: once Shutdown has returned
// nil, no registration reaches the backend and every late gauge reads 0. Registration checks that
// Shutdown has not started under the lock that dropGauges takes, so no late gauge registered while
// the shutdown completes outlives it (OBS-150).
func TestLateRegistrationRacesShutdown(t *testing.T) {
	m := &pollingMetrics{stop: make(chan struct{})}
	t.Cleanup(func() { close(m.stop); m.wg.Wait() })
	sys := lockstep.New(lockstep.WithMetrics(m), lockstep.WithLogger(slog.New(slog.DiscardHandler)))
	rec := &m.recMetrics
	// The held actor keeps the shutdown from completing until release.
	h := spawnObs(t, sys, "held", obsOf(&obsActor{}))
	release := lockstep.Hold(sys, h.Address())

	var stopReg atomic.Bool
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := 0; !stopReg.Load(); j++ {
				peer := strconv.Itoa(i) + "-" + strconv.Itoa(j)
				hooks.MetricGauge(sys, sessions, sessionsHelp, func() int64 { return 1 }, "peer", peer)
				if add := hooks.MetricCounter(sys, framesSent, framesSentHelp, "peer", peer); add != nil {
					add(1)
				}
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		done <- sys.Shutdown(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	before := len(rec.registered())
	time.Sleep(10 * time.Millisecond)
	stopReg.Store(true)
	wg.Wait()
	if after := len(rec.registered()); after != before {
		t.Errorf("%d registrations after Shutdown returned", after-before)
	}
	late := 0
	for _, s := range rec.registered() {
		if s.gauge && s.name == sessions {
			late++
			if v := s.fn(); v != 0 {
				t.Errorf("%s = %d after Shutdown, want 0", s, v)
			}
		}
	}
	t.Logf("%d late gauges registered before Shutdown", late)
}

// gaugeSource is what a late gauge function reads.
type gaugeSource struct {
	n    int64
	name string
}

// After Shutdown, a late gauge function that the backend keeps no longer refers to the function it
// wraps, so what that function refers to can be collected (OBS-100, OBS-150).
func TestLateGaugeDropsFunction(t *testing.T) {
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithMetrics(rec))
	collected := make(chan struct{})
	func() {
		// Not a bare int64: the runtime may batch a small allocation without pointers with others,
		// and its cleanup then waits for them.
		state := &gaugeSource{n: 6, name: "sessions"}
		runtime.AddCleanup(state, func(struct{}) { close(collected) }, struct{}{})
		hooks.MetricGauge(sys, sessions, sessionsHelp, func() int64 { return state.n })
	}()
	if got := rec.gauge(t, sessions); got != 6 {
		t.Fatalf("%s = %d, want 6", sessions, got)
	}

	shutdownNow(t, sys)
	if got := rec.gauge(t, sessions); got != 0 {
		t.Errorf("after Shutdown: %s = %d, want 0", sessions, got)
	}
	defer runtime.KeepAlive(rec) // the backend, and with it the gauge's function, lives through the loop
	for range 10 {
		runtime.GC()
		select {
		case <-collected:
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Error("the gauge's state was not collected within 10 GC cycles: the registered function still refers to it")
}

// Both hooks are safe for concurrent use, from turns and from other goroutines: each series is
// registered once, every caller gets the same counter, and a gauge read races with neither the
// registration nor Shutdown (OBS-150).
func TestLateRegistrationConcurrent(t *testing.T) {
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithMetrics(rec))
	atNew := len(rec.registered())

	const callers = 8
	var wg sync.WaitGroup
	register := func() {
		if add := hooks.MetricCounter(sys, framesSent, framesSentHelp); add != nil {
			add(1)
		} else {
			t.Error("MetricCounter returned nil before Shutdown")
		}
		hooks.MetricGauge(sys, sessions, sessionsHelp, func() int64 { return 1 })
	}
	for range callers {
		wg.Go(register)
	}
	// The same from inside turns.
	inTurn := make(chan struct{}, callers)
	for i := range callers {
		pid := spawnObs(t, sys, "caller"+strconv.Itoa(i), obsOf(&obsActor{receive: func(*lockstep.Context[ping], ping) {
			register()
			inTurn <- struct{}{}
		}}))
		if err := lockstep.Send(sys, pid, ping{}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	for range callers {
		recv(t, inTurn)
	}

	late := rec.registered()[atNew:]
	if len(late) != 2 {
		t.Fatalf("%d callers registered %d series, want 2", 2*callers, len(late))
	}
	if got := rec.count(t, framesSent); got != 2*callers {
		t.Errorf("%s = %d, want %d", framesSent, got, 2*callers)
	}

	// A backend may read the gauge while the system shuts down.
	gauge := rec.find(t, sessions)
	done := make(chan struct{})
	var reader sync.WaitGroup
	reader.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				if v := gauge.fn(); v != 0 && v != 1 {
					t.Errorf("%s = %d during Shutdown, want 0 or 1", sessions, v)
				}
			}
		}
	})
	shutdownNow(t, sys)
	close(done)
	reader.Wait()
	if got := rec.gauge(t, sessions); got != 0 {
		t.Errorf("after Shutdown: %s = %d, want 0", sessions, got)
	}
}
