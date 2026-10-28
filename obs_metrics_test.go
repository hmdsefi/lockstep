// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"runtime/metrics"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/overload"
)

// counterRow is one row of the counter table: a name, its help, and the label after node with its
// values in order (none for a series without a label).
type counterRow struct {
	name   string
	help   string
	label  string
	values []string
}

// wantPolicies is the label of lockstep_mailbox_full_total and lockstep_shed_total.
var wantPolicies = []string{"fail-send", "drop-newest", "drop-oldest", "block-sender"}

// counterRows is the counter table, in registration order. The names, help texts and label values
// are spelled out here, not taken from the code under test.
var counterRows = []counterRow{
	{"lockstep_actors_spawned_total", "Actors spawned.", "", nil},
	{"lockstep_actors_stopped_total", "Actors stopped for good.", "reason",
		[]string{"normal", "failed", "parent-stopping", "shutdown"}},
	{"lockstep_restarts_total", "Actor restarts.", "", nil},
	{"lockstep_mailbox_full_total", "Sends that found the target mailbox full.", "policy", wantPolicies},
	{"lockstep_shed_total", "User messages shed by admission control.", "policy", wantPolicies},
	{"lockstep_drops_total", "Messages and results discarded.", "reason",
		[]string{"drop-newest", "drop-oldest", "late-reply", "cancelled-effect"}},
	{"lockstep_deadletters_total", "Messages sent to dead letters.", "reason",
		[]string{"stopped", "not-found", "unreachable"}},
	{"lockstep_admission_transitions_total", "Admission control state changes.", "to",
		[]string{"refusing", "admitting"}},
	{"lockstep_stuck_turns_total", "Turns reported by the watchdog.", "", nil},
}

// wantSeries is one expected counter series: its identity as recSeries.String prints it, and its
// help text.
type wantSeries struct {
	key  string
	help string
}

// expandRows returns the series of rows on node, a labeled series once per value, in order.
func expandRows(node string) []wantSeries {
	var out []wantSeries
	for _, r := range counterRows {
		s := &recSeries{name: r.name, labels: []lockstep.MetricLabel{{Name: "node", Value: node}}}
		if r.label == "" {
			out = append(out, wantSeries{s.String(), r.help})
			continue
		}
		for _, v := range r.values {
			s := &recSeries{name: r.name, labels: []lockstep.MetricLabel{{Name: "node", Value: node}, {Name: r.label, Value: v}}}
			out = append(out, wantSeries{s.String(), r.help})
		}
	}
	return out
}

// labelValues returns the values of the label called label across the series called name that
// rec registered, in registration order.
func labelValues(rec *recMetrics, name, label string) []string {
	var out []string
	for _, s := range rec.registered() {
		if s.name != name {
			continue
		}
		for _, l := range s.labels {
			if l.Name == label {
				out = append(out, l.Value)
			}
		}
	}
	return out
}

// noopMetrics is a backend that accepts every registration and counts nothing.
type noopMetrics struct{}

type noopCounter struct{}

func (noopCounter) Add(uint64) {}

func (noopMetrics) Counter(string, string, ...lockstep.MetricLabel) lockstep.Counter {
	return noopCounter{}
}

func (noopMetrics) GaugeFunc(string, string, func() int64, ...lockstep.MetricLabel) {}

// waitGone waits until no actor is registered at pid's path on sys.
func waitGone(t *testing.T, sys *lockstep.System, pid lockstep.PID[ping]) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := lockstep.Lookup[ping](sys, pid.Address().Path()); errors.Is(err, lockstep.ErrNotFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%v did not stop within 5 s", pid)
		}
		time.Sleep(time.Millisecond)
	}
}

// counterScenario runs the five-actor scenario of AT-OBS-08 on a New system on node n1, built with
// opts, and shuts the system down. Every step is synchronous with the event it causes, so when it
// returns each event has happened and been counted.
func counterScenario(t *testing.T, opts ...lockstep.Option) {
	t.Helper()
	sys := newProdSystem(t, append([]lockstep.Option{lockstep.WithNodeName("n1")}, opts...)...)
	send := func(to lockstep.PID[ping]) error { return lockstep.Send(sys, to, ping{}) }

	// a panics on its first message and restarts: the restart is counted when the old incarnation
	// ends, before the new one's Started runs.
	started := make(chan struct{}, 2)
	a := spawnObs(t, sys, "a", obsOf(&obsActor{
		started: func(*lockstep.Context[ping]) { started <- struct{}{} },
		receive: func(*lockstep.Context[ping], ping) { panic("boom") },
	}))
	recv(t, started)
	if err := send(a); err != nil {
		t.Fatal(err)
	}
	recv(t, started)

	// b is stopped with Stop.
	b := spawnObs(t, sys, "b", obsOf(&obsActor{}))
	if err := lockstep.Stop(sys, b.Address()); err != nil {
		t.Fatal(err)
	}
	waitGone(t, sys, b)

	// c and d are held with a mailbox of one: the second message to c is refused, the second to d
	// dropped. They are released, and have processed their one message, before the shutdown, or the
	// stop would send it to dead letters.
	processed := make(chan struct{}, 2)
	onReceive := func(*lockstep.Context[ping], ping) { processed <- struct{}{} }
	c := spawnObs(t, sys, "c", obsOf(&obsActor{receive: onReceive}), lockstep.WithMailbox(1, lockstep.FailSend))
	d := spawnObs(t, sys, "d", obsOf(&obsActor{receive: onReceive}), lockstep.WithMailbox(1, lockstep.DropNewest))
	releaseC, releaseD := lockstep.Hold(sys, c.Address()), lockstep.Hold(sys, d.Address())
	t.Cleanup(releaseC) // a failed test must not leave a held actor that Shutdown cannot stop
	t.Cleanup(releaseD)
	for range 2 {
		err := send(c)
		if err != nil && !errors.Is(err, lockstep.ErrMailboxFull) {
			t.Fatal(err)
		}
		if err := send(d); err != nil {
			t.Fatal(err)
		}
	}
	releaseC()
	releaseD()
	recv(t, processed)
	recv(t, processed)

	// e answers its ask 50 ms late, from a timer, after the ask timed out after 10 ms.
	replied := make(chan struct{}, 1)
	var kept lockstep.ReplyTo[int]
	e, err := lockstep.Spawn(sys, "e", func() lockstep.Actor[lateAsk] {
		return &lateActor{receive: func(ctx *lockstep.Context[lateAsk], m lateAsk) {
			if m.answer {
				kept.Send(ctx, 1)
				replied <- struct{}{}
				return
			}
			kept = m.rt
			ctx.After(50*time.Millisecond, lateAsk{answer: true})
		}}
	})
	if err != nil {
		t.Fatal(err)
	}
	r := lockstep.Ask(sys, e, func(rt lockstep.ReplyTo[int]) lateAsk { return lateAsk{rt: rt} },
		lockstep.WithTimeout(10*time.Millisecond))
	if _, err := r.Await(context.Background()); !errors.Is(err, lockstep.ErrTimeout) {
		t.Fatalf("Await = %v, want ErrTimeout", err)
	}
	recv(t, replied)

	// A message to b's old PID is a dead letter with reason stopped, one to a PID on another node
	// one with reason unreachable: the system has no transport.
	if err := send(b); err != nil {
		t.Fatal(err)
	}
	if err := send(lockstep.PIDAt[ping]("n2", "/user/x")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// AT-OBS-08: OBS-090, OBS-120, OBS-140.
func TestATOBS08CountersFollowEvents(t *testing.T) {
	rec := &recMetrics{}
	counterScenario(t, lockstep.WithMetrics(rec))

	// Every other series is 0.
	want := map[string]uint64{
		`lockstep_actors_spawned_total{node="n1"}`:                    5,
		`lockstep_restarts_total{node="n1"}`:                          1,
		`lockstep_actors_stopped_total{node="n1",reason="normal"}`:    1,
		`lockstep_actors_stopped_total{node="n1",reason="shutdown"}`:  4, // a, c, d and e, not the guardians
		`lockstep_mailbox_full_total{node="n1",policy="fail-send"}`:   1,
		`lockstep_mailbox_full_total{node="n1",policy="drop-newest"}`: 1,
		`lockstep_drops_total{node="n1",reason="drop-newest"}`:        1,
		`lockstep_drops_total{node="n1",reason="late-reply"}`:         1,
		`lockstep_deadletters_total{node="n1",reason="stopped"}`:      1,
		`lockstep_deadletters_total{node="n1",reason="unreachable"}`:  1,
	}
	seen := make(map[string]bool)
	for _, s := range rec.counters() {
		key := s.String()
		seen[key] = true
		if got, w := s.n.Load(), want[key]; got != w {
			t.Errorf("%s = %d, want %d", key, got, w)
		}
	}
	for key := range want {
		if !seen[key] {
			t.Errorf("series %s was never registered", key)
		}
	}
}

// AT-OBS-11: OBS-090, OBS-110.
func TestATOBS11MetricsOff(t *testing.T) {
	t.Run("scenario without metrics", func(t *testing.T) {
		counterScenario(t) // no panic, and the same scenario as AT-OBS-08
	})

	t.Run("Send allocations", func(t *testing.T) {
		if lockstep.RaceEnabled {
			t.Skip("allocation counts are not stable under the race detector")
		}
		off := sendAllocs(t)
		noop := sendAllocs(t, lockstep.WithMetrics(noopMetrics{}))
		if off != noop {
			t.Errorf("allocations of an actor-to-actor Send: %v with no metrics, %v with a no-op Metrics; want equal", off, noop)
		}
	})
}

// sendAllocs returns testing.AllocsPerRun of one Send from an actor, inside its turn, to a held
// actor with room for every message, on a New system built with opts.
func sendAllocs(t *testing.T, opts ...lockstep.Option) float64 {
	t.Helper()
	const runs = 1000
	sys := newProdSystem(t, opts...)
	sink := spawnObs(t, sys, "sink", obsOf(&obsActor{}), lockstep.WithMailbox(runs+10, lockstep.FailSend))
	release := lockstep.Hold(sys, sink.Address())
	t.Cleanup(release)
	result := make(chan float64, 1)
	sender := spawnObs(t, sys, "sender", obsOf(&obsActor{receive: func(ctx *lockstep.Context[ping], _ ping) {
		result <- testing.AllocsPerRun(runs, func() {
			if err := lockstep.Send(ctx, sink, ping{}); err != nil {
				panic(err)
			}
		})
	}}))
	if err := lockstep.Send(sys, sender, ping{}); err != nil {
		t.Fatal(err)
	}
	return recv(t, result)
}

// AT-OBS-15 is ExampleWithMetrics.

// AT-OBS-16, the production half: OBS-145.
func TestATOBS16OneSpelling(t *testing.T) {
	rec := &recMetrics{}
	newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))

	policies := []string{
		lockstep.FailSend.String(), lockstep.DropNewest.String(), lockstep.DropOldest.String(), lockstep.BlockSender.String(),
	}
	for _, name := range []string{"lockstep_mailbox_full_total", "lockstep_shed_total"} {
		if got := labelValues(rec, name, "policy"); !slices.Equal(got, policies) {
			t.Errorf("%s policy labels = %v, want MailboxPolicy.String() in order: %v", name, got, policies)
		}
	}
	kinds := []string{
		lockstep.StopNormal.String(), lockstep.StopFailed.String(), lockstep.StopParentStopping.String(), lockstep.StopShutdown.String(),
	}
	if got := labelValues(rec, "lockstep_actors_stopped_total", "reason"); !slices.Equal(got, kinds) {
		t.Errorf("lockstep_actors_stopped_total reason labels = %v, want StopKind.String() in order: %v", got, kinds)
	}
	if got, want := labelValues(rec, "lockstep_deadletters_total", "reason"), lockstep.DeadLetterReasons(); !slices.Equal(got, want) {
		t.Errorf("lockstep_deadletters_total reason labels = %v, want the dead-letter reason list: %v", got, want)
	}
}

// TestCounterRegistrationOrder checks the 21 + D counter series of OBS-120: the order, the node
// label first, the help texts and the label values. Gauges, when New registers them, come after.
func TestCounterRegistrationOrder(t *testing.T) {
	rec := &recMetrics{}
	newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))
	rec.failOnCall(t) // New is the only caller until a package registers late

	want := expandRows("n1")
	if len(want) != 21+len(lockstep.DeadLetterReasons()) {
		t.Fatalf("the table has %d series, want 21 + D = %d", len(want), 21+len(lockstep.DeadLetterReasons()))
	}
	regs := rec.registered()
	if len(regs) < len(want) {
		t.Fatalf("%d registrations, want at least the %d counter series", len(regs), len(want))
	}
	for i, w := range want {
		s := regs[i]
		if s.gauge {
			t.Errorf("registration %d is a gauge %s, want counter %s", i, s, w.key)
			continue
		}
		if s.String() != w.key || s.help != w.help {
			t.Errorf("registration %d is %s with help %q, want %s with help %q", i, s, s.help, w.key, w.help)
		}
		if len(s.labels) == 0 || s.labels[0] != (lockstep.MetricLabel{Name: "node", Value: "n1"}) {
			t.Errorf("registration %d (%s) has labels %v, want node=n1 first", i, s, s.labels)
		}
	}
}

// panicMetrics is a backend whose Counter panics.
type panicMetrics struct{ noopMetrics }

func (panicMetrics) Counter(string, string, ...lockstep.MetricLabel) lockstep.Counter {
	panic("backend")
}

// OBS §7: a panic in the backend during New propagates out of New. The engine New started is
// stopped first; the package's goroutine-leak check (TestMain) fails the run if its workers stay.
func TestNewPropagatesBackendPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != "backend" {
			t.Errorf("New panicked with %v, want the backend's panic", r)
		}
	}()
	lockstep.New(lockstep.WithMetrics(panicMetrics{}), lockstep.WithWorkers(4))
	t.Error("New returned")
}

// The admission and watchdog counters have no event in AT-OBS-08's scenario. This test checks that
// each is registered where New can already change admission, and counted where the change and the
// stuck turn are reported (OBS-120).
func TestCountersAdmissionAndStuck(t *testing.T) {
	t.Run("admission", func(t *testing.T) {
		// A live heap of 2100 over the high mark of 2000 makes New's first sample turn admission off,
		// before New returns; 900 under the low mark of 1000 turns it on again at a later tick.
		var live atomic.Uint64
		live.Store(2100)
		t.Cleanup(overload.SetReadMetrics(func(samples []metrics.Sample) { // restored after the shutdown
			for i := range samples {
				switch samples[i].Name {
				case "/gc/heap/live:bytes":
					samples[i].Value = overload.Uint64Value(live.Load())
				case "/gc/gomemlimit:bytes":
					samples[i].Value = overload.Uint64Value(math.MaxInt64)
				}
			}
		}))
		rec := &recMetrics{}
		sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec), lockstep.WithAdmission(2000, 1000))

		const transitions = "lockstep_admission_transitions_total"
		waitCount := func(to string, n uint64) {
			t.Helper()
			deadline := time.Now().Add(10 * time.Second)
			for rec.count(t, transitions, "to", to) < n {
				if time.Now().After(deadline) {
					t.Fatalf("transitions to %s still %d after 10 s, want %d", to, rec.count(t, transitions, "to", to), n)
				}
				time.Sleep(time.Millisecond)
			}
		}
		if got := rec.count(t, transitions, "to", "refusing"); got != 1 {
			t.Errorf("transitions to refusing = %d after New, want 1: the change New's first sample made", got)
		}
		// Spawning refuses while admission is off: let it resume, spawn, and refuse again.
		live.Store(900)
		waitCount("admitting", 1)
		pid := spawnObs(t, sys, "a", obsOf(&obsActor{}), lockstep.WithMailbox(1, lockstep.FailSend))
		live.Store(2100)
		waitCount("refusing", 2)

		// A message to an actor with room is shed, not refused by a full mailbox.
		if err := lockstep.Send(sys, pid, ping{}); !errors.Is(err, lockstep.ErrOverloaded) {
			t.Fatalf("Send while refusing = %v, want ErrOverloaded", err)
		}
		if got := rec.count(t, "lockstep_shed_total", "policy", "fail-send"); got != 1 {
			t.Errorf("shed_total{fail-send} = %d, want 1", got)
		}
		if got := rec.count(t, "lockstep_mailbox_full_total", "policy", "fail-send"); got != 0 {
			t.Errorf("mailbox_full_total{fail-send} = %d, want 0: a shed message is not a full mailbox", got)
		}
		if got := rec.count(t, transitions, "to", "admitting"); got != 1 {
			t.Errorf("transitions to admitting = %d, want 1", got)
		}
	})

	t.Run("stuck turn", func(t *testing.T) {
		// The Info-level default logger leaves the Error event enabled: count it with the log off too.
		l, _ := newRecLogger(slog.LevelError + 1)
		rec := &recMetrics{}
		sys := newProdSystem(t, lockstep.WithMetrics(rec), lockstep.WithLogger(l), lockstep.WithWatchdog(50*time.Millisecond))
		entered, release := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { close(release) }) // before the shutdown, which waits for the turn
		pid := spawnObs(t, sys, "stuck", obsOf(&obsActor{receive: func(*lockstep.Context[ping], ping) {
			close(entered)
			<-release
		}}))
		if err := lockstep.Send(sys, pid, ping{}); err != nil {
			t.Fatal(err)
		}
		recv(t, entered)
		deadline := time.Now().Add(10 * time.Second)
		for rec.count(t, "lockstep_stuck_turns_total") == 0 {
			if time.Now().After(deadline) {
				t.Fatal("no stuck turn counted within 10 s")
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond) // the watchdog reports a turn once
		if got := rec.count(t, "lockstep_stuck_turns_total"); got != 1 {
			t.Errorf("stuck_turns_total = %d, want 1", got)
		}
	})
}
