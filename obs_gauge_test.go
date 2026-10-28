// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"math"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/overload"
)

// gaugeRows is the gauge table of OBS-130 in registration order: names and help texts spelled out
// here, not taken from the code under test.
var gaugeRows = []struct{ name, help string }{
	{"lockstep_actors", "Live actors."},
	{"lockstep_mailbox_depth_total", "User messages queued in all mailboxes."},
	{"lockstep_mailbox_depth_max", "User messages queued in the fullest mailbox."},
	{"lockstep_admission_enabled", "1 if memory admission control is on."},
	{"lockstep_admission_refusing", "1 while admission control refuses work."},
	{"lockstep_goroutines", "Goroutines in the process."},
}

// shutdownNow shuts sys down, waiting up to 30 s. The tests that call it check what the gauges
// return afterwards; a system the test shut down already returns at once, so a later cleanup that
// shuts it down again is harmless.
func shutdownNow(t *testing.T, sys *lockstep.System) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// fakeHeap makes the memory sampler of every system built until the test ends read a live heap of
// live bytes and no Go memory limit, whatever the process's environment sets.
func fakeHeap(t *testing.T, live *atomic.Uint64) {
	t.Helper()
	t.Cleanup(overload.SetReadMetrics(func(samples []metrics.Sample) {
		for i := range samples {
			switch samples[i].Name {
			case "/gc/heap/live:bytes":
				samples[i].Value = overload.Uint64Value(live.Load())
			case "/gc/gomemlimit:bytes":
				samples[i].Value = overload.Uint64Value(math.MaxInt64)
			}
		}
	}))
}

// AT-OBS-07: OBS-080, OBS-120, OBS-130.
func TestATOBS07RegistrationOrder(t *testing.T) {
	rec := &recMetrics{}
	newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))
	rec.failOnCall(t) // New is the only caller until a package registers late

	want := expandRows("n1") // the 21 + D counter series
	if len(want) != 21+len(lockstep.DeadLetterReasons()) {
		t.Fatalf("the counter table has %d series, want 21 + D = %d", len(want), 21+len(lockstep.DeadLetterReasons()))
	}
	counters := len(want)
	for _, g := range gaugeRows {
		s := &recSeries{name: g.name, labels: []lockstep.MetricLabel{{Name: "node", Value: "n1"}}}
		want = append(want, wantSeries{s.String(), g.help})
	}
	regs := rec.registered()
	if len(want) != 30 || len(regs) != len(want) {
		t.Fatalf("%d registrations, want %d (%d counter series, then %d gauges; 30 in all)",
			len(regs), len(want), counters, len(gaugeRows))
	}
	for i, w := range want {
		s := regs[i]
		if wantGauge := i >= counters; s.gauge != wantGauge {
			t.Errorf("registration %d (%s) is a gauge: %v, want %v", i, s, s.gauge, wantGauge)
		}
		if s.String() != w.key || s.help != w.help {
			t.Errorf("registration %d is %s with help %q, want %s with help %q", i, s, s.help, w.key, w.help)
		}
		if len(s.labels) == 0 || s.labels[0] != (lockstep.MetricLabel{Name: "node", Value: "n1"}) {
			t.Errorf("registration %d (%s) has labels %v, want node=n1 first", i, s, s.labels)
		}
		if s.gauge && s.fn == nil {
			t.Errorf("gauge %s was registered without a function", s)
		}
	}
}

// settleGauge polls the gauge name of rec until it returns want, and fails the test if it does not
// within 10 s.
func settleGauge(t *testing.T, rec *recMetrics, name string, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := rec.gauge(t, name)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s = %d after 10 s, want %d", name, got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// checkGauges fails the test unless each gauge of rec in want returns the value it names.
func checkGauges(t *testing.T, rec *recMetrics, want map[string]int64) {
	t.Helper()
	for name, w := range want {
		if got := rec.gauge(t, name); got != w {
			t.Errorf("%s = %d, want %d", name, got, w)
		}
	}
}

// checkGoroutines fails the test unless the gauge lockstep_goroutines of rec is within 2 of
// runtime.NumGoroutine(), as read just before and just after the gauge.
func checkGoroutines(t *testing.T, rec *recMetrics) {
	t.Helper()
	before := runtime.NumGoroutine()
	got := rec.gauge(t, "lockstep_goroutines")
	after := runtime.NumGoroutine()
	if got < int64(min(before, after)-2) || got > int64(max(before, after)+2) {
		t.Errorf("lockstep_goroutines = %d, want runtime.NumGoroutine() = %d..%d, give or take 2", got, before, after)
	}
}

// AT-OBS-09: OBS-100, OBS-130.
func TestATOBS09Gauges(t *testing.T) {
	fakeHeap(t, new(atomic.Uint64)) // no memory limit, so admission control is off
	rec := &recMetrics{}
	finalized := make(chan struct{})
	// The system lives only inside the function, so nothing the test keeps refers to it once the
	// gauges' functions, which the recording backend keeps, are the only thing left of it.
	func() {
		sys := lockstep.New(lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))
		// A cleanup, not a finalizer: a System holds a pointer to itself, through its guardians,
		// so a finalizer on it never runs, with metrics or without, while a cleanup runs once
		// nothing refers to the system but the system.
		runtime.AddCleanup(sys, func(struct{}) { close(finalized) }, struct{}{})

		for i := range 10 {
			spawnObs(t, sys, "idle"+strconv.Itoa(i), obsOf(&obsActor{}))
		}
		busy := spawnObs(t, sys, "busy", obsOf(&obsActor{}), lockstep.WithMailbox(100, lockstep.FailSend))
		release := lockstep.Hold(sys, busy.Address())
		// A failed test must neither leave a held actor, which Shutdown cannot stop, nor a system.
		defer func() {
			release()
			shutdownNow(t, sys)
		}()
		for range 7 {
			if err := lockstep.Send(sys, busy, ping{}); err != nil {
				t.Fatal(err)
			}
		}

		if got, want := rec.gauge(t, "lockstep_actors"), int64(lockstep.LiveActors(sys)); got != want {
			t.Errorf("lockstep_actors = %d, want the actor cap's live count %d", got, want)
		}
		checkGauges(t, rec, map[string]int64{
			"lockstep_actors":              11,
			"lockstep_mailbox_depth_total": 7,
			"lockstep_mailbox_depth_max":   7,
			// No memory limit and no WithAdmission: admission control is off.
			"lockstep_admission_enabled":  0,
			"lockstep_admission_refusing": 0,
		})
		checkGoroutines(t, rec)

		release()
		shutdownNow(t, sys)
		checkGauges(t, rec, map[string]int64{
			"lockstep_actors":              0,
			"lockstep_mailbox_depth_total": 0,
			"lockstep_mailbox_depth_max":   0,
			"lockstep_admission_enabled":   0,
			"lockstep_admission_refusing":  0,
		})
		checkGoroutines(t, rec) // the one gauge that keeps reporting
	}()

	// The gauges' functions, which rec still holds, must not keep the system alive: it is
	// unreachable, so its cleanup runs.
	defer runtime.KeepAlive(rec) // the backend, and with it the gauges' functions, lives through the loop
	for range 10 {
		runtime.GC()
		select {
		case <-finalized:
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Error("the cleanup of the *System did not run within 10 GC cycles: something still refers to the system")
}

// The admission gauges read the sampler's and the gate's state; they are 0 in AT-OBS-09's system,
// so this test turns both on and off.
func TestAdmissionGauges(t *testing.T) {
	// A live heap of 2100 over the high mark of 2000 makes New's first sample turn admission off;
	// one of 900 under the low mark of 1000 turns it on again at a later tick.
	var live atomic.Uint64
	live.Store(2100)
	fakeHeap(t, &live)
	rec := &recMetrics{}
	sys := lockstep.New(lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec), lockstep.WithAdmission(2000, 1000))
	t.Cleanup(func() { shutdownNow(t, sys) })

	checkGauges(t, rec, map[string]int64{"lockstep_admission_enabled": 1, "lockstep_admission_refusing": 1})
	live.Store(900)
	settleGauge(t, rec, "lockstep_admission_refusing", 0)
	checkGauges(t, rec, map[string]int64{"lockstep_admission_enabled": 1})
	live.Store(2100)
	settleGauge(t, rec, "lockstep_admission_refusing", 1)

	shutdownNow(t, sys)
	checkGauges(t, rec, map[string]int64{"lockstep_admission_enabled": 0, "lockstep_admission_refusing": 0})
}

// The depth gauges: the total is the sum and the maximum the largest of the user messages queued
// in the live actors' mailboxes, and they drop as the mailboxes drain.
func TestMailboxDepthGauges(t *testing.T) {
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))
	checkGauges(t, rec, map[string]int64{"lockstep_mailbox_depth_total": 0, "lockstep_mailbox_depth_max": 0})

	var processed atomic.Int64
	count := obsOf(&obsActor{receive: func(*lockstep.Context[ping], ping) { processed.Add(1) }})
	spawnHeld := func(name string, queued int) {
		t.Helper()
		pid := spawnObs(t, sys, name, count, lockstep.WithMailbox(5, lockstep.DropOldest))
		release := lockstep.Hold(sys, pid.Address())
		t.Cleanup(release) // runs before newProdSystem's Shutdown
		for range queued {
			if err := lockstep.Send(sys, pid, ping{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	spawnHeld("two", 2)
	spawnHeld("five", 5)
	// Seven messages to a DropOldest mailbox of five: each of the last two replaces the oldest.
	spawnHeld("over", 7)
	checkGauges(t, rec, map[string]int64{"lockstep_mailbox_depth_total": 12, "lockstep_mailbox_depth_max": 5})
}

// hop is the token of a pingPong.
type hop struct{ left int }

// pingPong is two actors that pass a token to each other: every hop is a Send from inside a turn.
type pingPong struct {
	sys     *lockstep.System
	a       lockstep.PID[hop]
	done    chan struct{} // receives one value when a token has made its hops
	timeout time.Duration // how long run waits for the token
}

// newPingPong spawns the pair /user/ping and /user/pong on sys.
func newPingPong(tb testing.TB, sys *lockstep.System) *pingPong {
	tb.Helper()
	p := &pingPong{sys: sys, done: make(chan struct{}, 1), timeout: 10 * time.Second}
	var a, b lockstep.PID[hop] // set before the first message, which is sent after both exist
	pass := func(to *lockstep.PID[hop]) func() lockstep.Actor[hop] {
		return func() lockstep.Actor[hop] {
			return &simActor[hop]{receive: func(ctx *lockstep.Context[hop], m hop) {
				if m.left == 0 {
					p.done <- struct{}{}
					return
				}
				if err := lockstep.Send(ctx, *to, hop{left: m.left - 1}); err != nil {
					panic(err)
				}
			}}
		}
	}
	var err error
	if a, err = lockstep.Spawn(sys, "ping", pass(&b)); err != nil {
		tb.Fatal(err)
	}
	if b, err = lockstep.Spawn(sys, "pong", pass(&a)); err != nil {
		tb.Fatal(err)
	}
	p.a = a
	return p
}

// run passes a token through hops Sends between the two actors and waits for it, up to p.timeout.
func (p *pingPong) run(tb testing.TB, hops int) {
	tb.Helper()
	if err := lockstep.Send(p.sys, p.a, hop{left: hops}); err != nil {
		tb.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(p.timeout):
		tb.Fatalf("%d hops took more than %v", hops, p.timeout)
	}
}

// spawnIdleTree spawns parents actors under /user, each of which spawns children idle actors in
// its Started hook, and returns once every one of them has started: parents + parents*children
// actors in all. The actors are spread under parents because Shutdown's time grows with the
// square of an actor's number of children.
func spawnIdleTree(tb testing.TB, sys *lockstep.System, parents, children int) {
	tb.Helper()
	var started sync.WaitGroup // Started has returned for every actor that exists
	started.Add(parents * (1 + children))
	var failed atomic.Pointer[error]
	idle := obsOf(&obsActor{started: func(*lockstep.Context[ping]) { started.Done() }})
	parent := func() lockstep.Actor[ping] {
		return &obsActor{started: func(ctx *lockstep.Context[ping]) {
			defer started.Done()
			for i := range children {
				if _, err := lockstep.Spawn(ctx, strconv.Itoa(i), idle); err != nil {
					failed.CompareAndSwap(nil, &err)
					started.Add(i - children) // the children that will never exist
					return
				}
			}
		}}
	}
	for i := range parents {
		if _, err := lockstep.Spawn(sys, "p"+strconv.Itoa(i), parent); err != nil {
			tb.Fatal(err)
		}
	}
	started.Wait()
	if err := failed.Load(); err != nil {
		tb.Fatalf("spawning the idle actors: %v", *err)
	}
	if got, want := lockstep.LiveActors(sys), parents+parents*children; got != want {
		tb.Fatalf("%d live actors, want %d", got, want)
	}
}

// AT-OBS-10: OBS-100.
func TestATOBS10GaugeDoesNotBlockSend(t *testing.T) {
	// Every message is a Send, and under the race detector, which CI runs with coverage counters
	// on a shared machine, each costs several times what it does otherwise. The run is lighter
	// there so that the bound of 10 s holds on a slow machine too: the poller still overlaps with
	// thousands of Sends, which is what the detector needs to see a race.
	messages := 100_000
	if lockstep.RaceEnabled {
		messages = 20_000
	}
	rec := &recMetrics{}
	sys := lockstep.New(lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))
	t.Cleanup(func() { shutdownNow(t, sys) })
	spawnIdleTree(t, sys, 100, 100) // 10,000 idle actors
	pp := newPingPong(t, sys)
	depth := rec.find(t, "lockstep_mailbox_depth_total").fn

	// The test goroutine sends and the poller collects, both for the whole run. The token is a
	// single message in flight. A call reads each mailbox once, at its own moment, so a token that
	// moves from one actor to the other during a call can be counted at both: the depth is 0, 1 or
	// 2, never more.
	var p struct {
		calls int
		peak  int64
	}
	stop, finished := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-stop:
				return
			default:
			}
			p.peak = max(p.peak, depth())
			p.calls++
			runtime.Gosched()
		}
	}()
	var stopOnce sync.Once
	stopPoller := func() {
		stopOnce.Do(func() { close(stop) })
		<-finished
	}
	// Registered after the system's shutdown, so it runs before it: a poller that outlived a failed
	// run would make every final stop of the 10,000 actors wait for one of its calls.
	t.Cleanup(stopPoller)

	pp.run(t, messages)
	stopPoller()
	if p.calls == 0 {
		t.Error("the poller never called the gauge")
	}
	if p.peak > 2 {
		t.Errorf("lockstep_mailbox_depth_total peaked at %d, want at most 2 (one message in flight)", p.peak)
	}
}

// OBS-100: a Send and a turn take no lock that a gauge function takes. The list of live mailboxes
// the depth gauges iterate is write-locked as a Spawn or a final stop locks it: the gauge waits,
// and the actors go on passing messages.
func TestGaugeLockIsNotTakenBySendOrTurn(t *testing.T) {
	rec := &recMetrics{}
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(rec))
	pp := newPingPong(t, sys)
	depth := rec.find(t, "lockstep_mailbox_depth_total").fn

	unlock := lockstep.LockLiveIndex(sys)
	var unlocked atomic.Bool
	t.Cleanup(func() {
		if unlocked.CompareAndSwap(false, true) {
			unlock()
		}
	})
	returned := make(chan int64, 1)
	go func() { returned <- depth() }()

	pp.run(t, 1000) // Send and turns finish while the gauge waits
	select {
	case v := <-returned:
		t.Fatalf("the gauge returned %d while the list was locked", v)
	default:
	}

	unlocked.Store(true)
	unlock()
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("the gauge did not return after the list was unlocked")
	}
}

// pollingMetrics is a recMetrics that calls every gauge function in a loop, on a goroutine of its
// own, from the moment it is registered: a backend that is scraped while a system is still being
// built does the same.
type pollingMetrics struct {
	recMetrics
	stop chan struct{}
	wg   sync.WaitGroup
}

func (m *pollingMetrics) GaugeFunc(name, help string, fn func() int64, labels ...lockstep.MetricLabel) {
	m.recMetrics.GaugeFunc(name, help, fn, labels...)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-m.stop:
				return
			default:
				fn()
				runtime.Gosched()
			}
		}
	}()
}

// OBS-100: the gauge functions are safe to call from any goroutine at any time: while New is still
// building the system, while actors spawn and stop, and while the system shuts down. The race
// detector checks it.
func TestGaugesCalledWhileSystemChanges(t *testing.T) {
	m := &pollingMetrics{stop: make(chan struct{})}
	t.Cleanup(func() { // runs after the system's shutdown, which the pollers run through
		close(m.stop)
		m.wg.Wait()
	})
	sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithMetrics(m))
	const parents, children = 5, 20
	spawnIdleTree(t, sys, parents, children)
	parent, err := lockstep.Lookup[ping](sys, "/user/p2")
	if err != nil {
		t.Fatal(err)
	}
	if err := lockstep.Stop(sys, parent.Address()); err != nil {
		t.Fatal(err)
	}
	settleGauge(t, &m.recMetrics, "lockstep_actors", (parents-1)*(1+children)) // all but the stopped parent's subtree
}

// The list the depth gauges iterate has one entry for each live actor: Spawn adds it, the final
// stop removes it, a restart keeps it, and a stop that follows a Spawn at once does not leave it
// behind.
func TestLiveIndexFollowsLiveActors(t *testing.T) {
	waitLive := func(t *testing.T, sys *lockstep.System, want int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for lockstep.LiveActors(sys) != want || lockstep.LiveIndexLen(sys) != want {
			if time.Now().After(deadline) {
				t.Fatalf("%d live actors and %d listed mailboxes after 10 s, want %d of each",
					lockstep.LiveActors(sys), lockstep.LiveIndexLen(sys), want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	t.Run("metrics off", func(t *testing.T) {
		sys := newProdSystem(t)
		spawnObs(t, sys, "a", obsOf(&obsActor{}))
		if n := lockstep.LiveIndexLen(sys); n != -1 {
			t.Errorf("a system without metrics keeps a list of %d mailboxes, want none", n)
		}
	})

	t.Run("spawn, stop, restart, shutdown", func(t *testing.T) {
		rec := &recMetrics{}
		sys := lockstep.New(lockstep.WithMetrics(rec))
		t.Cleanup(func() { shutdownNow(t, sys) })
		waitLive(t, sys, 0)
		spawnIdleTree(t, sys, 3, 5)
		waitLive(t, sys, 18)

		// Stopping a parent stops its children first.
		parent, err := lockstep.Lookup[ping](sys, "/user/p1")
		if err != nil {
			t.Fatal(err)
		}
		if err := lockstep.Stop(sys, parent.Address()); err != nil {
			t.Fatal(err)
		}
		waitLive(t, sys, 12)

		// A restart ends an incarnation, not the actor.
		failed := make(chan struct{}, 2)
		a := spawnObs(t, sys, "crash", obsOf(&obsActor{
			started: func(*lockstep.Context[ping]) { failed <- struct{}{} },
			receive: func(*lockstep.Context[ping], ping) { panic("boom") },
		}))
		recv(t, failed)
		if err := lockstep.Send(sys, a, ping{}); err != nil {
			t.Fatal(err)
		}
		recv(t, failed)
		waitLive(t, sys, 13)

		shutdownNow(t, sys)
		waitLive(t, sys, 0)
	})

	t.Run("stop right after spawn", func(t *testing.T) {
		rec := &recMetrics{}
		sys := newProdSystem(t, lockstep.WithMetrics(rec))
		idle := obsOf(&obsActor{})
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 250 {
					pid, err := lockstep.Spawn(sys, "", idle)
					if err != nil {
						t.Error(err)
						return
					}
					if err := lockstep.Stop(sys, pid.Address()); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		waitLive(t, sys, 0)
	})
}
