// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// lifeActor is a test actor with optional Started, Receive and Stopped hooks.
type lifeActor[M any] struct {
	started func(ctx *Context[M])
	receive func(ctx *Context[M], msg M)
	stopped func(ctx *Context[M], r StopReason)
}

func (a *lifeActor[M]) Started(ctx *Context[M]) {
	if a.started != nil {
		a.started(ctx)
	}
}

func (a *lifeActor[M]) Receive(ctx *Context[M], msg M) {
	if a.receive != nil {
		a.receive(ctx, msg)
	}
}

func (a *lifeActor[M]) Stopped(ctx *Context[M], r StopReason) {
	if a.stopped != nil {
		a.stopped(ctx, r)
	}
}

// stopLog collects the path and reason of every Stopped call, in call order, from any goroutine.
type stopLog struct {
	mu      sync.Mutex
	paths   []string
	reasons map[string]StopReason
}

// stopped is a Stopped hook that logs the actor.
func (l *stopLog) stopped(ctx *Context[any], r StopReason) {
	l.mu.Lock()
	defer l.mu.Unlock()
	path := ctx.Self().Address().Path()
	l.paths = append(l.paths, path)
	if l.reasons == nil {
		l.reasons = map[string]StopReason{}
	}
	l.reasons[path] = r
}

func (l *stopLog) snapshot() ([]string, map[string]StopReason) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.paths), l.reasons
}

// shutdownWithin calls sys.Shutdown with a context that ends after 10 s and fails the test on an
// error.
func shutdownWithin(t *testing.T, sys *System) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
}

// stoppedText is ErrStopped's text for sys (MSG §7, ACT §7): the system address is the user
// guardian's.
func stoppedText(sys *System) string { return "lockstep: stopped: " + sys.user.String() }

// isStoppedErr reports whether err is sys's ErrStopped error with its exact text.
func isStoppedErr(sys *System, err error) bool {
	return errors.Is(err, ErrStopped) && err != nil && err.Error() == stoppedText(sys) &&
		strings.HasPrefix(err.Error(), "lockstep: stopped: lockstep://"+sys.node+"/user#")
}

// shutdownPhases returns the indexes in recs of the lockstep.shutdown records with phase=begin and
// phase=done, each checked to carry node=local, the test systems' node.
func shutdownPhases(t *testing.T, recs []engine.Record) (begins, dones []int) {
	const node = "local"
	t.Helper()
	for i, r := range recs {
		if r.Kind != "lockstep.shutdown" {
			continue
		}
		attrs := recordsOf([]engine.Record{r}, r.Kind)[0]
		switch {
		case slices.Equal(attrs, []string{"node=" + node, "phase=begin"}):
			begins = append(begins, i)
		case slices.Equal(attrs, []string{"node=" + node, "phase=done"}):
			dones = append(dones, i)
		default:
			t.Errorf("lockstep.shutdown record %q, want node=%s and phase begin or done", attrs, node)
		}
	}
	return begins, dones
}

// waitUntil fails the test unless cond becomes true within 10 s.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 10 s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// AT-ACT-06: ACT-050. Shutdown stops the user guardian's children in reverse spawn order, each
// subtree depth first, then the system guardian's, between the phase=begin and phase=done
// records.
func TestATACT06ShutdownOrder(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	var log stopLog
	leaf := func() Actor[any] { return &lifeActor[any]{stopped: log.stopped} }
	spawned := make(chan error, 2)
	if _, err := Spawn(sys, "a", func() Actor[any] {
		return &lifeActor[any]{
			started: func(ctx *Context[any]) {
				_, err1 := Spawn(ctx, "a1", leaf)
				_, err2 := Spawn(ctx, "a2", leaf)
				spawned <- errors.Join(err1, err2)
			},
			stopped: log.stopped,
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Spawn(sys, "b", leaf); err != nil {
		t.Fatal(err)
	}
	if _, err := spawnSystem(sys, "s", leaf); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, spawned); err != nil {
		t.Fatalf("spawning a1 and a2: %v", err)
	}

	shutdownWithin(t, sys)

	paths, reasons := log.snapshot()
	if want := []string{"/user/b", "/user/a/a2", "/user/a/a1", "/user/a", "/system/s"}; !slices.Equal(paths, want) {
		t.Errorf("Stopped order %q, want %q", paths, want)
	}
	for _, p := range []string{"/user/a", "/user/b", "/system/s"} {
		if reasons[p] != Shutdown {
			t.Errorf("%s got reason %v, want Shutdown", p, reasons[p])
		}
	}
	recs := rec.Records()
	begins, dones := shutdownPhases(t, recs)
	if len(begins) != 1 || len(dones) != 1 {
		t.Fatalf("phase=begin at %v and phase=done at %v, want one of each", begins, dones)
	}
	stops := 0
	for i, r := range recs {
		if r.Kind != "lockstep.stop" {
			continue
		}
		stops++
		if i < begins[0] || i > dones[0] {
			t.Errorf("lockstep.stop record %d outside phase=begin (%d) and phase=done (%d)", i, begins[0], dones[0])
		}
	}
	if stops != 5 {
		t.Errorf("%d lockstep.stop records, want 5", stops)
	}
}

// AT-ACT-07: ACT-060, ENG-140. Shutdown's waiting rules: an external caller on the local engine
// waits until ctx ends or the system stops; a call during a turn and a call on the manual engine
// return nil at once; concurrent calls run one shutdown.
func TestATACT07ShutdownWaiting(t *testing.T) {
	t.Run("a cancelled ctx", func(t *testing.T) {
		sys := newLocalSystem(t)
		entered, release := make(chan struct{}, 1), make(chan struct{})
		if _, err := Spawn(sys, "slow", actorOf[int](&lifeActor[int]{stopped: func(*Context[int], StopReason) {
			entered <- struct{}{}
			<-release
		}})); err != nil {
			t.Fatal(err)
		}
		if err := sys.Shutdown(cancelled()); err != context.Canceled { //nolint:errorlint // ACT-060: unwrapped
			t.Errorf("Shutdown with a cancelled ctx = %v, want context.Canceled", err)
		}
		recv(t, entered)
		if sys.shut.engStopped.Load() {
			t.Error("the engine stopped while a Stopped hook runs")
		}
		close(release)
		if err := sys.Shutdown(context.Background()); err != nil {
			t.Errorf("second Shutdown = %v, want nil", err)
		}
		if !sys.shut.engStopped.Load() {
			t.Error("Shutdown returned nil before the engine stopped (ENG-140)")
		}
	})

	t.Run("b in Receive", func(t *testing.T) {
		sys := newLocalSystem(t)
		got := make(chan error, 1)
		pid, err := Spawn(sys, "a", actorOf[int](&lifeActor[int]{receive: func(*Context[int], int) {
			got <- sys.Shutdown(context.Background())
		}}))
		if err != nil {
			t.Fatal(err)
		}
		send(t, sys, pid, 1)
		if err := recv(t, got); err != nil {
			t.Errorf("Shutdown in Receive = %v, want nil", err)
		}
		// Nothing outside the engine calls Shutdown again: the system stops the engine itself.
		waitUntil(t, "the system stops its engine", sys.shut.engStopped.Load)
		if _, err := Lookup[int](sys, "/user/a"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Lookup after the shutdown = %v, want ErrNotFound", err)
		}
	})

	t.Run("c concurrent", func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		for _, name := range []string{"a", "b", "c"} {
			if _, err := Spawn(sys, name, idle[int]()); err != nil {
				t.Fatal(err)
			}
		}
		errs := make(chan error, 10)
		start := make(chan struct{})
		for range 10 {
			go func() {
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				errs <- sys.Shutdown(ctx)
			}()
		}
		close(start)
		for range 10 {
			if err := recv(t, errs); err != nil {
				t.Errorf("concurrent Shutdown = %v, want nil", err)
			}
		}
		begins, dones := shutdownPhases(t, rec.Records())
		if len(begins) != 1 || len(dones) != 1 {
			t.Errorf("%d phase=begin and %d phase=done records, want one of each", len(begins), len(dones))
		}
	})

	t.Run("d manual", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		a, err := Spawn(sys, "a", actorOf[int](&lifeActor[int]{started: func(ctx *Context[int]) {
			if _, err := Spawn(ctx, "a1", idle[int]()); err != nil {
				t.Errorf("Spawn a1: %v", err)
			}
		}}))
		if err != nil {
			t.Fatal(err)
		}
		b, err := Spawn(sys, "b", idle[int]())
		if err != nil {
			t.Fatal(err)
		}
		s, err := spawnSystem(sys, "s", idle[int]())
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()

		if err := sys.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if n := len(recordsOf(eng.Records(), "lockstep.stop")); n != 0 {
			t.Errorf("%d lockstep.stop records before RunUntilIdle, want 0", n)
		}
		if eng.Stopped() {
			t.Error("the manual engine stopped before RunUntilIdle")
		}
		eng.RunUntilIdle()
		var stopped []string
		for _, r := range recordsOf(eng.Records(), "lockstep.stop") {
			stopped = append(stopped, r[0])
		}
		want := []string{"actor=" + b.String(), "actor=lockstep://local/user/a/a1#", "actor=" + a.String(), "actor=" + s.String()}
		if len(stopped) != len(want) {
			t.Fatalf("lockstep.stop actors %q, want %q", stopped, want)
		}
		for i := range want {
			if !strings.HasPrefix(stopped[i], want[i]) {
				t.Errorf("lockstep.stop actors %q, want %q", stopped, want)
				break
			}
		}
		if _, dones := shutdownPhases(t, eng.Records()); len(dones) != 1 {
			t.Errorf("%d phase=done records, want 1", len(dones))
		}
		if !eng.Stopped() {
			t.Error("the manual engine is not stopped after RunUntilIdle")
		}
		if err := sys.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown after the stop = %v, want nil", err)
		}
	})
}

// AT-ACT-08: ACT-050, ACT-070. From the start of a shutdown every Spawn returns ErrStopped; after
// it, Lookup finds nothing and no goroutine of the system is left.
func TestATACT08AfterShutdown(t *testing.T) {
	sys := newLocalSystem(t)
	lateSpawn := make(chan error, 1)
	if _, err := Spawn(sys, "y", actorOf[any](&lifeActor[any]{stopped: func(ctx *Context[any], _ StopReason) {
		_, err := Spawn(ctx, "late", idle[int]())
		lateSpawn <- err
	}})); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	if _, err := Spawn(sys, "x", actorOf[any](&lifeActor[any]{stopped: func(*Context[any], StopReason) {
		entered <- struct{}{}
		<-release
	}})); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() { result <- sys.Shutdown(context.Background()) }()
	recv(t, entered)
	if _, err := Spawn(sys, "during", idle[int]()); !isStoppedErr(sys, err) {
		t.Errorf("Spawn from the system during the shutdown = %v, want ErrStopped with text %q", err, stoppedText(sys))
	}
	close(release)
	if err := recv(t, result); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	if err := recv(t, lateSpawn); !errors.Is(err, ErrStopped) {
		t.Errorf("Spawn from a Stopped hook during the shutdown = %v, want ErrStopped", err)
	}

	if _, err := Spawn(sys, "x", idle[int]()); !isStoppedErr(sys, err) {
		t.Errorf("Spawn after the shutdown = %v, want ErrStopped with text %q", err, stoppedText(sys))
	}
	for _, path := range []string{"/user/x", "/user/y"} {
		if _, err := Lookup[any](sys, path); !errors.Is(err, ErrNotFound) {
			t.Errorf("Lookup(%s) after the shutdown = %v, want ErrNotFound", path, err)
		}
	}
	if n := sys.reg.Len(); n != 0 {
		t.Errorf("registry holds %d entries after the shutdown, want 0", n)
	}
	goleak.VerifyNone(t)
}

// AT-ENG-25 (Shutdown clause): ACT-060, R41. Shutdown inside Receive on the manual engine returns
// nil at once without a failure; RunUntilIdle then stops the system and the engine.
func TestATENG25ShutdownInReceive(t *testing.T) {
	sys, eng := newManualSystem(t)
	var got error
	var inTurn, called bool
	pid, err := Spawn(sys, "a", actorOf[int](&lifeActor[int]{receive: func(*Context[int], int) {
		inTurn = sys.eng.Scheduler.InTurn()
		got = sys.Shutdown(context.Background())
		called = true
	}}))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, pid, 1)
	eng.RunUntilIdle()
	if !called || !inTurn || got != nil {
		t.Errorf("Shutdown in Receive (in turn %v) = %v, want nil", inTurn, got)
	}
	if f := eng.Failures(); len(f) != 0 {
		t.Errorf("Failures() = %v, want none", f)
	}
	if !eng.Stopped() {
		t.Error("the engine is not stopped after RunUntilIdle")
	}
	if _, dones := shutdownPhases(t, eng.Records()); len(dones) != 1 {
		t.Errorf("%d phase=done records, want 1", len(dones))
	}
}

// AT-MSG-04 (stopped clause): MSG-100. Send from a stopped system returns ErrStopped.
func TestATMSG04SendFromStoppedSystem(t *testing.T) {
	sys := newLocalSystem(t)
	acct := spawnAcct(t, sys, nil)
	shutdownWithin(t, sys)
	if err := Send(sys, acct, acctMsg(Deposit{1})); !isStoppedErr(sys, err) {
		t.Errorf("Send after the shutdown = %v, want ErrStopped with text %q", err, stoppedText(sys))
	}
	// The zero PID is checked first (MSG-100's order).
	if err := Send(sys, PID[acctMsg]{}, acctMsg(Deposit{1})); !errors.Is(err, ErrNotFound) {
		t.Errorf("Send to the zero PID after the shutdown = %v, want ErrNotFound", err)
	}
}

// Done is the message AT-MSG-07's Stopped hook sends.
type Done struct{}

// AT-MSG-07: MSG-100. A Stopped hook sends during the shutdown; the collector, spawned before it,
// stops after it and receives the message.
func TestATMSG07SendDuringShutdown(t *testing.T) {
	sys := newLocalSystem(t)
	received := make(chan any, 1)
	collector, err := Spawn(sys, "collector", actorOf[any](&lifeActor[any]{receive: func(_ *Context[any], m any) {
		received <- m
	}}))
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	if _, err := Spawn(sys, "sender", actorOf[int](&lifeActor[int]{stopped: func(ctx *Context[int], _ StopReason) {
		sent <- Send(ctx, collector, any(Done{}))
	}})); err != nil {
		t.Fatal(err)
	}
	shutdownWithin(t, sys)
	if err := recv(t, sent); err != nil {
		t.Errorf("Send from a Stopped hook during the shutdown = %v, want nil", err)
	}
	select {
	case m := <-received:
		if m != (Done{}) {
			t.Errorf("collector received %v, want Done{}", m)
		}
	default:
		t.Error("the collector did not receive Done{} during the shutdown")
	}
}

// AT-MSG-11 (stopped clause): MSG-300 step 3. Ask from a stopped system returns a reply already
// resolved with ErrStopped, without calling build, allocating or recording.
func TestATMSG11AskFromStoppedSystem(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	acct := spawnAcct(t, sys, nil)
	shutdownWithin(t, sys)
	counter := sys.asks.counter
	before := len(rec.Records())
	built := false
	r := Ask(sys, acct, func(rt ReplyTo[int64]) acctMsg { built = true; return GetBalance{rt} })
	if !r.resolved.Load() {
		t.Fatal("Ask from a stopped system returned a pending reply")
	}
	if _, err := r.Await(context.Background()); !isStoppedErr(sys, err) {
		t.Errorf("Await = %v, want ErrStopped with text %q", err, stoppedText(sys))
	}
	if built || sys.asks.counter != counter || len(rec.Records()) != before {
		t.Errorf("built %v, ask counter %d (was %d), %d new records: want no build, no allocation, no record",
			built, sys.asks.counter, counter, len(rec.Records())-before)
	}
	// An invalid option and the zero PID come first (MSG-300 step 3's order).
	if _, err := Ask(sys, acct, getBalance, WithTimeout(0)).Await(context.Background()); !errors.Is(err, ErrInvalidOption) {
		t.Errorf("Ask with WithTimeout(0) after the shutdown = %v, want ErrInvalidOption", err)
	}
	if _, err := Ask(sys, PID[acctMsg]{}, getBalance).Await(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Errorf("Ask to the zero PID after the shutdown = %v, want ErrNotFound", err)
	}
}

// AT-MSG-18: MSG-110, MSG-360, MSG-380. The pending asks of a system resolve with ErrStopped when
// it stops, in ask-counter order, each with a lockstep.reply record from the system address.
func TestATMSG18StopOfAskerSystem(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	other := PIDAt[int]("other", "/user/t")
	if _, err := Ask(sys, other, func(ReplyTo[int]) int { return 1 }).Await(cancelled()); !errors.Is(err, ErrTerminated) {
		t.Errorf("Ask to another node without a transport = %v, want ErrTerminated at once", err)
	}
	_, r1 := NewReply[int](sys, WithTimeout(time.Hour))
	_, r2 := NewReply[string](sys, WithTimeout(time.Hour))
	shutdownWithin(t, sys)
	for i, err := range []error{awaitErr(r1), awaitErr(r2)} {
		if !isStoppedErr(sys, err) {
			t.Errorf("reply %d: Await = %v, want ErrStopped with text %q", i+1, err, stoppedText(sys))
		}
	}
	recs := rec.Records()
	var replies [][]string
	for _, a := range recordsOf(recs, "lockstep.reply") {
		if slices.Contains(a, "from="+sys.user.String()) {
			replies = append(replies, a)
		}
	}
	errAttr := "err=" + stoppedText(sys)
	want := [][]string{
		{"ask=" + r1.ask.String(), "from=" + sys.user.String(), "outcome=resolved", errAttr},
		{"ask=" + r2.ask.String(), "from=" + sys.user.String(), "outcome=resolved", errAttr},
	}
	if !slices.EqualFunc(replies, want, slices.Equal) {
		t.Errorf("lockstep.reply records from the system = %q, want %q", replies, want)
	}
	_, dones := shutdownPhases(t, recs)
	if last := recordIndex(recs, "lockstep.reply", "ask="+r2.ask.String()); len(dones) != 1 || last > dones[0] {
		t.Errorf("the resolutions (last at %d) do not precede phase=done (%v)", last, dones)
	}
	if n := sys.asks.len(); n != 0 {
		t.Errorf("%d asks pending after the shutdown, want 0", n)
	}
}

// awaitErr returns the error r resolved with; r must be resolved.
func awaitErr[R any](r *Reply[R]) error {
	_, err := r.Await(cancelled())
	return err
}

// AT-MSG-35 (shutdown clause): MSG-380. NewReply on a stopped system returns the zero ReplyTo and
// a reply already resolved with ErrStopped, allocating and recording nothing.
func TestATMSG35NewReplyAfterShutdown(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	shutdownWithin(t, sys)
	counter := sys.asks.counter
	before := len(rec.Records())
	rt, r := NewReply[int](sys, WithTimeout(time.Hour))
	if rt != (ReplyTo[int]{}) {
		t.Errorf("NewReply after the shutdown returned ReplyTo %v, want the zero ReplyTo", rt)
	}
	if !r.resolved.Load() {
		t.Fatal("NewReply after the shutdown returned a pending reply")
	}
	if err := awaitErr(r); !isStoppedErr(sys, err) {
		t.Errorf("Await = %v, want ErrStopped with text %q", err, stoppedText(sys))
	}
	if sys.asks.counter != counter || len(rec.Records()) != before {
		t.Errorf("ask counter %d (was %d), %d new records: want no allocation and no record",
			sys.asks.counter, counter, len(rec.Records())-before)
	}
	// An invalid option comes first (MSG-380's order).
	if _, r := NewReply[int](sys, WithTimeout(-time.Second)); !errors.Is(awaitErr(r), ErrInvalidOption) {
		t.Errorf("NewReply with an invalid option after the shutdown = %v, want ErrInvalidOption", awaitErr(r))
	}
}

// MSG-410 (stopped clause), MSG-340: during the shutdown, reply attempts flow, the stop's
// Fail(ErrTerminated) among them; once the system is stopped, a reply attempt emits no record and
// reaches no transport.
func TestReplyAttemptFromStoppedSystem(t *testing.T) {
	eng := enginetest.New("local", 1)
	sys, err := newSystem(eng.Engine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetHost(host{sys})
	silent, stored := spawnSilent(t, sys)
	eng.RunUntilIdle()

	// A local ask and one from node n2, both tracked by silent when the shutdown begins.
	local := askSilent(sys, silent, WithTimeout(time.Hour))
	eng.Inbound(engine.Envelope{
		From:    remoteSender.engine(),
		To:      silent.Address().engine(),
		Payload: userPayload{msg: ReplyTo[int]{addr: n2Ask}, ask: n2Ask, typ: typeOf[any]()},
	})
	eng.RunUntilIdle()
	<-stored
	<-stored
	if err := sys.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	delivered := len(eng.Delivered())
	eng.RunUntilIdle()

	if err := awaitErr(local); !errors.Is(err, ErrTerminated) {
		t.Errorf("local ask tracked by silent: Await = %v, want ErrTerminated (MSG-340 during the shutdown)", err)
	}
	if n := len(eng.Delivered()) - delivered; n != 1 {
		t.Fatalf("%d envelopes delivered during the shutdown, want n2's ErrTerminated", n)
	}
	if _, p := lastReply(t, eng); !errors.Is(p.err, ErrTerminated) {
		t.Errorf("n2's reply payload %+v, want ErrTerminated", p)
	}

	if !eng.Stopped() {
		t.Fatal("the system did not stop")
	}
	recs, delivered := len(eng.Records()), len(eng.Delivered())
	ReplyTo[int]{addr: n2Ask}.Send(sys, 1)
	ReplyTo[int]{addr: n2Ask}.Fail(sys, errX)
	ReplyTo[int]{addr: local.ask}.Send(sys, 2)
	if n := len(eng.Records()) - recs; n != 0 {
		t.Errorf("%d records from reply attempts of a stopped system, want none", n)
	}
	if n := len(eng.Delivered()) - delivered; n != 0 {
		t.Errorf("%d envelopes delivered by a stopped system, want none", n)
	}
	// The zero ReplyTo is checked before the stopped clause (MSG-410's order).
	ReplyTo[int]{}.Send(sys, 3)
	if got := recordsOf(eng.Records()[recs:], "lockstep.reply"); len(got) != 1 || !slices.Contains(got[0], "outcome=zero") {
		t.Errorf("records of a zero ReplyTo on a stopped system = %q, want one outcome=zero", got)
	}
}

// ACT-050: a Spawn from the system that races with Shutdown either fails with ErrStopped or
// registers a child that the shutdown stops; none is left registered (the closing check is atomic
// with the registration).
func TestShutdownRacesSpawn(t *testing.T) {
	for round := range 20 {
		sys := newLocalSystem(t)
		var ok, refused atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for g := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := range 50 {
					_, err := Spawn(sys, "", idle[int]())
					switch {
					case err == nil:
						ok.Add(1)
					case errors.Is(err, ErrStopped):
						refused.Add(1)
					default:
						t.Errorf("round %d goroutine %d spawn %d: %v", round, g, i, err)
					}
				}
			}()
		}
		close(start)
		shutdownWithin(t, sys)
		wg.Wait()
		if n := sys.reg.Len(); n != 0 {
			t.Fatalf("round %d: registry holds %d entries after the shutdown (%d spawned, %d refused), want 0",
				round, n, ok.Load(), refused.Load())
		}
	}
}

// ACT-050, ACT-060, ENG-140: a system without actors stops at once, on either engine.
func TestShutdownWithoutActors(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		sys, rec := newRecordingSystem(t)
		shutdownWithin(t, sys)
		if !sys.shut.engStopped.Load() {
			t.Error("the engine is not stopped")
		}
		if begins, dones := shutdownPhases(t, rec.Records()); len(begins) != 1 || len(dones) != 1 {
			t.Errorf("phase=begin %v, phase=done %v, want one of each", begins, dones)
		}
	})
	t.Run("manual", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		if err := sys.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if eng.Stopped() {
			t.Error("the manual engine stopped before RunUntilIdle (ACT-060: the stops run in engine callbacks)")
		}
		eng.RunUntilIdle()
		if !eng.Stopped() {
			t.Error("the manual engine is not stopped after RunUntilIdle")
		}
		if err := sys.Shutdown(cancelled()); err != nil {
			t.Errorf("Shutdown of a stopped system with a cancelled ctx = %v, want nil", err)
		}
	})
}

// ACT-060: a later call during the turn that completes the shutdown on the local engine returns
// nil without waiting, and a stopped system's Shutdown returns nil even with a cancelled ctx.
func TestShutdownCalledFromLastStopped(t *testing.T) {
	sys := newLocalSystem(t)
	got := make(chan error, 1)
	if _, err := spawnSystem(sys, "s", actorOf[int](&lifeActor[int]{stopped: func(*Context[int], StopReason) {
		got <- sys.Shutdown(context.Background())
	}})); err != nil {
		t.Fatal(err)
	}
	shutdownWithin(t, sys)
	if err := recv(t, got); err != nil {
		t.Errorf("Shutdown from the last Stopped hook = %v, want nil", err)
	}
	if err := sys.Shutdown(cancelled()); err != nil {
		t.Errorf("Shutdown of a stopped system with a cancelled ctx = %v, want nil", err)
	}
}

// ACT-050, SUP-060: a guardian's child whose stop request was processed before its begin (another
// goroutine reached it between Registry.Add and the begin push) stops without an incarnation and
// still notifies the sweep, which completes the shutdown.
func TestShutdownChildStoppedBeforeBegin(t *testing.T) {
	sys, eng := newManualSystem(t)
	calls := 0
	pid, err := Spawn(sys, "x", func() Actor[int] { calls++; return &lifeActor[int]{} })
	if err != nil {
		t.Fatal(err)
	}
	c := cellOf(t, sys, pid)
	c.mu.Lock()
	c.sysq = fifo[sysMsg]{}
	c.stopReq = true
	c.sysq.push(stop{reason: Normal})
	c.sysq.push(begin{})
	c.mu.Unlock()

	if err := sys.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if calls != 0 {
		t.Errorf("the factory ran %d times, want 0", calls)
	}
	want := [][]string{{"actor=" + pid.String(), "inc=0", "reason=normal", "err="}}
	if got := recordsOf(eng.Records(), "lockstep.stop"); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("lockstep.stop records = %q, want %q", got, want)
	}
	if !eng.Stopped() || !sys.stopped.Load() {
		t.Errorf("engine stopped %v, system stopped %v, want both", eng.Stopped(), sys.stopped.Load())
	}
}
