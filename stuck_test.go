// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/overload"
)

// stuckTimeout is the watchdog timeout of the stuck-actor tests, as in AT-OVL-37.
const stuckTimeout = 100 * time.Millisecond

// gate blocks the turns of a test actor: a turn that calls wait signals entered and returns once
// the gate opens. open may be called more than once; the test opens it in a cleanup too, before
// the system's shutdown, which waits for the turn.
type gate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGate(t *testing.T) *gate {
	t.Helper()
	g := &gate{entered: make(chan struct{}, 4), release: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *gate) wait() {
	g.entered <- struct{}{}
	<-g.release
}

func (g *gate) open() { g.once.Do(func() { close(g.release) }) }

// quietLogs captures slog's default logger for the test, so the stuck-turn reports of a system
// built after it do not reach the test output. It restores the standard log package's output and
// flags, which slog.SetDefault redirects, once the capture has ended.
func quietLogs(t *testing.T) {
	t.Helper()
	w, f := log.Writer(), log.Flags()
	t.Cleanup(func() {
		log.SetOutput(w)
		log.SetFlags(f)
	})
	captureDefaultLogger(t)
}

// stuckActor spawns, as /user/stuck, an actor whose message 1 waits at g and whose other messages
// go to got, and counts in builds the incarnations its factory builds. It sends message 1 and
// waits until the watchdog has marked the actor stuck.
func stuckActor(t *testing.T, sys *System, g *gate, builds *atomic.Int32, got chan<- int, opts ...SpawnOption) (PID[int], *cell[int]) {
	t.Helper()
	pid, err := Spawn(sys, "stuck", func() Actor[int] {
		builds.Add(1)
		return &fnActor[int]{receive: func(_ *Context[int], n int) {
			if n == 1 {
				g.wait()
				return
			}
			got <- n
		}}
	}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, pid, 1)
	recv(t, g.entered)
	c := cellOf(t, sys, pid)
	waitUntil(t, "the actor marked stuck", c.stuck)
	return pid, c
}

// stoppedAt reports whether no actor is registered at pid's path any more.
func stoppedAt[M any](sys *System, pid PID[M]) bool {
	_, _, ok := sys.reg.Lookup(pid.Address().Path())
	return !ok
}

// AT-OVL-38: OVL-520. A Send to a stuck FailSend actor is refused with OVL-520's error, which
// matches ErrMailboxFull and not ErrOverloaded. A Stop sent while it is stuck is queued, not lost:
// once the turn returns, the actor stops, and is not restarted.
func TestATOVL38SendToStuckActor(t *testing.T) {
	verifyNoneAtEnd(t)
	quietLogs(t)
	sys := newLocalSystem(t, WithWatchdog(stuckTimeout))
	g := newGate(t)
	var builds atomic.Int32
	got := make(chan int, 4)
	pid, _ := stuckActor(t, sys, g, &builds, got)

	err := Send(sys, pid, 2)
	want := "lockstep: send to " + pid.Address().String() + ": actor stuck, mailbox treated as full"
	if err == nil || err.Error() != want {
		t.Fatalf("Send to a stuck actor = %v, want %q", err, want)
	}
	if !errors.Is(err, ErrMailboxFull) || errors.Is(err, ErrOverloaded) {
		t.Errorf("errors.Is(err, ErrMailboxFull) = %v, errors.Is(err, ErrOverloaded) = %v, want true, false",
			errors.Is(err, ErrMailboxFull), errors.Is(err, ErrOverloaded))
	}
	if err := Stop(sys, pid.Address()); err != nil {
		t.Fatalf("Stop of a stuck actor = %v", err)
	}

	g.open()
	waitUntil(t, "the actor stopped", func() bool { return stoppedAt(sys, pid) })
	if n := builds.Load(); n != 1 {
		t.Errorf("the actor was built %d times, want once: the queued Stop must stop it without a restart", n)
	}
	select {
	case n := <-got:
		t.Errorf("the actor handled message %d, which was refused", n)
	default:
	}
}

// AT-OVL-39: OVL-530. When the stuck turn returns, the actor fails with an error matching
// ErrTimeout, its supervisor's decider gets that error, and the actor restarts.
func TestATOVL39StuckTurnFails(t *testing.T) {
	verifyNoneAtEnd(t)
	quietLogs(t)
	decided := make(chan error, 4)
	sys := newLocalSystem(t, WithWatchdog(stuckTimeout), WithGuardianStrategy(OneForOne(func(err error) Directive {
		decided <- err
		return DirectiveRestart
	})))
	g := newGate(t)
	var builds atomic.Int32
	got := make(chan int, 4)
	pid, c := stuckActor(t, sys, g, &builds, got)

	g.open()
	err := recv(t, decided)
	if err == nil || !errors.Is(err, ErrTimeout) {
		t.Fatalf("the decider got %v, want an error matching ErrTimeout", err)
	}
	prefix := "lockstep: actor " + pid.Address().String() + " stuck for "
	if !strings.HasPrefix(err.Error(), prefix) {
		t.Errorf("the decider got %q, want the prefix %q", err, prefix)
	}
	if running, perr := time.ParseDuration(strings.TrimSuffix(strings.TrimPrefix(err.Error(), prefix), ": "+ErrTimeout.Error())); perr != nil || running < stuckTimeout {
		t.Errorf("the running time in %q is not a duration of at least %v", err, stuckTimeout)
	}
	waitUntil(t, "incarnation 2", func() bool { return builds.Load() == 2 })
	send(t, sys, pid, 2) // the restarted actor runs, and is not stuck
	if n := recv(t, got); n != 2 {
		t.Fatalf("incarnation 2 handled %d, want 2", n)
	}
	if c.stuck() {
		t.Error("the actor is still marked stuck after its turn returned")
	}
	select {
	case err := <-decided:
		t.Errorf("a second failure: %v", err)
	default:
	}
}

// AT-OVL-40: OVL-540. Shutdown does not wait for a stuck turn past its context: with a 200 ms
// context it returns after about 200 ms with the context's error. Once the turn returns, the
// system finishes stopping.
func TestATOVL40ShutdownWhileStuck(t *testing.T) {
	verifyNoneAtEnd(t)
	quietLogs(t)
	sys := newLocalSystem(t, WithWatchdog(stuckTimeout))
	g := newGate(t)
	var builds atomic.Int32
	stuckActor(t, sys, g, &builds, make(chan int, 4))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	began := time.Now()
	err := sys.Shutdown(ctx)
	elapsed := time.Since(began)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown while stuck = %v, want context.DeadlineExceeded", err)
	}
	if elapsed < 200*time.Millisecond || elapsed >= 2*time.Second {
		t.Errorf("Shutdown while stuck returned after %v, want about 200ms", elapsed)
	}

	g.open()
	done, cancelDone := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDone()
	if err := sys.Shutdown(done); err != nil {
		t.Fatalf("Shutdown after the turn returned = %v", err)
	}
}

// OVL-520, OVL §11: a report marks the actor only while the reported turn runs. A report of a turn
// that has ended marks nothing, nor does a mark whose turn counter is not the counter of the turn
// in progress; a report of the turn in progress marks the actor until that turn returns.
func TestStuckMarkIgnoresEndedTurn(t *testing.T) {
	verifyNoneAtEnd(t)
	sys := newLocalSystem(t, WithWatchdog(0)) // the test plays the watchdog
	g := newGate(t)
	pid, err := Spawn(sys, "slow", actorOf[int](&fnActor[int]{receive: func(*Context[int], int) { g.wait() }}))
	if err != nil {
		t.Fatal(err)
	}
	c := cellOf(t, sys, pid)
	turns := &runningTurns{pe: sys.prod}
	turnOf := func() (overload.Turn, bool) {
		for _, tr := range turns.running(nil) {
			if tr.Actor == engine.ActorRef(c) {
				return tr, true
			}
		}
		return overload.Turn{}, false
	}

	send(t, sys, pid, 1)
	recv(t, g.entered)
	first, ok := turnOf()
	if !ok {
		t.Fatal("the actor's turn is not running")
	}
	firstSeq := c.turnSeq()
	g.release <- struct{}{} // ends the first turn only
	waitUntil(t, "the first turn ended", func() bool { return c.turnSeq() != firstSeq })
	turns.markStuck(first)
	if c.stuck() || c.stuckAt.Load() != nil {
		t.Fatal("a report of an ended turn marked the actor")
	}

	send(t, sys, pid, 2)
	recv(t, g.entered)
	waitUntil(t, "the second turn", func() bool { _, ok := turnOf(); return ok })
	// The counter now names the second turn: only the fresh snapshot, which no longer holds the
	// first turn's (Worker, Start), keeps a late report of the first turn from marking it.
	turns.markStuck(first)
	if c.stuck() || c.stuckAt.Load() != nil {
		t.Fatal("a report of an ended turn marked the turn in progress")
	}
	c.markStuck(firstSeq, first.Start)
	if c.stuck() || c.stuckAt.Load() != nil {
		t.Fatal("a mark with the counter of an ended turn was stored")
	}
	// A mark stored just after its turn ended, as a report racing with the turn's end can, makes
	// nothing stuck.
	c.stuckAt.Store(&stuckMark{seq: firstSeq, start: first.Start})
	if c.stuck() {
		t.Fatal("a mark of an ended turn made the actor stuck")
	}

	second, _ := turnOf()
	turns.markStuck(second)
	if !c.stuck() {
		t.Fatal("a report of the turn in progress did not mark the actor")
	}
	g.open()
	waitUntil(t, "the mark cleared", func() bool { return !c.stuck() && c.stuckAt.Load() == nil })
}

// OVL-520, OVL-530, MBX-075: an outside sender to a stuck BlockSender actor waits; when the turn
// returns, the mark clears and the waiting message is enqueued, and the restarted actor gets it.
func TestStuckBlockSenderWaits(t *testing.T) {
	verifyNoneAtEnd(t)
	quietLogs(t)
	sys := newLocalSystem(t, WithWatchdog(stuckTimeout))
	g := newGate(t)
	var builds atomic.Int32
	got := make(chan int, 4)
	pid, c := stuckActor(t, sys, g, &builds, got, WithMailbox(8, BlockSender))

	sent := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		sent <- SendContext(ctx, sys, pid, 2)
	}()
	waitUntil(t, "the sender waits", func() bool { return c.user.Waiting() == 1 })
	select {
	case err := <-sent:
		t.Fatalf("a send to a stuck BlockSender actor returned %v while it was stuck", err)
	case <-time.After(50 * time.Millisecond):
	}

	g.open()
	if err := recv(t, sent); err != nil {
		t.Fatalf("the waiting send = %v, want nil", err)
	}
	if n := recv(t, got); n != 2 {
		t.Fatalf("the actor handled %d, want 2", n)
	}
	if n := builds.Load(); n != 2 {
		t.Errorf("the actor was built %d times, want 2: the stuck turn fails it", n)
	}
}

// OVL-530: a stuck Stopped is not a handler of the live incarnation: when it returns, the mark
// clears and the stop goes on, with no failure for the supervisor to decide.
func TestStuckStoppedOnlyClears(t *testing.T) {
	verifyNoneAtEnd(t)
	quietLogs(t)
	decided := make(chan error, 4)
	sys := newLocalSystem(t, WithWatchdog(stuckTimeout), WithGuardianStrategy(OneForOne(func(err error) Directive {
		decided <- err
		return DirectiveRestart
	})))
	g := newGate(t)
	pid, err := Spawn(sys, "stopping", actorOf[int](&gatedStopper{g: g}))
	if err != nil {
		t.Fatal(err)
	}
	if err := Stop(sys, pid.Address()); err != nil {
		t.Fatal(err)
	}
	recv(t, g.entered)
	c := cellOf(t, sys, pid)
	waitUntil(t, "the actor marked stuck", c.stuck)
	g.open()
	waitUntil(t, "the actor stopped", func() bool { return stoppedAt(sys, pid) })
	if c.stuck() || c.stuckAt.Load() != nil {
		t.Error("the mark is left after the stuck Stopped returned")
	}
	select {
	case err := <-decided:
		t.Errorf("the supervisor decided a failure: %v", err)
	default:
	}
}

// gatedStopper is an actor whose Stopped waits at g.
type gatedStopper struct{ g *gate }

func (*gatedStopper) Receive(*Context[int], int)          {}
func (a *gatedStopper) Stopped(*Context[int], StopReason) { a.g.wait() }

// OVL-520, OVL §8: a message refused because its actor is stuck is recorded as lockstep.overload
// event=shed with reason stuck, not as lockstep.mailbox.full; system messages are still queued.
// The test marks the turn in progress itself, as the watchdog's adapter does.
func TestStuckShedRecord(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	g := newGate(t)
	pid, err := Spawn(sys, "stuck", actorOf[int](&fnActor[int]{receive: func(*Context[int], int) { g.wait() }}),
		WithMailbox(4, DropNewest))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, pid, 1)
	recv(t, g.entered)
	c := cellOf(t, sys, pid)
	c.markStuck(c.turnSeq(), time.Now())
	if !c.stuck() {
		t.Fatal("the mark of the turn in progress did not make the actor stuck")
	}
	if err := Send(sys, pid, 2); err != nil {
		t.Fatalf("Send to a stuck DropNewest actor = %v, want nil", err)
	}
	if err := Stop(sys, pid.Address()); err != nil {
		t.Fatal(err)
	}
	g.open()
	waitUntil(t, "the actor stopped", func() bool { return stoppedAt(sys, pid) })

	sameRecords(t, recordsOf(rec.Records(), "lockstep.overload"), "lockstep.overload",
		[]string{"event=shed", "from=" + sys.user.String(), "to=" + pid.Address().String(), "reason=stuck",
			"policy=drop-newest", "dropped=new"},
	)
	if full := recordsOf(rec.Records(), "lockstep.mailbox.full"); len(full) != 0 {
		t.Errorf("lockstep.mailbox.full records %q, want none", full)
	}
}

// OVL-530: a stuck handler that then fails itself fails the incarnation once, with its own
// failure: the supervisor's decider gets the panic, not the stuck failure.
func TestStuckHandlerFailureWins(t *testing.T) {
	verifyNoneAtEnd(t)
	quietLogs(t)
	decided := make(chan error, 4)
	sys := newLocalSystem(t, WithWatchdog(stuckTimeout), WithGuardianStrategy(OneForOne(func(err error) Directive {
		decided <- err
		return DirectiveRestart
	})))
	g := newGate(t)
	var builds atomic.Int32
	pid, err := Spawn(sys, "stuck", func() Actor[int] {
		builds.Add(1)
		return &fnActor[int]{receive: func(_ *Context[int], n int) {
			if n == 1 {
				g.wait()
				panic("boom")
			}
		}}
	})
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, pid, 1)
	recv(t, g.entered)
	c := cellOf(t, sys, pid)
	waitUntil(t, "the actor marked stuck", c.stuck)

	g.open()
	err = recv(t, decided)
	var pe *PanicError
	if !errors.As(err, &pe) || errors.Is(err, ErrTimeout) {
		t.Fatalf("the decider got %v, want the handler's panic and not the stuck failure", err)
	}
	waitUntil(t, "incarnation 2", func() bool { return builds.Load() == 2 })
	if c.stuck() || c.stuckAt.Load() != nil {
		t.Error("the mark is left after the stuck handler failed")
	}
	select {
	case err := <-decided:
		t.Errorf("a second failure: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}
