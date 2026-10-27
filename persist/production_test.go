// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/persist"
	"github.com/hmdsefi/lockstep/persist/filestore"
)

// The counter fixture of the production-engine tests: an EventSourced counter whose commands
// reply with the sum once their events are committed, spawned by a parent that restarts it after
// every failure and keeps the errors. The same actor code runs in simulation (package sim's tests).

// CounterCmd is the counter's command type.
type CounterCmd interface{ counterCmd() }

// Add adds N and replies with the new sum.
type Add struct {
	N       int64
	ReplyTo lockstep.ReplyTo[int64]
}

// Get replies with the sum at once and persists nothing.
type Get struct{ ReplyTo lockstep.ReplyTo[int64] }

func (Add) counterCmd() {}
func (Get) counterCmd() {}

// Added is the counter's event.
type Added struct{ N int64 }

type counterCtx = persist.Context[CounterCmd, Added, int64]

// counter is the counter's EventSourced; its state is the sum.
type counter struct{}

func (counter) Initial() int64 { return 0 }

func (counter) Handle(ctx *counterCtx, sum int64, cmd CounterCmd) []Added {
	switch m := cmd.(type) {
	case Add:
		ctx.Then(func(ctx *counterCtx, sum int64) { m.ReplyTo.Send(ctx, sum) })
		return []Added{{N: m.N}}
	case Get:
		m.ReplyTo.Send(ctx, sum)
	}
	return nil
}

func (counter) Apply(sum int64, e Added) int64 { return sum + e.N }

func add(n int64) func(lockstep.ReplyTo[int64]) CounterCmd {
	return func(rt lockstep.ReplyTo[int64]) CounterCmd { return Add{N: n, ReplyTo: rt} }
}

func get(rt lockstep.ReplyTo[int64]) CounterCmd { return Get{ReplyTo: rt} }

// errLog is the parent's decider: it keeps the errors it sees, restarts the counter while it holds
// fewer than three, and then stops it. The decider runs on the parent's goroutine, the tests read
// from theirs.
type errLog struct {
	mu   sync.Mutex
	errs []error
}

func (l *errLog) decide(err error) lockstep.Directive {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, err)
	if len(l.errs) < 3 {
		return lockstep.DirectiveRestart
	}
	return lockstep.DirectiveStop
}

func (l *errLog) snapshot() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.errs...)
}

// await polls until the log holds at least n errors or the deadline passes.
func (l *errLog) await(tt testing.TB, n int) []error {
	tt.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if errs := l.snapshot(); len(errs) >= n {
			return errs
		}
		if time.Now().After(deadline) {
			tt.Fatalf("parent saw %d errors, want %d", len(l.snapshot()), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// parentActor spawns the counter in its Started hook.
type parentActor struct {
	started func(ctx *lockstep.Context[struct{}])
}

func (p parentActor) Started(ctx *lockstep.Context[struct{}])     { p.started(ctx) }
func (parentActor) Receive(*lockstep.Context[struct{}], struct{}) {}

// prodCounter is a production system with a persistent counter of persistence ID c-1 on journal.
type prodCounter struct {
	sys  *lockstep.System
	pid  lockstep.PID[CounterCmd]
	errs *errLog
	once sync.Once
	t    testing.TB
}

// newProdCounter starts a lockstep.New system with the counter under a parent that restarts it,
// and shuts the system down in t.Cleanup.
func newProdCounter(t *testing.T, journal persist.Journal) *prodCounter {
	t.Helper()
	p := &prodCounter{sys: lockstep.New(), errs: &errLog{}, t: t}
	t.Cleanup(p.shutdown)
	spawned := make(chan error, 1)
	_, err := lockstep.Spawn(p.sys, "parent", func() lockstep.Actor[struct{}] {
		return parentActor{started: func(ctx *lockstep.Context[struct{}]) {
			pid, err := persist.Spawn(ctx, "counter", "c-1",
				func() persist.EventSourced[CounterCmd, Added, int64] { return counter{} },
				persist.Config[Added, int64]{Journal: journal, Events: persist.JSON[Added]()})
			p.pid = pid
			spawned <- err
		}}
	}, lockstep.WithStrategy(lockstep.OneForOne(p.errs.decide)))
	if err != nil {
		t.Fatalf("Spawn parent: %v", err)
	}
	select {
	case err := <-spawned:
		if err != nil {
			t.Fatalf("persist.Spawn: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the parent did not spawn the counter")
	}
	return p
}

// shutdown shuts the system down once.
func (p *prodCounter) shutdown() {
	p.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.sys.Shutdown(ctx); err != nil {
			p.t.Errorf("Shutdown: %v", err)
		}
	})
}

// ask asks the counter with build and returns the reply or its error.
func (p *prodCounter) ask(build func(lockstep.ReplyTo[int64]) CounterCmd, opts ...lockstep.AskOption) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return lockstep.Ask(p.sys, p.pid, build, opts...).Await(ctx)
}

// commitFails is a Journal whose Commit returns err.
type commitFails struct {
	persist.Journal
	err error
}

func (j commitFails) Commit(context.Context, string) error { return j.err }

// A journal whose Commit fails puts the same error text in the parent's decider on the production
// engine as in simulation, and the command that caused it gets no reply.
func TestATPER25ProductionCommitError(t *testing.T) {
	store, err := filestore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	errX := errors.New("errX")
	p := newProdCounter(t, commitFails{Journal: store, err: errX})
	if _, err := p.ask(add(7), lockstep.WithTimeout(50*time.Millisecond)); !errors.Is(err, lockstep.ErrTimeout) {
		t.Errorf("Ask(Add{7}): %v, want ErrTimeout", err)
	}
	errs := p.errs.await(t, 1)
	const want = "persist: c-1: commit seq 1..1: errX"
	if got := errs[0]; got.Error() != want || !errors.Is(got, errX) {
		t.Errorf("error %q (errors.Is errX: %v), want %q", got, errors.Is(got, errX), want)
	}
}

// A second system on the same directory recovers the sum from the stored events and goes on.
func TestPersistProductionRecovery(t *testing.T) {
	dir := t.TempDir()
	// A directory has one Store: the first system and its Store are gone before the second opens it.
	func() {
		store, err := filestore.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		first := newProdCounter(t, store)
		for i := int64(1); i <= 3; i++ {
			if got, err := first.ask(add(1)); err != nil || got != i {
				t.Fatalf("Add{1} #%d: %d, %v, want %d", i, got, err, i)
			}
		}
		first.shutdown()
		if errs := first.errs.snapshot(); len(errs) != 0 {
			t.Fatalf("errors in the first system: %v", errs)
		}
	}()

	reopened, err := filestore.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	second := newProdCounter(t, reopened)
	if got, err := second.ask(get); err != nil || got != 3 {
		t.Fatalf("Get after recovery: %d, %v, want 3", got, err)
	}
	if got, err := second.ask(add(1)); err != nil || got != 4 {
		t.Fatalf("Add{1} after recovery: %d, %v, want 4", got, err)
	}
	if errs := second.errs.snapshot(); len(errs) != 0 {
		t.Errorf("errors in the second system: %v", errs)
	}
}
