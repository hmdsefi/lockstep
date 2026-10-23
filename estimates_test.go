// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// sized is a message that reports itself as that many bytes.
type sized int

func (s sized) Size() int { return int(s) }

// sizeCounter is a message whose Size counts its calls.
type sizeCounter struct{ calls *int }

func (m sizeCounter) Size() int { *m.calls++; return 1 }

// sizePanics is a message whose Size panics.
type sizePanics struct{}

func (sizePanics) Size() int { panic("size boom") }

// memProbe is an actor that counts its Receive turns and reports the count as its state size; its
// MemSize panics when the count reaches panicAt (0: never). A fixed size, when set, replaces the
// count. calls counts the MemSize calls of every incarnation that shares it.
type memProbe struct {
	turns   int
	panicAt int
	fixed   int
	calls   *int
	onMsg   func(ctx *Context[any], msg any)
}

func (a *memProbe) Receive(ctx *Context[any], msg any) {
	a.turns++
	if a.onMsg != nil {
		a.onMsg(ctx, msg)
	}
}

func (a *memProbe) MemSize() int {
	if a.calls != nil {
		*a.calls++
	}
	if a.panicAt > 0 && a.turns == a.panicAt {
		panic("memsize boom")
	}
	if a.fixed != 0 {
		return a.fixed
	}
	return a.turns
}

// spawnMem spawns a memProbe named name whose every incarnation is a new memProbe built by mk.
func spawnMem(t *testing.T, parent Spawner, name string, mk func() *memProbe) PID[any] {
	t.Helper()
	pid, err := Spawn(parent, name, func() Actor[any] { return mk() })
	if err != nil {
		t.Fatalf("Spawn %q: %v", name, err)
	}
	return pid
}

// estimateOf returns the entry of Estimates for addr, failing the test if there is none.
func estimateOf(t *testing.T, sys *System, addr Address) ActorEstimate {
	t.Helper()
	for _, e := range sys.Estimates(1 << 20) {
		if e.Addr == addr {
			return e
		}
	}
	t.Fatalf("Estimates has no entry for %v", addr)
	return ActorEstimate{}
}

// wantBytes fails the test unless addr's estimate is mailbox and state bytes.
func wantBytes(t *testing.T, when string, sys *System, addr Address, mailbox, state int64) {
	t.Helper()
	e := estimateOf(t, sys, addr)
	if e.MailboxBytes != mailbox || e.StateBytes != state {
		t.Errorf("%s: %v has MailboxBytes %d, StateBytes %d; want %d, %d", when, addr, e.MailboxBytes, e.StateBytes, mailbox, state)
	}
}

// AT-OVL-34: OVL-410. A Size panic reaches the Send caller from a *System and holds no lock
// meanwhile; from an actor it fails the sending turn like any panic in Receive. An Ask request
// whose Size panics leaves no pending ask.
func TestATOVL34SizePanics(t *testing.T) {
	t.Run("from system", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithMemoryEstimates())
		var got []any
		pid, err := Spawn(sys, "b", actorOf[any](&probe[any]{receive: func(_ *Context[any], m any) { got = append(got, m) }}))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		if v := mustPanic(t, func() { _ = Send(sys, pid, any(sizePanics{})) }); v != "size boom" {
			t.Errorf("Send panicked with %v, want size boom", v)
		}
		// Nothing was enqueued or recorded, and no mailbox lock is left held.
		if n := cellOf(t, sys, pid).user.Len(); n != 0 {
			t.Errorf("the mailbox holds %d messages after the panic, want 0", n)
		}
		send(t, sys, pid, any("after"))
		eng.RunUntilIdle()
		if !slices.Equal(got, []any{"after"}) {
			t.Errorf("received %v, want [after]", got)
		}
		wantBytes(t, "after", sys, pid.Address(), 0, 0)
	})
	t.Run("from actor", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithMemoryEstimates(), WithGuardianStrategy(r0()))
		b, err := Spawn(sys, "b", idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		a := spawnMem(t, sys, "a", func() *memProbe {
			return &memProbe{onMsg: func(ctx *Context[any], _ any) {
				_ = Send(ctx, b, any(sizePanics{}))
			}}
		})
		eng.RunUntilIdle()
		send(t, sys, a, any("go"))
		eng.RunUntilIdle()
		sameRecords(t, recordsOf(eng.Records(), "lockstep.fail"), "lockstep.fail",
			[]string{"actor=" + a.String(), "inc=1", "source=receive", "err=lockstep: panic: size boom"})
		sameRecords(t, recordsOf(eng.Records(), "lockstep.restart"), "lockstep.restart",
			[]string{"actor=" + a.String(), "inc=1", "err=lockstep: panic: size boom"})
		wantBytes(t, "after", sys, b.Address(), 0, 0)
	})
	t.Run("ask", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithMemoryEstimates())
		pid, err := Spawn(sys, "b", idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		mustPanic(t, func() {
			Ask(sys, pid, func(ReplyTo[any]) any { return sizePanics{} })
		})
		if n := sys.asks.len(); n != 0 {
			t.Errorf("%d asks pending after the panic, want 0", n)
		}
	})
}

// AT-OVL-35: OVL-400, OVL-420. With estimates off MemSize is never called; with them on its result
// is the actor's StateBytes after Started and after every turn; a panic in it fails the
// incarnation as a panic in Receive would, and the new incarnation starts at 0.
func TestATOVL35MemSize(t *testing.T) {
	t.Run("off", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		calls := 0
		pid := spawnMem(t, sys, "m", func() *memProbe { return &memProbe{calls: &calls} })
		eng.RunUntilIdle()
		for range 3 {
			send(t, sys, pid, any("x"))
			eng.RunUntilIdle()
		}
		if calls != 0 {
			t.Errorf("MemSize called %d times with estimates off, want 0", calls)
		}
		if got := sys.Estimates(5); got != nil {
			t.Errorf("Estimates = %v with estimates off, want nil", got)
		}
	})
	t.Run("on", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithMemoryEstimates(), WithGuardianStrategy(r0()))
		calls := 0
		pid := spawnMem(t, sys, "m", func() *memProbe { return &memProbe{panicAt: 5, calls: &calls} })
		eng.RunUntilIdle()
		wantBytes(t, "after Started", sys, pid.Address(), 0, 0)
		if calls != 1 {
			t.Errorf("MemSize called %d times after Started, want 1", calls)
		}
		for turn := int64(1); turn <= 4; turn++ {
			send(t, sys, pid, any("x"))
			eng.RunUntilIdle()
			wantBytes(t, "after turn "+strconv.FormatInt(turn, 10), sys, pid.Address(), 0, turn)
		}
		send(t, sys, pid, any("x")) // the fifth turn: MemSize panics
		eng.RunUntilIdle()
		sameRecords(t, recordsOf(eng.Records(), "lockstep.fail"), "lockstep.fail",
			[]string{"actor=" + pid.String(), "inc=1", "source=receive", "err=lockstep: panic: memsize boom"})
		wantBytes(t, "new incarnation", sys, pid.Address(), 0, 0)
		if st := eng.Usage(); st.Bytes != 0 {
			t.Errorf("Usage().Bytes = %d after the restart, want 0", st.Bytes)
		}
		send(t, sys, pid, any("x"))
		eng.RunUntilIdle()
		wantBytes(t, "new incarnation's first turn", sys, pid.Address(), 0, 1)
	})
	t.Run("restart backoff", func(t *testing.T) {
		// The ended incarnation's bytes leave the estimate when it ends, not only when the next
		// incarnation measures.
		sys, eng := newManualSystem(t, WithMemoryEstimates(), WithGuardianStrategy(withBackoff(5*time.Second)))
		pid := spawnMem(t, sys, "m", func() *memProbe {
			return &memProbe{fixed: 40, onMsg: func(*Context[any], any) { panic("boom") }}
		})
		eng.RunUntilIdle()
		wantBytes(t, "before the failure", sys, pid.Address(), 0, 40)
		send(t, sys, pid, any("x"))
		eng.RunUntilIdle()
		wantBytes(t, "waiting for the restart", sys, pid.Address(), 0, 0)
		if got := eng.Usage().Bytes; got != 0 {
			t.Errorf("Usage().Bytes = %d while the restart waits, want 0", got)
		}
		eng.Advance(6 * time.Second)
		wantBytes(t, "new incarnation", sys, pid.Address(), 0, 40)
	})
	t.Run("negative", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithMemoryEstimates())
		pid := spawnMem(t, sys, "m", func() *memProbe { return &memProbe{fixed: -7} })
		eng.RunUntilIdle()
		wantBytes(t, "negative MemSize", sys, pid.Address(), 0, 0)
	})
}

// AT-OVL-33 on the manual engine: OVL-410. Actor A sends B, in one turn, messages with Size 10 and
// -5 and one without Sizer. B's MailboxBytes is 10 after A's turn (the others count 0), 0 after B's
// first turn, which dequeues the Size 10 message, and 0 at the end.
func TestOVL410MailboxBytes(t *testing.T) {
	sys, eng := newManualSystem(t, WithMemoryEstimates())
	b, err := Spawn(sys, "b", idle[any]())
	if err != nil {
		t.Fatal(err)
	}
	a, _ := spawnProbe(t, sys, "a", func(ctx *Context[any], _ any) {
		for _, m := range []any{sized(10), sized(-5), "plain"} {
			if err := Send(ctx, b, m); err != nil {
				t.Errorf("Send(%v): %v", m, err)
			}
		}
	})
	eng.RunUntilIdle()
	send(t, sys, a, any("go"))
	if !eng.Step() { // A's turn
		t.Fatal("no turn to run for A")
	}
	wantBytes(t, "after A's turn", sys, b.Address(), 10, 0)
	if got := eng.Usage().Bytes; got != 10 {
		t.Errorf("Usage().Bytes = %d, want 10", got)
	}
	if !eng.Step() { // B's first turn
		t.Fatal("no turn to run for B")
	}
	wantBytes(t, "after B's first turn", sys, b.Address(), 0, 0)
	eng.RunUntilIdle()
	wantBytes(t, "at the end", sys, b.Address(), 0, 0)
}

// OVL-410: a message that no mailbox took adds nothing, an evicted or drained one is subtracted,
// an Ask request and an arrival from another node count like a Send, and a message to another node
// is never sized.
func TestOVL410MailboxAccounting(t *testing.T) {
	t.Run("full and drop policies", func(t *testing.T) {
		for _, tc := range []struct {
			policy  MailboxPolicy
			refused bool
			want    int64 // bytes after sized(1), sized(2), sized(4) into a mailbox of 2
		}{
			{FailSend, true, 3},
			{DropNewest, false, 3},
			{DropOldest, false, 6},
		} {
			t.Run(tc.policy.String(), func(t *testing.T) {
				sys, eng := newManualSystem(t, WithMemoryEstimates())
				pid, err := Spawn(sys, "b", idle[any](), WithMailbox(2, tc.policy))
				if err != nil {
					t.Fatal(err)
				}
				eng.RunUntilIdle()
				release := Hold(sys, pid.Address())
				for _, n := range []sized{1, 2, 4} {
					err := Send(sys, pid, any(n))
					if n == 4 && tc.refused != (err != nil) {
						t.Errorf("third Send error = %v, want refused %v", err, tc.refused)
					}
				}
				wantBytes(t, "full mailbox", sys, pid.Address(), tc.want, 0)
				if got := eng.Usage().Bytes; got != tc.want {
					t.Errorf("Usage().Bytes = %d, want %d", got, tc.want)
				}
				release()
				eng.RunUntilIdle()
				wantBytes(t, "drained", sys, pid.Address(), 0, 0)
			})
		}
	})
	t.Run("final stop drains", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithMemoryEstimates())
		pid, err := Spawn(sys, "b", idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		release := Hold(sys, pid.Address())
		send(t, sys, pid, any(sized(5)))
		send(t, sys, pid, any(sized(6)))
		if got := eng.Usage().Bytes; got != 11 {
			t.Fatalf("Usage().Bytes = %d, want 11", got)
		}
		if err := Stop(sys, pid.Address()); err != nil {
			t.Fatal(err)
		}
		release()
		eng.RunUntilIdle()
		if got := eng.Usage(); got != (engine.Usage{}) {
			t.Errorf("Usage() = %+v after the stop, want zero", got)
		}
		if got := sys.Estimates(5); len(got) != 0 {
			t.Errorf("Estimates = %v after the stop, want none", got)
		}
	})
	t.Run("ask request", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithMemoryEstimates())
		pid, err := Spawn(sys, "b", idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		release := Hold(sys, pid.Address())
		Ask(sys, pid, func(ReplyTo[any]) any { return sized(9) })
		wantBytes(t, "request queued", sys, pid.Address(), 9, 0)
		release()
		eng.RunUntilIdle()
		wantBytes(t, "request popped", sys, pid.Address(), 0, 0)
	})
	t.Run("arrival", func(t *testing.T) {
		sys, eng := newManualSystem(t, WithMemoryEstimates())
		eng.SetHost(host{sys})
		pid, err := Spawn(sys, "b", idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		release := Hold(sys, pid.Address())
		eng.Inbound(inbound(pid.Address(), 1, userPayload{msg: sized(13)}))
		wantBytes(t, "arrival queued", sys, pid.Address(), 13, 0)
		release()
		eng.RunUntilIdle()
		wantBytes(t, "arrival popped", sys, pid.Address(), 0, 0)
	})
	t.Run("off and remote", func(t *testing.T) {
		calls := 0
		sys, eng := newManualSystem(t)
		pid, err := Spawn(sys, "b", idle[any]())
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		send(t, sys, pid, any(sizeCounter{&calls}))
		eng.RunUntilIdle()
		if calls != 0 {
			t.Errorf("Size called %d times with estimates off, want 0", calls)
		}
		on, eng2 := newManualSystem(t, WithMemoryEstimates())
		remote := PID[any]{addr: Address{node: "n2", path: "/user/x", uid: 3}}
		send(t, on, remote, any(sizeCounter{&calls}))
		eng2.RunUntilIdle()
		if calls != 0 {
			t.Errorf("Size called %d times for a message to another node, want 0", calls)
		}
	})
}

// AT-OVL-36 on the manual engine: OVL-430. Four actors with estimates 30, 50, 50 and 10, spawned
// out of address order: Estimates(3) is the two 50s by address order, then 30; Estimates(0) and
// estimates off give nil. The total counts mailbox and state bytes.
func TestOVL430EstimatesOrder(t *testing.T) {
	sys, eng := newManualSystem(t, WithMemoryEstimates())
	var pids []PID[any]
	for _, s := range []struct {
		name string
		size int
	}{{"s", 30}, {"b", 50}, {"a", 50}, {"d", 10}} {
		pids = append(pids, spawnMem(t, sys, s.name, func() *memProbe { return &memProbe{fixed: s.size} }))
	}
	eng.RunUntilIdle()
	want := func(idx ...int) []ActorEstimate {
		var out []ActorEstimate
		for _, i := range idx {
			out = append(out, ActorEstimate{Addr: pids[i].Address(), StateBytes: int64([]int{30, 50, 50, 10}[i])})
		}
		return out
	}
	if got, w := sys.Estimates(3), want(2, 1, 0); !slices.Equal(got, w) {
		t.Errorf("Estimates(3) = %v, want %v", got, w)
	}
	if got, w := sys.Estimates(10), want(2, 1, 0, 3); !slices.Equal(got, w) {
		t.Errorf("Estimates(10) = %v, want %v", got, w)
	}
	for _, n := range []int{0, -1} {
		if got := sys.Estimates(n); got != nil {
			t.Errorf("Estimates(%d) = %v, want nil", n, got)
		}
	}
	// Mailbox bytes add to the total: d (10 state) with 45 queued passes the 50s.
	release := Hold(sys, pids[3].Address())
	send(t, sys, pids[3], any(sized(45)))
	if got := sys.Estimates(1); len(got) != 1 || got[0].Addr != pids[3].Address() || got[0].MailboxBytes != 45 {
		t.Errorf("Estimates(1) = %v, want d with 45 mailbox bytes", got)
	}
	release()

	off, offEng := newManualSystem(t)
	spawnMem(t, off, "a", func() *memProbe { return &memProbe{fixed: 5} })
	offEng.RunUntilIdle()
	if got := off.Estimates(3); got != nil {
		t.Errorf("Estimates(3) with estimates off = %v, want nil", got)
	}
}

// OVL-430: the walk covers children and system actors, and an actor leaves Estimates when it stops.
func TestOVL430EstimatesWalk(t *testing.T) {
	sys, eng := newManualSystem(t, WithMemoryEstimates())
	var child PID[any]
	parent := spawnMem(t, sys, "p", func() *memProbe {
		return &memProbe{fixed: 1, onMsg: func(ctx *Context[any], _ any) {
			var err error
			child, err = Spawn(ctx, "c", func() Actor[any] { return &memProbe{fixed: 2} })
			if err != nil {
				t.Errorf("Spawn child: %v", err)
			}
		}}
	})
	eng.RunUntilIdle()
	send(t, sys, parent, any("spawn"))
	eng.RunUntilIdle()
	got := sys.Estimates(10)
	if len(got) != 2 || got[0].Addr != child.Address() || got[1].Addr != parent.Address() {
		t.Fatalf("Estimates = %v, want the child then the parent", got)
	}
	if err := Stop(sys, parent.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if got := sys.Estimates(10); len(got) != 0 {
		t.Errorf("Estimates = %v after the stop, want none", got)
	}
}

// OVL-400, OVL-420: the usage function bound to Admission reports the live actors and the sum of
// the estimates, and 0 bytes while estimates are off.
func TestUsageReportsBytes(t *testing.T) {
	sys, eng := newManualSystem(t, WithMemoryEstimates())
	a := spawnMem(t, sys, "a", func() *memProbe { return &memProbe{fixed: 100} })
	b := spawnMem(t, sys, "b", func() *memProbe { return &memProbe{fixed: 20} })
	eng.RunUntilIdle()
	release := Hold(sys, b.Address())
	send(t, sys, b, any(sized(3)))
	if got, want := eng.Usage(), (engine.Usage{Actors: 2, Bytes: 123}); got != want {
		t.Errorf("Usage() = %+v, want %+v", got, want)
	}
	release()
	eng.RunUntilIdle()
	if got, want := eng.Usage(), (engine.Usage{Actors: 2, Bytes: 120}); got != want {
		t.Errorf("Usage() = %+v, want %+v", got, want)
	}
	if err := Stop(sys, a.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if got, want := eng.Usage(), (engine.Usage{Actors: 1, Bytes: 20}); got != want {
		t.Errorf("Usage() after a stop = %+v, want %+v", got, want)
	}

	off, offEng := newManualSystem(t)
	spawnMem(t, off, "a", func() *memProbe { return &memProbe{fixed: 100} })
	offEng.RunUntilIdle()
	if got, want := offEng.Usage(), (engine.Usage{Actors: 1}); got != want {
		t.Errorf("Usage() with estimates off = %+v, want %+v", got, want)
	}
}

// OVL-410 on the local engine: senders on many goroutines and the actor's turns change the
// mailbox bytes concurrently; once every message has been received the estimate is back to 0.
func TestOVL410ConcurrentSenders(t *testing.T) {
	sys := newLocalSystem(t, WithMemoryEstimates())
	var received atomic.Int64
	pid, _ := spawnProbe(t, sys, "b", func(_ *Context[any], m any) {
		if z, ok := m.(sized); ok {
			received.Add(int64(z))
		}
	})
	const senders, each = 4, 100
	var wg sync.WaitGroup
	for range senders {
		wg.Go(func() {
			for range each {
				if err := Send(sys, pid, any(sized(3))); err != nil {
					t.Errorf("Send: %v", err)
				}
			}
		})
	}
	wg.Wait()
	inProbe(t, sys, pid, func(*Context[any]) any { return nil }) // the actor has received every earlier message
	if got, want := received.Load(), int64(senders*each*3); got != want {
		t.Errorf("the actor received %d bytes, want %d", got, want)
	}
	wantBytes(t, "at the end", sys, pid.Address(), 0, 0)
	if got := sys.usage().Bytes; got != 0 {
		t.Errorf("usage().Bytes = %d, want 0", got)
	}
}

// OVL-430 on the local engine: Estimates walks the registry while Spawn registers new actors, and
// takes each address from the registry, not from a cell Spawn may still be setting up. Run under
// -race, this test fails if the walk reads a cell's address.
func TestEstimatesWhileSpawning(t *testing.T) {
	sys := newLocalSystem(t, WithMemoryEstimates())
	const n = 100
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
			}
			_ = sys.Estimates(n)
		}
	}()
	want := make([]Address, 0, n)
	for i := range n {
		pid, err := Spawn(sys, "e"+strconv.Itoa(i), func() Actor[any] { return &memProbe{} })
		if err != nil {
			t.Fatalf("Spawn e%d: %v", i, err)
		}
		want = append(want, pid.Address())
	}
	close(done)
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the Estimates goroutine did not stop within 10 s")
	}
	var got []Address
	for _, e := range sys.Estimates(n) {
		got = append(got, e.Addr)
	}
	slices.SortFunc(got, func(a, b Address) int { return strings.Compare(a.String(), b.String()) })
	slices.SortFunc(want, func(a, b Address) int { return strings.Compare(a.String(), b.String()) })
	if !slices.Equal(got, want) {
		t.Errorf("Estimates lists %d addresses, want the %d spawned ones: %v", len(got), len(want), got)
	}
}
