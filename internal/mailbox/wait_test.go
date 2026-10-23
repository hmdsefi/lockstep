// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package mailbox

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// liveness bounds every wait of these tests.
const liveness = 10 * time.Second

// waitResult is what one OfferWait call returned.
type waitResult struct {
	out Outcome
	v   int
	err error
}

// offerWait calls q.OfferWait on a new goroutine and delivers its result on the returned channel.
func offerWait(q *Queue[int], ctx context.Context, v int, admit func() bool) <-chan waitResult {
	ch := make(chan waitResult, 1)
	go func() {
		out, dropped, err := q.OfferWait(ctx, v, admit)
		ch <- waitResult{out, dropped, err}
	}()
	return ch
}

// result receives one OfferWait result, failing the test after the liveness bound.
func result(t *testing.T, ch <-chan waitResult) waitResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(liveness):
		t.Fatal("OfferWait did not return")
		return waitResult{}
	}
}

// stillWaiting fails the test if the OfferWait behind ch has returned.
func stillWaiting(t *testing.T, ch <-chan waitResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("OfferWait returned %v, %v while it should wait", r.out, r.err)
	default:
	}
}

// waitFor polls cond until it holds, failing the test after the liveness bound.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(liveness)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// waiting waits until q has n senders waiting.
func waiting(t *testing.T, q *Queue[int], n int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("Waiting() == %d", n), func() bool { return q.Waiting() == n })
}

func wantResult(t *testing.T, name string, got waitResult, out Outcome, err error) {
	t.Helper()
	if got.out != out || got.v != 0 || !errors.Is(got.err, err) || (err == nil && got.err != nil) {
		t.Fatalf("%s = %v, %d, %v, want %v, 0, %v", name, got.out, got.v, got.err, out, err)
	}
}

func TestATMBX08BlockingOrderAndHandoff(t *testing.T) {
	ctx := context.Background()
	q := New[int](1, BlockSender)
	q.Offer(0, Sent, false)

	g1 := offerWait(q, ctx, 1, nil)
	waiting(t, q, 1)
	g2 := offerWait(q, ctx, 2, nil)
	waiting(t, q, 2)

	if out, _ := q.Offer(3, Sent, false); out != Rejected {
		t.Fatalf("Offer(3) = %v, want rejected", out)
	}

	if v, kind, ok := q.Pop(); !ok || v != 0 || kind != Sent {
		t.Fatalf("first Pop = %d, %v, %v, want 0, Sent, true", v, kind, ok)
	}
	wantResult(t, "G1", result(t, g1), Enqueued, nil)
	if n := q.Waiting(); n != 1 {
		t.Fatalf("Waiting after the first Pop = %d, want 1", n)
	}
	stillWaiting(t, g2)

	// The slot the first Pop freed went to G1 inside Pop, so another sender finds no room.
	if out, _ := q.Offer(4, Sent, false); out != Rejected {
		t.Fatalf("Offer(4) after the hand-off = %v, want rejected", out)
	}
	if q.Len() != 1 || q.SentLen() != 1 {
		t.Fatalf("Len, SentLen after the hand-off = %d, %d, want 1, 1", q.Len(), q.SentLen())
	}

	if v, kind, ok := q.Pop(); !ok || v != 1 || kind != Sent {
		t.Fatalf("second Pop = %d, %v, %v, want 1, Sent, true", v, kind, ok)
	}
	wantResult(t, "G2", result(t, g2), Enqueued, nil)
	if n := q.Waiting(); n != 0 {
		t.Fatalf("Waiting after the second Pop = %d, want 0", n)
	}
	if got, want := drain(q), []Entry[int]{{2, Sent}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("queue = %v, want %v", got, want)
	}
}

func TestATMBX09BlockingCancelAndClose(t *testing.T) {
	full := func() *Queue[int] {
		q := New[int](1, BlockSender)
		q.Offer(0, Sent, false)
		return q
	}

	t.Run("cancelled while waiting", func(t *testing.T) {
		q := full()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch := offerWait(q, ctx, 7, nil)
		waiting(t, q, 1)
		cancel()
		wantResult(t, "OfferWait", result(t, ch), Rejected, context.Canceled)
		if n := q.Waiting(); n != 0 {
			t.Fatalf("Waiting = %d, want 0", n)
		}
		if got, want := drain(q), []Entry[int]{{0, Sent}}; !reflect.DeepEqual(got, want) {
			t.Fatalf("queue = %v, want %v", got, want)
		}
		if q.Len() != 0 || q.SentLen() != 0 {
			t.Fatalf("Len, SentLen = %d, %d, want 0, 0", q.Len(), q.SentLen())
		}
	})

	t.Run("cancelled before", func(t *testing.T) {
		q := full()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		wantResult(t, "OfferWait", result(t, offerWait(q, ctx, 7, nil)), Rejected, context.Canceled)
		if n := q.Waiting(); n != 0 {
			t.Fatalf("Waiting = %d, want 0", n)
		}
		if q.Len() != 1 || q.SentLen() != 1 {
			t.Fatalf("Len, SentLen = %d, %d, want 1, 1", q.Len(), q.SentLen())
		}
	})

	t.Run("close", func(t *testing.T) {
		q := full()
		ctx := context.Background()
		g1 := offerWait(q, ctx, 1, nil)
		waiting(t, q, 1)
		g2 := offerWait(q, ctx, 2, nil)
		waiting(t, q, 2)
		if got, want := q.Close(), []Entry[int]{{0, Sent}}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Close = %v, want %v", got, want)
		}
		wantResult(t, "G1", result(t, g1), Closed, nil)
		wantResult(t, "G2", result(t, g2), Closed, nil)
		if n := q.Waiting(); n != 0 {
			t.Fatalf("Waiting = %d, want 0", n)
		}
		out, v, err := q.OfferWait(ctx, 3, nil)
		wantResult(t, "OfferWait after Close", waitResult{out, v, err}, Closed, nil)
	})
}

func TestATMBX26WaitingOnAdmission(t *testing.T) {
	q := New[int](2, BlockSender)
	var adm atomic.Bool
	admit := adm.Load

	g1 := offerWait(q, context.Background(), 1, admit)
	waiting(t, q, 1)
	if n := q.SentLen(); n != 0 {
		t.Fatalf("SentLen while waiting on admission = %d, want 0", n)
	}

	q.Recheck()
	if q.Waiting() != 1 || q.SentLen() != 0 {
		t.Fatalf("after Recheck with admit false: Waiting, SentLen = %d, %d, want 1, 0", q.Waiting(), q.SentLen())
	}
	stillWaiting(t, g1)

	adm.Store(true)
	q.Recheck()
	wantResult(t, "G1", result(t, g1), Enqueued, nil)
	if q.Waiting() != 0 || q.SentLen() != 1 {
		t.Fatalf("after Recheck with admit true: Waiting, SentLen = %d, %d, want 0, 1", q.Waiting(), q.SentLen())
	}
	if got, want := drain(q), []Entry[int]{{1, Sent}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("queue = %v, want %v", got, want)
	}
}

// TestOfferWaitOtherPolicies checks that, for policies other than BlockSender, OfferWait is
// Offer(v, Sent, admit != nil && !admit()), whatever the context.
func TestOfferWaitOtherPolicies(t *testing.T) {
	yes := func() bool { return true }
	no := func() bool { return false }
	done, cancel := context.WithCancel(context.Background())
	cancel()
	states := []struct {
		name  string
		prime []Entry[int]
	}{
		{"room", []Entry[int]{{1, Sent}, {9, Self}}},
		{"full", []Entry[int]{{1, Sent}, {9, Self}, {2, Sent}}},
	}
	admits := []struct {
		name  string
		admit func() bool
	}{{"nil", nil}, {"true", yes}, {"false", no}}
	ctxs := []struct {
		name string
		ctx  context.Context
	}{{"background", context.Background()}, {"done", done}}

	for _, p := range []Policy{FailSend, DropNewest, DropOldest} {
		for _, s := range states {
			for _, a := range admits {
				for _, c := range ctxs {
					t.Run(fmt.Sprintf("%v/%s/admit-%s/%s", p, s.name, a.name, c.name), func(t *testing.T) {
						got, want := New[int](2, p), New[int](2, p)
						offerAll(got, s.prime...)
						offerAll(want, s.prime...)
						out, dropped, err := got.OfferWait(c.ctx, 3, a.admit)
						wantOut, wantDropped := want.Offer(3, Sent, a.admit != nil && !a.admit())
						if out != wantOut || dropped != wantDropped || err != nil {
							t.Fatalf("OfferWait = %v, %d, %v, want %v, %d, nil", out, dropped, err, wantOut, wantDropped)
						}
						if got.Waiting() != 0 {
							t.Fatalf("Waiting = %d, want 0", got.Waiting())
						}
						if g, w := drain(got), drain(want); !reflect.DeepEqual(g, w) {
							t.Fatalf("queue = %v, want %v", g, w)
						}
					})
				}
			}
		}
		t.Run(fmt.Sprintf("%v/closed", p), func(t *testing.T) {
			q := New[int](2, p)
			q.Close()
			out, dropped, err := q.OfferWait(context.Background(), 3, yes)
			if out != Closed || dropped != 0 || err != nil {
				t.Fatalf("OfferWait = %v, %d, %v, want closed, 0, nil", out, dropped, err)
			}
		})
	}
}

// TestOfferWaitWithRoom checks that a BlockSender OfferWait that need not wait enqueues at once,
// even when its context has ended.
func TestOfferWaitWithRoom(t *testing.T) {
	done, cancel := context.WithCancel(context.Background())
	cancel()
	yes := func() bool { return true }
	for _, c := range []struct {
		name  string
		ctx   context.Context
		admit func() bool
	}{
		{"admit nil", context.Background(), nil},
		{"admit true", context.Background(), yes},
		{"done context", done, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			q := New[int](2, BlockSender)
			q.Offer(1, Sent, false)
			out, v, err := q.OfferWait(c.ctx, 2, c.admit)
			wantResult(t, "OfferWait", waitResult{out, v, err}, Enqueued, nil)
			if got, want := drain(q), []Entry[int]{{1, Sent}, {2, Sent}}; !reflect.DeepEqual(got, want) {
				t.Fatalf("queue = %v, want %v", got, want)
			}
		})
	}
	t.Run("closed", func(t *testing.T) {
		q := New[int](2, BlockSender)
		q.Close()
		out, v, err := q.OfferWait(done, 2, nil)
		wantResult(t, "OfferWait", waitResult{out, v, err}, Closed, nil)
	})
}

// TestOfferWaitBehindWaiters checks that an OfferWait waits behind the senders already waiting
// even when the queue has room.
func TestOfferWaitBehindWaiters(t *testing.T) {
	q := New[int](2, BlockSender)
	var adm atomic.Bool
	g1 := offerWait(q, context.Background(), 1, adm.Load)
	waiting(t, q, 1)
	g2 := offerWait(q, context.Background(), 2, nil)
	waiting(t, q, 2)
	if n := q.SentLen(); n != 0 {
		t.Fatalf("SentLen = %d, want 0", n)
	}
	stillWaiting(t, g2)
	adm.Store(true)
	q.Recheck()
	wantResult(t, "G1", result(t, g1), Enqueued, nil)
	wantResult(t, "G2", result(t, g2), Enqueued, nil)
	if got, want := drain(q), []Entry[int]{{1, Sent}, {2, Sent}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("queue = %v, want %v", got, want)
	}
}

// TestWaiterHandedSlotAfterCancel checks that a waiter handed a slot returns Enqueued and nil
// although its context ended before it saw the hand-off. admit cancels the context during Pop,
// in the same critical section that hands the slot over.
func TestWaiterHandedSlotAfterCancel(t *testing.T) {
	q := New[int](1, BlockSender)
	q.Offer(0, Sent, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var armed atomic.Bool
	admit := func() bool {
		if armed.Load() {
			cancel()
		}
		return true
	}
	ch := offerWait(q, ctx, 1, admit)
	waiting(t, q, 1)
	armed.Store(true)
	if v, _, ok := q.Pop(); !ok || v != 0 {
		t.Fatalf("Pop = %d, %v, want 0, true", v, ok)
	}
	if ctx.Err() == nil {
		t.Fatal("admit did not cancel the context")
	}
	wantResult(t, "OfferWait", result(t, ch), Enqueued, nil)
	if got, want := drain(q), []Entry[int]{{1, Sent}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("queue = %v, want %v", got, want)
	}
}

// TestCancelledHeadLetsNextWaiterIn checks that when the first waiter leaves the line, a free slot
// goes to the next waiter if it is admitted.
func TestCancelledHeadLetsNextWaiterIn(t *testing.T) {
	q := New[int](1, BlockSender)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	g1 := offerWait(q, ctx1, 1, func() bool { return false })
	waiting(t, q, 1)
	g2 := offerWait(q, context.Background(), 2, nil)
	waiting(t, q, 2)
	cancel1()
	wantResult(t, "G1", result(t, g1), Rejected, context.Canceled)
	wantResult(t, "G2", result(t, g2), Enqueued, nil)
	if got, want := drain(q), []Entry[int]{{2, Sent}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("queue = %v, want %v", got, want)
	}
}

// TestPopOfSelfEntryHandsNothing checks that only a Pop that frees a Sent slot hands it over.
func TestPopOfSelfEntryHandsNothing(t *testing.T) {
	q := New[int](1, BlockSender)
	q.Offer(9, Self, false)
	q.Offer(0, Sent, false)
	ch := offerWait(q, context.Background(), 1, nil)
	waiting(t, q, 1)
	if v, kind, ok := q.Pop(); !ok || v != 9 || kind != Self {
		t.Fatalf("Pop = %d, %v, %v, want 9, Self, true", v, kind, ok)
	}
	if q.Waiting() != 1 {
		t.Fatalf("Waiting after popping a Self entry = %d, want 1", q.Waiting())
	}
	stillWaiting(t, ch)
	if v, _, ok := q.Pop(); !ok || v != 0 {
		t.Fatalf("Pop = %d, %v, want 0, true", v, ok)
	}
	wantResult(t, "OfferWait", result(t, ch), Enqueued, nil)
}

// TestConcurrentWaiters has several senders wait with deadlines while the test pops. Every
// sender's enqueued entries come out once, in the order it offered them, and no refused entry
// comes out. It is for -race and -count, to shake out lost wake-ups.
func TestConcurrentWaiters(t *testing.T) {
	const senders, each = 6, 60
	q := New[int](2, BlockSender)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		enqueued = map[int]bool{}
	)
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				v := s*each + i
				ctx, cancel := context.Background(), context.CancelFunc(func() {})
				if i%3 == s%3 {
					ctx, cancel = context.WithTimeout(context.Background(), time.Duration(i%4)*100*time.Microsecond)
				}
				out, _, err := q.OfferWait(ctx, v, nil)
				cancel()
				switch {
				case out == Enqueued && err == nil:
					mu.Lock()
					enqueued[v] = true
					mu.Unlock()
				case out == Rejected && errors.Is(err, context.DeadlineExceeded):
				default:
					t.Errorf("sender %d OfferWait(%d) = %v, %v", s, v, out, err)
					return
				}
			}
		}()
	}
	var finished atomic.Bool
	go func() {
		wg.Wait()
		finished.Store(true)
	}()

	last := make([]int, senders)
	for s := range last {
		last[s] = -1
	}
	var popped []int
	deadline := time.Now().Add(liveness)
	for {
		v, _, ok := q.Pop()
		if ok {
			popped = append(popped, v)
			s := v / each
			if v <= last[s] {
				t.Fatalf("sender %d: got %d after %d", s, v, last[s])
			}
			last[s] = v
			continue
		}
		if finished.Load() && q.Len() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %d waiting, %d popped", q.Waiting(), len(popped))
		}
		time.Sleep(10 * time.Microsecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(popped) != len(enqueued) {
		t.Fatalf("popped %d entries, %d were enqueued", len(popped), len(enqueued))
	}
	for _, v := range popped {
		if !enqueued[v] {
			t.Fatalf("popped %d, which was not enqueued", v)
		}
	}
	if q.Waiting() != 0 {
		t.Fatalf("Waiting = %d at the end, want 0", q.Waiting())
	}
}

// OfferWaitNotify calls waiting once, without the queue's lock, when the entry joins the line, and
// not at all when the entry is enqueued or refused without waiting.
func TestOfferWaitNotify(t *testing.T) {
	q := New[int](1, BlockSender)
	calls := 0
	count := func() { calls++ }
	if out, _, err := q.OfferWaitNotify(context.Background(), 1, nil, count); out != Enqueued || err != nil || calls != 0 {
		t.Fatalf("OfferWaitNotify with room = %v, %v after %d waiting calls, want Enqueued, nil after none", out, err, calls)
	}
	done, cancel := context.WithCancel(context.Background())
	cancel()
	if out, _, err := q.OfferWaitNotify(done, 2, nil, count); out != Rejected || !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("OfferWaitNotify, full, ctx done = %v, %v after %d waiting calls, want Rejected, context.Canceled after none", out, err, calls)
	}

	var inLine atomic.Int64
	ch := make(chan waitResult, 1)
	go func() {
		out, v, err := q.OfferWaitNotify(context.Background(), 3, nil, func() {
			inLine.Add(int64(q.Waiting())) // takes the lock: waiting runs without it
		})
		ch <- waitResult{out, v, err}
	}()
	waitFor(t, "the waiting call", func() bool { return inLine.Load() != 0 })
	if n := inLine.Load(); n != 1 {
		t.Fatalf("Waiting() inside waiting = %d, want 1", n)
	}
	if v, _, _ := q.Pop(); v != 1 {
		t.Fatalf("Pop = %d, want 1", v)
	}
	wantResult(t, "OfferWaitNotify", result(t, ch), Enqueued, nil)
	if v, _, _ := q.Pop(); v != 3 {
		t.Fatalf("Pop = %d, want 3", v)
	}
}

// A panic of waiting takes the entry out of the line and propagates; the queue keeps working.
func TestOfferWaitNotifyPanic(t *testing.T) {
	q := New[int](1, BlockSender)
	q.Offer(1, Sent, false)
	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Fatalf("recovered %v, want boom", r)
			}
		}()
		_, _, _ = q.OfferWaitNotify(context.Background(), 2, nil, func() { panic("boom") })
	}()
	if n := q.Waiting(); n != 0 {
		t.Fatalf("Waiting() = %d after the panic, want 0", n)
	}
	q.Pop()
	if out, _ := q.Offer(3, Sent, false); out != Enqueued {
		t.Fatalf("Offer after the panic = %v, want Enqueued", out)
	}
	if v, _, _ := q.Pop(); v != 3 {
		t.Fatalf("Pop = %d, want 3: the panicked entry was enqueued", v)
	}
}
