// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package mailbox

import "context"

// waitState is where a waiting sender stands.
type waitState uint8

const (
	waitInLine waitState = iota // waiting for a slot
	waitHanded                  // its entry was enqueued by Pop or Recheck
	waitClosed                  // the queue closed while it waited
)

// waiter is one sender waiting in OfferWait. Its fields are guarded by the queue mutex.
type waiter[T any] struct {
	v     T
	admit func() bool
	state waitState
}

// OfferWait adds a Sent entry for an outside sender (MBX-070). admit reports whether the System
// admits the entry now (OVL); nil means always. If the policy is BlockSender and the queue is
// full, admit reports false, or other senders are already waiting, it waits in line until it is
// handed a slot (Enqueued, nil), the queue closes (Closed, nil) or ctx ends (Rejected,
// ctx.Err()). For other policies it is Offer(v, Sent, admit != nil && !admit()).
//
// admit is called with the queue's lock held; it must not block or call the queue.
func (q *Queue[T]) OfferWait(ctx context.Context, v T, admit func() bool) (Outcome, T, error) {
	return q.OfferWaitNotify(ctx, v, admit, nil)
}

// OfferWaitNotify is OfferWait, except that it calls notify, if not nil, once when the entry
// joins the line, before it waits for the first time; it is not called when OfferWait returns
// without waiting. notify runs without the queue's lock held, so it may call the queue; the
// entry may be handed a slot, or the queue closed, while it runs. If notify panics, the entry
// leaves the line, unless it was handed a slot meanwhile, and the panic propagates.
func (q *Queue[T]) OfferWaitNotify(ctx context.Context, v T, admit func() bool, notify func()) (Outcome, T, error) {
	var zero T
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.policy != BlockSender {
		out, dropped := q.offer(v, Sent, admit != nil && !admit())
		return out, dropped, nil
	}
	if q.closed {
		return Closed, zero, nil
	}
	if len(q.waiters) == 0 && q.sent < q.capacity && (admit == nil || admit()) {
		q.push(v, Sent)
		q.sent++
		return Enqueued, zero, nil
	}
	if err := ctx.Err(); err != nil {
		return Rejected, zero, err
	}

	w := &waiter[T]{v: v, admit: admit}
	q.waiters = append(q.waiters, w)
	if notify != nil {
		q.notify(w, notify)
	}
	// The callback takes the lock so that its Broadcast cannot fall between this goroutine's check
	// of ctx and its cond.Wait. stop never waits for a running callback, so calling it with the
	// lock held cannot deadlock.
	//lockstep:blocking only an outside sender to a BlockSender mailbox waits here, on a blocking engine (MBX-070, MBX-080)
	stop := context.AfterFunc(ctx, q.wake)
	defer stop()
	for {
		switch w.state {
		case waitHanded:
			return Enqueued, zero, nil
		case waitClosed:
			return Closed, zero, nil
		}
		if err := ctx.Err(); err != nil {
			q.leave(w)
			return Rejected, zero, err
		}
		q.cond.Wait()
	}
}

// Recheck hands free slots to waiting senders whose admit now reports true, as Pop does (MBX-075).
func (q *Queue[T]) Recheck() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handOff()
}

// Waiting returns the number of senders waiting in OfferWait.
func (q *Queue[T]) Waiting() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.waiters)
}

// notify calls waiting for w, which has just joined the line, without the queue's lock held. The
// caller holds q.mu, and holds it again when notify returns or panics; a panic of waiting takes w
// out of the line if it is still in it.
func (q *Queue[T]) notify(w *waiter[T], waiting func()) {
	q.mu.Unlock()
	done := false
	defer func() {
		q.mu.Lock()
		if !done && w.state == waitInLine {
			q.leave(w)
		}
	}()
	waiting()
	done = true
}

// wake wakes every waiter so each one checks its context. It runs when a waiter's context ends.
func (q *Queue[T]) wake() {
	q.mu.Lock()
	q.cond.Broadcast()
	q.mu.Unlock()
}

// handOff enqueues the first waiter's entry and wakes it while there is room and that waiter is
// admitted, so a free slot is never visible to other senders while an admitted waiter exists
// (MBX-070, MBX-075). The caller holds q.mu.
func (q *Queue[T]) handOff() {
	woke := false
	for len(q.waiters) > 0 && q.sent < q.capacity {
		w := q.waiters[0]
		if w.admit != nil && !w.admit() {
			break
		}
		q.waiters[0] = nil
		q.waiters = q.waiters[1:]
		q.push(w.v, Sent)
		q.sent++
		var zero T
		w.v, w.state = zero, waitHanded
		woke = true
	}
	if len(q.waiters) == 0 {
		q.waiters = nil
	}
	if woke {
		q.cond.Broadcast()
	}
}

// leave takes w, whose context ended, out of the line. If w was first, the next waiter may now be
// served. The caller holds q.mu.
func (q *Queue[T]) leave(w *waiter[T]) {
	for i, x := range q.waiters {
		if x != w {
			continue
		}
		copy(q.waiters[i:], q.waiters[i+1:])
		q.waiters[len(q.waiters)-1] = nil
		q.waiters = q.waiters[:len(q.waiters)-1]
		if i == 0 {
			q.handOff()
		}
		return
	}
	panic("mailbox: waiter not in line")
}

// closeWaiters wakes every waiter with Closed. The caller holds q.mu.
func (q *Queue[T]) closeWaiters() {
	if len(q.waiters) == 0 {
		return
	}
	for _, w := range q.waiters {
		var zero T
		w.v, w.state = zero, waitClosed
	}
	q.waiters = nil
	q.cond.Broadcast()
}
