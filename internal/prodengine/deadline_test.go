// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// expirer is an engine.Expirer that calls fn.
type expirer func()

func (f expirer) Expire() { f() }

// TestDeadlines covers the bundle's Deadlines: exactly-once expiry in engine context, Disarm's
// result, slot reuse, early and late runtime timers, Stop, and the pending count (SCH-130,
// SCH-140).
func TestDeadlines(t *testing.T) {
	t.Run("ExpiresOnceInEngineContext", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		var runs atomic.Int32
		var inTurn atomic.Bool
		ran := make(chan time.Time, 2)
		start := time.Now()
		if h := e.dl.Arm(1, 20*time.Millisecond, expirer(func() {
			runs.Add(1)
			inTurn.Store(e.s.InTurn())
			ran <- time.Now()
		})); h == 0 {
			t.Fatal("Arm on a running engine returned 0")
		}
		if n := e.pendingTimers(); n != 1 {
			t.Errorf("pendingTimers() with one deadline armed = %d, want 1", n)
		}
		select {
		case at := <-ran:
			if d := at.Sub(start); d < 20*time.Millisecond {
				t.Errorf("the 20 ms deadline expired after %s", d)
			}
		case <-time.After(time.Second):
			t.Fatal("the 20 ms deadline did not expire within 1 s")
		}
		if !inTurn.Load() {
			t.Error("InTurn() inside Expire = false, want true")
		}
		if n := e.pendingTimers(); n != 0 {
			t.Errorf("pendingTimers() after the deadline expired = %d, want 0", n)
		}
		time.Sleep(50 * time.Millisecond)
		if n := runs.Load(); n != 1 {
			t.Errorf("the deadline expired %d times, want once", n)
		}
	})

	t.Run("NegativeDurationExpiresAfterArmReturned", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 1})
		var returned atomic.Bool
		got := make(chan bool, 1)
		a := newFake(t, 0)
		a.onTurn = func(*fakeActor, int) {
			e.dl.Arm(0, -1, expirer(func() { got <- returned.Load() }))
			returned.Store(true)
		}
		a.add()
		e.s.Ready(a)
		select {
		case afterReturn := <-got:
			if !afterReturn {
				t.Error("Expire ran before Arm returned")
			}
		case <-time.After(time.Second):
			t.Fatal("the deadline with duration -1 did not expire within 1 s")
		}
	})

	t.Run("DisarmIsExactlyOnce", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		var runs atomic.Int32
		h := e.dl.Arm(7, time.Hour, expirer(func() { runs.Add(1) }))
		if !e.dl.Disarm(h) {
			t.Error("first Disarm = false, want true")
		}
		if n := e.pendingTimers(); n != 0 {
			t.Errorf("pendingTimers() after Disarm = %d, want 0", n)
		}
		if e.dl.Disarm(h) {
			t.Error("second Disarm = true, want false")
		}
		if e.dl.Disarm(0) {
			t.Error("Disarm(0) = true, want false")
		}
		if n := runs.Load(); n != 0 {
			t.Errorf("the disarmed deadline expired %d times", n)
		}
	})

	t.Run("DisarmAfterExpiryIsFalse", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		ran := make(chan struct{})
		h := e.dl.Arm(3, 0, expirer(func() { close(ran) }))
		select {
		case <-ran:
		case <-time.After(time.Second):
			t.Fatal("the deadline did not expire within 1 s")
		}
		if e.dl.Disarm(h) {
			t.Error("Disarm after expiry = true, want false")
		}
	})

	t.Run("StaleHandleDisarmsNothing", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 1})
		old := e.dl.Arm(5, time.Hour, expirer(func() {}))
		e.dl.Disarm(old)
		ran := make(chan struct{})
		h := e.dl.Arm(5, 10*time.Millisecond, expirer(func() { close(ran) })) // reuses old's slot
		if h == old {
			t.Fatalf("Arm on a reused slot returned the old handle %#x", h)
		}
		if e.dl.Disarm(old) {
			t.Error("Disarm with the stale handle = true, want false")
		}
		select {
		case <-ran:
		case <-time.After(time.Second):
			t.Fatal("the stale handle's Disarm stopped the slot's new deadline")
		}
	})

	t.Run("EarlierDeadlineResetsTheTimer", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 1}) // 4 shards: keys 0 and 4 share one
		e.dl.Arm(0, time.Hour, expirer(func() {}))
		ran := make(chan struct{})
		e.dl.Arm(4, 10*time.Millisecond, expirer(func() { close(ran) }))
		select {
		case <-ran:
		case <-time.After(time.Second):
			t.Fatal("a 10 ms deadline behind a 1 h one did not expire within 1 s")
		}
	})

	t.Run("EarlyTimerIsSetAgain", func(t *testing.T) {
		// The shard's timer stays set for the disarmed 20 ms deadline; when it fires, nothing is
		// due, and it must be set again for the 60 ms one.
		e := newEngine(t, Config{Node: "n", Workers: 1})
		e.dl.Disarm(e.dl.Arm(0, 20*time.Millisecond, expirer(func() {})))
		ran := make(chan time.Time, 1)
		start := time.Now()
		e.dl.Arm(4, 60*time.Millisecond, expirer(func() { ran <- time.Now() }))
		select {
		case at := <-ran:
			if d := at.Sub(start); d < 60*time.Millisecond {
				t.Errorf("the 60 ms deadline expired after %s", d)
			}
		case <-time.After(time.Second):
			t.Fatal("the 60 ms deadline did not expire within 1 s")
		}
	})

	t.Run("ExpiresInOrder", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 1})
		var mu sync.Mutex
		var order []int
		done := make(chan struct{})
		for _, d := range []int{50, 10, 40, 20, 30} {
			e.dl.Arm(0, time.Duration(d)*time.Millisecond, expirer(func() {
				mu.Lock()
				defer mu.Unlock()
				order = append(order, d)
				if len(order) == 5 {
					close(done)
				}
			}))
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("five deadlines did not expire within 1 s")
		}
		mu.Lock()
		defer mu.Unlock()
		for i := 1; i < len(order); i++ {
			if order[i] < order[i-1] {
				t.Fatalf("expiry order %v, want ascending durations", order)
			}
		}
	})

	t.Run("StopClearsDeadlines", func(t *testing.T) {
		e := newEngine(t, Config{Node: "n", Workers: 2})
		var runs atomic.Int32
		e.dl.Arm(0, 50*time.Millisecond, expirer(func() { runs.Add(1) }))
		h := e.dl.Arm(1, time.Hour, expirer(func() { runs.Add(1) }))
		e.Stop()
		if n := e.pendingTimers(); n != 0 {
			t.Errorf("pendingTimers() after Stop = %d, want 0", n)
		}
		if e.dl.Disarm(h) {
			t.Error("Disarm after Stop = true, want false")
		}
		if h := e.dl.Arm(0, 0, expirer(func() { runs.Add(1) })); h != 0 {
			t.Errorf("Arm after Stop = %#x, want 0", h)
		}
		time.Sleep(100 * time.Millisecond)
		if n := runs.Load(); n != 0 {
			t.Errorf("deadlines armed around Stop expired %d times, want 0", n)
		}
	})

	t.Run("ConcurrentArmAndDisarm", func(t *testing.T) {
		// Each deadline either is disarmed (Disarm true) or expires, exactly once.
		e := newEngine(t, Config{Node: "n", Workers: 4})
		const goroutines, each = 8, 500
		var expired, disarmed atomic.Int64
		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range each {
					var runs atomic.Int32
					key := uint64(g*each + i) //nolint:gosec // G115: a small index
					h := e.dl.Arm(key, time.Duration(i%3)*time.Millisecond, expirer(func() {
						if runs.Add(1) == 1 {
							expired.Add(1)
						} else {
							t.Error("a deadline expired twice")
						}
					}))
					if i%2 == 0 && e.dl.Disarm(h) {
						disarmed.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		deadline := time.Now().Add(5 * time.Second)
		for expired.Load()+disarmed.Load() != goroutines*each && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if got := expired.Load() + disarmed.Load(); got != goroutines*each {
			t.Errorf("%d expired + %d disarmed = %d, want %d", expired.Load(), disarmed.Load(), got, goroutines*each)
		}
		if n := e.pendingTimers(); n != 0 {
			t.Errorf("pendingTimers() after every deadline ended = %d, want 0", n)
		}
	})
}
