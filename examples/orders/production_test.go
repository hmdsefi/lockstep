// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orders_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/orders"
)

// slowPayments is a payment provider whose charges take longer than Checkout's ask timeout.
type slowPayments struct {
	mu      sync.Mutex
	charges map[string]int
}

func (p *slowPayments) Screen(context.Context, string, int) error { return nil }

func (p *slowPayments) Charge(ctx context.Context, order string, _ int) error {
	select {
	case <-time.After(250 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.charges[order]++
	return nil
}

func (p *slowPayments) Refund(context.Context, string, int) error { return nil }

// count returns how often order was charged.
func (p *slowPayments) count(order string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.charges[order]
}

// TestProduction runs the same actors on the production engine. Every charge outlasts the ask
// timeout, so Checkout asks three times, and the order is still charged once. An order cancelled
// before it was submitted is never charged.
func TestProduction(t *testing.T) {
	api := &slowPayments{charges: map[string]int{}}
	sys := lockstep.New()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sys.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	ord, err := lockstep.Spawn(sys, "orders", orders.New(api, bugs(t)))
	if err != nil {
		t.Fatal(err)
	}
	co, err := lockstep.Spawn(sys, "checkout", orders.NewCheckout(orders.CheckoutConfig{
		Orders: ord, API: api, Timeout: 100 * time.Millisecond, Attempts: 3,
	}))
	if err != nil {
		t.Fatal(err)
	}

	// A Send to a local actor is queued before Send returns, so this Cancel arrives first.
	if err := lockstep.Send(sys, ord, orders.Msg(orders.Cancel{ID: "o2"})); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"o1", "o2"} {
		if err := lockstep.Send(sys, co, orders.CheckoutMsg(orders.Submit{ID: id, Amount: 40})); err != nil {
			t.Fatal(err)
		}
	}

	// The three asks for o1 end by about 300 ms and the one charge by about 350 ms.
	time.Sleep(700 * time.Millisecond)
	if n := api.count("o1"); n != 1 {
		t.Errorf("o1 charged %d times, want 1", n)
	}
	if n := api.count("o2"); n != 0 {
		t.Errorf("o2 charged %d times after its Cancel, want 0", n)
	}
}
