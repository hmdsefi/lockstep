// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orders_test

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/orders"
	"github.com/hmdsefi/lockstep/sim"
)

// The seeds on which the planted bugs fail first. TestPlantedBugs checks that they still do.
const (
	retrySeed       = 0x56247cd01be260d5 // the first seed of TestRetry that fails with BugNoDedup
	earlyCancelSeed = 0x144668b471a38445 // the first seed of TestCancelBeforeCreate that fails with BugDropEarlyCancel
)

// The final checks every scenario registers.
const (
	checkSetup         = "every spawn and send succeeds"
	checkChargedOnce   = "charge each order at most once"
	checkCancelRefunds = "a cancelled order ends up not charged"
)

// TestRetry submits one order while the payment provider is slow. Checkout's ask times out after
// 100 ms and asks again, although the first Create is on its way.
func TestRetry(t *testing.T) {
	sim.Simulate(t, options(200), retry(bugs(t)))
}

// TestRetryRegression runs TestRetry's body on the seed that found the no-dedup bug.
func TestRetryRegression(t *testing.T) {
	sim.SimulateSeeds(t, options(0), retry(bugs(t)), retrySeed)
}

// TestCancelBeforeCreate submits one order and cancels it 20 ms later. The Create goes through
// Checkout and its fraud check; the Cancel goes to Orders directly.
func TestCancelBeforeCreate(t *testing.T) {
	sim.Simulate(t, options(200), cancelBeforeCreate(bugs(t)))
}

// TestCancelBeforeCreateRegression runs TestCancelBeforeCreate's body on the seed that found the
// drop-early-cancel bug.
func TestCancelBeforeCreateRegression(t *testing.T) {
	sim.SimulateSeeds(t, options(0), cancelBeforeCreate(bugs(t)), earlyCancelSeed)
}

// options returns the options of the scenarios: seeds seeds of 2 s of simulated time each.
func options(seeds int) sim.Options {
	return sim.Options{Options: faultline.Options{Seeds: seeds, Duration: 2 * time.Second}}
}

// bugs returns the bugs that ORDERS_BUG names, such as no-dedup. Unset, it plants none.
func bugs(t *testing.T) orders.Bugs {
	t.Helper()
	b, err := orders.ParseBugs(os.Getenv("ORDERS_BUG"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// retry submits order o1 once.
func retry(bugs orders.Bugs) func(*sim.Sim) {
	return func(s *sim.Sim) {
		shop := newShop(s, bugs)
		s.World().RunFor(10 * time.Millisecond) // let the nodes boot
		shop.submit("o1", 40)
		s.World().RunFor(time.Second)
	}
}

// cancelBeforeCreate submits order o1 and cancels it 20 ms later.
func cancelBeforeCreate(bugs orders.Bugs) func(*sim.Sim) {
	return func(s *sim.Sim) {
		shop := newShop(s, bugs)
		s.World().RunFor(10 * time.Millisecond) // let the nodes boot
		shop.submit("o1", 40)
		s.World().RunFor(20 * time.Millisecond)
		shop.cancel("o1")
		s.World().RunFor(time.Second)
	}
}

// shop is the order service under test: Orders on node "orders", Checkout on node "web", and a
// client node "shopper". It plays the payment provider and keeps its ledger.
type shop struct {
	s         *sim.Sim
	orders    lockstep.PID[orders.Msg]
	checkout  lockstep.PID[orders.CheckoutMsg]
	charges   map[string]int // order ID: times charged
	refunds   map[string]int // order ID: times refunded
	cancelled []string       // order IDs the shopper cancelled
	errs      []error        // failed spawns and sends
}

// newShop boots the service with bugs planted, answers the payment effects after latencies
// drawn from the seed, and registers the final checks.
func newShop(s *sim.Sim, bugs orders.Bugs) *shop {
	sh := &shop{
		s:        s,
		orders:   lockstep.PIDAt[orders.Msg]("orders", "/user/orders"),
		checkout: lockstep.PIDAt[orders.CheckoutMsg]("web", "/user/checkout"),
		charges:  map[string]int{},
		refunds:  map[string]int{},
	}
	s.Node("orders", func(sys *lockstep.System) {
		_, err := lockstep.Spawn(sys, "orders", orders.New(nil, bugs))
		sh.note(err)
	})
	s.Node("web", func(sys *lockstep.System) {
		_, err := lockstep.Spawn(sys, "checkout", orders.NewCheckout(orders.CheckoutConfig{
			Orders:   sh.orders,
			Timeout:  100 * time.Millisecond,
			Attempts: 3,
		}))
		sh.note(err)
	})
	s.Client("shopper", func(*lockstep.System) {})

	// The fraud check answers in 5 to 50 ms; a charge or a refund in 20 to 200 ms.
	s.HandleEffect(orders.Screen{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		return sim.EffectResult{Value: struct{}{}, Latency: kernel.Uniform(c.Rand(), 5*time.Millisecond, 50*time.Millisecond)}
	})
	s.HandleEffect(orders.Charge{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		charge, _ := c.Effect().(orders.Charge)
		sh.charges[charge.Order]++
		return sim.EffectResult{Value: struct{}{}, Latency: kernel.Uniform(c.Rand(), 20*time.Millisecond, 200*time.Millisecond)}
	})
	s.HandleEffect(orders.Refund{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		refund, _ := c.Effect().(orders.Refund)
		sh.refunds[refund.Order]++
		return sim.EffectResult{Value: struct{}{}, Latency: kernel.Uniform(c.Rand(), 20*time.Millisecond, 200*time.Millisecond)}
	})

	w := s.World()
	w.Final(checkSetup, func() error { return errors.Join(sh.errs...) })
	w.Final(checkChargedOnce, func() error {
		for _, id := range sortedKeys(sh.charges) {
			if n := sh.charges[id]; n > 1 {
				return fmt.Errorf("order %s charged %d times", id, n)
			}
		}
		return nil
	})
	w.Final(checkCancelRefunds, func() error {
		for _, id := range sh.cancelled {
			if net := sh.charges[id] - sh.refunds[id]; net != 0 {
				return fmt.Errorf("order %s was cancelled but is still charged: charges=%d refunds=%d", id, sh.charges[id], sh.refunds[id])
			}
		}
		return nil
	})
	return sh
}

// submit sends Submit for an order to Checkout from the shopper.
func (sh *shop) submit(id string, amount int) {
	sh.note(sim.Send(sh.s, "shopper", sh.checkout, orders.CheckoutMsg(orders.Submit{ID: id, Amount: amount})))
}

// cancel sends Cancel for an order to Orders from the shopper, directly.
func (sh *shop) cancel(id string) {
	sh.cancelled = append(sh.cancelled, id)
	sh.note(sim.Send(sh.s, "shopper", sh.orders, orders.Msg(orders.Cancel{ID: id})))
}

// note keeps err, if any, for the final check checkSetup.
func (sh *shop) note(err error) {
	if err != nil {
		sh.errs = append(sh.errs, err)
	}
}

// sortedKeys returns m's keys in order.
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
