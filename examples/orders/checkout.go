// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orders

import (
	"errors"
	"time"

	"github.com/hmdsefi/lockstep"
)

// CheckoutMsg is the message type of Checkout.
type CheckoutMsg interface{ isCheckoutMsg() }

// Submit asks Checkout to screen order ID and then create it.
type Submit struct {
	ID     string
	Amount int
}

// screened is the result of the Screen effect.
type screened struct {
	order Submit
	err   error
}

// answered is the outcome of one Create ask.
type answered struct {
	order   Submit
	attempt int
	result  Result
	err     error
}

func (Submit) isCheckoutMsg()   {}
func (screened) isCheckoutMsg() {}
func (answered) isCheckoutMsg() {}

// CheckoutConfig configures Checkout.
type CheckoutConfig struct {
	Orders   lockstep.PID[Msg] // the Orders actor
	API      Payments          // nil in simulation
	Timeout  time.Duration     // timeout of one Create ask; 0 means the ask default
	Attempts int               // Create asks per order, the first included; 0 means 1
}

// Checkout screens each submitted order and asks Orders to create it. When an ask times out it
// asks again with the same order ID, up to Attempts asks in all. A timed-out ask does not mean
// that Orders never received the Create: the Create or its answer may still be on the way.
type Checkout struct {
	cfg CheckoutConfig
}

// NewCheckout returns a factory of Checkout.
func NewCheckout(cfg CheckoutConfig) func() lockstep.Actor[CheckoutMsg] {
	if cfg.Attempts < 1 {
		cfg.Attempts = 1
	}
	return func() lockstep.Actor[CheckoutMsg] { return &Checkout{cfg: cfg} }
}

// Receive handles one message.
func (c *Checkout) Receive(ctx *lockstep.Context[CheckoutMsg], msg CheckoutMsg) {
	switch m := msg.(type) {
	case Submit:
		lockstep.Do(ctx, Screen{API: c.cfg.API, Order: m.ID, Amount: m.Amount}, func(_ struct{}, err error) CheckoutMsg {
			return screened{order: m, err: err}
		})
	case screened:
		if m.err != nil {
			return // rejected by the fraud check
		}
		c.create(ctx, m.order, 1)
	case answered:
		if errors.Is(m.err, lockstep.ErrTimeout) && m.attempt < c.cfg.Attempts {
			c.create(ctx, m.order, m.attempt+1) // same ID: Orders can tell it is a retry
		}
	}
}

// create asks Orders to create order and pipes the outcome back as an answered message.
func (c *Checkout) create(ctx *lockstep.Context[CheckoutMsg], order Submit, attempt int) {
	var opts []lockstep.AskOption
	if c.cfg.Timeout > 0 {
		opts = append(opts, lockstep.WithTimeout(c.cfg.Timeout))
	}
	r := lockstep.Ask(ctx, c.cfg.Orders, func(reply lockstep.ReplyTo[Result]) Msg {
		return Create{ID: order.ID, Amount: order.Amount, Reply: reply}
	}, opts...)
	lockstep.Pipe(ctx, r, func(res Result, err error) CheckoutMsg {
		return answered{order: order, attempt: attempt, result: res, err: err}
	})
}
