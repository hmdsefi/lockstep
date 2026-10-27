// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orders

import (
	"context"
	"errors"
)

// Payments is the payment provider. The effects below call it in production; a simulation
// never calls it and answers the effects from handlers instead.
type Payments interface {
	// Screen runs a fraud check on an order and returns an error to reject it.
	Screen(ctx context.Context, order string, amount int) error
	// Charge charges the customer amount for order.
	Charge(ctx context.Context, order string, amount int) error
	// Refund refunds amount for order.
	Refund(ctx context.Context, order string, amount int) error
}

// ErrNoPayments is the error of an effect whose API is nil.
var ErrNoPayments = errors.New("orders: no Payments")

// Screen is the effect that runs a fraud check before Checkout creates an order.
type Screen struct {
	API    Payments // nil in simulation
	Order  string
	Amount int
}

// Name returns "payment.screen".
func (Screen) Name() string { return "payment.screen" }

// Run calls API.Screen.
func (e Screen) Run(ctx context.Context) (struct{}, error) {
	if e.API == nil {
		return struct{}{}, ErrNoPayments
	}
	return struct{}{}, e.API.Screen(ctx, e.Order, e.Amount)
}

// Charge is the effect that charges an order.
type Charge struct {
	API    Payments // nil in simulation
	Order  string
	Amount int
}

// Name returns "payment.charge".
func (Charge) Name() string { return "payment.charge" }

// Run calls API.Charge.
func (e Charge) Run(ctx context.Context) (struct{}, error) {
	if e.API == nil {
		return struct{}{}, ErrNoPayments
	}
	return struct{}{}, e.API.Charge(ctx, e.Order, e.Amount)
}

// Refund is the effect that refunds a cancelled order.
type Refund struct {
	API    Payments // nil in simulation
	Order  string
	Amount int
}

// Name returns "payment.refund".
func (Refund) Name() string { return "payment.refund" }

// Run calls API.Refund.
func (e Refund) Run(ctx context.Context) (struct{}, error) {
	if e.API == nil {
		return struct{}{}, ErrNoPayments
	}
	return struct{}{}, e.API.Refund(ctx, e.Order, e.Amount)
}
