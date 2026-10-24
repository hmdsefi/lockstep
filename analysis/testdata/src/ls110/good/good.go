// Package good holds code that LS110 accepts: Pipe in actor code, and Await outside it (ANL-170,
// AT-ANL-12).
package good

import (
	"context"

	"github.com/hmdsefi/lockstep"
)

// Msg is Account's message type.
type Msg int

// Account is an actor type.
type Account struct {
	n *lockstep.Reply[int]
}

// Receive pipes the reply back as a message. The goroutine's literal is not actor code, and the
// directive allows the one Await the actor keeps on purpose.
func (a *Account) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	lockstep.Pipe(ctx, a.n, func(v int, err error) Msg { return Msg(v) })
	_, _ = a.n.Await(context.Background()) //lockstep:allow LS110 the reply is already resolved
	var w waiter
	_, _ = w.Await(context.Background())
}

// run is a test-style function that no root reaches.
func run(r *lockstep.Reply[int]) {
	_, _ = r.Await(context.Background())
}

// waiter has an Await method of its own; it is not lockstep's.
type waiter struct{}

// Await returns at once.
func (waiter) Await(context.Context) (int, error) { return 0, nil }

// Wait is not an actor type's method; it awaits outside actor code.
func Wait(ctx context.Context, r *lockstep.Reply[int]) (int, error) {
	return r.Await(ctx)
}
