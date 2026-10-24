// Package good holds code that LS100's wall clock, randomness, goroutine, select and environment
// sub-rules accept (AT-ANL-10).
package good

import (
	"math/rand/v2"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is Account's message type.
type Msg int

// Account is an actor type.
type Account struct{}

// Receive uses the context's clock and random source, a seeded generator and duration arithmetic.
func (a *Account) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	_ = ctx.Now()
	_ = ctx.Rand().IntN(3)
	r := rand.New(rand.NewPCG(1, 2))
	_ = r.IntN(3)
	_ = time.Duration(3) * time.Second
	stampGenerated()
}

// unreached is reachable from no root.
func unreached() {
	_ = time.Now()
}

// Clock is not an actor type, so its methods are not actor code.
type Clock struct{}

// Stamp reads the wall clock outside actor code.
func (Clock) Stamp() time.Time {
	return time.Now()
}
