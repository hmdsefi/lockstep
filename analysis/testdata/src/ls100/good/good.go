// Package good holds code that LS100 accepts (AT-ANL-10): this file has the clock, randomness and
// Close calls, order.go the sorted map iteration and the calls of ANL-140's packages that do not
// block.
package good

import (
	"math/rand/v2"
	"net"
	"os"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is Account's message type.
type Msg int

// Account is an actor type.
type Account struct {
	m    map[string]int
	conn net.Conn
	f    *os.File
}

// Receive uses the context's clock and random source, a seeded generator and duration arithmetic.
func (a *Account) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	_ = ctx.Now()
	_ = ctx.Rand().IntN(3)
	r := rand.New(rand.NewPCG(1, 2))
	_ = r.IntN(3)
	_ = time.Duration(3) * time.Second
	stampGenerated()
	_ = a.sum()
	sorted(a.m, []int{1})
	nonBlocking(nil, nil, nil, nil, nil)
}

// Stopped closes the connection and the file: methods named Close are not blocking calls.
func (a *Account) Stopped(ctx *lockstep.Context[Msg], r lockstep.StopReason) {
	_ = a.conn.Close()
	_ = a.f.Close()
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
