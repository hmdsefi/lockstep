// Package bad holds the LS230 findings: members of a sealed message interface and self-message
// types with no case in Receive or its delegates (ANL-310 to ANL-330, AT-ANL-21, AT-ANL-22).
package bad

import (
	"context"
	"time"

	"github.com/hmdsefi/lockstep"
)

// AccountMsg is sealed. Its members are Deposit, Withdraw, GetBalance and *Close.
type AccountMsg interface{ accountMsg() } // want AccountMsg:`^sealed\(\*Close, Deposit, GetBalance, Withdraw\)$`

type (
	Deposit    struct{ N int }
	Withdraw   struct{ N int }
	GetBalance struct{}
	Close      struct{}
)

func (Deposit) accountMsg()    {}
func (Withdraw) accountMsg()   {}
func (GetBalance) accountMsg() {}
func (*Close) accountMsg()     {}

// Gen is generic and Sub is an interface: neither is a member. Sub is sealed, with no member.
type Gen[T any] struct{ v T }

func (Gen[T]) accountMsg() {}

type Sub interface { // want Sub:`^sealed\(\)$`
	AccountMsg
	Sub()
}

// Account misses *Close and Withdraw. Withdraw is a self-message too; it is reported once, as a
// member.
type Account struct{}

func (a *Account) Started(ctx *lockstep.Context[AccountMsg]) {
	ctx.After(time.Second, Withdraw{N: 1})
}

func (a *Account) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	switch msg.(type) { // want `^LS230: \(\*Account\)\.Receive has no case for message type \*Close of AccountMsg$` `^LS230: \(\*Account\)\.Receive has no case for message type Withdraw of AccountMsg$`
	case Deposit:
	case GetBalance:
	}
}

// Branch has Account's Receive, promoted: its findings are Account's, reported once.
type Branch struct{ Account }

// Ledger switches in its delegate handle, where the parameter m plays the role of msg.
type Ledger struct{}

func (a *Ledger) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	a.handle(ctx, msg)
}

func (a *Ledger) handle(ctx *lockstep.Context[AccountMsg], m AccountMsg) {
	switch m.(type) { // want `^LS230: \(\*Ledger\)\.Receive has no case for message type \*Close of AccountMsg$` `^LS230: \(\*Ledger\)\.Receive has no case for message type GetBalance of AccountMsg$` `^LS230: \(\*Ledger\)\.Receive has no case for message type Withdraw of AccountMsg$`
	case Deposit:
	}
}

// Teller has comma-ok assertions only: the findings are at the name of Receive.
type Teller struct{}

func (t *Teller) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) { // want `^LS230: \(\*Teller\)\.Receive has no case for message type \*Close of AccountMsg$` `^LS230: \(\*Teller\)\.Receive has no case for message type GetBalance of AccountMsg$` `^LS230: \(\*Teller\)\.Receive has no case for message type Withdraw of AccountMsg$`
	if d, ok := msg.(Deposit); ok {
		_ = d
	}
	_ = msg.(Withdraw) // not comma-ok: adds no case
}

// Relay switches only in its delegates. The findings are at the first type switch in visit order:
// first is visited before second, and deep, which first calls, before second too.
type Relay struct{}

func (r *Relay) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	first(msg)
	r.second(msg)
}

func first(m AccountMsg) {
	deep(0, m)
}

func deep(n int, m AccountMsg) {
	switch m.(type) { // want `^LS230: \(\*Relay\)\.Receive has no case for message type \*Close of AccountMsg$` `^LS230: \(\*Relay\)\.Receive has no case for message type Withdraw of AccountMsg$`
	case Deposit:
	}
}

func (r *Relay) second(m AccountMsg) {
	first(m) // visited already
	switch m.(type) {
	case GetBalance:
	}
}

// tick, fetched and the others are Poller's and Viewer's self-messages.
type (
	tick    struct{}
	fetch   struct{}
	fetched struct {
		v   int
		err error
	}
	tock  struct{}
	piped struct{ n int }
)

func (fetch) Name() string                         { return "fetch" }
func (fetch) Run(ctx context.Context) (int, error) { return 0, nil }

// Poller sends tick and fetched to itself and handles tick only (AT-ANL-22).
type Poller struct{}

func (p *Poller) Started(ctx *lockstep.Context[any]) {
	ctx.After(time.Second, tick{})
}

func (p *Poller) Receive(ctx *lockstep.Context[any], msg any) {
	switch msg.(type) { // want `^LS230: \(\*Poller\)\.Receive has no case for message type fetched, which the actor sends to itself$`
	case tick:
		lockstep.Do(ctx, fetch{}, func(v int, err error) any { return fetched{v, err} })
	}
}

// Echo shares Poller's message type; Poller's self-messages are not Echo's.
type Echo struct{}

func (e *Echo) Receive(ctx *lockstep.Context[any], msg any) {
	switch msg.(type) {
	case string:
	}
}

// Viewer has a value receiver and finds its self-messages in a helper and a Pipe literal.
type Viewer struct{ r *lockstep.Reply[int] }

func (v Viewer) Receive(ctx *lockstep.Context[any], msg any) {
	switch msg.(type) { // want `^LS230: Viewer\.Receive has no case for message type piped, which the actor sends to itself$` `^LS230: Viewer\.Receive has no case for message type tock, which the actor sends to itself$`
	case string:
		v.arm(ctx)
	}
}

func (v Viewer) arm(ctx *lockstep.Context[any]) {
	ctx.Every(time.Second, tock{})
	lockstep.Pipe(ctx, v.r, func(n int, err error) any {
		if err != nil {
			return nil
		}
		return piped{n}
	})
}

// Vault is generic; its delegate covers GetBalance only.
type Vault[T any] struct{ v T }

func (v *Vault[T]) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	v.handle(msg)
}

func (v *Vault[T]) handle(m AccountMsg) {
	switch m.(type) { // want `^LS230: \(\*Vault\)\.Receive has no case for message type \*Close of AccountMsg$` `^LS230: \(\*Vault\)\.Receive has no case for message type Deposit of AccountMsg$` `^LS230: \(\*Vault\)\.Receive has no case for message type Withdraw of AccountMsg$`
	case GetBalance:
	}
}

// Shape is sealed through the embedded base shapeBase, which is not a member (ANL-320). Painter
// misses Square only.
type Shape interface{ shape() } // want Shape:`^sealed\(Circle, Square\)$`

type shapeBase struct{}

func (shapeBase) shape() {}

type (
	Circle struct{ shapeBase }
	Square struct{ shapeBase }
)

type Painter struct{}

func (p *Painter) Receive(ctx *lockstep.Context[Shape], msg Shape) {
	switch msg.(type) { // want `^LS230: \(\*Painter\)\.Receive has no case for message type Square of Shape$`
	case Circle:
	}
}
