// Package good holds actors whose Receive covers every expected message type, or inspects no
// message type at all (ANL-310 to ANL-330, AT-ANL-21, AT-ANL-22). Their sealed message type is
// msgs.AccountMsg: a sealed interface declared here would carry a fact, and good packages hold
// no want comment.
package good

import (
	"context"
	"fmt"
	"time"

	"github.com/hmdsefi/lockstep"

	. "ls230/msgs"
)

// Account covers every member; nil and default add nothing.
type Account struct{}

func (a *Account) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	switch m := msg.(type) {
	case nil:
	case Deposit, Withdraw:
		_ = m
	case GetBalance:
	case *Close:
	default:
	}
}

// Bank covers Deposit and Withdraw through the interface Money.
type Bank struct{}

func (b *Bank) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	switch m := msg.(type) {
	case Money:
		_ = m.Amount()
	case GetBalance, *Close:
	}
}

// Logger never inspects the type of msg: it handles messages uniformly, its self-message included.
type Logger struct{ n int }

func (l *Logger) Started(ctx *lockstep.Context[AccountMsg]) {
	ctx.After(time.Second, GetBalance{})
}

func (l *Logger) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	l.n++
	ctx.After(time.Second, msg) // an interface type is not a self-message type
}

// Mixed covers Deposit by a comma-ok assertion and the rest by a type switch.
type Mixed struct{}

func (x *Mixed) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	if d, ok := msg.(Deposit); ok {
		_ = d
		return
	}
	var _, ok = msg.(*Close)
	_ = ok
	switch msg.(type) {
	case Withdraw, GetBalance:
	}
}

// Ledger covers everything in a delegate called as a method expression.
type Ledger struct{}

func (l *Ledger) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	(*Ledger).handle(l, msg)
}

func (l *Ledger) handle(m AccountMsg) {
	switch m.(type) {
	case Deposit, Withdraw, GetBalance, *Close:
	}
}

// Forwarder passes msg to a function field, which may handle any message: nothing is reported
// although its assertion covers Deposit only (ANL-330).
type Forwarder struct {
	other func(*lockstep.Context[AccountMsg], AccountMsg)
}

func (f *Forwarder) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	if d, ok := msg.(Deposit); ok {
		_ = d
		return
	}
	f.other(ctx, msg)
}

// Quiet has no name for its message parameter, so it can switch on nothing.
type Quiet struct{}

func (Quiet) Started(ctx *lockstep.Context[any]) { ctx.After(time.Second, tick{}) }

func (Quiet) Receive(*lockstep.Context[any], any) {}

type (
	tick    struct{}
	fetch   struct{}
	fetched struct {
		v   int
		err error
	}
	later struct{}
)

func (fetch) Name() string                         { return "fetch" }
func (fetch) Run(ctx context.Context) (int, error) { return 0, nil }

// Poller handles both of its self-messages (AT-ANL-22). The result of a done function that is
// not a literal is not a self-message type.
type Poller struct{ r *lockstep.Reply[int] }

func (p *Poller) Started(ctx *lockstep.Context[any]) {
	ctx.After(time.Second, tick{})
	lockstep.Do(ctx, fetch{}, done)
}

func (p *Poller) Receive(ctx *lockstep.Context[any], msg any) {
	switch msg.(type) {
	case tick:
		lockstep.Do(ctx, fetch{}, func(v int, err error) any { return fetched{v, err} })
		lockstep.Pipe(ctx, p.r, func(n int, err error) any { return nil })
	case fetched:
	}
}

func done(v int, err error) any { return later{} }

// Switcher passes the variable its type switch binds, which holds msg unchanged in the default
// clause, to a delegate that covers the rest (ANL-330).
type Switcher struct{}

func (s *Switcher) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	switch m := msg.(type) {
	case Deposit:
		_ = m.N
	default:
		s.rest(m)
	}
}

func (s *Switcher) rest(m AccountMsg) {
	switch m.(type) {
	case Withdraw, GetBalance, *Close:
	}
}

// Splitter forwards the variable its type switch binds in a clause that lists several types,
// where it holds msg unchanged, to a function field: nothing is reported (ANL-330).
type Splitter struct {
	other func(AccountMsg)
}

func (s *Splitter) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	switch m := msg.(type) {
	case Deposit, Withdraw:
		s.other(m)
	}
}

// Passer forwards the bound variable of its default clause to a function field.
type Passer struct {
	other func(AccountMsg)
}

func (p *Passer) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	switch msg := msg.(type) {
	case Deposit:
	default:
		p.other(msg)
	}
}

// Table forwards msg through an element of a slice of functions, a call with no static callee.
type Table struct {
	h []func(*lockstep.Context[AccountMsg], AccountMsg)
}

func (t *Table) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	if _, ok := msg.(Deposit); ok {
		return
	}
	t.h[0](ctx, msg)
}

// Printer handles messages uniformly. describe takes any, not AccountMsg, so it is not a delegate
// and its type switch is not on the message (ANL-330).
type Printer struct{}

func (p *Printer) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	_ = describe(msg)
}

func describe(v any) string {
	switch v := v.(type) {
	case fmt.Stringer:
		return v.String()
	}
	return ""
}

// Noter covers Note's members; the embedded base is not one (ANL-320).
type Noter struct{}

func (n *Noter) Receive(ctx *lockstep.Context[Note], msg Note) {
	switch msg.(type) {
	case Memo, *Ping:
	}
}

// Box is generic and covers every member in a delegate.
type Box[T any] struct{ v T }

func (b *Box[T]) Receive(ctx *lockstep.Context[AccountMsg], msg AccountMsg) {
	b.handle(msg)
}

func (b *Box[T]) handle(m AccountMsg) {
	switch m.(type) {
	case Deposit, Withdraw, GetBalance, *Close:
	}
}
