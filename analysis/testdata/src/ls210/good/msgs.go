package good

import (
	"slices"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Snapshot is immutable, so sharing it is safe.
//
//lockstep:immutable
type Snapshot struct{ Rows []string }

// Msg is Account's message type.
type Msg struct {
	Items []int
}

// Account is an actor type whose messages carry copies and safe values.
type Account struct {
	balance int
	name    string
	when    time.Time
	items   []int
	snap    Snapshot
	snapPtr *Snapshot
	self    lockstep.PID[Msg]
	addr    lockstep.Address
}

// Deposit carries an int.
type Deposit struct{ Amount int }

// Batch carries a slice.
type Batch struct{ Items []int }

// Who carries a PID.
type Who struct{ PID lockstep.PID[Msg] }

// Snap carries an immutable value.
type Snap struct{ S Snapshot }

// Stamp carries a string and a time.
type Stamp struct {
	Name string
	When time.Time
	Addr lockstep.Address
}

// The destinations of the messages above.
var (
	depositPID  lockstep.PID[Deposit]
	batchPID    lockstep.PID[Batch]
	whoPID      lockstep.PID[Who]
	snapPID     lockstep.PID[Snap]
	snapOnePID  lockstep.PID[Snapshot]
	snapPtrPID  lockstep.PID[*Snapshot]
	stampPID    lockstep.PID[Stamp]
	selfPID     lockstep.PID[lockstep.PID[Msg]]
	itemsPID    lockstep.PID[[]int]
	plainPID    lockstep.PID[Plain]
	plainBoxPID lockstep.PID[Box]
)

// Receive sends values that share nothing with the actor.
func (a *Account) Receive(ctx *lockstep.Context[Msg], m Msg) {
	_ = lockstep.Send(ctx, depositPID, Deposit{Amount: a.balance})
	_ = lockstep.Send(ctx, batchPID, Batch{Items: slices.Clone(a.items)})
	_ = lockstep.Send(ctx, whoPID, Who{PID: ctx.Self()})
	_ = lockstep.Send(ctx, whoPID, Who{PID: a.self})
	_ = lockstep.Send(ctx, snapPID, Snap{S: a.snap})
	_ = lockstep.Send(ctx, snapOnePID, a.snap)
	_ = lockstep.Send(ctx, snapPtrPID, a.snapPtr)
	_ = lockstep.Send(ctx, stampPID, Stamp{Name: a.name, When: a.when, Addr: a.addr})
	_ = lockstep.Send(ctx, selfPID, a.self)
	items := slices.Clone(a.items)
	_ = lockstep.Send(ctx, batchPID, Batch{Items: items})
	_ = lockstep.Send(ctx, itemsPID, []int{a.balance})
	ctx.After(time.Second, Msg{Items: a.items})
	ctx.Every(time.Second, Msg{Items: a.items})
	_ = lockstep.Ask(ctx, batchPID, func(rt lockstep.ReplyTo[int]) Batch {
		return Batch{Items: slices.Clone(a.items)}
	})
	// A function literal is not a method of an actor type.
	later := func() {
		_ = lockstep.Send(ctx, batchPID, Batch{Items: a.items})
	}
	later()
}

// Plain is a value receiver without references.
type Plain struct{ n int }

// Box carries a Plain.
type Box struct{ P Plain }

// PlainMsg is Plain's message type.
type PlainMsg int

// Receive places the receiver, which holds no references, in messages.
func (p Plain) Receive(ctx *lockstep.Context[PlainMsg], m PlainMsg) {
	_ = lockstep.Send(ctx, plainPID, p)
	_ = lockstep.Send(ctx, plainBoxPID, Box{P: p})
}

// share is a function, not a method of an actor type.
func share(sys *lockstep.System, a *Account) {
	_ = lockstep.Send(sys, batchPID, Batch{Items: a.items})
	_ = lockstep.Send(sys, snapPID, Snap{})
}

// helper is a method of a type that is not an actor type.
type helper struct{ items []int }

func (h *helper) share(sys *lockstep.System) {
	_ = lockstep.Send(sys, batchPID, Batch{Items: h.items})
}
