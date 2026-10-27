package bad

import "github.com/hmdsefi/lockstep"

// Msg is Account's message type.
type Msg struct {
	ReplyTo lockstep.ReplyTo[[]string]
}

// Account is an actor type whose state sits in messages.
type Account struct {
	items   []int
	cache   cacheState
	buf     [8]int
	rows    []string
	balance int
	peer    *Account
}

// cacheState is a field type that holds a slice.
type cacheState struct {
	items []int
}

// Hello carries an actor.
type Hello struct{ From *Account }

// Batch carries a slice.
type Batch struct{ Items []int }

// View carries a slice of an array.
type View struct{ S []int }

// Job carries a context.
type Job struct{ Ctx *lockstep.Context[Msg] }

// Ptr carries a pointer to an int.
type Ptr struct{ N *int }

// Slices carries a pointer to a slice.
type Slices struct{ P *[]int }

// Req asks for rows.
type Req struct {
	Reply lockstep.ReplyTo[[]string]
	Rows  []string
}

// The destinations of the messages above.
var (
	helloPID  lockstep.PID[Hello]
	batchPID  lockstep.PID[Batch]
	viewPID   lockstep.PID[View]
	jobPID    lockstep.PID[Job]
	ptrPID    lockstep.PID[Ptr]
	slicesPID lockstep.PID[Slices]
	reqPID    lockstep.PID[Req]
	wrapPID   lockstep.PID[Wrap]
)

// Receive holds the findings of ANL-280, one per placed leaf.
func (a *Account) Receive(ctx *lockstep.Context[Msg], m Msg) {
	_ = lockstep.Send(ctx, helloPID, Hello{From: a})             // want `^LS210: the actor itself \(a\) is placed in a message; send ctx\.Self\(\) instead$`
	_ = lockstep.Send(ctx, batchPID, Batch{Items: a.items})      // want `^LS210: actor field a\.items \(\[\]int\) is placed in a message; send a copy \(slices\.Clone, maps\.Clone\) or mark its type //lockstep:immutable$`
	_ = lockstep.Send(ctx, batchPID, Batch{Items: a.peer.items}) // want `^LS210: actor field a\.peer\.items \(\[\]int\) is placed in a message; send a copy \(slices\.Clone, maps\.Clone\) or mark its type //lockstep:immutable$`
	b := Batch{Items: a.cache.items}                             // want `^LS210: actor field a\.cache\.items \(\[\]int\) is placed in a message; send a copy \(slices\.Clone, maps\.Clone\) or mark its type //lockstep:immutable$`
	_ = lockstep.Send(ctx, batchPID, b)
	_ = lockstep.Send(ctx, viewPID, View{S: a.buf[:]})     // want `^LS210: the address of actor field a\.buf is placed in a message$`
	_ = lockstep.Send(ctx, ptrPID, Ptr{N: &a.balance})     // want `^LS210: the address of actor field a\.balance is placed in a message$`
	_ = lockstep.Send(ctx, slicesPID, Slices{P: &a.items}) // want `^LS210: the address of actor field a\.items is placed in a message$`
	_ = lockstep.Send(ctx, jobPID, Job{Ctx: ctx})          // want `^LS210: ctx \(\*lockstep\.Context\) is placed in a message; it is valid only during the current turn$`
	m.ReplyTo.Send(ctx, a.rows)                            // want `^LS210: actor field a\.rows \(\[\]string\) is placed in a message; send a copy \(slices\.Clone, maps\.Clone\) or mark its type //lockstep:immutable$`
	_ = lockstep.Ask(ctx, reqPID, func(rt lockstep.ReplyTo[[]string]) Req {
		return Req{Reply: rt, Rows: a.rows} // want `^LS210: actor field a\.rows \(\[\]string\) is placed in a message; send a copy \(slices\.Clone, maps\.Clone\) or mark its type //lockstep:immutable$`
	})
	a.flush(ctx)
}

// flush is an actor-code method reached from Receive.
func (a *Account) flush(ctx *lockstep.Context[Msg]) {
	_ = lockstep.Send(ctx, batchPID, Batch{Items: a.items}) // want `^LS210: actor field a\.items \(\[\]int\) is placed in a message; send a copy \(slices\.Clone, maps\.Clone\) or mark its type //lockstep:immutable$`
}

// ValMsg is Val's message type.
type ValMsg int

// Wrap carries a Val.
type Wrap struct{ V Val }

// Val is an actor type with a value receiver.
type Val struct{ items []int }

// Receive places the receiver, a value that holds a slice, in a message.
func (v Val) Receive(ctx *lockstep.Context[ValMsg], m ValMsg) {
	_ = lockstep.Send(ctx, wrapPID, Wrap{V: v}) // want `^LS210: the actor itself \(v\) is placed in a message; send ctx\.Self\(\) instead$`
}
