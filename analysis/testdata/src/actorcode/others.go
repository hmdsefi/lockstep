package actorcode

import "github.com/hmdsefi/lockstep"

// Value is an actor type with a value receiver.
type Value struct{}

func (Value) Receive(ctx *lockstep.Context[string], msg string) {} // want `^actor code: \(Value\)\.Receive$`

// Router is a generic actor type.
type Router[M any] struct{ last M }

func (r *Router[M]) Receive(ctx *lockstep.Context[M], msg M) { r.last = msg } // want `^actor code: \(\*Router\[M\]\)\.Receive$`

// NotActor's Receive takes a message of another type than the context's type argument.
type NotActor struct{}

func (NotActor) Receive(ctx *lockstep.Context[int], msg string) {}

// Other's Receive has one parameter.
type Other struct{}

func (Other) Receive(msg int) {}

// Plain has a method named like a root but no Receive, so it is not an actor type.
type Plain struct{}

func (*Plain) Stopped(ctx *lockstep.Context[int], r lockstep.StopReason) {}

// Embedder is an actor type through the Receive it promotes from Account; promoted methods are
// not its roots.
type Embedder struct{ Account }
