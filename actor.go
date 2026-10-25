// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

// Actor is implemented by actor values. M is the actor's message type.
type Actor[M any] interface {
	// Receive handles one user message. It runs during a turn and must not block.
	Receive(ctx *Context[M], msg M)
}

// Starter is implemented by actors that need a hook when an incarnation starts. Started runs
// after the factory and before the first message of the incarnation.
type Starter[M any] interface {
	Started(ctx *Context[M])
}

// Stopper is implemented by actors that release resources. Stopped runs exactly once per
// incarnation: when the actor stops, before every restart, and after a failure, but not when a
// simulated node crashes. Children have already stopped when it runs.
type Stopper[M any] interface {
	Stopped(ctx *Context[M], reason StopReason)
}
