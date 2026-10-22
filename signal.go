// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

// Signal is a notification from lockstep itself, delivered through SignalReceiver[M].Signal and
// never as an M. It is implemented only by Terminated and ChildFailed (SUP), Overloaded (OVL) and,
// from Phase 3, NodeDown and NodeUp (CLU).
type Signal interface {
	signal() // unexported: no other implementations
}

// SignalReceiver is implemented by actors that receive lockstep's notifications (MSG-810). An actor
// without it ignores signals; its supervisor strategy still handles its children's failures
// (SUP-150 to SUP-230).
type SignalReceiver[M any] interface {
	Signal(ctx *Context[M], sig Signal)
}
