// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"

	"github.com/hmdsefi/lockstep/internal/hooks"
)

// hookSystem returns sys as the *System that hook, the name of the hooks function, was passed. It
// panics for any other type, a nil *System included (SIM §5.9).
func hookSystem(hook string, sys any) *System {
	s, ok := sys.(*System)
	if !ok || s == nil {
		panic(fmt.Sprintf("lockstep: hooks.%s: sys has type %T, want a non-nil *lockstep.System", hook, sys))
	}
	return s
}

// discardHook is hooks.Discard (SIM-110).
func discardHook(sys any) { hookSystem("Discard", sys).discard() }

// askResolvedHook is hooks.AskResolved (SIM-200): it reads the state of reply, a *Reply[R] of any
// R, without waiting.
func askResolvedHook(reply any) bool {
	r, ok := reply.(interface{ isResolved() bool })
	if !ok {
		panic(fmt.Sprintf("lockstep: hooks.AskResolved: reply has type %T, want a *lockstep.Reply", reply))
	}
	return r.isResolved()
}

// pendingAsksHook is hooks.PendingAsks (MSG-370): the pending asks of sys in ask-counter order,
// nil for a stopped or discarded system.
func pendingAsksHook(sys any) []hooks.PendingAsk {
	s := hookSystem("PendingAsks", sys)
	asks := s.pendingAsks()
	if len(asks) == 0 {
		return nil
	}
	out := make([]hooks.PendingAsk, len(asks))
	for i, p := range asks {
		out[i] = hooks.PendingAsk{
			Ask:    p.addr.engine(),
			Asker:  p.asker.engine(),
			Target: p.target.engine(),
			// A *System sender asks as the user guardian, which no actor shares (ACT-210).
			ByActor: p.asker != s.user,
		}
	}
	return out
}

// discard discards s after its node crashed (SUP-360, MSG-360). It marks s discarded, so a later
// Spawn returns ErrStopped and Shutdown does nothing, and stopped, so a later Send, SendContext,
// Ask or NewReply with s as the Sender returns an error wrapping ErrStopped and a reply attempt is
// dropped. It then resolves every pending ask with ErrStopped, in ask-counter order, exactly as the
// stop of a system does (completeShutdown): through settle, which resolves each ask once, whatever
// a concurrent reply or timeout does. It touches no cell: no Stopped hook runs, no resource is
// closed, no Terminated is pushed and no lockstep.stop is recorded. The engine is the caller's to
// stop.
func (s *System) discard() {
	s.discarded.Store(true)
	err := s.errStopped()
	for _, ask := range s.asks.stop(&s.stopped) {
		s.settle(ask, resolution{how: byStop, from: s.user, err: err})
	}
}
