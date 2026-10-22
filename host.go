// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "github.com/hmdsefi/lockstep/internal/engine"

// host is a system's engine.Host: the engine hands it the envelopes that arrive for the system's
// node (ENG-080). It is a type of its own so that *System has no exported Inbound method.
type host struct{ sys *System }

// Inbound applies an arriving envelope. A user message, or an ask request, goes to its target
// through the engine's interposer first (ENG-150, MBX-120); one that names no live actor is a
// dead letter, and an ask request then fails back to its asker (MSG-350). A reply attempt is
// applied to the system's table (MSG-410, MSG-420). Remote watches arrive with 1.2-09; any other
// payload is dropped.
func (h host) Inbound(env engine.Envelope) {
	switch p := env.Payload.(type) {
	case userPayload:
		h.sys.deliverLocal(routeRequest{
			from: addressOf(env.From),
			to:   addressOf(env.To),
			typ:  p.typ,
			msg:  p.msg,
			ask:  p.ask,
		})
	case replyPayload:
		h.sys.settle(addressOf(env.To), resolution{how: byReply, from: addressOf(env.From), v: p.v, err: p.err})
	}
}
