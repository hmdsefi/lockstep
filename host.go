// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "github.com/hmdsefi/lockstep/internal/engine"

// host is a system's engine.Host: the engine hands it the envelopes that arrive for the system's
// node (ENG-080). It is a type of its own so that *System has no exported Inbound method.
type host struct{ sys *System }

// Inbound hands an arriving user message to its target, through the engine's interposer first
// (ENG-150, MBX-120). An envelope that names no live actor is a dead letter. Payloads of other
// kinds are not handled yet: replies (MSG-410) and remote watches (ACT-602, Phase 3) are
// dropped, like anything else.
func (h host) Inbound(env engine.Envelope) {
	if p, ok := env.Payload.(userPayload); ok {
		h.sys.deliverLocal(routeRequest{
			from: addressOf(env.From),
			to:   addressOf(env.To),
			typ:  p.typ,
			msg:  p.msg,
			ask:  p.ask,
		})
	}
}
