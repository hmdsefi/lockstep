// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "github.com/hmdsefi/lockstep/internal/engine"

// host is a system's engine.Host: the engine hands it the envelopes that arrive for the system's
// node (ENG-080). It is a type of its own so that *System has no exported Inbound method.
type host struct{ sys *System }

// Inbound applies an arriving envelope. A user message, or an ask request, goes to its target
// through the engine's interposer first (ENG-150, MBX-120), in call order, and is offered as a
// Sent entry that records the envelope's Seq. The sender counts as an actor sender: the remote
// Send has returned already, so a refusal or drop is only recorded, and for an ask request
// reported to askRefused (MBX-092). A message that names no live actor is a dead letter, and an
// ask request then fails back to its asker (MSG-350). An ask request whose ask address names this
// system's own node is malformed: only the asker's system allocates an ask address, and its own
// requests never arrive inbound. It is a dead letter with reason not-found, is not tracked, and
// fails no ask of this system. A reply attempt is
// applied to the system's table (MSG-410, MSG-420). A RemoteTerminated reaches the watcher once,
// unless the watch it names by number ended first (remoteTerminated: REM-282, ACT-630). Any other
// payload is dropped: the remoting core registers other nodes' watches with hooks.WatchFor.
func (h host) Inbound(env engine.Envelope) {
	switch p := env.Payload.(type) {
	case userPayload:
		from := addressOf(env.From) // the mailbox entry points at it (entry)
		if p.ask.node == h.sys.node {
			h.sys.deadLetter(from, addressOf(env.To), reasonNotFound, p.msg)
			return
		}
		r := routeRequest{
			from: &from,
			to:   addressOf(env.To),
			typ:  p.typ,
			msg:  p.msg,
			seq:  env.Seq,
			size: h.sys.sizeOf(p.msg), // engine context, no lock held (OVL-410)
		}
		if !p.ask.IsZero() {
			ask := p.ask
			r.ask = &ask
		}
		h.sys.deliverLocal(r)
	case replyPayload:
		h.sys.settle(addressOf(env.To), resolution{how: byReply, from: addressOf(env.From), v: p.v, err: p.err})
	case terminatedPayload:
		h.sys.remoteTerminated(addressOf(env.To), addressOf(env.From), p.reason, p.seq)
	}
}
