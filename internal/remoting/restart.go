// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// recordRestart is written when a peer's handshake shows a new incarnation (§8).
const recordRestart = "lockstep.remote.restart"

// restart ends what the core holds for the incarnation of p that s's handshake showed has been
// replaced by inc, in the one event that saw it (REM-294):
//
//  1. it records lockstep.remote.restart;
//  2. the frames queued for the old incarnation become dead letters "peer-restarted";
//  3. hooks.FailAsks fails every pending ask of an actor on p with Errors.PeerRestarted;
//  4. hooks.WatchesDown ends every watch of a local actor on an actor of p with that error;
//  5. the outgoing watches are dropped, the watches of p's actors on local actors are cancelled,
//     and the lookups sent to p fail with that error;
//  6. inc becomes p's known incarnation;
//  7. the up sessions of the old incarnation other than s close with CLOSE replaced.
//
// The dead letters, the hooks, the cancels and the failed lookups run once the lock is released,
// in that order. Frames queued while p's incarnation was unknown are kept.
func (c *Core) restart(p *peer, inc uint32, s *session) {
	old := p.inc
	if c.tracer != nil {
		c.tracer.Record(recordRestart,
			engine.Attr{Key: "peer", Value: p.name},
			engine.Attr{Key: "old-inc", Value: strconv.FormatUint(uint64(old), 10)},
			engine.Attr{Key: "new-inc", Value: strconv.FormatUint(uint64(inc), 10)},
		)
	}
	err := c.errs.PeerRestarted(p.name)

	var dead, keep []*outFrame
	for _, f := range p.queue {
		if f.inc == old {
			dead = append(dead, f)
		} else {
			keep = append(keep, f)
		}
	}
	p.queue = keep
	c.deadLetters(dead, DeadPeerRestarted)

	sys, node := c.sys, p.name
	c.deferred = append(c.deferred,
		func() { hooks.FailAsks(sys, node, err) },
		func() { hooks.WatchesDown(sys, node, err) },
	)
	p.watches = nil
	c.endWatches(p)
	c.failLookups(p, err)
	p.inc = inc

	for _, st := range [...]*session{p.out, p.in} {
		if st != nil && st != s && st.up && st.peerInc == old {
			c.closeSession(st, CloseReplaced, DownReplaced)
		}
	}
}
