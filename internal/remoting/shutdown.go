// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"maps"
	"slices"
)

// Shutdown ends remoting on this node. In order:
//
//  1. every up outgoing session writes the frames queued for its peer, as far as its link takes
//     them;
//  2. every session gets CLOSE shutdown, best effort: by peer name, a peer's outgoing session
//     before its incoming one, then the incoming sessions still waiting for a HELLO, in the order
//     they were accepted;
//  3. the frames still queued become dead letters "shutdown", by peer name;
//  4. every link is closed and each session's down is recorded with reason "shutdown", in the
//     order of step 2;
//  5. every timer of the core is stopped (the heartbeat tick, the redials, the dial timeouts and
//     the timers that fail asks), the watches of other nodes' actors on local actors are
//     cancelled, and the outgoing watches and pending lookups are forgotten;
//  6. the core leaves the table of CoreOf; a simulated core leaves a stopped marker for its
//     system (Stopped).
//
// The dead letters, the cancels and the table update run once the lock is released, so the
// dead-letter records follow the down records. Afterwards the core opens and accepts no session,
// Deliver makes every envelope a dead letter "shutdown", Connect and Lookup return an error, and
// calls about links do nothing. Calls after the first do nothing. Call it in engine context, after
// the system's last actor stopped: the runtime's transport stop function.
func (c *Core) Shutdown() {
	c.mu.Lock()
	defer c.unlock()
	if c.shut {
		return
	}
	c.shut = true
	if c.tick != nil {
		c.tick()
		c.tick = nil
	}
	names := slices.Sorted(maps.Keys(c.peers))
	for _, name := range names {
		if s := c.peers[name].out; s != nil && s.up {
			c.flush(s)
		}
	}

	var order []*session
	listed := make(map[*session]bool)
	for _, name := range names {
		p := c.peers[name]
		for _, s := range [...]*session{p.out, p.in} {
			if s != nil {
				order = append(order, s)
				listed[s] = true
			}
		}
	}
	for _, s := range c.sessions() {
		if !listed[s] {
			order = append(order, s)
		}
	}
	for _, s := range order {
		if !s.held() && s.link.Write(AppendFrame(nil, &Frame{Kind: KindClose, Payload: []byte(CloseShutdown)})) {
			s.lastSend = c.clock.Now()
		}
	}

	for _, name := range names {
		p := c.peers[name]
		c.deadLetters(p.queue, DeadShutdown)
		p.queue = nil
	}
	for _, s := range order {
		c.down(s, DownShutdown)
	}

	for _, name := range names {
		p := c.peers[name]
		if p.redial != nil {
			p.redial()
			p.redial = nil
		}
		p.want = false
		p.watches = nil
		c.endWatches(p)
		p.lookups = nil
		p.closedIn = nil
	}
	for _, t := range c.failTimers {
		t.cancel()
	}
	c.failTimers = nil
	clear(c.closed)
	c.deferred = append(c.deferred, func() { unregister(c.sys, c) })
}
