// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import "github.com/hmdsefi/lockstep/internal/engine"

// Counts holds per-run record counts.
type Counts struct {
	Spawns      uint64 // lockstep.spawn with result=ok
	Sends       uint64 // lockstep.send
	Asks        uint64 // lockstep.ask
	Deliveries  uint64 // lockstep.deliver
	Turns       uint64 // lockstep.turn
	Drops       uint64 // lockstep.drop
	DeadLetters uint64 // lockstep.deadletter
	MailboxFull uint64 // lockstep.mailbox.full
	Failures    uint64 // lockstep.fail
	Restarts    uint64 // lockstep.restart
	Timeouts    uint64 // lockstep.timeout
	Effects     uint64 // lockstep.effect.start
}

// Counts returns how many records of each counted kind this run has emitted so far.
func (s *Sim) Counts() Counts {
	s.check()
	return s.counts
}

// add counts one record of kind with attributes attrs (SIM-185).
func (c *Counts) add(kind string, attrs []engine.Attr) {
	switch kind {
	case "lockstep.spawn":
		if attrValue(attrs, "result") == "ok" {
			c.Spawns++
		}
	case "lockstep.send":
		c.Sends++
	case "lockstep.ask":
		c.Asks++
	case "lockstep.deliver":
		c.Deliveries++
	case "lockstep.turn":
		c.Turns++
	case "lockstep.drop":
		c.Drops++
	case "lockstep.deadletter":
		c.DeadLetters++
	case "lockstep.mailbox.full":
		c.MailboxFull++
	case "lockstep.fail":
		c.Failures++
	case "lockstep.restart":
		c.Restarts++
	case "lockstep.timeout":
		c.Timeouts++
	case "lockstep.effect.start":
		c.Effects++
	}
}
