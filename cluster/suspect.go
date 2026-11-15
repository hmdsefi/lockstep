// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"math"
	"strconv"

	"github.com/hmdsefi/lockstep"
)

// Suspicion and refutation (CLU-300 to CLU-320). A member that becomes Suspect is declared Dead
// SuspicionTimeout later unless it changed meanwhile; a member that learns it is suspected or
// declared dead refutes it by raising its own incarnation.

// sourceSuspicion is the source of the change a suspicion timer makes (§8).
const sourceSuspicion = "suspicion"

// suspectTimeout is the SuspicionTimeout timer armed by a change of Node into Suspect with ID and
// Inc (CLU-300).
type suspectTimeout struct {
	Node string
	ID   uint32
	Inc  uint64
}

func (suspectTimeout) memberMsg() {}

// armSuspicion arms the suspicion timer of a change into Suspect (CLU-300). Every such change arms
// one, a change from Suspect at a lower incarnation too, since the timer of that one is stale.
// Timers are never cancelled; a stale one does nothing when it fires.
func (a *memberActor) armSuspicion(ctx *lockstep.Context[memberMsg], c change) {
	ctx.After(a.m.cfg.SuspicionTimeout, suspectTimeout{Node: c.Node, ID: c.ID, Inc: c.Inc})
}

// suspicionExpired declares the member of t Dead with the same incarnation if its entry still has
// t's ID and incarnation and is Suspect; otherwise the timer is stale and does nothing (CLU-300).
func (a *memberActor) suspicionExpired(ctx *lockstep.Context[memberMsg], t suspectTimeout) {
	m, ok := a.tbl.Get(t.Node)
	if !ok || m.ID != t.ID || m.Incarnation != t.Inc || m.State != Suspect {
		return
	}
	m.State = Dead
	if c, res := a.tbl.Apply(m, true, sourceSuspicion, ctx.Now()); res == applyChanged {
		a.applied(ctx, c)
	}
}

// refute handles a merged update u about the own entry (own name and ID): a Suspect or Dead at
// the own incarnation or above sets the own incarnation to u.Inc + 1, Alive, enqueues the own
// entry for broadcast and records the refutation (CLU-310). Any other update about self changes
// nothing, and so does one at the largest incarnation, which cannot be raised.
func (a *memberActor) refute(ctx *lockstep.Context[memberMsg], u update) {
	self := a.tbl.Self()
	if (u.State != Suspect && u.State != Dead) || u.Inc < self.Incarnation || u.Inc == math.MaxUint64 || self.State != Alive {
		return
	}
	c, ok := a.tbl.SetSelf(Alive, u.Inc+1, ctx.Now())
	if !ok {
		return
	}
	a.applied(ctx, c)
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(recordPrefix+"refute", "inc", strconv.FormatUint(c.Inc, 10), "about", u.State.String())
	}
}
