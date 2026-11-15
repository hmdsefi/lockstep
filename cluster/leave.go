// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"strconv"

	"github.com/hmdsefi/lockstep"
)

// Leave, shutdown and restart (CLU-600 to CLU-620). A node leaves when Membership.Leave asks the
// actor to, or when the actor stops for any reason but a restart, as System.Shutdown stops it; the
// other members merge the leave's Left and signal NodeDown with Left set. A restarted actor keeps
// the table of the last published view.

// leaveCmd is the local message of Membership.Leave: leave the cluster, then stop (CLU-600).
type leaveCmd struct{}

func (leaveCmd) memberMsg() {}

// leave makes the node leave, unless it has left already (CLU-600): the own entry becomes Left at
// its incarnation (recorded and enqueued as every change), every up member other than self gets a
// leave from the new own entry, in name order, and the view that shows it is published.
func (a *memberActor) leave(ctx *lockstep.Context[memberMsg]) {
	self := a.tbl.Self()
	if self.State == Left {
		return
	}
	c, _ := a.tbl.SetSelf(Left, self.Incarnation, ctx.Now())
	a.applied(ctx, c)
	from := toUpdate(a.tbl.Self())
	n := 0
	for _, m := range a.tbl.Members() {
		if m.Node != a.tbl.self && m.State.Up() {
			a.send(ctx, m.Node, leave{From: from})
			n++
		}
	}
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(recordPrefix+"leave", "members", strconv.Itoa(n))
	}
	a.m.left.Store(true)
	a.publish(ctx)
}

// Stopped leaves the cluster as Leave does, without stopping again, unless the node has left
// already or the incarnation ends for a restart (CLU-610). So System.Shutdown, a Stop of the actor
// and a failure that stops it all leave gracefully.
func (a *memberActor) Stopped(ctx *lockstep.Context[memberMsg], reason lockstep.StopReason) {
	if a.tbl == nil || reason.Kind == lockstep.StopRestarting {
		return
	}
	a.leave(ctx)
}

// rebuild sets the table of a restarted actor from the last published view: every entry with its
// ID, incarnation and state, the own entry from the view's Self, and no retired IDs or tombstones
// (CLU-620). The suspicion timer of every Suspect entry and the reap timer of every Dead or Left
// one are armed again with their full durations. Nothing is signalled and no view is published:
// the subscribers and the last view already show this table.
func (a *memberActor) rebuild(ctx *lockstep.Context[memberMsg]) {
	a.tbl = rebuildTable(a.m.view.Load(), a.m.cfg.DeadRetention, ctx.Now())
	for _, m := range a.tbl.Members() {
		if m.Node != a.tbl.self {
			a.armTimers(ctx, m)
		}
	}
}
