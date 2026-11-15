// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// Signals and death watches (CLU-510 to CLU-530). Each change of a turn that brings a member other
// than self up or down is a transition; at the end of the turn the actor publishes the view and
// then, for each transition in change order, ends or allows the death watches of the member's
// node and signals the subscribers. The actor alone keeps the subscribers (Membership.subs) and
// handles Subscribe and Unsubscribe in its turns, so no subscriber falls between a view and its
// signals, and no lock is needed.

// subscribeCmd is the local message of Membership.Subscribe (CLU-520).
type subscribeCmd struct{ Addr lockstep.Address }

// unsubscribeCmd is the local message of Membership.Unsubscribe (CLU-520).
type unsubscribeCmd struct{ Addr lockstep.Address }

func (subscribeCmd) memberMsg()   {}
func (unsubscribeCmd) memberMsg() {}

// transition is one node-up or node-down of a turn (CLU-510).
type transition struct {
	up   bool
	node string
	id   uint32
	left bool // node-down only: the member went down by a Left update
}

// transitions returns the transitions of the change c of a member other than self (CLU-510):
// down to up (an addition, a revive or a restored tombstone) is node-up; up to down (a leave
// included) is node-down; a supersede of an up entry is node-down of the old ID, then node-up of
// the new one when the new state is up; Suspect to Alive and back is neither.
func transitions(c change) []transition {
	wasUp := c.From.Up()
	var out []transition
	if c.Superseded && wasUp {
		out = append(out, transition{node: c.Node, id: c.OldID})
		wasUp = false
	}
	switch {
	case !wasUp && c.To.Up():
		out = append(out, transition{up: true, node: c.Node, id: c.ID})
	case wasUp && !c.To.Up():
		out = append(out, transition{node: c.Node, id: c.ID, left: c.To == Left})
	}
	return out
}

// noteTransitions keeps the transitions of the change c for the end of the turn. Changes of the own
// entry give none.
func (a *memberActor) noteTransitions(c change) {
	if c.Node != a.tbl.self {
		a.trans = append(a.trans, transitions(c)...)
	}
}

// publish ends a turn: if the table changed in it, it publishes the next view and records it
// (CLU-500), then delivers the turn's transitions in order (CLU-530). A published view is never
// changed.
func (a *memberActor) publish(ctx *lockstep.Context[memberMsg]) {
	if !a.changed {
		return
	}
	a.changed = false
	trans := a.trans
	a.trans = nil
	m := a.m
	v := &View{Version: m.view.Load().Version + 1, Self: a.tbl.Self(), Members: a.tbl.Members()}
	m.view.Store(v)
	ctl := tracer(ctx)
	if ctl != nil {
		up := 0
		for _, mb := range v.Members {
			if mb.State.Up() {
				up++
			}
		}
		ctl.Record(recordPrefix+"view", "version", strconv.FormatUint(v.Version, 10),
			"members", strconv.Itoa(len(v.Members)), "up", strconv.Itoa(up))
	}
	for _, t := range trans {
		var sig lockstep.Signal
		if t.up {
			hooks.NodeUp(m.sys, t.node)
			if ctl != nil {
				ctl.Record(recordPrefix+"node.up", "node", t.node, "id", strconv.FormatUint(uint64(t.id), 10))
			}
			sig = lockstep.NodeUp{Node: t.node, ID: t.id}
		} else {
			hooks.NodeDown(m.sys, t.node, fmt.Errorf("%w: %s", ErrNodeDown, t.node))
			if ctl != nil {
				ctl.Record(recordPrefix+"node.down", "node", t.node, "id", strconv.FormatUint(uint64(t.id), 10),
					"left", strconv.FormatBool(t.left))
			}
			sig = lockstep.NodeDown{Node: t.node, ID: t.id, Left: t.left}
		}
		// Every subscriber in subscription order; one that refused the signal, since no actor is
		// alive at its address, is removed (CLU-520).
		m.subs = slices.DeleteFunc(m.subs, func(addr lockstep.Address) bool {
			return !hooks.Signal(m.sys, hooks.EngineAddress(addr), sig)
		})
	}
}

// subscribe adds the subscriber addr (CLU-520): it sends addr one NodeUp per up member other than
// self, in name order, from the table, which is the last published view at the start of a turn,
// then appends it. A subscriber already there is left alone; one that refuses a NodeUp is not
// added.
func (a *memberActor) subscribe(addr lockstep.Address) {
	m := a.m
	if slices.Contains(m.subs, addr) {
		return
	}
	to := hooks.EngineAddress(addr)
	for _, mb := range a.tbl.Members() {
		if mb.Node == a.tbl.self || !mb.State.Up() {
			continue
		}
		if !hooks.Signal(m.sys, to, lockstep.NodeUp{Node: mb.Node, ID: mb.ID}) {
			return
		}
	}
	m.subs = append(m.subs, addr)
}

// unsubscribe removes the subscriber addr, if it is one (CLU-520).
func (a *memberActor) unsubscribe(addr lockstep.Address) {
	if i := slices.Index(a.m.subs, addr); i >= 0 {
		a.m.subs = slices.Delete(a.m.subs, i, i+1)
	}
}
