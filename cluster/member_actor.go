// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// The membership actor (CLU-030 to CLU-050, CLU-410 to CLU-460, CLU-500). It owns the member table
// and the broadcast queue, joins through the seeds, merges every message it receives into the
// table, exchanges whole tables with the other members, and publishes a new view after every turn
// that changed the table. Probing and suspicion (3.3-05), and signals, leave and restart (3.3-06)
// build on it.

// The membership actor's name under /system, and the path every member sends to (CLU-460).
const (
	actorName = "cluster"
	actorPath = "/system/" + actorName
)

// recordPrefix starts the kind of every record of the package (§8); the wire names start with it
// too, and the source of a merged update is its message's wire name without it (CLU-410).
const recordPrefix = "lockstep.cluster."

// The reasons of the sync record (§8).
const (
	syncJoin      = "join"
	syncPeriodic  = "periodic"
	syncReconnect = "reconnect"
)

// joinTick is the JoinRetryInterval timer's message: send the next join attempt (CLU-040).
type joinTick struct{}

// syncTick is the SyncInterval timer's message: send the periodic and the reconnect syncs
// (CLU-440, CLU-450).
type syncTick struct{}

// reapTimer is the DeadRetention timer armed by a change into Dead or Left: reap the entry if it
// still has this ID, incarnation and state (CLU-160).
type reapTimer struct {
	Node  string
	ID    uint32
	Inc   uint64
	State MemberState
}

func (joinTick) memberMsg()  {}
func (syncTick) memberMsg()  {}
func (reapTimer) memberMsg() {}

// memberActor is the membership actor of one node. Its state belongs to its turns.
type memberActor struct {
	m     *Membership
	seeds []string // Config.Seeds other than the own node, sorted by name (CLU-450)
	tbl   *table
	queue *broadcasts

	joinTimer lockstep.TimerID // the JoinRetryInterval timer until joined
	attempt   int              // the last join attempt, from 1 (CLU-040)
	changed   bool             // the table changed in this turn (CLU-500)
}

// newMemberActor returns the membership actor of m.
func newMemberActor(m *Membership) *memberActor {
	seeds := slices.DeleteFunc(slices.Clone(m.cfg.Seeds), func(s string) bool { return s == m.self.Node })
	slices.Sort(seeds)
	return &memberActor{m: m, seeds: seeds}
}

// Started records the start, then either joins at once (no other seed, CLU-030) or sends the first
// join attempt and arms the retry timer (CLU-040). Every incarnation starts from a table of only
// the own entry: rebuilding a restarted actor's table from the last published view, and recording
// restart=true, come with 3.3-06 (CLU-620). Only the joined flag already survives a restart.
func (a *memberActor) Started(ctx *lockstep.Context[memberMsg]) {
	cfg := &a.m.cfg
	a.tbl = newTable(a.m.self, cfg.DeadRetention, ctx.Now())
	a.queue = newBroadcasts()
	if ctl := tracer(ctx); ctl != nil {
		self := a.tbl.Self()
		ctl.Record(recordPrefix+"start", "node", self.Node, "id", strconv.FormatUint(uint64(self.ID), 10),
			"inc", strconv.FormatUint(self.Incarnation, 10), "seeds", strconv.Itoa(len(cfg.Seeds)), "restart", "false")
	}
	switch {
	case a.m.joined:
		a.startSync(ctx)
	case len(a.seeds) == 0:
		a.becomeJoined(ctx)
	default:
		a.joinAttempt(ctx)
		a.joinTimer = ctx.Every(cfg.JoinRetryInterval, joinTick{})
	}
	a.publish(ctx)
}

// Receive handles one message, then publishes a view if the table changed (CLU-500).
func (a *memberActor) Receive(ctx *lockstep.Context[memberMsg], msg memberMsg) {
	switch msg := msg.(type) {
	case syncReq:
		if a.mergeMessage(ctx, wireSyncReq, msg.From, msg.Members) && msg.From.Node != a.tbl.self {
			a.send(ctx, msg.From.Node, syncResp{From: toUpdate(a.tbl.Self()), Members: a.entries()})
		}
	case syncResp:
		if a.mergeMessage(ctx, wireSyncResp, msg.From, msg.Members) && !a.m.joined {
			a.becomeJoined(ctx)
		}
	case ping:
		a.mergeMessage(ctx, wirePing, msg.From, msg.Updates)
	case pingReq:
		a.mergeMessage(ctx, wirePingReq, msg.From, msg.Updates)
	case ack:
		a.mergeMessage(ctx, wireAck, msg.From, msg.Updates)
	case leave:
		a.mergeMessage(ctx, wireLeave, msg.From, nil)
	case joinTick:
		if !a.m.joined {
			a.joinAttempt(ctx)
		}
	case syncTick:
		a.syncRound(ctx)
	case reapTimer:
		a.reap(ctx, msg)
	case update:
		// A bare update is registered only to be encodable on its own; it carries no sender.
	}
	a.publish(ctx)
}

// tracer returns the controls of ctx when tracing is on, else nil, so that records are built only
// when they are emitted (§8).
func tracer(ctx *lockstep.Context[memberMsg]) hooks.ActorControl {
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		return ctl
	}
	return nil
}

// send sends msg to the membership actor of node (CLU-460). A send error is ignored: the protocol
// tolerates loss, and only the timers retry.
func (*memberActor) send(ctx *lockstep.Context[memberMsg], node string, msg memberMsg) {
	if err := lockstep.Send(ctx, lockstep.PIDAt[memberMsg](node, actorPath), msg); err != nil {
		return // ignored: a lost sync is made up for by the next timer
	}
}

// entries returns every entry of the table, sorted by name, in wire form.
func (a *memberActor) entries() []update {
	ms := a.tbl.Members()
	out := make([]update, len(ms))
	for i, m := range ms {
		out[i] = toUpdate(m)
	}
	return out
}

// sendSync sends a syncReq with the whole table to peer and records it with reason (CLU-040,
// CLU-440, CLU-450). attempt is the join attempt, or 0 for the other reasons.
func (a *memberActor) sendSync(ctx *lockstep.Context[memberMsg], peer, reason string, attempt int) {
	a.send(ctx, peer, syncReq{From: toUpdate(a.tbl.Self()), Members: a.entries()})
	if ctl := tracer(ctx); ctl != nil {
		if attempt > 0 {
			ctl.Record(recordPrefix+"sync", "peer", peer, "reason", reason, "attempt", strconv.Itoa(attempt))
		} else {
			ctl.Record(recordPrefix+"sync", "peer", peer, "reason", reason)
		}
	}
}

// joinAttempt sends the next join attempt: a syncReq to every seed other than the own node, in
// the order of Config.Seeds (CLU-040).
func (a *memberActor) joinAttempt(ctx *lockstep.Context[memberMsg]) {
	a.attempt++
	for _, s := range a.m.cfg.Seeds {
		if s != a.tbl.self {
			a.sendSync(ctx, s, syncJoin, a.attempt)
		}
	}
}

// becomeJoined marks the node joined, stops the join attempts, records it and starts the periodic
// syncs (CLU-030).
func (a *memberActor) becomeJoined(ctx *lockstep.Context[memberMsg]) {
	a.m.joined = true
	if a.joinTimer != 0 {
		ctx.CancelTimer(a.joinTimer)
		a.joinTimer = 0
	}
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(recordPrefix+"joined", "members", strconv.Itoa(a.tbl.Len()))
	}
	a.startSync(ctx)
}

// startSync arms the SyncInterval timer (CLU-440).
func (a *memberActor) startSync(ctx *lockstep.Context[memberMsg]) {
	ctx.Every(a.m.cfg.SyncInterval, syncTick{})
}

// syncRound sends the periodic sync to one Alive member drawn at random (draw 3 of §6), then a
// reconnect sync to every Dead member and every seed without an up entry (CLU-440, CLU-450).
func (a *memberActor) syncRound(ctx *lockstep.Context[memberMsg]) {
	members := a.tbl.Members()
	periodic, ok := periodicTarget(ctx.Rand(), members, a.tbl.self)
	if ok {
		a.sendSync(ctx, periodic, syncPeriodic, 0)
	}
	for _, peer := range reconnectTargets(members, a.seeds, a.tbl.self, periodic) {
		a.sendSync(ctx, peer, syncReconnect, 0)
	}
}

// periodicTarget draws the target of the periodic sync: r.IntN(len) over the Alive members other
// than self, in name order (members is sorted by name). It draws nothing and returns false when
// there is no such member (draw 3 of §6).
func periodicTarget(r *rand.Rand, members []Member, self string) (string, bool) {
	var alive []string
	for _, m := range members {
		if m.State == Alive && m.Node != self {
			alive = append(alive, m.Node)
		}
	}
	if len(alive) == 0 {
		return "", false
	}
	return alive[r.IntN(len(alive))], true
}

// reconnectTargets returns the peers of the reconnect syncs (CLU-450): every Dead member other
// than self in name order (members is sorted by name), then every seed of seeds (sorted, self
// excluded) that has no up entry, in name order, leaving out the periodic target and the peers
// already listed.
func reconnectTargets(members []Member, seeds []string, self, periodic string) []string {
	var out []string
	for _, m := range members {
		if m.State == Dead && m.Node != self {
			out = append(out, m.Node)
		}
	}
	for _, s := range seeds {
		i, ok := slices.BinarySearchFunc(members, s, compareNode)
		if (ok && members[i].State.Up()) || s == periodic || slices.Contains(out, s) {
			continue
		}
		out = append(out, s)
	}
	return out
}

// mergeMessage merges a received message into the table (CLU-410, CLU-710): it drops the message
// when its From is invalid, then merges From, then each update in slice order, dropping the
// invalid ones. wire is the message's wire name; the source of its changes is that name without
// the prefix. It reports whether the message was merged.
func (a *memberActor) mergeMessage(ctx *lockstep.Context[memberMsg], wire string, from update, updates []update) bool {
	if reason := validUpdate(from); reason != "" {
		a.invalid(ctx, from.Node, wire, reason)
		return false
	}
	source := strings.TrimPrefix(wire, recordPrefix)
	a.merge(ctx, from, source)
	for _, u := range updates {
		if reason := validUpdate(u); reason != "" {
			a.invalid(ctx, from.Node, wire, reason)
			continue
		}
		a.merge(ctx, u, source)
	}
	return true
}

// invalid records an invalid update that was dropped (CLU-710).
func (*memberActor) invalid(ctx *lockstep.Context[memberMsg], peer, wire, reason string) {
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(recordPrefix+"invalid", "peer", peer, "type", wire, "reason", reason)
	}
}

// merge applies one valid update from source to the table. An update about the own entry changes
// nothing here; refuting it belongs to probing (CLU-310).
func (a *memberActor) merge(ctx *lockstep.Context[memberMsg], u update, source string) {
	if c, res := a.tbl.Apply(u.member(), source, ctx.Now()); res == applyChanged {
		a.applied(ctx, c)
	}
}

// applied handles one change of the table (CLU-170): it records it, enqueues the new entry for
// broadcast, and arms the reap timer of a change into Dead or Left (CLU-160).
func (a *memberActor) applied(ctx *lockstep.Context[memberMsg], c change) {
	a.changed = true
	if ctl := tracer(ctx); ctl != nil {
		from := "none"
		if c.From != 0 {
			from = c.From.String()
		}
		ctl.Record(recordPrefix+"member", "node", c.Node, "id", strconv.FormatUint(uint64(c.ID), 10),
			"inc", strconv.FormatUint(c.Inc, 10), "from", from, "to", c.To.String(), "source", c.Source)
	}
	a.queue.enqueue(toUpdate(c.member()))
	if !c.To.Up() {
		ctx.After(a.m.cfg.DeadRetention, reapTimer{Node: c.Node, ID: c.ID, Inc: c.Inc, State: c.To})
	}
}

// reap reaps the entry the timer t was armed for, if it has not changed since (CLU-160).
func (a *memberActor) reap(ctx *lockstep.Context[memberMsg], t reapTimer) {
	if !a.tbl.Reap(t.Node, t.ID, t.Inc, t.State, ctx.Now()) {
		return
	}
	a.changed = true
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(recordPrefix+"reap", "node", t.Node, "id", strconv.FormatUint(uint64(t.ID), 10),
			"inc", strconv.FormatUint(t.Inc, 10))
	}
}

// publish ends a turn: if the table changed in it, it publishes the next view and records it
// (CLU-500). A published view is never changed.
func (a *memberActor) publish(ctx *lockstep.Context[memberMsg]) {
	if !a.changed {
		return
	}
	a.changed = false
	v := &View{Version: a.m.view.Load().Version + 1, Self: a.tbl.Self(), Members: a.tbl.Members()}
	a.m.view.Store(v)
	if ctl := tracer(ctx); ctl != nil {
		up := 0
		for _, m := range v.Members {
			if m.State.Up() {
				up++
			}
		}
		ctl.Record(recordPrefix+"view", "version", strconv.FormatUint(v.Version, 10),
			"members", strconv.Itoa(len(v.Members)), "up", strconv.Itoa(up))
	}
}
