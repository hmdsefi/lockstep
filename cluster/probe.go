// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"strconv"

	"github.com/hmdsefi/lockstep"
)

// Probing (CLU-200 to CLU-250). Every ProbeInterval the membership actor resolves its outstanding
// probe, then pings the next member of a shuffled probe list. A probe that has no ack after
// ProbeTimeout asks up to IndirectProbes helpers to ping the target for it; a probe that has no
// ack by the next tick fails, and its target becomes Suspect (suspect.go). Every ping, pingReq and
// ack carries the updates of the broadcast queue (CLU-400).

// The via attribute of the ack record (§8): the ack came from the target itself, or from a helper.
const (
	viaDirect   = "direct"
	viaIndirect = "indirect"
)

// sourceProbe is the source of the change a failed probe makes (§8).
const sourceProbe = "probe"

// probeTick is the ProbeInterval timer's message: resolve the outstanding probe, then probe the
// next member (CLU-200).
type probeTick struct{}

// probeTimeout is the ProbeTimeout timer of the probe Seq: send the indirect probes if it has no
// ack yet (CLU-230).
type probeTimeout struct{ Seq uint64 }

// relayExpire is the ProbeTimeout timer of the relay Seq: drop it if it is still there (CLU-230).
type relayExpire struct{ Seq uint64 }

func (probeTick) memberMsg()    {}
func (probeTimeout) memberMsg() {}
func (relayExpire) memberMsg()  {}

// outstanding is the probe the actor waits for an ack of (CLU-220, CLU-240).
type outstanding struct {
	target string
	id     uint32 // the target's ID when it was probed
	seq    uint64
	acked  bool
}

// relay is a ping a helper sent for another member's probe (CLU-230): its own seq, and where the
// target's ack goes on to.
type relay struct {
	seq       uint64 // the helper's seq of its ping
	requester string // the node that sent the pingReq
	reqSeq    uint64 // the requester's seq
	target    string
	targetID  uint32 // the ID the requester probed
}

// prober is the probing state of one actor incarnation. Nothing of it survives a restart.
type prober struct {
	seq    uint64      // the last sequence number used, by probes and relays alike (CLU-220)
	list   []string    // the probe list (CLU-210)
	next   int         // the index in list of the next name to probe
	probe  outstanding // the outstanding probe; seq 0: none
	relays []relay     // the live relays, by seq ascending
}

// nextSeq returns the next sequence number, from 1.
func (p *prober) nextSeq() uint64 {
	p.seq++
	return p.seq
}

// startProbing arms the ProbeInterval timer (CLU-200).
func (a *memberActor) startProbing(ctx *lockstep.Context[memberMsg]) {
	ctx.Every(a.m.cfg.ProbeInterval, probeTick{})
}

// probeTick resolves the outstanding probe (CLU-240), then probes the next name of the probe list
// whose entry is still up, rebuilding the list when it is exhausted (CLU-210).
func (a *memberActor) probeTick(ctx *lockstep.Context[memberMsg]) {
	a.resolveProbe(ctx)
	target, ok := a.nextTarget(ctx)
	if !ok {
		return
	}
	p := &a.probing
	p.probe = outstanding{target: target.Node, id: target.ID, seq: p.nextSeq()}
	a.send(ctx, target.Node, ping{
		From: toUpdate(a.tbl.Self()), Seq: p.probe.seq, Target: target.Node, TargetID: target.ID,
		Updates: a.piggyback(target.Node),
	})
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(recordPrefix+"probe", "target", target.Node, "seq", strconv.FormatUint(p.probe.seq, 10))
	}
	ctx.After(a.m.cfg.ProbeTimeout, probeTimeout{Seq: p.probe.seq})
}

// nextTarget returns the entry of the next name of the probe list that is still up. When the list
// is exhausted it is rebuilt, once per tick: a rebuilt list holds only up names, so its first name
// is the target, and an empty one probes nothing.
func (a *memberActor) nextTarget(ctx *lockstep.Context[memberMsg]) (Member, bool) {
	p := &a.probing
	for rebuilt := false; ; {
		for p.next < len(p.list) {
			name := p.list[p.next]
			p.next++
			if m, ok := a.tbl.Get(name); ok && m.State.Up() {
				return m, true
			}
		}
		if rebuilt {
			return Member{}, false
		}
		p.list, p.next, rebuilt = probeOrder(ctx.Rand(), a.tbl.Members(), a.tbl.self), 0, true
		if len(p.list) == 0 {
			return Member{}, false
		}
	}
}

// probeOrder returns a new probe list: the names of the up members other than self, in name order
// (members is sorted by name), shuffled with r.Shuffle (draw 1 of §6). It draws nothing when there
// is no such member.
func probeOrder(r *rand.Rand, members []Member, self string) []string {
	var names []string
	for _, m := range members {
		if m.State.Up() && m.Node != self {
			names = append(names, m.Node)
		}
	}
	if len(names) > 0 {
		r.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	}
	return names
}

// resolveProbe ends the outstanding probe. One without an ack fails: it is recorded, and its
// target becomes Suspect with its current incarnation if it is still Alive with the probed ID
// (CLU-240).
func (a *memberActor) resolveProbe(ctx *lockstep.Context[memberMsg]) {
	p := &a.probing
	pr := p.probe
	p.probe = outstanding{}
	if pr.seq == 0 || pr.acked {
		return
	}
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(recordPrefix+"probe.fail", "target", pr.target, "seq", strconv.FormatUint(pr.seq, 10))
	}
	if m, ok := a.tbl.Get(pr.target); ok && m.ID == pr.id && m.State == Alive {
		m.State = Suspect
		if c, res := a.tbl.Apply(m, true, sourceProbe, ctx.Now()); res == applyChanged {
			a.applied(ctx, c)
		}
	}
}

// probeTimedOut sends the indirect probes of the probe seq if it is still outstanding without an
// ack: a pingReq to each helper that draw 2 chooses (CLU-230). The probe.indirect record is made
// also when there is no helper. A target that went down or changed its ID since the probe is not
// probed indirectly: its probe will fail without a suspicion.
func (a *memberActor) probeTimedOut(ctx *lockstep.Context[memberMsg], seq uint64) {
	pr := a.probing.probe
	if pr.seq != seq || pr.acked {
		return
	}
	if m, ok := a.tbl.Get(pr.target); !ok || m.ID != pr.id || !m.State.Up() {
		return
	}
	helpers := drawHelpers(ctx.Rand(), a.tbl.Members(), a.tbl.self, pr.target, a.m.cfg.IndirectProbes)
	if ctl := tracer(ctx); ctl != nil {
		ctl.Record(recordPrefix+"probe.indirect", "target", pr.target, "seq", strconv.FormatUint(seq, 10),
			"helpers", strconv.Itoa(len(helpers)))
	}
	for _, h := range helpers {
		a.send(ctx, h, pingReq{
			From: toUpdate(a.tbl.Self()), Seq: seq, Target: pr.target, TargetID: pr.id,
			Updates: a.piggyback(h),
		})
	}
}

// drawHelpers chooses the helpers of an indirect probe of target (draw 2 of §6): from the Alive
// members other than self and target, in name order (members is sorted by name), k = min(n, len)
// by a partial Fisher-Yates shuffle, in drawn order. It draws k values.
func drawHelpers(r *rand.Rand, members []Member, self, target string, n int) []string {
	var cands []string
	for _, m := range members {
		if m.State == Alive && m.Node != self && m.Node != target {
			cands = append(cands, m.Node)
		}
	}
	k := min(n, len(cands))
	for i := range k {
		j := i + r.IntN(len(cands)-i)
		cands[i], cands[j] = cands[j], cands[i]
	}
	return cands[:k]
}

// answerPing answers a merged ping with an ack from self, whatever the pinger's state in the
// table (CLU-250). A ping from another ID of the own node is not answered: the actor sends nothing
// to its own node.
func (a *memberActor) answerPing(ctx *lockstep.Context[memberMsg], msg ping) {
	if msg.From.Node == a.tbl.self {
		return
	}
	a.send(ctx, msg.From.Node, ack{
		From: toUpdate(a.tbl.Self()), Seq: msg.Seq, Target: a.tbl.self, Updates: a.piggyback(msg.From.Node),
	})
}

// relayPing handles a merged pingReq as a helper: it pings the target with its own next seq and
// keeps a relay that expires ProbeTimeout later (CLU-230). A pingReq from or about the own node is
// ignored.
func (a *memberActor) relayPing(ctx *lockstep.Context[memberMsg], msg pingReq) {
	if msg.From.Node == a.tbl.self || msg.Target == a.tbl.self || msg.Target == "" {
		return
	}
	p := &a.probing
	r := relay{seq: p.nextSeq(), requester: msg.From.Node, reqSeq: msg.Seq, target: msg.Target, targetID: msg.TargetID}
	p.relays = append(p.relays, r)
	a.send(ctx, msg.Target, ping{
		From: toUpdate(a.tbl.Self()), Seq: r.seq, Target: msg.Target, TargetID: msg.TargetID,
		Updates: a.piggyback(msg.Target),
	})
	ctx.After(a.m.cfg.ProbeTimeout, relayExpire{Seq: r.seq})
}

// handleAck matches a merged ack (CLU-240, CLU-230): an ack whose Seq and Target are those of the
// outstanding probe acks it, once, if it comes from a helper or from the probed ID of the target;
// an ack from the target's probed ID for a live relay is sent on to the requester with the
// requester's seq, and the relay is dropped. Any other ack is ignored: a ping is delivered by node
// name, so another process of the target may answer it, and that answer says nothing about the
// probed one.
func (a *memberActor) handleAck(ctx *lockstep.Context[memberMsg], msg ack) {
	p := &a.probing
	if pr := &p.probe; pr.seq != 0 && pr.seq == msg.Seq && pr.target == msg.Target {
		if pr.acked || (msg.From.Node == pr.target && msg.From.ID != pr.id) {
			return
		}
		pr.acked = true
		if ctl := tracer(ctx); ctl != nil {
			via := viaIndirect
			if msg.From.Node == pr.target {
				via = viaDirect
			}
			ctl.Record(recordPrefix+"ack", "target", pr.target, "seq", strconv.FormatUint(pr.seq, 10), "via", via)
		}
		return
	}
	i, ok := p.findRelay(msg.Seq)
	if !ok || p.relays[i].target != msg.Target || msg.From.Node != msg.Target || msg.From.ID != p.relays[i].targetID {
		return
	}
	r := p.relays[i]
	p.relays = slices.Delete(p.relays, i, i+1)
	a.send(ctx, r.requester, ack{
		From: toUpdate(a.tbl.Self()), Seq: r.reqSeq, Target: r.target, Updates: a.piggyback(r.requester),
	})
}

// expireRelay drops the relay seq if it is still there (CLU-230).
func (p *prober) expireRelay(seq uint64) {
	if i, ok := p.findRelay(seq); ok {
		p.relays = slices.Delete(p.relays, i, i+1)
	}
}

// findRelay returns the index of the relay seq, and whether there is one.
func (p *prober) findRelay(seq uint64) (int, bool) {
	return slices.BinarySearchFunc(p.relays, seq, func(r relay, s uint64) int { return cmp.Compare(r.seq, s) })
}

// piggyback returns the updates for one outgoing ping, pingReq or ack to node to: the forced
// update about to first, then up to MaxPiggyback queued updates (CLU-400).
func (a *memberActor) piggyback(to string) []update {
	cfg := &a.m.cfg
	return a.queue.take(forcedUpdate(a.tbl, to), cfg.MaxPiggyback, retransmitLimit(cfg.RetransmitMult, a.tbl.Len()))
}
