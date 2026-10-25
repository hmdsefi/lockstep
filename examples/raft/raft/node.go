// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import "github.com/hmdsefi/lockstep"

// The Node's messages to itself. Only the Node sends them.
type (
	// electionTimeout is the election timer of generation gen firing (RFT-150).
	electionTimeout struct{ gen uint64 }
	// tick is the heartbeat timer firing (RFT-080).
	tick struct{}
	// persisted is the result of the in-flight Persist (RFT-090, RFT-110).
	persisted struct{ err error }
	// loaded is the result of the Load issued at start (RFT-080, RFT-110).
	loaded struct {
		d   Durable
		err error
	}
)

func (electionTimeout) raftMsg() {}
func (tick) raftMsg()            {}
func (persisted) raftMsg()       {}
func (loaded) raftMsg()          {}

// Node is the Raft actor. Create it with New.
type Node struct {
	cfg  Config
	send func(lockstep.Sender, lockstep.PID[Msg], Msg) error // never nil

	ctx      *lockstep.Context[Msg] // this incarnation's context, set by Started
	peers    []lockstep.PID[Msg]    // peers[p] addresses the Node of Peers[p] (RFT-050)
	core     *core                  // nil until the Load result arrives
	election lockstep.TimerID       // the running election timer, 0 if none
	dropped  uint64                 // peer messages whose send failed (RFT-060)
}

// Started loads the durable state with the raft.load effect and starts the heartbeat timer
// (RFT-080).
func (n *Node) Started(ctx *lockstep.Context[Msg]) {
	n.ctx = ctx
	n.peers = make([]lockstep.PID[Msg], len(n.cfg.Peers))
	for p, peer := range n.cfg.Peers {
		n.peers[p] = lockstep.PIDAt[Msg](peer, DefaultPath)
	}
	lockstep.Do(ctx, Load{Store: n.cfg.Store, Node: n.cfg.ID}, loadDone)
	ctx.Every(n.cfg.Heartbeat, tick{})
}

// Receive handles msg, then ends the turn: it issues the next persist, sends the outbox messages
// that may leave and answers clients, in that order (RFT-090, RFT-280). Until the Load result
// arrives it drops peer messages and timers and answers Propose with NotLeader (RFT-080). A Load
// or Persist error fails the Node, which restarts it (RFT-110); in production that includes an
// effect that runs past lockstep.DefaultEffectTimeout.
func (n *Node) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	switch m := msg.(type) {
	case loaded:
		if m.err != nil {
			ctx.Fail(m.err)
			return
		}
		n.core = newCore(n.cfg, m.d, n.cfg.NewStateMachine(), n)
		n.core.start()
	case persisted:
		if n.core == nil {
			return
		}
		if m.err != nil {
			ctx.Fail(m.err)
			return
		}
		n.core.persisted()
	case electionTimeout:
		if n.core == nil {
			return
		}
		n.core.onElectionTimeout(m.gen)
	case tick:
		if n.core == nil {
			return
		}
		n.core.onTick()
	case RequestVote, VoteReply, AppendEntries, AppendReply:
		if n.core == nil {
			return
		}
		n.core.step(m)
	case Propose:
		if n.core == nil {
			m.ReplyTo.Send(ctx, Result{Status: NotLeader})
			return
		}
		n.core.step(m)
	}
	n.endTurn(ctx)
}

// Stopped answers Unknown to a leader's pending proposals (RFT-260). It runs before every restart
// too; it arms no timer and draws no randomness.
func (n *Node) Stopped(ctx *lockstep.Context[Msg], _ lockstep.StopReason) {
	if n.core == nil {
		return
	}
	n.core.stepDown()
	n.answer(ctx)
}

// endTurn issues the persist of a dirty state, sends the outbox messages whose persist has
// completed and answers clients. Every Observer call of the turn happens before these sends
// (RFT-280).
func (n *Node) endTurn(ctx *lockstep.Context[Msg]) {
	if p, ok := n.core.endTurn(); ok {
		lockstep.Do(ctx, p, persistDone)
	}
	for _, q := range n.core.takeSends() {
		if err := n.send(ctx, n.peers[q.peer], q.msg); err != nil {
			n.dropped++ // message loss (RFT-060)
		}
	}
	n.answer(ctx)
}

// answer sends the queued client replies (RFT-100).
func (n *Node) answer(ctx *lockstep.Context[Msg]) {
	for _, r := range n.core.takeReplies() {
		r.to.Send(ctx, r.r)
	}
}

// armElection implements env: it cancels the running election timer and starts one that delivers
// electionTimeout{gen} after a delay drawn from ctx.Rand() (RFT-150).
func (n *Node) armElection(gen uint64) {
	n.cancelElection()
	d := electionDelay(n.ctx.Rand(), n.cfg.ElectionMin, n.cfg.ElectionMax)
	n.election = n.ctx.After(d, electionTimeout{gen: gen})
}

// cancelElection implements env: it cancels the running election timer, if any.
func (n *Node) cancelElection() {
	if n.election != 0 {
		n.ctx.CancelTimer(n.election)
		n.election = 0
	}
}

// loadDone turns the Load result into the loaded message.
func loadDone(d Durable, err error) Msg { return loaded{d: d, err: err} }

// persistDone turns a Persist result into the persisted message.
func persistDone(_ struct{}, err error) Msg { return persisted{err: err} }
