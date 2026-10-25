// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"math/rand/v2"
	"slices"
	"time"
)

// role is a server's Raft role.
type role uint8

const (
	follower role = iota
	candidate
	leader
)

// String returns "follower", "candidate" or "leader".
func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	}
	return "role(?)"
}

// env is what a core needs from the actor that runs it. The Node implements it with its context's
// timers; tests pass a fake.
type env interface {
	// armElection cancels the running election timer and starts one that delivers
	// electionTimeout{gen} after electionDelay (RFT-150).
	armElection(gen uint64)
	// cancelElection cancels the running election timer, if any.
	cancelElection()
}

// queued is a message in the outbox: msg goes to Peers[peer] once tag persists have completed
// (RFT-090).
type queued struct {
	peer int
	tag  uint64
	msg  Msg
}

// core is one incarnation's Raft state and its transitions, without actors (RFT §11): inputs
// change the state and queue messages in the outbox, and the Node sends what takeSends returns.
// Per-peer state lives in slices indexed by the position in Peers.
//
// core is reached from actor code: it reads no clock, starts no goroutine, ranges over no map and
// draws randomness only through the *rand.Rand it is given.
type core struct {
	cfg  Config
	obs  Observer // never nil (RFT-280)
	env  env
	self int // position of cfg.ID in cfg.Peers
	sm   StateMachine

	term   uint64
	vote   string
	role   role
	leader string // the leader known in term, "" if none
	log    raftLog
	gen    uint64 // generation of the latest election timer (RFT-150)

	// Candidate state, reset on every election: replied[p] is set by any VoteReply of the term
	// from Peers[p], granted[p] by a granting one. The candidate's own entries are set.
	replied []bool
	granted []bool

	// Leader state, set on winning (RFT-180).
	next  []uint64
	match []uint64

	// Persist pipeline (RFT-090).
	dirty     bool   // term, vote or log changed since the last issued persist
	from      uint64 // lowest log index written or truncated since the last issued persist; 0 if none
	issued    uint64 // persists issued in this incarnation
	completed uint64 // persists completed in this incarnation
	inFlight  bool
	// The values the in-flight persist's PersistStarted call reported.
	startedHS    HardState
	startedLast  uint64
	startedChain uint64
	durableLast  uint64 // the last index of the log's durable prefix

	outbox []queued
	sent   []queued // takeSends's buffer, reused across turns
}

// newCore returns the follower state of RFT-080 for durable state d: commit and applied index 0,
// no known leader. It copies d.Log, makes no Observer call and arms no timer; start does both.
func newCore(cfg Config, d Durable, sm StateMachine, e env) *core {
	obs := cfg.Observer
	if obs == nil {
		obs = nopObserver{}
	}
	n := len(cfg.Peers)
	c := &core{
		cfg:     cfg,
		obs:     obs,
		env:     e,
		self:    slices.Index(cfg.Peers, cfg.ID),
		sm:      sm,
		term:    d.HardState.Term,
		vote:    d.HardState.Vote,
		replied: make([]bool, n),
		granted: make([]bool, n),
		next:    make([]uint64, n),
		match:   make([]uint64, n),
	}
	c.log.append(d.Log...)
	c.durableLast = c.log.LastIndex()
	return c
}

// start calls Observer.Recovered and arms the election timer (RFT-080).
func (c *core) start() {
	c.obs.Recovered(c.cfg.ID, c.hardState(), &c.log)
	c.armElection()
}

// electionDelay draws an election timeout in [minD, maxD] (RFT-150).
func electionDelay(r *rand.Rand, minD, maxD time.Duration) time.Duration {
	return minD + time.Duration(r.Int64N(int64(maxD-minD)+1))
}

// step handles a message from a peer or a client. Of AppendEntries and AppendReply it applies the
// term rules of RFT-140 only; replication and Propose are not handled here yet.
func (c *core) step(m Msg) {
	switch m := m.(type) {
	case RequestVote:
		if p, ok := c.observeTerm(m.Candidate, m.Term); ok {
			c.onRequestVote(p, m)
		}
	case VoteReply:
		if p, ok := c.observeTerm(m.From, m.Term); ok && m.Term == c.term {
			c.onVoteReply(p, m)
		}
	case AppendEntries:
		if p, ok := c.observeTerm(m.Leader, m.Term); ok && m.Term < c.term {
			c.queue(p, AppendReply{Term: c.term, From: c.cfg.ID, LastIndex: c.log.LastIndex()})
		}
	case AppendReply:
		c.observeTerm(m.From, m.Term)
	}
}

// observeTerm applies RFT-140 to a peer message from sender with term. It returns the sender's
// position in Peers, and false if the sender is not in Peers, in which case nothing changed.
func (c *core) observeTerm(sender string, term uint64) (int, bool) {
	p := slices.Index(c.cfg.Peers, sender)
	if p < 0 {
		return 0, false
	}
	if term > c.term {
		c.stepDown()
		c.setTerm(term)
		c.role = follower
		c.leader = ""
	}
	return p, true
}

// onRequestVote answers a RequestVote from Peers[p] whose term is not above the current one
// (RFT-140, RFT-170).
func (c *core) onRequestVote(p int, m RequestVote) {
	granted := false
	if m.Term == c.term && (c.vote == "" || c.vote == m.Candidate) &&
		(c.cfg.Bugs&BugVoteIgnoresLog != 0 || c.logUpToDate(m.LastIndex, m.LastTerm)) {
		granted = true
		c.setVote(m.Candidate)
		c.armElection()
	}
	c.queue(p, VoteReply{Term: c.term, From: c.cfg.ID, Granted: granted})
}

// logUpToDate reports whether a log ending at lastIndex with lastTerm is at least as up to date as
// this server's (RFT-170).
func (c *core) logUpToDate(lastIndex, lastTerm uint64) bool {
	own := c.log.Term(c.log.LastIndex())
	return lastTerm > own || lastTerm == own && lastIndex >= c.log.LastIndex()
}

// onVoteReply counts a VoteReply of the current term from Peers[p] (RFT-180).
func (c *core) onVoteReply(p int, m VoteReply) {
	if c.role != candidate {
		return
	}
	c.replied[p] = true
	if m.Granted {
		c.granted[p] = true
	}
	c.winIfMajority()
}

// onElectionTimeout handles electionTimeout{gen}. Only the latest timer of a follower or candidate
// starts an election (RFT-150, RFT-160).
func (c *core) onElectionTimeout(gen uint64) {
	if gen != c.gen || c.role == leader {
		return
	}
	c.setTerm(c.term + 1)
	c.setVote(c.cfg.ID)
	c.role = candidate
	c.leader = ""
	clear(c.replied)
	clear(c.granted)
	c.replied[c.self] = true
	c.granted[c.self] = true
	c.armElection()
	c.requestVotes()
	c.winIfMajority()
}

// onTick handles the heartbeat tick: a candidate asks again every peer that has not replied in its
// term (RFT-160); a leader sends AppendEntries (RFT-190).
func (c *core) onTick() {
	switch c.role {
	case candidate:
		c.requestVotes()
	case leader:
		c.broadcastAppend()
	}
}

// requestVotes queues a RequestVote to every peer that has not replied in this term, in Peers
// order.
func (c *core) requestVotes() {
	last := c.log.LastIndex()
	rv := RequestVote{Term: c.term, Candidate: c.cfg.ID, LastIndex: last, LastTerm: c.log.Term(last)}
	for p := range c.cfg.Peers {
		if !c.replied[p] {
			c.queue(p, rv)
		}
	}
}

// winIfMajority makes a candidate with a majority of granted votes the leader (RFT-180).
func (c *core) winIfMajority() {
	votes := 0
	for _, g := range c.granted {
		if g {
			votes++
		}
	}
	if votes <= len(c.cfg.Peers)/2 {
		return
	}
	c.role = leader
	c.leader = c.cfg.ID
	c.cancelElection()
	last := c.log.LastIndex()
	for p := range c.cfg.Peers {
		c.next[p] = last + 1
		c.match[p] = 0
	}
	c.obs.BecameLeader(c.cfg.ID, c.term, &c.log)
	c.appendLog(Entry{Term: c.term})
	c.broadcastAppend()
}

// broadcastAppend queues an AppendEntries to every peer (RFT-190). It is where a new leader and
// every tick of a leader start replication; this version replicates nothing yet.
func (c *core) broadcastAppend() {}

// stepDown makes a leader a follower that knows no leader and drops its leader state (RFT-140,
// RFT-260). It does nothing on a follower or candidate.
func (c *core) stepDown() {
	if c.role != leader {
		return
	}
	c.role = follower
	c.leader = ""
}

// setTerm sets the current term and clears the vote (RFT-140).
func (c *core) setTerm(term uint64) {
	c.term = term
	c.vote = ""
	c.dirty = true
}

// setVote records the vote of the current term.
func (c *core) setVote(v string) {
	if c.vote != v {
		c.vote = v
		c.dirty = true
	}
}

// appendLog appends entries to the log, marks it for the next persist and calls
// Observer.LogWritten with the first appended index (RFT-090, RFT-200).
func (c *core) appendLog(es ...Entry) {
	from := c.log.LastIndex() + 1
	c.log.append(es...)
	c.wrote(from)
}

// wrote records a log write or truncation from index from on for the next persist and calls
// Observer.LogWritten.
func (c *core) wrote(from uint64) {
	if c.from == 0 || from < c.from {
		c.from = from
	}
	c.dirty = true
	c.obs.LogWritten(c.cfg.ID, from, &c.log)
}

// armElection starts a new election timer generation (RFT-150).
func (c *core) armElection() {
	c.gen++
	c.env.armElection(c.gen)
}

// cancelElection cancels the election timer. It also starts a new generation, so a timeout that
// fired before the cancel is ignored.
func (c *core) cancelElection() {
	c.gen++
	c.env.cancelElection()
}

// queue puts msg for Peers[p] in the outbox, tagged with the persist that must complete before it
// leaves (RFT-090).
func (c *core) queue(p int, msg Msg) {
	tag := c.issued
	if c.dirty {
		tag++
	}
	c.outbox = append(c.outbox, queued{peer: p, tag: tag, msg: msg})
}

// hardState returns the current term and vote.
func (c *core) hardState() HardState { return HardState{Term: c.term, Vote: c.vote} }

// endTurn returns the persist to issue at the end of a turn, if the state is dirty and no persist
// is in flight (RFT-090). It calls Observer.PersistStarted, and the state is clean afterwards.
func (c *core) endTurn() (Persist, bool) {
	if !c.dirty || c.inFlight {
		return Persist{}, false
	}
	hs := c.hardState()
	last := c.log.LastIndex()
	c.startedHS, c.startedLast, c.startedChain = hs, last, c.log.Chain(last)
	c.obs.PersistStarted(c.cfg.ID, hs, last, c.startedChain)
	p := Persist{Store: c.cfg.Store, Node: c.cfg.ID, HardState: hs, From: c.from}
	if c.cfg.Bugs&BugVoteNotDurable != 0 {
		p.HardState.Vote = ""
	}
	if c.from > 0 {
		p.Entries = c.log.slice(c.from, last)
	}
	c.dirty = false
	c.from = 0
	c.issued++
	c.inFlight = true
	return p, true
}

// persisted handles the successful completion of the in-flight persist: it records the durable
// last index, calls Observer.Durable with the values PersistStarted reported, and lets the
// messages waiting for this persist leave (RFT-090).
func (c *core) persisted() {
	c.inFlight = false
	c.completed++
	c.durableLast = c.startedLast
	if c.from > 0 { // the log was rewritten from c.from on after the persist was issued
		c.durableLast = min(c.durableLast, c.from-1)
	}
	c.obs.Durable(c.cfg.ID, c.startedHS, c.startedLast, c.startedChain)
}

// takeSends removes and returns the outbox messages whose persist has completed, in queue order.
// Tags never decrease along the outbox, so they are a prefix of it. The result is valid until the
// next call.
func (c *core) takeSends() []queued {
	k := 0
	for k < len(c.outbox) && c.outbox[k].tag <= c.completed {
		k++
	}
	clear(c.sent) // release the previous turn's messages
	c.sent = append(c.sent[:0], c.outbox[:k]...)
	n := copy(c.outbox, c.outbox[k:])
	clear(c.outbox[n:]) // release the moved messages
	c.outbox = c.outbox[:n]
	return c.sent
}

// nopObserver is the Observer a core uses when Config.Observer is nil (RFT-280).
type nopObserver struct{}

func (nopObserver) Recovered(string, HardState, LogView)              {}
func (nopObserver) PersistStarted(string, HardState, uint64, uint64)  {}
func (nopObserver) Durable(string, HardState, uint64, uint64)         {}
func (nopObserver) LogWritten(string, uint64, LogView)                {}
func (nopObserver) BecameLeader(string, uint64, LogView)              {}
func (nopObserver) Committed(string, uint64, uint64, uint64, LogView) {}
func (nopObserver) Applied(string, uint64, Entry, uint64)             {}
func (nopObserver) Replied(string, uint32, uint64, Result)            {}
