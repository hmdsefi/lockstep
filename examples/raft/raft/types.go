// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"strconv"
	"strings"

	"github.com/hmdsefi/lockstep"
)

// Entry is one log entry. It is a value type, so it is a safe message part.
type Entry struct {
	Term   uint64
	Client uint32 // 0 for the leader's no-op
	Seq    uint64 // 0 for the leader's no-op
	Cmd    string // opaque to Raft; "" for the no-op
}

// HardState is the part of Raft's state that is persisted besides the log.
type HardState struct {
	Term uint64
	Vote string // server voted for in Term; "" for none
}

// Durable is the persisted state of one server.
type Durable struct {
	HardState HardState
	Log       []Entry // index 1 is Log[0]
}

// StateMachine is the replicated state machine. Raft calls it only from the actor's turns.
type StateMachine interface {
	// Apply applies the committed command cmd at index and returns its output.
	Apply(index uint64, cmd string) string
	// Query evaluates a read-only cmd against the current state. Only BugStaleRead calls it.
	Query(cmd string) string
	// Hash returns a hash of the whole state.
	Hash() uint64
}

// Status is the outcome of a Propose.
type Status uint8

const (
	OK        Status = iota + 1 // applied; Output and Index are set
	NotLeader                   // not appended; Leader is a hint ("" if unknown)
	Busy                        // not appended: MaxPending proposals are waiting
	Unknown                     // appended, but this server cannot tell whether it will apply
)

// String returns "ok", "not-leader", "busy", "unknown", or "status(<n>)".
func (s Status) String() string {
	switch s {
	case OK:
		return "ok"
	case NotLeader:
		return "not-leader"
	case Busy:
		return "busy"
	case Unknown:
		return "unknown"
	}
	return "status(" + strconv.Itoa(int(s)) + ")"
}

// Result answers a Propose.
type Result struct {
	Status Status
	Output string // StateMachine output when OK
	Index  uint64 // log index of the command when OK (BugStaleRead: the applied index)
	Leader string // NotLeader: the leader this server knows of, or ""
}

// Msg is the sealed message type of a Node.
type Msg interface{ raftMsg() }

// RequestVote asks for a vote in Term.
type RequestVote struct {
	Term      uint64
	Candidate string
	LastIndex uint64
	LastTerm  uint64
}

// VoteReply answers a RequestVote.
type VoteReply struct {
	Term    uint64
	From    string
	Granted bool
}

// AppendEntries replicates entries and carries the leader's commit index; empty Entries is a
// heartbeat.
type AppendEntries struct {
	Term      uint64
	Leader    string
	PrevIndex uint64
	PrevTerm  uint64
	Entries   []Entry // a fresh slice per message, never shared
	Commit    uint64
}

// AppendReply answers an AppendEntries.
type AppendReply struct {
	Term      uint64
	From      string
	Success   bool
	Match     uint64 // Success: PrevIndex + len(Entries)
	LastIndex uint64 // !Success: the follower's last index
}

// Propose asks a server to replicate a command and reply when it is applied.
type Propose struct {
	Client  uint32
	Seq     uint64
	Cmd     string
	Read    bool // the command does not change state (only BugStaleRead uses this)
	ReplyTo lockstep.ReplyTo[Result]
}

func (RequestVote) raftMsg()   {}
func (VoteReply) raftMsg()     {}
func (AppendEntries) raftMsg() {}
func (AppendReply) raftMsg()   {}
func (Propose) raftMsg()       {}

// LogView reads a server's log. It is valid only during the Observer call it is passed to.
type LogView interface {
	LastIndex() uint64
	Term(i uint64) uint64  // 0 for i == 0 or i > LastIndex()
	Entry(i uint64) Entry  // panics unless 1 <= i <= LastIndex()
	Chain(i uint64) uint64 // chain hash of [1, i]; Chain(0) == ChainSeed; panics if i > LastIndex()
}

// ChainSeed is Chain(0): the FNV-1a 64 offset basis.
const ChainSeed uint64 = 14695981039346656037

// Observer receives a Node's state changes. It is a test hook: production leaves it nil. Every
// method is called synchronously from the Node's turn and must not keep log.
type Observer interface {
	Recovered(node string, hs HardState, log LogView)
	// PersistStarted and Durable report the state a persist covers: hs, the last log index and
	// Chain(last) at the moment the persist was issued.
	PersistStarted(node string, hs HardState, last, chain uint64)
	Durable(node string, hs HardState, last, chain uint64)
	LogWritten(node string, from uint64, log LogView)
	BecameLeader(node string, term uint64, log LogView)
	Committed(node string, term, from, to uint64, log LogView)
	Applied(node string, index uint64, e Entry, stateHash uint64)
	Replied(node string, client uint32, seq uint64, r Result)
}

// Bug is a bitmask of planted bugs (RFT-540). The zero value plants none.
type Bug uint32

const (
	BugVoteIgnoresLog Bug = 1 << iota // grant votes without the log up-to-date check
	BugStaleRead                      // a leader answers reads from local state, not the log
	BugVoteNotDurable                 // Persist writes Vote as ""
)

// bugNames holds the name of each defined bit, in declaration order.
var bugNames = [...]string{"vote-ignores-log", "stale-read", "vote-not-durable"}

// allBugs is the union of the defined bits.
const allBugs = BugVoteIgnoresLog | BugStaleRead | BugVoteNotDurable

// String returns the set bits' names without the "Bug" prefix as lowercase words joined by
// "-", in declaration order, joined with ",", for example "vote-ignores-log,stale-read"; "" for 0;
// undefined bits as "0x<hex>".
func (b Bug) String() string {
	var parts []string
	for i, name := range bugNames {
		if b&(1<<i) != 0 {
			parts = append(parts, name)
		}
	}
	if undefined := b &^ allBugs; undefined != 0 {
		parts = append(parts, "0x"+strconv.FormatUint(uint64(undefined), 16))
	}
	return strings.Join(parts, ",")
}
