// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// outboxEvent is one send of a VoteReply or AppendReply, or one PersistStarted or Durable call,
// of a server. The events are kept in the order they happened.
type outboxEvent struct {
	node string // the server that sent or called
	at   kernel.Time

	// A send: the message and its destination server.
	msg Msg
	to  string

	// A PersistStarted (started) or Durable (durable) call: the state it covers.
	started, durable bool
	hs               HardState
	last             uint64
}

// outboxRecorder is the Observer and the send hook of one simulated cluster. It records the
// events of every server in one list, so their relative order is the order the simulation ran.
type outboxRecorder struct {
	nopObserver
	s      *sim.Sim
	events []outboxEvent
}

func (r *outboxRecorder) now() kernel.Time { return r.s.World().Sim.Now() }

func (r *outboxRecorder) PersistStarted(node string, hs HardState, last, _ uint64) {
	r.events = append(r.events, outboxEvent{node: node, at: r.now(), started: true, hs: hs, last: last})
}

func (r *outboxRecorder) Durable(node string, hs HardState, last, _ uint64) {
	r.events = append(r.events, outboxEvent{node: node, at: r.now(), durable: true, hs: hs, last: last})
}

// sendHook returns the send hook of server id: it records the replies and sends as the Node does.
func (r *outboxRecorder) sendHook(id string) func(lockstep.Sender, lockstep.PID[Msg], Msg) error {
	return func(from lockstep.Sender, to lockstep.PID[Msg], msg Msg) error {
		switch msg.(type) {
		case VoteReply, AppendReply:
			r.events = append(r.events, outboxEvent{node: id, at: r.now(), msg: msg, to: to.Address().Node()})
		}
		return lockstep.Send(from, to, msg)
	}
}

// TestATRFT09OutboxAfterDurable checks RFT-090 and RFT-100: with every persist taking 20 ms in a
// 3-server simulation, each VoteReply and AppendReply of a server leaves after a Durable call
// that covers the vote or the entries it acknowledges, and no server ever has two persists in
// flight. The test sits in package raft because a lockstep.send record names only the message
// type, not the vote or the entries behind it.
func TestATRFT09OutboxAfterDurable(t *testing.T) {
	const persistLatency = 20 * time.Millisecond
	opts := sim.Options{Options: faultline.Options{Seeds: 1, Duration: 2 * time.Second, BaseSeed: 1}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		rec := &outboxRecorder{s: s}
		newSimCluster(t, s, 3, func(cfg *Config) {
			cfg.Observer = rec
			cfg.send = rec.sendHook(cfg.ID)
		})
		handleSimEffects(s, NewMemStore(), persistLatency, time.Millisecond)
		s.World().RunFor(2 * time.Second)

		inFlight := map[string]kernel.Time{} // PersistStarted time per server with a persist in flight
		var durable []outboxEvent            // Durable calls so far
		votes, appends := 0, 0
		for _, e := range rec.events {
			switch {
			case e.started:
				if at, busy := inFlight[e.node]; busy {
					t.Errorf("%s: persist started at %s while the one started at %s is in flight", e.node, e.at, at)
				}
				inFlight[e.node] = e.at
			case e.durable:
				at, busy := inFlight[e.node]
				if !busy {
					t.Errorf("%s: Durable at %s without a persist in flight", e.node, e.at)
				} else if e.at-at != kernel.Time(persistLatency) {
					t.Errorf("%s: persist started at %s was durable at %s, want %v later", e.node, at, e.at, persistLatency)
				}
				delete(inFlight, e.node)
				durable = append(durable, e)
			default:
				if !covered(durable, e) {
					t.Errorf("%s sent %+v to %s at %s before a Durable call covering it", e.node, e.msg, e.to, e.at)
				}
				if _, ok := e.msg.(VoteReply); ok {
					votes++
				} else {
					appends++
				}
			}
		}
		if votes == 0 || appends == 0 {
			t.Errorf("checked %d VoteReply and %d AppendReply sends, want both above 0", votes, appends)
		}
	})
}

// covered reports whether a Durable call of e's server in durable covers what the reply e
// acknowledges: its term, for a granted vote also the vote for the receiver, and for a successful
// AppendReply the entries up to Match; for a failed one the log up to LastIndex.
func covered(durable []outboxEvent, e outboxEvent) bool {
	for _, d := range durable {
		if d.node != e.node {
			continue
		}
		switch m := e.msg.(type) {
		case VoteReply:
			if d.hs.Term >= m.Term && (!m.Granted || d.hs.Term == m.Term && d.hs.Vote == e.to) {
				return true
			}
		case AppendReply:
			need := m.LastIndex
			if m.Success {
				need = m.Match
			}
			if d.hs.Term >= m.Term && d.last >= need {
				return true
			}
		}
	}
	return false
}
