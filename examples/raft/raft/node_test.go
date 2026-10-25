// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/sim"
)

// TestATRFT06SendErrorsAreLoss checks RFT-060: in a 3-server simulation without faults where every
// send to a peer fails with lockstep.ErrMailboxFull, no Node fails, every Node keeps ticking, and
// each Node counts every failed send as a dropped message.
func TestATRFT06SendErrorsAreLoss(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 1, Duration: time.Second, BaseSeed: 1}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		calls := map[string]uint64{} // send hook calls per server
		cl := newSimCluster(t, s, 3, func(cfg *Config) {
			id := cfg.ID
			cfg.send = func(lockstep.Sender, lockstep.PID[Msg], Msg) error {
				calls[id]++
				return lockstep.ErrMailboxFull
			}
		})
		handleSimEffects(s, NewMemStore(), time.Millisecond, time.Millisecond)

		s.World().RunFor(500 * time.Millisecond)
		half := map[string]uint64{}
		for _, id := range cl.peers {
			half[id] = calls[id]
		}
		s.World().RunFor(500 * time.Millisecond)

		if c := s.Counts(); c.Failures != 0 || c.Restarts != 0 {
			t.Errorf("Counts: %d failures, %d restarts; want none", c.Failures, c.Restarts)
		}
		for _, id := range cl.peers {
			nodes := cl.byID(id)
			if len(nodes) != 1 {
				t.Errorf("%s: %d Nodes created; want 1", id, len(nodes))
				continue
			}
			if got := nodes[0].dropped; got != calls[id] || got == 0 {
				t.Errorf("%s: dropped %d messages, send hook called %d times; want equal and above 0",
					id, got, calls[id])
			}
			// A candidate asks both peers again on every 50 ms tick, so the second half second
			// makes about 20 calls; election timeouts alone (at least 150 ms apart) make at most 8.
			if grew := calls[id] - half[id]; grew < 16 {
				t.Errorf("%s: %d send hook calls in the second half second; want at least 16 (ticks stopped?)",
					id, grew)
			}
		}
	})
}

// TestSimClusterCommits checks the Node shell in a 3-server simulation without faults: the servers
// elect one leader through PIDAt sends (RFT-050), and the leader commits, applies and answers a
// proposal.
func TestSimClusterCommits(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 1, Duration: 2 * time.Second, BaseSeed: 1}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		cl := newSimCluster(t, s, 3, nil)
		handleSimEffects(s, NewMemStore(), time.Millisecond, time.Millisecond)
		s.World().RunFor(time.Second)

		var leaders []string
		for _, n := range cl.nodes {
			if n.core != nil && n.core.role == leader {
				leaders = append(leaders, n.cfg.ID)
			}
		}
		if len(leaders) != 1 {
			t.Fatalf("leaders after 1 s: %v; want one", leaders)
		}
		r, err := sim.Ask(s, leaders[0], lockstep.PIDAt[Msg](leaders[0], DefaultPath),
			func(rt lockstep.ReplyTo[Result]) Msg {
				return Propose{Client: 1, Seq: 1, Cmd: kv.EncodePut("k", "v"), ReplyTo: rt}
			})
		if err != nil || r.Status != OK || r.Index == 0 {
			t.Errorf("Propose to leader %s: %+v, %v; want OK at an index above 0", leaders[0], r, err)
		}
		if c := s.Counts(); c.Failures != 0 || c.Restarts != 0 {
			t.Errorf("Counts: %d failures, %d restarts; want none", c.Failures, c.Restarts)
		}
	})
}
