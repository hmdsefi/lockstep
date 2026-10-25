// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/sim"
)

// simCluster is a simulated cluster of servers n1…nN, each running a Node at DefaultPath.
type simCluster struct {
	peers []string
	// nodes holds every Node the servers' factories created, in creation order. A restart adds a
	// new Node for the same server.
	nodes []*Node
}

// newSimCluster adds servers n1…nN to s. At every boot, server n<i> spawns at DefaultPath the Node
// that New builds from a Config with ID n<i>, Peers n1…nN and a kv state machine, after edit
// changed it (edit may be nil; it may set the send hook). A New or Spawn error fails the test.
// The cluster needs effect handlers, for example from handleSimEffects.
func newSimCluster(t *testing.T, s *sim.Sim, n int, edit func(cfg *Config)) *simCluster {
	t.Helper()
	cl := &simCluster{}
	for i := 1; i <= n; i++ {
		cl.peers = append(cl.peers, "n"+strconv.Itoa(i))
	}
	for _, id := range cl.peers {
		s.Node(id, func(sys *lockstep.System) {
			cfg := Config{
				ID:              id,
				Peers:           cl.peers,
				NewStateMachine: func() StateMachine { return kv.New() },
			}
			if edit != nil {
				edit(&cfg)
			}
			factory, err := New(cfg)
			if err != nil {
				t.Errorf("%s: %v", id, err)
				return
			}
			wrapped := func() lockstep.Actor[Msg] {
				a := factory()
				if n, ok := a.(*Node); ok {
					cl.nodes = append(cl.nodes, n)
				} else {
					t.Errorf("%s: the factory returned %T", id, a)
				}
				return a
			}
			if _, err := lockstep.Spawn(sys, "raft", wrapped); err != nil {
				t.Errorf("%s: %v", id, err)
			}
		})
	}
	return cl
}

// byID returns the Nodes of server id, oldest first.
func (cl *simCluster) byID(id string) []*Node {
	var out []*Node
	for _, n := range cl.nodes {
		if n.cfg.ID == id {
			out = append(out, n)
		}
	}
	return out
}

// handleSimEffects installs handlers for the raft.persist and raft.load effects on s. They keep
// every server's durable state in store, keyed by Persist.Node and Load.Node, so a server's state
// survives its restarts as with a production Store. A persist result arrives persistLatency after
// the Do call, a load result loadLatency after it. A Save error fails the persist.
func handleSimEffects(s *sim.Sim, store *MemStore, persistLatency, loadLatency time.Duration) {
	s.HandleEffect(Persist{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		p, ok := c.Effect().(Persist)
		if !ok {
			return sim.EffectResult{Err: fmt.Errorf("raft.persist handler got %T", c.Effect())}
		}
		err := store.Save(p.Node, p.HardState, p.From, p.Entries)
		return sim.EffectResult{Value: struct{}{}, Err: err, Latency: persistLatency}
	})
	s.HandleEffect(Load{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		l, ok := c.Effect().(Load)
		if !ok {
			return sim.EffectResult{Err: fmt.Errorf("raft.load handler got %T", c.Effect())}
		}
		d, err := store.Load(l.Node)
		return sim.EffectResult{Value: d, Err: err, Latency: loadLatency}
	})
}
