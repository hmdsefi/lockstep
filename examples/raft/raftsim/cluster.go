// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hmdsefi/faultline"

	"github.com/hmdsefi/lockstep/examples/raft/client"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
	"github.com/hmdsefi/lockstep/sim"
)

// Faults selects a fault preset (RFT-420).
type Faults uint8

const (
	FaultsNone Faults = iota
	FaultsPartition
	FaultsCrash
	FaultsDefault
)

// String returns "none", "partition", "crash", "default", or "faults(<n>)".
func (f Faults) String() string {
	switch f {
	case FaultsNone:
		return "none"
	case FaultsPartition:
		return "partition"
	case FaultsCrash:
		return "crash"
	case FaultsDefault:
		return "default"
	}
	return "faults(" + strconv.Itoa(int(f)) + ")"
}

// Cluster configures one simulated cluster.
type Cluster struct {
	Servers        int // 3 or 5
	Clients        int // 1..8
	Keys           int // 1..16; keys "k0".."k<Keys-1>"
	Faults         Faults
	Bugs           raft.Bug
	PersistMin     time.Duration // raft.persist latency bounds
	PersistMax     time.Duration
	ProgressWithin time.Duration // RFT-520
	Raft           raft.Config   // timing fields only; ID, Peers, Store, Observer, Bugs, NewStateMachine are set by Setup
	Client         client.Config // workload fields only; Name, ID, Servers, Keys, Probe, StartAfter, Recorder are set by Setup
}

// DefaultCluster returns 3 servers, 3 clients, 4 keys, FaultsDefault, no bugs, persist latency
// 1ms..5ms, ProgressWithin 3s, and zero Raft and Client (package defaults).
func DefaultCluster() Cluster {
	return Cluster{
		Servers:        3,
		Clients:        3,
		Keys:           4,
		Faults:         FaultsDefault,
		PersistMin:     time.Millisecond,
		PersistMax:     5 * time.Millisecond,
		ProgressWithin: 3 * time.Second,
	}
}

// Options returns sim options for n seeds: 20 s virtual per seed, BaseSeed 1.
func Options(seeds int) sim.Options {
	return sim.Options{Options: faultline.Options{Seeds: seeds, Duration: 20 * time.Second, BaseSeed: 1}}
}

// allBugs is the union of raft's defined bug bits.
const allBugs = raft.BugVoteIgnoresLog | raft.BugStaleRead | raft.BugVoteNotDurable

// validate returns the first rule of §7 that c breaks, or nil. It checks the cluster's own fields
// first, then builds the server and client configs Setup would build and returns the first
// raft.New or client.New error.
func (c Cluster) validate() error {
	switch {
	case c.Servers != 3 && c.Servers != 5:
		return fmt.Errorf("raftsim: Servers (%d) must be 3 or 5", c.Servers)
	case c.Clients < 1 || c.Clients > 8:
		return fmt.Errorf("raftsim: Clients (%d) must be in [1, 8]", c.Clients)
	case c.Keys < 1 || c.Keys > 16:
		return fmt.Errorf("raftsim: Keys (%d) must be in [1, 16]", c.Keys)
	case c.PersistMin < 0 || c.PersistMin > c.PersistMax:
		return fmt.Errorf("raftsim: PersistMin (%v) must be in [0, PersistMax (%v)]", c.PersistMin, c.PersistMax)
	case c.ProgressWithin <= 0:
		return fmt.Errorf("raftsim: ProgressWithin (%v) must be positive", c.ProgressWithin)
	case c.Faults > FaultsDefault:
		return fmt.Errorf("raftsim: unknown Faults %d", uint8(c.Faults))
	case c.Bugs&^allBugs != 0:
		return fmt.Errorf("raftsim: Bugs has undefined bits 0x%x", uint32(c.Bugs&^allBugs))
	}
	servers := c.serverNames()
	for _, name := range servers {
		if _, err := raft.New(c.raftConfig(name, servers, nil)); err != nil {
			return fmt.Errorf("raftsim: %w", err)
		}
	}
	for k := 1; k <= c.Clients+1; k++ {
		if _, err := client.New(c.clientConfig(k, servers, historyRecorder{}, 0)); err != nil {
			return fmt.Errorf("raftsim: %w", err)
		}
	}
	return nil
}

// serverNames returns the server node names n1..n<Servers>.
func (c Cluster) serverNames() []string {
	names := make([]string, c.Servers)
	for i := range names {
		names[i] = "n" + strconv.Itoa(i+1)
	}
	return names
}

// keyNames returns the keys k0..k<Keys-1>.
func (c Cluster) keyNames() []string {
	keys := make([]string, c.Keys)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	return keys
}

// clientName returns the node and process name of client k: c<k> for a workload client, probe
// for client Clients+1.
func (c Cluster) clientName(k int) string {
	if k == c.Clients+1 {
		return probeProcess
	}
	return "c" + strconv.Itoa(k)
}

// raftConfig returns the config of server id: c.Raft's timing fields with the fields Setup owns
// set (RFT-390 step 2).
func (c Cluster) raftConfig(id string, peers []string, obs raft.Observer) raft.Config {
	cfg := c.Raft
	cfg.ID = id
	cfg.Peers = peers
	cfg.Store = nil
	cfg.Observer = obs
	cfg.Bugs = c.Bugs
	cfg.NewStateMachine = newStateMachine
	return cfg
}

// clientConfig returns the config of client k (1..Clients, then the probe): c.Client's workload
// fields with the fields Setup owns set (RFT-390 step 5).
func (c Cluster) clientConfig(k int, servers []string, rec client.Recorder, startAfter time.Duration) client.Config {
	cfg := c.Client
	cfg.Name = c.clientName(k)
	cfg.ID = uint32(k) //nolint:gosec // G115: k is at most 9
	cfg.Servers = servers
	cfg.Keys = c.keyNames()
	cfg.Probe = k == c.Clients+1
	cfg.StartAfter = 0
	if cfg.Probe {
		cfg.StartAfter = startAfter
	}
	cfg.Recorder = rec
	return cfg
}
