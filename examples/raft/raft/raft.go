// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package raft implements Raft leader election, log replication and commit as one lockstep actor
// per server. Durable state goes through effects, so the actor never blocks.
package raft

import (
	"fmt"
	"slices"
	"time"

	"github.com/hmdsefi/lockstep"
)

// DefaultPath is the path of the Raft actor on every server.
const DefaultPath = "/user/raft"

// Defaults applied by New to zero Config fields.
const (
	DefaultElectionMin = 150 * time.Millisecond
	DefaultElectionMax = 300 * time.Millisecond
	DefaultHeartbeat   = 50 * time.Millisecond
	DefaultMaxBatch    = 64  // entries per AppendEntries
	DefaultMaxPending  = 256 // proposals waiting for commit on a leader
)

// Config configures one server.
type Config struct {
	ID              string   // this server's node name
	Peers           []string // every server's node name, ID included, in one fixed order
	ElectionMin     time.Duration
	ElectionMax     time.Duration
	Heartbeat       time.Duration
	MaxBatch        int
	MaxPending      int
	NewStateMachine func() StateMachine // called once per incarnation
	Store           Store               // production storage; nil in simulation
	Observer        Observer            // nil in production
	Bugs            Bug

	// send sends msg to a peer; nil means lockstep.Send[Msg]. Tests set it to see or fail sends.
	send func(from lockstep.Sender, to lockstep.PID[Msg], msg Msg) error
}

// New validates cfg, applies defaults and returns the actor factory.
func New(cfg Config) (func() lockstep.Actor[Msg], error) {
	cfg.Peers = slices.Clone(cfg.Peers)
	if cfg.ElectionMin == 0 {
		cfg.ElectionMin = DefaultElectionMin
	}
	if cfg.ElectionMax == 0 {
		cfg.ElectionMax = DefaultElectionMax
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = DefaultHeartbeat
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = DefaultMaxBatch
	}
	if cfg.MaxPending == 0 {
		cfg.MaxPending = DefaultMaxPending
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	send := cfg.send
	if send == nil {
		send = lockstep.Send[Msg]
	}
	return func() lockstep.Actor[Msg] { return &Node{cfg: cfg, send: send} }, nil
}

// Spawn spawns New(cfg)'s actor as "raft" under sys's user guardian, at DefaultPath.
func Spawn(sys *lockstep.System, cfg Config) (lockstep.PID[Msg], error) {
	factory, err := New(cfg)
	if err != nil {
		return lockstep.PID[Msg]{}, err
	}
	return lockstep.Spawn(sys, "raft", factory)
}

// validate returns the first broken rule of a config whose defaults are applied.
func (c Config) validate() error {
	if c.ID == "" {
		return fmt.Errorf("raft: ID is empty")
	}
	for i, p := range c.Peers {
		if slices.Contains(c.Peers[:i], p) {
			return fmt.Errorf("raft: Peers has duplicate %q", p)
		}
	}
	if !slices.Contains(c.Peers, c.ID) {
		return fmt.Errorf("raft: ID %q is not in Peers", c.ID)
	}
	if c.ElectionMin <= 0 || c.ElectionMin > c.ElectionMax {
		return fmt.Errorf("raft: ElectionMin (%v) must be positive and at most ElectionMax (%v)",
			c.ElectionMin, c.ElectionMax)
	}
	if c.Heartbeat <= 0 || c.Heartbeat >= c.ElectionMin {
		return fmt.Errorf("raft: Heartbeat (%v) must be positive and below ElectionMin (%v)",
			c.Heartbeat, c.ElectionMin)
	}
	if c.MaxBatch <= 0 {
		return fmt.Errorf("raft: MaxBatch (%d) must be positive", c.MaxBatch)
	}
	if c.MaxPending <= 0 {
		return fmt.Errorf("raft: MaxPending (%d) must be positive", c.MaxPending)
	}
	if c.NewStateMachine == nil {
		return fmt.Errorf("raft: NewStateMachine is nil")
	}
	if undefined := c.Bugs &^ allBugs; undefined != 0 {
		return fmt.Errorf("raft: Bugs has undefined bits 0x%x", uint32(undefined))
	}
	return nil
}
