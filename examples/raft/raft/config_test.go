// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"strings"
	"testing"
	"time"
)

// validConfig returns a valid three-server config for server n1.
func validConfig() Config {
	return Config{
		ID:              "n1",
		Peers:           []string{"n1", "n2", "n3"},
		ElectionMin:     100 * time.Millisecond,
		ElectionMax:     200 * time.Millisecond,
		Heartbeat:       20 * time.Millisecond,
		MaxBatch:        8,
		MaxPending:      16,
		NewStateMachine: func() StateMachine { return nil },
	}
}

// TestATRFT04NewErrors checks that New reports the first broken rule, in order, with exact texts.
func TestATRFT04NewErrors(t *testing.T) {
	tests := []struct {
		name string
		edit func(c *Config)
		want string
	}{
		{"empty ID", func(c *Config) { c.ID = "" }, "raft: ID is empty"},
		{"duplicate peer", func(c *Config) { c.Peers = []string{"n1", "n2", "n2"} }, `raft: Peers has duplicate "n2"`},
		{"ID not in Peers", func(c *Config) { c.ID = "n9" }, `raft: ID "n9" is not in Peers`},
		{"no Peers", func(c *Config) { c.Peers = nil }, `raft: ID "n1" is not in Peers`},
		{
			"ElectionMin above ElectionMax",
			func(c *Config) { c.ElectionMin = 300 * time.Millisecond },
			"raft: ElectionMin (300ms) must be positive and at most ElectionMax (200ms)",
		},
		{
			"negative ElectionMin",
			func(c *Config) { c.ElectionMin = -time.Second },
			"raft: ElectionMin (-1s) must be positive and at most ElectionMax (200ms)",
		},
		{
			"Heartbeat equals ElectionMin",
			func(c *Config) { c.Heartbeat = 100 * time.Millisecond },
			"raft: Heartbeat (100ms) must be positive and below ElectionMin (100ms)",
		},
		{
			"negative Heartbeat",
			func(c *Config) { c.Heartbeat = -time.Millisecond },
			"raft: Heartbeat (-1ms) must be positive and below ElectionMin (100ms)",
		},
		{"negative MaxBatch", func(c *Config) { c.MaxBatch = -1 }, "raft: MaxBatch (-1) must be positive"},
		{"negative MaxPending", func(c *Config) { c.MaxPending = -5 }, "raft: MaxPending (-5) must be positive"},
		{"nil NewStateMachine", func(c *Config) { c.NewStateMachine = nil }, "raft: NewStateMachine is nil"},
		{"undefined Bugs bit", func(c *Config) { c.Bugs = 1 << 3 }, "raft: Bugs has undefined bits 0x8"},
		{
			"undefined Bugs bits with defined ones",
			func(c *Config) { c.Bugs = BugStaleRead | 1<<4 | 1<<9 },
			"raft: Bugs has undefined bits 0x210",
		},
		{
			// Two rules broken: the earlier one (Peers) wins.
			"duplicate peer and negative MaxBatch",
			func(c *Config) {
				c.Peers = []string{"n1", "n1", "n3"}
				c.MaxBatch = -1
			},
			`raft: Peers has duplicate "n1"`,
		},
		{
			// Two rules broken: MaxBatch comes before NewStateMachine.
			"MaxBatch and NewStateMachine",
			func(c *Config) {
				c.MaxBatch = -2
				c.NewStateMachine = nil
			},
			"raft: MaxBatch (-2) must be positive",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.edit(&cfg)
			f, err := New(cfg)
			if err == nil {
				t.Fatalf("New succeeded, want error %q", tc.want)
			}
			if err.Error() != tc.want {
				t.Fatalf("New error = %q, want %q", err.Error(), tc.want)
			}
			if f != nil {
				t.Fatal("New returned a factory together with an error")
			}
		})
	}
}

// TestNewAppliesDefaults checks that zero timing and size fields get the documented defaults.
func TestNewAppliesDefaults(t *testing.T) {
	cfg := validConfig()
	cfg.ElectionMin, cfg.ElectionMax, cfg.Heartbeat, cfg.MaxBatch, cfg.MaxPending = 0, 0, 0, 0, 0
	f, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n, ok := f().(*Node)
	if !ok {
		t.Fatalf("factory returned %T, want *Node", f())
	}
	got := n.cfg
	if got.ElectionMin != DefaultElectionMin || got.ElectionMax != DefaultElectionMax ||
		got.Heartbeat != DefaultHeartbeat || got.MaxBatch != DefaultMaxBatch ||
		got.MaxPending != DefaultMaxPending {
		t.Fatalf("defaults = %v/%v/%v/%d/%d, want %v/%v/%v/%d/%d",
			got.ElectionMin, got.ElectionMax, got.Heartbeat, got.MaxBatch, got.MaxPending,
			DefaultElectionMin, DefaultElectionMax, DefaultHeartbeat, DefaultMaxBatch, DefaultMaxPending)
	}
	if DefaultElectionMin != 150*time.Millisecond || DefaultElectionMax != 300*time.Millisecond ||
		DefaultHeartbeat != 50*time.Millisecond || DefaultMaxBatch != 64 || DefaultMaxPending != 256 {
		t.Fatal("default constants differ from the documented values")
	}
}

// TestNewKeepsSetFields checks that non-zero fields are not replaced by defaults.
func TestNewKeepsSetFields(t *testing.T) {
	cfg := validConfig()
	f, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n, ok := f().(*Node)
	if !ok {
		t.Fatalf("factory returned %T, want *Node", f())
	}
	if n.cfg.ElectionMin != cfg.ElectionMin || n.cfg.Heartbeat != cfg.Heartbeat ||
		n.cfg.MaxBatch != cfg.MaxBatch || n.cfg.MaxPending != cfg.MaxPending {
		t.Fatalf("New changed set fields: %+v", n.cfg)
	}
}

// TestNewCopiesPeers checks that later changes to the caller's Peers do not reach the actor.
func TestNewCopiesPeers(t *testing.T) {
	cfg := validConfig()
	f, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cfg.Peers[1] = "zz"
	n, ok := f().(*Node)
	if !ok {
		t.Fatalf("factory returned %T, want *Node", f())
	}
	if got := strings.Join(n.cfg.Peers, ","); got != "n1,n2,n3" {
		t.Fatalf("actor Peers = %s, want n1,n2,n3", got)
	}
}

// TestFactoryReturnsFreshNodes checks that each factory call returns a new actor.
func TestFactoryReturnsFreshNodes(t *testing.T) {
	f, err := New(validConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if f() == f() {
		t.Fatal("factory returned the same actor twice")
	}
}
