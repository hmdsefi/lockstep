// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// Store is the storage a Persist or Load effect uses in production. Simulation never calls it.
type Store interface {
	// Save writes hs and replaces the log from index from on: entries at index >= from are
	// removed, then entries are appended. from == 0 leaves the log unchanged.
	Save(node string, hs HardState, from uint64, entries []Entry) error
	// Load returns the durable state of node, or the zero Durable if nothing was saved.
	Load(node string) (Durable, error)
}

// MemStore is an in-memory Store, safe for concurrent use. Its contents last as long as the value.
type MemStore struct {
	mu    sync.Mutex
	nodes map[string]Durable
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{nodes: make(map[string]Durable)}
}

// Save implements Store. It copies entries, and it returns an error without changing anything
// when from is more than one past the last stored index, because the log would have a hole.
func (s *MemStore) Save(node string, hs HardState, from uint64, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.nodes[node]
	if from > 0 {
		if from > uint64(len(d.Log))+1 {
			return fmt.Errorf("raft: Save from %d leaves a hole after log index %d", from, len(d.Log))
		}
		d.Log = append(slices.Clone(d.Log[:from-1]), entries...)
	}
	d.HardState = hs
	s.nodes[node] = d
	return nil
}

// Load implements Store. It returns copies, so callers never share the store's slices.
func (s *MemStore) Load(node string) (Durable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.nodes[node]
	d.Log = slices.Clone(d.Log)
	return d, nil
}

// Persist is the effect that makes a server's state durable. Its name is "raft.persist".
type Persist struct {
	Store     Store // used by Run; nil in simulation
	Node      string
	HardState HardState
	From      uint64  // see Store.Save
	Entries   []Entry // owned by the effect after Do
}

// Name returns "raft.persist".
func (Persist) Name() string { return "raft.persist" }

// Run calls p.Store.Save. It returns ErrNoStore if p.Store is nil.
func (p Persist) Run(context.Context) (struct{}, error) {
	if p.Store == nil {
		return struct{}{}, ErrNoStore
	}
	return struct{}{}, p.Store.Save(p.Node, p.HardState, p.From, p.Entries)
}

// Load is the effect that reads a server's durable state at start. Its name is "raft.load".
type Load struct {
	Store Store // used by Run; nil in simulation
	Node  string
}

// Name returns "raft.load".
func (Load) Name() string { return "raft.load" }

// Run calls l.Store.Load. It returns ErrNoStore if l.Store is nil.
func (l Load) Run(context.Context) (Durable, error) {
	if l.Store == nil {
		return Durable{}, ErrNoStore
	}
	return l.Store.Load(l.Node)
}

// ErrNoStore is returned by Persist.Run and Load.Run when Store is nil.
var ErrNoStore = errors.New("raft: no Store configured")
