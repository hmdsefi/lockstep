// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"slices"
	"testing"
)

var (
	e1 = Entry{Term: 1, Client: 1, Seq: 1, Cmd: "put a 1"}
	e2 = Entry{Term: 1, Client: 1, Seq: 2, Cmd: "put b 2"}
	e3 = Entry{Term: 2, Client: 2, Seq: 1, Cmd: "put c 3"}
)

// TestATRFT07MemStore checks Save's replace-from-index rule, Load's zero value and the copying.
func TestATRFT07MemStore(t *testing.T) {
	s := NewMemStore()
	if got, err := s.Load("a"); err != nil || got.HardState != (HardState{}) || got.Log != nil {
		t.Fatalf("Load of an unknown node = %+v, %v; want the zero Durable", got, err)
	}
	if err := s.Save("a", HardState{Term: 2, Vote: "b"}, 1, []Entry{e1, e2}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save("a", HardState{Term: 2, Vote: "b"}, 2, []Entry{e3}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Load("a")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := (HardState{Term: 2, Vote: "b"}); got.HardState != want {
		t.Fatalf("HardState = %+v, want %+v", got.HardState, want)
	}
	if want := []Entry{e1, e3}; !slices.Equal(got.Log, want) {
		t.Fatalf("Log = %+v, want %+v", got.Log, want)
	}

	got.Log[0] = Entry{Term: 9}
	got.Log = append(got.Log, Entry{Term: 9})
	again, err := s.Load("a")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := []Entry{e1, e3}; !slices.Equal(again.Log, want) {
		t.Fatalf("second Load Log = %+v, want %+v: the first Load shared its slice", again.Log, want)
	}
}

// TestMemStoreSaveCopiesEntries checks that changing the slice passed to Save does not change the store.
func TestMemStoreSaveCopiesEntries(t *testing.T) {
	s := NewMemStore()
	entries := []Entry{e1, e2}
	if err := s.Save("a", HardState{Term: 1}, 1, entries); err != nil {
		t.Fatalf("Save: %v", err)
	}
	entries[0] = Entry{Term: 9}
	got, err := s.Load("a")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := []Entry{e1, e2}; !slices.Equal(got.Log, want) {
		t.Fatalf("Log = %+v, want %+v: Save kept the caller's slice", got.Log, want)
	}
}

// TestMemStoreSaveFrom checks the from rules: 0 keeps the log, a smaller from truncates, from past the end fails.
func TestMemStoreSaveFrom(t *testing.T) {
	s := NewMemStore()
	if err := s.Save("a", HardState{Term: 1}, 1, []Entry{e1, e2, e3}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// from == 0 changes the hard state only, even when entries are passed.
	if err := s.Save("a", HardState{Term: 3, Vote: "c"}, 0, []Entry{e1}); err != nil {
		t.Fatalf("Save from 0: %v", err)
	}
	got, _ := s.Load("a")
	if got.HardState != (HardState{Term: 3, Vote: "c"}) || !slices.Equal(got.Log, []Entry{e1, e2, e3}) {
		t.Fatalf("after from 0: %+v, want the new hard state and the log unchanged", got)
	}

	// from == last+1 appends.
	if err := s.Save("a", HardState{Term: 3}, 4, []Entry{e3}); err != nil {
		t.Fatalf("Save append: %v", err)
	}
	// from == 2 with no entries truncates to one entry.
	if err := s.Save("a", HardState{Term: 3}, 2, nil); err != nil {
		t.Fatalf("Save truncate: %v", err)
	}
	got, _ = s.Load("a")
	if !slices.Equal(got.Log, []Entry{e1}) {
		t.Fatalf("after truncate Log = %+v, want [e1]", got.Log)
	}

	// from past last+1 would leave a hole.
	if err := s.Save("a", HardState{Term: 3}, 3, []Entry{e3}); err == nil {
		t.Fatal("Save past the end of the log succeeded, want an error")
	}
	got, _ = s.Load("a")
	if got.HardState != (HardState{Term: 3}) || !slices.Equal(got.Log, []Entry{e1}) {
		t.Fatalf("a failed Save changed the store: %+v", got)
	}
}

// TestMemStoreNodesAreSeparate checks that two nodes do not share state.
func TestMemStoreNodesAreSeparate(t *testing.T) {
	s := NewMemStore()
	if err := s.Save("a", HardState{Term: 1, Vote: "a"}, 1, []Entry{e1}); err != nil {
		t.Fatalf("Save a: %v", err)
	}
	if err := s.Save("b", HardState{Term: 5}, 1, []Entry{e2}); err != nil {
		t.Fatalf("Save b: %v", err)
	}
	a, _ := s.Load("a")
	b, _ := s.Load("b")
	if a.HardState.Term != 1 || b.HardState.Term != 5 || a.Log[0] != e1 || b.Log[0] != e2 {
		t.Fatalf("a = %+v, b = %+v: states mixed", a, b)
	}
}

// TestEffectsWithoutStore checks the effect names and the ErrNoStore path.
func TestEffectsWithoutStore(t *testing.T) {
	ctx := context.Background()
	if got := (Persist{}).Name(); got != "raft.persist" {
		t.Fatalf("Persist.Name = %q, want raft.persist", got)
	}
	if got := (Load{}).Name(); got != "raft.load" {
		t.Fatalf("Load.Name = %q, want raft.load", got)
	}
	if _, err := (Persist{}).Run(ctx); !errors.Is(err, ErrNoStore) {
		t.Fatalf("Persist{}.Run error = %v, want ErrNoStore", err)
	}
	if _, err := (Load{}).Run(ctx); !errors.Is(err, ErrNoStore) {
		t.Fatalf("Load{}.Run error = %v, want ErrNoStore", err)
	}
	if got, want := ErrNoStore.Error(), "raft: no Store configured"; got != want {
		t.Fatalf("ErrNoStore text = %q, want %q", got, want)
	}
}

// TestEffectsWithStore checks that Persist.Run saves and Load.Run reads through the Store.
func TestEffectsWithStore(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	p := Persist{Store: s, Node: "n1", HardState: HardState{Term: 4, Vote: "n2"}, From: 1, Entries: []Entry{e1, e2}}
	if _, err := p.Run(ctx); err != nil {
		t.Fatalf("Persist.Run: %v", err)
	}
	got, err := (Load{Store: s, Node: "n1"}).Run(ctx)
	if err != nil {
		t.Fatalf("Load.Run: %v", err)
	}
	if got.HardState != (HardState{Term: 4, Vote: "n2"}) || !slices.Equal(got.Log, []Entry{e1, e2}) {
		t.Fatalf("Load.Run = %+v, want the persisted state", got)
	}
	zero, err := (Load{Store: s, Node: "unknown"}).Run(ctx)
	if err != nil || zero.HardState != (HardState{}) || len(zero.Log) != 0 {
		t.Fatalf("Load.Run of an unknown node = %+v, %v; want the zero Durable", zero, err)
	}
}

// failStore is a Store whose methods return a fixed error.
type failStore struct{ err error }

func (f failStore) Save(string, HardState, uint64, []Entry) error { return f.err }
func (f failStore) Load(string) (Durable, error)                  { return Durable{}, f.err }

// TestEffectsReturnStoreErrors checks that Run passes the Store's error through.
func TestEffectsReturnStoreErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("disk full")
	if _, err := (Persist{Store: failStore{boom}}).Run(ctx); !errors.Is(err, boom) {
		t.Fatalf("Persist.Run error = %v, want %v", err, boom)
	}
	if _, err := (Load{Store: failStore{boom}}).Run(ctx); !errors.Is(err, boom) {
		t.Fatalf("Load.Run error = %v, want %v", err, boom)
	}
}
