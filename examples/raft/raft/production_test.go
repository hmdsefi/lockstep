// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/raft/kv"
)

// askTimeout bounds every ask and wait of the production tests.
const askTimeout = 5 * time.Second

// oneServer returns the Config of the one-server cluster n1 with a kv state machine and store.
func oneServer(store Store) Config {
	return Config{
		ID:              "n1",
		Peers:           []string{"n1"},
		Store:           store,
		NewStateMachine: func() StateMachine { return kv.New() },
	}
}

// spawnOne spawns cfg's Node on a new production System that the test shuts down at its end.
func spawnOne(t *testing.T, cfg Config) (*lockstep.System, lockstep.PID[Msg]) {
	t.Helper()
	sys := lockstep.New()
	t.Cleanup(func() { shutdown(t, sys) })
	pid, err := Spawn(sys, cfg)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	return sys, pid
}

// shutdown shuts sys down, waiting at most askTimeout.
func shutdown(t *testing.T, sys *lockstep.System) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), askTimeout)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// ask sends Propose{Client: 1, Seq: seq, Cmd: cmd} to pid from sys.
func ask(sys *lockstep.System, pid lockstep.PID[Msg], seq uint64, cmd string) *lockstep.Reply[Result] {
	return lockstep.Ask(sys, pid, func(rt lockstep.ReplyTo[Result]) Msg {
		return Propose{Client: 1, Seq: seq, Cmd: cmd, ReplyTo: rt}
	}, lockstep.WithTimeout(askTimeout))
}

// propose asks pid to apply cmd and awaits the result, asking again while the server answers
// NotLeader (it leads after its first election timeout), for at most askTimeout in all.
func propose(t *testing.T, sys *lockstep.System, pid lockstep.PID[Msg], seq uint64, cmd string) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), askTimeout)
	defer cancel()
	for {
		r, err := ask(sys, pid, seq, cmd).Await(ctx)
		if err != nil {
			t.Fatalf("Propose %q: %v", cmd, err)
		}
		if r.Status != NotLeader {
			return r
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Propose %q: still %v after %v", cmd, r.Status, askTimeout)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestATRFT11SingleServerProduction checks RFT-130: a one-server cluster on the production engine
// elects itself, commits and applies proposals, and answers them with OK.
func TestATRFT11SingleServerProduction(t *testing.T) {
	sys, pid := spawnOne(t, oneServer(NewMemStore()))

	put := propose(t, sys, pid, 1, kv.EncodePut("k", "v"))
	if put.Status != OK {
		t.Fatalf("put: %+v; want OK", put)
	}
	get := propose(t, sys, pid, 2, kv.EncodeGet("k"))
	if get.Status != OK {
		t.Fatalf("get: %+v; want OK", get)
	}
	if v, ok := kv.DecodeGetOutput(get.Output); !ok || v != "v" {
		t.Errorf("get output %q decodes to (%q, %v); want (\"v\", true)", get.Output, v, ok)
	}
	if get.Index <= put.Index {
		t.Errorf("get index %d, put index %d; want the get after the put", get.Index, put.Index)
	}
}

// blockingStore is a MemStore whose Save and Load wait until release is closed.
type blockingStore struct {
	*MemStore
	release chan struct{}
}

func (s blockingStore) Save(node string, hs HardState, from uint64, entries []Entry) error {
	<-s.release
	return s.MemStore.Save(node, hs, from, entries)
}

func (s blockingStore) Load(node string) (Durable, error) {
	<-s.release
	return s.MemStore.Load(node)
}

// saveBlockingStore is a MemStore whose Save waits until release is closed.
type saveBlockingStore struct {
	*MemStore
	release chan struct{}
}

func (s saveBlockingStore) Save(node string, hs HardState, from uint64, entries []Entry) error {
	<-s.release
	return s.MemStore.Save(node, hs, from, entries)
}

// progressObserver publishes how far a Node got, for a test on another goroutine.
type progressObserver struct {
	nopObserver
	leader  atomic.Bool   // BecameLeader was called
	written atomic.Uint64 // the highest from of a LogWritten call
}

func (o *progressObserver) BecameLeader(string, uint64, LogView) { o.leader.Store(true) }

func (o *progressObserver) LogWritten(_ string, from uint64, _ LogView) {
	if from > o.written.Load() {
		o.written.Store(from)
	}
}

// waitFor polls cond until it holds, failing the test after askTimeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(askTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("no %s after %v", what, askTimeout)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestStoppedRepliesUnknown checks RFT-260's Stopped: a leader that stops answers Unknown to its
// pending proposals. The store never completes a Save, so the proposal stays pending.
func TestStoppedRepliesUnknown(t *testing.T) {
	store := saveBlockingStore{MemStore: NewMemStore(), release: make(chan struct{})}
	defer close(store.release)
	obs := &progressObserver{}
	cfg := oneServer(store)
	cfg.Observer = obs
	sys := lockstep.New()
	pid, err := Spawn(sys, cfg)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// The leader's first persist (term, vote and no-op) never completes, but it leads in memory.
	waitFor(t, "leader", obs.leader.Load)
	reply := ask(sys, pid, 1, kv.EncodePut("k", "v"))
	waitFor(t, "appended proposal", func() bool { return obs.written.Load() >= 2 })

	shutdown(t, sys)
	ctx, cancel := context.WithTimeout(context.Background(), askTimeout)
	defer cancel()
	r, err := reply.Await(ctx)
	if err != nil || r != (Result{Status: Unknown}) {
		t.Errorf("reply after Shutdown: %+v, %v; want {Status: Unknown}, nil", r, err)
	}
}

// TestStoppedBeforeLoaded checks RFT-080 before the load completes: a Propose gets NotLeader with
// no leader hint, and stopping the Node is clean.
func TestStoppedBeforeLoaded(t *testing.T) {
	store := blockingStore{MemStore: NewMemStore(), release: make(chan struct{})}
	defer close(store.release)
	sys := lockstep.New()
	pid, err := Spawn(sys, oneServer(store))
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), askTimeout)
	defer cancel()
	r, err := ask(sys, pid, 1, kv.EncodePut("k", "v")).Await(ctx)
	if err != nil || r != (Result{Status: NotLeader}) {
		t.Errorf("reply before load: %+v, %v; want {Status: NotLeader}, nil", r, err)
	}
	shutdown(t, sys)
}

// TestSpawnInvalidConfig checks that Spawn returns New's error and spawns nothing.
func TestSpawnInvalidConfig(t *testing.T) {
	sys := lockstep.New()
	defer shutdown(t, sys)
	cfg := oneServer(NewMemStore())
	cfg.ID = ""
	if _, err := Spawn(sys, cfg); err == nil || err.Error() != "raft: ID is empty" {
		t.Errorf("Spawn: %v; want raft: ID is empty", err)
	}
	if _, err := Spawn(sys, oneServer(NewMemStore())); err != nil {
		t.Errorf("Spawn after a refused one: %v", err)
	}
	if _, err := Spawn(sys, oneServer(NewMemStore())); !errors.Is(err, lockstep.ErrNameTaken) {
		t.Errorf("second Spawn: %v; want ErrNameTaken", err)
	}
}
