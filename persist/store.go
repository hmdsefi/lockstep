// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist

import (
	"context"
	"errors"
)

const (
	// MaxIDLen is the longest persistence ID in bytes.
	MaxIDLen = 80
	// DefaultReplayBatch is the number of events recovery reads per Journal.Read call.
	DefaultReplayBatch = 256
)

var (
	// ErrInvalidID is returned when a persistence ID is empty, is longer than MaxIDLen bytes,
	// or is not accepted by the store.
	ErrInvalidID = errors.New("persist: invalid persistence id")
	// ErrInvalidConfig is returned when a configuration breaks a documented requirement.
	ErrInvalidConfig = errors.New("persist: invalid config")
	// ErrSeqConflict is returned when the first sequence number of a Write is not the next one
	// of the stream.
	ErrSeqConflict = errors.New("persist: sequence conflict")
	// ErrMissingEvents is returned when a Read starts below the lowest retained sequence number.
	ErrMissingEvents = errors.New("persist: events no longer retained")
	// ErrInconsistent is returned when recovery finds a journal and a snapshot that do not fit
	// together.
	ErrInconsistent = errors.New("persist: journal and snapshot are inconsistent")
)

// Recovery reports how the state of an actor incarnation was rebuilt.
type Recovery struct {
	SnapshotSeq uint64 // sequence number of the snapshot used; 0 if none
	Events      uint64 // events replayed after the snapshot
	LastSeq     uint64 // sequence number of the last recovered event; 0 for a new stream
	Repaired    int64  // bytes the journal discarded as a torn tail (Bounds.Repaired)
}

// Event is one stored event.
type Event struct {
	Seq  uint64
	Data []byte
}

// Snapshot is one stored snapshot: the encoded state after event Seq.
type Snapshot struct {
	Seq  uint64
	Data []byte
}

// Bounds describes what a journal holds for one persistence ID.
type Bounds struct {
	First    uint64 // lowest retained sequence number; Last+1 when no event is retained
	Last     uint64 // highest committed sequence number; 0 for a new stream
	Repaired int64  // bytes Recover discarded as a torn tail; 0 for stores that never repair
}

// Journal stores the events of many persistence IDs. Every implementation must meet the
// following for each persistence ID.
//
// Sequence numbers start at 1 and are consecutive.
//
// Recover returns Bounds{First, Last, Repaired} with 1 <= First <= Last+1. Every event with
// First <= Seq <= Last is durable. An event written before the call and outside that range is
// gone for good: the next accepted Write starts at Last+1.
//
// A Write of events whose Seq values are W+1, W+2, and so on is accepted, where W is the last
// written sequence number: Last after Recover, then advanced by each accepted Write. Any other
// sequence numbers return an error wrapping ErrSeqConflict. An empty batch returns an error
// wrapping fs.ErrInvalid. A rejected Write changes nothing.
//
// A Commit that returns nil means every event written before the call is durable. After an error,
// the events written since the last successful Commit may or may not be durable; the next Recover
// reports which.
//
// Batches are atomic: a later Recover reports all events of a Write or none of them.
//
// Read with from < Bounds.First returns an error wrapping ErrMissingEvents. Read with from above
// the last committed sequence number returns (nil, nil). Otherwise Read returns the committed
// events from through min(last committed, from+max-1) in order.
//
// DeleteEvents never deletes an event with a Seq above to and never changes Bounds.Last.
//
// Calls for different persistence IDs may run concurrently. Calls for one ID are serialized.
//
// Write must not modify or keep events[i].Data after it returns. Slices that Read returns belong
// to the caller.
//
// A call whose context is already done returns ctx.Err() and changes nothing.
type Journal interface {
	// Recover makes every written event of id durable or discards it, repairs the stream if the
	// store can, and returns its bounds. Persist calls it at the start of every recovery, before
	// any Write of that recovery.
	Recover(ctx context.Context, id string) (Bounds, error)
	// Read returns up to max committed events of id with Seq >= from, in order, without gaps.
	// max must be at least 1.
	Read(ctx context.Context, id string, from uint64, max int) ([]Event, error)
	// Write appends one batch. The events need not be durable until Commit returns nil.
	Write(ctx context.Context, id string, events []Event) error
	// Commit makes every event of id written before the call durable.
	Commit(ctx context.Context, id string) error
	// DeleteEvents may delete events of id with Seq <= to. It never deletes later events.
	DeleteEvents(ctx context.Context, id string, to uint64) error
}

// SnapshotStore stores snapshots of many persistence IDs. Every implementation must meet the
// following for each persistence ID.
//
// Save is durable when it returns nil: a later Load can return the snapshot.
//
// Load returns the intact snapshot with the greatest Seq and skips damaged ones.
//
// DeleteSnapshots deletes only snapshots with Seq <= to.
//
// Calls for different persistence IDs may run concurrently. Calls for one ID are serialized.
//
// Save must not modify or keep snap.Data after it returns. The Data that Load returns belongs to
// the caller.
//
// A call whose context is already done returns ctx.Err() and changes nothing.
type SnapshotStore interface {
	// Save stores snap durably.
	Save(ctx context.Context, id string, snap Snapshot) error
	// Load returns the newest intact snapshot of id, or ok == false if there is none.
	Load(ctx context.Context, id string) (snap Snapshot, ok bool, err error)
	// DeleteSnapshots deletes the snapshots of id with Seq <= to.
	DeleteSnapshots(ctx context.Context, id string, to uint64) error
}
