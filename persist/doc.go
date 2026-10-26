// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package persist defines the storage contracts for durable actor state: a Journal that stores
// the events of many persistence IDs, a SnapshotStore that stores their snapshots, and a Codec
// that turns events and states into bytes.
//
// A persistence ID is a string of 1 to MaxIDLen bytes that names one stream of events. Events in
// a stream are numbered 1, 2, 3 and so on, without gaps. A snapshot records the encoded state
// after one event, so recovery can start from the newest snapshot and replay only the events
// after it.
//
// A Journal separates writing from committing. Write accepts a batch of events, Commit makes
// every written event durable, and only committed events count as stored. If the process stops
// between the two, Recover on the next start reports which events survived.
//
// JSON returns a Codec for concrete types that uses encoding/json. A type that needs another
// format implements Codec itself.
//
// The package persist/filestore provides a Journal and a SnapshotStore that keep their data in
// files.
package persist
