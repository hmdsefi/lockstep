// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package filestore stores the events and snapshots of persistent actors in files. A Store is a
// persist.Journal and a persist.SnapshotStore: it appends events to segment files of
// length-prefixed records protected by CRC-32C checksums, calls fsync when a commit returns, and
// repairs a torn tail when recovery runs.
//
// Open returns a Store on a directory of the operating system's file system. New returns a Store
// on any FS, the small file system interface a Store runs on, so tests can run it on a simulated
// disk that injects write and sync failures.
//
// A Store keeps its data under one root directory:
//
//	FORMAT                                   the store format, "lockstep-filestore 1\n"
//	journal/id-<enc>/<20-digit first seq>.seg
//	snapshots/id-<enc>/<20-digit seq>.snap
//
// <enc> is the persistence ID encoded byte by byte. The lower-case letters a to z, the digits,
// '-' and '_' stay as they are. Every other byte becomes '%' and two upper-case hex digits, so
// "Acct.1" becomes "%41cct%2E1". Two IDs never share a directory, even on a file system that
// ignores case, and the longest ID (persist.MaxIDLen bytes) gives a name below 255 bytes.
//
// A Store opens every file inside the call that needs it and closes the file before the call
// returns. Calls for one persistence ID run one after another; calls for different IDs may
// overlap.
//
// Every error wraps the error of the FS call that caused it, so errors.Is reaches the io/fs
// sentinels and any error the FS defines. A file that fails its checksum or does not parse
// reports an error that wraps ErrCorrupt.
package filestore
