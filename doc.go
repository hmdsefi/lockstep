// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package lockstep is an actor runtime for Go whose actor code runs unchanged on the local engine
// of [New] and on a seeded simulation in tests (package lockstep/sim), where a failing seed replays
// exactly.
//
// The local engine runs every actor turn on one goroutine, so actors interleave but never run in
// parallel; effects started with [Do] run on separate goroutines.
package lockstep
