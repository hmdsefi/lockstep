// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package lockstep is an actor runtime for Go whose actor code runs unchanged on the worker pool of
// [New] and on a seeded simulation in tests (package lockstep/sim), where a failing seed replays
// exactly.
//
// A system built by [New] runs actor turns on a fixed pool of worker goroutines ([WithWorkers]):
// turns of different actors can run in parallel, while the turns of one actor never overlap.
// Effects started with [Do] run on a separate bounded pool ([WithEffectPool]).
package lockstep
