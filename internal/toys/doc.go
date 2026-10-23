// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package toys holds the toy actor systems of the golden suite: small actors with fixed behavior
// whose simulation runs have the same trace on every machine (DET-200).
//
// The toys use only the public API of package lockstep. Their actors keep state in fields set by
// the factory, so a restart starts fresh; they draw randomness only from Context.Rand, use no maps
// and describe every message with a Describe method. A probe is a test-owned struct that a toy
// writes to from inside its actors and the test reads after the run. That is shared state between
// actors and a test, safe only because a simulation runs on one goroutine, and it is the reason
// the toys are for simulation tests.
package toys
