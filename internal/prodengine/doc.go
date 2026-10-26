// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package prodengine is the production engine behind lockstep.New from Phase 2 (architecture §13):
// a fixed pool of workers runs every turn and callback.
package prodengine

// LocalQueueSize is the capacity of each worker's local run queue.
const LocalQueueSize = 256
