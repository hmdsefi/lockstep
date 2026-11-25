// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build race

package cluster

// The default numbers of seeds of the grain property tests under the race detector, which slows
// the simulations several times over; the full numbers run without it (grain_seeds_norace_test.go).
// FAULTLINE_SEEDS overrides both.
const (
	// grainSeeds is the number of seeds of AT-VAC-21 and AT-VAC-22.
	grainSeeds = 5
	// drawSeeds is the number of seeds of each workload of AT-VAC-23.
	drawSeeds = 2
)
