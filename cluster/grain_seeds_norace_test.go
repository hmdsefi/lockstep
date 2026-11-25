// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !race

package cluster

// The default numbers of seeds of the grain property tests (grain_properties_sim_test.go), without
// the race detector; grain_seeds_race_test.go has fewer under it. FAULTLINE_SEEDS overrides both.
const (
	// grainSeeds is the number of seeds of AT-VAC-21 and AT-VAC-22.
	grainSeeds = 30
	// drawSeeds is the number of seeds of each workload of AT-VAC-23.
	drawSeeds = 8
)
