// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"strconv"
	"testing"
)

// skipHeavy skips tests that run 100 or more seeds, or run seeds twice, under -short and under
// the race detector (RFT §10). Run them with plain go test.
func skipHeavy(t *testing.T) {
	t.Helper()
	if testing.Short() || raceEnabled {
		t.Skip("runs hundreds of simulated seeds; skipped under -short and -race")
	}
}

// AT-RFT-47 (RFT-560): the bug-free cluster passes 200 seeds of each fault preset with 3 and
// with 5 servers.
func TestATRFT47BugFree(t *testing.T) {
	skipHeavy(t)
	for _, f := range []Faults{FaultsPartition, FaultsCrash, FaultsDefault} {
		for _, n := range []int{3, 5} {
			t.Run(f.String()+"/"+strconv.Itoa(n), func(t *testing.T) {
				c := DefaultCluster()
				c.Faults = f
				c.Servers = n
				Test(t, Options(200), c)
			})
		}
	}
}

// AT-RFT-49 (§6): every seed of DefaultCluster() runs twice with equal traces, through both
// lockstep's and faultline's determinism checks.
func TestATRFT49Determinism(t *testing.T) {
	skipHeavy(t)
	t.Setenv("LOCKSTEP_CHECK_DETERMINISM", "1")
	t.Setenv("FAULTLINE_CHECK_DETERMINISM", "1")
	Test(t, Options(20), DefaultCluster())
}
