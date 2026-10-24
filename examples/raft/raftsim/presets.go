// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"slices"
	"time"

	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/faultline/kernel/simnet"
)

// leaderRole is the role of the current leader (RFT-410). Isolation rules target it.
const leaderRole = "leader"

// preset returns a new planner for f (RFT-420), or nil for FaultsNone and unknown presets.
// MaxDown and Quiet stay 0, faultline's defaults: at most (n-1)/2 servers down or paused at once,
// and no new faults in the last quarter of the run. A fault.Random must not be shared by runs
// that execute concurrently, so every call builds a new one.
func preset(f Faults) *fault.Random {
	partition := []fault.Rule{
		{Kind: fault.KindPartition, Every: 2 * time.Second, MinFor: 300 * time.Millisecond, MaxFor: 1500 * time.Millisecond},
		{Kind: fault.KindIsolate, Every: 4 * time.Second, MinFor: 300 * time.Millisecond, MaxFor: 1500 * time.Millisecond, Target: leaderRole},
	}
	crash := []fault.Rule{
		{Kind: fault.KindCrash, Every: 2 * time.Second, MinFor: 50 * time.Millisecond, MaxFor: time.Second},
	}
	var rules []fault.Rule
	switch f {
	case FaultsPartition:
		rules = partition
	case FaultsCrash:
		rules = crash
	case FaultsDefault:
		rules = slices.Concat(partition, crash, []fault.Rule{
			{Kind: fault.KindPause, Every: 4 * time.Second, MaxFor: 500 * time.Millisecond},
			{
				Kind: fault.KindLink, Every: 3 * time.Second, MinFor: 200 * time.Millisecond, MaxFor: time.Second,
				Link: &simnet.Link{Latency: time.Millisecond, Jitter: 20 * time.Millisecond, DropPPM: 100000, FIFO: true},
			},
			{Kind: fault.KindClockDrift, Every: 10 * time.Second, Magnitude: 100000},
		})
	default:
		return nil
	}
	return &fault.Random{Rules: rules}
}
