// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/faultline/kernel/simnet"

	"github.com/hmdsefi/lockstep/examples/raft/raft"
	"github.com/hmdsefi/lockstep/sim"
)

func TestFaultsString(t *testing.T) {
	tests := []struct {
		f    Faults
		want string
	}{
		{FaultsNone, "none"},
		{FaultsPartition, "partition"},
		{FaultsCrash, "crash"},
		{FaultsDefault, "default"},
		{Faults(9), "faults(9)"},
	}
	for _, tt := range tests {
		if got := tt.f.String(); got != tt.want {
			t.Errorf("Faults(%d).String() = %q, want %q", uint8(tt.f), got, tt.want)
		}
	}
}

// TestATRFT35Presets checks RFT-420: each preset's rules are the table's, in order, with the
// faultline defaults for MaxDown and Quiet, and FaultsNone has no planner.
func TestATRFT35Presets(t *testing.T) {
	partition := []fault.Rule{
		{Kind: fault.KindPartition, Every: 2 * time.Second, MinFor: 300 * time.Millisecond, MaxFor: 1500 * time.Millisecond},
		{Kind: fault.KindIsolate, Every: 4 * time.Second, MinFor: 300 * time.Millisecond, MaxFor: 1500 * time.Millisecond, Target: "leader"},
	}
	crash := []fault.Rule{
		{Kind: fault.KindCrash, Every: 2 * time.Second, MinFor: 50 * time.Millisecond, MaxFor: time.Second},
	}
	var all []fault.Rule
	all = append(all, partition...)
	all = append(all, crash...)
	all = append(all,
		fault.Rule{Kind: fault.KindPause, Every: 4 * time.Second, MaxFor: 500 * time.Millisecond},
		fault.Rule{
			Kind: fault.KindLink, Every: 3 * time.Second, MinFor: 200 * time.Millisecond, MaxFor: time.Second,
			Link: &simnet.Link{Latency: time.Millisecond, Jitter: 20 * time.Millisecond, DropPPM: 100000, FIFO: true},
		},
		fault.Rule{Kind: fault.KindClockDrift, Every: 10 * time.Second, Magnitude: 100000},
	)

	if p := preset(FaultsNone); p != nil {
		t.Errorf("preset(none) = %+v, want nil", p)
	}
	tests := []struct {
		f    Faults
		want []fault.Rule
	}{
		{FaultsPartition, partition},
		{FaultsCrash, crash},
		{FaultsDefault, all},
	}
	for _, tt := range tests {
		p := preset(tt.f)
		if p == nil {
			t.Errorf("preset(%s) = nil, want a planner", tt.f)
			continue
		}
		if !reflect.DeepEqual(p.Rules, tt.want) {
			t.Errorf("preset(%s).Rules =\n%+v\nwant\n%+v", tt.f, p.Rules, tt.want)
		}
		if p.MaxDown != 0 || p.Quiet != 0 {
			t.Errorf("preset(%s): MaxDown %d, Quiet %v; want 0 and 0", tt.f, p.MaxDown, p.Quiet)
		}
	}
	if a, b := preset(FaultsDefault), preset(FaultsDefault); a == b || &a.Rules[0] == &b.Rules[0] {
		t.Error("preset returns shared values; want a new planner per call")
	}
}

func TestDefaultCluster(t *testing.T) {
	want := Cluster{
		Servers: 3, Clients: 3, Keys: 4, Faults: FaultsDefault,
		PersistMin: time.Millisecond, PersistMax: 5 * time.Millisecond, ProgressWithin: 3 * time.Second,
	}
	if got := DefaultCluster(); !reflect.DeepEqual(got, want) {
		t.Errorf("DefaultCluster() = %+v, want %+v", got, want)
	}
	if got, want := Options(7), (sim.Options{Options: faultline.Options{Seeds: 7, Duration: 20 * time.Second, BaseSeed: 1}}); !reflect.DeepEqual(got, want) {
		t.Errorf("Options(7) = %+v, want %+v", got, want)
	}
}

// TestATRFT32SetupErrors checks §7: an invalid Cluster gives the first broken rule's text, Setup
// fails the test with it, and a second Setup on one Sim panics.
func TestATRFT32SetupErrors(t *testing.T) {
	edit := func(f func(c *Cluster)) Cluster {
		c := DefaultCluster()
		f(&c)
		return c
	}
	tests := []struct {
		name string
		c    Cluster
		want string
	}{
		{"servers", edit(func(c *Cluster) { c.Servers = 4 }), "raftsim: Servers (4) must be 3 or 5"},
		{"no servers", edit(func(c *Cluster) { c.Servers = 0 }), "raftsim: Servers (0) must be 3 or 5"},
		{"no clients", edit(func(c *Cluster) { c.Clients = 0 }), "raftsim: Clients (0) must be in [1, 8]"},
		{"clients", edit(func(c *Cluster) { c.Clients = 9 }), "raftsim: Clients (9) must be in [1, 8]"},
		{"no keys", edit(func(c *Cluster) { c.Keys = 0 }), "raftsim: Keys (0) must be in [1, 16]"},
		{"keys", edit(func(c *Cluster) { c.Keys = 17 }), "raftsim: Keys (17) must be in [1, 16]"},
		{"persist min negative", edit(func(c *Cluster) { c.PersistMin = -time.Millisecond }),
			"raftsim: PersistMin (-1ms) must be in [0, PersistMax (5ms)]"},
		{"persist min above max", edit(func(c *Cluster) { c.PersistMin = 6 * time.Millisecond }),
			"raftsim: PersistMin (6ms) must be in [0, PersistMax (5ms)]"},
		{"progress", edit(func(c *Cluster) { c.ProgressWithin = 0 }), "raftsim: ProgressWithin (0s) must be positive"},
		{"faults", edit(func(c *Cluster) { c.Faults = 4 }), "raftsim: unknown Faults 4"},
		{"bugs", edit(func(c *Cluster) { c.Bugs = raft.BugStaleRead | 1<<5 }), "raftsim: Bugs has undefined bits 0x20"},
		{"raft config", edit(func(c *Cluster) { c.Raft.Heartbeat = time.Second }),
			"raftsim: raft: Heartbeat (1s) must be positive and below ElectionMin (150ms)"},
		{"client config", edit(func(c *Cluster) { c.Client.ReadPercent = 101 }),
			"raftsim: client: ReadPercent (101) must be in [0, 100]"},
		{"two rules", edit(func(c *Cluster) { c.Keys = 0; c.Servers = 1 }), "raftsim: Servers (1) must be 3 or 5"},
		{"raft before client", edit(func(c *Cluster) { c.Raft.MaxBatch = -1; c.Client.Timeout = -1 }),
			"raftsim: raft: MaxBatch (-1) must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.c.validate()
			if err == nil || err.Error() != tt.want {
				t.Errorf("validate() = %v, want %q", err, tt.want)
			}
		})
	}
	if err := DefaultCluster().validate(); err != nil {
		t.Errorf("DefaultCluster().validate() = %v, want nil", err)
	}

	t.Run("child", func(t *testing.T) {
		out, code := runChild(t, "TestSetupErrorChild", "RAFTDEMO_SETUP_CHILD=1")
		if code != 1 || !strings.Contains(out, "raftsim: Servers (4) must be 3 or 5") {
			t.Errorf("child exited %d; want 1 and the Servers text in its output:\n%s", code, out)
		}
	})

	t.Run("twice", func(t *testing.T) {
		opts := sim.Options{Options: faultline.Options{Seeds: 1, Duration: 100 * time.Millisecond, BaseSeed: 1}}
		sim.Simulate(t, opts, func(s *sim.Sim) {
			setup(s, DefaultCluster(), hooks{reg: &recordingRegistrar{}})
			defer func() {
				if got := recover(); got != "raftsim: Setup called twice" {
					t.Errorf("second Setup panicked with %v, want %q", got, "raftsim: Setup called twice")
				}
			}()
			Setup(s, DefaultCluster())
		})
	})
}

// TestSetupErrorChild runs only as the child process of TestATRFT32SetupErrors: it runs Test with
// an invalid Cluster, which fails the test.
func TestSetupErrorChild(t *testing.T) {
	if os.Getenv("RAFTDEMO_SETUP_CHILD") == "" {
		t.Skip("runs only as a child process of TestATRFT32SetupErrors")
	}
	c := DefaultCluster()
	c.Servers = 4
	Test(t, Options(1), c)
}
