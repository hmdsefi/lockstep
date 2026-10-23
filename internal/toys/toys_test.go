// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import "testing"

// TestToyArguments checks DET §7: a spawn function rejects an argument below 1 with
// "toys: invalid <name> <value>" before it spawns anything. The nil System would make a spawn
// panic, so a returned error shows that validation comes first.
func TestToyArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func() error
		want string
	}{
		{"producers", func() error { return SpawnFanIn(nil, 0, 10, &FanInProbe{}) }, "toys: invalid producers 0"},
		{"negative producers", func() error { return SpawnFanIn(nil, -2, 10, &FanInProbe{}) }, "toys: invalid producers -2"},
		{"items", func() error { return SpawnFanIn(nil, 3, 0, &FanInProbe{}) }, "toys: invalid items 0"},
		{"negative items", func() error { return SpawnFanIn(nil, 3, -1, &FanInProbe{}) }, "toys: invalid items -1"},
		{"producers before items", func() error { return SpawnFanIn(nil, 0, 0, &FanInProbe{}) }, "toys: invalid producers 0"},
		{"ticks", func() error { return SpawnTicker(nil, 0, &TickerProbe{}) }, "toys: invalid ticks 0"},
		{"negative ticks", func() error { return SpawnTicker(nil, -7, &TickerProbe{}) }, "toys: invalid ticks -7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil || err.Error() != tc.want {
				t.Fatalf("got error %v, want %q", err, tc.want)
			}
		})
	}
}

// TestDescribe checks the texts of DET §5.6.
func TestDescribe(t *testing.T) {
	type describer interface{ Describe() string }
	for _, tc := range []struct {
		msg  describer
		want string
	}{
		{StartPing{Rounds: 50}, "start-ping 50"},
		{Ping{N: 3}, "ping 3"},
		{Pong{N: 4}, "pong 4"},
		{Item{Producer: 2, N: 7}, "item p2 7"},
		{Tick{}, "tick"},
		{Query{N: 5}, "query 5"},
		{StartAsks{Count: 20}, "start-asks 20"},
		{StartBeacon{Beats: 40}, "start-beacon 40"},
		{Beat{N: 9}, "beat 9"},
		{answer{N: 6}, "answer 6"},
		{next{}, "next"},
		{result{N: 8}, "result 8"},
		{beat{}, "beat"},
		{idle{}, "idle"},
	} {
		if got := tc.msg.Describe(); got != tc.want {
			t.Errorf("%T describes as %q, want %q", tc.msg, got, tc.want)
		}
	}
}
