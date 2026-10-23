// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package supervise

import (
	"math"
	"testing"
	"time"
)

// TestATSUP28Backoff checks the unit part of AT-SUP-28.
func TestATSUP28Backoff(t *testing.T) {
	defaults := Limits{MaxRestarts: 10, Window: time.Minute, MinBackoff: 10 * time.Millisecond, MaxBackoff: 10 * time.Second}
	for _, c := range []struct {
		name  string
		l     Limits
		count int
		want  time.Duration
	}{
		{"defaults 1", defaults, 1, 10 * time.Millisecond},
		{"defaults 2", defaults, 2, 20 * time.Millisecond},
		{"defaults 3", defaults, 3, 40 * time.Millisecond},
		{"defaults 10", defaults, 10, 5120 * time.Millisecond},
		{"defaults 11 capped", defaults, 11, 10 * time.Second},
		{"3s/10s 1", Limits{MinBackoff: 3 * time.Second, MaxBackoff: 10 * time.Second}, 1, 3 * time.Second},
		{"3s/10s 2", Limits{MinBackoff: 3 * time.Second, MaxBackoff: 10 * time.Second}, 2, 6 * time.Second},
		{"3s/10s 3", Limits{MinBackoff: 3 * time.Second, MaxBackoff: 10 * time.Second}, 3, 10 * time.Second},
		{"3s/10s 4", Limits{MinBackoff: 3 * time.Second, MaxBackoff: 10 * time.Second}, 4, 10 * time.Second},
		{"huge min", Limits{MinBackoff: 1 << 62, MaxBackoff: 10 * time.Second}, 5, 10 * time.Second},
		{"huge min and max", Limits{MinBackoff: 1 << 62, MaxBackoff: math.MaxInt64}, 5, math.MaxInt64},
		{"huge max", Limits{MinBackoff: time.Second, MaxBackoff: math.MaxInt64}, 100, math.MaxInt64},
		{"huge count", defaults, math.MaxInt, 10 * time.Second},
		{"zero min", Limits{MinBackoff: 0, MaxBackoff: time.Second}, 7, 0},
		{"min equals max", Limits{MinBackoff: time.Second, MaxBackoff: time.Second}, 3, time.Second},
		{"max exact double", Limits{MinBackoff: time.Second, MaxBackoff: 4 * time.Second}, 3, 4 * time.Second},
		{"odd max", Limits{MinBackoff: 2, MaxBackoff: 5}, 2, 4},
		{"odd max capped", Limits{MinBackoff: 2, MaxBackoff: 5}, 3, 5},
	} {
		if got := Backoff(c.count, c.l); got != c.want {
			t.Errorf("%s: Backoff(%d) = %v, want %v", c.name, c.count, got, c.want)
		}
	}
}
