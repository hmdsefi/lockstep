// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload

import (
	"testing"
	"time"
)

// OVL-500: the watchdog polls every max(d/5, 10ms).
func TestWatchdogPollPeriod(t *testing.T) {
	for _, c := range []struct{ d, want time.Duration }{
		{time.Millisecond, 10 * time.Millisecond},
		{50 * time.Millisecond, 10 * time.Millisecond},
		{51 * time.Millisecond, 10200 * time.Microsecond},
		{100 * time.Millisecond, 20 * time.Millisecond},
		{5 * time.Second, time.Second},
	} {
		if got := pollPeriod(c.d); got != c.want {
			t.Errorf("pollPeriod(%v) = %v, want %v", c.d, got, c.want)
		}
	}
}
