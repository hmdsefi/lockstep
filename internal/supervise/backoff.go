// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package supervise

import "time"

// Backoff returns the delay of SUP-280 for the count-th restart in the window (count >= 1):
// min(MaxBackoff, MinBackoff x 2^(count-1)). Doubling stops at MaxBackoff, so the result never
// overflows. A MinBackoff of zero gives zero. A count below 1 is treated as 1.
func Backoff(count int, l Limits) time.Duration {
	d := l.MinBackoff
	if d <= 0 {
		return 0
	}
	for i := 1; i < count; i++ {
		if d > l.MaxBackoff/2 {
			return l.MaxBackoff
		}
		d *= 2
	}
	return min(d, l.MaxBackoff)
}
