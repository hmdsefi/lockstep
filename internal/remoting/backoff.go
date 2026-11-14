// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"math/rand/v2"
	"time"
)

// redialBase returns REM-100's b = min(hi, lo << (f-1)) for f consecutive failures, saturating
// at hi instead of overflowing. lo <= hi and f >= 1; a smaller f counts as 1.
func redialBase(f int, lo, hi time.Duration) time.Duration {
	shift := max(f-1, 0)
	if shift >= 63 || lo > hi>>shift {
		return hi
	}
	return lo << shift
}

// redialDelay returns REM-100's delay b/2 + r.Int64N(b - b/2 + 1) in nanoseconds, with b from
// redialBase: a value in [b/2, b], drawn with exactly one Int64N call. Integer arithmetic only.
func redialDelay(f int, lo, hi time.Duration, r *rand.Rand) time.Duration {
	b := int64(redialBase(f, lo, hi))
	h := b / 2
	return time.Duration(h + r.Int64N(b-h+1))
}
