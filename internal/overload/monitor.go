// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload

// Monitor samples the goroutine count and warns above a bound (production). The Sampler holds one
// and calls it on every 10th tick (OVL-600).
type Monitor struct{}
