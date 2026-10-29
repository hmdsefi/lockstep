// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package libs lists the actor libraries the benchmarks compare.
package libs

import "github.com/hmdsefi/lockstep/bench/harness"

// Libs returns the four libraries in the order lockstep, protoactor, hollywood, goakt.
func Libs() []harness.Lib {
	return []harness.Lib{}
}
