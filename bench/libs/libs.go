// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package libs lists the actor libraries the benchmarks compare.
package libs

import (
	"github.com/hmdsefi/lockstep/bench/harness"
	// protoreg lets hollywood and protoactor-go share one binary; it must be initialized before
	// either registers its protobuf file.
	_ "github.com/hmdsefi/lockstep/bench/internal/protoreg"
	"github.com/hmdsefi/lockstep/bench/libs/lockstep"
	"github.com/hmdsefi/lockstep/bench/libs/protoactor"
)

// Libs returns the four libraries in the order lockstep, protoactor, hollywood, goakt.
func Libs() []harness.Lib {
	return []harness.Lib{lockstep.Lib{}, protoactor.Lib{}}
}
