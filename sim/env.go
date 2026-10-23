// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"
	"os"

	"github.com/hmdsefi/faultline"
)

// applyEnv reads LOCKSTEP_CHECK_DETERMINISM and returns o with CheckDeterminism set when it is
// "1" (SIM-030). "" and "0" leave o unchanged; any other value is an error. This file is the only
// one in package sim that reads the environment (DET-055, DET-145).
func applyEnv(o faultline.Options) (faultline.Options, error) {
	switch v := os.Getenv("LOCKSTEP_CHECK_DETERMINISM"); v {
	case "", "0":
	case "1":
		o.CheckDeterminism = true
	default:
		return o, fmt.Errorf("lockstep: sim: invalid LOCKSTEP_CHECK_DETERMINISM=%q: want 0 or 1", v)
	}
	return o, nil
}
