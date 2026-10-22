// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"go.uber.org/goleak"
)

// EnvCheckDeterminism is the environment variable read by Enabled and by sim/env.go (DET-145).
const EnvCheckDeterminism = "LOCKSTEP_CHECK_DETERMINISM"

// EnvWorkload is the environment variable naming a workload file for sim (MIN §5.4), read by
// sim/env.go only (DET-145, MIN-072).
const EnvWorkload = "LOCKSTEP_WORKLOAD"

// envFaultline is faultline's run-twice switch (faultline v0.1.2).
const envFaultline = "FAULTLINE_CHECK_DETERMINISM"

// Enabled reports whether LOCKSTEP_CHECK_DETERMINISM asks for determinism checks (DET-130). The
// empty string and "0" mean no, "1" means yes; any other value is an error.
func Enabled() (bool, error) {
	switch v := os.Getenv(EnvCheckDeterminism); v {
	case "", "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("dettest: invalid %s=%q: want 0 or 1", EnvCheckDeterminism, v)
	}
}

var prepareOnce sync.Once

// Prepare checks LOCKSTEP_CHECK_DETERMINISM and turns it on for faultline.Run calls of the
// process (DET-140). A TestMain that calls goleak.VerifyTestMain itself calls Prepare first.
//
// Prepare acts on its first call only. With an invalid value it prints the error of Enabled and
// exits with status 2. With the switch on and FAULTLINE_CHECK_DETERMINISM empty or "0", it sets
// that variable to "1"; any other value is left for faultline to validate. Prepare reads no other
// variable.
func Prepare() {
	prepareOnce.Do(func() {
		on, err := Enabled()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if !on {
			return
		}
		if v := os.Getenv(envFaultline); v == "" || v == "0" {
			if err := os.Setenv(envFaultline, "1"); err != nil {
				fmt.Fprintln(os.Stderr, "dettest:", err)
				os.Exit(2)
			}
		}
	})
}

// Main is exactly Prepare followed by goleak.VerifyTestMain(m) (DET-140, R50). It does not
// return.
func Main(m *testing.M) {
	Prepare()
	goleak.VerifyTestMain(m)
}
