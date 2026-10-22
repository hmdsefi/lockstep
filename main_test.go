// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/dettest"
)

// TestMain runs the package's tests under goleak with the run-twice switch prepared (DET-150).
// Both test packages of the directory share this one TestMain; every test stops the engines it
// starts, so goleak finds no goroutine left over.
func TestMain(m *testing.M) { dettest.Main(m) }
