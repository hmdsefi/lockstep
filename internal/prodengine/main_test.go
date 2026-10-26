// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/dettest"
)

// TestMain runs the package's tests under goleak with the run-twice switch prepared (BEN-200).
func TestMain(m *testing.M) { dettest.Main(m) }
