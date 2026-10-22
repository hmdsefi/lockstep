// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import "testing"

// TestMain runs the package's tests under goleak with the run-twice switch prepared (DET-140,
// DET-150). The package runs simulations in TestGolden.
func TestMain(m *testing.M) { Main(m) }
