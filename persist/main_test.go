// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/dettest"
)

// TestMain runs the package's tests under goleak (DET-150).
func TestMain(m *testing.M) { dettest.Main(m) }
