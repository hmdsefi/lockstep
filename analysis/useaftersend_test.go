// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "testing"

// TestUseAfterSend checks the LS200 corpus (AT-ANL-13, AT-ANL-14): one finding at the first use of
// every case of ls200/bad, and none in ls200/good.
func TestUseAfterSend(t *testing.T) {
	runCorpus(t, UseAfterSend, "ls200/bad", "ls200/good")
}
