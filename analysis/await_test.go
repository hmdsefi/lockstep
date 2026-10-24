// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "testing"

// TestAwaitInActor checks the LS110 corpus (AT-ANL-12): every use of Reply.Await in actor code
// of ls110/bad with its message and chain, and none in ls110/good.
func TestAwaitInActor(t *testing.T) {
	runCorpus(t, AwaitInActor, "ls110/bad", "ls110/good")
}
