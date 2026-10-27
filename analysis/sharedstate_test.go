// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "testing"

// TestSharedState checks the LS210 corpus (AT-ANL-17, AT-ANL-18): the writes to package variables
// in actor code of ls210/bad with their message and chain, the actor state placed in messages
// there, and none of these in ls210/good.
func TestSharedState(t *testing.T) {
	runCorpus(t, SharedState, "ls210/bad", "ls210/good")
}
