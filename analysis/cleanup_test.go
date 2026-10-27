// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "testing"

// TestCleanup checks the LS240 corpus (AT-ANL-23, AT-ANL-24): every resource field that is
// neither owned nor released and every resource type without a release method in ls240/bad, none
// in ls240/good, and the resource markers of imported types in ls240/remote.
func TestCleanup(t *testing.T) {
	runCorpus(t, Cleanup, "ls240/bad", "ls240/good", "ls240/remote")
}
