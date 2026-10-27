// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "testing"

// TestImmutable checks the LS201 corpus: the fields of immutable types that are not immutable
// (AT-ANL-15) and the writes to their fields (AT-ANL-16) in ls201/bad, none in ls201/good, and the
// writes of ls201/importer to a type it sees as marked only through the marker's fact.
func TestImmutable(t *testing.T) {
	runCorpus(t, Immutable, "ls201/bad", "ls201/good")
	runCorpus(t, Immutable, "ls201/importer")
}
