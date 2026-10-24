// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "golang.org/x/tools/go/analysis"

// All returns the analyzers of this release in check-ID order: Directives, Determinism,
// AwaitInActor (Phase 1), then UseAfterSend, Immutable, SharedState, MustUse, Exhaustive, Cleanup
// (Phase 2). Each call returns a new slice holding the same analyzer values.
func All() []*analysis.Analyzer {
	return []*analysis.Analyzer{Directives, Determinism, AwaitInActor}
}
