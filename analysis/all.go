// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "golang.org/x/tools/go/analysis"

// All returns the analyzers of this release in check-ID order: Directives, Determinism,
// AwaitInActor. Each call returns a new slice holding the same analyzer values.
func All() []*analysis.Analyzer {
	return []*analysis.Analyzer{Directives, Determinism, AwaitInActor}
}
