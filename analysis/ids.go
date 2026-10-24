// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import "slices"

// The IDs of the directive checks. They report on //lockstep: comments themselves, so a
// //lockstep:allow cannot suppress them (ANL-080).
const (
	idDirectiveSyntax = "LS001"
	idDirectiveName   = "LS002"
)

// suppressibleIDs are the check IDs of this release that a //lockstep:allow may name, ascending
// (ANL-100).
var suppressibleIDs = []string{"LS100", "LS110"}

// isSuppressibleID reports whether id is one of suppressibleIDs.
func isSuppressibleID(id string) bool {
	return slices.Contains(suppressibleIDs, id)
}
