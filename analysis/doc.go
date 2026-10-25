// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package analysis holds lockstep's static checks for actor code as go/analysis analyzers.
//
// Every diagnostic message starts with its check ID, for example "LS100: ". The checks are:
//
//	LS001, LS002  Directives    malformed and unknown //lockstep: directives
//	LS100         Determinism   nondeterminism and blocking in actor code
//	LS110         AwaitInActor  Reply.Await in actor code
//
// A finding is suppressed with //lockstep:allow LSnnn <reason> on the reported line, on the line
// above it, or in the doc comment of an enclosing declaration.
package analysis
