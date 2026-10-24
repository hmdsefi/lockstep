// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package analysis holds lockstep's static checks for actor code as go/analysis analyzers.
//
// Every diagnostic message starts with its check ID, for example "LS100: ". The checks are:
//
//	LS001, LS002  Directives    malformed and unknown //lockstep: directives      Phase 1
//	LS100         Determinism   nondeterminism and blocking in actor code          Phase 1
//	LS110         AwaitInActor  Reply.Await in actor code                          Phase 1
//	LS200         UseAfterSend  use of a message's references after it is sent     Phase 2
//	LS201         Immutable     verification of //lockstep:immutable types         Phase 2
//	LS210         SharedState   package variables written by actors; actor state in messages  Phase 2
//	LS220         MustUse       ignored replies and ignored Send and Spawn errors  Phase 2
//	LS230         Exhaustive    message types without a case in Receive            Phase 2
//	LS240         Cleanup       actor resources that are never released            Phase 2
//
// A finding is suppressed with //lockstep:allow LSnnn <reason> on the reported line, on the line
// above it, or in the doc comment of an enclosing declaration.
package analysis
