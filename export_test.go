// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

// UserGuardianUID returns the uid of sys's user guardian, for the external tests.
func UserGuardianUID(sys *System) uint64 { return sys.user.UID() }

// SystemGuardianUID returns the uid of sys's system guardian, for the external tests.
func SystemGuardianUID(sys *System) uint64 { return sys.system.UID() }
