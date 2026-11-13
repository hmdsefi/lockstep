// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import "github.com/hmdsefi/lockstep/codec"

// pairPing is sent across nodes by the pair tests; the external tests register the other types
// (plan 3.1-07).
func init() { codec.Register[pairPing]("simtest.PairPing") } //nolint:gochecknoinits // registrations must exist before the first test runs

// PairPingValue returns a pairPing, for the external test of the registrations.
func PairPingValue() any { return pairPing{} }
