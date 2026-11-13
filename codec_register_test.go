// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/codec"
)

// The types the tests of this package ask across simulated nodes (plan 3.1-07). The internal tests
// of package lockstep cannot import codec, so they register nothing.
func init() { //nolint:gochecknoinits // registrations must exist before the first test runs
	codec.Register[refusedReq]("locksteptest.RefusedReq")
}

// TestCrossNodeTypesRegistered checks that each type this package sends, asks or replies with
// across nodes has its wire name, and that the registry passes the JSON support check.
func TestCrossNodeTypesRegistered(t *testing.T) {
	cases := []struct {
		v    any
		name string
	}{
		{refusedReq{}, "locksteptest.RefusedReq"},
	}
	for _, c := range cases {
		got, err := codec.Default().NameOf(c.v)
		if err != nil || got != c.name {
			t.Errorf("NameOf(%T) = %q, %v, want %q", c.v, got, err, c.name)
		}
	}
	if err := codec.Default().Check(codec.JSON{}); err != nil {
		t.Errorf("Check(JSON) = %v, want nil", err)
	}
}
