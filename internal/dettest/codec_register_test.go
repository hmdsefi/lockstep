// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"testing"

	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/toys"
)

// The types of internal/toys that the scenarios send or ask across nodes (plan 3.1-07). internal/toys
// may import only lockstep (DET-080), so the test package that runs them registers them.
func init() { //nolint:gochecknoinits // registrations must exist before the first test runs
	codec.Register[toys.Beat]("dettesttest.Beat")
	codec.Register[toys.Ping]("dettesttest.Ping")
	codec.Register[toys.Pong]("dettesttest.Pong")
	codec.Register[toys.Query]("dettesttest.Query")
	codec.Register[toys.Request]("dettesttest.Request")
}

// TestCrossNodeTypesRegistered checks that each type this package sends, asks or replies with
// across nodes has its wire name, and that the registry passes the JSON support check.
func TestCrossNodeTypesRegistered(t *testing.T) {
	cases := []struct {
		v    any
		name string
	}{
		{toys.Beat{}, "dettesttest.Beat"},
		{toys.Ping{}, "dettesttest.Ping"},
		{toys.Pong{}, "dettesttest.Pong"},
		{toys.Query{}, "dettesttest.Query"},
		{toys.Request{}, "dettesttest.Request"},
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
