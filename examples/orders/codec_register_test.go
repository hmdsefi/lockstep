// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orders_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/examples/orders"
)

// The types the simulation tests send, ask or reply with across nodes (plan 3.1-07).
func init() { //nolint:gochecknoinits // registrations must exist before the first test runs
	codec.Register[orders.Create]("orderstest.Create")
	codec.Register[orders.Cancel]("orderstest.Cancel")
	codec.Register[orders.Submit]("orderstest.Submit")
	codec.Register[orders.Result]("orderstest.Result")
}

// TestCrossNodeTypesRegistered checks that each type this package sends, asks or replies with
// across nodes has its wire name, and that the registry passes the JSON support check.
func TestCrossNodeTypesRegistered(t *testing.T) {
	cases := []struct {
		v    any
		name string
	}{
		{orders.Create{}, "orderstest.Create"},
		{orders.Cancel{}, "orderstest.Cancel"},
		{orders.Submit{}, "orderstest.Submit"},
		{orders.Result{}, "orderstest.Result"},
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
