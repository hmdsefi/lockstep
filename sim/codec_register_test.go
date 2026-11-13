// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/sim"
)

// The types the tests of this package send or ask across nodes (plan 3.1-07). pairPing, which an
// internal test sends, is registered in codec_register_internal_test.go.
func init() { //nolint:gochecknoinits // registrations must exist before the first test runs
	codec.Register[Add]("simtest.Add")
	codec.Register[Count]("simtest.Count")
	codec.Register[Deposit]("simtest.Deposit")
	codec.Register[DlvDeposit]("simtest.DlvDeposit")
	codec.Register[Get]("simtest.Get")
	codec.Register[Inc]("simtest.Inc")
	codec.Register[Num]("simtest.Num")
	codec.Register[Ping]("simtest.Ping")
	codec.Register[Tick]("simtest.Tick")
	codec.Register[Wait]("simtest.Wait")
	codec.Register[*box]("simtest.BoxPtr")
	codec.Register[[]int]("simtest.IntSlice")
	codec.Register[map[string]int]("simtest.StringIntMap")
}

// TestCrossNodeTypesRegistered checks that each type this package sends, asks or replies with
// across nodes has its wire name, and that the registry passes the JSON support check.
func TestCrossNodeTypesRegistered(t *testing.T) {
	cases := []struct {
		v    any
		name string
	}{
		{Add{}, "simtest.Add"},
		{Count{}, "simtest.Count"},
		{Deposit{}, "simtest.Deposit"},
		{DlvDeposit{}, "simtest.DlvDeposit"},
		{Get{}, "simtest.Get"},
		{Inc{}, "simtest.Inc"},
		{Num{}, "simtest.Num"},
		{Ping{}, "simtest.Ping"},
		{Tick{}, "simtest.Tick"},
		{Wait{}, "simtest.Wait"},
		{&box{}, "simtest.BoxPtr"},
		{[]int{}, "simtest.IntSlice"},
		{map[string]int{}, "simtest.StringIntMap"},
		{sim.PairPingValue(), "simtest.PairPing"},
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
