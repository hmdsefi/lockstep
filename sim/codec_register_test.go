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
	codec.Register[Builtins]("simtest.Builtins")
	codec.Register[Carry]("simtest.Carry")
	codec.Register[Count]("simtest.Count")
	codec.Register[Deposit]("simtest.Deposit")
	codec.Register[DlvDeposit]("simtest.DlvDeposit")
	codec.Register[Forward]("simtest.Forward")
	codec.Register[Get]("simtest.Get")
	codec.Register[GetBalance]("simtest.GetBalance")
	codec.Register[GetRate]("simtest.GetRate")
	codec.Register[GetSeq]("simtest.GetSeq")
	codec.Register[Inc]("simtest.Inc")
	codec.Register[Num]("simtest.Num")
	codec.Register[Ping]("simtest.Ping")
	codec.Register[Seq]("simtest.Seq")
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
		{Builtins{}, "simtest.Builtins"},
		{Carry{}, "simtest.Carry"},
		{Count{}, "simtest.Count"},
		{Deposit{}, "simtest.Deposit"},
		{DlvDeposit{}, "simtest.DlvDeposit"},
		{Forward{}, "simtest.Forward"},
		{Get{}, "simtest.Get"},
		{GetBalance{}, "simtest.GetBalance"},
		{GetRate{}, "simtest.GetRate"},
		{GetSeq{}, "simtest.GetSeq"},
		{Inc{}, "simtest.Inc"},
		{Num{}, "simtest.Num"},
		{Ping{}, "simtest.Ping"},
		{Seq{}, "simtest.Seq"},
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
