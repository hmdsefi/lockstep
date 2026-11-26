// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package protobuf_test

import (
	"fmt"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/codec/protobuf"
)

// Example registers a generated message type, as its pointer type, and encodes and decodes a
// value of it with the protocol buffer codec. Any generated message type works the same way;
// wrapperspb.StringValue stands in for one here.
func Example() {
	reg := codec.NewRegistry()
	codec.RegisterIn[*wrapperspb.StringValue](reg, "orders.Note")
	if err := reg.Check(protobuf.Codec{}); err != nil {
		panic(err)
	}

	name, data, err := reg.Encode(protobuf.Codec{}, wrapperspb.String("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s % x\n", name, data)

	// ... on the receiving node:
	v, err := reg.Decode(protobuf.Codec{}, name, data)
	if err != nil {
		panic(err)
	}
	note, ok := v.(*wrapperspb.StringValue)
	fmt.Printf("%T %t %s\n", v, ok, note.GetValue())
	// Output:
	// orders.Note 0a 05 68 65 6c 6c 6f
	// *wrapperspb.StringValue true hello
}
