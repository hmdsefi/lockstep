// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec_test

import (
	"fmt"

	"github.com/hmdsefi/lockstep/codec"
)

// Ping is a message that crosses nodes.
type Ping struct{ Seq int }

// Order has a field that the JSON codec cannot encode.
type Order struct {
	ID   string
	Note error
}

// Example registers a type under a wire name, encodes a value of it with the JSON codec and
// decodes the bytes back, as the sending and the receiving node do.
func Example() {
	reg := codec.NewRegistry()
	codec.RegisterIn[Ping](reg, "orders.Ping")

	name, data, err := reg.Encode(codec.JSON{}, Ping{Seq: 1})
	if err != nil {
		panic(err)
	}
	fmt.Println(name, string(data))

	// ... on the receiving node:
	v, err := reg.Decode(codec.JSON{}, name, data)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%T %+v\n", v, v)
	// Output:
	// orders.Ping {"Seq":1}
	// codec_test.Ping {Seq:1}
}

// ExampleRegistry_Check finds a registered type that the codec cannot encode before any message
// of that type is sent.
func ExampleRegistry_Check() {
	reg := codec.NewRegistry()
	codec.RegisterIn[Ping](reg, "orders.Ping")
	codec.RegisterIn[Order](reg, "orders.Order")

	fmt.Println(reg.Check(codec.JSON{}))
	// Output:
	// codec: unsupported type: json cannot encode codec_test.Order: field Note: interface type error
}
