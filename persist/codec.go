// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist

import (
	"encoding/json"
	"reflect"
)

// Codec encodes events or states. Encode must be deterministic and return a slice that it does
// not reuse or modify afterwards: persist keeps it until the Write or Save that carries it
// returns.
type Codec[T any] interface {
	Encode(v T) ([]byte, error)
	Decode(data []byte) (T, error)
}

// JSON returns a Codec that uses encoding/json. Encode is json.Marshal and Decode is
// json.Unmarshal into a new T; both return the errors of encoding/json unwrapped. When Decode
// returns an error, the value it returns is meaningless.
//
// JSON panics if T is an interface type, because the encoded bytes do not say which concrete
// type to decode into. Use a codec that records the concrete type for such events.
func JSON[T any]() Codec[T] {
	if t := reflect.TypeFor[T](); t.Kind() == reflect.Interface {
		panic("persist: JSON: " + t.String() + " is an interface type; " +
			"use a codec that records the concrete type")
	}
	return jsonCodec[T]{}
}

type jsonCodec[T any] struct{}

func (jsonCodec[T]) Encode(v T) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec[T]) Decode(data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}
