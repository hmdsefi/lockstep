// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec

import (
	"bytes"
	"encoding/json"
	"reflect"
)

// JSON is the JSON codec, built on encoding/json. Its zero value is ready to use.
//
// Marshal is deterministic: equal values give identical bytes, since encoding/json writes struct
// fields in declaration order and map keys sorted. It does not escape '<', '>' and '&', and the
// bytes do not end in a newline. Unmarshal is json.Unmarshal, so fields the target type does not
// have are ignored.
//
// lockstep.Address, lockstep.PID and lockstep.ReplyTo values encode as JSON strings of their text
// forms, both as values and as map keys.
//
// Supports rejects a type that encoding/json would encode lossily or not at all, so that it is found
// before its first message is sent rather than decoded wrong on the other node:
//
//   - A type with a MarshalJSON method whose pointer has no UnmarshalJSON method, or a type with a
//     MarshalText method whose pointer has no UnmarshalText method. A type with both methods of a
//     pair is accepted as it is: its fields are not checked. For a pointer type, the unmarshal
//     method is looked for on the pointer type itself.
//   - Interface, channel, function, complex and unsafe.Pointer types, anywhere in the type.
//   - A map whose key type is not a string or integer type and has no MarshalText and
//     UnmarshalText pair.
//   - A struct with an unexported field, or an embedded field that is a pointer to an unexported
//     struct type. A field tagged `json:"-"` is never encoded and is not checked.
//
// The error names the field path to the rejected part, outermost field first, for example
//
//	codec: unsupported type: json cannot encode main.Order: field Items: field Note: interface type error
//
// for an Order whose Items field is a slice of a struct with a Note field of type error.
type JSON struct{}

var _ Codec = JSON{}

// Name returns "json".
func (JSON) Name() string { return "json" }

// Supports returns nil if JSON encodes values of type t without loss, or an error wrapping
// ErrUnsupported that says which part of t it cannot encode.
func (JSON) Supports(t reflect.Type) error { return checkJSON(t) }

// Marshal returns the JSON encoding of v, without HTML escaping and without a trailing newline.
// An error from encoding/json, such as the one for a NaN float, is returned unchanged.
func (JSON) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'}), nil
}

// Unmarshal decodes data into the value v points to, as json.Unmarshal does.
func (JSON) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
