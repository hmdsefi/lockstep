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
//   - A MarshalJSON or MarshalText method without the matching UnmarshalJSON or UnmarshalText
//     method on the pointer, an unmarshal method without the marshal method, and a marshal method
//     on the pointer only. A type with a JSON pair, or else a text pair, is accepted as it is: its
//     fields are not checked. A pointer type *T whose methods are all on the pointer, such as
//     *big.Int, is accepted when it has both methods of a pair, since encoding/json always uses
//     them there; T itself, as a value, is not.
//   - A struct that gets its pair from an embedded field, when it has other fields, which the pair
//     would hide, or when the embedded field is a pointer, whose methods panic when it is nil.
//   - Interface, channel, function, complex and unsafe.Pointer types, anywhere in the type.
//   - A map whose keys encoding/json would not write and read alike: a string key type is accepted
//     when its pointer has no UnmarshalText method, an integer key type when it has both
//     MarshalText and UnmarshalText or neither, and any other key type only with both.
//   - A struct with an unexported field, an unexported embedded field of a type that is not a
//     struct or that has a json tag, or an embedded field that is a pointer to an unexported
//     struct type. A field tagged `json:"-"` is never encoded and is not checked.
//   - A struct with an untagged embedded struct that has a marshal method: encoding/json ignores
//     the method and encodes the embedded struct's fields in its place.
//   - A struct in which two fields, promoted ones included, get the same JSON name along any of
//     the paths that embed them, so that encoding/json drops one of them, and a struct that embeds
//     itself through a pointer.
//
// The error names the field path to the rejected part, outermost field first, for example
//
//	codec: unsupported type: json cannot encode main.Order: field Items: field Note: interface type error
//
// for an Order whose Items field is a slice of a struct with a Note field of type error.
//
// Some loss depends on the value, and no check of the type finds it: invalid UTF-8 in a string is
// replaced with U+FFFD, a non-nil embedded pointer to a struct that encodes no fields decodes as
// nil, and a time.Time loses its monotonic clock reading and its location name.
type JSON struct{}

var _ Codec = JSON{}

// Name returns "json".
func (JSON) Name() string { return "json" }

// Supports returns nil if JSON can encode and decode values of type t, or an error wrapping
// ErrUnsupported that says which part of t it cannot encode. See JSON for the rules and for the
// loss that depends on the value.
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
