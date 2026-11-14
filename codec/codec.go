// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package codec turns the messages that cross nodes into bytes and back.
//
// A message sent to an actor on another node travels as a wire name and the bytes of its value.
// The wire name tells the receiving node which Go type to decode the bytes into, so every type
// that crosses nodes is registered under one name, the same on every node:
//
//	type Ping struct{ Seq int }
//
//	func init() { codec.Register[Ping]("orders.Ping") }
//
// # The registry
//
// A Registry maps wire names to Go types and back. Default returns the registry of the process,
// which Register adds to; NewRegistry returns a private one, for tests or for a system that must
// not share its types, and RegisterIn adds to it. NameOf and TypeOf look a mapping up in either
// direction, and Names lists the registered names.
//
// The mapping is one to one and never changes once made: a name holds one type, and a type holds
// one name. Registering the same name and type again does nothing; any other misuse panics, since
// it is a programming error that would otherwise only show when the first message is sent. A
// Registry is safe for concurrent use, and a registration is visible to every call that starts
// after it returns. Register types before the first message that uses them is sent, typically
// from an init function or a package-level variable.
//
// Type identity is exact. A struct T, its pointer *T and a named type declared over T are three
// types, and each needs its own name. A value of a type that is not registered has no name, and
// NameOf returns an error wrapping ErrNotRegistered.
//
// # Wire names
//
// A wire name is 1 to 255 bytes long. Its first byte is an ASCII letter; the rest are ASCII
// letters, digits, '.', '_', '/' and '-'. Names are case-sensitive and compared byte for byte.
// A name such as "orders.Ping" or "orders/v2.Ping" keeps the names of different packages apart.
// Names starting with "lockstep." are reserved for lockstep's own types.
//
// # Built-in types
//
// Every registry, the process registry and each new one alike, starts with 19 built-in types, so
// that a plain value, such as the int64 answer to an ask, crosses nodes without registration:
//
//	bool           lockstep.bool       uint           lockstep.uint
//	string         lockstep.string     uint8 (byte)   lockstep.uint8
//	int            lockstep.int        uint16         lockstep.uint16
//	int8           lockstep.int8       uint32         lockstep.uint32
//	int16          lockstep.int16      uint64         lockstep.uint64
//	int32 (rune)   lockstep.int32      float32        lockstep.float32
//	int64          lockstep.int64      float64        lockstep.float64
//	[]byte         lockstep.bytes      struct{}       lockstep.empty
//	time.Duration  lockstep.duration   time.Time      lockstep.time
//	lockstep.Address  lockstep.address
//
// A time.Time keeps only its instant across nodes: the monotonic clock reading and the location
// name are lost, so compare a received time with Time.Equal. lockstep.PID and lockstep.ReplyTo
// are generic, so no single built-in covers them; they cross nodes as fields of registered
// messages.
//
// # Codecs
//
// A Codec is one way of turning values into bytes, such as JSON or protocol buffers. Every node
// of a cluster uses the same codec. Not every codec can encode every type; a codec's Supports
// method reports whether it can, so that a type it cannot encode is found when the system starts
// rather than when the first message of that type is sent.
//
// Registry.Encode turns a registered value into its wire name and the codec's bytes, and
// Registry.Decode turns them back into a value of the registered type. Registry.Check asks a codec
// about every registered type at once, for use when a system starts:
//
//	name, data, err := codec.Default().Encode(codec.JSON{}, Ping{Seq: 1})
//	// ... on the receiving node:
//	v, err := codec.Default().Decode(codec.JSON{}, name, data) // v is a Ping
package codec

import (
	"errors"
	"reflect"
)

// Codec turns registered values into bytes and back.
type Codec interface {
	// Name returns the codec's name, such as "json" or "protobuf". Two nodes can exchange
	// messages only when their codecs have the same name, and two codecs with the same name must
	// encode and decode alike: a Registry may treat them as one codec.
	Name() string

	// Supports returns nil if the codec can encode and decode values of type t, or an error
	// wrapping ErrUnsupported that says why not. The answer for a type never changes.
	Supports(t reflect.Type) error

	// Marshal returns the bytes of v. Equal values give equal bytes.
	Marshal(v any) ([]byte, error)

	// Unmarshal decodes data into the value v points to; v is a non-nil pointer. The decoded
	// value must not refer to data, and Unmarshal must not keep data after it returns: the caller
	// may reuse the buffer, as the remoting drivers do with the frames they read.
	Unmarshal(data []byte, v any) error
}

// ErrNotRegistered is wrapped by the error returned for a type or wire name that the registry
// does not hold.
var ErrNotRegistered = errors.New("codec: type not registered")

// ErrUnsupported is wrapped by the error a Codec returns for a type it cannot encode.
var ErrUnsupported = errors.New("codec: unsupported type")
