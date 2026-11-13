// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package protobuf is a codec.Codec that encodes values as protocol buffers.
//
// It lives in its own module, github.com/hmdsefi/lockstep/codec/protobuf, so that the lockstep
// module itself never depends on google.golang.org/protobuf. Use it in place of codec.JSON when the
// messages that cross nodes are already protocol buffer messages:
//
//	func init() { codec.Register[*orderspb.Ping]("orders.Ping") }
//
//	name, data, err := codec.Default().Encode(protobuf.Codec{}, &orderspb.Ping{Seq: 1})
//	// ... on the receiving node:
//	v, err := codec.Default().Decode(protobuf.Codec{}, name, data) // v is a *orderspb.Ping
//
// # Supported types
//
// Codec supports pointer types that implement proto.Message, such as the *M of a generated
// message M, and the 19 built-in types of package codec. A message type is registered as its
// pointer type, *M, and decodes as a new *M.
//
// The built-in types travel as the well-known wrapper messages:
//
//	bool                          google.protobuf.BoolValue
//	string                        google.protobuf.StringValue
//	int8, int16, int32            google.protobuf.Int32Value
//	int, int64                    google.protobuf.Int64Value
//	uint8, uint16, uint32         google.protobuf.UInt32Value
//	uint, uint64                  google.protobuf.UInt64Value
//	float32                       google.protobuf.FloatValue
//	float64                       google.protobuf.DoubleValue
//	[]byte                        google.protobuf.BytesValue
//	struct{}                      google.protobuf.Empty
//	time.Duration                 google.protobuf.Duration
//	time.Time                     google.protobuf.Timestamp
//	lockstep.Address              google.protobuf.StringValue of its text form
//
// so another protocol buffer implementation can read and write them. Decoding into a type
// narrower than its wrapper checks the range: an Int32Value of 300 decoded as an int8 fails with
// "protobuf: value 300 out of range for int8". A time.Time keeps only its instant and decodes in
// UTC. Any other type, such as a struct that is not a message or a named type over a built-in
// type, is not supported.
package protobuf

import (
	"fmt"
	"reflect"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
)

// Codec is the protocol buffer codec. Its zero value is ready to use.
//
// Marshal is deterministic: equal values give identical bytes, since it marshals with
// proto.MarshalOptions{Deterministic: true}, which writes map entries in key order. Unmarshal
// takes a **M for a message type *M and stores a newly allocated message there, and takes a
// pointer to the value for a built-in type.
type Codec struct{}

var _ codec.Codec = Codec{}

// Name returns "protobuf".
func (Codec) Name() string { return "protobuf" }

// Supports returns nil for a pointer type that implements proto.Message and for the built-in types
// of package codec, and otherwise an error wrapping codec.ErrUnsupported that reads
//
//	codec: unsupported type: protobuf cannot encode <type>: not a proto.Message or built-in type
func (Codec) Supports(t reflect.Type) error {
	if isMessage(t) || slices.Contains(builtins, t) {
		return nil
	}
	return unsupported(t)
}

// Marshal returns the protocol buffer encoding of v, a proto.Message or a value of a built-in
// type. For any other value it returns the error of Supports.
func (Codec) Marshal(v any) ([]byte, error) {
	m, err := toMessage(v)
	if err != nil {
		return nil, err
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}

// Unmarshal decodes data into the value v points to. For a message type *M, v is a **M, and
// Unmarshal allocates a new M; for a built-in type, v points to a value of that type. A value out
// of the range of the target type returns an error reading
// "protobuf: value <n> out of range for <type>", and a Timestamp or Duration that is not valid
// returns the error of its CheckValid method. A target of any other type returns the error of
// Supports for that type.
func (Codec) Unmarshal(data []byte, v any) error {
	rv := reflect.ValueOf(v)
	switch {
	case !rv.IsValid():
		return fmt.Errorf("protobuf: Unmarshal(nil)")
	case rv.Kind() != reflect.Pointer:
		return fmt.Errorf("protobuf: Unmarshal(non-pointer %s)", rv.Type())
	case rv.IsNil():
		return fmt.Errorf("protobuf: Unmarshal(nil %s)", rv.Type())
	}
	if t := rv.Type().Elem(); isMessage(t) {
		p := reflect.New(t.Elem()) // a new M, as a *M
		if m, ok := p.Interface().(proto.Message); ok {
			if err := proto.Unmarshal(data, m); err != nil {
				return err
			}
			rv.Elem().Set(p)
			return nil
		}
	}
	return unmarshalBuiltin(data, v)
}

// builtins is this module's copy of the built-in types of package codec (COD-040), in the spec's
// table order. TestATCOD17Protobuf keeps it equal to the types codec.NewRegistry holds.
var builtins = []reflect.Type{
	reflect.TypeFor[bool](),
	reflect.TypeFor[string](),
	reflect.TypeFor[int](),
	reflect.TypeFor[int8](),
	reflect.TypeFor[int16](),
	reflect.TypeFor[int32](),
	reflect.TypeFor[int64](),
	reflect.TypeFor[uint](),
	reflect.TypeFor[uint8](),
	reflect.TypeFor[uint16](),
	reflect.TypeFor[uint32](),
	reflect.TypeFor[uint64](),
	reflect.TypeFor[float32](),
	reflect.TypeFor[float64](),
	reflect.TypeFor[[]byte](),
	reflect.TypeFor[struct{}](),
	reflect.TypeFor[time.Duration](),
	reflect.TypeFor[time.Time](),
	reflect.TypeFor[lockstep.Address](),
}

var messageType = reflect.TypeFor[proto.Message]()

// isMessage reports whether t is a pointer type implementing proto.Message.
func isMessage(t reflect.Type) bool {
	return t != nil && t.Kind() == reflect.Pointer && t.Implements(messageType)
}

// unsupported returns the error of Supports for t (COD-170).
func unsupported(t reflect.Type) error {
	return fmt.Errorf("%w: protobuf cannot encode %v: not a proto.Message or built-in type", codec.ErrUnsupported, t)
}

// toMessage returns the message that carries v: v itself for a message, or the wrapper of COD-170's
// table for a built-in. The type switch matches exact types, so a named type over a built-in type
// falls through to the Supports error.
func toMessage(v any) (proto.Message, error) {
	switch x := v.(type) {
	case bool:
		return wrapperspb.Bool(x), nil
	case string:
		return wrapperspb.String(x), nil
	case int:
		return wrapperspb.Int64(int64(x)), nil
	case int8:
		return wrapperspb.Int32(int32(x)), nil
	case int16:
		return wrapperspb.Int32(int32(x)), nil
	case int32:
		return wrapperspb.Int32(x), nil
	case int64:
		return wrapperspb.Int64(x), nil
	case uint:
		return wrapperspb.UInt64(uint64(x)), nil
	case uint8:
		return wrapperspb.UInt32(uint32(x)), nil
	case uint16:
		return wrapperspb.UInt32(uint32(x)), nil
	case uint32:
		return wrapperspb.UInt32(x), nil
	case uint64:
		return wrapperspb.UInt64(x), nil
	case float32:
		return wrapperspb.Float(x), nil
	case float64:
		return wrapperspb.Double(x), nil
	case []byte:
		return wrapperspb.Bytes(x), nil
	case struct{}:
		return &emptypb.Empty{}, nil
	case time.Duration:
		return durationpb.New(x), nil
	case time.Time:
		ts := timestamppb.New(x)
		if err := ts.CheckValid(); err != nil {
			return nil, err // the receiver could not decode it
		}
		return ts, nil
	case lockstep.Address:
		text, err := x.MarshalText()
		if err != nil {
			return nil, err
		}
		return wrapperspb.String(string(text)), nil
	}
	if m, ok := v.(proto.Message); ok && isMessage(reflect.TypeOf(v)) {
		return m, nil
	}
	return nil, unsupported(reflect.TypeOf(v))
}

// unmarshalBuiltin decodes data into *v for the built-in types, through the wrapper of COD-170's
// table, range-checking narrowing conversions.
func unmarshalBuiltin(data []byte, v any) error {
	switch p := v.(type) {
	case *bool:
		var w wrapperspb.BoolValue
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *string:
		var w wrapperspb.StringValue
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *int:
		var w wrapperspb.Int64Value
		return decode(data, &w, func() error { return setInt(p, w.GetValue(), "int") })
	case *int8:
		var w wrapperspb.Int32Value
		return decode(data, &w, func() error { return setInt(p, int64(w.GetValue()), "int8") })
	case *int16:
		var w wrapperspb.Int32Value
		return decode(data, &w, func() error { return setInt(p, int64(w.GetValue()), "int16") })
	case *int32:
		var w wrapperspb.Int32Value
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *int64:
		var w wrapperspb.Int64Value
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *uint:
		var w wrapperspb.UInt64Value
		return decode(data, &w, func() error { return setUint(p, w.GetValue(), "uint") })
	case *uint8:
		var w wrapperspb.UInt32Value
		return decode(data, &w, func() error { return setUint(p, uint64(w.GetValue()), "uint8") })
	case *uint16:
		var w wrapperspb.UInt32Value
		return decode(data, &w, func() error { return setUint(p, uint64(w.GetValue()), "uint16") })
	case *uint32:
		var w wrapperspb.UInt32Value
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *uint64:
		var w wrapperspb.UInt64Value
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *float32:
		var w wrapperspb.FloatValue
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *float64:
		var w wrapperspb.DoubleValue
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *[]byte:
		var w wrapperspb.BytesValue
		return decode(data, &w, func() error { *p = w.GetValue(); return nil })
	case *struct{}:
		var w emptypb.Empty
		return decode(data, &w, func() error { *p = struct{}{}; return nil })
	case *time.Duration:
		var w durationpb.Duration
		return decode(data, &w, func() error { return setDuration(p, &w) })
	case *time.Time:
		var w timestamppb.Timestamp
		return decode(data, &w, func() error {
			if err := w.CheckValid(); err != nil {
				return err
			}
			*p = w.AsTime()
			return nil
		})
	case *lockstep.Address:
		var w wrapperspb.StringValue
		return decode(data, &w, func() error { return p.UnmarshalText([]byte(w.GetValue())) })
	}
	return unsupported(reflect.TypeOf(v).Elem())
}

// decode unmarshals data into the wrapper w and then calls set, which stores w's value.
func decode(data []byte, w proto.Message, set func() error) error {
	if err := proto.Unmarshal(data, w); err != nil {
		return err
	}
	return set()
}

// setInt stores n in *p, or returns the range error if *p's type, named name, cannot hold n.
func setInt[T int | int8 | int16](p *T, n int64, name string) error {
	if int64(T(n)) != n {
		return outOfRange(n, name)
	}
	*p = T(n)
	return nil
}

// setUint stores n in *p, or returns the range error if *p's type, named name, cannot hold n.
func setUint[T uint | uint8 | uint16](p *T, n uint64, name string) error {
	if uint64(T(n)) != n {
		return outOfRange(n, name)
	}
	*p = T(n)
	return nil
}

// setDuration stores d in *p. A Duration that fails CheckValid returns that error; a valid one
// beyond the range of time.Duration (about 292 years) returns the range error, with the value in
// seconds, such as "315576000000s".
func setDuration(p *time.Duration, d *durationpb.Duration) error {
	if err := d.CheckValid(); err != nil {
		return err
	}
	// AsDuration saturates at the bounds of time.Duration; converting back shows whether it did.
	got := d.AsDuration()
	if back := durationpb.New(got); back.GetSeconds() != d.GetSeconds() || back.GetNanos() != d.GetNanos() {
		return outOfRange(seconds(d), "time.Duration")
	}
	*p = got
	return nil
}

// seconds formats a valid Duration as decimal seconds: "-1.500000000s", or "3s" when it has no
// nanoseconds. CheckValid guarantees the seconds and nanoseconds have the same sign.
func seconds(d *durationpb.Duration) string {
	s, n := d.GetSeconds(), int64(d.GetNanos())
	sign := ""
	if s < 0 || n < 0 {
		sign, s, n = "-", -s, -n
	}
	if n == 0 {
		return fmt.Sprintf("%s%ds", sign, s)
	}
	return fmt.Sprintf("%s%d.%09ds", sign, s, n)
}

// outOfRange returns the range error of COD-170 for the value n and the Go type named name.
func outOfRange[N int64 | uint64 | string](n N, name string) error {
	return fmt.Errorf("protobuf: value %v out of range for %s", n, name)
}
