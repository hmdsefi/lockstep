// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package protobuf

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
)

const rootModule = "github.com/hmdsefi/lockstep"

type (
	plain    struct{ N int }
	namedInt int

	// wrappedMsg implements proto.Message through its embedded pointer, which is nil in a new
	// wrappedMsg, so the protobuf runtime cannot decode into one.
	wrappedMsg struct{ *wrapperspb.Int32Value }
)

// namedBytesMessage is a message type that is not a struct.
type namedBytesMessage []byte

func (*namedBytesMessage) ProtoReflect() protoreflect.Message { return nil }

// wantErr checks that err reads want and, when target is non-nil, wraps it.
func wantErr(t *testing.T, what string, err error, want string, target error) {
	t.Helper()
	if err == nil || err.Error() != want || (target != nil && !errors.Is(err, target)) {
		t.Errorf("%s: error %v; want %q wrapping %v", what, err, want, target)
	}
}

// mustMarshal returns the deterministic encoding of m.
func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatalf("marshal %T: %v", m, err)
	}
	return data
}

// AT-COD-17, COD-170: messages, the built-ins through their wrappers, range errors and Supports.
func TestATCOD17Protobuf(t *testing.T) {
	c := Codec{}

	t.Run("name", func(t *testing.T) {
		if got := c.Name(); got != "protobuf" {
			t.Errorf("Name() = %q, want %q", got, "protobuf")
		}
	})

	// The module's copy of the 19 built-ins equals the types a new codec registry starts with.
	t.Run("builtins match package codec", func(t *testing.T) {
		r := codec.NewRegistry()
		var root []reflect.Type
		for _, name := range r.Names() {
			typ, err := r.TypeOf(name)
			if err != nil {
				t.Fatalf("TypeOf(%q): %v", name, err)
			}
			root = append(root, typ)
		}
		sortTypes := func(ts []reflect.Type) []string {
			var s []string
			for _, typ := range ts {
				s = append(s, typ.String())
			}
			slices.Sort(s)
			return s
		}
		if len(builtins) != 19 {
			t.Errorf("the module lists %d built-ins, want 19", len(builtins))
		}
		if got, want := sortTypes(builtins), sortTypes(root); !slices.Equal(got, want) {
			t.Errorf("built-ins:\n got %v\nwant %v", got, want)
		}
		for _, typ := range root {
			if !slices.Contains(builtins, typ) {
				t.Errorf("codec built-in %v is missing from the module's list", typ)
			}
		}
		if err := r.Check(c); err != nil {
			t.Errorf("Check of a new registry: %v", err)
		}
	})

	t.Run("messages round trip", func(t *testing.T) {
		r := codec.NewRegistry()
		codec.RegisterIn[*wrapperspb.StringValue](r, "test.String")
		codec.RegisterIn[*structpb.Struct](r, "test.Struct")
		codec.RegisterIn[*timestamppb.Timestamp](r, "test.Timestamp")
		codec.RegisterIn[*durationpb.Duration](r, "test.Duration")
		codec.RegisterIn[*emptypb.Empty](r, "test.Empty")
		if err := r.Check(c); err != nil {
			t.Fatalf("Check: %v", err)
		}

		st, err := structpb.NewStruct(map[string]any{
			"name": "order-1", "count": 3.0, "tags": []any{"a", "b"},
			"nested": map[string]any{"z": true, "a": nil},
		})
		if err != nil {
			t.Fatal(err)
		}
		msgs := []struct {
			name string
			msg  proto.Message
		}{
			{"test.String", wrapperspb.String("héllo <&>")},
			{"test.Struct", st},
			{"test.Timestamp", timestamppb.New(time.Date(2026, 11, 13, 10, 30, 15, 123456789, time.UTC))},
			{"test.Duration", durationpb.New(-90 * time.Minute)},
			{"test.Empty", &emptypb.Empty{}},
		}
		for _, m := range msgs {
			name, data, err := r.Encode(c, m.msg)
			if err != nil || name != m.name {
				t.Errorf("Encode(%T) = %q, %v; want %q", m.msg, name, err, m.name)
				continue
			}
			if want := mustMarshal(t, m.msg); !bytes.Equal(data, want) {
				t.Errorf("Encode(%T) bytes %x, want %x", m.msg, data, want)
			}
			got, err := r.Decode(c, name, data)
			if err != nil {
				t.Errorf("Decode(%q): %v", name, err)
				continue
			}
			gm, ok := got.(proto.Message)
			if !ok || reflect.TypeOf(got) != reflect.TypeOf(m.msg) {
				t.Errorf("Decode(%q) gave a %T, want %T", name, got, m.msg)
				continue
			}
			if gm == m.msg || !proto.Equal(gm, m.msg) {
				t.Errorf("Decode(%q) = %v, want a new message equal to %v", name, got, m.msg)
			}
		}
	})

	t.Run("unmarshal allocates a new message", func(t *testing.T) {
		data := mustMarshal(t, wrapperspb.String("x"))
		old := wrapperspb.String("old")
		target := old
		if err := c.Unmarshal(data, &target); err != nil {
			t.Fatal(err)
		}
		if target == old || target.GetValue() != "x" || old.GetValue() != "old" {
			t.Errorf("Unmarshal into **M: target %v, old %v; want a new message \"x\" and old untouched", target, old)
		}
	})

	t.Run("deterministic bytes", func(t *testing.T) {
		keys := make([]string, 50)
		for i := range keys {
			keys[i] = string(rune('a'+i%26)) + string(rune('A'+i/26))
		}
		build := func(order []string) *structpb.Struct {
			s := &structpb.Struct{Fields: map[string]*structpb.Value{}}
			for _, k := range order {
				s.Fields[k] = structpb.NewStringValue("v-" + k)
			}
			return s
		}
		reversed := slices.Clone(keys)
		slices.Reverse(reversed)
		want, err := c.Marshal(build(keys))
		if err != nil {
			t.Fatal(err)
		}
		for i := range 20 {
			order := keys
			if i%2 == 1 {
				order = reversed
			}
			got, err := c.Marshal(build(order))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("run %d: bytes differ for equal messages", i)
			}
		}
	})

	t.Run("built-ins", func(t *testing.T) {
		var addr lockstep.Address
		if err := addr.UnmarshalText([]byte("lockstep://n1/user/worker#1f")); err != nil {
			t.Fatal(err)
		}
		when := time.Date(2026, 11, 13, 10, 30, 15, 123456789, time.FixedZone("X", 3600))
		cases := []struct {
			v       any
			name    string
			wrapper proto.Message // the message the value travels as
		}{
			{true, "lockstep.bool", wrapperspb.Bool(true)},
			{"s<&>", "lockstep.string", wrapperspb.String("s<&>")},
			{int(math.MinInt), "lockstep.int", wrapperspb.Int64(math.MinInt)},
			{int8(math.MinInt8), "lockstep.int8", wrapperspb.Int32(math.MinInt8)},
			{int16(math.MaxInt16), "lockstep.int16", wrapperspb.Int32(math.MaxInt16)},
			{int32(math.MinInt32), "lockstep.int32", wrapperspb.Int32(math.MinInt32)},
			{int64(math.MaxInt64), "lockstep.int64", wrapperspb.Int64(math.MaxInt64)},
			{uint(math.MaxUint), "lockstep.uint", wrapperspb.UInt64(math.MaxUint)},
			{uint8(math.MaxUint8), "lockstep.uint8", wrapperspb.UInt32(math.MaxUint8)},
			{uint16(math.MaxUint16), "lockstep.uint16", wrapperspb.UInt32(math.MaxUint16)},
			{uint32(math.MaxUint32), "lockstep.uint32", wrapperspb.UInt32(math.MaxUint32)},
			{uint64(math.MaxUint64), "lockstep.uint64", wrapperspb.UInt64(math.MaxUint64)},
			{float32(-1.5), "lockstep.float32", wrapperspb.Float(-1.5)},
			{math.SmallestNonzeroFloat64, "lockstep.float64", wrapperspb.Double(math.SmallestNonzeroFloat64)},
			{[]byte{0, 1, 0xff}, "lockstep.bytes", wrapperspb.Bytes([]byte{0, 1, 0xff})},
			{struct{}{}, "lockstep.empty", &emptypb.Empty{}},
			{-90*time.Minute - time.Nanosecond, "lockstep.duration", &durationpb.Duration{Seconds: -5400, Nanos: -1}},
			{when, "lockstep.time", &timestamppb.Timestamp{Seconds: when.Unix(), Nanos: 123456789}},
			{addr, "lockstep.address", wrapperspb.String("lockstep://n1/user/worker#1f")},
			{lockstep.Address{}, "lockstep.address", wrapperspb.String("")},
		}
		r := codec.NewRegistry()
		seen := map[string]bool{}
		for _, tc := range cases {
			seen[tc.name] = true
			name, data, err := r.Encode(c, tc.v)
			if err != nil || name != tc.name {
				t.Errorf("Encode(%T %v) = %q, %v; want %q", tc.v, tc.v, name, err, tc.name)
				continue
			}
			if want := mustMarshal(t, tc.wrapper); !bytes.Equal(data, want) {
				t.Errorf("Encode(%T %v) bytes %x, want those of %T %x", tc.v, tc.v, data, tc.wrapper, want)
			}
			got, err := r.Decode(c, name, data)
			if err != nil {
				t.Errorf("Decode(%q): %v", name, err)
				continue
			}
			if !sameValue(got, tc.v) {
				t.Errorf("Decode(%q) = %#v, want %#v", name, got, tc.v)
			}
		}
		if len(seen) != 19 {
			t.Errorf("the cases cover %d built-ins, want 19", len(seen))
		}
	})

	t.Run("range errors", func(t *testing.T) {
		r := codec.NewRegistry()
		cases := []struct {
			name string // the built-in's wire name
			msg  proto.Message
			want string
		}{
			{"lockstep.int8", wrapperspb.Int32(300), "protobuf: value 300 out of range for int8"},
			{"lockstep.int8", wrapperspb.Int32(math.MinInt8 - 1), "protobuf: value -129 out of range for int8"},
			{"lockstep.int16", wrapperspb.Int32(math.MaxInt16 + 1), "protobuf: value 32768 out of range for int16"},
			{"lockstep.int16", wrapperspb.Int32(math.MinInt32), "protobuf: value -2147483648 out of range for int16"},
			{"lockstep.uint8", wrapperspb.UInt32(256), "protobuf: value 256 out of range for uint8"},
			{"lockstep.uint8", wrapperspb.UInt32(math.MaxUint32), "protobuf: value 4294967295 out of range for uint8"},
			{"lockstep.uint16", wrapperspb.UInt32(math.MaxUint16 + 1), "protobuf: value 65536 out of range for uint16"},
			{
				"lockstep.duration", &durationpb.Duration{Seconds: 315576000000},
				"protobuf: value 315576000000s out of range for time.Duration",
			},
			{
				"lockstep.duration", &durationpb.Duration{Seconds: -9223372037, Nanos: -5},
				"protobuf: value -9223372037.000000005s out of range for time.Duration",
			},
		}
		for _, tc := range cases {
			data := mustMarshal(t, tc.msg)
			got, err := r.Decode(c, tc.name, data)
			wantErr(t, "Decode("+tc.name+")", err, "codec: decode "+tc.name+": "+tc.want, nil)
			if got != nil {
				t.Errorf("Decode(%q) value %v, want nil", tc.name, got)
			}
		}

		// The edges themselves decode.
		edges := []struct {
			name string
			msg  proto.Message
			want any
		}{
			{"lockstep.int8", wrapperspb.Int32(math.MaxInt8), int8(math.MaxInt8)},
			{"lockstep.int16", wrapperspb.Int32(math.MinInt16), int16(math.MinInt16)},
			{"lockstep.uint16", wrapperspb.UInt32(math.MaxUint16), uint16(math.MaxUint16)},
			{"lockstep.duration", durationpb.New(math.MaxInt64), time.Duration(math.MaxInt64)},
			{"lockstep.duration", durationpb.New(math.MinInt64), time.Duration(math.MinInt64)},
		}
		for _, e := range edges {
			got, err := r.Decode(c, e.name, mustMarshal(t, e.msg))
			if err != nil || got != e.want {
				t.Errorf("Decode(%q, %v) = %v, %v; want %v", e.name, e.msg, got, err, e.want)
			}
		}

		// A failed range check leaves the target unchanged.
		n := int8(7)
		if err := c.Unmarshal(mustMarshal(t, wrapperspb.Int32(300)), &n); err == nil || n != 7 {
			t.Errorf("Unmarshal(300 into int8) = %v, n = %d; want an error and n = 7", err, n)
		}
	})

	t.Run("invalid timestamp and duration", func(t *testing.T) {
		ts := &timestamppb.Timestamp{Seconds: -62135596801} // one second before year 1
		var when time.Time
		wantErr(t, "Unmarshal(invalid Timestamp)", c.Unmarshal(mustMarshal(t, ts), &when), ts.CheckValid().Error(), nil)

		ts = &timestamppb.Timestamp{Seconds: 1, Nanos: 1e9}
		wantErr(t, "Unmarshal(Timestamp nanos)", c.Unmarshal(mustMarshal(t, ts), &when), ts.CheckValid().Error(), nil)

		d := &durationpb.Duration{Seconds: 1, Nanos: -1} // signs differ
		var dur time.Duration
		wantErr(t, "Unmarshal(invalid Duration)", c.Unmarshal(mustMarshal(t, d), &dur), d.CheckValid().Error(), nil)

		d = &durationpb.Duration{Seconds: 315576000001}
		wantErr(t, "Unmarshal(Duration beyond 10000 years)", c.Unmarshal(mustMarshal(t, d), &dur), d.CheckValid().Error(), nil)

		if !when.IsZero() || dur != 0 {
			t.Errorf("targets changed: %v, %v", when, dur)
		}

		// A time.Time no Timestamp can hold fails on the sending side.
		late := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		_, err := c.Marshal(late)
		wantErr(t, "Marshal(year 10000)", err, timestamppb.New(late).CheckValid().Error(), nil)
	})

	t.Run("invalid address and bytes", func(t *testing.T) {
		var addr lockstep.Address
		err := c.Unmarshal(mustMarshal(t, wrapperspb.String("lockstep://bad node#1")), &addr)
		wantErr(t, "Unmarshal(invalid address)", err, `lockstep: invalid address "lockstep://bad node#1"`, nil)

		var s string
		if err := c.Unmarshal([]byte{0xff}, &s); err == nil {
			t.Error("Unmarshal of malformed bytes returned nil")
		}
		var m *wrapperspb.StringValue
		if err := c.Unmarshal([]byte{0xff}, &m); err == nil || m != nil {
			t.Errorf("Unmarshal of malformed bytes into **M = %v, %v; want an error and nil", err, m)
		}
	})

	t.Run("supports", func(t *testing.T) {
		for _, typ := range append(slices.Clone(builtins),
			reflect.TypeFor[*wrapperspb.Int32Value](),
			reflect.TypeFor[*structpb.Struct](),
		) {
			if err := c.Supports(typ); err != nil {
				t.Errorf("Supports(%v) = %v, want nil", typ, err)
			}
		}
		rejected := []reflect.Type{
			reflect.TypeFor[wrapperspb.Int32Value](), // the message struct, not its pointer
			reflect.TypeFor[**wrapperspb.Int32Value](),
			reflect.TypeFor[proto.Message](), // an interface type, not a pointer type
			reflect.TypeFor[plain](),
			reflect.TypeFor[*plain](),
			reflect.TypeFor[namedInt](),
			reflect.TypeFor[*int](),
			reflect.TypeFor[[]int](),
			reflect.TypeFor[map[string]int](),
			reflect.TypeFor[complex128](),
			reflect.TypeFor[lockstep.PID[int]](),
			reflect.TypeFor[*wrappedMsg](),        // implements proto.Message through a nil embedded pointer
			reflect.TypeFor[*dynamicpb.Message](), // its zero value has no descriptor
			reflect.TypeFor[*namedBytesMessage](), // a pointer to a non-struct type
			nil,
		}
		for _, typ := range rejected {
			want := "codec: unsupported type: protobuf cannot encode " + typeString(typ) +
				": not a proto.Message or built-in type"
			wantErr(t, "Supports", c.Supports(typ), want, codec.ErrUnsupported)
		}

		// Marshal, Unmarshal and the registry return the same error for such a type.
		want := "codec: unsupported type: protobuf cannot encode protobuf.plain: not a proto.Message or built-in type"
		_, err := c.Marshal(plain{})
		wantErr(t, "Marshal(plain)", err, want, codec.ErrUnsupported)
		var p plain
		wantErr(t, "Unmarshal(*plain)", c.Unmarshal(nil, &p), want, codec.ErrUnsupported)
		_, err = c.Marshal(namedInt(1))
		wantErr(t, "Marshal(namedInt)", err,
			"codec: unsupported type: protobuf cannot encode protobuf.namedInt: not a proto.Message or built-in type",
			codec.ErrUnsupported)
		var msg wrapperspb.StringValue // *M rather than **M
		wantErr(t, "Unmarshal(*M)", c.Unmarshal(nil, &msg),
			"codec: unsupported type: protobuf cannot encode wrapperspb.StringValue: not a proto.Message or built-in type",
			codec.ErrUnsupported)

		r := codec.NewRegistry()
		codec.RegisterIn[plain](r, "test.Plain")
		_, _, err = r.Encode(c, plain{N: 1})
		wantErr(t, "Encode(plain)", err, want, codec.ErrUnsupported)
		_, err = r.Decode(c, "test.Plain", nil)
		wantErr(t, "Decode(test.Plain)", err, want, codec.ErrUnsupported)
		wantErr(t, "Check", r.Check(c), want, codec.ErrUnsupported)

		// Message types whose zero value is unusable return the Supports error rather than panic.
		wantWrapped := "codec: unsupported type: protobuf cannot encode *protobuf.wrappedMsg: not a proto.Message or built-in type"
		var wm *wrappedMsg
		wantErr(t, "Unmarshal(**wrappedMsg)", c.Unmarshal(mustMarshal(t, wrapperspb.Int32(1)), &wm), wantWrapped, codec.ErrUnsupported)
		_, err = c.Marshal(&wrappedMsg{wrapperspb.Int32(1)})
		wantErr(t, "Marshal(*wrappedMsg)", err, wantWrapped, codec.ErrUnsupported)
		wantDynamic := "codec: unsupported type: protobuf cannot encode *dynamicpb.Message: not a proto.Message or built-in type"
		var dm *dynamicpb.Message
		wantErr(t, "Unmarshal(**dynamicpb.Message)", c.Unmarshal([]byte{8, 1}, &dm), wantDynamic, codec.ErrUnsupported)
		_, err = c.Marshal(&dynamicpb.Message{})
		wantErr(t, "Marshal(*dynamicpb.Message)", err, wantDynamic, codec.ErrUnsupported)
		if wm != nil || dm != nil {
			t.Errorf("targets changed: %v, %v", wm, dm)
		}
		r = codec.NewRegistry()
		codec.RegisterIn[*wrappedMsg](r, "test.Wrapped")
		_, err = r.Decode(c, "test.Wrapped", mustMarshal(t, wrapperspb.Int32(1)))
		wantErr(t, "Decode(test.Wrapped)", err, wantWrapped, codec.ErrUnsupported)
		wantErr(t, "Check(test.Wrapped)", r.Check(c), wantWrapped, codec.ErrUnsupported)
	})

	t.Run("nil message", func(t *testing.T) {
		r := codec.NewRegistry()
		codec.RegisterIn[*wrapperspb.StringValue](r, "test.String")
		name, data, err := r.Encode(c, (*wrapperspb.StringValue)(nil))
		if err != nil || len(data) != 0 {
			t.Fatalf("Encode(nil message) = %q, %x, %v; want empty bytes", name, data, err)
		}
		got, err := r.Decode(c, name, data)
		m, ok := got.(*wrapperspb.StringValue)
		if err != nil || !ok || m == nil || m.GetValue() != "" {
			t.Errorf("Decode(empty bytes) = %#v, %v; want a new empty message", got, err)
		}
	})

	t.Run("unmarshal needs a non-nil pointer", func(t *testing.T) {
		var n int
		var np *int
		for _, tc := range []struct {
			v    any
			want string
		}{
			{nil, "protobuf: Unmarshal(nil)"},
			{n, "protobuf: Unmarshal(non-pointer int)"},
			{np, "protobuf: Unmarshal(nil *int)"},
		} {
			wantErr(t, "Unmarshal", c.Unmarshal(nil, tc.v), tc.want, nil)
		}
	})
}

// typeString is how the Supports error prints t.
func typeString(t reflect.Type) string {
	if t == nil {
		return "<nil>"
	}
	return t.String()
}

// sameValue reports whether the decoded value got equals want: same type, and equal by the
// type's own notion of equality.
func sameValue(got, want any) bool {
	switch w := want.(type) {
	case []byte:
		g, ok := got.([]byte)
		return ok && bytes.Equal(g, w)
	case time.Time:
		g, ok := got.(time.Time)
		return ok && g.Equal(w) && g.Location() == time.UTC
	}
	return reflect.TypeOf(got) == reflect.TypeOf(want) && got == want
}

// AT-COD-18, COD-180: the root module neither requires nor links google.golang.org/protobuf.
func TestATCOD18ModuleBoundary(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	// run runs the go command in the root module, without any workspace.
	run := func(args ...string) []byte {
		t.Helper()
		cmd := exec.Command(goBin, args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOWORK=off")
		var stderr strings.Builder
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
		}
		return out
	}
	const protobufModule = "google.golang.org/protobuf"

	var mod struct {
		Module  struct{ Path string }
		Require []struct{ Path, Version string }
	}
	if err := json.Unmarshal(run("mod", "edit", "-json", filepath.Join(root, "go.mod")), &mod); err != nil {
		t.Fatal(err)
	}
	if mod.Module.Path != rootModule {
		t.Fatalf("%s/go.mod is module %s, want %s", root, mod.Module.Path, rootModule)
	}
	for _, req := range mod.Require {
		if req.Path == protobufModule || strings.HasPrefix(req.Path, protobufModule+"/") {
			t.Errorf("the root go.mod requires %s %s", req.Path, req.Version)
		}
	}

	deps := strings.Fields(string(run("list", "-deps", "-test", "./...")))
	if len(deps) == 0 {
		t.Fatal("go list -deps printed nothing")
	}
	if !slices.Contains(deps, rootModule) || !slices.Contains(deps, rootModule+"/codec") {
		t.Fatalf("go list -deps of the root module does not list %s and its codec package", rootModule)
	}
	for _, p := range deps {
		if p == protobufModule || strings.HasPrefix(p, protobufModule+"/") {
			t.Errorf("a package of the root module depends on %s", p)
		}
	}
}
