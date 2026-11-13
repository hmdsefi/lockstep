// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
)

type (
	encItem struct{ N int }
	encMsg  struct {
		Name  string
		Tags  map[string]int
		Items []encItem
		At    lockstep.Address
	}
	encNamed encMsg
	encPtr   struct{ V int }
	encChan  struct{ C chan int }
	encFloat struct{ F float64 }

	chkA struct{ N int }
	chkM struct{ N int }
	chkZ struct{ N int }
)

var errBoom = errors.New("boom")

// fakeCodec is a Codec that rejects the types in reject and otherwise marshals as JSON does,
// unless marshalErr or unmarshalErr is set. It records each Supports call, in order.
type fakeCodec struct {
	name                     string
	reject                   []reflect.Type
	marshalErr, unmarshalErr error

	mu         sync.Mutex
	asked      []reflect.Type
	marshals   int
	unmarshals int
}

var _ codec.Codec = (*fakeCodec)(nil)

func newFake(name string, reject ...reflect.Type) *fakeCodec {
	return &fakeCodec{name: name, reject: reject}
}

func (f *fakeCodec) Name() string { return f.name }

func (f *fakeCodec) Supports(t reflect.Type) error {
	f.mu.Lock()
	f.asked = append(f.asked, t)
	f.mu.Unlock()
	if slices.Contains(f.reject, t) {
		return fmt.Errorf("%w: fake rejects %s", codec.ErrUnsupported, t)
	}
	return nil
}

func (f *fakeCodec) Marshal(v any) ([]byte, error) {
	f.mu.Lock()
	f.marshals++
	f.mu.Unlock()
	if f.marshalErr != nil {
		return nil, f.marshalErr
	}
	return codec.JSON{}.Marshal(v)
}

func (f *fakeCodec) Unmarshal(data []byte, v any) error {
	f.mu.Lock()
	f.unmarshals++
	f.mu.Unlock()
	if f.unmarshalErr != nil {
		return f.unmarshalErr
	}
	return codec.JSON{}.Unmarshal(data, v)
}

// calls returns how many times Supports was asked about t.
func (f *fakeCodec) calls(t reflect.Type) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, a := range f.asked {
		if a == t {
			n++
		}
	}
	return n
}

// counts returns the number of Supports, Marshal and Unmarshal calls so far.
func (f *fakeCodec) counts() (supports, marshals, unmarshals int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.asked), f.marshals, f.unmarshals
}

// wantErr checks that err reads want and wraps target.
func wantErr(t *testing.T, what string, err error, want string, target error) {
	t.Helper()
	if err == nil || err.Error() != want || !errors.Is(err, target) {
		t.Errorf("%s: error %v; want %q wrapping %v", what, err, want, target)
	}
}

// AT-COD-04 (the Encode part), COD-040: the 19 built-ins encode and decode with JSON in a new
// registry and in the process registry, without registration.
func TestATCOD04BuiltinEncode(t *testing.T) {
	var addr lockstep.Address
	fromText(t, &addr, "lockstep://n1/user/worker#1f")
	when := time.Date(2026, 11, 13, 10, 30, 15, 123456789, time.UTC)

	builtins := []struct {
		v    any
		name string
	}{
		{true, "lockstep.bool"},
		{"s<&>", "lockstep.string"},
		{int(-7), "lockstep.int"},
		{int8(math.MinInt8), "lockstep.int8"},
		{int16(math.MaxInt16), "lockstep.int16"},
		{rune('x'), "lockstep.int32"},
		{int64(math.MinInt64), "lockstep.int64"},
		{uint(7), "lockstep.uint"},
		{byte(math.MaxUint8), "lockstep.uint8"},
		{uint16(math.MaxUint16), "lockstep.uint16"},
		{uint32(math.MaxUint32), "lockstep.uint32"},
		{uint64(math.MaxUint64), "lockstep.uint64"},
		{float32(1.5), "lockstep.float32"},
		{float64(-2.25), "lockstep.float64"},
		{[]byte{0, 1, 0xff}, "lockstep.bytes"},
		{struct{}{}, "lockstep.empty"},
		{90 * time.Second, "lockstep.duration"},
		{when, "lockstep.time"},
		{addr, "lockstep.address"},
	}
	if len(builtins) != 19 {
		t.Fatalf("the test lists %d built-ins, want 19", len(builtins))
	}

	for _, reg := range []struct {
		what string
		r    *codec.Registry
	}{{"NewRegistry", codec.NewRegistry()}, {"Default", codec.Default()}} {
		t.Run(reg.what, func(t *testing.T) {
			for _, b := range builtins {
				name, data, err := reg.r.Encode(codec.JSON{}, b.v)
				if err != nil || name != b.name {
					t.Errorf("Encode(%T) = %q, %s, %v; want %q", b.v, name, data, err, b.name)
					continue
				}
				want, _ := codec.JSON{}.Marshal(b.v)
				if string(data) != string(want) {
					t.Errorf("Encode(%T) bytes %s, want JSON's %s", b.v, data, want)
				}
				got, err := reg.r.Decode(codec.JSON{}, name, data)
				if err != nil {
					t.Errorf("Decode(%q, %s): %v", name, data, err)
					continue
				}
				if reflect.TypeOf(got) != reflect.TypeOf(b.v) {
					t.Errorf("Decode(%q) has type %T, want %T", name, got, b.v)
					continue
				}
				if tm, ok := got.(time.Time); ok {
					if !tm.Equal(when) {
						t.Errorf("Decode(%q) = %v, want %v", name, tm, when)
					}
				} else if !reflect.DeepEqual(got, b.v) {
					t.Errorf("Decode(%q) = %#v, want %#v", name, got, b.v)
				}
			}
			if err := reg.r.Check(codec.JSON{}); err != nil && reg.what == "NewRegistry" {
				t.Errorf("Check(JSON) of the built-ins: %v", err)
			}
		})
	}
}

// AT-COD-05, COD-060, COD-070: a registered value round trips through Encode and Decode; Decode
// returns a value of the registered type, a T for T and a *X for *X.
func TestATCOD05RoundTrip(t *testing.T) {
	r := codec.NewRegistry()
	codec.RegisterIn[encMsg](r, "test.Msg")
	codec.RegisterIn[encNamed](r, "test.Named")
	codec.RegisterIn[*encPtr](r, "test.Ptr")

	var addr lockstep.Address
	fromText(t, &addr, "lockstep://n1/user/worker#1f")
	msg := encMsg{
		Name:  "order <1>",
		Tags:  map[string]int{"b": 2, "a": 1},
		Items: []encItem{{1}, {2}},
		At:    addr,
	}

	t.Run("value", func(t *testing.T) {
		name, data, err := r.Encode(codec.JSON{}, msg)
		if err != nil || name != "test.Msg" {
			t.Fatalf("Encode = %q, %s, %v; want test.Msg", name, data, err)
		}
		want := `{"Name":"order <1>","Tags":{"a":1,"b":2},"Items":[{"N":1},{"N":2}],"At":"lockstep://n1/user/worker#1f"}`
		if string(data) != want {
			t.Errorf("Encode bytes %s\nwant           %s", data, want)
		}
		got, err := r.Decode(codec.JSON{}, name, data)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		m, ok := got.(encMsg)
		if !ok {
			t.Fatalf("Decode returned %T, want codec_test.encMsg", got)
		}
		if !reflect.DeepEqual(m, msg) {
			t.Errorf("Decode = %+v, want %+v", m, msg)
		}
	})

	t.Run("named type", func(t *testing.T) {
		name, data, err := r.Encode(codec.JSON{}, encNamed(msg))
		if err != nil || name != "test.Named" {
			t.Fatalf("Encode = %q, %v; want test.Named", name, err)
		}
		got, err := r.Decode(codec.JSON{}, name, data)
		if n, ok := got.(encNamed); err != nil || !ok || !reflect.DeepEqual(n, encNamed(msg)) {
			t.Errorf("Decode = %#v, %v; want %#v", got, err, encNamed(msg))
		}
	})

	t.Run("pointer", func(t *testing.T) {
		in := &encPtr{V: 7}
		name, data, err := r.Encode(codec.JSON{}, in)
		if err != nil || name != "test.Ptr" || string(data) != `{"V":7}` {
			t.Fatalf("Encode = %q, %s, %v; want test.Ptr, {\"V\":7}", name, data, err)
		}
		got, err := r.Decode(codec.JSON{}, name, data)
		p, ok := got.(*encPtr)
		if err != nil || !ok {
			t.Fatalf("Decode = %#v, %v; want a *encPtr", got, err)
		}
		if p == in || *p != *in {
			t.Errorf("Decode = %p %+v; want a new pointer to %+v", p, *p, *in)
		}

		// A nil *encPtr encodes as null and decodes as a nil *encPtr.
		name, data, err = r.Encode(codec.JSON{}, (*encPtr)(nil))
		if err != nil || name != "test.Ptr" || string(data) != "null" {
			t.Fatalf("Encode(nil *encPtr) = %q, %s, %v; want test.Ptr, null", name, data, err)
		}
		got, err = r.Decode(codec.JSON{}, name, data)
		if p, ok := got.(*encPtr); err != nil || !ok || p != nil {
			t.Errorf("Decode(null) = %#v, %v; want a nil *encPtr", got, err)
		}

		// The value type encPtr is not registered: only *encPtr is.
		if _, _, err := r.Encode(codec.JSON{}, encPtr{V: 7}); !errors.Is(err, codec.ErrNotRegistered) {
			t.Errorf("Encode(encPtr) error %v, want ErrNotRegistered", err)
		}
	})
}

// AT-COD-06, COD-060, COD-070, §4.1: the errors of each step of Encode and Decode, their texts and
// what they wrap; a later step does not run after an earlier one fails; a nil codec panics.
func TestATCOD06Errors(t *testing.T) {
	r := codec.NewRegistry()
	codec.RegisterIn[encMsg](r, "test.Msg")
	codec.RegisterIn[encChan](r, "test.Chan")
	codec.RegisterIn[encFloat](r, "test.Float")
	const chanErr = "codec: unsupported type: json cannot encode codec_test.encChan: field C: chan type chan int"

	t.Run("encode not registered", func(t *testing.T) {
		f := newFake("fake-unreg", reflect.TypeFor[unknown]())
		for _, tt := range []struct {
			v    any
			want string
		}{
			{unknown{}, "codec: type not registered: codec_test.unknown"},
			{nil, "codec: type not registered: <nil>"},
			{&encMsg{}, "codec: type not registered: *codec_test.encMsg"},
		} {
			for _, c := range []codec.Codec{codec.JSON{}, f} {
				name, data, err := r.Encode(c, tt.v)
				wantErr(t, fmt.Sprintf("Encode(%s, %T)", c.Name(), tt.v), err, tt.want, codec.ErrNotRegistered)
				if name != "" || data != nil {
					t.Errorf("Encode(%T) = %q, %v; want \"\", nil on error", tt.v, name, data)
				}
			}
		}
		if s, m, _ := f.counts(); s != 0 || m != 0 {
			t.Errorf("after a lookup failure: %d Supports and %d Marshal calls, want 0", s, m)
		}
	})

	t.Run("encode unsupported", func(t *testing.T) {
		name, data, err := r.Encode(codec.JSON{}, encChan{})
		wantErr(t, "Encode(json, encChan)", err, chanErr, codec.ErrUnsupported)
		if name != "" || data != nil {
			t.Errorf("Encode(encChan) = %q, %v; want \"\", nil on error", name, data)
		}
		if want := (codec.JSON{}).Supports(reflect.TypeFor[encChan]()); err.Error() != want.Error() {
			t.Errorf("Encode error %q, want Supports's %q unchanged", err, want)
		}

		f := newFake("fake-unsup", reflect.TypeFor[encMsg]())
		f.marshalErr = errBoom
		name, data, err = r.Encode(f, encMsg{})
		wantErr(t, "Encode(fake, encMsg)", err, "codec: unsupported type: fake rejects codec_test.encMsg",
			codec.ErrUnsupported)
		if name != "" || data != nil {
			t.Errorf("Encode(encMsg) = %q, %v; want \"\", nil on error", name, data)
		}
		if _, m, _ := f.counts(); m != 0 {
			t.Errorf("after a Supports failure: %d Marshal calls, want 0", m)
		}
	})

	t.Run("encode marshal error", func(t *testing.T) {
		f := newFake("fake-marshal")
		f.marshalErr = errBoom
		name, data, err := r.Encode(f, encMsg{})
		wantErr(t, "Encode(fake, encMsg)", err, "codec: encode test.Msg: boom", errBoom)
		if name != "" || data != nil {
			t.Errorf("Encode(encMsg) = %q, %v; want \"\", nil on error", name, data)
		}

		name, data, err = r.Encode(codec.JSON{}, encFloat{F: math.NaN()})
		var uve *json.UnsupportedValueError
		if !errors.As(err, &uve) || err.Error() != "codec: encode test.Float: json: unsupported value: NaN" {
			t.Errorf("Encode(NaN) error %v; want \"codec: encode test.Float: json: unsupported value: NaN\" "+
				"wrapping *json.UnsupportedValueError", err)
		}
		if name != "" || data != nil {
			t.Errorf("Encode(NaN) = %q, %v; want \"\", nil on error", name, data)
		}
	})

	t.Run("decode not registered", func(t *testing.T) {
		f := newFake("fake-dec-unreg")
		for _, c := range []codec.Codec{codec.JSON{}, f} {
			got, err := r.Decode(c, "no.Such", []byte("{}"))
			wantErr(t, "Decode("+c.Name()+", no.Such)", err, `codec: type not registered: name "no.Such"`,
				codec.ErrNotRegistered)
			if got != nil {
				t.Errorf("Decode(no.Such) = %#v, want nil on error", got)
			}
		}
		if s, _, u := f.counts(); s != 0 || u != 0 {
			t.Errorf("after a lookup failure: %d Supports and %d Unmarshal calls, want 0", s, u)
		}
	})

	t.Run("decode unsupported", func(t *testing.T) {
		got, err := r.Decode(codec.JSON{}, "test.Chan", []byte("{}"))
		wantErr(t, "Decode(json, test.Chan)", err, chanErr, codec.ErrUnsupported)
		if got != nil {
			t.Errorf("Decode(test.Chan) = %#v, want nil on error", got)
		}

		f := newFake("fake-dec-unsup", reflect.TypeFor[encMsg]())
		got, err = r.Decode(f, "test.Msg", []byte("{}"))
		wantErr(t, "Decode(fake, test.Msg)", err, "codec: unsupported type: fake rejects codec_test.encMsg",
			codec.ErrUnsupported)
		if got != nil {
			t.Errorf("Decode(test.Msg) = %#v, want nil on error", got)
		}
		if _, _, u := f.counts(); u != 0 {
			t.Errorf("after a Supports failure: %d Unmarshal calls, want 0", u)
		}
	})

	t.Run("decode unmarshal error", func(t *testing.T) {
		f := newFake("fake-unmarshal")
		f.unmarshalErr = errBoom
		got, err := r.Decode(f, "test.Msg", []byte("{}"))
		wantErr(t, "Decode(fake, test.Msg)", err, "codec: decode test.Msg: boom", errBoom)
		if got != nil {
			t.Errorf("Decode(test.Msg) = %#v, want nil on error", got)
		}

		got, err = r.Decode(codec.JSON{}, "test.Msg", []byte("{"))
		var se *json.SyntaxError
		if !errors.As(err, &se) || err.Error() != "codec: decode test.Msg: unexpected end of JSON input" {
			t.Errorf("Decode({) error %v; want \"codec: decode test.Msg: unexpected end of JSON input\" "+
				"wrapping *json.SyntaxError", err)
		}
		if got != nil {
			t.Errorf("Decode({) = %#v, want nil on error", got)
		}

		got, err = r.Decode(codec.JSON{}, "lockstep.int64", []byte(`"x"`))
		var ute *json.UnmarshalTypeError
		if !errors.As(err, &ute) || !strings.HasPrefix(err.Error(), "codec: decode lockstep.int64: json: ") {
			t.Errorf("Decode(\"x\" as int64) error %v; want \"codec: decode lockstep.int64: json: ...\" "+
				"wrapping *json.UnmarshalTypeError", err)
		}
		if got != nil {
			t.Errorf("Decode(\"x\" as int64) = %#v, want nil on error", got)
		}
	})

	t.Run("nil codec", func(t *testing.T) {
		var c codec.Codec
		const want = "codec: nil codec"
		// The codec is checked first, so even a value that is not registered panics.
		for _, tt := range []struct {
			what string
			f    func()
		}{
			{"Encode", func() { _, _, _ = r.Encode(c, encMsg{}) }},
			{"Encode of an unknown type", func() { _, _, _ = r.Encode(c, unknown{}) }},
			{"Decode", func() { _, _ = r.Decode(c, "test.Msg", []byte("{}")) }},
			{"Decode of an unknown name", func() { _, _ = r.Decode(c, "no.Such", nil) }},
			{"Check", func() { _ = r.Check(c) }},
			{"Check of a new registry", func() { _ = codec.NewRegistry().Check(c) }},
		} {
			if got := panicText(t, tt.f); got != want {
				t.Errorf("%s with a nil codec: panic %q, want %q", tt.what, got, want)
			}
		}
	})
}

// AT-COD-09, COD-080, COD-085: Check asks the codec about every registered type in Names order and
// returns the first error unchanged, or nil; Supports results are cached per codec name and type.
func TestATCOD09Check(t *testing.T) {
	r := codec.NewRegistry()
	codec.RegisterIn[chkA](r, "a.First") // sorts before the built-ins
	codec.RegisterIn[chkM](r, "m.Middle")
	codec.RegisterIn[chkZ](r, "z.Last")
	names := r.Names()
	order := make([]reflect.Type, len(names))
	for i, name := range names {
		typ, err := r.TypeOf(name)
		if err != nil {
			t.Fatalf("TypeOf(%q): %v", name, err)
		}
		order[i] = typ
	}

	t.Run("all supported", func(t *testing.T) {
		if err := r.Check(codec.JSON{}); err != nil {
			t.Errorf("Check(JSON) = %v, want nil", err)
		}
		f := newFake("fake-all")
		if err := r.Check(f); err != nil {
			t.Errorf("Check(fake) = %v, want nil", err)
		}
		f.mu.Lock()
		asked := slices.Clone(f.asked)
		f.mu.Unlock()
		if !slices.Equal(asked, order) {
			t.Errorf("Check asked about %v, want Names order %v", asked, order)
		}

		// Cached: neither a second Check nor an Encode or Decode asks again.
		if err := r.Check(f); err != nil {
			t.Errorf("second Check(fake) = %v, want nil", err)
		}
		if _, data, err := r.Encode(f, chkM{N: 1}); err != nil {
			t.Errorf("Encode(chkM): %v", err)
		} else if _, err := r.Decode(f, "m.Middle", data); err != nil {
			t.Errorf("Decode(m.Middle): %v", err)
		}
		for _, typ := range order {
			if n := f.calls(typ); n != 1 {
				t.Errorf("Supports(%v) called %d times, want 1", typ, n)
			}
		}

		// Another codec with the same name shares the cache; a registry has its own cache.
		same := newFake("fake-all", order...)
		if err := r.Check(same); err != nil {
			t.Errorf("Check(fake with the same name) = %v, want the cached nil", err)
		}
		if s, _, _ := same.counts(); s != 0 {
			t.Errorf("Check(fake with the same name) asked %d times, want 0", s)
		}
		if err := codec.NewRegistry().Check(same); err == nil {
			t.Errorf("Check(fake) of a new registry = nil, want its own answer, an error")
		}
	})

	for _, tt := range []struct {
		codec  string
		reject []reflect.Type
		first  reflect.Type
	}{
		{"fake-1", []reflect.Type{reflect.TypeFor[chkZ](), reflect.TypeFor[int64]()}, reflect.TypeFor[int64]()},
		{"fake-2", []reflect.Type{reflect.TypeFor[chkZ](), reflect.TypeFor[chkM]()}, reflect.TypeFor[chkM]()},
		{"fake-3", []reflect.Type{reflect.TypeFor[int64](), reflect.TypeFor[chkA]()}, reflect.TypeFor[chkA]()},
		// Names order, not the built-in table's: lockstep.address sorts before lockstep.bool.
		{"fake-4", []reflect.Type{reflect.TypeFor[bool](), reflect.TypeFor[lockstep.Address]()},
			reflect.TypeFor[lockstep.Address]()},
		{"fake-5", []reflect.Type{reflect.TypeFor[chkZ]()}, reflect.TypeFor[chkZ]()},
	} {
		t.Run(tt.codec, func(t *testing.T) {
			f := newFake(tt.codec, tt.reject...)
			want := fmt.Sprintf("codec: unsupported type: fake rejects %s", tt.first)
			wantErr(t, "Check", r.Check(f), want, codec.ErrUnsupported)

			// Check stops at the first error: it asked about the types up to it, once each.
			stop := slices.Index(order, tt.first) + 1
			f.mu.Lock()
			asked := slices.Clone(f.asked)
			f.mu.Unlock()
			if !slices.Equal(asked, order[:stop]) {
				t.Errorf("Check asked about %v, want %v", asked, order[:stop])
			}

			// The cached error comes back, without asking again, from Check and Encode.
			wantErr(t, "second Check", r.Check(f), want, codec.ErrUnsupported)
			v := reflect.New(tt.first).Elem().Interface()
			_, _, err := r.Encode(f, v)
			wantErr(t, fmt.Sprintf("Encode(%T)", v), err, want, codec.ErrUnsupported)
			if n := f.calls(tt.first); n != 1 {
				t.Errorf("Supports(%v) called %d times, want 1", tt.first, n)
			}
		})
	}

	t.Run("json", func(t *testing.T) {
		rc := codec.NewRegistry()
		codec.RegisterIn[encChan](rc, "test.Chan")
		codec.RegisterIn[encMsg](rc, "test.Msg")
		want := (codec.JSON{}).Supports(reflect.TypeFor[encChan]())
		if err := rc.Check(codec.JSON{}); err == nil || err.Error() != want.Error() ||
			!errors.Is(err, codec.ErrUnsupported) {
			t.Errorf("Check(JSON) = %v, want %v", err, want)
		}
	})
}

// slot is a family of distinct types for AT-COD-12, one per type argument.
type slot[K any] struct{ V int }

// exercise registers T under name in r, closes ready, and round trips v through Encode and Decode;
// the registration must be visible to these calls, which start after RegisterIn returns.
func exercise[T any](t *testing.T, r *codec.Registry, c codec.Codec, name string, v T, ready chan<- struct{}) {
	codec.RegisterIn[T](r, name)
	close(ready)
	for range 50 {
		got, data, err := r.Encode(c, v)
		if err != nil || got != name {
			t.Errorf("Encode(%T) = %q, %v; want %q", v, got, err, name)
			return
		}
		back, err := r.Decode(c, name, data)
		if b, ok := back.(T); err != nil || !ok || !reflect.DeepEqual(b, v) {
			t.Errorf("Decode(%q) = %#v, %v; want %#v", name, back, err, v)
			return
		}
	}
}

// AT-COD-12, COD-085: registering, encoding, decoding and checking run concurrently without a data
// race, and a registration is visible to every call that starts after RegisterIn returns, in any
// goroutine.
func TestATCOD12Concurrent(t *testing.T) {
	r := codec.NewRegistry()
	fake := newFake("fake-conc")
	codecs := []codec.Codec{codec.JSON{}, fake}

	const types = 8
	names := make([]string, types)
	ready := make([]chan struct{}, types)
	for i := range types {
		names[i] = fmt.Sprintf("conc.Slot%d", i)
		ready[i] = make(chan struct{})
	}
	registerers := []func(c codec.Codec){
		func(c codec.Codec) { exercise(t, r, c, names[0], slot[[0]byte]{V: 0}, ready[0]) },
		func(c codec.Codec) { exercise(t, r, c, names[1], slot[[1]byte]{V: 1}, ready[1]) },
		func(c codec.Codec) { exercise(t, r, c, names[2], slot[[2]byte]{V: 2}, ready[2]) },
		func(c codec.Codec) { exercise(t, r, c, names[3], slot[[3]byte]{V: 3}, ready[3]) },
		func(c codec.Codec) { exercise(t, r, c, names[4], slot[[4]byte]{V: 4}, ready[4]) },
		func(c codec.Codec) { exercise(t, r, c, names[5], slot[[5]byte]{V: 5}, ready[5]) },
		func(c codec.Codec) { exercise(t, r, c, names[6], slot[[6]byte]{V: 6}, ready[6]) },
		func(c codec.Codec) { exercise(t, r, c, names[7], slot[[7]byte]{V: 7}, ready[7]) },
	}

	var wg sync.WaitGroup
	for i, register := range registerers {
		c := codecs[i%len(codecs)]
		wg.Go(func() { register(c) })

		// Another goroutine uses the name once RegisterIn has returned in the first.
		wg.Go(func() {
			<-ready[i]
			for _, c := range codecs {
				v, err := r.Decode(c, names[i], []byte(`{"V":42}`))
				if err != nil {
					t.Errorf("Decode(%q) after RegisterIn returned: %v", names[i], err)
					continue
				}
				if name, err := r.NameOf(v); err != nil || name != names[i] {
					t.Errorf("NameOf(%T) = %q, %v; want %q", v, name, err, names[i])
				}
				if name, _, err := r.Encode(c, v); err != nil || name != names[i] {
					t.Errorf("Encode(%T) = %q, %v; want %q", v, name, err, names[i])
				}
			}
		})

		// Every goroutine also registers one shared pair, which is a no-op after the first.
		wg.Go(func() { codec.RegisterIn[encMsg](r, "conc.Shared") })
	}

	// Meanwhile, others encode and decode built-ins and check the whole registry.
	for i := range 4 {
		c := codecs[i%len(codecs)]
		wg.Go(func() {
			for n := range 100 {
				name, data, err := r.Encode(c, int64(n))
				if err != nil {
					t.Errorf("Encode(int64): %v", err)
					return
				}
				if v, err := r.Decode(c, name, data); err != nil || v != int64(n) {
					t.Errorf("Decode(%q) = %v, %v; want %d", name, v, err, n)
					return
				}
				if err := r.Check(c); err != nil {
					t.Errorf("Check(%s): %v", c.Name(), err)
					return
				}
				if names := r.Names(); !slices.IsSorted(names) {
					t.Errorf("Names = %q, not sorted", names)
					return
				}
			}
		})
	}
	wg.Wait()

	got := r.Names()
	for _, name := range append(slices.Clone(names), "conc.Shared") {
		if !slices.Contains(got, name) {
			t.Errorf("Names lacks %q", name)
		}
	}
	for _, c := range codecs {
		if err := r.Check(c); err != nil {
			t.Errorf("Check(%s) after the goroutines: %v", c.Name(), err)
		}
	}
}
