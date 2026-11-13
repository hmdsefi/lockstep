// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec_test

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
)

type (
	ping       struct{ Seq int }
	namedPing  ping
	pong       struct{ Seq int }
	celsius    float64
	unknown    struct{}
	box[T any] struct{ V T }
	// defaultOnly is registered in the process registry only.
	defaultOnly struct{ N int }
)

// panicText runs f and returns the text of its panic, or "" if it returned normally.
func panicText(t *testing.T, f func()) (text string) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			s, ok := p.(string)
			if !ok {
				t.Fatalf("panic value %#v has type %T, want string", p, p)
			}
			text = s
		}
	}()
	f()
	return ""
}

// wantName checks that r gives v the wire name want and that want leads back to v's type.
func wantName(t *testing.T, r *codec.Registry, v any, want string) {
	t.Helper()
	got, err := r.NameOf(v)
	if err != nil || got != want {
		t.Errorf("NameOf(%T) = %q, %v; want %q, nil", v, got, err, want)
	}
	typ, err := r.TypeOf(want)
	if err != nil || typ != reflect.TypeOf(v) {
		t.Errorf("TypeOf(%q) = %v, %v; want %T, nil", want, typ, err, v)
	}
}

// wantNotRegistered checks that err wraps ErrNotRegistered and reads want.
func wantNotRegistered(t *testing.T, what string, err error, want string) {
	t.Helper()
	if !errors.Is(err, codec.ErrNotRegistered) || err.Error() != want {
		t.Errorf("%s: error %v; want %q wrapping ErrNotRegistered", what, err, want)
	}
}

// AT-COD-01, COD-050: registration round trips through NameOf and TypeOf; T, *T and a named type
// over T are distinct; nil and unregistered types wrap ErrNotRegistered.
func TestATCOD01Register(t *testing.T) {
	r := codec.NewRegistry()
	codec.RegisterIn[ping](r, "test.Ping")
	codec.RegisterIn[*ping](r, "test/PingPtr")
	codec.RegisterIn[namedPing](r, "test.Named_Ping-2")
	codec.RegisterIn[box[int]](r, "test.BoxInt")

	wantName(t, r, ping{Seq: 1}, "test.Ping")
	wantName(t, r, &ping{Seq: 1}, "test/PingPtr")
	wantName(t, r, namedPing{Seq: 1}, "test.Named_Ping-2")
	wantName(t, r, box[int]{V: 1}, "test.BoxInt")
	wantName(t, r, (*ping)(nil), "test/PingPtr") // a typed nil pointer has its pointer type's name

	// A type with the same structure is still another type, as is a named type over a built-in.
	_, err := r.NameOf(pong{})
	wantNotRegistered(t, "NameOf(pong)", err, "codec: type not registered: codec_test.pong")
	_, err = r.NameOf(celsius(21))
	wantNotRegistered(t, "NameOf(celsius)", err, "codec: type not registered: codec_test.celsius")
	_, err = r.NameOf(box[string]{}) // each instantiation of a generic type is its own type
	wantNotRegistered(t, "NameOf(box[string])", err, "codec: type not registered: codec_test.box[string]")
	_, err = r.NameOf(&pong{})
	wantNotRegistered(t, "NameOf(*pong)", err, "codec: type not registered: *codec_test.pong")

	name, err := r.NameOf(nil)
	if name != "" {
		t.Errorf("NameOf(nil) name = %q, want empty", name)
	}
	wantNotRegistered(t, "NameOf(nil)", err, "codec: type not registered: <nil>")

	typ, err := r.TypeOf("test.Missing")
	if typ != nil {
		t.Errorf("TypeOf(missing) type = %v, want nil", typ)
	}
	wantNotRegistered(t, "TypeOf(missing)", err, `codec: type not registered: name "test.Missing"`)
	_, err = r.TypeOf("Test.Ping") // names are case-sensitive
	wantNotRegistered(t, "TypeOf(Test.Ping)", err, `codec: type not registered: name "Test.Ping"`)

	// Register adds to the process registry only; a private registry does not see it, and the
	// process registry does not see a private registration.
	codec.Register[defaultOnly]("test.DefaultOnly")
	codec.Register[defaultOnly]("test.DefaultOnly") // the same pair again: no-op, also on -count=2
	wantName(t, codec.Default(), defaultOnly{}, "test.DefaultOnly")
	if d := codec.Default(); d != codec.Default() {
		t.Error("Default returned two registries")
	}
	_, err = r.NameOf(defaultOnly{})
	wantNotRegistered(t, "private NameOf(defaultOnly)", err,
		"codec: type not registered: codec_test.defaultOnly")
	_, err = codec.Default().TypeOf("test.Ping")
	wantNotRegistered(t, "Default TypeOf(test.Ping)", err, `codec: type not registered: name "test.Ping"`)
	_, err = codec.NewRegistry().TypeOf("test.DefaultOnly")
	wantNotRegistered(t, "new TypeOf(test.DefaultOnly)", err,
		`codec: type not registered: name "test.DefaultOnly"`)
}

// AT-COD-02, COD-020, COD-030: RegisterIn panics in table order with the exact texts, and
// registering the same pair again is a no-op.
func TestATCOD02Panics(t *testing.T) {
	// setup registers ping as "a" and pong as "b" in a new registry.
	setup := func() *codec.Registry {
		r := codec.NewRegistry()
		codec.RegisterIn[ping](r, "a")
		codec.RegisterIn[pong](r, "b")
		return r
	}
	cases := []struct {
		name string
		f    func(r *codec.Registry)
		want string
	}{
		// Each row, then a call that also breaks every later row, to show the order.
		{"1 nil registry", func(*codec.Registry) { codec.RegisterIn[ping](nil, "c") },
			"codec: RegisterIn with nil registry"},
		{"1 before 2", func(*codec.Registry) { codec.RegisterIn[error](nil, "") },
			"codec: RegisterIn with nil registry"},
		// A zero Registry, checked right after row 1.
		{"zero registry", func(*codec.Registry) { codec.RegisterIn[ping](&codec.Registry{}, "c") },
			"codec: zero Registry; use NewRegistry"},
		{"zero registry before 2", func(*codec.Registry) { codec.RegisterIn[error](&codec.Registry{}, "") },
			"codec: zero Registry; use NewRegistry"},
		{"2 invalid name", func(r *codec.Registry) { codec.RegisterIn[unknown](r, "1x") },
			`codec: invalid wire name "1x"`},
		{"2 before 4", func(r *codec.Registry) { codec.RegisterIn[error](r, "x y") },
			`codec: invalid wire name "x y"`},
		{"3 reserved", func(r *codec.Registry) { codec.RegisterIn[unknown](r, "lockstep.x") },
			`codec: wire name "lockstep.x" is reserved`},
		{"3 before 4 and 5", func(r *codec.Registry) { codec.RegisterIn[any](r, "lockstep.int64") },
			`codec: wire name "lockstep.int64" is reserved`},
		{"3 before the same-pair no-op", func(r *codec.Registry) { codec.RegisterIn[int64](r, "lockstep.int64") },
			`codec: wire name "lockstep.int64" is reserved`},
		{"4 interface type", func(r *codec.Registry) { codec.RegisterIn[fmt.Stringer](r, "c") },
			"codec: cannot register interface type fmt.Stringer"},
		{"4 before 5", func(r *codec.Registry) { codec.RegisterIn[any](r, "a") },
			"codec: cannot register interface type interface {}"},
		{"5 name held", func(r *codec.Registry) { codec.RegisterIn[unknown](r, "a") },
			`codec: wire name "a" already registered for codec_test.ping`},
		{"5 before 6", func(r *codec.Registry) { codec.RegisterIn[pong](r, "a") },
			`codec: wire name "a" already registered for codec_test.ping`},
		{"6 type held", func(r *codec.Registry) { codec.RegisterIn[ping](r, "c") },
			`codec: type codec_test.ping already registered as "a"`},
		{"6 built-in type", func(r *codec.Registry) { codec.RegisterIn[int64](r, "c") },
			`codec: type int64 already registered as "lockstep.int64"`},
		{"Register uses the same texts", func(*codec.Registry) { codec.Register[unknown]("") },
			`codec: invalid wire name ""`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := setup()
			before := r.Names()
			if got := panicText(t, func() { tc.f(r) }); got != tc.want {
				t.Errorf("panic %q, want %q", got, tc.want)
			}
			// A panic changes nothing and leaves the registry usable.
			if after := r.Names(); !slices.Equal(after, before) {
				t.Errorf("Names after the panic = %q, want %q", after, before)
			}
			codec.RegisterIn[unknown](r, "c")
			wantName(t, r, unknown{}, "c")
		})
	}

	// Every lookup on a zero Registry panics too, rather than report the built-ins missing.
	for _, tc := range []struct {
		what string
		f    func(r *codec.Registry)
	}{
		{"NameOf", func(r *codec.Registry) { _, _ = r.NameOf(int64(1)) }},
		{"TypeOf", func(r *codec.Registry) { _, _ = r.TypeOf("lockstep.int64") }},
		{"Names", func(r *codec.Registry) { _ = r.Names() }},
	} {
		const want = "codec: zero Registry; use NewRegistry"
		if got := panicText(t, func() { tc.f(&codec.Registry{}) }); got != want {
			t.Errorf("zero Registry %s: panic %q, want %q", tc.what, got, want)
		}
	}

	// COD-030: the same pair again is a no-op.
	r := setup()
	before := r.Names()
	if got := panicText(t, func() { codec.RegisterIn[ping](r, "a") }); got != "" {
		t.Fatalf("registering the same pair again panicked: %q", got)
	}
	if after := r.Names(); !slices.Equal(after, before) {
		t.Errorf("Names after the same pair again = %q, want %q", after, before)
	}
	wantName(t, r, ping{}, "a")
}

// AT-COD-03, COD-010: the wire-name grammar at its edges, and the reserved prefix.
func TestATCOD03Names(t *testing.T) {
	const reserved = "reserved"
	cases := []struct {
		name string
		want string // "" when valid, "reserved", or "invalid"
	}{
		// Length: 0, 1, 255 and 256 bytes.
		{"", "invalid"},
		{"a", ""},
		{"Z", ""},
		{strings.Repeat("a", 255), ""},
		{strings.Repeat("a", 256), "invalid"},
		{"a" + strings.Repeat("-", 254), ""},
		// The first byte: an ASCII letter only.
		{"0a", "invalid"},
		{".a", "invalid"},
		{"_a", "invalid"},
		{"/a", "invalid"},
		{"-a", "invalid"},
		{" a", "invalid"},
		{"@a", "invalid"}, // the neighbours of 'A'..'Z' and 'a'..'z'
		{"[a", "invalid"},
		{"`a", "invalid"},
		{"{a", "invalid"},
		{"éa", "invalid"},
		// Later bytes: every class that is allowed, at both ends of each range.
		{"aazAZ09._/-", ""},
		{"a.b", ""},
		{"a_b", ""},
		{"a/b", ""},
		{"a-b", ""},
		{"a0", ""},
		{"a9", ""},
		// Later bytes that are not allowed, including the neighbours of each allowed range.
		{"a b", "invalid"},
		{"a:b", "invalid"},
		{"a+b", "invalid"},
		{"a,b", "invalid"},
		{"a@b", "invalid"},
		{"a[b", "invalid"},
		{"a`b", "invalid"},
		{"a{b", "invalid"},
		{"a\\b", "invalid"},
		{"a\x00", "invalid"},
		{"a\x7f", "invalid"},
		{"aé", "invalid"},
		{"a\n", "invalid"},
		// The reserved prefix: exactly "lockstep.", compared byte for byte.
		{"lockstep.x", reserved},
		{"lockstep.", reserved},
		{"lockstep.cluster.Join", reserved},
		{"lockstep", ""},
		{"lockstepx", ""},
		{"lockstep_x", ""},
		{"Lockstep.x", ""},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%.20q", tc.name), func(t *testing.T) {
			r := codec.NewRegistry()
			got := panicText(t, func() { codec.RegisterIn[ping](r, tc.name) })
			switch tc.want {
			case "":
				if got != "" {
					t.Fatalf("panic %q, want none", got)
				}
				wantName(t, r, ping{}, tc.name)
			case reserved:
				if want := fmt.Sprintf("codec: wire name %q is reserved", tc.name); got != want {
					t.Errorf("panic %q, want %q", got, want)
				}
			default:
				if want := fmt.Sprintf("codec: invalid wire name %q", tc.name); got != want {
					t.Errorf("panic %q, want %q", got, want)
				}
			}
		})
	}
}

// AT-COD-04 (the Names part), COD-040, COD-085: every registry holds the 19 built-ins under their
// names; Names is sorted and a new copy on each call.
func TestATCOD04Names(t *testing.T) {
	builtins := []struct {
		v    any
		name string
	}{
		{true, "lockstep.bool"},
		{"s", "lockstep.string"},
		{int(1), "lockstep.int"},
		{int8(1), "lockstep.int8"},
		{int16(1), "lockstep.int16"},
		{rune(1), "lockstep.int32"},
		{int64(1), "lockstep.int64"},
		{uint(1), "lockstep.uint"},
		{byte(1), "lockstep.uint8"},
		{uint16(1), "lockstep.uint16"},
		{uint32(1), "lockstep.uint32"},
		{uint64(1), "lockstep.uint64"},
		{float32(1), "lockstep.float32"},
		{float64(1), "lockstep.float64"},
		{[]byte("b"), "lockstep.bytes"},
		{struct{}{}, "lockstep.empty"},
		{time.Second, "lockstep.duration"},
		{time.Unix(0, 0), "lockstep.time"},
		{lockstep.Address{}, "lockstep.address"},
	}
	if len(builtins) != 19 {
		t.Fatalf("the test lists %d built-ins, want 19", len(builtins))
	}
	var want []string
	for _, b := range builtins {
		want = append(want, b.name)
	}
	slices.Sort(want)

	for _, reg := range []struct {
		what string
		r    *codec.Registry
	}{{"NewRegistry", codec.NewRegistry()}, {"Default", codec.Default()}} {
		t.Run(reg.what, func(t *testing.T) {
			for _, b := range builtins {
				wantName(t, reg.r, b.v, b.name)
			}
			names := reg.r.Names()
			if !slices.IsSorted(names) {
				t.Errorf("Names = %q, not sorted", names)
			}
			// The process registry may hold types other tests registered.
			if reg.what == "NewRegistry" && !slices.Equal(names, want) {
				t.Errorf("Names = %q, want %q", names, want)
			}
			for _, name := range want {
				if !slices.Contains(names, name) {
					t.Errorf("Names lacks %q", name)
				}
			}
		})
	}

	// Pointers to built-ins and other slices are not built-ins.
	r := codec.NewRegistry()
	for _, v := range []any{new(int64), []int{1}, []string{"s"}, map[string]int{}, struct{ A int }{}} {
		if name, err := r.NameOf(v); !errors.Is(err, codec.ErrNotRegistered) {
			t.Errorf("NameOf(%T) = %q, %v; want ErrNotRegistered", v, name, err)
		}
	}

	// Names stays sorted as types are added, and each call returns a new slice.
	codec.RegisterIn[ping](r, "zz.Ping")
	codec.RegisterIn[pong](r, "A.Pong") // uppercase sorts before lowercase, byte by byte
	codec.RegisterIn[namedPing](r, "lockstepx")
	names := r.Names()
	wantAll := slices.Sorted(slices.Values(append(slices.Clone(want), "zz.Ping", "A.Pong", "lockstepx")))
	if !slices.Equal(names, wantAll) {
		t.Errorf("Names = %q, want %q", names, wantAll)
	}
	if names[0] != "A.Pong" {
		t.Errorf("Names[0] = %q, want %q", names[0], "A.Pong")
	}
	names[0] = "changed"
	if again := r.Names(); !slices.Equal(again, wantAll) {
		t.Errorf("Names after changing an earlier result = %q, want %q", again, wantAll)
	}
}
