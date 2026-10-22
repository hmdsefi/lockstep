// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// mustPanic calls f and returns the value it panicked with, failing the test if f returns.
func mustPanic(t *testing.T, f func()) (v any) {
	t.Helper()
	defer func() { v = recover() }()
	f()
	t.Fatal("no panic")
	return nil
}

// AT-ENG-01's field sets, ACT-410, ENG-020.
func TestAddressStringMatchesEngine(t *testing.T) {
	cases := []struct {
		e    engine.Address
		want string
	}{
		{engine.Address{Node: "n1", Path: "/user/orders/o-17", UID: 42}, "lockstep://n1/user/orders/o-17#2a"},
		{engine.Address{}, "<zero>"},
		{engine.Address{Node: "local", Path: "/"}, "lockstep://local/#0"},
		{engine.Address{Node: "a", Path: "/system", UID: 1<<64 - 1}, "lockstep://a/system#ffffffffffffffff"},
		{engine.Address{Node: "local", Path: "/user/a"}, "lockstep://local/user/a#0"},
	}
	for _, c := range cases {
		a := addressOf(c.e)
		if got := a.String(); got != c.e.String() || got != c.want {
			t.Errorf("Address(%+v).String() = %q, engine %q, want %q", c.e, got, c.e.String(), c.want)
		}
		if got := (PID[int]{addr: a}).String(); got != c.want {
			t.Errorf("PID.String() = %q, want %q", got, c.want)
		}
		if a.Node() != c.e.Node || a.Path() != c.e.Path || a.UID() != c.e.UID {
			t.Errorf("accessors of %+v = %q, %q, %d", c.e, a.Node(), a.Path(), a.UID())
		}
		if back := a.engine(); back != c.e {
			t.Errorf("engine() = %+v, want %+v", back, c.e)
		}
		if a.IsZero() != (c.e == engine.Address{}) {
			t.Errorf("IsZero of %+v = %v", c.e, a.IsZero())
		}
	}
	if (Address{}).String() != "<zero>" {
		t.Errorf(`Address{}.String() = %q`, Address{}.String())
	}
}

// AT-ACT-20 value clauses, ACT-400.
func TestPIDValues(t *testing.T) {
	if !(PID[int]{}).IsZero() {
		t.Error("PID[int]{} is not zero")
	}
	a := Address{node: "local", path: "/user/a", uid: 7}
	p1, p2 := PID[int]{addr: a}, PID[int]{addr: Address{node: "local", path: "/user/a", uid: 7}}
	if p1 != p2 {
		t.Error("PIDs of equal addresses differ")
	}
	if p1.IsZero() {
		t.Error("non-zero PID reports zero")
	}
	if got := map[PID[int]]int{p1: 1}[p2]; got != 1 {
		t.Errorf("map lookup by an equal PID = %d, want 1", got)
	}
	if p1.Address() != a {
		t.Errorf("Address() = %v, want %v", p1.Address(), a)
	}
	if p1.String() != a.String() || p1.String() != "lockstep://local/user/a#7" {
		t.Errorf("String() = %q", p1.String())
	}
	if (PID[int]{addr: Address{node: "local", path: "/user/a", uid: 8}}) == p1 {
		t.Error("PIDs of different uids are equal")
	}
	if unsafe.Sizeof(PID[int]{}) != unsafe.Sizeof(Address{}) {
		t.Errorf("sizeof PID = %d, sizeof Address = %d", unsafe.Sizeof(PID[int]{}), unsafe.Sizeof(Address{}))
	}
}

// AT-ACT-34 value and panic clauses, ACT-440.
func TestPIDAtValues(t *testing.T) {
	p := PIDAt[int]("local", "/user/a")
	if p.Address().UID() != 0 {
		t.Errorf("uid = %d, want 0", p.Address().UID())
	}
	if p.String() != "lockstep://local/user/a#0" {
		t.Errorf("String() = %q", p.String())
	}
	if p.IsZero() {
		t.Error("PIDAt result is zero")
	}
	if p.Address().Node() != "local" || p.Address().Path() != "/user/a" {
		t.Errorf("address = %v", p.Address())
	}
	if p != PIDAt[int]("local", "/user/a") {
		t.Error("equal path addresses differ")
	}
	if g := PIDAt[string]("n2", "/user/$1").String(); g != "lockstep://n2/user/$1#0" {
		t.Errorf("generated path: %q", g)
	}

	cases := []struct {
		name       string
		node, path string
		want       string
	}{
		{"node", "-x", "/user/a", `lockstep: PIDAt with invalid node "-x"`},
		{"path", "local", "user/a", `lockstep: PIDAt with invalid path "user/a"`},
		{"empty node", "", "/user/a", `lockstep: PIDAt with invalid node ""`},
		{"node checked first", "-x", "user/a", `lockstep: PIDAt with invalid node "-x"`},
		{"trailing slash", "local", "/user/", `lockstep: PIDAt with invalid path "/user/"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mustPanic(t, func() { PIDAt[int](c.node, c.path) })
			if s, ok := got.(string); !ok || s != c.want {
				t.Errorf("panic = %#v, want %q", got, c.want)
			}
		})
	}
}

func TestAddressOfRoundTrip(t *testing.T) {
	e := engine.Address{Node: "n1", Path: strings.Repeat("/a", 3), UID: 1<<32 | 3}
	if got := addressOf(e).engine(); got != e {
		t.Errorf("round trip = %+v, want %+v", got, e)
	}
}
