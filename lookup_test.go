// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unsafe"
)

// AccountMsg is the typed message of the lookup tests' actors (AT-ACT-23).
type AccountMsg struct{ Amount int }

// AT-ACT-22: ACT-510, ACT-520.
func TestATACT22LookupPaths(t *testing.T) {
	sys, _ := newManualSystem(t)
	if _, err := Spawn(sys, "a", idle[int]()); err != nil {
		t.Fatal(err)
	}
	gen, err := Spawn(sys, "", idle[int]())
	if err != nil {
		t.Fatal(err)
	}
	if got := gen.Address().Path(); got != "/user/$1" {
		t.Fatalf("generated path = %q, want /user/$1", got)
	}

	for _, path := range []string{"", "user/a", "/user/", "/user//a", "/user/$01", "/user/a b", "/", "/user/a/"} {
		pid, err := Lookup[int](sys, path)
		if !errors.Is(err, ErrInvalidName) || !pid.IsZero() {
			t.Errorf("Lookup(%q) = %v, %v; want the zero PID and ErrInvalidName", path, pid, err)
			continue
		}
		if want := "lockstep: invalid name: path " + strconv.Quote(path); err.Error() != want {
			t.Errorf("Lookup(%q) error %q, want %q", path, err, want)
		}
	}

	pid, err := Lookup[int](sys, "/user/zz")
	if !errors.Is(err, ErrNotFound) || !pid.IsZero() {
		t.Errorf("Lookup(/user/zz) = %v, %v; want the zero PID and ErrNotFound", pid, err)
	}
	if want := `lockstep: not found: path "/user/zz"`; err == nil || err.Error() != want {
		t.Errorf("Lookup(/user/zz) error %v, want %q", err, want)
	}
	// A well-formed path below a missing parent is not found, not invalid.
	if _, err := Lookup[int](sys, "/user/zz/b"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Lookup(/user/zz/b) error %v, want ErrNotFound", err)
	}

	got, err := Lookup[int](sys, "/user/$1")
	if err != nil || got != gen {
		t.Errorf("Lookup(/user/$1) = %v, %v; want %v", got, err, gen)
	}
	if got, err := Lookup[int](sys, "/user/a"); err != nil || got.Address().Path() != "/user/a" {
		t.Errorf("Lookup(/user/a) = %v, %v", got, err)
	}
}

// AT-ACT-23: ACT-530.
func TestATACT23LookupTypeCheck(t *testing.T) {
	sys, _ := newManualSystem(t)
	acct, err := Spawn(sys, "acct", idle[AccountMsg]())
	if err != nil {
		t.Fatal(err)
	}
	anyPID, err := Spawn(sys, "any", idle[any]())
	if err != nil {
		t.Fatal(err)
	}

	if got, err := Lookup[AccountMsg](sys, "/user/acct"); err != nil || got != acct {
		t.Errorf("Lookup[AccountMsg](/user/acct) = %v, %v; want %v", got, err, acct)
	}
	if got, err := Lookup[any](sys, "/user/any"); err != nil || got != anyPID {
		t.Errorf("Lookup[any](/user/any) = %v, %v; want %v", got, err, anyPID)
	}

	pid, err := Lookup[any](sys, "/user/acct")
	if !errors.Is(err, ErrWrongType) || !pid.IsZero() {
		t.Fatalf("Lookup[any](/user/acct) = %v, %v; want the zero PID and ErrWrongType", pid, err)
	}
	// reflect.Type.String prints the names for the test's package: the type is defined here.
	want := "lockstep: wrong message type: " + acct.String() + ": spawned as lockstep.AccountMsg, looked up as interface {}"
	if err.Error() != want {
		t.Errorf("error %q, want %q", err, want)
	}

	acctPID, err := Lookup[AccountMsg](sys, "/user/any")
	if !errors.Is(err, ErrWrongType) || !acctPID.IsZero() {
		t.Fatalf("Lookup[AccountMsg](/user/any) = %v, %v; want the zero PID and ErrWrongType", acctPID, err)
	}
	want = "lockstep: wrong message type: " + anyPID.String() + ": spawned as interface {}, looked up as lockstep.AccountMsg"
	if err.Error() != want {
		t.Errorf("error %q, want %q", err, want)
	}
}

// AT-ACT-03 (the Lookup and uid clauses): ACT-030, ACT-040, ACT-045. Stop and Watch of a guardian
// are tested with Watch and Stop.
func TestATACT03GuardiansLookup(t *testing.T) {
	sys, eng := newManualSystem(t)
	for _, path := range []string{"/user", "/system"} {
		if pid, err := Lookup[any](sys, path); !errors.Is(err, ErrNotFound) || !pid.IsZero() {
			t.Errorf("Lookup[any](%s) = %v, %v; want the zero PID and ErrNotFound", path, pid, err)
		}
	}

	var parent Address
	pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
		started: func(ctx *Context[int]) { parent = ctx.Parent() },
	}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	epoch := uint64(sys.Incarnation())
	if parent.Path() != "/user" || parent.UID() != epoch<<32|1 {
		t.Errorf("Parent() = %v, want path /user and uid epoch<<32|1 = %#x", parent, epoch<<32|1)
	}
	if got := pid.Address().UID() >> 32; got != epoch {
		t.Errorf("the actor's uid epoch = %d, want sys.Incarnation() = %d", got, epoch)
	}
	if got, want := pid.Address().UID(), epoch<<32|3; got != want {
		t.Errorf("the first spawned actor's uid = %#x, want epoch<<32|3 = %#x", got, want)
	}
}

// AT-ACT-20: ACT-400, ACT-410, ACT-430.
func TestATACT20AddressAndPIDValues(t *testing.T) {
	sys, _ := newManualSystem(t)
	pid, err := Spawn(sys, "a", idle[int]())
	if err != nil {
		t.Fatal(err)
	}
	pid2, err := Lookup[int](sys, "/user/a")
	if err != nil {
		t.Fatal(err)
	}

	want := "lockstep://local/user/a#" + strconv.FormatUint(pid.Address().UID(), 16)
	if got := pid.Address().String(); got != want {
		t.Errorf("Address().String() = %q, want %q", got, want)
	}
	if got := pid.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if got := (Address{}).String(); got != "<zero>" {
		t.Errorf("Address{}.String() = %q, want <zero>", got)
	}
	if !(PID[int]{}).IsZero() {
		t.Error("PID[int]{}.IsZero() = false")
	}
	if pid.IsZero() {
		t.Error("a spawned PID is zero")
	}
	if pid != pid2 {
		t.Errorf("Lookup PID %v != Spawn PID %v", pid2, pid)
	}
	if got := map[PID[int]]int{pid: 1}[pid2]; got != 1 {
		t.Errorf("map[PID[int]]int{pid: 1}[pid2] = %d, want 1", got)
	}
	if a, b := unsafe.Sizeof(PID[int]{}), unsafe.Sizeof(Address{}); a != b {
		t.Errorf("unsafe.Sizeof(PID[int]{}) = %d, want unsafe.Sizeof(Address{}) = %d", a, b)
	}
}

// AT-ACT-20's last clause: the API snapshot lists no exported function or method that takes an
// Address and returns a PID, or converts between PID types (ACT-430).
func TestNoAddressToPIDConversion(t *testing.T) {
	data, err := os.ReadFile("api/lockstep.txt")
	if err != nil {
		t.Fatal(err)
	}
	fromAddress := regexp.MustCompile(`\([^)]*\bAddress\b[^)]*\) \(?PID\b`)
	pidToPID := regexp.MustCompile(`\([^)]*\bPID\[[^)]*\) \(?PID\[`)
	seen := false
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, "func Lookup[") {
			seen = true
		}
		if fromAddress.MatchString(line) || pidToPID.MatchString(line) {
			t.Errorf("the snapshot lists a conversion: %s", line)
		}
	}
	if !seen {
		t.Error("the snapshot does not list Lookup (regenerate it with -update)")
	}
}

// ACT §7: a nil *System passed to Lookup is a programming error.
func TestLookupNilSystem(t *testing.T) {
	v := mustPanic(t, func() { _, _ = Lookup[int](nil, "/user/a") })
	if want := "lockstep: Lookup with nil System"; v != want {
		t.Errorf("Lookup(nil, ...) panicked with %v, want %q", v, want)
	}
	// The nil check comes before the path check.
	v = mustPanic(t, func() { _, _ = Lookup[int](nil, "bad") })
	if want := "lockstep: Lookup with nil System"; v != want {
		t.Errorf("Lookup(nil, bad path) panicked with %v, want %q", v, want)
	}
}
