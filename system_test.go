// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// mustPanicError calls f, which must panic with an error value, and returns that error.
func mustPanicError(t *testing.T, f func()) error {
	t.Helper()
	v := mustPanic(t, f)
	err, ok := v.(error)
	if !ok {
		t.Fatalf("panic value %v has type %T, want error", v, v)
	}
	return err
}

// AT-ACT-02: ACT-010, ACT-020, ACT-045.
func TestATACT02NodeNameAndInvalidOptions(t *testing.T) {
	t.Run("custom node name", func(t *testing.T) {
		sys := newLocalSystem(t, WithNodeName("n-1.x"))
		if got := sys.Node(); got != "n-1.x" {
			t.Errorf("Node() = %q, want n-1.x", got)
		}
		if got := sys.user.Node(); got != "n-1.x" {
			t.Errorf("user guardian node %q, want n-1.x", got)
		}
		if got := sys.system.Node(); got != "n-1.x" {
			t.Errorf("system guardian node %q, want n-1.x", got)
		}
	})
	t.Run("default node name", func(t *testing.T) {
		if got := newLocalSystem(t).Node(); got != "local" {
			t.Errorf("Node() = %q, want local", got)
		}
	})
	for _, name := range []string{"", "-a", "a/b", strings.Repeat("a", 65)} {
		t.Run("invalid "+name, func(t *testing.T) {
			err := mustPanicError(t, func() { New(WithNodeName(name)) })
			if !errors.Is(err, ErrInvalidOption) {
				t.Errorf("errors.Is(%v, ErrInvalidOption) is false", err)
			}
			want := `lockstep: invalid option: node name "` + name +
				`": want 1-64 characters [A-Za-z0-9._-], starting with a letter or digit`
			if err.Error() != want {
				t.Errorf("message %q, want %q", err.Error(), want)
			}
		})
	}
	t.Run("a 64-byte name is valid", func(t *testing.T) {
		name := strings.Repeat("a", 64)
		if got := newLocalSystem(t, WithNodeName(name)).Node(); got != name {
			t.Errorf("Node() = %q, want %q", got, name)
		}
	})
	t.Run("the last WithNodeName wins", func(t *testing.T) {
		if got := newLocalSystem(t, WithNodeName("a"), WithNodeName("b")).Node(); got != "b" {
			t.Errorf("Node() = %q, want b", got)
		}
	})
	t.Run("an invalid name that a later option replaces is no error", func(t *testing.T) {
		if got := newLocalSystem(t, WithNodeName(""), WithNodeName("b")).Node(); got != "b" {
			t.Errorf("Node() = %q, want b", got)
		}
	})
	t.Run("nil Option", func(t *testing.T) {
		err := mustPanicError(t, func() { New(nil) })
		if !errors.Is(err, ErrInvalidOption) {
			t.Errorf("errors.Is(%v, ErrInvalidOption) is false", err)
		}
		if want := "lockstep: invalid option: nil Option"; err.Error() != want {
			t.Errorf("message %q, want %q", err.Error(), want)
		}
	})
	t.Run("the first invalid setting in argument order is reported", func(t *testing.T) {
		err := mustPanicError(t, func() { New(WithNodeName(""), nil) })
		if !strings.Contains(err.Error(), "node name") {
			t.Errorf("New(WithNodeName(\"\"), nil) panicked with %q, want the node name error", err)
		}
		err = mustPanicError(t, func() { New(nil, WithNodeName("")) })
		if err.Error() != "lockstep: invalid option: nil Option" {
			t.Errorf("New(nil, WithNodeName(\"\")) panicked with %q, want the nil Option error", err)
		}
	})
}

// ACT-015, ACT-020: an engine's node name wins and an option's name is neither validated nor an
// error.
func TestNewSystemOnEngineIgnoresNodeName(t *testing.T) {
	sys, _ := newManualSystem(t)
	if sys.Node() != "local" {
		t.Errorf("Node() = %q, want local", sys.Node())
	}
	b := engineBundle(t, "n7")
	for _, name := range []string{"other", "-a", ""} {
		got, err := newSystem(b, []Option{WithNodeName(name)})
		if err != nil {
			t.Fatalf("newSystem with WithNodeName(%q): %v", name, err)
		}
		if got.Node() != "n7" {
			t.Errorf("WithNodeName(%q): Node() = %q, want n7", name, got.Node())
		}
		b = engineBundle(t, "n7") // Bind runs once per bundle
	}
	if _, err := newSystem(b, []Option{nil}); !errors.Is(err, ErrInvalidOption) {
		t.Errorf("newSystem with a nil Option: err = %v, want ErrInvalidOption", err)
	}
}

// ACT-200: *System is a Spawner and a Sender.
func TestSystemIsSpawnerAndSender(t *testing.T) {
	var _ Spawner = (*System)(nil)
	var _ Sender = (*System)(nil)
}

// ACT-030, ACT-040: guardians are created in order and are registry roots, not registered
// entries. The rest of AT-ACT-03 is in 1.2-05, 1.2-10 and 1.2-11.
func TestGuardianUIDs(t *testing.T) {
	check := func(t *testing.T, sys *System) {
		t.Helper()
		epoch := uint64(sys.Incarnation())
		if epoch == 0 {
			t.Fatal("Incarnation() is 0")
		}
		if got, want := sys.user, (Address{node: sys.Node(), path: "/user", uid: epoch<<32 | 1}); got != want {
			t.Errorf("user guardian %v, want %v", got, want)
		}
		if got, want := sys.system, (Address{node: sys.Node(), path: "/system", uid: epoch<<32 | 2}); got != want {
			t.Errorf("system guardian %v, want %v", got, want)
		}
		if got := sys.reg.Len(); got != 0 {
			t.Errorf("registry holds %d entries, want 0: guardians are not registered", got)
		}
		for _, p := range []string{"/user", "/system"} {
			if _, _, ok := sys.reg.Lookup(p); ok {
				t.Errorf("Lookup(%q) found a guardian", p)
			}
			// A declared root accepts children; Add panics under an undeclared parent.
			if _, ok := sys.reg.Add(p, "x", func() uint64 { return 7 }, nil); !ok {
				t.Errorf("Add under %q failed: guardian is not a registry root", p)
			}
		}
	}
	t.Run("manual", func(t *testing.T) {
		sys, _ := newManualSystem(t)
		check(t, sys)
		if sys.Incarnation() != 1 {
			t.Errorf("Incarnation() = %d, want 1", sys.Incarnation())
		}
	})
	t.Run("local", func(t *testing.T) { check(t, newLocalSystem(t)) })
}

// ACT-045: Node and Incarnation come from the engine bundle.
func TestNodeAndIncarnationFromEngine(t *testing.T) {
	b := engineBundle(t, "n7")
	b.Epoch = 9
	sys, err := newSystem(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sys.Node() != "n7" || sys.Incarnation() != 9 {
		t.Errorf("Node, Incarnation = %q, %d; want n7, 9", sys.Node(), sys.Incarnation())
	}
	if want := uint64(9)<<32 | 2; sys.system.UID() != want {
		t.Errorf("system guardian uid %#x, want %#x", sys.system.UID(), want)
	}
}

// ACT-010: New emits no record.
func TestNewEmitsNoRecord(t *testing.T) {
	_, rec := newRecordingSystem(t)
	if got := rec.Records(); len(got) != 0 {
		t.Errorf("construction emitted %d records, want 0: %v", len(got), got)
	}
}

// ENG-125: the system binds admission exactly once, while it is built.
func TestBindOnceWithNoopCallbacks(t *testing.T) {
	_, eng := newManualSystem(t)
	if got := eng.Usage(); got != (engine.Usage{}) {
		t.Errorf("Usage() = %+v, want zero", got)
	}
	// A change before OVL (plan 1.3) does nothing and does not panic.
	eng.ChangeAdmission(engine.AdmissionChange{Source: engine.SourcePressure, On: true, Shedding: true})
}

// host.Inbound drops every envelope until 1.2-06.
func TestInboundDropsEverything(t *testing.T) {
	sys, _ := newManualSystem(t)
	var h engine.Host = host{sys}
	h.Inbound(engine.Envelope{To: sys.user.engine(), Payload: 1})
	h.Inbound(engine.Envelope{})
}

// engineBundle returns a fresh manual engine's bundle for node.
func engineBundle(t *testing.T, node string) engine.Engine {
	t.Helper()
	return enginetest.New(node, 1).Engine()
}
