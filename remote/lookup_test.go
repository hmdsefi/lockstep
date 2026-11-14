// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/remote"
)

type ping struct{ N int }

type pingActor struct{}

func (pingActor) Receive(*lockstep.Context[ping], ping) {}

func newSystem(t *testing.T) *lockstep.System {
	t.Helper()
	sys := lockstep.New(lockstep.WithNodeName("a"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sys.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return sys
}

// TestLookupLocalNode checks that Lookup on the system's own node is lockstep.Lookup: the PID of
// the actor with its uid, and lockstep.Lookup's errors.
func TestLookupLocalNode(t *testing.T) {
	sys := newSystem(t)
	pid, err := lockstep.Spawn(sys, "p", func() lockstep.Actor[ping] { return pingActor{} })
	if err != nil {
		t.Fatal(err)
	}
	got, err := remote.Lookup[ping](context.Background(), sys, "a", "/user/p")
	if err != nil || got != pid || got.Address().UID() == 0 {
		t.Errorf("Lookup = %v, %v; want %v", got, err, pid)
	}
	if _, err := remote.Lookup[string](context.Background(), sys, "a", "/user/p"); !errors.Is(err, lockstep.ErrWrongType) {
		t.Errorf("Lookup of the wrong type: %v, want ErrWrongType", err)
	}
	if _, err := remote.Lookup[ping](context.Background(), sys, "a", "/user/none"); !errors.Is(err, lockstep.ErrNotFound) {
		t.Errorf("Lookup of a missing path: %v, want ErrNotFound", err)
	}
	if _, err := remote.Lookup[ping](context.Background(), sys, "a", "user/p"); !errors.Is(err, lockstep.ErrInvalidName) {
		t.Errorf("Lookup of a malformed path: %v, want ErrInvalidName", err)
	}
}

// TestLookupNotEnabled checks Lookup on another node of a system without remoting, and the names
// it refuses first.
func TestLookupNotEnabled(t *testing.T) {
	sys := newSystem(t)
	_, err := remote.Lookup[ping](context.Background(), sys, "b", "/user/p")
	if !errors.Is(err, remote.ErrUnreachable) || !strings.Contains(err.Error(), `remoting is not enabled on node "a"`) {
		t.Errorf("Lookup without remoting: %v, want ErrUnreachable saying remoting is not enabled", err)
	}
	for _, tc := range []struct{ node, path string }{{"B!", "/user/p"}, {"b", "user/p"}, {"b", "/user/p#1"}, {"b", ""}} {
		if _, err := remote.Lookup[ping](context.Background(), sys, tc.node, tc.path); !errors.Is(err, lockstep.ErrInvalidName) {
			t.Errorf("Lookup(%q, %q) = %v, want ErrInvalidName", tc.node, tc.path, err)
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("Lookup with a nil System did not panic")
		}
	}()
	_, _ = remote.Lookup[ping](context.Background(), nil, "b", "/user/p")
}
