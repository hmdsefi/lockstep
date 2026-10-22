// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"reflect"
)

// Lookup returns the PID of the actor registered at path on sys, if it was spawned with exactly
// message type M. It finds an actor whose stop has begun and none whose stop has completed. It
// returns ErrInvalidName if path is not well-formed, ErrNotFound if no actor is registered there
// (the guardians /user and /system are not), and ErrWrongType if the actor's message type is not
// M. Lookup panics if sys is nil.
func Lookup[M any](sys *System, path string) (PID[M], error) {
	if sys == nil {
		panic("lockstep: Lookup with nil System")
	}
	if !validPath(path) {
		return PID[M]{}, fmt.Errorf("%w: path %q", ErrInvalidName, path)
	}
	uid, a, ok := sys.reg.Lookup(path)
	if !ok {
		return PID[M]{}, fmt.Errorf("%w: path %q", ErrNotFound, path)
	}
	// The registry stores the cell as an any; the assertion to *cell[M] is the exact type identity
	// of ACT-530 (ACT §11). The address needs no read of the cell: its node is the system's and
	// its path and uid are the registry's.
	addr := Address{node: sys.node, path: path, uid: uid}
	if _, ok := a.(*cell[M]); !ok {
		spawned := "unknown"
		if c, ok := a.(actorCell); ok {
			spawned = c.msgType().String()
		}
		return PID[M]{}, fmt.Errorf("%w: %s: spawned as %s, looked up as %s",
			ErrWrongType, addr, spawned, reflect.TypeFor[M]().String())
	}
	return PID[M]{addr: addr}, nil
}
