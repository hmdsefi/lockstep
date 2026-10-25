// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Address is the untyped address of one actor: node, path and uid. It is comparable; two
// addresses are equal when all three parts are equal. The zero Address names no actor.
type Address struct {
	node string
	path string
	uid  uint64
}

// Node returns the node name.
func (a Address) Node() string { return a.node }

// Path returns the absolute path, for example "/user/orders/o-17".
func (a Address) Path() string { return a.path }

// UID returns the uid.
func (a Address) UID() uint64 { return a.uid }

// IsZero reports whether a is the zero Address.
func (a Address) IsZero() bool { return a == Address{} }

// String returns "lockstep://<node><path>#<uid in lowercase hex>", or "<zero>" for the zero
// Address.
func (a Address) String() string { return a.engine().String() }

// addressOf converts the engine's address type to Address (ENG-020).
func addressOf(a engine.Address) Address {
	return Address{node: a.Node, path: a.Path, uid: a.UID}
}

// engine converts a to the engine's address type; it is the inverse of addressOf.
func (a Address) engine() engine.Address {
	return engine.Address{Node: a.node, Path: a.path, UID: a.uid}
}

// PID is the typed address of an actor whose message type is M. It is comparable and contains
// only its Address. The zero PID names no actor.
type PID[M any] struct{ addr Address }

// Address returns the PID's untyped address.
func (p PID[M]) Address() Address { return p.addr }

// IsZero reports whether p is the zero PID.
func (p PID[M]) IsZero() bool { return p.addr.IsZero() }

// String returns p.Address().String().
func (p PID[M]) String() string { return p.addr.String() }

// PIDAt returns a PID with uid 0 that addresses whichever actor is registered at path on node
// when the PID is used. A message to it when no actor of type M is registered at path goes to
// dead letters on that node. path has the form Lookup accepts. PIDAt panics if node is not a valid node name or path is malformed.
func PIDAt[M any](node, path string) PID[M] {
	if !validNode(node) {
		panic(fmt.Sprintf("lockstep: PIDAt with invalid node %q", node))
	}
	if !validPath(path) {
		panic(fmt.Sprintf("lockstep: PIDAt with invalid path %q", path))
	}
	return PID[M]{addr: Address{node: node, path: path}}
}
