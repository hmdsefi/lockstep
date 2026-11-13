// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
)

const (
	// maxNameLen is the longest wire name, in bytes (COD-010).
	maxNameLen = 255
	// reservedPrefix starts the names of the built-in types; RegisterIn rejects it (COD-010).
	reservedPrefix = "lockstep."
)

// Registry maps wire names to Go types and back. It holds the built-in types from the start.
// A Registry is safe for concurrent use. The zero Registry is not usable; call NewRegistry.
type Registry struct {
	// mu guards byName and byType, which always hold the same pairs (COD-085). Lookups take the
	// read lock, registrations the write lock.
	mu     sync.RWMutex
	byName map[string]reflect.Type
	byType map[reflect.Type]string
}

// defaultRegistry is the process registry that Register adds to.
var defaultRegistry = NewRegistry()

// NewRegistry returns a new registry that holds only the built-in types. Types registered in it
// are not visible in Default, nor the other way round.
func NewRegistry() *Registry {
	r := &Registry{
		byName: make(map[string]reflect.Type, len(builtins)),
		byType: make(map[reflect.Type]string, len(builtins)),
	}
	for _, b := range builtins {
		r.byName[b.name] = b.typ
		r.byType[b.typ] = b.name
	}
	return r
}

// Default returns the registry of the process, which Register adds to.
func Default() *Registry { return defaultRegistry }

// Register registers type T under the wire name name in the process registry; it is
// RegisterIn(Default(), name) and panics in the same cases.
func Register[T any](name string) { RegisterIn[T](defaultRegistry, name) }

// RegisterIn registers type T under the wire name name in r. Registering a name and type that r
// already holds together does nothing. RegisterIn panics, checking in this order, if r is nil,
// if name is not a valid wire name, if name starts with the reserved "lockstep.", if T is an
// interface type, if name is held by another type, or if T is held under another name.
func RegisterIn[T any](r *Registry, name string) {
	if r == nil {
		panic("codec: RegisterIn with nil registry")
	}
	r.register(name, reflect.TypeFor[T]())
}

// register adds the pair (name, t), panicking with COD-020's rows 2 to 6 in order; an existing
// identical pair is a no-op, checked after row 4 (COD-030).
func (r *Registry) register(name string, t reflect.Type) {
	if !validName(name) {
		panic(fmt.Sprintf("codec: invalid wire name %q", name))
	}
	if strings.HasPrefix(name, reservedPrefix) {
		panic(fmt.Sprintf("codec: wire name %q is reserved", name))
	}
	if t.Kind() == reflect.Interface {
		panic(fmt.Sprintf("codec: cannot register interface type %s", t))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	other, nameHeld := r.byName[name]
	if nameHeld && other == t {
		return // the same pair again (COD-030); the maps agree, so byType[t] == name
	}
	if nameHeld {
		panic(fmt.Sprintf("codec: wire name %q already registered for %s", name, other))
	}
	if otherName, ok := r.byType[t]; ok {
		panic(fmt.Sprintf("codec: type %s already registered as %q", t, otherName))
	}
	r.byName[name] = t
	r.byType[t] = name
}

// validName reports whether name follows COD-010's grammar: 1 to 255 bytes, an ASCII letter
// first, then ASCII letters, digits, '.', '_', '/' and '-'.
func validName(name string) bool {
	if name == "" || len(name) > maxNameLen || !isLetter(name[0]) {
		return false
	}
	for i := 1; i < len(name); i++ {
		switch c := name[i]; {
		case isLetter(c), '0' <= c && c <= '9', c == '.', c == '_', c == '/', c == '-':
		default:
			return false
		}
	}
	return true
}

// isLetter reports whether c is an ASCII letter.
func isLetter(c byte) bool { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }

// NameOf returns the wire name of v's dynamic type. If the type is not registered, or v is nil,
// the error wraps ErrNotRegistered.
func (r *Registry) NameOf(v any) (string, error) {
	t := reflect.TypeOf(v)
	r.mu.RLock()
	name, ok := r.byType[t]
	r.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("%w: %v", ErrNotRegistered, t)
	}
	return name, nil
}

// TypeOf returns the type registered under the wire name name. If no type is, the error wraps
// ErrNotRegistered.
func (r *Registry) TypeOf(name string) (reflect.Type, error) {
	r.mu.RLock()
	t, ok := r.byName[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: name %q", ErrNotRegistered, name)
	}
	return t, nil
}

// Names returns the registered wire names, built-ins included, in increasing byte order. The
// slice is new on each call; the caller may change it.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Sorted(maps.Keys(r.byName))
}
