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
	// zeroRegistry is the panic of every method called on a zero Registry, which has no maps and
	// no built-ins. RegisterIn checks it right after its nil-registry row.
	zeroRegistry = "codec: zero Registry; use NewRegistry"
)

// Registry maps wire names to Go types and back, and encodes and decodes registered values with a
// Codec. It holds the built-in types from the start. A Registry is safe for concurrent use. The
// zero Registry is not usable: its methods panic, as RegisterIn with it does. Call NewRegistry.
type Registry struct {
	// mu guards byName and byType, which always hold the same pairs (COD-085). Lookups take the
	// read lock, registrations the write lock.
	mu     sync.RWMutex
	byName map[string]reflect.Type
	byType map[reflect.Type]string

	// supportMu guards supports, the cache of Codec.Supports results (COD-085). An entry holds the
	// error Supports returned, nil included; codecs with the same Name share entries.
	supportMu sync.RWMutex
	supports  map[supportKey]error
}

// supportKey identifies a cached Supports result: a codec, by name, and a type.
type supportKey struct {
	codec string
	typ   reflect.Type
}

// mustInit panics unless r was made by NewRegistry. The maps are set only there and never
// replaced, so reading the field needs no lock.
func (r *Registry) mustInit() {
	if r.byName == nil {
		panic(zeroRegistry)
	}
}

// defaultRegistry is the process registry that Register adds to.
var defaultRegistry = NewRegistry()

// NewRegistry returns a new registry that holds only the built-in types. Types registered in it
// are not visible in Default, nor the other way round.
func NewRegistry() *Registry {
	r := &Registry{
		byName:   make(map[string]reflect.Type, len(builtins)),
		byType:   make(map[reflect.Type]string, len(builtins)),
		supports: make(map[supportKey]error),
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
// if r is a zero Registry rather than one from NewRegistry, if name is not a valid wire name, if name starts with the reserved "lockstep.", if T is an
// interface type, if name is held by another type, or if T is held under another name.
func RegisterIn[T any](r *Registry, name string) {
	if r == nil {
		panic("codec: RegisterIn with nil registry")
	}
	r.mustInit()
	r.register(name, reflect.TypeFor[T]())
}

// register adds the pair (name, t) to an initialised r, panicking with COD-020's rows 2 to 6 in
// order; an existing
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
	r.mustInit()
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
	r.mustInit()
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
	r.mustInit()
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Sorted(maps.Keys(r.byName))
}

// Encode returns the wire name of v's dynamic type and the bytes c makes of v. It fails if the
// type is not registered (the error wraps ErrNotRegistered), if c does not support the type (the
// error c.Supports returned, which wraps ErrUnsupported), or if c.Marshal fails (the error wraps
// it and reads "codec: encode <name>: ..."). On error, name is "" and data is nil. Encode panics
// if c is nil.
//
// The registry asks c.Supports about each type once and keeps the answer for every codec with the
// same name.
func (r *Registry) Encode(c Codec, v any) (name string, data []byte, err error) {
	mustCodec(c)
	name, err = r.NameOf(v)
	if err != nil {
		return "", nil, err
	}
	if err := r.supported(c, reflect.TypeOf(v)); err != nil {
		return "", nil, err
	}
	data, err = c.Marshal(v)
	if err != nil {
		return "", nil, fmt.Errorf("codec: encode %s: %w", name, err)
	}
	return name, data, nil
}

// Decode decodes data into a new value of the type registered under the wire name name, and
// returns that value. The value has the registered type itself: a struct type T comes back as a
// T, not a *T, and a pointer type *T registered as such comes back as a *T. Decode fails if no
// type is registered under name (the error wraps ErrNotRegistered), if c does not support the
// type (the error c.Supports returned), or if c.Unmarshal fails (the error wraps it and reads
// "codec: decode <name>: ..."). On error, the value is nil. Decode panics if c is nil.
func (r *Registry) Decode(c Codec, name string, data []byte) (any, error) {
	mustCodec(c)
	t, err := r.TypeOf(name)
	if err != nil {
		return nil, err
	}
	if err := r.supported(c, t); err != nil {
		return nil, err
	}
	p := reflect.New(t)
	if err := c.Unmarshal(data, p.Interface()); err != nil {
		return nil, fmt.Errorf("codec: decode %s: %w", name, err)
	}
	return p.Elem().Interface(), nil
}

// Check reports whether c supports every registered type, built-ins included. It asks about the
// types in the order Names lists their names and returns the first error c.Supports returns,
// unchanged, or nil. Call it when a system starts, so that a type c cannot encode is found before
// the first message of that type is sent. Check panics if c is nil.
func (r *Registry) Check(c Codec) error {
	mustCodec(c)
	r.mu.RLock()
	names := slices.Sorted(maps.Keys(r.byName))
	types := make([]reflect.Type, len(names))
	for i, name := range names {
		types[i] = r.byName[name]
	}
	r.mu.RUnlock()
	for _, t := range types {
		if err := r.supported(c, t); err != nil {
			return err
		}
	}
	return nil
}

// mustCodec panics if c is nil (§4.1).
func mustCodec(c Codec) {
	if c == nil {
		panic("codec: nil codec")
	}
}

// supported returns c.Supports(t), asking c only the first time for each (c.Name(), t) pair
// (COD-085). Supports runs outside the lock, so two concurrent first calls may both ask; a codec's
// answer for a type never changes, so keeping either is right.
func (r *Registry) supported(c Codec, t reflect.Type) error {
	key := supportKey{codec: c.Name(), typ: t}
	r.supportMu.RLock()
	err, ok := r.supports[key]
	r.supportMu.RUnlock()
	if ok {
		return err
	}
	err = c.Supports(t)
	r.supportMu.Lock()
	r.supports[key] = err
	r.supportMu.Unlock()
	return err
}
