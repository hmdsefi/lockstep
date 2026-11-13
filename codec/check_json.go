// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
)

var (
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
)

// checkJSON is JSON.Supports (COD-090): it walks t and returns nil, or an error wrapping
// ErrUnsupported whose why part is built outermost first.
func checkJSON(t reflect.Type) error {
	w := jsonWalk{visited: make(map[reflect.Type]bool)}
	if why, ok := w.walk(t); !ok {
		return fmt.Errorf("%w: json cannot encode %s: %s", ErrUnsupported, t, why)
	}
	return nil
}

// jsonWalk is one COD-090 walk. A type already in visited is accepted, which ends the recursion
// of recursive types: if its first visit fails, the walk has already returned that failure.
type jsonWalk struct {
	visited map[reflect.Type]bool
}

// walk applies COD-090's six rules to u, in order, and returns ok, or the why text and false.
func (w *jsonWalk) walk(u reflect.Type) (why string, ok bool) {
	if w.visited[u] {
		return "", true
	}
	w.visited[u] = true

	// Rule 1: marshaler methods. The walk does not descend into a type that has them.
	if why, decided := marshalerPair(u); decided {
		return why, why == ""
	}

	switch u.Kind() {
	// Rule 2: kinds encoding/json cannot encode, or cannot decode into.
	case reflect.Interface, reflect.Chan, reflect.Func, reflect.Complex64, reflect.Complex128,
		reflect.UnsafePointer:
		return fmt.Sprintf("%s type %s", u.Kind(), u), false

	// Rule 3: pointers, slices and arrays add no text.
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return w.walk(u.Elem())

	// Rule 4: maps.
	case reflect.Map:
		if k := u.Key(); !jsonMapKey(k) {
			return fmt.Sprintf("map key type %s", k), false
		}
		return w.walk(u.Elem())

	// Rule 5: structs, field by field in declaration order.
	case reflect.Struct:
		for i := range u.NumField() {
			f := u.Field(i)
			switch {
			case f.Tag.Get("json") == "-":
				continue
			case !f.IsExported() && !f.Anonymous:
				return fmt.Sprintf("unexported field %s", f.Name), false
			case f.Anonymous && !f.IsExported() && f.Type.Kind() == reflect.Pointer &&
				f.Type.Elem().Kind() == reflect.Struct:
				return fmt.Sprintf("unexported embedded pointer field %s", f.Name), false
			}
			if why, ok := w.walk(f.Type); !ok {
				return fmt.Sprintf("field %s: %s", f.Name, why), false
			}
		}
		return "", true

	// Rule 6: bool, the integer kinds including uintptr, the float kinds and string.
	default:
		return "", true
	}
}

// marshalerPair applies rule 1 to u. decided is false when u has neither MarshalJSON nor
// MarshalText; otherwise why is "" when u has the matching unmarshal method, or says which is
// missing.
//
// The unmarshal method is looked for on *u, except for a pointer type u, where it is looked for on
// u itself: encoding/json decodes into a pointer by allocating its element and calling the method
// on the pointer, and **T has no methods. Without this, *time.Time and *lockstep.Address fields
// would be rejected although they round trip. This refines the rule as COD-090 writes it.
func marshalerPair(u reflect.Type) (why string, decided bool) {
	target := u
	if u.Kind() != reflect.Pointer {
		target = reflect.PointerTo(u)
	}
	switch {
	case u.Implements(jsonMarshalerType):
		if target.Implements(jsonUnmarshalerType) {
			return "", true
		}
		return fmt.Sprintf("%s has MarshalJSON but no UnmarshalJSON", u), true
	case u.Implements(textMarshalerType):
		if target.Implements(textUnmarshalerType) {
			return "", true
		}
		return fmt.Sprintf("%s has MarshalText but no UnmarshalText", u), true
	}
	return "", false
}

// jsonMapKey reports whether encoding/json encodes and decodes map keys of type k (rule 4): a
// string kind, an integer kind, or a type with MarshalText whose pointer has UnmarshalText.
// encoding/json cannot decode into a map key of pointer type, so no pointer exception applies.
func jsonMapKey(k reflect.Type) bool {
	switch k.Kind() {
	case reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	}
	return k.Implements(textMarshalerType) && reflect.PointerTo(k).Implements(textUnmarshalerType)
}
