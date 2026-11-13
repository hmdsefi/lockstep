// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec

import (
	"cmp"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode"
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

	// Rule 1: marshaler methods, for a u that is not a pointer. The walk does not descend into a
	// type with an accepted pair.
	if u.Kind() != reflect.Pointer {
		if why, decided := marshalerPair(u); decided {
			return why, why == ""
		}
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

	// Rule 5: structs.
	case reflect.Struct:
		return w.walkStruct(u)

	// Rule 6: bool, the integer kinds including uintptr, the float kinds and string.
	default:
		return "", true
	}
}

// walkStruct applies rule 5 to the struct type u: each field in declaration order, then the JSON
// names of all of u's fields, promoted ones included.
func (w *jsonWalk) walkStruct(u reflect.Type) (why string, ok bool) {
	for i := range u.NumField() {
		f := u.Field(i)
		if f.Tag.Get("json") == "-" {
			continue
		}
		if !f.IsExported() {
			if !f.Anonymous {
				return fmt.Sprintf("unexported field %s", f.Name), false
			}
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() != reflect.Struct {
				// encoding/json drops an unexported embedded field of a non-struct type.
				return fmt.Sprintf("unexported field %s", f.Name), false
			}
			if f.Type.Kind() == reflect.Pointer {
				// encoding/json cannot allocate it when decoding.
				return fmt.Sprintf("unexported embedded pointer field %s", f.Name), false
			}
		}
		if why, ok := w.walk(f.Type); !ok {
			return fmt.Sprintf("field %s: %s", f.Name, why), false
		}
	}
	return jsonNameConflict(u)
}

// marshalerPair applies rule 1 to the non-pointer type u. decided is false when u has none of
// the four methods; otherwise why is "" when u has an accepted pair, or says what is wrong.
//
// The JSON methods are looked at first: when u has a JSON pair, encoding/json uses it in both
// directions and the text methods do not matter.
func marshalerPair(u reflect.Type) (why string, decided bool) {
	p := reflect.PointerTo(u)
	for _, m := range []struct {
		marshal, unmarshal         reflect.Type
		marshalName, unmarshalName string
	}{
		{jsonMarshalerType, jsonUnmarshalerType, "MarshalJSON", "UnmarshalJSON"},
		{textMarshalerType, textUnmarshalerType, "MarshalText", "UnmarshalText"},
	} {
		hasMarshal, hasUnmarshal := u.Implements(m.marshal), p.Implements(m.unmarshal)
		switch {
		case !hasMarshal && p.Implements(m.marshal):
			// encoding/json would use it only for addressable values.
			return fmt.Sprintf("%s has %s on its pointer only", u, m.marshalName), true
		case hasMarshal && !hasUnmarshal:
			return fmt.Sprintf("%s has %s but no %s", u, m.marshalName, m.unmarshalName), true
		case hasUnmarshal && !hasMarshal:
			return fmt.Sprintf("%s has %s but no %s", u, m.unmarshalName, m.marshalName), true
		case hasMarshal:
			return embeddedMarshaler(u, m.marshal, m.marshalName), true
		}
	}
	return "", false
}

// embeddedMarshaler returns "" unless u is a struct whose marshal method (of interface type
// marshal) comes from, or is also had by, an embedded field F, and u has another field that
// encoding/json would encode were it not for the method: then F's method hides that field.
func embeddedMarshaler(u reflect.Type, marshal reflect.Type, name string) string {
	if u.Kind() != reflect.Struct {
		return ""
	}
	embedded := -1
	for i := range u.NumField() {
		f := u.Field(i)
		if !f.Anonymous {
			continue
		}
		if f.Type.Implements(marshal) ||
			(f.Type.Kind() != reflect.Pointer && reflect.PointerTo(f.Type).Implements(marshal)) {
			embedded = i
			break
		}
	}
	if embedded < 0 {
		return ""
	}
	for i := range u.NumField() {
		if i != embedded && u.Field(i).Tag.Get("json") != "-" {
			return fmt.Sprintf("field %s: embedded %s hides the other fields", u.Field(embedded).Name, name)
		}
	}
	return ""
}

// jsonMapKey reports whether encoding/json encodes and decodes map keys of type k alike (rule 4).
// encoding/json writes a string-kind key as it is, else through MarshalText, else as an integer,
// and reads a key through UnmarshalText first.
func jsonMapKey(k reflect.Type) bool {
	hasMarshal := k.Implements(textMarshalerType)
	hasUnmarshal := reflect.PointerTo(k).Implements(textUnmarshalerType)
	switch k.Kind() {
	case reflect.String:
		return !hasUnmarshal
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return hasMarshal == hasUnmarshal
	}
	return hasMarshal && hasUnmarshal
}

// jsonField is a field of a struct as encoding/json's typeFields lists it.
type jsonField struct {
	name   string // the JSON name
	tagged bool   // the name comes from a json tag
	index  []int  // the field's index sequence from the outer struct
}

// jsonNameConflict returns the why text for the first JSON name that encoding/json gives to more
// than one field of the struct u: it encodes at most one of them and drops the others. The fields
// are computed as encoding/json's typeFields computes them: breadth first through embedded
// structs, a field's name from its tag or else its Go name, and a field hidden by one with the
// same name at a shallower depth, or tagged at the same depth. The two fields named are the first
// two in encoding/json's order: by depth, then tagged first, then by index sequence.
func jsonNameConflict(u reflect.Type) (why string, ok bool) {
	var fields []jsonField
	next := []jsonLevel{{typ: u}}
	visited := make(map[reflect.Type]bool)
	for len(next) > 0 {
		current := next
		next = nil
		// count holds, for each struct type of this level, the index sequences that reach it; a
		// type reached twice has each of its fields twice, so they conflict.
		var count map[reflect.Type][][]int
		count, current = levelCounts(current)
		for _, l := range current {
			if visited[l.typ] {
				continue
			}
			visited[l.typ] = true
			for i := range l.typ.NumField() {
				sf := l.typ.Field(i)
				if sf.Anonymous {
					t := sf.Type
					if t.Kind() == reflect.Pointer {
						t = t.Elem()
					}
					if !sf.IsExported() && t.Kind() != reflect.Struct {
						continue
					}
				} else if !sf.IsExported() {
					continue
				}
				tag := sf.Tag.Get("json")
				if tag == "-" {
					continue
				}
				name, _, _ := strings.Cut(tag, ",")
				if !validJSONTag(name) {
					name = ""
				}
				ft := sf.Type
				if ft.Name() == "" && ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if name != "" || !sf.Anonymous || ft.Kind() != reflect.Struct {
					tagged := name != ""
					if name == "" {
						name = sf.Name
					}
					for _, prefix := range count[l.typ] {
						fields = append(fields, jsonField{name: name, tagged: tagged, index: append(slices.Clone(prefix), i)})
					}
					continue
				}
				next = append(next, jsonLevel{typ: ft, index: append(slices.Clone(l.index), i)})
			}
		}
	}

	slices.SortStableFunc(fields, func(a, b jsonField) int {
		if c := cmp.Compare(a.name, b.name); c != 0 {
			return c
		}
		if c := cmp.Compare(len(a.index), len(b.index)); c != 0 {
			return c
		}
		if a.tagged != b.tagged {
			if a.tagged {
				return -1
			}
			return 1
		}
		return slices.Compare(a.index, b.index)
	})
	for i := 1; i < len(fields); i++ {
		if fields[i].name == fields[i-1].name {
			return fmt.Sprintf("fields %s and %s share the JSON name %q",
				fieldPath(u, fields[i-1].index), fieldPath(u, fields[i].index), fields[i].name), false
		}
	}
	return "", true
}

// jsonLevel is a struct type that jsonNameConflict looks into, and the index sequence of the
// embedded field that reaches it.
type jsonLevel struct {
	typ   reflect.Type
	index []int
}

// levelCounts merges the entries of one level that reach the same struct type, keeping the first
// in order, and returns for each type every index sequence that reaches it.
func levelCounts(levels []jsonLevel) (map[reflect.Type][][]int, []jsonLevel) {
	count := make(map[reflect.Type][][]int)
	var out []jsonLevel
	for _, l := range levels {
		if len(count[l.typ]) == 0 {
			out = append(out, l)
		}
		count[l.typ] = append(count[l.typ], l.index)
	}
	return count, out
}

// fieldPath returns the dotted Go field names of the index sequence index in the struct u.
func fieldPath(u reflect.Type, index []int) string {
	names := make([]string, len(index))
	t := u
	for i, x := range index {
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		f := t.Field(x)
		names[i] = f.Name
		t = f.Type
	}
	return strings.Join(names, ".")
}

// validJSONTag reports whether encoding/json accepts s as a field name in a json tag.
func validJSONTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", c):
		case !unicode.IsLetter(c) && !unicode.IsDigit(c):
			return false
		}
	}
	return true
}
