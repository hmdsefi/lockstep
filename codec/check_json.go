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

	// Rule 1: marshaler methods, for a u that is not a pointer, and for a pointer whose element
	// has its methods on the pointer only. The walk does not descend into a type with an
	// accepted pair.
	marshalers := marshalerPair
	if u.Kind() == reflect.Pointer {
		marshalers = pointerPair
	}
	if why, decided := marshalers(u); decided {
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
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if !f.IsExported() {
			if !f.Anonymous || jsonTagName(f) != "" {
				// A tagged embedded field is an ordinary field: encoding/json neither inlines
				// it nor can it call its methods through the unexported field.
				return fmt.Sprintf("unexported field %s", f.Name), false
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
		if f.Anonymous && jsonTagName(f) == "" && ft.Kind() == reflect.Struct {
			// encoding/json inlines the fields of an untagged embedded struct and ignores its
			// methods, so rule 1 must not accept it here.
			if name := marshalName(ft); name != "" {
				return fmt.Sprintf("field %s: embedded %s is ignored", f.Name, name), false
			}
		}
		if why, ok := w.walk(f.Type); !ok {
			return fmt.Sprintf("field %s: %s", f.Name, why), false
		}
	}
	return jsonNameConflict(u)
}

// marshalName returns "MarshalJSON" if the struct type t or its pointer has that method, else
// "MarshalText" if one of them has that method, else "".
func marshalName(t reflect.Type) string {
	p := reflect.PointerTo(t)
	switch {
	case t.Implements(jsonMarshalerType) || p.Implements(jsonMarshalerType):
		return "MarshalJSON"
	case t.Implements(textMarshalerType) || p.Implements(textMarshalerType):
		return "MarshalText"
	}
	return ""
}

// methodPair is a marshal method and its unmarshal method.
type methodPair struct {
	marshal, unmarshal         reflect.Type
	marshalName, unmarshalName string
}

// methodPairs are rule 1's pairs, JSON first: when a type has a JSON pair, encoding/json uses it
// in both directions and the text methods do not matter.
var methodPairs = [...]methodPair{
	{jsonMarshalerType, jsonUnmarshalerType, "MarshalJSON", "UnmarshalJSON"},
	{textMarshalerType, textUnmarshalerType, "MarshalText", "UnmarshalText"},
}

// marshalerPair applies rule 1 to the non-pointer type u. decided is false when u has none of
// the four methods; otherwise why is "" when u has an accepted pair, or says what is wrong.
func marshalerPair(u reflect.Type) (why string, decided bool) {
	p := reflect.PointerTo(u)
	for _, m := range methodPairs {
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
			return embeddedMarshaler(u, m), true
		}
	}
	return "", false
}

// pointerPair applies rule 1's pointer exception to the pointer type u = *T. decided is false
// unless T is neither a pointer nor an interface, T has no marshal method and u has one: then
// encoding/json always uses u's methods (nil encodes as null, and decoding allocates a T), so u
// needs both methods of the pair, and T's embedded fields are checked as for a non-pointer type.
func pointerPair(u reflect.Type) (why string, decided bool) {
	t := u.Elem()
	if t.Kind() == reflect.Pointer || t.Kind() == reflect.Interface ||
		t.Implements(jsonMarshalerType) || t.Implements(textMarshalerType) ||
		!u.Implements(jsonMarshalerType) && !u.Implements(textMarshalerType) {
		return "", false
	}
	for _, m := range methodPairs {
		hasMarshal, hasUnmarshal := u.Implements(m.marshal), u.Implements(m.unmarshal)
		switch {
		case hasMarshal && !hasUnmarshal:
			return fmt.Sprintf("%s has %s but no %s", u, m.marshalName, m.unmarshalName), true
		case hasUnmarshal && !hasMarshal:
			return fmt.Sprintf("%s has %s but no %s", u, m.unmarshalName, m.marshalName), true
		case hasMarshal:
			return embeddedMarshaler(t, m), true
		}
	}
	return "", false
}

// embeddedMarshaler returns "" unless u is a struct with an embedded field F that has m's marshal
// method (on F's type or its pointer), so that u's pair comes from, or is also had by, F. Then, if
// F is a pointer, encoding/json calls the method through it and panics when it is nil; else, if u
// has another field not tagged `json:"-"`, F's method hides that field.
func embeddedMarshaler(u reflect.Type, m methodPair) string {
	if u.Kind() != reflect.Struct {
		return ""
	}
	embedded := -1
	for i := range u.NumField() {
		f := u.Field(i)
		if !f.Anonymous {
			continue
		}
		if f.Type.Implements(m.marshal) ||
			(f.Type.Kind() != reflect.Pointer && reflect.PointerTo(f.Type).Implements(m.marshal)) {
			embedded = i
			break
		}
	}
	if embedded < 0 {
		return ""
	}
	f := u.Field(embedded)
	if f.Type.Kind() == reflect.Pointer {
		return fmt.Sprintf("field %s: embedded pointer %s panics when nil", f.Name, m.marshalName)
	}
	for i := range u.NumField() {
		if i != embedded && u.Field(i).Tag.Get("json") != "-" {
			return fmt.Sprintf("field %s: embedded %s hides the other fields", f.Name, m.marshalName)
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

// jsonField is a field of a struct as encoding/json names it.
type jsonField struct {
	name   string // the JSON name
	tagged bool   // the name comes from a json tag
	index  []int  // the field's index sequence from the outer struct
}

// jsonTagName returns the name in f's json tag, or "" if the tag gives no valid name.
func jsonTagName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
	if !validJSONTag(name) {
		return ""
	}
	return name
}

// jsonNameConflict returns the why text for the first JSON name that more than one field of the
// struct u gets, promoted fields included: encoding/json encodes at most one of them and drops
// the others. Names are given as encoding/json gives them (from the tag, else the Go name), but
// over the full embedding tree: every embedded struct is expanded along every path that reaches
// it, which also finds the fields encoding/json drops without a name clash (a struct embedded at
// two depths, a diamond at any depth). The two fields named are the first two in encoding/json's
// order: by depth, then tagged first, then by index sequence.
func jsonNameConflict(u reflect.Type) (why string, ok bool) {
	var fields []jsonField
	if why, ok := expandFields(u, nil, []reflect.Type{u}, &fields); !ok {
		return why, false
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

// expandFields appends to fields the JSON fields of the struct t, reached from the outer struct
// through the index sequence prefix, expanding untagged embedded structs as encoding/json does.
// path holds the struct types being expanded; an embedded struct already on it is a cycle, whose
// fields encoding/json drops.
func expandFields(t reflect.Type, prefix []int, path []reflect.Type, fields *[]jsonField) (why string, ok bool) {
	for i := range t.NumField() {
		sf := t.Field(i)
		if sf.Anonymous {
			at := sf.Type
			if at.Kind() == reflect.Pointer {
				at = at.Elem()
			}
			if !sf.IsExported() && at.Kind() != reflect.Struct {
				continue
			}
		} else if !sf.IsExported() {
			continue
		}
		if sf.Tag.Get("json") == "-" {
			continue
		}
		name := jsonTagName(sf)
		index := append(slices.Clone(prefix), i)
		ft := sf.Type
		if ft.Name() == "" && ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if name != "" || !sf.Anonymous || ft.Kind() != reflect.Struct {
			tagged := name != ""
			if name == "" {
				name = sf.Name
			}
			*fields = append(*fields, jsonField{name: name, tagged: tagged, index: index})
			continue
		}
		if slices.Contains(path, ft) {
			return fmt.Sprintf("field %s: recursive embedding hides its fields", sf.Name), false
		}
		if why, ok := expandFields(ft, index, append(path[:len(path):len(path)], ft), fields); !ok {
			return why, false
		}
	}
	return "", true
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
