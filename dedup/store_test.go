// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

// testKind names the store in the tests below.
var testKind = kind{name: "Test", ctor: "NewTest", tag: "test"}

// versionField appends `,"v":<v>`, the field a store with V = uint64 adds to each entry.
func versionField(b []byte, v uint64) ([]byte, error) {
	b = append(b, `,"v":`...)
	return strconv.AppendUint(b, v, 10), nil
}

// DLV-060, DLV-200: moveLast gives an entry the effective now and places it last, and the encoding
// appends the type's entry field after "at".
func TestStoreMoveLast(t *testing.T) {
	s := newStore[string, uint64](Config{3, 10 * sec})
	e := s.begin(testKind, t0)
	s.add(e, "a", 1)
	s.add(e, "b", 1)
	e = s.begin(testKind, at(sec))
	n, ok := s.get(e, "a")
	if !ok {
		t.Fatal(`get("a") missed`)
	}
	n.v = 2
	s.moveLast(e, n)
	b, err := s.marshal(testKind, versionField)
	const want = `{"dedup":1,"type":"test","capacity":3,"ttl":10000000000,"last":2000000000,"entries":[` +
		`{"k":"b","at":1000000000,"v":1},{"k":"a","at":2000000000,"v":2}]}`
	if err != nil || string(b) != want {
		t.Errorf("marshal = %s, %v; want\n%s", b, err, want)
	}
	if got := s.count(at(10 * sec).UnixNano()); got != 1 {
		t.Errorf("count(t0+10s) = %d, want 1", got)
	}
}

// DLV-200: an entry field that fails gives the entry's index, wrapping the field's error.
func TestStoreEntryFieldError(t *testing.T) {
	errField := errors.New("field fails")
	s := newStore[string, uint64](Config{3, 0})
	e := s.begin(testKind, t0)
	s.add(e, "a", 1)
	s.add(e, "b", 2)
	b, err := s.marshal(testKind, func(b []byte, v uint64) ([]byte, error) {
		if v == 2 {
			return nil, errField
		}
		return versionField(b, v)
	})
	if b != nil || err == nil || !strings.HasPrefix(err.Error(), "dedup: encode entry 1: ") || !errors.Is(err, errField) {
		t.Errorf("marshal = %q, %v; want nil and an error starting %q wrapping %v", b, err, "dedup: encode entry 1: ", errField)
	}
}
