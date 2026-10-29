// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"errors"
	"strings"
	"testing"
)

// testKind names the store in the tests below.
var testKind = kind{name: "Test", ctor: "NewTest", tag: "test"}

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
