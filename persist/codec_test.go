// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist_test

import (
	"encoding/json"
	"testing"

	"github.com/hmdsefi/lockstep/persist"
)

// counterState is the state type of AT-PER-24.
type counterState struct{ Sum, Count int64 }

// I is a local interface type for the JSON panic test.
type I interface{ M() }

// TestJSONRoundTrip covers AT-PER-24: encoding is encoding/json's, decoding goes into a new
// value, and a decode error comes back unwrapped.
func TestJSONRoundTrip(t *testing.T) {
	c := persist.JSON[counterState]()
	want := counterState{Sum: 3, Count: 2}

	data, err := c.Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got, wantJSON := string(data), `{"Sum":3,"Count":2}`; got != wantJSON {
		t.Fatalf("Encode = %s, want %s", got, wantJSON)
	}

	got, err := c.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != want {
		t.Fatalf("Decode = %+v, want %+v", got, want)
	}

	bad := []byte(`{"Sum":`)
	_, err = c.Decode(bad)
	want2 := json.Unmarshal(bad, new(counterState))
	if err == nil || err.Error() != want2.Error() {
		t.Fatalf("Decode error = %v, want encoding/json's %v", err, want2)
	}
	if _, ok := err.(*json.SyntaxError); !ok {
		t.Fatalf("Decode error is %T, want an unwrapped *json.SyntaxError", err)
	}
}

// TestJSONInterfacePanics covers PER-230: an interface type has no concrete type to decode into.
func TestJSONInterfacePanics(t *testing.T) {
	const want = "persist: JSON: persist_test.I is an interface type; " +
		"use a codec that records the concrete type"
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("JSON[I]() did not panic")
		}
		if got, ok := r.(string); !ok || got != want {
			t.Fatalf("panic = %#v, want %q", r, want)
		}
	}()
	persist.JSON[I]()
}
