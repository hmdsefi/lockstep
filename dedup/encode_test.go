// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

// receiver is a value under test for decoding: it encodes and decodes.
type receiver interface {
	json.Marshaler
	json.Unmarshaler
	Config() Config
}

// invalidPrefix starts the text of every error UnmarshalJSON returns.
const invalidPrefix = "dedup: invalid encoding: "

// windowReceiver returns a Window[string] {2, 0} holding "a" at t0.
func windowReceiver() receiver {
	w := NewWindow[string](Config{2, 0})
	w.Add(t0, "a")
	return w
}

// repliesReceiver returns a Replies[string, int] {2, 0} holding "a" at t0.
func repliesReceiver() receiver {
	c := NewReplies[string, int](Config{2, 0})
	c.Put(t0, "a", 1)
	return c
}

// versionsReceiver returns a Versions[string] {2, 0} holding "a" at t0.
func versionsReceiver() receiver {
	v := NewVersions[string](Config{2, 0})
	v.Accept(t0, "a", 1)
	return v
}

// anyWindowReceiver returns a Window[any] {2, 0} holding "a" at t0.
func anyWindowReceiver() receiver {
	w := NewWindow[any](Config{2, 0})
	w.Add(t0, "a")
	return w
}

// A decodeCase is an input of AT-DLV-18 for a receiver. want is the error text after invalidPrefix.
// Where the tail is an encoding/json error it is built by jsonErr from the same bytes, so the test
// does not depend on the wording of one Go release.
type decodeCase struct {
	name     string
	receiver func() receiver
	input    string
	want     string
}

// jsonErr returns the text of the error encoding/json gives for data decoded into a *T.
func jsonErr[T any](data string) string {
	var v T
	err := json.Unmarshal([]byte(data), &v)
	if err == nil {
		panic("jsonErr: " + data + " decodes")
	}
	return err.Error()
}

// unhashable returns the text of the runtime error a map with interface keys panics with when it
// is looked up with k, whose dynamic type cannot be hashed.
func unhashable(k any) string {
	return fmt.Sprint(panicOf(func() { _ = map[any]bool{}[k] }))
}

// decodeCases are the invalid encodings of AT-DLV-18 and the order of DLV-210's checks. Each
// receiver holds one entry, "a", before the call.
var decodeCases = []decodeCase{
	{name: "Window truncated JSON", receiver: windowReceiver, input: `{`,
		want: jsonErr[*envelope](`{`)},
	{name: "Window not an object", receiver: windowReceiver, input: `[]`,
		want: jsonErr[*envelope](`[]`)},
	{name: "Window empty object", receiver: windowReceiver, input: `{}`,
		want: "format 0, want 1"},
	{name: "Window format 2", receiver: windowReceiver,
		input: `{"dedup":2,"type":"window","capacity":2,"ttl":0,"last":0,"entries":[]}`,
		want:  "format 2, want 1"},
	{name: "Window replies encoding", receiver: windowReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":1000000000,"entries":[{"k":"r","at":1000000000,"r":7}]}`,
		want:  `type "replies", want "window"`},
	{name: "Window versions encoding", receiver: windowReceiver,
		input: `{"dedup":1,"type":"versions","capacity":2,"ttl":0,"last":1000000000,"entries":[{"k":"e","at":1000000000,"v":3}]}`,
		want:  `type "versions", want "window"`},
	{name: "Window missing type", receiver: windowReceiver,
		input: `{"dedup":1,"capacity":2,"ttl":0,"last":0,"entries":[]}`,
		want:  `type "", want "window"`},
	{name: "Window capacity 0", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":0,"ttl":0,"last":0,"entries":[]}`,
		want:  "Capacity must be at least 1 (got 0)"},
	{name: "Window capacity -1", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":-1,"ttl":0,"last":0,"entries":[]}`,
		want:  "Capacity must be at least 1 (got -1)"},
	{name: "Window ttl -1", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":-1,"last":0,"entries":[]}`,
		want:  "TTL must not be negative (got -1ns)"},
	{name: "Window capacity before ttl", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":0,"ttl":-1,"last":0,"entries":[]}`,
		want:  "Capacity must be at least 1 (got 0)"},
	{name: "Window three entries, capacity 2", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":3,"entries":[{"k":"a","at":1},{"k":"b","at":2},{"k":"c","at":3}]}`,
		want:  "3 entries, more than capacity 2"},
	{name: "Window key is a number", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":1,"entries":[{"k":5,"at":1}]}`,
		want:  "entry 0: key: " + jsonErr[string](`5`)},
	{name: "Window missing key", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":1,"entries":[{"at":1}]}`,
		want:  "entry 0: key: " + jsonErr[string]("")},
	{name: "Window at 2 then 1", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":2,"entries":[{"k":"a","at":2},{"k":"b","at":1}]}`,
		want:  "entry 1: at 1 before entry 0's 2"},
	{name: "Window at 5, last 4", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":4,"entries":[{"k":"a","at":5}]}`,
		want:  "entry 0: at 5 after last 4"},
	{name: "Window key a twice", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":2,"entries":[{"k":"a","at":1},{"k":"a","at":2}]}`,
		want:  "entry 1: key equals entry 0's"},
	{name: "Window key decode before at order", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":3,"ttl":0,"last":9,"entries":[{"k":"a","at":2},{"k":7,"at":1}]}`,
		want:  "entry 1: key: " + jsonErr[string](`7`)},
	{name: "Window last before duplicate", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":3,"ttl":0,"last":1,"entries":[{"k":"a","at":1},{"k":"a","at":2}]}`,
		want:  "entry 1: at 2 after last 1"},
	{name: "Window first failing entry wins", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":4,"ttl":0,"last":5,"entries":[{"k":"a","at":1},{"k":"b","at":2},{"k":"b","at":3},{"k":"c","at":9}]}`,
		want:  "entry 2: key equals entry 1's"},
	{name: "Window type before bounds", receiver: windowReceiver,
		input: `{"dedup":1,"type":"replies","capacity":0,"ttl":0,"last":0,"entries":[]}`,
		want:  `type "replies", want "window"`},
	{name: "Window entry count before entries", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":3,"entries":[{"k":5,"at":1},{"k":"b","at":2},{"k":"c","at":3}]}`,
		want:  "3 entries, more than capacity 2"},
	{name: "Window duplicate names the earlier equal entry", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":3,"ttl":0,"last":1,"entries":[{"k":"a","at":1},{"k":"b","at":1},{"k":"a","at":1}]}`,
		want:  "entry 2: key equals entry 0's"},
	{name: "Window at order before duplicate", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":2,"entries":[{"k":"a","at":2},{"k":"a","at":1}]}`,
		want:  "entry 1: at 1 before entry 0's 2"},
	{name: "Window null entry", receiver: windowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":1,"entries":[null]}`,
		want:  "entry 0: key: " + jsonErr[string]("")},
	{name: "Window[any] key that cannot be hashed", receiver: anyWindowReceiver,
		input: `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":1,"entries":[{"k":{"a":1},"at":1}]}`,
		want:  "entry 0: key: " + unhashable(map[string]any{})},

	// Replies.
	{name: "Replies truncated JSON", receiver: repliesReceiver, input: `{`,
		want: jsonErr[*envelope](`{`)},
	{name: "Replies empty object", receiver: repliesReceiver, input: `{}`,
		want: "format 0, want 1"},
	{name: "Replies format 2", receiver: repliesReceiver,
		input: `{"dedup":2,"type":"replies","capacity":2,"ttl":0,"last":0,"entries":[]}`,
		want:  "format 2, want 1"},
	{name: "Replies window encoding", receiver: repliesReceiver,
		input: windowJSON(2, 0, 1_000_000_000, ent{"a", 1_000_000_000}),
		want:  `type "window", want "replies"`},
	{name: "Replies versions encoding", receiver: repliesReceiver,
		input: versionsJSON(2, 0, 1_000_000_000, verEnt{"a", 1_000_000_000, 1}),
		want:  `type "versions", want "replies"`},
	{name: "Replies capacity 0", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":0,"ttl":0,"last":0,"entries":[]}`,
		want:  "Capacity must be at least 1 (got 0)"},
	{name: "Replies ttl -1", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":-1,"last":0,"entries":[]}`,
		want:  "TTL must not be negative (got -1ns)"},
	{name: "Replies three entries, capacity 2", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":3,"entries":[{"k":"a","at":1,"r":1},{"k":"b","at":2,"r":2},{"k":"c","at":3,"r":3}]}`,
		want:  "3 entries, more than capacity 2"},
	{name: "Replies key is a number", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":1,"entries":[{"k":5,"at":1,"r":1}]}`,
		want:  "entry 0: key: " + jsonErr[string](`5`)},
	{name: "Replies reply is a string", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":1,"entries":[{"k":"a","at":1,"r":"x"}]}`,
		want:  "entry 0: reply: " + jsonErr[int](`"x"`)},
	{name: "Replies missing reply", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":1,"entries":[{"k":"a","at":1}]}`,
		want:  "entry 0: reply: " + jsonErr[int]("")},
	{name: "Replies key fails before reply", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":1,"entries":[{"k":5,"at":1,"r":"x"}]}`,
		want:  "entry 0: key: " + jsonErr[string](`5`)},
	{name: "Replies reply fails before at order", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":3,"ttl":0,"last":9,"entries":[{"k":"a","at":2,"r":1},{"k":"b","at":1,"r":"x"}]}`,
		want:  "entry 1: reply: " + jsonErr[int](`"x"`)},
	{name: "Replies at 2 then 1", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":2,"entries":[{"k":"a","at":2,"r":1},{"k":"b","at":1,"r":2}]}`,
		want:  "entry 1: at 1 before entry 0's 2"},
	{name: "Replies at 5, last 4", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":4,"entries":[{"k":"a","at":5,"r":1}]}`,
		want:  "entry 0: at 5 after last 4"},
	{name: "Replies key a twice", receiver: repliesReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":2,"entries":[{"k":"a","at":1,"r":1},{"k":"a","at":2,"r":2}]}`,
		want:  "entry 1: key equals entry 0's"},

	// Versions.
	{name: "Versions truncated JSON", receiver: versionsReceiver, input: `{`,
		want: jsonErr[*envelope](`{`)},
	{name: "Versions empty object", receiver: versionsReceiver, input: `{}`,
		want: "format 0, want 1"},
	{name: "Versions format 2", receiver: versionsReceiver,
		input: `{"dedup":2,"type":"versions","capacity":2,"ttl":0,"last":0,"entries":[]}`,
		want:  "format 2, want 1"},
	{name: "Versions replies encoding", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":1000000000,"entries":[{"k":"r","at":1000000000,"r":7}]}`,
		want:  `type "replies", want "versions"`},
	{name: "Versions window encoding", receiver: versionsReceiver,
		input: windowJSON(2, 0, 1_000_000_000, ent{"a", 1_000_000_000}),
		want:  `type "window", want "versions"`},
	{name: "Versions capacity -1", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"versions","capacity":-1,"ttl":0,"last":0,"entries":[]}`,
		want:  "Capacity must be at least 1 (got -1)"},
	{name: "Versions ttl -1", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"versions","capacity":2,"ttl":-1,"last":0,"entries":[]}`,
		want:  "TTL must not be negative (got -1ns)"},
	{name: "Versions three entries, capacity 2", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"versions","capacity":2,"ttl":0,"last":3,"entries":[{"k":"a","at":1,"v":1},{"k":"b","at":2,"v":1},{"k":"c","at":3,"v":1}]}`,
		want:  "3 entries, more than capacity 2"},
	{name: "Versions key is a number", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"versions","capacity":2,"ttl":0,"last":1,"entries":[{"k":5,"at":1,"v":1}]}`,
		want:  "entry 0: key: " + jsonErr[string](`5`)},
	{name: "Versions at 2 then 1", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"versions","capacity":2,"ttl":0,"last":2,"entries":[{"k":"a","at":2,"v":1},{"k":"b","at":1,"v":1}]}`,
		want:  "entry 1: at 1 before entry 0's 2"},
	{name: "Versions at 5, last 4", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"versions","capacity":2,"ttl":0,"last":4,"entries":[{"k":"a","at":5,"v":1}]}`,
		want:  "entry 0: at 5 after last 4"},
	{name: "Versions key a twice", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"versions","capacity":2,"ttl":0,"last":2,"entries":[{"k":"a","at":1,"v":1},{"k":"a","at":2,"v":2}]}`,
		want:  "entry 1: key equals entry 0's"},
	{name: "Versions negative version", receiver: versionsReceiver,
		input: `{"dedup":1,"type":"versions","capacity":2,"ttl":0,"last":1,"entries":[{"k":"a","at":1,"v":-1}]}`,
		want:  jsonErr[*envelope](`{"entries":[{"v":-1}]}`)},
}

// AT-DLV-18, DLV-210: each invalid input returns an error wrapping ErrInvalidEncoding with its text
// and leaves the receiver as it was.
func TestATDLV18DecodeErrors(t *testing.T) {
	for _, tc := range decodeCases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.receiver()
			before := encode(t, r)
			err := r.UnmarshalJSON([]byte(tc.input))
			switch {
			case err == nil:
				t.Fatalf("UnmarshalJSON(%s) = nil, want an error", tc.input)
			case !errors.Is(err, ErrInvalidEncoding):
				t.Errorf("error %q does not wrap ErrInvalidEncoding", err)
			default:
				if want := invalidPrefix + tc.want; err.Error() != want {
					t.Errorf("error %q, want %q", err, want)
				}
			}
			if got := encode(t, r); got != before {
				t.Errorf("after the error the encoding is\n%s\nwant\n%s", got, before)
			}
		})
	}
}

// A key that fails to decode wraps the encoding/json error (DLV-210).
func TestDecodeKeyErrorWraps(t *testing.T) {
	w := NewWindow[string](Config{2, 0})
	err := w.UnmarshalJSON([]byte(`{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":1,"entries":[{"k":5,"at":1}]}`))
	var typeErr *json.UnmarshalTypeError
	if !errors.As(err, &typeErr) {
		t.Errorf("error %v does not wrap a *json.UnmarshalTypeError", err)
	}
}

// A replaceCase is a valid encoding for a receiver: decoding it replaces the receiver's bounds,
// last and entries completely, and the receiver then encodes as want (input when want is empty).
type replaceCase struct {
	name       string
	receiver   func() receiver
	input      string
	want       string
	wantConfig Config
}

// replaceCases are valid encodings of AT-DLV-18, decoded into a receiver that holds another entry
// and other bounds (one entry "a", {2, 0}).
var replaceCases = []replaceCase{
	{
		name:       "Window with two keys",
		receiver:   windowReceiver,
		input:      windowJSON(3, 10*sec, 2_000_000_000, ent{"b", 1_000_000_000}, ent{"c", 2_000_000_000}),
		wantConfig: Config{3, 10 * sec},
	},
	{
		name:       "Window without entries",
		receiver:   windowReceiver,
		input:      windowJSON(1, 0, -9223372036854775808),
		wantConfig: Config{1, 0},
	},
	{
		name:       "Window keeping an expired entry",
		receiver:   windowReceiver,
		input:      windowJSON(4, sec, 5_000_000_000, ent{"x", 1_000_000_000}, ent{"y", 5_000_000_000}),
		wantConfig: Config{4, sec},
	},
	{
		name:       "Window ignoring unknown fields",
		receiver:   windowReceiver,
		input:      `{"extra":[1,2],"dedup":1,"type":"window","capacity":2,"ttl":0,"last":7,"entries":[{"k":"z","at":7,"more":null}]}`,
		want:       windowJSON(2, 0, 7, ent{"z", 7}),
		wantConfig: Config{2, 0},
	},
	{
		name:       "Window missing last decodes as 0",
		receiver:   windowReceiver,
		input:      `{"dedup":1,"type":"window","capacity":2,"ttl":0,"entries":[{"k":"z","at":0}]}`,
		want:       windowJSON(2, 0, 0, ent{"z", 0}),
		wantConfig: Config{2, 0},
	},
	{
		name:       "Window missing entries decodes as none",
		receiver:   windowReceiver,
		input:      `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":7}`,
		want:       windowJSON(2, 0, 7),
		wantConfig: Config{2, 0},
	},
	{
		name:       "Window null entries decodes as none",
		receiver:   windowReceiver,
		input:      `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":7,"entries":null}`,
		want:       windowJSON(2, 0, 7),
		wantConfig: Config{2, 0},
	},
	{
		name:       "Window null key decodes as the zero key",
		receiver:   windowReceiver,
		input:      `{"dedup":1,"type":"window","capacity":2,"ttl":0,"last":1,"entries":[{"k":null,"at":1}]}`,
		want:       windowJSON(2, 0, 1, ent{"", 1}),
		wantConfig: Config{2, 0},
	},
	{
		name:       "Replies with two keys",
		receiver:   repliesReceiver,
		input:      repliesJSON(3, 10*sec, 2_000_000_000, repEnt{"b", 1_000_000_000, 5}, repEnt{"c", 2_000_000_000, 6}),
		wantConfig: Config{3, 10 * sec},
	},
	{
		name:       "Replies without entries",
		receiver:   repliesReceiver,
		input:      repliesJSON(1, 0, -9223372036854775808),
		wantConfig: Config{1, 0},
	},
	{
		name:       "Replies ignoring unknown fields",
		receiver:   repliesReceiver,
		input:      `{"extra":[1,2],"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":7,"entries":[{"k":"z","at":7,"r":9,"v":3,"more":null}]}`,
		want:       repliesJSON(2, 0, 7, repEnt{"z", 7, 9}),
		wantConfig: Config{2, 0},
	},
	{
		name:       "Replies with a reply of other whitespace",
		receiver:   repliesReceiver,
		input:      `{"dedup":1, "type":"replies", "capacity":2, "ttl":0, "last":7, "entries":[{"k":"z", "at":7, "r": 9 }]}`,
		want:       repliesJSON(2, 0, 7, repEnt{"z", 7, 9}),
		wantConfig: Config{2, 0},
	},
	{
		name:       "Versions with two keys",
		receiver:   versionsReceiver,
		input:      versionsJSON(3, 10*sec, 2_000_000_000, verEnt{"b", 1_000_000_000, 5}, verEnt{"c", 2_000_000_000, 6}),
		wantConfig: Config{3, 10 * sec},
	},
	{
		name:       "Versions without entries",
		receiver:   versionsReceiver,
		input:      versionsJSON(1, 0, -9223372036854775808),
		wantConfig: Config{1, 0},
	},
	{
		name:       "Versions with the largest version",
		receiver:   versionsReceiver,
		input:      versionsJSON(2, 0, 1_000_000_000, verEnt{"b", 1_000_000_000, 18446744073709551615}),
		wantConfig: Config{2, 0},
	},
	{
		name:       "Versions ignoring unknown fields",
		receiver:   versionsReceiver,
		input:      `{"extra":[1,2],"dedup":1,"type":"versions","capacity":2,"ttl":0,"last":7,"entries":[{"k":"z","at":7,"v":9,"r":1,"more":null}]}`,
		want:       versionsJSON(2, 0, 7, verEnt{"z", 7, 9}),
		wantConfig: Config{2, 0},
	},
}

// AT-DLV-18, DLV-210, DLV-220: a valid encoding replaces the receiver's bounds and entries
// completely, and decoding then encoding gives the same bytes.
func TestATDLV18DecodeReplaces(t *testing.T) {
	for _, tc := range replaceCases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.receiver()
			if err := r.UnmarshalJSON([]byte(tc.input)); err != nil {
				t.Fatalf("UnmarshalJSON(%s) = %v", tc.input, err)
			}
			want := tc.want
			if want == "" {
				want = tc.input
			}
			if got := encode(t, r); got != want {
				t.Errorf("encoding after decoding is\n%s\nwant\n%s", got, want)
			}
			if got := r.Config(); got != tc.wantConfig {
				t.Errorf("Config() = %+v, want %+v", got, tc.wantConfig)
			}
		})
	}
}

// nullReceivers are the receivers of AT-DLV-18's "null" case.
var nullReceivers = []struct {
	name     string
	receiver func() receiver
}{
	{"Window", windowReceiver},
	{"Replies", repliesReceiver},
	{"Versions", versionsReceiver},
}

// AT-DLV-18, DLV-210: null returns nil and changes nothing.
func TestATDLV18DecodeNull(t *testing.T) {
	for _, tc := range nullReceivers {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.receiver()
			before := encode(t, r)
			if err := r.UnmarshalJSON([]byte("null")); err != nil {
				t.Errorf(`UnmarshalJSON("null") = %v, want nil`, err)
			}
			if got := encode(t, r); got != before {
				t.Errorf("after null the encoding is\n%s\nwant\n%s", got, before)
			}
		})
	}
}

// DLV-210: a decode that fails leaves a zero value unusable, and one that succeeds fills it.
func TestDecodeIntoZeroValue(t *testing.T) {
	var w Window[string]
	if err := w.UnmarshalJSON([]byte(`{"dedup":2}`)); err == nil {
		t.Fatal("UnmarshalJSON of format 2 = nil, want an error")
	}
	if _, err := w.MarshalJSON(); err == nil || err.Error() != "dedup: Window used before NewWindow or UnmarshalJSON" {
		t.Errorf("after a failed decode MarshalJSON error = %v, want the zero-value error", err)
	}
	if err := w.UnmarshalJSON([]byte("null")); err != nil {
		t.Fatalf(`UnmarshalJSON("null") = %v`, err)
	}
	if _, err := w.MarshalJSON(); err == nil {
		t.Error(`after "null" the zero value encodes, want the zero-value error`)
	}
}
