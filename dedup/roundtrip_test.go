// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/persist"
)

// opKind is the kind of a random call of AT-DLV-17.
type opKind int

const (
	opWrite  opKind = iota // Add, Put or Accept
	opRead                 // Contains or Get
	opLen                  // Len
	opForget               // Forget
)

// op is one random call: a key index from the pool, the now to pass and a number for the reply or
// version a write stores.
type op struct {
	kind opKind
	key  int
	now  time.Time
	n    int
}

// Parameters of the round-trip driver (AT-DLV-17).
const (
	rtCalls    = 2000
	rtEvery    = 100
	rtKeyPool  = 20
	rtCapacity = 8
	rtTTL      = 50 * time.Millisecond
)

// rtConfig is the bounds of every value the driver makes.
var rtConfig = Config{rtCapacity, rtTTL}

// genOps returns n random calls: writes, reads, Len and Forget over a pool of keys, with now
// mostly increasing and sometimes stepping back.
func genOps(rng *rand.Rand, n int) []op {
	ops := make([]op, n)
	now := t0
	for i := range ops {
		if rng.IntN(10) == 0 {
			now = now.Add(-time.Duration(1+rng.IntN(30)) * time.Millisecond)
		} else {
			now = now.Add(time.Duration(rng.IntN(21)) * time.Millisecond)
		}
		var kind opKind
		switch r := rng.IntN(100); {
		case r < 45:
			kind = opWrite
		case r < 75:
			kind = opRead
		case r < 85:
			kind = opLen
		default:
			kind = opForget
		}
		ops[i] = op{kind: kind, key: rng.IntN(rtKeyPool), now: now, n: rng.IntN(1000)}
	}
	return ops
}

// A subject is a value under test in the round-trip driver, whatever its type and key.
type subject interface {
	// call performs op and returns its result as text ("" when it has none).
	call(op op) string
	// marshal returns the value's encoding.
	marshal() ([]byte, error)
	// decodeFresh returns a new value decoded from the encoding of this one.
	decodeFresh() (subject, error)
}

// windowSubject drives a Window with keys of type K made by key.
type windowSubject[K comparable] struct {
	w   *Window[K]
	key func(i int) K
}

func newWindowSubject[K comparable](key func(i int) K) subject {
	return windowSubject[K]{NewWindow[K](rtConfig), key}
}

func (s windowSubject[K]) call(o op) string {
	k := s.key(o.key)
	switch o.kind {
	case opWrite:
		return strconv.FormatBool(s.w.Add(o.now, k))
	case opRead:
		return strconv.FormatBool(s.w.Contains(o.now, k))
	case opLen:
		return strconv.Itoa(s.w.Len(o.now))
	default:
		s.w.Forget(k)
		return ""
	}
}

func (s windowSubject[K]) marshal() ([]byte, error) { return s.w.MarshalJSON() }

func (s windowSubject[K]) decodeFresh() (subject, error) {
	b, err := s.w.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var w Window[K]
	if err := w.UnmarshalJSON(b); err != nil {
		return nil, err
	}
	return windowSubject[K]{&w, s.key}, nil
}

// sumReply is the reply type of AT-DLV-17.
type sumReply = struct {
	Sum  int64
	Note string
}

// repliesSubject drives a Replies with keys of type K made by key and replies of type sumReply.
type repliesSubject[K comparable] struct {
	c   *Replies[K, sumReply]
	key func(i int) K
}

func newRepliesSubject[K comparable](key func(i int) K) subject {
	return repliesSubject[K]{NewReplies[K, sumReply](rtConfig), key}
}

func (s repliesSubject[K]) call(o op) string {
	k := s.key(o.key)
	switch o.kind {
	case opWrite:
		return strconv.FormatBool(s.c.Put(o.now, k, sumReply{Sum: int64(o.n), Note: "n" + strconv.Itoa(o.n)}))
	case opRead:
		r, ok := s.c.Get(o.now, k)
		return fmt.Sprintf("%+v %t", r, ok)
	case opLen:
		return strconv.Itoa(s.c.Len(o.now))
	default:
		s.c.Forget(k)
		return ""
	}
}

func (s repliesSubject[K]) marshal() ([]byte, error) { return s.c.MarshalJSON() }

func (s repliesSubject[K]) decodeFresh() (subject, error) {
	b, err := s.c.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var c Replies[K, sumReply]
	if err := c.UnmarshalJSON(b); err != nil {
		return nil, err
	}
	return repliesSubject[K]{&c, s.key}, nil
}

// versionsSubject drives a Versions with keys of type K made by key.
type versionsSubject[K comparable] struct {
	v   *Versions[K]
	key func(i int) K
}

func newVersionsSubject[K comparable](key func(i int) K) subject {
	return versionsSubject[K]{NewVersions[K](rtConfig), key}
}

func (s versionsSubject[K]) call(o op) string {
	k := s.key(o.key)
	switch o.kind {
	case opWrite:
		return strconv.FormatBool(s.v.Accept(o.now, k, uint64(o.n))) //nolint:gosec // G115: o.n is in [0, 1000)
	case opRead:
		n, ok := s.v.Get(o.now, k)
		return fmt.Sprintf("%d %t", n, ok)
	case opLen:
		return strconv.Itoa(s.v.Len(o.now))
	default:
		s.v.Forget(k)
		return ""
	}
}

func (s versionsSubject[K]) marshal() ([]byte, error) { return s.v.MarshalJSON() }

func (s versionsSubject[K]) decodeFresh() (subject, error) {
	b, err := s.v.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var v Versions[K]
	if err := v.UnmarshalJSON(b); err != nil {
		return nil, err
	}
	return versionsSubject[K]{&v, s.key}, nil
}

// pairKey is the struct key of AT-DLV-17.
type pairKey struct {
	A string
	B int
}

func stringKey(i int) string  { return "key-" + strconv.Itoa(i) }
func pairKeyOf(i int) pairKey { return pairKey{A: "a" + strconv.Itoa(i%3), B: i} }

// roundTripCases are the values of AT-DLV-17.
var roundTripCases = []struct {
	name string
	new  func() subject
}{
	{"Window[string]", func() subject { return newWindowSubject(stringKey) }},
	{"Window[struct]", func() subject { return newWindowSubject(pairKeyOf) }},
	{"Replies[string]", func() subject { return newRepliesSubject(stringKey) }},
	{"Replies[struct]", func() subject { return newRepliesSubject(pairKeyOf) }},
	{"Versions[string]", func() subject { return newVersionsSubject(stringKey) }},
	{"Versions[struct]", func() subject { return newVersionsSubject(pairKeyOf) }},
}

// AT-DLV-17, DLV-220: after every 100th call the value is encoded and decoded into a fresh value
// with the same encoding; the original and the decoded copy then get the same next 100 calls and
// return the same results.
func TestATDLV17RoundTrip(t *testing.T) {
	for _, tc := range roundTripCases {
		t.Run(tc.name, func(t *testing.T) {
			ops := genOps(rand.New(rand.NewPCG(1, 2)), rtCalls) //nolint:gosec // G404: a seeded test PRNG makes the calls reproducible (AT-DLV-17)
			x := tc.new()
			var y subject // the decoded copy, or nil before the first checkpoint
			checkpoints, withEntries := 0, 0
			for i, o := range ops {
				got := x.call(o)
				if y != nil {
					if want := y.call(o); got != want {
						t.Fatalf("call %d (%+v): original returned %q, decoded copy %q", i, o, got, want)
					}
				}
				if (i+1)%rtEvery != 0 {
					continue
				}
				xb, err := x.marshal()
				if err != nil {
					t.Fatalf("call %d: encode: %v", i, err)
				}
				if y != nil {
					if yb, err := y.marshal(); err != nil || string(yb) != string(xb) {
						t.Fatalf("call %d: after the same calls the decoded copy encodes as\n%s\n%v\nwant\n%s", i, yb, err, xb)
					}
				}
				if y, err = x.decodeFresh(); err != nil {
					t.Fatalf("call %d: decode: %v", i, err)
				}
				yb, err := y.marshal()
				if err != nil || string(yb) != string(xb) {
					t.Fatalf("call %d: the decoded copy encodes as\n%s\n%v\nwant\n%s", i, yb, err, xb)
				}
				checkpoints++
				if !strings.HasSuffix(string(xb), `"entries":[]}`) {
					withEntries++
				}
			}
			if checkpoints != rtCalls/rtEvery || withEntries < checkpoints/2 {
				t.Errorf("%d checkpoints, %d with entries: the calls do not exercise the value", checkpoints, withEntries)
			}
		})
	}
}

// st is the persisted state of AT-DLV-19: pointer fields, one of them nil.
type st struct {
	W *Window[string]
	R *Replies[string, int64]
	V *Versions[string]
	N *Window[string]
}

// A stateCase persists a state holding values through persist.JSON.
type stateCase struct {
	name  string
	check func(t *testing.T)
}

// persistRoundTrip encodes s through persist.JSON, checks the bytes against want when given, decodes
// them, and checks that each part of the decoded state encodes as the same part of s does (a nil
// pointer as null).
func persistRoundTrip[S any](t *testing.T, s S, want string, parts func(S) []any) {
	t.Helper()
	codec := persist.JSON[S]()
	b, err := codec.Encode(s)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if want != "" && string(b) != want {
		t.Errorf("Encode = %s, want\n%s", b, want)
	}
	got, err := codec.Decode(b)
	if err != nil {
		t.Fatalf("Decode(%s): %v", b, err)
	}
	orig, dec := parts(s), parts(got)
	for i := range orig {
		ob, err := json.Marshal(orig[i])
		if err != nil {
			t.Fatalf("part %d: Marshal: %v", i, err)
		}
		db, err := json.Marshal(dec[i])
		if err != nil {
			t.Fatalf("part %d: Marshal of the decoded part: %v", i, err)
		}
		if string(ob) != string(db) {
			t.Errorf("part %d decoded as\n%s\nwant\n%s", i, db, ob)
		}
	}
}

// persistBad checks that a state with a Window held by value encodes it as {} and that decoding the
// bytes fails with an ErrInvalidEncoding error ending wantEnd.
func persistBad[S any](t *testing.T, s S, wantEncoded, wantEnd string) {
	t.Helper()
	codec := persist.JSON[S]()
	b, err := codec.Encode(s)
	if err != nil || string(b) != wantEncoded {
		t.Fatalf("Encode = %s, %v; want %s", b, err, wantEncoded)
	}
	_, err = codec.Decode(b)
	if err == nil || !errors.Is(err, ErrInvalidEncoding) || !strings.HasSuffix(err.Error(), wantEnd) {
		t.Errorf("Decode(%s) error = %v; want one wrapping ErrInvalidEncoding and ending %q", b, err, wantEnd)
	}
}

// persistCases are the states of AT-DLV-19 held in pointer fields.
var persistCases = []stateCase{
	{"Window, Replies and Versions in pointer fields", func(t *testing.T) {
		w := NewWindow[string](Config{3, 10 * sec})
		w.Add(t0, "a")
		w.Add(at(sec), "b")
		r := NewReplies[string, int64](Config{2, 0})
		r.Put(t0, "r1", 7)
		r.Put(at(sec), "r2", -8)
		v := NewVersions[string](Config{2, time.Minute})
		v.Accept(t0, "e", 3)
		v.Accept(at(2*sec), "f", 4)
		v.Accept(at(3*sec), "e", 5) // moves e last
		want := `{"W":` + windowJSON(3, 10*sec, 2_000_000_000, ent{"a", 1_000_000_000}, ent{"b", 2_000_000_000}) +
			`,"R":{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":2000000000,"entries":[{"k":"r1","at":1000000000,"r":7},{"k":"r2","at":2000000000,"r":-8}]}` +
			`,"V":` + versionsJSON(2, time.Minute, 4_000_000_000, verEnt{"f", 3_000_000_000, 4}, verEnt{"e", 4_000_000_000, 5}) +
			`,"N":null}`
		s := st{W: w, R: r, V: v}
		persistRoundTrip(t, s, want, func(s st) []any { return []any{s.W, s.R, s.V, s.N} })
		got, err := persist.JSON[st]().Decode([]byte(want))
		if err != nil || got.W == nil || got.R == nil || got.V == nil || got.N != nil {
			t.Fatalf("Decode: W=%v R=%v V=%v N=%v err=%v; want W, R, V set and N nil", got.W, got.R, got.V, got.N, err)
		}
		check(t, `decoded Window Contains(t0, "a")`, got.W.Contains(t0, "a"), true)
		check(t, `decoded Window Add(t0+1s, "b")`, got.W.Add(at(sec), "b"), false)
		if r, ok := got.R.Get(at(sec), "r2"); r != -8 || !ok {
			t.Errorf(`decoded Replies Get("r2") = %d, %t; want -8, true`, r, ok)
		}
		check(t, `decoded Replies Put(t0+1s, "r1", 9)`, got.R.Put(at(sec), "r1", 9), false)
		if n, ok := got.V.Get(at(4*sec), "e"); n != 5 || !ok {
			t.Errorf(`decoded Versions Get("e") = %d, %t; want 5, true`, n, ok)
		}
		check(t, `decoded Versions Accept(t0+4s, "e", 5)`, got.V.Accept(at(4*sec), "e", 5), false)
	}},
}

// badStateCases are the states of AT-DLV-19 that hold a value in a non-pointer field.
var badStateCases = []stateCase{
	{"Window in a non-pointer field", func(t *testing.T) {
		type bad struct{ W Window[string] }
		var s bad
		if err := s.W.UnmarshalJSON([]byte(windowJSON(3, 10*sec, 1_000_000_000, ent{"a", 1_000_000_000}))); err != nil {
			t.Fatal(err)
		}
		persistBad(t, s, `{"W":{}}`, "format 0, want 1")
	}},
	{"Replies in a non-pointer field", func(t *testing.T) {
		type bad struct{ R Replies[string, int64] }
		var s bad
		if err := s.R.UnmarshalJSON([]byte(repliesJSON(3, 10*sec, 1_000_000_000, repEnt{"a", 1_000_000_000, 1}))); err != nil {
			t.Fatal(err)
		}
		persistBad(t, s, `{"R":{}}`, "format 0, want 1")
	}},
	{"Versions in a non-pointer field", func(t *testing.T) {
		type bad struct{ V Versions[string] }
		var s bad
		if err := s.V.UnmarshalJSON([]byte(versionsJSON(3, 10*sec, 1_000_000_000, verEnt{"a", 1_000_000_000, 1}))); err != nil {
			t.Fatal(err)
		}
		persistBad(t, s, `{"V":{}}`, "format 0, want 1")
	}},
}

// AT-DLV-19, DLV-230: values in pointer fields of a persisted state survive persist.JSON, a nil
// pointer stays nil, and a value in a non-pointer field encodes as {} and fails to decode.
func TestATDLV19PersistedState(t *testing.T) {
	for _, tc := range persistCases {
		t.Run(tc.name, tc.check)
	}
	for _, tc := range badStateCases {
		t.Run(tc.name, tc.check)
	}
}
