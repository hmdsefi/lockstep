// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	t0   = time.Unix(0, 1_000_000_000).UTC()
	minT = time.Unix(0, math.MinInt64)
	maxT = time.Unix(0, math.MaxInt64)
)

const sec = time.Second

// at returns t0+d.
func at(d time.Duration) time.Time { return t0.Add(d) }

// panicOf runs f and returns the value it panics with, or nil.
func panicOf(f func()) (v any) {
	defer func() { v = recover() }()
	f()
	return nil
}

// wantPanic checks that f panics with a value whose text is want.
func wantPanic(t *testing.T, call, want string, f func()) {
	t.Helper()
	v := panicOf(f)
	if v == nil {
		t.Errorf("%s did not panic, want %q", call, want)
	} else if got := fmt.Sprint(v); got != want {
		t.Errorf("%s panicked with %q, want %q", call, got, want)
	}
}

// encode returns m's encoding.
func encode(t *testing.T, m json.Marshaler) string {
	t.Helper()
	b, err := m.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	return string(b)
}

// check reports a call whose result differs from want.
func check(t *testing.T, call string, got, want bool) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %t, want %t", call, got, want)
	}
}

// ent is a Window[string] entry in an expected encoding.
type ent struct {
	k  string
	at int64
}

// windowJSON returns the encoding of a Window[string] with the given bounds, last and entries.
func windowJSON(capacity int, ttl time.Duration, last int64, entries ...ent) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"dedup":1,"type":"window","capacity":%d,"ttl":%d,"last":%d,"entries":[`, capacity, int64(ttl), last)
	for i, e := range entries {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"k":%q,"at":%d}`, e.k, e.at)
	}
	b.WriteString("]}")
	return b.String()
}

// AT-DLV-02, DLV-020: a constructor given an invalid Config panics with Validate's error.
func TestATDLV02ConstructorsPanic(t *testing.T) {
	bad := Config{0, 0}
	for _, tc := range []struct {
		name string
		f    func()
	}{
		{"NewWindow[string]", func() { NewWindow[string](bad) }},
	} {
		v := panicOf(tc.f)
		err, ok := v.(error)
		if !ok {
			t.Errorf("%s(%+v) panicked with %#v, want an error", tc.name, bad, v)
			continue
		}
		if want := bad.Validate().Error(); err.Error() != want || !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s(%+v) panicked with %q, want %q wrapping ErrInvalidConfig", tc.name, bad, err, want)
		}
	}
	// A value decoded from an encoding with "capacity":3 reports the encoded bounds.
	for _, tc := range decodedConfigCases {
		got, err := tc.decode()
		if want := (Config{3, 7 * sec}); err != nil || got != want {
			t.Errorf("%s: decoded Config() = %+v, %v; want %+v", tc.name, got, err, want)
		}
	}
}

// decodedConfigCases fill a value by UnmarshalJSON from an encoding with "capacity":3 and a TTL of 7s.
var decodedConfigCases = []struct {
	name   string
	decode func() (Config, error)
}{
	{"Window", func() (Config, error) {
		var w Window[string]
		if err := w.UnmarshalJSON([]byte(windowJSON(3, 7*sec, 1_000_000_000, ent{"a", 1_000_000_000}))); err != nil {
			return Config{}, err
		}
		return w.Config(), nil
	}},
}

// AT-DLV-03, DLV-030, DLV-040, DLV-050: now outside the UnixNano range panics and changes nothing;
// the range ends work, and an age of 2^64-1 ns is computed without wrapping.
func TestATDLV03TimeRange(t *testing.T) {
	w := NewWindow[string](Config{4, time.Hour})
	w.Add(t0, "x")
	before := encode(t, w)
	wantPanic(t, `Add(time.Time{}, "a")`, "dedup: now 0001-01-01T00:00:00Z is outside the range of UnixNano",
		func() { w.Add(time.Time{}, "a") })
	wantPanic(t, `Contains(2263-01-01, "a")`, "dedup: now 2263-01-01T00:00:00Z is outside the range of UnixNano",
		func() { w.Contains(time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC), "a") })
	for _, now := range []time.Time{minT.Add(-1), maxT.Add(1)} {
		want := "dedup: now " + now.UTC().Format(time.RFC3339Nano) + " is outside the range of UnixNano"
		wantPanic(t, "Len("+now.String()+")", want, func() { w.Len(now) })
	}
	if got := encode(t, w); got != before {
		t.Errorf("after the panics the encoding is\n%s\nwant\n%s", got, before)
	}

	w = NewWindow[string](Config{4, time.Hour})
	check(t, `Add(minT, "a")`, w.Add(minT, "a"), true)
	check(t, `Contains(maxT, "a")`, w.Contains(maxT, "a"), false)
	check(t, `Add(maxT, "b")`, w.Add(maxT, "b"), true)
	check(t, `Contains(maxT, "b")`, w.Contains(maxT, "b"), true)
	if got, want := encode(t, w), windowJSON(4, time.Hour, math.MaxInt64, ent{"b", math.MaxInt64}); got != want {
		t.Errorf("encoding is\n%s\nwant\n%s", got, want)
	}

	now := time.Now()
	w1, w2 := NewWindow[string](Config{4, time.Hour}), NewWindow[string](Config{4, time.Hour})
	w1.Add(now, "a")
	w2.Add(now.Round(0), "a")
	if a, b := encode(t, w1), encode(t, w2); a != b {
		t.Errorf("a now with a monotonic reading encodes as\n%s\nits Round(0) copy as\n%s", a, b)
	}
}

// AT-DLV-04, DLV-100, DLV-040: a zero Window panics on every method, before the range check, and
// MarshalJSON returns the text as an error.
func TestATDLV04ZeroValue(t *testing.T) {
	const want = "dedup: Window used before NewWindow or UnmarshalJSON"
	var w Window[string]
	wantPanic(t, `Add(t0, "a")`, want, func() { w.Add(t0, "a") })
	wantPanic(t, `Contains(t0, "a")`, want, func() { w.Contains(t0, "a") })
	wantPanic(t, `Len(t0)`, want, func() { w.Len(t0) })
	wantPanic(t, `Forget("a")`, want, func() { w.Forget("a") })
	wantPanic(t, `Config()`, want, func() { w.Config() })
	wantPanic(t, `Add(time.Time{}, "a")`, want, func() { w.Add(time.Time{}, "a") })
	b, err := w.MarshalJSON()
	if err == nil || err.Error() != want || b != nil {
		t.Errorf("MarshalJSON() = %q, %v; want nil, %q", b, err, want)
	}
	// UnmarshalJSON fills a zero value of each type, which then accepts a write.
	for _, tc := range zeroFillCases {
		add, err := tc.fill()
		if err != nil {
			t.Errorf("%s: UnmarshalJSON: %v", tc.name, err)
			continue
		}
		check(t, tc.name+": Add after UnmarshalJSON", add(), true)
	}
}

// zeroFillCases are zero values that UnmarshalJSON fills with a valid encoding and that then accept
// a write.
var zeroFillCases = []struct {
	name string
	fill func() (add func() bool, err error)
}{
	{"Window", func() (func() bool, error) {
		var w Window[string]
		err := w.UnmarshalJSON([]byte(windowJSON(2, 0, 1_000_000_000, ent{"a", 1_000_000_000})))
		return func() bool { return w.Add(at(sec), "b") }, err
	}},
}

// AT-DLV-05, DLV-110, DLV-120: Add records once; Contains records nothing.
func TestATDLV05AddContains(t *testing.T) {
	w := NewWindow[string](Config{4, 10 * sec})
	check(t, `Contains(t0, "a")`, w.Contains(t0, "a"), false)
	check(t, `Add(t0, "a")`, w.Add(t0, "a"), true)
	check(t, `Add(t0, "a") again`, w.Add(t0, "a"), false)
	check(t, `Contains(t0, "a")`, w.Contains(t0, "a"), true)
	check(t, `Contains(t0, "b")`, w.Contains(t0, "b"), false)
	if n := w.Len(t0); n != 1 {
		t.Errorf("Len(t0) = %d, want 1", n)
	}
}

// AT-DLV-06, DLV-050: an entry exactly TTL old is gone.
func TestATDLV06TTLBoundary(t *testing.T) {
	w := NewWindow[string](Config{4, 10 * sec})
	w.Add(t0, "a")
	check(t, `Contains(t0+10s-1ns, "a")`, w.Contains(at(10*sec-1), "a"), true)
	check(t, `Contains(t0+10s, "a")`, w.Contains(at(10*sec), "a"), false)
	check(t, `Add(t0+10s, "a")`, w.Add(at(10*sec), "a"), true)
	check(t, `Contains(t0+20s-1ns, "a")`, w.Contains(at(20*sec-1), "a"), true)
	check(t, `Contains(t0+20s, "a")`, w.Contains(at(20*sec), "a"), false)
}

// AT-DLV-07, DLV-110: Add of a live key does not refresh it.
func TestATDLV07NoRefresh(t *testing.T) {
	w := NewWindow[string](Config{4, 10 * sec})
	w.Add(t0, "a")
	check(t, `Add(t0+5s, "a")`, w.Add(at(5*sec), "a"), false)
	check(t, `Contains(t0+10s, "a")`, w.Contains(at(10*sec), "a"), false)
}

// AT-DLV-08, DLV-060, DLV-070: a full window removes the oldest entry in write order.
func TestATDLV08CapacityEviction(t *testing.T) {
	w := NewWindow[string](Config{2, 0})
	for _, k := range []string{"a", "b", "c"} {
		check(t, `Add(t0, "`+k+`")`, w.Add(t0, k), true)
	}
	check(t, `Contains(t0, "a")`, w.Contains(t0, "a"), false)
	check(t, `Contains(t0, "b")`, w.Contains(t0, "b"), true)
	check(t, `Contains(t0, "c")`, w.Contains(t0, "c"), true)
	check(t, `Add(t0, "b")`, w.Add(t0, "b"), false)
	check(t, `Add(t0, "d")`, w.Add(t0, "d"), true)
	check(t, `Contains(t0, "b")`, w.Contains(t0, "b"), false)
	check(t, `Contains(t0, "c")`, w.Contains(t0, "c"), true)
	check(t, `Contains(t0, "d")`, w.Contains(t0, "d"), true)
	if got, want := encode(t, w), windowJSON(2, 0, 1e9, ent{"c", 1e9}, ent{"d", 1e9}); got != want {
		t.Errorf("encoding is\n%s\nwant\n%s", got, want)
	}
	check(t, `Contains(t0+1000h, "c")`, w.Contains(at(1000*time.Hour), "c"), true)
}

// AT-DLV-09, DLV-060, DLV-070, DLV-160: a write removes expired entries before capacity applies;
// Len counts without removing.
func TestATDLV09ExpiredBeforeCapacity(t *testing.T) {
	w := NewWindow[string](Config{2, 10 * sec})
	check(t, `Add(t0, "a")`, w.Add(t0, "a"), true)
	check(t, `Add(t0+5s, "b")`, w.Add(at(5*sec), "b"), true)
	check(t, `Add(t0+10s, "c")`, w.Add(at(10*sec), "c"), true)
	check(t, `Contains(t0+10s, "a")`, w.Contains(at(10*sec), "a"), false)
	check(t, `Contains(t0+10s, "b")`, w.Contains(at(10*sec), "b"), true)
	check(t, `Contains(t0+10s, "c")`, w.Contains(at(10*sec), "c"), true)
	want := windowJSON(2, 10*sec, 11e9, ent{"b", 6e9}, ent{"c", 11e9})
	if got := encode(t, w); got != want {
		t.Errorf("encoding is\n%s\nwant\n%s", got, want)
	}
	for range 2 {
		if n := w.Len(at(15 * sec)); n != 1 {
			t.Errorf("Len(t0+15s) = %d, want 1", n)
		}
	}
	if got := encode(t, w); got != want {
		t.Errorf("after Len the encoding is\n%s\nwant\n%s", got, want)
	}
}

// AT-DLV-10, DLV-040: an earlier now is treated as last; reads do not move last.
func TestATDLV10BackwardNow(t *testing.T) {
	w := NewWindow[string](Config{4, 10 * sec})
	w.Add(at(20*sec), "x")
	check(t, `Add(t0, "y")`, w.Add(t0, "y"), true)
	if got, want := encode(t, w), windowJSON(4, 10*sec, 21e9, ent{"x", 21e9}, ent{"y", 21e9}); got != want {
		t.Errorf("encoding is\n%s\nwant\n%s", got, want)
	}
	check(t, `Contains(t0, "x")`, w.Contains(t0, "x"), true)
	check(t, `Contains(t0+29s, "y")`, w.Contains(at(29*sec), "y"), true)
	check(t, `Contains(t0+30s, "y")`, w.Contains(at(30*sec), "y"), false)
	check(t, `Contains(t0+100s, "x")`, w.Contains(at(100*sec), "x"), false)
	check(t, `Add(t0+21s, "z")`, w.Add(at(21*sec), "z"), true)
	check(t, `Contains(t0+30s, "z")`, w.Contains(at(30*sec), "z"), true)
	if got, want := encode(t, w), windowJSON(4, 10*sec, 22e9, ent{"x", 21e9}, ent{"y", 21e9}, ent{"z", 22e9}); got != want {
		t.Errorf("encoding is\n%s\nwant\n%s", got, want)
	}
}

// AT-DLV-11, DLV-090: Forget removes a stored entry, live or not, and keeps last.
func TestATDLV11Forget(t *testing.T) {
	w := NewWindow[string](Config{4, 10 * sec})
	w.Add(t0, "a")
	before := encode(t, w)
	w.Forget("b")
	if got := encode(t, w); got != before {
		t.Errorf(`after Forget("b") the encoding is %s, want %s`, got, before)
	}
	w.Forget("a")
	check(t, `Contains(t0, "a")`, w.Contains(t0, "a"), false)
	if got, want := encode(t, w), windowJSON(4, 10*sec, 1e9); got != want {
		t.Errorf(`after Forget("a") the encoding is %s, want %s`, got, want)
	}
	check(t, `Add(t0, "a")`, w.Add(t0, "a"), true)

	w = NewWindow[string](Config{4, 10 * sec})
	w.Add(t0, "a")
	w.Add(at(5*sec), "b")
	check(t, `Contains(t0+10s, "a")`, w.Contains(at(10*sec), "a"), false) // expired, still stored
	w.Forget("a")
	if got, want := encode(t, w), windowJSON(4, 10*sec, 6e9, ent{"b", 6e9}); got != want {
		t.Errorf(`after Forget of an expired key the encoding is %s, want %s`, got, want)
	}
}

// readsCases are the values of AT-DLV-12: each holds three entries, of which one has expired at
// now, and lists its read calls.
var readsCases = []struct {
	name string
	// make returns the value, the now at which one of its entries has expired, and its read calls.
	make func() (json.Marshaler, []func(now time.Time))
}{
	{"Window", func() (json.Marshaler, []func(time.Time)) {
		w := NewWindow[string](Config{4, 10 * sec})
		w.Add(t0, "a")
		w.Add(at(5*sec), "b")
		w.Add(at(8*sec), "c")
		return w, []func(time.Time){
			func(now time.Time) { w.Contains(now, "b") },            // hit
			func(now time.Time) { w.Contains(now, "z") },            // miss
			func(now time.Time) { w.Contains(now, "a") },            // expired
			func(time.Time) { w.Contains(t0, "a") },                 // earlier now
			func(now time.Time) { w.Len(now) },                      // Len
			func(time.Time) { w.Config() },                          // Config
			func(time.Time) { _, _ = w.MarshalJSON() },              // MarshalJSON
			func(time.Time) { w.Contains(at(1000*time.Hour), "c") }, // later now, all expired
		}
	}},
}

// AT-DLV-12, DLV-080: read calls change nothing.
func TestATDLV12ReadsChangeNothing(t *testing.T) {
	now := at(12 * sec)
	for _, tc := range readsCases {
		t.Run(tc.name, func(t *testing.T) {
			v, reads := tc.make()
			before := encode(t, v)
			for i, read := range reads {
				read(now)
				if got := encode(t, v); got != before {
					t.Fatalf("read %d changed the encoding to\n%s\nfrom\n%s", i, got, before)
				}
			}
		})
	}
}

// failKey is a key whose encoding fails for the value 2.
type failKey int

var errFailKey = errors.New("key 2 does not encode")

func (k failKey) MarshalJSON() ([]byte, error) {
	if k == 2 {
		return nil, errFailKey
	}
	return strconv.AppendInt(nil, int64(k), 10), nil
}

// encodingCases are the encodings of AT-DLV-16: either want or, for a failing encoding, wantErr
// (a prefix of the error text) and wantIs (an error it wraps).
var encodingCases = []struct {
	name    string
	marshal func() ([]byte, error)
	want    string
	wantErr string
	wantIs  error
}{
	{
		name:    "new Window",
		marshal: NewWindow[string](Config{1, 0}).MarshalJSON,
		want:    `{"dedup":1,"type":"window","capacity":1,"ttl":0,"last":-9223372036854775808,"entries":[]}`,
	},
	{
		name: "Window with two keys",
		marshal: func() ([]byte, error) {
			w := NewWindow[string](Config{3, 10 * sec})
			w.Add(t0, "a")
			w.Add(at(sec), "b")
			return w.MarshalJSON()
		},
		want: `{"dedup":1,"type":"window","capacity":3,"ttl":10000000000,"last":2000000000,"entries":[{"k":"a","at":1000000000},{"k":"b","at":2000000000}]}`,
	},
	{
		name: "Window key that does not encode",
		marshal: func() ([]byte, error) {
			w := NewWindow[failKey](Config{3, 0})
			w.Add(t0, 1)
			w.Add(t0, 2)
			return w.MarshalJSON()
		},
		wantErr: "dedup: encode entry 1: ",
		wantIs:  errFailKey,
	},
}

// AT-DLV-16, DLV-200: exact encodings, and the error of an entry that does not encode.
func TestATDLV16Encoding(t *testing.T) {
	for _, tc := range encodingCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.marshal()
			if tc.wantErr == "" {
				if err != nil || string(b) != tc.want {
					t.Errorf("MarshalJSON() = %s, %v; want\n%s", b, err, tc.want)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), tc.wantErr) || b != nil {
				t.Errorf("MarshalJSON() = %q, %v; want nil and an error starting %q", b, err, tc.wantErr)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("MarshalJSON() error %v does not wrap %v", err, tc.wantIs)
			}
		})
	}
}

// AT-DLV-20, DLV-400, DLV-410, DLV-420: a Window on a retry schedule catches the duplicate exactly
// when TTL exceeds the age at its handling and capacity keeps the key.
func TestATDLV20RetrySchedule(t *testing.T) {
	const ms = time.Millisecond
	type add struct {
		d    time.Duration
		k    string
		want bool
	}
	for _, tc := range []struct {
		name string
		cfg  Config
		adds []add
	}{
		{"TTL equals the age", Config{4, 110 * ms}, []add{{60 * ms, "k", true}, {170 * ms, "k", true}}},
		{"TTL above the age", Config{4, 110*ms + 1}, []add{{60 * ms, "k", true}, {170 * ms, "k", false}}},
		{"capacity removes the key", Config{1, sec}, []add{{60 * ms, "k", true}, {100 * ms, "other", true}, {170 * ms, "k", true}}},
		{"clock step, TTL equals the age", Config{4, 160 * ms}, []add{{60 * ms, "k", true}, {220 * ms, "k", true}}},
		{"clock step, TTL above the age", Config{4, 160*ms + 1}, []add{{60 * ms, "k", true}, {220 * ms, "k", false}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWindow[string](tc.cfg)
			for _, a := range tc.adds {
				check(t, fmt.Sprintf("Add(t0+%v, %q)", a.d, a.k), w.Add(at(a.d), a.k), a.want)
			}
		})
	}
}
