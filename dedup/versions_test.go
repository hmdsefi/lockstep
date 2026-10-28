// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// checkVersion reports a Get whose result differs from want.
func checkVersion(t *testing.T, call string, got uint64, ok bool, want uint64, wantOK bool) {
	t.Helper()
	checkReply(t, call, got, ok, want, wantOK)
}

// versionsJSON returns the encoding of a Versions[string] with the given bounds, last and entries.
func versionsJSON(capacity int, ttl time.Duration, last int64, entries ...verEnt) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"dedup":1,"type":"versions","capacity":%d,"ttl":%d,"last":%d,"entries":[`, capacity, int64(ttl), last)
	for i, e := range entries {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"k":%q,"at":%d,"v":%d}`, e.k, e.at, e.v)
	}
	b.WriteString("]}")
	return b.String()
}

// verEnt is a Versions[string] entry in an expected encoding.
type verEnt struct {
	k  string
	at int64
	v  uint64
}

// AT-DLV-14, DLV-140: Accept compares first; an accepted newer version moves the entry last with
// the effective now, and a rejected one changes nothing for the key.
func TestATDLV14Versions(t *testing.T) {
	v := NewVersions[string](Config{2, 10 * sec})
	check(t, `Accept(t0, "a", 5)`, v.Accept(t0, "a", 5), true)
	check(t, `Accept(t0, "a", 5)`, v.Accept(t0, "a", 5), false)
	check(t, `Accept(t0, "a", 4)`, v.Accept(t0, "a", 4), false)
	check(t, `Accept(t0+8s, "a", 6)`, v.Accept(at(8*sec), "a", 6), true)
	n, ok := v.Get(at(15*sec), "a") // ages from t0+8s
	checkVersion(t, `Get(t0+15s, "a")`, n, ok, 6, true)
	check(t, `Accept(t0+9s, "a", 1)`, v.Accept(at(9*sec), "a", 1), false)
	n, ok = v.Get(at(18*sec), "a") // the rejection did not refresh
	checkVersion(t, `Get(t0+18s, "a")`, n, ok, 0, false)

	// Move to back.
	v = NewVersions[string](Config{2, 0})
	check(t, `Accept(t0, "a", 1)`, v.Accept(t0, "a", 1), true)
	check(t, `Accept(t0+1s, "b", 1)`, v.Accept(at(sec), "b", 1), true)
	check(t, `Accept(t0+2s, "a", 2)`, v.Accept(at(2*sec), "a", 2), true)
	check(t, `Accept(t0+3s, "c", 1)`, v.Accept(at(3*sec), "c", 1), true)
	n, ok = v.Get(at(3*sec), "b")
	checkVersion(t, `Get("b")`, n, ok, 0, false)
	n, ok = v.Get(at(3*sec), "a")
	checkVersion(t, `Get("a")`, n, ok, 2, true)
	n, ok = v.Get(at(3*sec), "c")
	checkVersion(t, `Get("c")`, n, ok, 1, true)
	if got, want := encode(t, v), versionsJSON(2, 0, 4e9, verEnt{"a", 3e9, 2}, verEnt{"c", 4e9, 1}); got != want {
		t.Errorf("encoding is\n%s\nwant\n%s", got, want)
	}
}

// DLV-140, DLV-040, DLV-060: a rejected Accept is a write call, so it sets last and removes expired
// entries, but leaves the key's entry, its time and its position alone.
func TestVersionsRejectedAcceptIsAWrite(t *testing.T) {
	v := NewVersions[string](Config{4, 10 * sec})
	v.Accept(t0, "old", 1)
	v.Accept(at(5*sec), "a", 7)
	v.Accept(at(6*sec), "b", 1)
	check(t, `Accept(t0+12s, "a", 7)`, v.Accept(at(12*sec), "a", 7), false)
	// old expired and was removed; a and b keep their times and order; last moved to t0+12s (13e9 ns).
	want := versionsJSON(4, 10*sec, 13e9, verEnt{"a", 6e9, 7}, verEnt{"b", 7e9, 1})
	if got := encode(t, v); got != want {
		t.Errorf("encoding is\n%s\nwant\n%s", got, want)
	}
}

// DLV-040, DLV-140: an earlier now is treated as last, so an accepted version is stamped with it.
func TestVersionsBackwardNow(t *testing.T) {
	v := NewVersions[string](Config{4, 10 * sec})
	v.Accept(at(20*sec), "x", 1)
	check(t, `Accept(t0, "x", 2)`, v.Accept(t0, "x", 2), true)
	if got, want := encode(t, v), versionsJSON(4, 10*sec, 21e9, verEnt{"x", 21e9, 2}); got != want {
		t.Errorf("encoding is\n%s\nwant\n%s", got, want)
	}
}

// DLV-140: the largest version is accepted once and nothing is greater.
func TestVersionsMaxVersion(t *testing.T) {
	v := NewVersions[string](Config{2, 0})
	check(t, `Accept(max)`, v.Accept(t0, "a", math.MaxUint64), true)
	check(t, `Accept(max) again`, v.Accept(t0, "a", math.MaxUint64), false)
	n, ok := v.Get(t0, "a")
	checkVersion(t, `Get`, n, ok, math.MaxUint64, true)
	var d Versions[string]
	if err := d.UnmarshalJSON([]byte(encode(t, v))); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	n, ok = d.Get(t0, "a")
	checkVersion(t, `decoded Get`, n, ok, math.MaxUint64, true)
}

// AT-DLV-15, DLV-150: a key without an entry accepts any version, including 0.
func TestATDLV15ForgottenEntity(t *testing.T) {
	v := NewVersions[string](Config{1, 0})
	check(t, `Accept(t0, "a", 5)`, v.Accept(t0, "a", 5), true)
	check(t, `Accept(t0, "b", 1)`, v.Accept(t0, "b", 1), true) // removes a
	check(t, `Accept(t0, "a", 3)`, v.Accept(t0, "a", 3), true)

	v = NewVersions[string](Config{1, 0})
	check(t, `Accept(t0, "z", 0)`, v.Accept(t0, "z", 0), true)
	check(t, `Accept(t0, "z", 0) again`, v.Accept(t0, "z", 0), false)

	// Forget and expiry also leave the key without an entry.
	v = NewVersions[string](Config{4, 10 * sec})
	v.Accept(t0, "a", 9)
	v.Forget("a")
	check(t, `Accept after Forget`, v.Accept(t0, "a", 3), true)
	check(t, `Accept after expiry`, v.Accept(at(10*sec), "a", 2), true)
}
