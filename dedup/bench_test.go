// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"strconv"
	"testing"
	"time"
)

// Benchmark sizes of DLV §9: the entries a value holds.
var benchSizes = []int{10_000, 100_000}

// benchTTL is long enough that no entry expires during a benchmark, which runs at one fixed now.
const benchTTL = time.Hour

// benchRun runs f once per size as a sub-benchmark named by the size.
func benchRun(b *testing.B, f func(b *testing.B, n int)) {
	for _, n := range benchSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) { f(b, n) })
	}
}

// benchKeys returns n distinct keys.
func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	return keys
}

// fullWindow returns a Window at capacity n holding keys[:n], and keys, of which the first n are
// held and the next n are not.
func fullWindow(n int) (*Window[string], []string) {
	keys := benchKeys(2 * n)
	w := NewWindow[string](Config{n, benchTTL})
	for _, k := range keys[:n] {
		w.Add(t0, k)
	}
	return w, keys
}

// fullReplies returns a Replies[string, int64] at capacity n holding keys[:n], and keys.
func fullReplies(n int) (*Replies[string, int64], []string) {
	keys := benchKeys(2 * n)
	c := NewReplies[string, int64](Config{n, benchTTL})
	for i, k := range keys[:n] {
		c.Put(t0, k, int64(i))
	}
	return c, keys
}

// BenchmarkWindowAddEvict measures Add of a new key to a full window: the eviction path. A key is
// evicted n adds after it was added, so cycling through 2n keys makes every Add new.
func BenchmarkWindowAddEvict(b *testing.B) {
	benchRun(b, func(b *testing.B, n int) {
		b.ReportAllocs()
		w, keys := fullWindow(n)
		i := 0
		for b.Loop() {
			if !w.Add(t0, keys[(n+i)%(2*n)]) {
				b.Fatal("Add of a new key returned false")
			}
			i++
		}
	})
}

// BenchmarkWindowContains measures Contains of a key the window holds.
func BenchmarkWindowContains(b *testing.B) {
	benchRun(b, func(b *testing.B, n int) {
		b.ReportAllocs()
		w, keys := fullWindow(n)
		i := 0
		for b.Loop() {
			if !w.Contains(t0, keys[i%n]) {
				b.Fatal("Contains of a held key returned false")
			}
			i++
		}
	})
}

// BenchmarkRepliesPutGet measures Put of a new reply to a full cache, then Get of it.
func BenchmarkRepliesPutGet(b *testing.B) {
	benchRun(b, func(b *testing.B, n int) {
		b.ReportAllocs()
		c, keys := fullReplies(n)
		i := 0
		for b.Loop() {
			k := keys[(n+i)%(2*n)]
			if !c.Put(t0, k, int64(i)) {
				b.Fatal("Put of a new key returned false")
			}
			if r, ok := c.Get(t0, k); !ok || r != int64(i) {
				b.Fatalf("Get = %d, %t; want %d, true", r, ok, i)
			}
			i++
		}
	})
}

// BenchmarkVersionsAccept measures Accept of a higher version for a key the value holds, which
// moves its entry last.
func BenchmarkVersionsAccept(b *testing.B) {
	benchRun(b, func(b *testing.B, n int) {
		b.ReportAllocs()
		keys := benchKeys(n)
		v := NewVersions[string](Config{n, benchTTL})
		for _, k := range keys {
			v.Accept(t0, k, 1)
		}
		i := 0
		for b.Loop() {
			if !v.Accept(t0, keys[i%n], uint64(i)+2) { //nolint:gosec // G115: i is not negative
				b.Fatal("Accept of a higher version returned false")
			}
			i++
		}
	})
}

// BenchmarkRepliesMarshal measures MarshalJSON of a full Replies[string, int64].
func BenchmarkRepliesMarshal(b *testing.B) {
	benchRun(b, func(b *testing.B, n int) {
		b.ReportAllocs()
		c, _ := fullReplies(n)
		for b.Loop() {
			if _, err := c.MarshalJSON(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRepliesUnmarshal measures UnmarshalJSON of the encoding of a full Replies[string, int64].
func BenchmarkRepliesUnmarshal(b *testing.B) {
	benchRun(b, func(b *testing.B, n int) {
		b.ReportAllocs()
		c, _ := fullReplies(n)
		data, err := c.MarshalJSON()
		if err != nil {
			b.Fatal(err)
		}
		for b.Loop() {
			var d Replies[string, int64]
			if err := d.UnmarshalJSON(data); err != nil {
				b.Fatal(err)
			}
		}
	})
}
