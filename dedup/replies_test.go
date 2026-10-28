// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// repEnt is a Replies[string, int] entry in an expected encoding.
type repEnt struct {
	k  string
	at int64
	r  int
}

// repliesJSON returns the encoding of a Replies[string, int] with the given bounds, last and entries.
func repliesJSON(capacity int, ttl time.Duration, last int64, entries ...repEnt) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"dedup":1,"type":"replies","capacity":%d,"ttl":%d,"last":%d,"entries":[`, capacity, int64(ttl), last)
	for i, e := range entries {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"k":%q,"at":%d,"r":%d}`, e.k, e.at, e.r)
	}
	b.WriteString("]}")
	return b.String()
}

// checkReply reports a Get whose result differs from want.
func checkReply[R comparable](t *testing.T, call string, got R, ok bool, want R, wantOK bool) {
	t.Helper()
	if got != want || ok != wantOK {
		t.Errorf("%s = %v, %t; want %v, %t", call, got, ok, want, wantOK)
	}
}

// AT-DLV-13, DLV-130: Put keeps the first reply and its age; Get reads without removing; the
// capacity removes the oldest reply.
func TestATDLV13Replies(t *testing.T) {
	c := NewReplies[string, int](Config{2, 10 * sec})
	r, ok := c.Get(t0, "k")
	checkReply(t, `Get(t0, "k")`, r, ok, 0, false)
	check(t, `Put(t0, "k", 1)`, c.Put(t0, "k", 1), true)
	check(t, `Put(t0+1s, "k", 2)`, c.Put(at(sec), "k", 2), false)
	r, ok = c.Get(at(9*sec), "k")
	checkReply(t, `Get(t0+9s, "k")`, r, ok, 1, true)
	r, ok = c.Get(at(9*sec), "k") // Get does not remove
	checkReply(t, `Get(t0+9s, "k") again`, r, ok, 1, true)
	r, ok = c.Get(at(10*sec), "k") // the second Put did not refresh the age
	checkReply(t, `Get(t0+10s, "k")`, r, ok, 0, false)

	c = NewReplies[string, int](Config{2, 10 * sec})
	check(t, `Put(t0, "x", 1)`, c.Put(t0, "x", 1), true)
	check(t, `Put(t0, "y", 2)`, c.Put(t0, "y", 2), true)
	check(t, `Put(t0, "z", 3)`, c.Put(t0, "z", 3), true)
	r, ok = c.Get(t0, "x")
	checkReply(t, `Get(t0, "x")`, r, ok, 0, false)
	r, ok = c.Get(t0, "y")
	checkReply(t, `Get(t0, "y")`, r, ok, 2, true)
	r, ok = c.Get(t0, "z")
	checkReply(t, `Get(t0, "z")`, r, ok, 3, true)
	if n := c.Len(t0); n != 2 {
		t.Errorf("Len(t0) = %d, want 2", n)
	}
}

// DLV-130: Put of a live key keeps the first reply and does not move the entry in write order.
func TestRepliesPutKeepsFirstAndOrder(t *testing.T) {
	c := NewReplies[string, string](Config{2, 0})
	c.Put(t0, "a", "first")
	c.Put(t0, "b", "b1")
	check(t, `Put(t0, "a", "second")`, c.Put(at(sec), "a", "second"), false)
	c.Put(at(sec), "c", "c1") // removes the oldest, which is still a
	r, ok := c.Get(at(sec), "a")
	checkReply(t, `Get("a")`, r, ok, "", false)
	r, ok = c.Get(at(sec), "b")
	checkReply(t, `Get("b")`, r, ok, "b1", true)
}

// DLV-130: an expired reply is replaced by Put, and Forget lets the next Put store a new one.
func TestRepliesExpiredAndForget(t *testing.T) {
	c := NewReplies[string, int](Config{4, 10 * sec})
	c.Put(t0, "k", 1)
	check(t, `Put(t0+10s, "k", 2)`, c.Put(at(10*sec), "k", 2), true)
	r, ok := c.Get(at(10*sec), "k")
	checkReply(t, `Get(t0+10s, "k")`, r, ok, 2, true)
	c.Forget("k")
	r, ok = c.Get(at(10*sec), "k")
	checkReply(t, `Get after Forget`, r, ok, 0, false)
	check(t, `Put after Forget`, c.Put(at(10*sec), "k", 3), true)
	c.Forget("absent") // does nothing
	if n := c.Len(at(10 * sec)); n != 1 {
		t.Errorf("Len = %d, want 1", n)
	}
}

// DLV-130: the cache stores the reply as given, a nil reply included, and it survives encoding.
func TestRepliesNilReply(t *testing.T) {
	c := NewReplies[string, *int](Config{2, 0})
	check(t, `Put(t0, "k", nil)`, c.Put(t0, "k", nil), true)
	r, ok := c.Get(t0, "k")
	if r != nil || !ok {
		t.Errorf(`Get("k") = %v, %t; want nil, true`, r, ok)
	}
	b := encode(t, c)
	if want := `{"dedup":1,"type":"replies","capacity":2,"ttl":0,"last":1000000000,"entries":[{"k":"k","at":1000000000,"r":null}]}`; b != want {
		t.Fatalf("encoding is\n%s\nwant\n%s", b, want)
	}
	var d Replies[string, *int]
	if err := d.UnmarshalJSON([]byte(b)); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if _, ok := d.Get(t0, "k"); !ok || encode(t, &d) != b {
		t.Errorf("the decoded cache lost the nil reply: ok=%t, encoding %s", ok, encode(t, &d))
	}
}

// replyFail is a reply whose encoding fails.
type replyFail struct{}

var errReplyFail = errors.New("reply does not encode")

func (replyFail) MarshalJSON() ([]byte, error) { return nil, errReplyFail }
