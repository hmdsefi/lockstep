// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote

import (
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
)

// TestReadBody checks that readBody reads a frame larger than its chunk size whole, and that a
// stream that ends early is an unexpected EOF, however long the declared length, without
// allocating more than one chunk beyond the bytes that arrived.
func TestReadBody(t *testing.T) {
	prefix := [4]byte{0, 0, 0, 9}
	body := bytes.Repeat([]byte("abcdefgh"), 3*readBuffer/8+1)
	got, err := readBody(bytes.NewReader(body), prefix, len(body))
	if err != nil || !bytes.Equal(got[:4], prefix[:]) || !bytes.Equal(got[4:], body) {
		t.Errorf("readBody of %d bytes: %d bytes, %v", len(body), len(got), err)
	}

	const declared = 1 << 30
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = readBody(bytes.NewReader([]byte("short")), prefix, declared)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("readBody of a short stream: %v, want ErrUnexpectedEOF", err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 4*readBuffer {
		t.Errorf("readBody of 5 bytes declared as %d allocated %d bytes", declared, n)
	}
}

// TestWildcard checks which listener addresses peers cannot dial back.
func TestWildcard(t *testing.T) {
	for _, tc := range []struct {
		addr net.Addr
		want bool
	}{
		{&net.TCPAddr{IP: net.IPv4zero, Port: 1}, true},
		{&net.TCPAddr{IP: net.IPv6unspecified, Port: 1}, true},
		{&net.TCPAddr{Port: 1}, true},
		{&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, false},
		{&net.UnixAddr{Name: "x"}, false},
	} {
		if got := wildcard(tc.addr); got != tc.want {
			t.Errorf("wildcard(%v) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}
