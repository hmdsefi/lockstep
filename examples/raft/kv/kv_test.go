// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package kv_test

import (
	"encoding/binary"
	"hash/fnv"
	"testing"

	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
)

var _ raft.StateMachine = kv.New()

// TestATRFT24KVApply checks the outputs of Apply and Query and the resulting state.
func TestATRFT24KVApply(t *testing.T) {
	s := kv.New()
	steps := []struct {
		cmd  string
		want string
	}{
		{kv.EncodePut("k", "v"), ""},
		{kv.EncodeGet("k"), "1v"},
		{kv.EncodeGet("x"), "0"},
		{"zz", "!"},
	}
	for i, st := range steps {
		if got := s.Apply(uint64(i+1), st.cmd); got != st.want {
			t.Errorf("Apply(%d, %q) = %q, want %q", i+1, st.cmd, got, st.want)
		}
	}
	if got := s.Query(kv.EncodePut("a", "b")); got != "!" {
		t.Errorf("Query(put) = %q, want %q", got, "!")
	}
	if _, ok := s.Get("a"); ok {
		t.Error("Query(put) changed the state")
	}
	if v, ok := s.Get("k"); !ok || v != "v" {
		t.Errorf("Get(k) = (%q, %v), want (v, true)", v, ok)
	}
	if v, ok := s.Get("x"); ok || v != "" {
		t.Errorf("Get(x) = (%q, %v), want (empty, false)", v, ok)
	}
	if got := s.Query(kv.EncodeGet("k")); got != "1v" {
		t.Errorf("Query(get k) = %q, want 1v", got)
	}
	if got := s.Query("zz"); got != "!" {
		t.Errorf("Query(zz) = %q, want !", got)
	}
}

// TestKVEncoding checks the command and output encodings and the malformed-command cases.
func TestKVEncoding(t *testing.T) {
	if got := kv.EncodePut("ab", "c"); got != "p2:abc" {
		t.Errorf("EncodePut = %q, want p2:abc", got)
	}
	if got := kv.EncodeGet("ab"); got != "gab" {
		t.Errorf("EncodeGet = %q, want gab", got)
	}
	if v, ok := kv.DecodeGetOutput("1x"); v != "x" || !ok {
		t.Errorf("DecodeGetOutput(1x) = (%q, %v), want (x, true)", v, ok)
	}
	if v, ok := kv.DecodeGetOutput("0"); v != "" || ok {
		t.Errorf("DecodeGetOutput(0) = (%q, %v), want (empty, false)", v, ok)
	}
	if v, ok := kv.DecodeGetOutput("1"); v != "" || !ok {
		t.Errorf("DecodeGetOutput(1) = (%q, %v), want (empty, true)", v, ok)
	}

	// Values may be empty and keys and values may contain the separators.
	s := kv.New()
	s.Apply(1, kv.EncodePut("a:b", ""))
	s.Apply(2, kv.EncodePut("", "v"))
	if out := s.Apply(3, kv.EncodeGet("a:b")); out != "1" {
		t.Errorf("get of an empty value = %q, want 1", out)
	}
	if out := s.Apply(4, kv.EncodeGet("")); out != "1v" {
		t.Errorf("get of the empty key = %q, want 1v", out)
	}

	malformed := []string{"", "p", "p:", "p5:ab", "p2", "pa:bc", "p-1:a", "p+1:ab", "p1x:ab", "p99999999999999999999:a"}
	for _, cmd := range malformed {
		s := kv.New()
		if got := s.Apply(1, cmd); got != "!" {
			t.Errorf("Apply(%q) = %q, want !", cmd, got)
		}
		if h := s.Hash(); h != raft.ChainSeed {
			t.Errorf("Apply(%q) changed the state", cmd)
		}
	}
}

// refHash is RFT-330's encoding, folded with the standard library's FNV-1a 64: for each pair, in
// the order given, be32(len(k)) || k || be32(len(v)) || v.
func refHash(pairs [][2]string) uint64 {
	var b []byte
	for _, p := range pairs {
		b = binary.BigEndian.AppendUint32(b, uint32(len(p[0]))) //nolint:gosec // G115: test keys are short
		b = append(b, p[0]...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(p[1]))) //nolint:gosec // G115: test values are short
		b = append(b, p[1]...)
	}
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// TestATRFT25KVHash checks that Hash follows RFT-330's encoding with the keys in byte order, does
// not depend on insertion order, and that an empty store hashes to ChainSeed.
func TestATRFT25KVHash(t *testing.T) {
	if got := kv.New().Hash(); got != raft.ChainSeed {
		t.Errorf("empty Hash = %d, want ChainSeed %d", got, raft.ChainSeed)
	}

	// Inserted out of byte order; Hash folds the keys in byte order.
	ak := kv.New()
	ak.Apply(1, kv.EncodePut("k", "v"))
	ak.Apply(2, kv.EncodePut("a", "b"))
	if got, want := ak.Hash(), uint64(0x591ae43210b4cceb); got != want {
		t.Errorf("Hash of {a:b, k:v} = %#x, want %#x", got, want)
	}
	if got, want := ak.Hash(), refHash([][2]string{{"a", "b"}, {"k", "v"}}); got != want {
		t.Errorf("Hash of {a:b, k:v} = %#x, reference fold %#x", got, want)
	}
	pairs := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"", "e"}, {"d", ""}}
	sorted := [][2]string{{"", "e"}, {"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", ""}} // pairs in byte order
	forward, backward := kv.New(), kv.New()
	for _, p := range pairs {
		forward.Apply(1, kv.EncodePut(p[0], p[1]))
	}
	for i := len(pairs) - 1; i >= 0; i-- {
		backward.Apply(1, kv.EncodePut(pairs[i][0], pairs[i][1]))
	}
	if f, b := forward.Hash(), backward.Hash(); f != b {
		t.Errorf("Hash depends on insertion order: %d != %d", f, b)
	}
	if got, want := forward.Hash(), refHash(sorted); got != want {
		t.Errorf("Hash = %#x, reference fold over the keys in byte order %#x", got, want)
	}
	if forward.Hash() == raft.ChainSeed {
		t.Error("a non-empty store hashes to ChainSeed")
	}

	other := kv.New()
	for _, p := range pairs {
		other.Apply(1, kv.EncodePut(p[0], p[1]))
	}
	other.Apply(9, kv.EncodePut("a", "changed"))
	if other.Hash() == forward.Hash() {
		t.Error("Hash ignores a changed value")
	}

	// The length prefixes keep ("ab","c") and ("a","bc") apart.
	x, y := kv.New(), kv.New()
	x.Apply(1, kv.EncodePut("ab", "c"))
	y.Apply(1, kv.EncodePut("a", "bc"))
	if x.Hash() == y.Hash() {
		t.Error("Hash does not separate key and value")
	}
}
