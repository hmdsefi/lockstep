// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package kv is the replicated key-value state machine of the Raft demo.
package kv

import (
	"maps"
	"slices"
	"strconv"
)

// Command and output prefixes (RFT-310).
const (
	putPrefix = 'p'
	getPrefix = 'g'
	present   = "1"
	absent    = "0"
	invalid   = "!"
)

// FNV-1a 64 parameters. The empty store hashes to the offset basis, which equals raft.ChainSeed
// (kv does not import raft).
const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

// EncodePut returns the command that sets key to value.
func EncodePut(key, value string) string {
	return string(putPrefix) + strconv.Itoa(len(key)) + ":" + key + value
}

// EncodeGet returns the command that reads key.
func EncodeGet(key string) string {
	return string(getPrefix) + key
}

// DecodeGetOutput decodes a get output: the value and whether the key was present.
func DecodeGetOutput(out string) (value string, ok bool) {
	if len(out) >= 1 && out[0] == present[0] {
		return out[1:], true
	}
	return "", false
}

// Store implements raft.StateMachine.
type Store struct {
	m map[string]string
}

// New returns an empty Store.
func New() *Store {
	return &Store{m: make(map[string]string)}
}

// Apply executes cmd and returns its output: "" for a put, "1"+value or "0" for a get, and "!" for
// a command that does not decode, which leaves the state unchanged. The index is not used.
func (s *Store) Apply(index uint64, cmd string) string {
	if key, value, ok := decodePut(cmd); ok {
		s.m[key] = value
		return ""
	}
	return s.Query(cmd)
}

// Query executes a get without recording anything. It returns "!" for a put and for any command
// that does not decode.
func (s *Store) Query(cmd string) string {
	if len(cmd) >= 1 && cmd[0] == getPrefix {
		if v, ok := s.m[cmd[1:]]; ok {
			return present + v
		}
		return absent
	}
	return invalid
}

// Hash returns FNV-1a 64 over the keys in byte order, each as be32(len(k)) || k || be32(len(v)) ||
// v. The empty store hashes to the FNV-1a offset basis, raft.ChainSeed.
func (s *Store) Hash() uint64 {
	h := fnvOffset
	for _, k := range slices.Sorted(maps.Keys(s.m)) {
		v := s.m[k]
		h = hashUint32(h, uint32(len(k))) //nolint:gosec // G115: a key is far shorter than 4 GiB
		h = hashString(h, k)
		h = hashUint32(h, uint32(len(v))) //nolint:gosec // G115: a value is far shorter than 4 GiB
		h = hashString(h, v)
	}
	return h
}

// Get reads the current value of key.
func (s *Store) Get(key string) (string, bool) {
	v, ok := s.m[key]
	return v, ok
}

// decodePut splits a put command "p<len>:<key><value>". It reports false for anything else,
// including a length that is not a plain decimal number or that overruns the command.
func decodePut(cmd string) (key, value string, ok bool) {
	if len(cmd) < 1 || cmd[0] != putPrefix {
		return "", "", false
	}
	rest := cmd[1:]
	colon := 0
	for colon < len(rest) && rest[colon] >= '0' && rest[colon] <= '9' {
		colon++
	}
	if colon == 0 || colon == len(rest) || rest[colon] != ':' {
		return "", "", false
	}
	n, err := strconv.Atoi(rest[:colon])
	if err != nil {
		return "", "", false
	}
	body := rest[colon+1:]
	if n > len(body) {
		return "", "", false
	}
	return body[:n], body[n:], true
}

// hashUint32 feeds x to the FNV-1a state h as four big-endian bytes.
func hashUint32(h uint64, x uint32) uint64 {
	for shift := 24; shift >= 0; shift -= 8 {
		h ^= uint64(x) >> uint(shift) & 0xff
		h *= fnvPrime
	}
	return h
}

// hashString feeds the bytes of s to the FNV-1a state h.
func hashString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime
	}
	return h
}
