// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"strconv"
	"time"
)

var versionsKind = kind{name: "Versions", ctor: "NewVersions", tag: "versions"}

// Versions keeps the highest version accepted per entity, so an update older than one already
// applied is rejected. Create it with NewVersions and keep it in actor state as a *Versions; it
// must not be copied. K is as for Window.
//
// An entity that Versions no longer holds, because it expired, was removed for capacity or was
// forgotten, accepts any version. Size TTL and Capacity so that no update can arrive after its
// entity is removed, or keep the version with the entity's own state instead.
type Versions[K comparable] struct {
	s store[K, uint64]
}

// NewVersions returns an empty Versions bounded by cfg. It panics with the error of cfg.Validate
// if cfg is invalid.
func NewVersions[K comparable](cfg Config) *Versions[K] {
	return &Versions[K]{s: newStore[K, uint64](cfg)}
}

// Accept records version as k's version at now and returns true if Versions holds no version
// for k or version is greater than the one it holds; the entry then ages from now. Otherwise it
// returns false and changes nothing for k.
func (v *Versions[K]) Accept(now time.Time, k K, version uint64) bool {
	e := v.s.begin(versionsKind, now) // a write call even when the version is rejected (DLV-040, DLV-060)
	n, ok := v.s.index[k]             // live: begin removed the expired entries (DLV-140)
	if !ok {
		v.s.add(e, k, version)
		return true
	}
	if version <= n.v {
		return false
	}
	n.v = version
	v.s.moveLast(e, n)
	return true
}

// Get returns k's version at now and true, or 0 and false if Versions holds none.
func (v *Versions[K]) Get(now time.Time, k K) (uint64, bool) {
	n, ok := v.s.get(v.s.eff(versionsKind, now), k)
	if !ok {
		return 0, false
	}
	return n.v, true
}

// Forget removes k's version, if there is one.
func (v *Versions[K]) Forget(k K) {
	v.s.usable(versionsKind)
	v.s.remove(k)
}

// Len returns the number of entities held at now.
func (v *Versions[K]) Len(now time.Time) int {
	return v.s.count(v.s.eff(versionsKind, now))
}

// Config returns the bounds.
func (v *Versions[K]) Config() Config {
	v.s.usable(versionsKind)
	return v.s.cfg
}

// MarshalJSON encodes the bounds and the entries in write order. It returns an error if a key
// does not encode.
func (v *Versions[K]) MarshalJSON() ([]byte, error) {
	return v.s.marshal(versionsKind, versionField)
}

// UnmarshalJSON replaces v with the value that data encodes, as Window.UnmarshalJSON does.
func (v *Versions[K]) UnmarshalJSON(data []byte) error {
	return v.s.unmarshal(versionsKind, data, versionValue)
}

// versionField appends `,"v":<version>` to an entry (DLV-200).
func versionField(b []byte, version uint64) ([]byte, error) {
	b = append(b, `,"v":`...)
	return strconv.AppendUint(b, version, 10), nil
}

// versionValue returns the "v" of an entry (DLV-210); a version that is not a uint64 already
// failed the first check, which decodes the whole encoding.
func versionValue(e *rawEntry) (uint64, error) { return e.V, nil }
