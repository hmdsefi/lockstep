// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import "time"

var windowKind = kind{name: "Window", ctor: "NewWindow", tag: "window"}

// Window is a bounded set of keys: request keys already handled, or tombstones of entities that
// must not appear later. Create it with NewWindow and keep it in actor state as a *Window; a
// Window must not be copied. K must be comparable to itself (no NaN), and must encode with
// encoding/json to a value that decodes back to an equal key.
type Window[K comparable] struct {
	s store[K, struct{}]
}

// NewWindow returns an empty Window bounded by cfg. It panics with the error of cfg.Validate if
// cfg is invalid.
func NewWindow[K comparable](cfg Config) *Window[K] {
	return &Window[K]{s: newStore[K, struct{}](cfg)}
}

// Add records k at now and returns true if k was not in the window. If k is already in the window
// it returns false and leaves k's entry as it is: its age still counts from the first Add.
func (w *Window[K]) Add(now time.Time, k K) bool {
	e := w.s.begin(windowKind, now)
	if _, ok := w.s.index[k]; ok { // live: begin removed the expired entries (DLV-110)
		return false
	}
	w.s.add(e, k, struct{}{})
	return true
}

// Contains reports whether k is in the window at now. It records nothing.
func (w *Window[K]) Contains(now time.Time, k K) bool {
	_, ok := w.s.get(w.s.eff(windowKind, now), k)
	return ok
}

// Forget removes k from the window, if it is there.
func (w *Window[K]) Forget(k K) {
	w.s.usable(windowKind)
	w.s.remove(k)
}

// Len returns the number of keys in the window at now.
func (w *Window[K]) Len(now time.Time) int {
	return w.s.count(w.s.eff(windowKind, now))
}

// Config returns the window's bounds.
func (w *Window[K]) Config() Config {
	w.s.usable(windowKind)
	return w.s.cfg
}

// MarshalJSON encodes the window, its bounds and its entries in write order. It returns an error
// if a key does not encode.
func (w *Window[K]) MarshalJSON() ([]byte, error) {
	return w.s.marshal(windowKind, nil)
}
