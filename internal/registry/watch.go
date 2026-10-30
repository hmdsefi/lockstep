// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package registry

import "slices"

// Watcher is one watching incarnation.
type Watcher struct {
	Addr Addr   // the watching actor
	Inc  uint32 // its incarnation number
}

// Watch is one entry of a target's watcher list: who watches, under which key (the address
// passed to Watch, which is a path address for PIDAt watches), and the watcher's sequence number
// for that Watch call (ACT-630).
type Watch struct {
	Watcher Watcher
	Key     Addr
	Seq     uint64
}

// target returns the entry a watch key refers to: the actor registered at key.Path, and, when the
// key carries a uid, only if that uid matches. A path key (UID 0) matches whatever uid is
// registered at the path. The caller holds r.mu.
func (r *Registry) target(key Addr) *entry {
	e, ok := r.entries[key.Path]
	if !ok || (key.UID != 0 && e.addr.UID != key.UID) {
		return nil
	}
	return e
}

// AddWatch is the atomic alive check and registration of ACT-600. If key refers to a registered
// actor, alive is true and w is appended to the target's watcher list under key with sequence
// number seq; added is false when that exact entry (same watcher, same key) was already listed
// (ACT-620), in which case the entry keeps its place in the list and takes seq. If the target is
// not registered, a guardian, or has another uid, alive and added are both false and nothing is
// stored.
func (r *Registry) AddWatch(key Addr, w Watcher, seq uint64) (alive, added bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.target(key)
	if e == nil {
		return false, false
	}
	for i, x := range e.watches {
		if x.Watcher == w && x.Key == key {
			e.watches[i].Seq = seq
			return true, false
		}
	}
	e.watches = append(e.watches, Watch{Watcher: w, Key: key, Seq: seq})
	r.watching[w] = append(r.watching[w], key.Path)
	return true, true
}

// RemoveWatch removes the entry of w under key from its target's watcher list and reports whether
// there was one.
func (r *Registry) RemoveWatch(key Addr, w Watcher) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.target(key)
	if e == nil {
		return false
	}
	for i, x := range e.watches {
		if x.Watcher == w && x.Key == key {
			e.watches = append(e.watches[:i], e.watches[i+1:]...)
			r.unindex(w, e.addr.Path)
			return true
		}
	}
	return false
}

// Watchers returns the watcher list of the actor registered at path, in the order the entries were
// added. The slice is a copy.
func (r *Registry) Watchers(path string) []Watch {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[path]
	if !ok {
		return nil
	}
	return append([]Watch(nil), e.watches...)
}

// DropWatcher removes w from every watcher list it is on (ACT-640). It removes one entry of w per
// path in w's drop index, each found from both ends of the target's list (indexNearEnds): workers
// that watch one actor stop newest first in a shutdown, and their entries are in watch order.
func (r *Registry) DropWatcher(w Watcher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, path := range r.watching[w] {
		e, ok := r.entries[path]
		if !ok {
			continue
		}
		if i := indexNearEnds(e.watches, func(x Watch) bool { return x.Watcher == w }); i >= 0 {
			e.watches = slices.Delete(e.watches, i, i+1)
		}
	}
	delete(r.watching, w)
}

// unindex removes one occurrence of path from w's drop index, found from both ends
// (indexNearEnds): a monitor that watches many actors sees them stop newest first in a shutdown,
// and its index is in watch order. The caller holds r.mu.
func (r *Registry) unindex(w Watcher, path string) {
	paths := r.watching[w]
	if i := indexNearEnds(paths, func(p string) bool { return p == path }); i >= 0 {
		paths = slices.Delete(paths, i, i+1)
	}
	if len(paths) == 0 {
		delete(r.watching, w)
		return
	}
	r.watching[w] = paths
}
