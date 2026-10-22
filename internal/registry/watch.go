// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package registry

// Watcher is one watching incarnation.
type Watcher struct {
	Addr Addr   // the watching actor
	Inc  uint32 // its incarnation number
}

// Watch is one entry of a target's watcher list: who watches, and under which key (the address
// passed to Watch, which is a path address for PIDAt watches).
type Watch struct {
	Watcher Watcher
	Key     Addr
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
// actor, alive is true and w is appended to the target's watcher list under key; added is false
// when that exact entry (same watcher, same key) was already listed (ACT-620), in which case the
// list is unchanged. If the target is not registered, a guardian, or has another uid, alive and
// added are both false and nothing is stored.
func (r *Registry) AddWatch(key Addr, w Watcher) (alive, added bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.target(key)
	if e == nil {
		return false, false
	}
	for _, x := range e.watches {
		if x.Watcher == w && x.Key == key {
			return true, false
		}
	}
	e.watches = append(e.watches, Watch{Watcher: w, Key: key})
	for _, p := range r.watching[w] {
		if p == key.Path {
			return true, true
		}
	}
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
			r.reindex(w, e)
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

// DropWatcher removes w from every watcher list it is on (ACT-640).
func (r *Registry) DropWatcher(w Watcher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, path := range r.watching[w] {
		e, ok := r.entries[path]
		if !ok {
			continue
		}
		kept := e.watches[:0]
		for _, x := range e.watches {
			if x.Watcher != w {
				kept = append(kept, x)
			}
		}
		clear(e.watches[len(kept):])
		e.watches = kept
	}
	delete(r.watching, w)
}

// reindex drops path e from w's index when no entry of w remains on e. The caller holds r.mu.
func (r *Registry) reindex(w Watcher, e *entry) {
	for _, x := range e.watches {
		if x.Watcher == w {
			return
		}
	}
	r.unindex(w, e.addr.Path)
}

// unindex removes path from w's list of watched targets. The caller holds r.mu.
func (r *Registry) unindex(w Watcher, path string) {
	paths := r.watching[w]
	for i, p := range paths {
		if p == path {
			paths = append(paths[:i], paths[i+1:]...)
			break
		}
	}
	if len(paths) == 0 {
		delete(r.watching, w)
		return
	}
	r.watching[w] = paths
}
