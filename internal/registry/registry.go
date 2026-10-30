// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package registry holds a system's actor names, child lists, generated-name counters and watch
// tables (ACT-500). It cannot import package lockstep, so addresses are its own Addr and the typed
// actor cell is an opaque any.
//
// A Registry is safe for concurrent use: one mutex guards every operation. Maps are used for lookup
// only. Every listing (children, watchers) is built from a slice in insertion order and never by
// ranging over a map, so results are deterministic on the simulation engine.
package registry

import (
	"slices"
	"strconv"
	"sync"
)

// Addr mirrors lockstep.Address; the registry cannot import package lockstep.
type Addr struct {
	Node, Path string
	UID        uint64
}

// node is what a guardian and a registered actor have in common: a spawn-ordered child list and
// a generated-name counter.
type node struct {
	nodeName string // node name inherited by the children
	children []Addr
	counter  uint64
}

// entry is one registered actor.
type entry struct {
	node
	addr    Addr
	cell    any
	parent  string
	watches []Watch // watchers in the order their entries were added
}

// Registry is the per-system table of registered actors. The zero value is not usable; call New.
type Registry struct {
	mu      sync.Mutex
	roots   map[string]*node
	entries map[string]*entry
	// watching maps a watcher to the paths of the targets whose lists hold it, one path per entry,
	// for DropWatcher: a watcher of one target under its uid and its path has the path twice. It is
	// looked up by key only, never ranged over.
	watching map[Watcher][]string
	// listed holds the watchID of every entry on a watcher list, for AddWatch's duplicate check
	// (ACT-620) and RemoveWatch's miss. It is looked up by key only, never ranged over.
	listed map[watchID]struct{}
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{
		roots:    make(map[string]*node),
		entries:  make(map[string]*entry),
		watching: make(map[Watcher][]string),
		listed:   make(map[watchID]struct{}),
	}
}

// AddRoot declares a guardian: it has children and a generated-name counter but is not registered,
// so Lookup does not find it and a watch of it is never alive. Declaring the same path again does
// nothing.
func (r *Registry) AddRoot(a Addr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.roots[a.Path]; !ok {
		r.roots[a.Path] = &node{nodeName: a.Node}
	}
}

// parentOf returns the node of a guardian or registered actor, or nil. The caller holds r.mu.
func (r *Registry) parentOf(path string) *node {
	if n, ok := r.roots[path]; ok {
		return n
	}
	if e, ok := r.entries[path]; ok {
		return &e.node
	}
	return nil
}

// Taken reports whether a registered child of parent has the given name.
func (r *Registry) Taken(parent, name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.entries[parent+"/"+name]
	return ok
}

// Add registers an actor as the last child of parent and returns its path, parent + "/" + name.
// An empty name generates "$<n>" from the parent's counter (ACT-320): the counter advances only
// when a generated name is taken, never for a named child and never when ok is false. ok is false
// when a registered child of parent already has the name (ACT-330). Add panics if parent is
// neither a declared root nor a registered actor; the runtime spawns only under live parents.
//
// uid allocates the actor's uid (ACT-040). Add calls it exactly once, under the registry's lock,
// only after the name check passed and the generated name is fixed, so a spawn that loses the name
// race allocates no uid. uid must not call back into the registry.
func (r *Registry) Add(parent, name string, uid func() uint64, cell any) (path string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.parentOf(parent)
	if p == nil {
		panic("registry: Add under unknown parent " + strconv.Quote(parent))
	}
	generated := name == ""
	if generated {
		name = "$" + strconv.FormatUint(p.counter+1, 10)
	}
	path = parent + "/" + name
	if _, taken := r.entries[path]; taken {
		return "", false
	}
	if generated {
		p.counter++
	}
	a := Addr{Node: p.nodeName, Path: path, UID: uid()}
	r.entries[path] = &entry{node: node{nodeName: p.nodeName}, addr: a, cell: cell, parent: parent}
	p.children = append(p.children, a)
	return path, true
}

// Lookup returns the uid and the cell registered at path. A guardian is not registered.
func (r *Registry) Lookup(path string) (uid uint64, cell any, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[path]
	if !ok {
		return 0, nil, false
	}
	return e.addr.UID, e.cell, true
}

// Children returns the registered children of parent in spawn order. The slice is a copy.
func (r *Registry) Children(parent string) []Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.parentOf(parent)
	if p == nil {
		return nil
	}
	return append([]Addr(nil), p.children...)
}

// LastChild returns the most recently spawned registered child of parent, and false when parent
// has none. It copies nothing: a stop of many children reads it once per child, newest first
// (ACT-050, SUP-060, SUP-070).
func (r *Registry) LastChild(parent string) (Addr, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.parentOf(parent)
	if p == nil || len(p.children) == 0 {
		return Addr{}, false
	}
	return p.children[len(p.children)-1], true
}

// Remove completes the final stop of the actor registered at path: it removes the entry, frees its
// name, and returns the watchers listed on it in the order they were added (ACT-650), each with
// the key it watched under. Remove of an unregistered path returns nil. It panics if the actor
// still has registered children; children complete their stops first (ACT-550).
func (r *Registry) Remove(path string) []Watch {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[path]
	if !ok {
		return nil
	}
	if len(e.children) > 0 {
		panic("registry: Remove of " + strconv.Quote(path) + " with registered children")
	}
	delete(r.entries, path)
	if p := r.parentOf(e.parent); p != nil {
		p.children = removeAddr(p.children, path)
	}
	for _, w := range e.watches {
		r.unindex(w.Watcher, path)
		delete(r.listed, watchID{w.Watcher, w.Key})
	}
	return e.watches
}

// removeAddr removes path's address from s and clears the slot it frees. It searches from both
// ends (indexNearEnds).
func removeAddr(s []Addr, path string) []Addr {
	if i := indexNearEnds(s, func(a Addr) bool { return a.Path == path }); i >= 0 {
		return slices.Delete(s, i, i+1)
	}
	return s
}

// indexNearEnds returns the index of the element of s nearest an end for which match is true, the
// newest end first, or -1 if there is none. Actors stopped together stop newest first (ACT-050,
// SUP-060, SUP-070), and actors that stop on their own tend to stop oldest first, so either finds
// an actor's entry in a few steps for any length of s.
func indexNearEnds[E any](s []E, match func(E) bool) int {
	for lo, hi := 0, len(s)-1; lo <= hi; lo, hi = lo+1, hi-1 {
		if match(s[hi]) {
			return hi
		}
		if match(s[lo]) {
			return lo
		}
	}
	return -1
}

// Len returns the number of registered actors; guardians are not counted.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}
