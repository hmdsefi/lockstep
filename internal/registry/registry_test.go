// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package registry

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
)

func newTestRegistry() *Registry {
	r := New()
	r.AddRoot(Addr{Node: "n1", Path: "/user"})
	r.AddRoot(Addr{Node: "n1", Path: "/system"})
	return r
}

// fixed returns a uid allocator that yields uid.
func fixed(uid uint64) func() uint64 { return func() uint64 { return uid } }

func mustAdd(t *testing.T, r *Registry, parent, name string, uid uint64) string {
	t.Helper()
	p, ok := r.Add(parent, name, fixed(uid), "cell-"+name)
	if !ok {
		t.Fatalf("Add(%q, %q) not ok", parent, name)
	}
	return p
}

func w(path string, uid uint64, inc uint32) Watcher {
	return Watcher{Addr: Addr{Node: "n1", Path: path, UID: uid}, Inc: inc}
}

func TestAddAndLookup(t *testing.T) {
	r := newTestRegistry()
	p := mustAdd(t, r, "/user", "a", 7)
	if p != "/user/a" {
		t.Fatalf("path = %q, want /user/a", p)
	}
	q := mustAdd(t, r, p, "b", 8)
	if q != "/user/a/b" {
		t.Fatalf("path = %q, want /user/a/b", q)
	}
	uid, cell, ok := r.Lookup("/user/a")
	if !ok || uid != 7 || cell != "cell-a" {
		t.Fatalf("Lookup(/user/a) = %d, %v, %v", uid, cell, ok)
	}
	if _, _, ok := r.Lookup("/user/a/b"); !ok {
		t.Fatal("Lookup(/user/a/b) not found")
	}
	for _, path := range []string{"/user", "/system", "/", "", "/user/zz"} {
		if _, _, ok := r.Lookup(path); ok {
			t.Fatalf("Lookup(%q) found; roots and unknown paths are not registered", path)
		}
	}
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}
	if r.Taken("/user", "zz") || !r.Taken("/user", "a") || !r.Taken("/user/a", "b") {
		t.Fatal("Taken disagrees with the registered children")
	}
}

func TestAddCallsUIDOnlyOnSuccess(t *testing.T) {
	r := newTestRegistry()
	var calls int
	var last uint64 = 40
	next := func() uint64 { calls++; last++; return last }

	p, ok := r.Add("/user", "a", next, "c")
	if !ok || p != "/user/a" || calls != 1 {
		t.Fatalf("Add = %q, %v after %d uid calls; want one call", p, ok, calls)
	}
	if uid, _, found := r.Lookup("/user/a"); !found || uid != 41 {
		t.Fatalf("Lookup uid = %d, %v; want 41", uid, found)
	}
	if got := r.Children("/user"); len(got) != 1 || got[0].UID != 41 {
		t.Fatalf("Children = %v", got)
	}
	// A taken name allocates no uid.
	if _, ok := r.Add("/user", "a", next, "c"); ok || calls != 1 {
		t.Fatalf("taken Add ok = %v with %d uid calls; want false and 1", ok, calls)
	}
	// A generated name calls it once and the uid is the allocator's.
	if p, ok := r.Add("/user", "", next, "c"); !ok || p != "/user/$1" || calls != 2 {
		t.Fatalf("generated Add = %q, %v after %d uid calls", p, ok, calls)
	}
	if uid, _, _ := r.Lookup("/user/$1"); uid != 42 {
		t.Fatalf("generated uid = %d, want 42", uid)
	}
}

func TestAddUnknownParentPanics(t *testing.T) {
	r := newTestRegistry()
	defer func() {
		if recover() == nil {
			t.Fatal("Add under an unknown parent did not panic")
		}
	}()
	r.Add("/user/missing", "a", fixed(1), nil)
}

func TestGeneratedNames(t *testing.T) {
	r := newTestRegistry()
	if p := mustAdd(t, r, "/user", "", 1); p != "/user/$1" {
		t.Fatalf("first generated = %q", p)
	}
	if p := mustAdd(t, r, "/user", "", 2); p != "/user/$2" {
		t.Fatalf("second generated = %q", p)
	}
	// Independent counter under another guardian and under an actor.
	if p := mustAdd(t, r, "/system", "", 3); p != "/system/$1" {
		t.Fatalf("system generated = %q", p)
	}
	a := mustAdd(t, r, "/user", "a", 4)
	if p := mustAdd(t, r, a, "", 5); p != "/user/a/$1" {
		t.Fatalf("child generated = %q", p)
	}
	// A named spawn consumes no number, and neither does a failed one (ACT-320).
	mustAdd(t, r, "/user", "named", 6)
	if _, ok := r.Add("/user", "named", fixed(7), nil); ok {
		t.Fatal("Add of a taken name succeeded")
	}
	if p := mustAdd(t, r, "/user", "", 8); p != "/user/$3" {
		t.Fatalf("third generated = %q, want /user/$3", p)
	}
	// The counter never goes back when a generated child is removed.
	r.Remove("/user/$3")
	if p := mustAdd(t, r, "/user", "", 9); p != "/user/$4" {
		t.Fatalf("after removal generated = %q, want /user/$4", p)
	}
}

func TestNameFreedAtRemove(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "a", 1)
	if _, ok := r.Add("/user", "a", fixed(2), nil); ok {
		t.Fatal("duplicate name accepted")
	}
	// The name stays taken, and the actor found, until Remove (stop started is not removed).
	if !r.Taken("/user", "a") {
		t.Fatal("name not taken")
	}
	if uid, _, ok := r.Lookup("/user/a"); !ok || uid != 1 {
		t.Fatal("entry not found before Remove")
	}
	r.Remove("/user/a")
	if r.Taken("/user", "a") {
		t.Fatal("name still taken after Remove")
	}
	if _, _, ok := r.Lookup("/user/a"); ok {
		t.Fatal("entry found after Remove")
	}
	if p, ok := r.Add("/user", "a", fixed(3), "again"); !ok || p != "/user/a" {
		t.Fatalf("re-Add = %q, %v", p, ok)
	}
	if uid, cell, _ := r.Lookup("/user/a"); uid != 3 || cell != "again" {
		t.Fatalf("Lookup after re-Add = %d, %v", uid, cell)
	}
}

func TestRemoveRequiresNoChildren(t *testing.T) {
	r := newTestRegistry()
	a := mustAdd(t, r, "/user", "a", 1)
	mustAdd(t, r, a, "b", 2)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Remove of a parent with a registered child did not panic")
			}
		}()
		r.Remove(a)
	}()
	if !r.Taken("/user", "a") || r.Len() != 2 {
		t.Fatal("the failed Remove changed the registry")
	}
	r.Remove("/user/a/b")
	r.Remove(a)
	if r.Len() != 0 {
		t.Fatalf("Len = %d, want 0", r.Len())
	}
	// Removing an unknown path does nothing.
	if got := r.Remove("/user/a"); got != nil {
		t.Fatalf("Remove of unknown path = %v", got)
	}
}

func TestChildrenOrder(t *testing.T) {
	r := newTestRegistry()
	for i, n := range []string{"c", "a", "", "b"} {
		mustAdd(t, r, "/user", n, uint64(i+1))
	}
	paths := func() []string {
		var out []string
		for _, c := range r.Children("/user") {
			out = append(out, c.Path)
		}
		return out
	}
	want := []string{"/user/c", "/user/a", "/user/$1", "/user/b"}
	if got := paths(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Children = %v, want %v", got, want)
	}
	cs := r.Children("/user")
	if cs[1] != (Addr{Node: "n1", Path: "/user/a", UID: 2}) {
		t.Fatalf("child address = %+v", cs[1])
	}
	r.Remove("/user/a")
	want = []string{"/user/c", "/user/$1", "/user/b"}
	if got := paths(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Children after Remove = %v, want %v", got, want)
	}
	// The result is a copy.
	cs = r.Children("/user")
	cs[0].Path = "/mutated"
	if r.Children("/user")[0].Path != "/user/c" {
		t.Fatal("Children returned the registry's own slice")
	}
	if got := r.Children("/system"); len(got) != 0 {
		t.Fatalf("Children of an empty parent = %v", got)
	}
	if got := r.Children("/nowhere"); len(got) != 0 {
		t.Fatalf("Children of an unknown parent = %v", got)
	}
}

// LastChild is the most recently spawned child that is still registered (ACT-050, SUP-060,
// SUP-070), and reading it copies nothing.
func TestLastChild(t *testing.T) {
	r := newTestRegistry()
	for _, parent := range []string{"/user", "/nowhere"} {
		if got, ok := r.LastChild(parent); ok {
			t.Fatalf("LastChild(%q) = %+v, want none", parent, got)
		}
	}
	for i, n := range []string{"c", "a", "b"} {
		mustAdd(t, r, "/user", n, uint64(i+1))
	}
	steps := []struct {
		remove string
		want   Addr
	}{
		{"", Addr{Node: "n1", Path: "/user/b", UID: 3}},
		{"/user/a", Addr{Node: "n1", Path: "/user/b", UID: 3}}, // a middle child
		{"/user/b", Addr{Node: "n1", Path: "/user/c", UID: 1}},
	}
	for _, s := range steps {
		if s.remove != "" {
			r.Remove(s.remove)
		}
		if got, ok := r.LastChild("/user"); !ok || got != s.want {
			t.Fatalf("after Remove(%q): LastChild = %+v, %v, want %+v", s.remove, got, ok, s.want)
		}
	}
	r.Remove("/user/c")
	if got, ok := r.LastChild("/user"); ok {
		t.Fatalf("LastChild after the last Remove = %+v, want none", got)
	}

	for i := range 1000 {
		mustAdd(t, r, "/user", "", uint64(i+1))
	}
	if n := testing.AllocsPerRun(100, func() { r.LastChild("/user") }); n != 0 {
		t.Errorf("LastChild of 1000 children allocates %v times, want 0", n)
	}
}

// removeAddr searches the child list from both ends: a stop of many children removes the newest
// first (SUP-070), and actors that stop on their own tend to stop oldest first. A child list never
// holds a path twice; the duplicates here only show which end the search reaches first. The slot
// the removal frees is cleared, so the list does not keep a removed address reachable.
func TestRemoveAddrSearchesBothEnds(t *testing.T) {
	a := func(path string, uid uint64) Addr { return Addr{Node: "n1", Path: path, UID: uid} }
	for _, tc := range []struct {
		name     string
		in, want []Addr
	}{
		{
			"newest end first",
			[]Addr{a("/x", 1), a("/b", 2), a("/c", 3), a("/d", 4), a("/x", 5)},
			[]Addr{a("/x", 1), a("/b", 2), a("/c", 3), a("/d", 4)},
		},
		{
			"oldest end before deeper",
			[]Addr{a("/x", 1), a("/b", 2), a("/c", 3), a("/x", 4), a("/e", 5)},
			[]Addr{a("/b", 2), a("/c", 3), a("/x", 4), a("/e", 5)},
		},
		{
			"middle",
			[]Addr{a("/a", 1), a("/b", 2), a("/x", 3), a("/d", 4), a("/e", 5)},
			[]Addr{a("/a", 1), a("/b", 2), a("/d", 4), a("/e", 5)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backing := tc.in[:len(tc.in):len(tc.in)]
			if got := removeAddr(tc.in, "/x"); !slices.Equal(got, tc.want) {
				t.Errorf("removeAddr = %+v, want %+v", got, tc.want)
			}
			if last := backing[len(backing)-1]; last != (Addr{}) {
				t.Errorf("the freed slot holds %+v, want the zero Addr", last)
			}
		})
	}
	in := []Addr{a("/a", 1), a("/b", 2)}
	if got := removeAddr(in, "/x"); !slices.Equal(got, []Addr{a("/a", 1), a("/b", 2)}) {
		t.Errorf("removeAddr of an absent path = %+v, want the list unchanged", got)
	}
}

func TestWatchAlive(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 5)
	watcher := w("/user/w", 9, 1)

	if alive, added := r.AddWatch(Addr{Node: "n1", Path: "/user/t", UID: 5}, watcher, 0); !alive || !added {
		t.Fatalf("AddWatch registered uid = %v, %v", alive, added)
	}
	// A path key (UID 0) is alive when the path is registered, whatever its uid.
	other := w("/user/w2", 10, 1)
	if alive, added := r.AddWatch(Addr{Node: "n1", Path: "/user/t"}, other, 0); !alive || !added {
		t.Fatalf("AddWatch path key = %v, %v", alive, added)
	}
	// A different uid at the same path is not alive.
	if alive, added := r.AddWatch(Addr{Node: "n1", Path: "/user/t", UID: 4}, w("/user/w3", 11, 1), 0); alive || added {
		t.Fatalf("AddWatch stale uid = %v, %v", alive, added)
	}
	// Never added, and roots.
	for _, k := range []Addr{
		{Node: "n1", Path: "/user/nope", UID: 5},
		{Node: "n1", Path: "/user/nope"},
		{Node: "n1", Path: "/user"},
		{},
	} {
		if alive, added := r.AddWatch(k, watcher, 0); alive || added {
			t.Fatalf("AddWatch(%+v, 0) = %v, %v", k, alive, added)
		}
	}
	// Removed.
	r.Remove("/user/t")
	if alive, added := r.AddWatch(Addr{Node: "n1", Path: "/user/t", UID: 5}, watcher, 0); alive || added {
		t.Fatalf("AddWatch of removed = %v, %v", alive, added)
	}
	if alive, _ := r.AddWatch(Addr{Node: "n1", Path: "/user/t"}, watcher, 0); alive {
		t.Fatal("path key alive after Remove")
	}
	if got := r.Watchers("/user/t"); len(got) != 0 {
		t.Fatalf("Watchers after Remove = %v", got)
	}
}

func TestWatchDuplicate(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 5)
	key := Addr{Node: "n1", Path: "/user/t", UID: 5}
	watcher := w("/user/w", 9, 1)
	if alive, added := r.AddWatch(key, watcher, 1); !alive || !added {
		t.Fatalf("first = %v, %v", alive, added)
	}
	if alive, added := r.AddWatch(key, watcher, 2); !alive || added {
		t.Fatalf("second = %v, %v; want alive and not added", alive, added)
	}
	// The one entry takes the second call's sequence number (ACT-630).
	if got := r.Watchers("/user/t"); len(got) != 1 || got[0].Seq != 2 {
		t.Fatalf("Watchers = %v, want one entry with Seq 2", got)
	}
	// Another incarnation of the same actor is another watcher; another key is another entry.
	if _, added := r.AddWatch(key, w("/user/w", 9, 2), 0); !added {
		t.Fatal("new incarnation not added")
	}
	if _, added := r.AddWatch(Addr{Node: "n1", Path: "/user/t"}, watcher, 0); !added {
		t.Fatal("same watcher under the path key not added")
	}
	if got := r.Watchers("/user/t"); len(got) != 3 {
		t.Fatalf("Watchers = %v, want three entries", got)
	}
	if !r.RemoveWatch(key, watcher) {
		t.Fatal("RemoveWatch reported nothing removed")
	}
	if r.RemoveWatch(key, watcher) {
		t.Fatal("second RemoveWatch removed something")
	}
	if len(r.listed) != 2 {
		t.Fatalf("index holds %d entries, want 2", len(r.listed))
	}
	if r.RemoveWatch(Addr{Node: "n1", Path: "/user/t", UID: 6}, w("/user/w", 9, 2)) {
		t.Fatal("RemoveWatch with a stale uid removed something")
	}
	if r.RemoveWatch(Addr{Node: "n1", Path: "/user/none", UID: 6}, watcher) {
		t.Fatal("RemoveWatch of an unknown target removed something")
	}
}

func TestWatcherOrder(t *testing.T) {
	// AT-ACT-31: W3, W1, W2 spawned in that order watch t in the order W2, W3, W1.
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 1)
	w3, w1, w2 := w("/user/w3", 2, 1), w("/user/w1", 3, 1), w("/user/w2", 4, 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	for _, x := range []Watcher{w2, w3, w1} {
		r.AddWatch(key, x, 0)
	}
	order := func() []Watcher {
		var out []Watcher
		for _, x := range r.Watchers("/user/t") {
			out = append(out, x.Watcher)
		}
		return out
	}
	if got, want := order(), []Watcher{w2, w3, w1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	// W2 unwatches and watches again: it moves to the end.
	if !r.RemoveWatch(key, w2) {
		t.Fatal("RemoveWatch(w2) failed")
	}
	r.AddWatch(key, w2, 0)
	if got, want := order(), []Watcher{w3, w1, w2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order after re-watch = %v, want %v", got, want)
	}
}

// AddWatch decides whether an entry is new from the registry's index of listed entries, without
// reading the target's list: n actors that watch one target would otherwise take a step per
// earlier watcher on each call. The list here holds a copy of the entry that the index lacks, which
// a real registry never has, so AddWatch adds the entry again instead of finding the copy.
func TestAddWatchChecksTheIndex(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	x := w("/user/x", 2, 1)
	planted := Watch{Watcher: x, Key: key, Seq: 1}
	r.entries["/user/t"].watches = []Watch{planted}
	if alive, added := r.AddWatch(key, x, 2); !alive || !added {
		t.Fatalf("AddWatch = %v, %v; want alive and added", alive, added)
	}
	if got, want := r.Watchers("/user/t"), []Watch{planted, {x, key, 2}}; !slices.Equal(got, want) {
		t.Fatalf("Watchers = %+v, want %+v", got, want)
	}
}

// A re-watch of a listed entry finds it from both ends of the target's list, the newest first, and
// gives it the new sequence number in place (ACT-630). The lists hold the entry twice, which a real
// list never does: a match at the newest end wins, then one at the oldest end, before any deeper
// match. The runtime never re-watches a listed entry (Watch returns early for an address in the
// watched set), so only the registry's own callers reach this search.
func TestRewatchSearchesBothEnds(t *testing.T) {
	x, o1, o2 := w("/user/x", 2, 1), w("/user/o1", 3, 1), w("/user/o2", 4, 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	e := func(x Watcher, seq uint64) Watch { return Watch{Watcher: x, Key: key, Seq: seq} }
	for _, tc := range []struct {
		name     string
		in, want []Watch
	}{
		{"newest end first", []Watch{e(x, 1), e(o1, 2), e(o2, 3), e(x, 4)}, []Watch{e(x, 1), e(o1, 2), e(o2, 3), e(x, 9)}},
		{"oldest end before deeper", []Watch{e(x, 1), e(o1, 2), e(x, 3), e(o2, 4)}, []Watch{e(x, 9), e(o1, 2), e(x, 3), e(o2, 4)}},
		{"middle", []Watch{e(o1, 1), e(x, 2), e(o2, 3)}, []Watch{e(o1, 1), e(x, 9), e(o2, 3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRegistry()
			mustAdd(t, r, "/user", "t", 1)
			if alive, added := r.AddWatch(key, x, 1); !alive || !added {
				t.Fatalf("first AddWatch = %v, %v", alive, added)
			}
			r.entries["/user/t"].watches = tc.in
			if alive, added := r.AddWatch(key, x, 9); !alive || added {
				t.Fatalf("re-watch = %v, %v; want alive and not added", alive, added)
			}
			if got := r.Watchers("/user/t"); !slices.Equal(got, tc.want) {
				t.Errorf("Watchers = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The index and the lists always agree in a real registry. Should a bug ever leave an entry in the
// index that its target's list lacks, a re-watch panics with a message that names the registry and
// the entry, not with an index out of range.
func TestRewatchPanicsWhenTheIndexDisagrees(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	x := w("/user/x", 2, 1)
	if alive, added := r.AddWatch(key, x, 1); !alive || !added {
		t.Fatalf("AddWatch = %v, %v", alive, added)
	}
	r.entries["/user/t"].watches = nil
	defer func() {
		if msg, _ := recover().(string); !strings.HasPrefix(msg, "registry: ") {
			t.Fatalf("panic = %q, want a message that starts with %q", msg, "registry: ")
		}
	}()
	r.AddWatch(key, x, 2)
}

// Remove takes the target's entries out of the index with its list: the watcher of a removed path
// watches the actor registered there next as a new entry, under the same key.
func TestWatchAfterRemove(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 1)
	pathKey := Addr{Node: "n1", Path: "/user/t"}
	a := w("/user/a", 2, 1)
	if alive, added := r.AddWatch(pathKey, a, 1); !alive || !added {
		t.Fatalf("first AddWatch = %v, %v", alive, added)
	}
	r.Remove("/user/t")
	mustAdd(t, r, "/user", "t", 3)
	if alive, added := r.AddWatch(pathKey, a, 2); !alive || !added {
		t.Fatalf("AddWatch after Remove = %v, %v; want alive and added", alive, added)
	}
	if got, want := r.Watchers("/user/t"), []Watch{{a, pathKey, 2}}; !slices.Equal(got, want) {
		t.Fatalf("Watchers = %+v, want %+v", got, want)
	}
}

// RemoveWatch decides whether there is an entry to remove from the registry's index of listed
// entries, without reading the target's list, so an Unwatch that has no entry takes no step per
// watcher of the target. The list here holds the entry and the index lacks it, which a real
// registry never has, so RemoveWatch reports nothing removed and leaves the list alone.
func TestRemoveWatchChecksTheIndex(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	x := w("/user/x", 2, 1)
	planted := Watch{Watcher: x, Key: key, Seq: 1}
	r.entries["/user/t"].watches = []Watch{planted}
	r.watching[x] = []string{"/user/t"}
	if r.RemoveWatch(key, x) {
		t.Fatal("RemoveWatch of an entry the index lacks reported it removed")
	}
	if got, want := r.Watchers("/user/t"), []Watch{planted}; !slices.Equal(got, want) {
		t.Fatalf("Watchers = %+v, want %+v", got, want)
	}
	if got, want := r.watching[x], []string{"/user/t"}; !slices.Equal(got, want) {
		t.Fatalf("drop index = %q, want %q", got, want)
	}
}

// RemoveWatch finds a listed entry from both ends of the target's list, the newest first. The lists
// hold the entry twice, which a real list never does: a match at the newest end goes, then one at
// the oldest end, before any deeper match, and the same watcher's entry under another key stays.
// Workers that watch one actor and unwatch it in watch order, or in the reverse, would otherwise
// take a step per remaining watcher on each call.
func TestRemoveWatchSearchesBothEnds(t *testing.T) {
	x, o1, o2 := w("/user/x", 2, 1), w("/user/o1", 3, 1), w("/user/o2", 4, 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	pathKey := Addr{Node: "n1", Path: "/user/t"}
	e := func(x Watcher, seq uint64) Watch { return Watch{Watcher: x, Key: key, Seq: seq} }
	for _, tc := range []struct {
		name     string
		in, want []Watch
	}{
		{"newest end first", []Watch{e(x, 1), e(o1, 2), e(o2, 3), e(x, 4)}, []Watch{e(x, 1), e(o1, 2), e(o2, 3)}},
		{"oldest end before deeper", []Watch{e(x, 1), e(o1, 2), e(x, 3), e(o2, 4)}, []Watch{e(o1, 2), e(x, 3), e(o2, 4)}},
		{"middle", []Watch{e(o1, 1), e(x, 2), e(o2, 3)}, []Watch{e(o1, 1), e(o2, 3)}},
		{"other key stays", []Watch{e(o1, 1), e(x, 2), {x, pathKey, 3}}, []Watch{e(o1, 1), {x, pathKey, 3}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRegistry()
			mustAdd(t, r, "/user", "t", 1)
			if alive, added := r.AddWatch(key, x, 1); !alive || !added {
				t.Fatalf("AddWatch = %v, %v", alive, added)
			}
			backing := tc.in[:len(tc.in):len(tc.in)]
			r.entries["/user/t"].watches = tc.in
			if !r.RemoveWatch(key, x) {
				t.Fatal("RemoveWatch reported nothing removed")
			}
			if got := r.Watchers("/user/t"); !slices.Equal(got, tc.want) {
				t.Errorf("Watchers = %+v, want %+v", got, tc.want)
			}
			if last := backing[len(backing)-1]; last != (Watch{}) {
				t.Errorf("the freed slot holds %+v, want the zero Watch", last)
			}
			if _, ok := r.listed[watchID{x, key}]; ok {
				t.Error("the removed entry is still in the index")
			}
			if _, ok := r.watching[x]; ok {
				t.Error("the removed entry's path is still in the drop index")
			}
		})
	}
}

func TestDropWatcher(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t1", 1)
	mustAdd(t, r, "/user", "t2", 2)
	k1 := Addr{Node: "n1", Path: "/user/t1", UID: 1}
	k2 := Addr{Node: "n1", Path: "/user/t2", UID: 2}
	a, b := w("/user/a", 3, 1), w("/user/b", 4, 1)
	r.AddWatch(k1, a, 0)
	r.AddWatch(k1, b, 0)
	r.AddWatch(k2, a, 0)
	r.AddWatch(Addr{Node: "n1", Path: "/user/t2"}, a, 0)
	r.AddWatch(k2, b, 0)
	r.DropWatcher(a)
	if got := r.Watchers("/user/t1"); len(got) != 1 || got[0].Watcher != b {
		t.Fatalf("t1 watchers = %v", got)
	}
	if got := r.Watchers("/user/t2"); len(got) != 1 || got[0].Watcher != b {
		t.Fatalf("t2 watchers = %v", got)
	}
	if len(r.listed) != 2 {
		t.Fatalf("index holds %d entries, want 2", len(r.listed))
	}
	// Dropping a watcher that watches nothing, or twice, is fine; a can watch again.
	r.DropWatcher(a)
	r.DropWatcher(w("/user/never", 99, 1))
	if _, added := r.AddWatch(k1, a, 0); !added {
		t.Fatal("watch after DropWatcher not added")
	}
	// A dropped watcher is not returned by Remove.
	r.DropWatcher(b)
	got := r.Remove("/user/t1")
	if len(got) != 1 || got[0].Watcher != a {
		t.Fatalf("Remove returned %v", got)
	}
	// Only the other incarnation stays.
	a2 := w("/user/a", 3, 2)
	r.AddWatch(k2, a2, 0)
	r.DropWatcher(a)
	if got := r.Watchers("/user/t2"); len(got) != 1 || got[0].Watcher != a2 {
		t.Fatalf("t2 watchers = %v", got)
	}
}

// ACT-640: DropWatcher removes one entry of the watcher per index entry, found from both ends of
// the target's list, the newest first. The lists hold the watcher twice for one index entry, which
// a real list never does: a match at the newest end wins, then one at the oldest end, before any
// deeper match. Workers that watch one actor stop newest first in a shutdown, so a search from one
// end only, or a scan of the whole list, would take a step per remaining watcher.
func TestDropWatcherSearchesBothEnds(t *testing.T) {
	x, o1, o2 := w("/user/x", 2, 1), w("/user/o1", 3, 1), w("/user/o2", 4, 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	e := func(x Watcher, seq uint64) Watch { return Watch{Watcher: x, Key: key, Seq: seq} }
	for _, tc := range []struct {
		name     string
		in, want []Watch
	}{
		{"newest end first", []Watch{e(x, 1), e(o1, 2), e(o2, 3), e(x, 4)}, []Watch{e(x, 1), e(o1, 2), e(o2, 3)}},
		{"oldest end before deeper", []Watch{e(x, 1), e(o1, 2), e(x, 3), e(o2, 4)}, []Watch{e(o1, 2), e(x, 3), e(o2, 4)}},
		{"middle", []Watch{e(o1, 1), e(x, 2), e(o2, 3)}, []Watch{e(o1, 1), e(o2, 3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRegistry()
			mustAdd(t, r, "/user", "t", 1)
			target := r.entries["/user/t"]
			backing := tc.in[:len(tc.in):len(tc.in)]
			target.watches = tc.in
			r.watching[x] = []string{"/user/t"}
			r.DropWatcher(x)
			if got := r.Watchers("/user/t"); !slices.Equal(got, tc.want) {
				t.Errorf("Watchers = %+v, want %+v", got, tc.want)
			}
			if last := backing[len(backing)-1]; last != (Watch{}) {
				t.Errorf("the freed slot holds %+v, want the zero Watch", last)
			}
			if _, ok := r.watching[x]; ok {
				t.Error("the dropped watcher is still in the drop index")
			}
		})
	}
}

// Remove takes the removed target's path out of each watcher's drop index, searching the index
// from both ends, the newest first. The indexes hold the path twice for one watch entry, which a
// real index never does. A monitor that watches many actors sees them stop newest first in a
// shutdown, so a search from the oldest end only would take a step per remaining target.
func TestRemoveUnindexesFromBothEnds(t *testing.T) {
	x := w("/user/m", 2, 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	for _, tc := range []struct {
		name     string
		in, want []string
	}{
		{"newest end first", []string{"/user/t", "/b", "/c", "/user/t"}, []string{"/user/t", "/b", "/c"}},
		{"oldest end before deeper", []string{"/user/t", "/b", "/user/t", "/c"}, []string{"/b", "/user/t", "/c"}},
		{"middle", []string{"/a", "/user/t", "/c"}, []string{"/a", "/c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRegistry()
			mustAdd(t, r, "/user", "t", 1)
			if alive, added := r.AddWatch(key, x, 1); !alive || !added {
				t.Fatalf("AddWatch = %v, %v", alive, added)
			}
			backing := tc.in[:len(tc.in):len(tc.in)]
			r.watching[x] = tc.in
			r.Remove("/user/t")
			if got := r.watching[x]; !slices.Equal(got, tc.want) {
				t.Errorf("drop index = %q, want %q", got, tc.want)
			}
			if last := backing[len(backing)-1]; last != "" {
				t.Errorf("the freed slot holds %q, want the empty path", last)
			}
		})
	}
}

// The drop index holds a target's path once per watch entry, so DropWatcher removes every entry,
// under the uid key and the path key, and RemoveWatch of one leaves the other's.
func TestDropWatcherAfterRemoveWatch(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	pathKey := Addr{Node: "n1", Path: "/user/t"}
	a, b := w("/user/a", 2, 1), w("/user/b", 3, 1)
	r.AddWatch(key, a, 1)
	r.AddWatch(key, b, 2)
	r.AddWatch(pathKey, a, 3)
	r.AddWatch(pathKey, b, 4)
	if !r.RemoveWatch(key, a) {
		t.Fatal("RemoveWatch reported nothing removed")
	}
	r.DropWatcher(a)
	if got, want := r.Watchers("/user/t"), []Watch{{b, key, 2}, {b, pathKey, 4}}; !slices.Equal(got, want) {
		t.Fatalf("Watchers = %v, want %v", got, want)
	}
	r.DropWatcher(b)
	if got := r.Watchers("/user/t"); len(got) != 0 {
		t.Fatalf("Watchers = %v, want none", got)
	}
	if len(r.watching) != 0 {
		t.Fatalf("drop index = %v, want empty", r.watching)
	}
	if len(r.listed) != 0 {
		t.Fatalf("index = %v, want empty", r.listed)
	}
}

func TestRemoveReturnsWatchers(t *testing.T) {
	r := newTestRegistry()
	mustAdd(t, r, "/user", "t", 1)
	key := Addr{Node: "n1", Path: "/user/t", UID: 1}
	pathKey := Addr{Node: "n1", Path: "/user/t"}
	a, b, c := w("/user/a", 2, 1), w("/user/b", 3, 1), w("/user/c", 4, 2)
	r.AddWatch(key, b, 7)
	r.AddWatch(pathKey, a, 8)
	r.AddWatch(key, c, 9)
	want := []Watch{{b, key, 7}, {a, pathKey, 8}, {c, key, 9}}
	if got := r.Remove("/user/t"); !reflect.DeepEqual(got, want) {
		t.Fatalf("Remove = %v, want %v", got, want)
	}
	if len(r.listed) != 0 {
		t.Fatalf("index = %v, want empty", r.listed)
	}
	if got := r.Watchers("/user/t"); len(got) != 0 {
		t.Fatalf("Watchers after Remove = %v", got)
	}
	// The entry's lists are gone: a re-added target starts empty.
	mustAdd(t, r, "/user", "t", 9)
	if got := r.Watchers("/user/t"); len(got) != 0 {
		t.Fatalf("new entry has watchers %v", got)
	}
	if got := r.Remove("/user/t"); len(got) != 0 {
		t.Fatalf("Remove of fresh entry = %v", got)
	}
	// Watchers of a removed target no longer appear in the drop index.
	r.DropWatcher(a)
}

func TestWatchRaceWithRemove(t *testing.T) {
	for i := 0; i < 1000; i++ {
		r := newTestRegistry()
		mustAdd(t, r, "/user", "t", 1)
		key := Addr{Node: "n1", Path: "/user/t", UID: 1}
		watcher := w("/user/w", 2, 1)
		var (
			wg      sync.WaitGroup
			alive   bool
			removed []Watch
			start   = make(chan struct{})
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			alive, _ = r.AddWatch(key, watcher, 0)
		}()
		go func() {
			defer wg.Done()
			<-start
			removed = r.Remove("/user/t")
		}()
		close(start)
		wg.Wait()
		found := false
		for _, x := range removed {
			if x.Watcher == watcher {
				found = true
			}
		}
		if alive != found {
			t.Fatalf("iteration %d: alive = %v but watcher in Remove result = %v", i, alive, found)
		}
		if got := r.Watchers("/user/t"); len(got) != 0 {
			t.Fatalf("iteration %d: leftover watchers %v", i, got)
		}
	}
}

func ExampleRegistry() {
	r := New()
	r.AddRoot(Addr{Node: "n1", Path: "/user"})
	p, _ := r.Add("/user", "", fixed(1), nil)
	fmt.Println(p)
	// Output: /user/$1
}
