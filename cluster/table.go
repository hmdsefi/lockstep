// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"slices"
	"strings"
	"time"
)

// Bounds of the identity rules (CLU-150).
const (
	maxRetired    = 16   // retired IDs kept per node name, oldest evicted first
	maxTombstones = 4096 // tombstones kept in all, oldest evicted first
)

// sourceSelf is the source of a change the node makes to its own entry (§8).
const sourceSelf = "self"

// change is one change of the member table (CLU-170). The membership actor records it as
// lockstep.cluster.member and enqueues the new entry, member(), in the broadcast queue.
type change struct {
	Node       string
	ID         uint32
	Inc        uint64
	From, To   MemberState // From is 0 for an addition, which the member record prints as "none"
	OldID      uint32      // the replaced ID when Superseded
	Superseded bool
	Source     string // where the update came from (CLU-410), or "self"
}

// member returns the entry the change produced.
func (c change) member() Member {
	return Member{Node: c.Node, ID: c.ID, Incarnation: c.Inc, State: c.To}
}

// applyResult says what Apply did with an update.
type applyResult uint8

const (
	applyIgnored   applyResult = iota // the update changed nothing
	applyChanged                      // the update changed the table; the change is returned
	applyAboutSelf                    // the update is about the own entry (own name and ID)
)

// record holds what the table knows about one node name: its entry or, after the entry was
// reaped, its tombstone (CLU-140), plus the IDs retired for the name (CLU-130).
type record struct {
	m       Member
	changed time.Time // when the entry last changed (CLU-160)
	tomb    bool      // m is a tombstone, not an entry
	tombSeq uint64    // when tomb, the order in which tombstones were made (CLU-150)
	retired []uint32  // oldest first, at most maxRetired
}

// isRetired reports whether id is retired for the record's name.
func (r *record) isRetired(id uint32) bool { return slices.Contains(r.retired, id) }

// retire retires id for the record's name, evicting the oldest retired ID beyond maxRetired.
// Retiring an ID that is already retired changes nothing (CLU-150).
func (r *record) retire(id uint32) {
	if r.isRetired(id) {
		return
	}
	r.retired = append(r.retired, id)
	if len(r.retired) > maxRetired {
		r.retired = slices.Delete(r.retired, 0, len(r.retired)-maxRetired)
	}
}

// table is the member table (CLU-100 to CLU-170). It is pure: it never reads a clock, its callers
// pass the current time, and its records are kept sorted by node name. Each name has either an
// entry or a tombstone, never both. The own entry is created with the table and never removed.
//
// A table is not safe for concurrent use; the membership actor owns it.
type table struct {
	self      string        // the own node name
	retention time.Duration // Config.DeadRetention
	recs      []record      // sorted by m.Node
	tombs     int           // number of records that are tombstones
	tombSeq   uint64        // the tombSeq of the newest tombstone
}

// newTable returns a table that holds only the own entry, self, created at now. Dead and Left
// entries are reaped retention after their last change.
func newTable(self Member, retention time.Duration, now time.Time) *table {
	return &table{
		self:      self.Node,
		retention: retention,
		recs:      []record{{m: self, changed: now}},
	}
}

// find returns the index of the record of name, or where it would be inserted, and whether it
// exists.
func (t *table) find(name string) (int, bool) {
	return slices.BinarySearchFunc(t.recs, name, func(r record, n string) int { return strings.Compare(r.m.Node, n) })
}

// own returns the own record.
func (t *table) own() *record {
	i, _ := t.find(t.self)
	return &t.recs[i]
}

// Self returns the own entry.
func (t *table) Self() Member { return t.own().m }

// Len returns the number of entries, self included; tombstones do not count.
func (t *table) Len() int { return len(t.recs) - t.tombs }

// Get returns the entry of name, and whether there is one. A tombstone is not an entry.
func (t *table) Get(name string) (Member, bool) {
	i, ok := t.find(name)
	if !ok || t.recs[i].tomb {
		return Member{}, false
	}
	return t.recs[i].m, true
}

// Members returns every entry, self included, sorted by node name, in a new slice.
func (t *table) Members() []Member {
	out := make([]Member, 0, t.Len())
	for i := range t.recs {
		if !t.recs[i].tomb {
			out = append(out, t.recs[i].m)
		}
	}
	return out
}

// Belief returns what the table believes about name for the forced update (CLU-140, CLU-400): its
// entry, or else its tombstone (tomb is true). ok is false when the table knows nothing of name.
func (t *table) Belief(name string) (m Member, tomb, ok bool) {
	i, found := t.find(name)
	if !found {
		return Member{}, false, false
	}
	return t.recs[i].m, t.recs[i].tomb, true
}

// wins reports whether the update u beats the entry or tombstone e with the same ID (CLU-110).
// Nothing beats Left; Left beats everything else.
func wins(e, u Member) bool {
	switch {
	case e.State == Left:
		return false
	case u.State == Left:
		return true
	case e.State == Alive && u.State == Suspect, e.State != Dead && u.State == Dead:
		return u.Incarnation >= e.Incarnation
	default:
		return u.Incarnation > e.Incarnation
	}
}

// Apply merges the update u, received from source, into the table at now and returns the change
// it made (CLU-110 to CLU-140). direct says that u is direct evidence from the member's own
// process: the From of a message it sent, or the actor's own conclusion about the entry. Any other
// update, an element of a message's Updates or Members, is hearsay. The rules:
//
//   - An update about the own name with the own ID changes nothing and is reported as
//     applyAboutSelf, so the actor can refute it. Another ID of the own name is ignored, and
//     retired when its state is up.
//   - An update with an ID retired for its name, or with an empty node or an undefined state, is
//     ignored.
//   - An update about an unknown name adds an entry when its state is up.
//   - An update with the ID of the name's entry or tombstone replaces its state and incarnation
//     when it wins by CLU-110; a tombstone is restored to an entry.
//   - An up update with another ID replaces the entry (Superseded, From the old state) or the
//     tombstone (an addition); a down one is ignored. Only direct evidence retires the old ID.
//     Hearsay with another ID does not replace an up entry, and when it replaces a down entry or
//     a tombstone the old ID stays usable, since IDs are not ordered and the hearsay may be about
//     an older process of the name than the entry. The member's own messages, or the next probe of
//     the name, bring its current ID directly, and that direct evidence retires the stale one.
func (t *table) Apply(u Member, direct bool, source string, now time.Time) (change, applyResult) {
	if u.Node == "" || u.State < Alive || u.State > Left {
		return change{}, applyIgnored
	}
	if u.Node == t.self {
		r := t.own()
		if u.ID == r.m.ID {
			return change{}, applyAboutSelf
		}
		if u.State.Up() {
			r.retire(u.ID)
		}
		return change{}, applyIgnored
	}
	c := change{Node: u.Node, ID: u.ID, Inc: u.Incarnation, To: u.State, Source: source}
	i, found := t.find(u.Node)
	if !found {
		if !u.State.Up() {
			return change{}, applyIgnored
		}
		t.recs = slices.Insert(t.recs, i, record{m: u, changed: now})
		return c, applyChanged
	}
	r := &t.recs[i]
	switch {
	case r.isRetired(u.ID):
		return change{}, applyIgnored
	case u.ID == r.m.ID:
		if !wins(r.m, u) {
			return change{}, applyIgnored
		}
		c.From = r.m.State
	case !u.State.Up(), !direct && !r.tomb && r.m.State.Up():
		return change{}, applyIgnored
	default:
		if direct {
			r.retire(r.m.ID)
		}
		if !r.tomb {
			c.From, c.OldID, c.Superseded = r.m.State, r.m.ID, true
		}
	}
	if r.tomb {
		r.tomb = false
		t.tombs--
	}
	r.m, r.changed = u, now
	return c, applyChanged
}

// SetSelf sets the own entry's state and incarnation at now and returns the change, or false when
// both are already set. The own entry changes only through SetSelf.
func (t *table) SetSelf(state MemberState, inc uint64, now time.Time) (change, bool) {
	r := t.own()
	if r.m.State == state && r.m.Incarnation == inc {
		return change{}, false
	}
	c := change{Node: r.m.Node, ID: r.m.ID, Inc: inc, From: r.m.State, To: state, Source: sourceSelf}
	r.m.State, r.m.Incarnation, r.changed = state, inc, now
	return c, true
}

// Reap turns the entry of name into a tombstone when it still has id, inc and state, state is Dead
// or Left, and its last change is at least the retention before now (CLU-160). The own entry is
// never reaped. Beyond maxTombstones the oldest tombstone is evicted with its retired IDs, so the
// table forgets that name. Reap reports whether it reaped.
func (t *table) Reap(name string, id uint32, inc uint64, state MemberState, now time.Time) bool {
	if name == t.self || state.Up() {
		return false
	}
	i, ok := t.find(name)
	if !ok {
		return false
	}
	r := &t.recs[i]
	if r.tomb || r.m.ID != id || r.m.Incarnation != inc || r.m.State != state || now.Sub(r.changed) < t.retention {
		return false
	}
	t.tombSeq++
	r.tomb, r.tombSeq = true, t.tombSeq
	t.tombs++
	if t.tombs > maxTombstones {
		t.evictOldestTombstone()
	}
	return true
}

// evictOldestTombstone removes the record of the oldest tombstone (CLU-150).
func (t *table) evictOldestTombstone() {
	oldest := -1
	for i := range t.recs {
		if t.recs[i].tomb && (oldest < 0 || t.recs[i].tombSeq < t.recs[oldest].tombSeq) {
			oldest = i
		}
	}
	t.recs = slices.Delete(t.recs, oldest, oldest+1)
	t.tombs--
}
