// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"math/bits"
	"slices"
	"strings"
)

// queued is one update in the broadcast queue.
type queued struct {
	u     update
	count int    // transmissions so far
	seq   uint64 // enqueue order; larger is newer
}

// broadcasts is the broadcast queue (CLU-400): the updates still to be piggybacked on probe
// traffic, one per node name. It is pure: no clock, no map iteration; entries are kept sorted by
// node name. It is not safe for concurrent use; the membership actor owns it.
type broadcasts struct {
	items []queued // sorted by u.Node
	seq   uint64
}

// newBroadcasts returns an empty queue.
func newBroadcasts() *broadcasts { return &broadcasts{} }

func (b *broadcasts) find(name string) (int, bool) {
	return slices.BinarySearchFunc(b.items, name, func(q queued, n string) int { return strings.Compare(q.u.Node, n) })
}

// len returns the number of queued updates.
func (b *broadcasts) len() int { return len(b.items) }

// enqueue queues u. An update already queued about the same node is replaced, and its transmit
// count starts again at 0.
func (b *broadcasts) enqueue(u update) {
	b.seq++
	q := queued{u: u, seq: b.seq}
	i, ok := b.find(u.Node)
	if ok {
		b.items[i] = q
		return
	}
	b.items = slices.Insert(b.items, i, q)
}

// take selects the updates for one outgoing message. forced, when not nil, is the sender's belief
// about the recipient to correct (it is not queued and is not counted against max); when the
// queue holds an update about forced.Node, that queued update is the forced one instead, counts
// as a transmission and is not repeated. Then up to max queued updates follow, by transmit count
// ascending, ties newest enqueue first. Every queued update taken has its count raised by 1 and
// is removed once the count reaches limit.
func (b *broadcasts) take(forced *update, max, limit int) []update {
	var out []update
	skip := -1
	if forced != nil {
		if i, ok := b.find(forced.Node); ok {
			skip = i
			out = append(out, b.items[i].u)
		} else {
			out = append(out, *forced)
		}
	}

	order := make([]int, 0, len(b.items))
	for i := range b.items {
		if i != skip {
			order = append(order, i)
		}
	}
	slices.SortFunc(order, func(x, y int) int {
		a, c := b.items[x], b.items[y]
		if a.count != c.count {
			return a.count - c.count
		}
		switch {
		case a.seq > c.seq:
			return -1
		case a.seq < c.seq:
			return 1
		}
		return 0
	})
	if max < 0 {
		max = 0
	}
	if len(order) > max {
		order = order[:max]
	}
	for _, i := range order {
		out = append(out, b.items[i].u)
	}

	// Count the transmissions, then drop the updates that reached the limit.
	if skip >= 0 {
		order = append(order, skip)
	}
	for _, i := range order {
		b.items[i].count++
	}
	b.items = slices.DeleteFunc(b.items, func(q queued) bool { return q.count >= limit })
	return out
}

// forcedUpdate returns the forced update for a message to recipient (CLU-400): the table's belief
// about it when that is Suspect or Dead, or a tombstone, so that the recipient can correct it. It
// returns nil when the table believes the recipient is Alive or Left, or knows nothing of it.
func forcedUpdate(t *table, recipient string) *update {
	m, tomb, ok := t.Belief(recipient)
	if !ok || (!tomb && m.State != Suspect && m.State != Dead) {
		return nil
	}
	u := toUpdate(m)
	return &u
}

// retransmitLimit returns the number of transmissions after which a queued update is removed:
// retransmitMult * bits.Len(uint(n)), where n is the number of entries of the member table.
func retransmitLimit(retransmitMult, n int) int {
	return retransmitMult * bits.Len(uint(n))
}
