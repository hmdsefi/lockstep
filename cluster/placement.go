// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"slices"

	"github.com/hmdsefi/tenure/core"
)

// Score returns the rendezvous score of node for the grain key of kind. A higher score wins. It is
// the 64-bit FNV-1a hash of kind, a zero byte, key, a zero byte and node, passed through
// MurmurHash3's 64-bit finalizer, so every node computes the same score from the same names.
func Score(node, kind, key string) uint64 { return core.Score(kind, key, node) }

// Owner returns the node that owns the grain key of kind in view v: among the up members that host
// kind, the one with the highest Score, and on equal scores the lowest node name. A suspect member
// is up and counts; the member's ID does not. It returns false when no up member hosts kind.
func Owner(v *View, kind, key string) (string, bool) {
	r := rank(v, kind, key, 1)
	if len(r) == 0 {
		return "", false
	}
	return r[0], true
}

// rank returns the first n nodes of the ranking of the up members of v that host kind for key
// (VAC-110): score descending, ties by node name ascending.
func rank(v *View, kind, key string, n int) []string {
	best := core.Rank(kind, key, hosting(v.Members, kind), n)
	if len(best) == 0 {
		return nil
	}
	nodes := make([]string, len(best))
	for i, m := range best {
		nodes[i] = m.ID
	}
	return nodes
}

// runnerUp returns the second node of the ranking for key (VAC-120), the node that takes the key
// over when its owner goes. It returns false when fewer than two up members host kind.
func runnerUp(v *View, kind, key string) (string, bool) {
	r := rank(v, kind, key, 2)
	if len(r) < 2 {
		return "", false
	}
	return r[1], true
}

// hosting returns, for the members ms sorted by node, a core.Member per up member whose Kinds
// contain kind, in the same order. Hosts build it once per published view and kind, then look keys
// up with lookup.
func hosting(ms []Member, kind string) []core.Member {
	var out []core.Member
	for _, m := range ms {
		if m.State.Up() && slices.Contains(m.Kinds, kind) {
			out = append(out, core.Member{ID: m.Node})
		}
	}
	return out
}

// lookup returns the positions in members (from hosting) of the owner and the runner-up of key, or
// -1 for a place the ranking does not have. It allocates nothing.
func lookup(members []core.Member, kind, key string) (owner, runner int) {
	var buf [2]int
	r := core.AppendRank(buf[:0], kind, key, members, 2)
	owner, runner = -1, -1
	if len(r) > 0 {
		owner = r[0]
	}
	if len(r) > 1 {
		runner = r[1]
	}
	return owner, runner
}

// placement is the immutable placement of one published view (VAC-110): for every kind named in
// the Kinds of an up member, sorted by name, the hosting members (from hosting). The membership
// actor builds it when it publishes the view, so that Grain.Send and the hosts rank keys with
// lookup and allocate nothing per message.
type placement struct {
	kinds   []string
	members [][]core.Member
}

// newPlacement returns the placement of the members ms, sorted by node.
func newPlacement(ms []Member) *placement {
	var kinds []string
	for _, m := range ms {
		if m.State.Up() {
			kinds = append(kinds, m.Kinds...)
		}
	}
	slices.Sort(kinds)
	kinds = slices.Compact(kinds)
	p := &placement{kinds: kinds, members: make([][]core.Member, len(kinds))}
	for i, k := range kinds {
		p.members[i] = hosting(ms, k)
	}
	return p
}

// hosts returns the hosting members of kind, sorted by node, or nil when no up member hosts it.
// The slice is shared: callers must not modify it. A nil placement hosts nothing.
func (p *placement) hosts(kind string) []core.Member {
	if p == nil {
		return nil
	}
	i, ok := slices.BinarySearch(p.kinds, kind)
	if !ok {
		return nil
	}
	return p.members[i]
}

// owner returns the owner of key of kind, as Owner does for the view of p, and false when no up
// member hosts kind. It allocates nothing.
func (p *placement) owner(kind, key string) (string, bool) {
	ms := p.hosts(kind)
	o, _ := lookup(ms, kind, key)
	if o < 0 {
		return "", false
	}
	return ms[o].ID, true
}

const (
	maxKeyLen = 256
	upperHex  = "0123456789ABCDEF"
)

// validKey reports whether key is a grain key (VAC-020): 1 to 256 bytes, any bytes.
func validKey(key string) bool { return len(key) >= 1 && len(key) <= maxKeyLen }

// keepByte reports whether escapeKey writes b as it is: A-Z a-z 0-9 - . _
func keepByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' ||
		b == '-' || b == '.' || b == '_'
}

// escapeKey maps a key to the actor name of its activation (VAC-030): it keeps A-Z a-z 0-9 - . _
// and writes every other byte as %XX with uppercase hex digits. The keys "." and ".." are not
// actor names, so every dot of them is escaped. The mapping is injective because a '%' is always
// escaped. The result for a valid key has 1 to 768 bytes.
func escapeKey(key string) string {
	dots := key == "." || key == ".."
	out := make([]byte, 0, len(key)+8)
	for i := 0; i < len(key); i++ {
		b := key[i]
		if keepByte(b) && (!dots || b != '.') {
			out = append(out, b)
			continue
		}
		out = append(out, '%', upperHex[b>>4], upperHex[b&0xF])
	}
	return string(out)
}

// unescapeKey reverses escapeKey. It returns false for a string that escapeKey cannot produce from
// a valid key: a bad or lowercase %XX, a %XX of a byte escapeKey keeps, a byte outside the kept
// set, or a key of the wrong length.
func unescapeKey(s string) (string, bool) {
	key := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			key = append(key, s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", false
		}
		hi, ok1 := upperHexValue(s[i+1])
		lo, ok2 := upperHexValue(s[i+2])
		if !ok1 || !ok2 {
			return "", false
		}
		key = append(key, hi<<4|lo)
		i += 2
	}
	k := string(key)
	// Escaping the key again rejects everything that is not its canonical form.
	if !validKey(k) || escapeKey(k) != s {
		return "", false
	}
	return k, true
}

func upperHexValue(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
