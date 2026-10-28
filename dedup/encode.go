// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"encoding/json"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

// entryField appends the type's own field of an entry holding v, after its "at": `,"r":<reply>`
// for Replies, `,"v":<version>` for Versions. Window has none and passes nil. An error is the
// value's encoding error; marshal adds the entry index.
type entryField[V any] func(b []byte, v V) ([]byte, error)

// marshal returns s's encoding as type k (DLV-200):
//
//	{"dedup":1,"type":"<tag>","capacity":<n>,"ttl":<ns>,"last":<ns>,"entries":[<entry>,...]}
//
// with each entry {"k":<key>,"at":<ns>} followed by field's output, in write order. A zero value
// returns k's DLV-100 error, and an entry that does not encode `dedup: encode entry <i>: <err>`;
// either way no bytes.
func (s *store[K, V]) marshal(k kind, field entryField[V]) ([]byte, error) {
	if s.cfg.Capacity == 0 {
		return nil, k.unusable()
	}
	b := make([]byte, 0, 96+32*len(s.index))
	b = append(b, `{"dedup":1,"type":"`...)
	b = append(b, k.tag...)
	b = append(b, `","capacity":`...)
	b = strconv.AppendInt(b, int64(s.cfg.Capacity), 10)
	b = append(b, `,"ttl":`...)
	b = strconv.AppendInt(b, int64(s.cfg.TTL), 10)
	b = append(b, `,"last":`...)
	b = strconv.AppendInt(b, s.last, 10)
	b = append(b, `,"entries":[`...)
	i := 0
	for n := s.head; n != nil; n = n.next {
		if i > 0 {
			b = append(b, ',')
		}
		key, err := json.Marshal(n.k)
		if err != nil {
			return nil, fmt.Errorf("dedup: encode entry %d: %w", i, err)
		}
		b = append(b, `{"k":`...)
		b = append(b, key...)
		b = append(b, `,"at":`...)
		b = strconv.AppendInt(b, n.at, 10)
		if field != nil {
			if b, err = field(b, n.v); err != nil {
				return nil, fmt.Errorf("dedup: encode entry %d: %w", i, err)
			}
		}
		b = append(b, '}')
		i++
	}
	b = append(b, "]}"...)
	return b, nil
}

// rawEntry is one entry of an encoding before its key and its type's own field are decoded. A
// store's entryValue reads R or V.
type rawEntry struct {
	K  json.RawMessage `json:"k"`
	At int64           `json:"at"`
	R  json.RawMessage `json:"r"`
	V  uint64          `json:"v"`
}

// envelope is the whole encoding, as decoded by encoding/json (unknown fields are ignored).
type envelope struct {
	Dedup    int        `json:"dedup"`
	Type     string     `json:"type"`
	Capacity int        `json:"capacity"`
	TTL      int64      `json:"ttl"`
	Last     int64      `json:"last"`
	Entries  []rawEntry `json:"entries"`
}

// entryValue returns the value of an entry from its raw fields: the decoded reply for Replies, the
// version for Versions. Window has none and passes nil. An error is the value's decoding error,
// which names the field ("reply: <err>"); unmarshal adds the entry index.
type entryValue[V any] func(e *rawEntry) (V, error)

// invalidf returns an error wrapping ErrInvalidEncoding with the formatted text appended.
func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidEncoding}, args...)...)
}

// unmarshal replaces s with the store that data encodes as type k (DLV-210). It checks data in the
// order of DLV-210, builds the entries into a fresh store and assigns it to s only when every check
// passes, so any error leaves s unchanged. data "null" is a no-op.
func (s *store[K, V]) unmarshal(k kind, data []byte, value entryValue[V]) error {
	var env *envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return invalidf("%w", err)
	}
	if env == nil { // null
		return nil
	}
	if env.Dedup != 1 {
		return invalidf("format %d, want 1", env.Dedup)
	}
	if env.Type != k.tag {
		return invalidf("type %q, want %q", env.Type, k.tag)
	}
	cfg := Config{Capacity: env.Capacity, TTL: time.Duration(env.TTL)}
	if p := cfg.problem(); p != "" {
		return invalidf("%s", p)
	}
	if len(env.Entries) > env.Capacity {
		return invalidf("%d entries, more than capacity %d", len(env.Entries), env.Capacity)
	}

	fresh := store[K, V]{
		cfg:   cfg,
		last:  env.Last,
		index: make(map[K]*node[K, V], len(env.Entries)), // never sized by the decoded capacity
	}
	nodes := make([]*node[K, V], 0, len(env.Entries)) // by entry index, to name j in a duplicate
	for i := range env.Entries {
		e := &env.Entries[i]
		var key K
		if err := json.Unmarshal(e.K, &key); err != nil {
			return invalidf("entry %d: key: %w", i, err)
		}
		var v V
		if value != nil {
			var err error
			if v, err = value(e); err != nil {
				return invalidf("entry %d: %w", i, err)
			}
		}
		if i > 0 && e.At < env.Entries[i-1].At {
			return invalidf("entry %d: at %d before entry %d's %d", i, e.At, i-1, env.Entries[i-1].At)
		}
		if e.At > env.Last {
			return invalidf("entry %d: at %d after last %d", i, e.At, env.Last)
		}
		prev, dup, err := lookup(fresh.index, key)
		if err != nil {
			return invalidf("entry %d: key: %v", i, err)
		}
		if dup {
			j := slices.Index(nodes, prev)
			return invalidf("entry %d: key equals entry %d's", i, j)
		}
		n := &node[K, V]{k: key, v: v, at: e.At}
		fresh.index[key] = n
		fresh.append(n)
		nodes = append(nodes, n)
	}
	*s = fresh
	return nil
}

// lookup returns index[key]. A key whose dynamic type cannot be hashed, such as a map decoded into
// an interface K, makes the map panic; lookup recovers that runtime error, and no other panic, and
// returns it, so such a key fails the decode instead of crashing it.
func lookup[K comparable, V any](index map[K]*node[K, V], key K) (n *node[K, V], ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			re, isRuntime := r.(runtime.Error)
			if !isRuntime || !strings.Contains(re.Error(), "hash of unhashable type") {
				panic(r)
			}
			err = re
		}
	}()
	n, ok = index[key]
	return n, ok, nil
}
