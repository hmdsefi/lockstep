// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"encoding/json"
	"fmt"
	"strconv"
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
