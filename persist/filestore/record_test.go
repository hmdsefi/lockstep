// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/persist"
)

var testCRC = crc32.MakeTable(crc32.Castagnoli)

// frame returns payload behind a correct record header.
func frame(payload []byte) []byte {
	b := make([]byte, 8, 8+len(payload))
	binary.LittleEndian.PutUint32(b[0:], uint32(len(payload))) //nolint:gosec // small test payloads
	binary.LittleEndian.PutUint32(b[4:], crc32.Checksum(payload, testCRC))
	return append(b, payload...)
}

func u32(v uint32) []byte { return binary.LittleEndian.AppendUint32(nil, v) }
func u64(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func TestRecordCodec(t *testing.T) {
	events := []persist.Event{{Seq: 1, Data: []byte("ab")}, {Seq: 2, Data: []byte("c")}}

	t.Run("journal bytes", func(t *testing.T) {
		payload := cat(u64(1), u32(2), u32(2), []byte("ab"), u32(1), []byte("c"))
		want := cat(u32(0x17), u32(crc32.Checksum(payload, testCRC)), payload)
		if len(want) != 31 {
			t.Fatalf("expected record is %d bytes, want 31", len(want))
		}
		got, err := encodeJournalRecord(events)
		if err != nil {
			t.Fatalf("encodeJournalRecord: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("record = % x\nwant     % x", got, want)
		}
		// The literal bytes of the format, independent of the helpers above.
		if !bytes.HasPrefix(got, []byte{0x17, 0, 0, 0}) ||
			!bytes.Equal(got[8:], []byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 'a', 'b', 1, 0, 0, 0, 'c'}) {
			t.Fatalf("record layout is wrong: % x", got)
		}
	})

	t.Run("journal round trip", func(t *testing.T) {
		batch := []persist.Event{{Seq: 40, Data: []byte("x")}, {Seq: 41, Data: nil}, {Seq: 42, Data: []byte("zzz")}}
		rec, err := encodeJournalRecord(batch)
		if err != nil {
			t.Fatal(err)
		}
		got, n, err := parseJournalRecord(cat(rec, []byte("trailing bytes of the next record")))
		if err != nil {
			t.Fatalf("parseJournalRecord: %v", err)
		}
		if n != len(rec) {
			t.Fatalf("consumed %d bytes, want %d", n, len(rec))
		}
		if len(got) != len(batch) {
			t.Fatalf("parsed %d events, want %d", len(got), len(batch))
		}
		for i := range batch {
			if got[i].Seq != batch[i].Seq || !bytes.Equal(got[i].Data, batch[i].Data) {
				t.Fatalf("event %d = %+v, want %+v", i, got[i], batch[i])
			}
		}
	})

	t.Run("parsed data does not alias the neighbours", func(t *testing.T) {
		rec, _ := encodeJournalRecord(events)
		got, _, err := parseJournalRecord(rec)
		if err != nil {
			t.Fatal(err)
		}
		_ = append(got[0].Data, 'X', 'X', 'X') // must not overwrite event 2's length or data
		again, _, err := parseJournalRecord(rec)
		if err != nil || !bytes.Equal(again[1].Data, []byte("c")) {
			t.Fatalf("record changed through an appended slice: %v % x", err, rec)
		}
	})

	t.Run("snapshot bytes and round trip", func(t *testing.T) {
		payload := cat(u64(7), []byte("state"))
		want := cat(u32(13), u32(crc32.Checksum(payload, testCRC)), payload)
		got, err := encodeSnapshotRecord(persist.Snapshot{Seq: 7, Data: []byte("state")})
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("snapshot record = % x, %v\nwant % x", got, err, want)
		}
		snap, n, err := parseSnapshotRecord(got)
		if err != nil || n != len(got) || snap.Seq != 7 || string(snap.Data) != "state" {
			t.Fatalf("parseSnapshotRecord = %+v, %d, %v", snap, n, err)
		}
		empty, err := encodeSnapshotRecord(persist.Snapshot{Seq: 1})
		if err != nil || len(empty) != 16 {
			t.Fatalf("empty snapshot record = % x, %v; want 16 bytes", empty, err)
		}
		if snap, _, err := parseSnapshotRecord(empty); err != nil || snap.Seq != 1 || len(snap.Data) != 0 {
			t.Fatalf("parse of empty snapshot = %+v, %v", snap, err)
		}
	})

	t.Run("sizes at the limits", func(t *testing.T) {
		// A journal payload is 12 + 4 + len(data) for one event.
		fits := []persist.Event{{Seq: 1, Data: make([]byte, MaxRecordSize-16)}}
		rec, err := encodeJournalRecord(fits)
		if err != nil {
			t.Fatalf("batch of exactly MaxRecordSize payload bytes: %v", err)
		}
		if _, _, err := parseJournalRecord(rec); err != nil {
			t.Fatalf("parse of a maximal record: %v", err)
		}
		over := []persist.Event{{Seq: 1, Data: make([]byte, MaxRecordSize-15)}}
		if _, err := encodeJournalRecord(over); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("oversize batch: %v, want ErrTooLarge", err)
		}
		many := make([]persist.Event, 0, 5)
		for i := range 5 {
			many = append(many, persist.Event{Seq: uint64(i + 1), Data: make([]byte, 4<<20)})
		}
		if _, err := encodeJournalRecord(many); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("20 MiB batch of five events: %v, want ErrTooLarge", err)
		}
		if _, err := encodeSnapshotRecord(persist.Snapshot{Seq: 1, Data: make([]byte, MaxSnapshotSize-8)}); err != nil {
			t.Fatalf("snapshot of exactly MaxSnapshotSize payload bytes: %v", err)
		}
		if _, err := encodeSnapshotRecord(persist.Snapshot{Seq: 1, Data: make([]byte, MaxSnapshotSize-7)}); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("oversize snapshot: %v, want ErrTooLarge", err)
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		if _, err := encodeJournalRecord(nil); err == nil {
			t.Fatal("encodeJournalRecord(nil) = nil error")
		}
	})
}

// TestRecordValidity is the validity table of a journal record.
func TestRecordValidity(t *testing.T) {
	good, err := encodeJournalRecord([]persist.Event{{Seq: 9, Data: []byte("ab")}, {Seq: 10, Data: []byte("c")}})
	if err != nil {
		t.Fatal(err)
	}
	flip := func(b []byte, i int) []byte {
		c := bytes.Clone(b)
		c[i] ^= 0x01
		return c
	}
	header := func(l uint32) []byte { return cat(u32(l), u32(0)) }

	tests := []struct {
		name string
		rec  []byte
		why  string // the reason the error message names
	}{
		{"empty", nil, "incomplete header"},
		{"incomplete header", good[:7], "incomplete header"},
		{"L below the minimum", header(15), "out of range"},
		{"L zero", header(0), "out of range"},
		{"L above the maximum, payload absent", header(MaxRecordSize + 1), "out of range"},
		{"incomplete payload", good[:len(good)-1], "incomplete payload"},
		{"header only", good[:8], "incomplete payload"},
		{"checksum mismatch in payload", flip(good, 12), "checksum mismatch"},
		{"checksum mismatch in header", flip(good, 5), "checksum mismatch"},
		{"count zero", frame(cat(u64(1), u32(0), u32(0))), "count 0"},
		{"entry length beyond the payload", frame(cat(u64(1), u32(1), u32(99), []byte("ab"))), "entry lengths"},
		{"entry lengths short of the payload", frame(cat(u64(1), u32(1), u32(1), []byte("ab"))), "entry lengths"},
		{"count above the entries present", frame(cat(u64(1), u32(2), u32(1), []byte("a"))), "entry lengths"},
		{"count larger than the payload can hold", frame(cat(u64(1), u32(0xFFFFFFFF), u32(0))), "entry lengths"},
		{"second entry length cut off", frame(cat(u64(1), u32(2), u32(4), []byte("abcd"))), "entry lengths"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			events, n, err := parseJournalRecord(tc.rec)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
			if !strings.Contains(err.Error(), tc.why) {
				t.Fatalf("err = %q, want it to mention %q", err, tc.why)
			}
			if events != nil || n != 0 {
				t.Fatalf("got %d events and n = %d with the error", len(events), n)
			}
		})
	}

	t.Run("snapshot", func(t *testing.T) {
		snapGood, _ := encodeSnapshotRecord(persist.Snapshot{Seq: 3, Data: []byte("data")})
		for _, tc := range []struct {
			name, why string
			rec       []byte
		}{
			{"incomplete header", "incomplete header", snapGood[:3]},
			{"L below the minimum", "out of range", header(7)},
			{"L above the maximum", "out of range", header(MaxSnapshotSize + 1)},
			{"incomplete payload", "incomplete payload", snapGood[:len(snapGood)-1]},
			{"checksum mismatch", "checksum mismatch", flip(snapGood, len(snapGood)-1)},
		} {
			if _, n, err := parseSnapshotRecord(tc.rec); !errors.Is(err, ErrCorrupt) ||
				!strings.Contains(err.Error(), tc.why) || n != 0 {
				t.Fatalf("%s: n = %d, err = %v; want ErrCorrupt mentioning %q", tc.name, n, err, tc.why)
			}
		}
	})
}
