// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/hmdsefi/lockstep/persist"
)

const (
	headerSize = 8 // uint32 payload length, uint32 CRC-32C of the payload

	journalFixed    = 12               // uint64 firstSeq, uint32 count
	minJournalBytes = journalFixed + 4 // at least one entry: its length prefix
	snapshotFixed   = 8                // uint64 seq
	entryLenSize    = 4                // uint32 length before every event
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// encodeJournalRecord returns the record of one batch: the header, then firstSeq (the Seq of
// events[0]), the count and the length-prefixed event data. The sequence numbers of the later
// events are implied. It returns an error wrapping ErrTooLarge if the payload would exceed
// MaxRecordSize; the caller adds the package prefix. It panics for an empty batch, which Write
// refuses before it encodes.
func encodeJournalRecord(events []persist.Event) ([]byte, error) {
	if len(events) == 0 {
		panic("filestore: encodeJournalRecord: empty batch")
	}
	size := int64(journalFixed)
	for _, e := range events {
		size += entryLenSize + int64(len(e.Data))
		if size > MaxRecordSize {
			return nil, fmt.Errorf("batch of %d events exceeds %d bytes: %w",
				len(events), MaxRecordSize, ErrTooLarge)
		}
	}
	rec := make([]byte, headerSize, headerSize+int(size))
	rec = binary.LittleEndian.AppendUint64(rec, events[0].Seq)
	rec = binary.LittleEndian.AppendUint32(rec, uint32(len(events))) //nolint:gosec // len(events) <= size/4 <= MaxRecordSize
	for _, e := range events {
		rec = binary.LittleEndian.AppendUint32(rec, uint32(len(e.Data))) //nolint:gosec // len(e.Data) < MaxRecordSize
		rec = append(rec, e.Data...)
	}
	return seal(rec), nil
}

// encodeSnapshotRecord returns the record of a snapshot: the header, then seq and the data. It
// returns an error wrapping ErrTooLarge if the payload would exceed MaxSnapshotSize.
func encodeSnapshotRecord(snap persist.Snapshot) ([]byte, error) {
	if int64(len(snap.Data))+snapshotFixed > MaxSnapshotSize {
		return nil, fmt.Errorf("filestore: snapshot of %d bytes exceeds %d bytes: %w",
			len(snap.Data), MaxSnapshotSize-snapshotFixed, ErrTooLarge)
	}
	rec := make([]byte, headerSize, headerSize+snapshotFixed+len(snap.Data))
	rec = binary.LittleEndian.AppendUint64(rec, snap.Seq)
	rec = append(rec, snap.Data...)
	return seal(rec), nil
}

// seal fills the header of rec, whose first headerSize bytes are reserved, from the payload
// behind them.
func seal(rec []byte) []byte {
	payload := rec[headerSize:]
	binary.LittleEndian.PutUint32(rec[0:], uint32(len(payload))) //nolint:gosec // callers bound the payload by MaxRecordSize or MaxSnapshotSize
	binary.LittleEndian.PutUint32(rec[4:], crc32.Checksum(payload, castagnoli))
	return rec
}

// Reasons of an invalid record, as recovery reports them (PER-370). A length outside its range
// is reported as "bad length <L>".
const (
	reasonIncomplete = "incomplete record"
	reasonChecksum   = "checksum mismatch"
	reasonMalformed  = "malformed payload"
)

// invalidRecord is the error of a record that is not valid. It wraps ErrCorrupt. reason is the
// short cause a scan reports; detail describes the failure.
type invalidRecord struct{ reason, detail string }

func (e *invalidRecord) Error() string { return ErrCorrupt.Error() + ": " + e.detail }
func (e *invalidRecord) Unwrap() error { return ErrCorrupt }

func invalid(reason, format string, args ...any) error {
	return &invalidRecord{reason: reason, detail: fmt.Sprintf(format, args...)}
}

// reasonOf returns the reason of an error from the record parsers.
func reasonOf(err error) string {
	var inv *invalidRecord
	if errors.As(err, &inv) {
		return inv.reason
	}
	return err.Error()
}

// headerLen returns the payload length that the header at the start of b declares. The header
// must be complete and the length in [minLen, maxLen]; a failure wraps ErrCorrupt.
func headerLen(b []byte, minLen, maxLen uint32) (uint32, error) {
	if len(b) < headerSize {
		return 0, invalid(reasonIncomplete, "incomplete header (%d of %d bytes)", len(b), headerSize)
	}
	l := binary.LittleEndian.Uint32(b[0:])
	if l < minLen || l > maxLen {
		return 0, invalid(fmt.Sprintf("bad length %d", l), "record length %d out of range [%d, %d]", l, minLen, maxLen)
	}
	return l, nil
}

// payloadOf returns the payload of the record at the start of b and the record's size. It
// applies the checks every record shares, in order: the header is complete, the length is in
// [minLen, maxLen], the payload is complete, the checksum matches. A failure wraps ErrCorrupt.
func payloadOf(b []byte, minLen, maxLen uint32) (payload []byte, size int, err error) {
	l, err := headerLen(b, minLen, maxLen)
	if err != nil {
		return nil, 0, err
	}
	if len(b)-headerSize < int(l) { // l is at most maxLen, so it fits an int
		return nil, 0, invalid(reasonIncomplete, "incomplete payload (%d of %d bytes)", len(b)-headerSize, l)
	}
	payload = b[headerSize : headerSize+int(l)]
	if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(b[4:]) {
		return nil, 0, invalid(reasonChecksum, "checksum mismatch")
	}
	return payload, headerSize + int(l), nil
}

// parseJournalRecord parses the journal record at the start of b and returns its events, with
// Seq values firstSeq, firstSeq+1 and so on, and the record's size in bytes. Bytes after the
// record are ignored. The Data of the events point into b, with their capacity limited so an
// append cannot reach a neighbour. A record that is not valid (incomplete, out of range, bad
// checksum, or not parsing exactly) returns an error wrapping ErrCorrupt.
func parseJournalRecord(b []byte) ([]persist.Event, int, error) {
	payload, size, err := payloadOf(b, minJournalBytes, MaxRecordSize)
	if err != nil {
		return nil, 0, err
	}
	first := binary.LittleEndian.Uint64(payload[0:])
	count := binary.LittleEndian.Uint32(payload[8:])
	body := payload[journalFixed:]
	if count == 0 {
		return nil, 0, invalid(reasonMalformed, "batch count 0")
	}
	if uint64(count)*entryLenSize > uint64(len(body)) {
		return nil, 0, invalid(reasonMalformed, "%d entries do not fit in %d bytes (entry lengths)", count, len(body))
	}
	events := make([]persist.Event, 0, count)
	for i := range count {
		if len(body) < entryLenSize {
			return nil, 0, invalid(reasonMalformed, "entry %d is cut off (entry lengths)", i)
		}
		n := binary.LittleEndian.Uint32(body)
		body = body[entryLenSize:]
		if uint64(n) > uint64(len(body)) {
			return nil, 0, invalid(reasonMalformed, "entry %d of %d bytes exceeds the payload (entry lengths)", i, n)
		}
		events = append(events, persist.Event{Seq: first + uint64(i), Data: body[:n:n]})
		body = body[n:]
	}
	if len(body) != 0 {
		return nil, 0, invalid(reasonMalformed, "%d bytes after the last entry (entry lengths)", len(body))
	}
	return events, size, nil
}

// parseSnapshotRecord parses the snapshot record at the start of b and returns the snapshot, its
// Data pointing into b, and the record's size in bytes. An invalid record returns an error
// wrapping ErrCorrupt.
func parseSnapshotRecord(b []byte) (persist.Snapshot, int, error) {
	payload, size, err := payloadOf(b, snapshotFixed, MaxSnapshotSize)
	if err != nil {
		return persist.Snapshot{}, 0, err
	}
	return persist.Snapshot{
		Seq:  binary.LittleEndian.Uint64(payload),
		Data: payload[snapshotFixed:len(payload):len(payload)],
	}, size, nil
}
