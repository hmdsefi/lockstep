// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"

	"github.com/hmdsefi/lockstep/persist"
)

// idState is what a Store keeps in memory for one persistence ID. Its fields are used only
// while the ID's lock (lockID) is held.
type idState struct {
	// unusable is set when a repair after a failed write or commit failed (PER-360). It is never
	// cleared: every later call for the ID returns it.
	unusable error
	// journal is the loaded journal state; nil until the scan of PER-370 has run, and again
	// after a scan failed or Recover dropped it.
	journal *journal
	// snapDirSynced is set once a SyncDir of the snapshots directory has succeeded for this ID.
	// Until then, whether the ID's snapshot directory entry is durable is unknown (an earlier
	// process may have created it), so every Save syncs the snapshots directory.
	snapDirSynced bool
}

// journal is the in-memory state of one ID's journal (PER §5.10). "Since the last commit" means
// since the last successful Commit, or since the scan that loaded the state.
type journal struct {
	dir        string // journal/id-<enc>
	dirExists  bool
	dirCreated bool // the ID's directory was created since the last commit
	// entriesChanged: a segment was created or removed since the last commit, so the ID's
	// directory needs a SyncDir.
	entriesChanged bool
	segs           []*segment // ascending by name
	lastWritten    uint64
	lastCommitted  uint64
}

// segment is one segment file of a journal.
type segment struct {
	seq       uint64 // the file's name: the sequence number of its first record
	size      int64
	committed int64 // size at the last commit
	dirty     bool  // written or truncated since its last Sync
	created   bool  // created since the last commit
}

func (j *journal) segPath(seq uint64) string { return j.dir + "/" + seqName(seq, segExt) }

// state returns the state of id, creating it on first use. The caller holds id's lock. A state
// lives as long as the Store: its journal part could be dropped and rebuilt by a scan, but its
// unusable error must persist for the Store's lifetime (PER-360).
func (s *Store) state(id string) *idState {
	s.statesMu.Lock()
	defer s.statesMu.Unlock()
	if s.states == nil {
		s.states = make(map[string]*idState)
	}
	st := s.states[id]
	if st == nil {
		st = &idState{}
		s.states[id] = st
	}
	return st
}

// begin runs the checks every method starts with, in order: a done ctx returns ctx.Err(), an
// invalid id returns an error wrapping persist.ErrInvalidID, the store is initialized, id's
// lock is taken, and an unusable id returns its error. On success the caller holds the lock
// and must call unlock.
func (s *Store) begin(ctx context.Context, id string) (st *idState, unlock func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := checkID(id); err != nil {
		return nil, nil, err
	}
	if err := s.ensureInit(); err != nil {
		return nil, nil, err
	}
	unlock = s.lockID(id)
	st = s.state(id)
	if st.unusable != nil {
		unlock()
		return nil, nil, st.unusable
	}
	return st, unlock, nil
}

// loadJournal loads the journal state of id with the scan of PER-370 if it is not loaded. Like
// Recover of an ID that is not loaded, it makes the records it finds durable.
func (s *Store) loadJournal(id string, st *idState) error {
	if st.journal != nil {
		return nil
	}
	j, _, err := s.scan(id, "load", true)
	if err != nil {
		return err
	}
	st.journal = j
	return nil
}

// Recover commits the events of id written since the last commit, drops the in-memory state of
// id and rebuilds it from the files. Every event it reports is durable when it returns. A torn
// last record is truncated and its size reported in Bounds.Repaired; damaged data that is not a
// torn tail returns an error wrapping ErrCorrupt.
func (s *Store) Recover(ctx context.Context, id string) (persist.Bounds, error) {
	st, unlock, err := s.begin(ctx, id)
	if err != nil {
		return persist.Bounds{}, err
	}
	defer unlock()
	// A loaded ID is committed first, so its files hold only synced data. An ID that is not
	// loaded may hold records an earlier process wrote and never synced; the scan syncs them.
	loaded := st.journal != nil
	if loaded {
		if err := s.commit(id, st); err != nil {
			return persist.Bounds{}, err
		}
		st.journal = nil
	}
	j, b, err := s.scan(id, "recover", !loaded)
	if err != nil {
		return persist.Bounds{}, err
	}
	st.journal = j
	return b, nil
}

// Write appends events to the journal of id as one record. The events are durable once Commit
// returns nil.
func (s *Store) Write(ctx context.Context, id string, events []persist.Event) error {
	st, unlock, err := s.begin(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	if len(events) == 0 {
		return fmt.Errorf("filestore: write %s: empty batch: %w", id, fs.ErrInvalid)
	}
	if err := s.loadJournal(id, st); err != nil {
		return err
	}
	j := st.journal
	for i, e := range events {
		if want := j.lastWritten + 1 + uint64(i); e.Seq != want { //nolint:gosec // i >= 0
			return fmt.Errorf("filestore: write %s: seq %d at index %d, expected %d: %w",
				id, e.Seq, i, want, persist.ErrSeqConflict)
		}
	}
	first, last := events[0].Seq, events[len(events)-1].Seq
	rec, err := encodeJournalRecord(events)
	if err == nil {
		err = s.appendRecord(id, st, first, rec)
	}
	if err != nil {
		return fmt.Errorf("filestore: write %s seq %d..%d: %w", id, first, last, err)
	}
	j.lastWritten = last
	return nil
}

// appendRecord writes rec, whose first sequence number is first, at the end of the last
// segment, or of a new segment if there is none or the last one is full (PER-340). If the write
// fails after the segment exists, the segment is truncated back to its size before the call
// (PER-360); if that fails too, the ID becomes unusable.
func (s *Store) appendRecord(id string, st *idState, first uint64, rec []byte) error {
	j := st.journal
	var seg *segment
	var f File
	if n := len(j.segs); n > 0 && j.segs[n-1].size < s.set.segmentSize {
		seg = j.segs[n-1]
		var err error
		if f, err = s.fs.Open(j.segPath(seg.seq)); err != nil {
			return err // nothing was written
		}
	} else {
		if !j.dirExists {
			if err := s.fs.MkdirAll(j.dir); err != nil {
				return err
			}
			j.dirExists, j.dirCreated = true, true
		}
		var err error
		if f, err = s.fs.Create(j.segPath(first)); err != nil {
			return err
		}
		seg = &segment{seq: first, created: true}
		j.segs = append(j.segs, seg)
		j.entriesChanged = true
	}

	before := seg.size
	n, err := f.WriteAt(rec, before)
	if err == nil && n < len(rec) {
		err = io.ErrShortWrite
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	seg.dirty = true
	if err != nil {
		if terr := s.truncateFile(j.segPath(seg.seq), before); terr != nil {
			st.unusable = fmt.Errorf("filestore: %s: %w: %w", id, ErrUnusable, terr)
		}
		return err
	}
	seg.size = before + int64(len(rec))
	return nil
}

// Commit makes every event of id written before the call durable: it syncs the segments
// written since their last sync, then the directories whose entries changed.
func (s *Store) Commit(ctx context.Context, id string) error {
	st, unlock, err := s.begin(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	if err := s.loadJournal(id, st); err != nil {
		return err
	}
	return s.commit(id, st)
}

// commit runs PER-350 and, if it fails, the repair of PER-360.
func (s *Store) commit(id string, st *idState) error {
	if err := s.syncJournal(st.journal); err != nil {
		s.repairCommit(id, st)
		return fmt.Errorf("filestore: commit %s: %w", id, err)
	}
	return nil
}

// syncJournal syncs, in order, every segment written or truncated since its last sync
// (ascending), the ID's directory if a segment was created or removed since the last commit,
// and the journal directory if the ID's directory was created since the last commit. It
// performs no I/O if there is nothing to sync. On success the current sizes and the last
// written sequence number become the committed ones.
func (s *Store) syncJournal(j *journal) error {
	for _, seg := range j.segs {
		if !seg.dirty {
			continue
		}
		if err := s.syncFile(j.segPath(seg.seq)); err != nil {
			return err
		}
		seg.dirty = false
	}
	if j.entriesChanged {
		if err := s.fs.SyncDir(j.dir); err != nil {
			return err
		}
	}
	if j.dirCreated {
		if err := s.fs.SyncDir(journalDir); err != nil {
			return err
		}
	}
	// The created flags clear only here, on success, so a failed commit cannot let a later one
	// skip a directory sync.
	for _, seg := range j.segs {
		seg.committed = seg.size
		seg.created = false
	}
	j.entriesChanged, j.dirCreated = false, false
	j.lastCommitted = j.lastWritten
	return nil
}

// repairCommit undoes everything written since the last commit after a failed commit: it
// truncates every segment written since then to its committed size and resets the last written
// sequence number to the last committed one (PER-360). A segment created since the last commit
// whose name is above the next sequence number holds no committed event and is removed: left
// behind empty as the last segment, the scan would report its name minus one as committed. The
// repair is not synced; the next commit or scan syncs it. If the repair fails, the ID becomes
// unusable.
//
// Limit (PER-360): because the repair is not synced, a crash before the next commit can make
// some of its truncations and removals durable and not others. With one Write per Commit, as
// persist does, everything written since the last commit is in the last segment, and any mix
// recovers either the batch or none of it. With several Writes over more than one segment
// between commits, an earlier segment's truncation can reach the disk while a later segment's
// removal does not: the scan, which reads only the first record of the first segment and the
// records of the last one, then reports events behind a gap that Read rejects as corrupt. The
// same holds for an empty file that a failed Create may leave named after the last written
// sequence number plus one.
func (s *Store) repairCommit(id string, st *idState) {
	j := st.journal
	next := j.lastCommitted + 1
	kept := j.segs[:0]
	var err error
	for _, seg := range j.segs {
		switch {
		case err != nil:
		case seg.created && seg.seq > next:
			if err = s.fs.Remove(j.segPath(seg.seq)); err == nil {
				j.entriesChanged = true
				continue
			}
		case seg.size != seg.committed:
			if err = s.truncateFile(j.segPath(seg.seq), seg.committed); err == nil {
				seg.size = seg.committed
				seg.dirty = true
			}
		}
		kept = append(kept, seg)
	}
	clear(j.segs[len(kept):])
	j.segs = kept
	j.lastWritten = j.lastCommitted
	if err != nil {
		st.unusable = fmt.Errorf("filestore: %s: %w: %w", id, ErrUnusable, err)
	}
}

// syncFile opens name, syncs it and closes it.
func (s *Store) syncFile(name string) (err error) {
	f, err := s.fs.Open(name)
	if err != nil {
		return err
	}
	defer closeFile(f, &err)
	return f.Sync()
}

// truncateFile opens name, truncates it to size and closes it.
func (s *Store) truncateFile(name string, size int64) (err error) {
	f, err := s.fs.Open(name)
	if err != nil {
		return err
	}
	defer closeFile(f, &err)
	return f.Truncate(size)
}

// closeFile closes f and stores the Close error in *err unless *err already holds one.
func closeFile(f File, err *error) {
	if cerr := f.Close(); *err == nil {
		*err = cerr
	}
}

// scan reads the journal files of id and returns its state and bounds (PER-370). It checks the
// first record of the first segment and every record of the last one. A torn tail of the last
// segment is truncated and synced; damage that is not a torn tail returns an error wrapping
// ErrCorrupt.
//
// With durable set, as for an ID whose state was not loaded, it also makes the records it reports
// durable: if the last segment holds a record, it syncs the segment, the ID's directory and the
// journal directory, since an earlier process may have written them without a commit. Earlier
// segments are not synced: a segment is followed by a new one only after a Write, and with one
// Write per Commit the Commit of the earlier batch synced it.
//
// Whether the directory entries of an existing ID directory are durable is unknown to a scan.
// If the directory holds no segment, or its last segment is empty after the scan, the next
// Write may fill that segment or create the first one without creating a directory; so the
// returned state asks the first successful Commit to sync the ID's directory and the journal
// directory, as if both were created since the last commit. An I/O error is reported as
// "filestore: <op> <id>: …".
func (s *Store) scan(id, op string, durable bool) (*journal, persist.Bounds, error) {
	j, b, err := s.scanDir(journalDir+"/"+idDirName(id), durable)
	if err != nil && !errors.Is(err, ErrCorrupt) {
		err = fmt.Errorf("filestore: %s %s: %w", op, id, err)
	}
	return j, b, err
}

func (s *Store) scanDir(dir string, durable bool) (*journal, persist.Bounds, error) {
	j := &journal{dir: dir}
	infos, err := s.fs.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return j, persist.Bounds{First: 1}, nil
	}
	if err != nil {
		return nil, persist.Bounds{}, err
	}
	j.dirExists = true
	for _, in := range infos {
		if in.IsDir {
			continue
		}
		if seq, ok := parseSeqName(in.Name, segExt); ok && seq > 0 {
			j.segs = append(j.segs, &segment{seq: seq, size: in.Size, committed: in.Size})
		}
	}
	if len(j.segs) == 0 {
		j.entriesChanged, j.dirCreated = true, true
		return j, persist.Bounds{First: 1}, nil
	}
	first, last := j.segs[0], j.segs[len(j.segs)-1]
	if first != last {
		if err := s.checkFirstRecord(j.segPath(first.seq), first.seq); err != nil {
			return nil, persist.Bounds{}, err
		}
	}
	next, repaired, err := s.scanLast(j.segPath(last.seq), last, durable)
	if err != nil {
		return nil, persist.Bounds{}, err
	}
	if last.size == 0 {
		j.entriesChanged, j.dirCreated = true, true
	} else if durable {
		if err := s.fs.SyncDir(dir); err != nil {
			return nil, persist.Bounds{}, err
		}
		if err := s.fs.SyncDir(journalDir); err != nil {
			return nil, persist.Bounds{}, err
		}
	}
	j.lastWritten, j.lastCommitted = next-1, next-1
	return j, persist.Bounds{First: first.seq, Last: next - 1, Repaired: repaired}, nil
}

// checkFirstRecord checks that the segment name starts with a valid record whose first
// sequence number is seq.
func (s *Store) checkFirstRecord(name string, seq uint64) (err error) {
	f, err := s.fs.Open(name)
	if err != nil {
		return err
	}
	defer closeFile(f, &err)
	size, err := f.Size()
	if err != nil {
		return err
	}
	r := &segReader{f: f, size: size}
	rec, err := r.record(0)
	if err != nil {
		return err
	}
	if rec.invalid != "" {
		return corruptAt(name, 0, rec.invalid)
	}
	if got := rec.events[0].Seq; got != seq {
		return corruptAt(name, 0, fmt.Sprintf("seq %d, expected %d", got, seq))
	}
	return nil
}

// scanLast reads the records of the last segment seg, at name, from offset 0 and returns the
// sequence number after the last valid one. At the first invalid record, at offset o, it decides
// between corruption and a torn tail: corruption if the record's length is in range, the record
// ends before the end of the file and a valid record starts there; otherwise it truncates the
// segment to o, syncs it, sets seg's sizes and returns the number of bytes removed. With durable
// set, it also syncs a segment that holds a record after the scan.
func (s *Store) scanLast(name string, seg *segment, durable bool) (next uint64, repaired int64, err error) {
	f, err := s.fs.Open(name)
	if err != nil {
		return 0, 0, err
	}
	defer closeFile(f, &err)
	size, err := f.Size()
	if err != nil {
		return 0, 0, err
	}
	r := &segReader{f: f, size: size}
	next = seg.seq
	off := int64(0)
	for off < size {
		rec, err := r.record(off)
		if err != nil {
			return 0, 0, err
		}
		if rec.invalid != "" {
			if rec.size > 0 && off+rec.size < size {
				after, err := r.record(off + rec.size)
				if err != nil {
					return 0, 0, err
				}
				if after.invalid == "" {
					return 0, 0, corruptAt(name, off, rec.invalid)
				}
			}
			break // a torn tail
		}
		if got := rec.events[0].Seq; got != next {
			return 0, 0, corruptAt(name, off, fmt.Sprintf("seq %d, expected %d", got, next))
		}
		next += uint64(len(rec.events))
		off += rec.size
	}
	torn := off < size
	if torn {
		if err := f.Truncate(off); err != nil {
			return 0, 0, err
		}
	}
	if torn || (durable && off > 0) {
		if err := f.Sync(); err != nil {
			return 0, 0, err
		}
	}
	seg.size, seg.committed = off, off
	return next, size - off, nil
}

// corruptAt returns the corruption error of PER-370.
func corruptAt(name string, off int64, reason string) error {
	return fmt.Errorf("filestore: corrupt %s at offset %d: %s: %w", name, off, reason, ErrCorrupt)
}

// readWindow is how many bytes a segReader reads at least per ReadAt call.
const readWindow = 64 << 10

// segReader reads the records of a segment file of the given size through a buffer, so a scan
// of many small records needs few ReadAt calls.
type segReader struct {
	f    File
	size int64
	buf  []byte
	off  int64 // file offset of buf[0]
}

// at returns the n bytes at off; off+n must not exceed the size. The result is valid until the
// next call.
func (r *segReader) at(off int64, n int) ([]byte, error) {
	if off >= r.off && off+int64(n) <= r.off+int64(len(r.buf)) {
		return r.buf[off-r.off : off-r.off+int64(n)], nil
	}
	want := int(min(max(int64(n), readWindow), r.size-off))
	if cap(r.buf) < want {
		r.buf = make([]byte, want)
	}
	r.buf, r.off = r.buf[:want], off
	got, err := r.f.ReadAt(r.buf, off)
	if got < want {
		r.buf = r.buf[:0]
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF // the file is shorter than its size
		}
		return nil, err
	}
	return r.buf[:n], nil
}

// scannedRecord is the result of reading the record at one offset.
type scannedRecord struct {
	events  []persist.Event // Data points into the reader's buffer
	size    int64           // header plus payload; 0 if the header is incomplete or its length out of range
	invalid string          // "" for a valid record; else the reason
}

// record reads the record at off and validates it (PER-310).
func (r *segReader) record(off int64) (scannedRecord, error) {
	header, err := r.at(off, int(min(r.size-off, headerSize)))
	if err != nil {
		return scannedRecord{}, err
	}
	l, err := headerLen(header, minJournalBytes, MaxRecordSize)
	if err != nil {
		return scannedRecord{invalid: reasonOf(err)}, nil
	}
	size := headerSize + int64(l)
	if off+size > r.size {
		return scannedRecord{size: size, invalid: reasonIncomplete}, nil
	}
	b, err := r.at(off, int(size))
	if err != nil {
		return scannedRecord{}, err
	}
	events, _, err := parseJournalRecord(b)
	if err != nil {
		return scannedRecord{size: size, invalid: reasonOf(err)}, nil
	}
	return scannedRecord{events: events, size: size}, nil
}

// Read returns up to max committed events of id with Seq >= from, in order. It validates every
// record it reads: damaged data, a gap between segments or bytes after the last record of a
// segment return an error wrapping ErrCorrupt. A from below the first retained sequence number
// returns an error wrapping persist.ErrMissingEvents, a from above the last committed one returns
// no events, and max < 1 returns an error wrapping fs.ErrInvalid.
func (s *Store) Read(ctx context.Context, id string, from uint64, max int) ([]persist.Event, error) {
	st, unlock, err := s.begin(ctx, id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if max < 1 {
		return nil, fmt.Errorf("filestore: read %s: max %d is less than 1: %w", id, max, fs.ErrInvalid)
	}
	if err := s.loadJournal(id, st); err != nil {
		return nil, err
	}
	j := st.journal
	first := uint64(1)
	if len(j.segs) > 0 {
		first = j.segs[0].seq
	}
	if from < first {
		return nil, fmt.Errorf("filestore: read %s: from %d is below the first retained seq %d: %w",
			id, from, first, persist.ErrMissingEvents)
	}
	if from > j.lastCommitted {
		return nil, nil
	}
	// The segment with the greatest name <= from exists: from >= first.
	start := sort.Search(len(j.segs), func(i int) bool { return j.segs[i].seq > from }) - 1
	events, err := s.readSegments(j, start, from, max)
	if err != nil && !errors.Is(err, ErrCorrupt) {
		err = fmt.Errorf("filestore: read %s: %w", id, err)
	}
	return events, err
}

// readSegments returns up to max committed events with Seq >= from, reading the segments of j
// from index start on.
func (s *Store) readSegments(j *journal, start int, from uint64, max int) ([]persist.Event, error) {
	var out []persist.Event
	next := j.segs[start].seq // the sequence number the next record must start with
	for i := start; i < len(j.segs); i++ {
		seg := j.segs[i]
		if seg.seq != next {
			return nil, corruptAt(j.segPath(seg.seq), 0, fmt.Sprintf("seq %d, expected %d", seg.seq, next))
		}
		var err error
		out, next, err = s.readSegment(j, i, next, from, max, out)
		if err != nil {
			return nil, err
		}
		if len(out) == max || next > j.lastCommitted {
			return out, nil
		}
	}
	return out, nil
}

// readSegment appends to out the events of segment i that are at least from, up to max events in
// all and up to the last committed one, and returns the sequence number after the last record it
// read. Segment i starts at next. For a segment that is not the last it reads the records up to
// the next segment's name and then requires the file to end; for the last it reads up to the
// committed size. It stops early once max events are collected or the last committed event is
// read.
func (s *Store) readSegment(j *journal, i int, next, from uint64, max int, out []persist.Event) (_ []persist.Event, _ uint64, err error) {
	seg := j.segs[i]
	name := j.segPath(seg.seq)
	f, err := s.fs.Open(name)
	if err != nil {
		return nil, 0, err
	}
	defer closeFile(f, &err)
	size, err := f.Size()
	if err != nil {
		return nil, 0, err
	}
	last := i == len(j.segs)-1
	limit, stop := size, j.segs[min(i+1, len(j.segs)-1)].seq
	if last {
		if size < seg.committed { // the file lost committed bytes behind the Store
			return nil, 0, corruptAt(name, size, reasonIncomplete)
		}
		limit = seg.committed
	}
	r := &segReader{f: f, size: limit}
	off := int64(0)
	for off < limit && (last || next < stop) {
		rec, err := r.record(off)
		if err != nil {
			return nil, 0, err
		}
		if rec.invalid != "" {
			return nil, 0, corruptAt(name, off, rec.invalid)
		}
		if got := rec.events[0].Seq; got != next {
			return nil, 0, corruptAt(name, off, fmt.Sprintf("seq %d, expected %d", got, next))
		}
		for _, e := range rec.events {
			if e.Seq >= from && len(out) < max {
				out = append(out, persist.Event{Seq: e.Seq, Data: bytes.Clone(e.Data)})
			}
		}
		next += uint64(len(rec.events))
		off += rec.size
		if len(out) == max || next > j.lastCommitted {
			return out, next, nil
		}
	}
	if !last && off < limit {
		return nil, 0, corruptAt(name, off, "trailing bytes")
	}
	return out, next, nil
}

// DeleteEvents removes the segments of id whose events are all at most to, except the last
// segment, which always stays. It never changes the bounds that Recover reports except First,
// which becomes the first sequence number of the first remaining segment.
func (s *Store) DeleteEvents(ctx context.Context, id string, to uint64) error {
	st, unlock, err := s.begin(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	if err := s.loadJournal(id, st); err != nil {
		return err
	}
	j := st.journal
	// Segment i holds only events below its successor's name, so it goes when that name is at
	// most to+1. It stays while its successor holds no committed data: after a crash, the
	// successor may be gone and the committed events with it.
	removed := 0
	var rerr error
	for removed+1 < len(j.segs) && j.segs[removed+1].seq-1 <= to && j.segs[removed+1].committed > 0 {
		if rerr = s.fs.Remove(j.segPath(j.segs[removed].seq)); rerr != nil {
			break
		}
		removed++
	}
	if removed > 0 {
		clear(j.segs[:removed])
		j.segs = j.segs[removed:]
		if err := s.fs.SyncDir(j.dir); err != nil {
			// The removals are not durable yet: the next commit syncs the directory again.
			j.entriesChanged = true
			rerr = errors.Join(rerr, err)
		}
	}
	if rerr != nil {
		return fmt.Errorf("filestore: delete events of %s up to %d: %w", id, to, rerr)
	}
	return nil
}
