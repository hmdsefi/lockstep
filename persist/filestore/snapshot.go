// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/hmdsefi/lockstep/persist"
)

// snapDir is the directory of the snapshots of id.
func snapDir(id string) string { return snapshotsDir + "/" + idDirName(id) }

// Save stores snap durably as one file named after snap.Seq. A snapshot with the same Seq is
// replaced. Stale temporary files of earlier failed saves are removed. A failed Save removes its
// temporary file. It returns an error wrapping fs.ErrInvalid if snap.Seq is 0 and one
// wrapping ErrTooLarge if the snapshot does not fit in one record.
func (s *Store) Save(ctx context.Context, id string, snap persist.Snapshot) error {
	st, unlock, err := s.begin(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	if err := s.save(st, id, snap); err != nil {
		return fmt.Errorf("filestore: save snapshot %s seq %d: %w", id, snap.Seq, err)
	}
	return nil
}

// save runs PER-400. The ID's directory entry in the snapshots directory is durable only after a
// SyncDir of snapshots. This Store may not have created the directory (an earlier process or a
// failed Save may have), so it syncs snapshots in the first successful Save of the ID, not only
// when it saw the directory missing; the ID's state remembers that the sync succeeded.
func (s *Store) save(st *idState, id string, snap persist.Snapshot) (err error) {
	if snap.Seq < 1 {
		return fmt.Errorf("seq must be at least 1: %w", fs.ErrInvalid)
	}
	rec, err := encodeSnapshotRecord(snap)
	if err != nil {
		return err
	}
	dir := snapDir(id)
	infos, err := s.fs.ReadDir(dir)
	switch {
	case err == nil:
	case errors.Is(err, fs.ErrNotExist):
		infos = nil
	default:
		return err
	}
	if err := s.fs.MkdirAll(dir); err != nil {
		return err
	}
	for _, in := range infos {
		if in.IsDir || !strings.HasSuffix(in.Name, tmpExt) {
			continue
		}
		if err := s.fs.Remove(dir + "/" + in.Name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	tmp := dir + "/" + seqName(snap.Seq, tmpExt)
	defer func() {
		if err != nil {
			_ = s.fs.Remove(tmp) // the cause is the error to report
		}
	}()
	if err := s.createSynced(tmp, rec); err != nil {
		return err
	}
	if err := s.fs.Rename(tmp, dir+"/"+seqName(snap.Seq, snapExt)); err != nil {
		return err
	}
	if err := s.fs.SyncDir(dir); err != nil {
		return err
	}
	if !st.snapDirSynced {
		if err := s.fs.SyncDir(snapshotsDir); err != nil {
			return err
		}
		st.snapDirSynced = true
	}
	return nil
}

// Load returns the newest intact snapshot of id. A file is intact if it holds exactly one valid
// snapshot record whose Seq is the sequence number in its name; damaged and misnamed files are
// skipped. It returns ok == false if there is no intact snapshot.
func (s *Store) Load(ctx context.Context, id string) (snap persist.Snapshot, ok bool, err error) {
	_, unlock, err := s.begin(ctx, id)
	if err != nil {
		return persist.Snapshot{}, false, err
	}
	defer unlock()
	snap, ok, err = s.load(id)
	if err != nil {
		return persist.Snapshot{}, false, fmt.Errorf("filestore: load snapshot %s: %w", id, err)
	}
	return snap, ok, nil
}

// load runs PER-410: it tries the .snap entries from the highest sequence number down.
func (s *Store) load(id string) (persist.Snapshot, bool, error) {
	dir := snapDir(id)
	infos, err := s.fs.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return persist.Snapshot{}, false, nil
	}
	if err != nil {
		return persist.Snapshot{}, false, err
	}
	for i := len(infos) - 1; i >= 0; i-- {
		seq, ok := parseSeqName(infos[i].Name, snapExt)
		if !ok || infos[i].IsDir {
			continue
		}
		snap, intact, err := s.readSnapshot(dir+"/"+infos[i].Name, seq)
		if err != nil {
			return persist.Snapshot{}, false, err
		}
		if intact {
			return snap, true, nil
		}
	}
	return persist.Snapshot{}, false, nil
}

// readSnapshot reads the snapshot file name, whose name holds seq. intact is false, with a nil
// error, if the file is not exactly one valid snapshot record of that seq. The file is sized
// against the length in its header before the rest is read, so a damaged length cannot cause a
// large allocation.
func (s *Store) readSnapshot(name string, seq uint64) (snap persist.Snapshot, intact bool, err error) {
	f, err := s.fs.Open(name)
	if err != nil {
		return persist.Snapshot{}, false, err
	}
	defer closeFile(f, &err)
	size, err := f.Size()
	if err != nil {
		return persist.Snapshot{}, false, err
	}
	if size < headerSize+snapshotFixed || size > headerSize+MaxSnapshotSize {
		return persist.Snapshot{}, false, nil
	}
	var header [headerSize]byte
	if err := readFull(f, header[:]); err != nil {
		return persist.Snapshot{}, false, err
	}
	if l, err := headerLen(header[:], snapshotFixed, MaxSnapshotSize); err != nil || int64(l)+headerSize != size {
		return persist.Snapshot{}, false, nil
	}
	buf := make([]byte, size)
	if err := readFull(f, buf); err != nil {
		return persist.Snapshot{}, false, err
	}
	snap, n, err := parseSnapshotRecord(buf)
	if err != nil || n != len(buf) || snap.Seq != seq {
		return persist.Snapshot{}, false, nil //nolint:nilerr // a damaged snapshot is skipped, not an error
	}
	return snap, true, nil
}

// readFull fills buf from the start of f. A file shorter than buf is an error.
func readFull(f File, buf []byte) error {
	n, err := f.ReadAt(buf, 0)
	if n == len(buf) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// DeleteSnapshots removes the snapshots of id with a sequence number of at most to, oldest
// first, and syncs the ID's directory if it removed any.
func (s *Store) DeleteSnapshots(ctx context.Context, id string, to uint64) error {
	_, unlock, err := s.begin(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	if err := s.deleteSnapshots(id, to); err != nil {
		return fmt.Errorf("filestore: delete snapshots of %s up to %d: %w", id, to, err)
	}
	return nil
}

// deleteSnapshots runs PER-420. It stops at the first failed removal, and syncs the directory if
// it removed anything before.
func (s *Store) deleteSnapshots(id string, to uint64) error {
	dir := snapDir(id)
	infos, err := s.fs.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	removed := false
	var rerr error
	for _, in := range infos {
		seq, ok := parseSeqName(in.Name, snapExt)
		if !ok || in.IsDir {
			continue
		}
		if seq > to {
			break
		}
		if rerr = s.fs.Remove(dir + "/" + in.Name); rerr != nil {
			break
		}
		removed = true
	}
	if removed {
		rerr = errors.Join(rerr, s.fs.SyncDir(dir))
	}
	return rerr
}
