// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

const (
	// DefaultSegmentSize is the segment size of a Store without WithSegmentSize: a Write starts a
	// new segment file once the last one has reached this size.
	DefaultSegmentSize int64 = 64 << 20
	// MaxRecordSize is the largest journal record payload in bytes. A batch of events that needs
	// more is rejected with ErrTooLarge.
	MaxRecordSize = 16 << 20
	// MaxSnapshotSize is the largest snapshot record payload in bytes. A snapshot that needs more
	// is rejected with ErrTooLarge.
	MaxSnapshotSize = 256 << 20

	maxSegmentSize = 1 << 30
)

var (
	// ErrCorrupt is returned when data in a file fails its checksum or does not parse.
	ErrCorrupt = errors.New("filestore: corrupt data")
	// ErrFormat is returned when the store directory has a FORMAT file this version does not
	// support.
	ErrFormat = errors.New("filestore: unsupported store format")
	// ErrUnusable is returned by every call for a persistence ID after the repair that followed a
	// failed write or commit of that ID failed too.
	ErrUnusable = errors.New("filestore: persistence id unusable after a failed repair")
	// ErrTooLarge is returned when a batch or a snapshot does not fit in one record.
	ErrTooLarge = errors.New("filestore: record too large")
)

const (
	formatName    = "FORMAT"
	formatContent = "lockstep-filestore 1\n"
	journalDir    = "journal"
	snapshotsDir  = "snapshots"
)

// Option configures a Store.
type Option func(*settings)

// settings is unexported; Option values are the only way to change it.
type settings struct{ segmentSize int64 }

// WithSegmentSize sets the segment size: a Write starts a new segment file once the last one has
// reached bytes. It panics if bytes is outside [1, 1<<30].
func WithSegmentSize(bytes int64) Option {
	if bytes < 1 || bytes > maxSegmentSize {
		panic(fmt.Sprintf("filestore: WithSegmentSize: %d out of range [1, %d]", bytes, maxSegmentSize))
	}
	return func(s *settings) { s.segmentSize = bytes }
}

// Store keeps the events and snapshots of many persistence IDs in files below one root
// directory. It is safe for concurrent use: calls for one ID run one after another, calls for
// different IDs may overlap.
//
// Use one Store per directory. Two Stores on the same directory, in one process or in two, are
// not detected and can overwrite each other's committed events.
type Store struct {
	fs  FS
	set settings

	initMu sync.Mutex
	ready  bool // initialization succeeded

	locksMu sync.Mutex
	locks   map[string]*idLock
}

// idLock is the lock of one persistence ID. refs counts the callers that hold it or wait for it.
type idLock struct {
	mu   sync.Mutex
	refs int
}

// Open returns a Store on directory dir of the operating system's file system, creating dir and
// any missing parent if needed. It syncs the parent of every directory it creates, so that dir
// survives a machine crash. It prepares the store before returning: it creates the journal and
// snapshots directories and the FORMAT file, or checks an existing FORMAT file.
//
// Use one Store per directory. Two Stores on the same directory, in one process or in two, are
// not detected and can overwrite each other's committed events.
func Open(dir string, opts ...Option) (*Store, error) {
	if err := makeRoot(dir); err != nil {
		return nil, err
	}
	s := New(&osFS{root: dir}, opts...)
	if err := s.ensureInit(); err != nil {
		return nil, err
	}
	return s, nil
}

// New returns a Store on fsys. New does no I/O: the store is prepared by its first operation,
// and by the next one if that fails.
//
// Use one Store per directory. Two Stores on the same directory, in one process or in two, are
// not detected and can overwrite each other's committed events.
func New(fsys FS, opts ...Option) *Store {
	s := &Store{fs: fsys, set: settings{segmentSize: DefaultSegmentSize}}
	for _, o := range opts {
		o(&s.set)
	}
	return s
}

// syncDirOS syncs a directory of the operating system's file system. Tests replace it to see
// which directories Open syncs.
var syncDirOS = syncDir

// makeRoot creates dir and every missing parent (PER-430). os.MkdirAll alone makes the new
// directories visible but not durable: an entry is durable only once its parent directory is
// synced. So makeRoot finds the directories that do not exist yet, creates them, and then syncs
// the parent of each one, deepest first. Without that, a machine crash after the first commit
// could lose dir, with every acknowledged event in it. A directory that already exists is not
// synced again.
func makeRoot(dir string) error {
	var created []string // the directories MkdirAll creates, deepest first
	for p := filepath.Clean(dir); ; {
		if _, err := os.Stat(p); !errors.Is(err, fs.ErrNotExist) {
			break // p exists, or MkdirAll reports why it cannot be created
		}
		created = append(created, p)
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // the store's directories are world-readable by design
		return fmt.Errorf("filestore: %w", err)
	}
	for _, d := range created {
		if err := syncDirOS(filepath.Dir(d)); err != nil {
			return fmt.Errorf("filestore: %w", err)
		}
	}
	return nil
}

// ensureInit prepares the store once: it creates the journal and snapshots directories, checks
// FORMAT or writes it, and syncs the root directory. After an error the next call tries again.
func (s *Store) ensureInit() error {
	s.initMu.Lock()
	defer s.initMu.Unlock()
	if s.ready {
		return nil
	}
	for _, d := range []string{journalDir, snapshotsDir} {
		if err := s.fs.MkdirAll(d); err != nil {
			return fmt.Errorf("filestore: %w", err)
		}
	}
	content, err := s.readFormat()
	switch {
	case err == nil:
		if content != formatContent {
			return fmt.Errorf("filestore: FORMAT %q: %w", content, ErrFormat)
		}
	case errors.Is(err, fs.ErrNotExist):
		if err := s.writeFormat(); err != nil {
			return err
		}
	default:
		return err
	}
	if err := s.fs.SyncDir("."); err != nil {
		return fmt.Errorf("filestore: %w", err)
	}
	s.ready = true
	return nil
}

// maxFormatRead bounds how much of a FORMAT file is read, so a wrong file cannot exhaust memory.
// The file of this version is 21 bytes.
const maxFormatRead = 256

// readFormat returns the start of the FORMAT file. The error wraps fs.ErrNotExist if the file
// does not exist.
func (s *Store) readFormat() (content string, err error) {
	f, err := s.fs.Open(formatName)
	if err != nil {
		return "", fmt.Errorf("filestore: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("filestore: %w", cerr)
		}
	}()
	size, err := f.Size()
	if err != nil {
		return "", fmt.Errorf("filestore: %w", err)
	}
	buf := make([]byte, min(size, maxFormatRead))
	n, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("filestore: %w", err)
	}
	return string(buf[:n]), nil
}

// writeFormat creates FORMAT durably: it writes FORMAT.tmp, syncs and closes it, and renames it.
// A leftover FORMAT.tmp of an earlier attempt is truncated.
func (s *Store) writeFormat() error {
	const tmp = formatName + tmpExt
	if err := s.writeFile(tmp, formatContent); err != nil {
		return err
	}
	if err := s.fs.Rename(tmp, formatName); err != nil {
		return fmt.Errorf("filestore: %w", err)
	}
	return nil
}

// writeFile creates name with the given content, syncs it and closes it.
func (s *Store) writeFile(name, content string) (err error) {
	f, err := s.fs.Create(name)
	if err != nil {
		return fmt.Errorf("filestore: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("filestore: %w", cerr)
		}
	}()
	n, err := f.WriteAt([]byte(content), 0)
	if err == nil && n != len(content) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("filestore: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("filestore: %w", err)
	}
	return nil
}

// lockID takes the lock of persistence ID id and returns the function that releases it. Calls
// for one ID are serialized; calls for different IDs do not wait for each other. The lock of an
// ID that nobody holds or waits for is forgotten.
func (s *Store) lockID(id string) (unlock func()) {
	s.locksMu.Lock()
	if s.locks == nil {
		s.locks = make(map[string]*idLock)
	}
	l := s.locks[id]
	if l == nil {
		l = &idLock{}
		s.locks[id] = l
	}
	l.refs++
	s.locksMu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		s.locksMu.Lock()
		if l.refs--; l.refs == 0 {
			delete(s.locks, id)
		}
		s.locksMu.Unlock()
	}
}
