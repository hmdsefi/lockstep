// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

// FS is the file system a Store runs on. Names are slash-separated paths relative to the store
// root that satisfy io/fs.ValidPath; "." is the root. Errors are *fs.PathError values whose Err
// matches the io/fs sentinels (fs.ErrNotExist, fs.ErrExist, fs.ErrInvalid, fs.ErrClosed) with
// errors.Is, as os errors do.
type FS interface {
	Create(name string) (File, error) // create, or truncate an existing file; read-write
	Open(name string) (File, error)   // open an existing file read-write
	Remove(name string) error         // a file or an empty directory
	Rename(oldname, newname string) error
	MkdirAll(name string) error
	ReadDir(name string) ([]Info, error) // sorted by name, ascending byte order
	SyncDir(name string) error           // make the directory's entry changes durable
}

// File is an open read-write file.
type File interface {
	ReadAt(p []byte, off int64) (int, error) // os.File.ReadAt semantics
	WriteAt(p []byte, off int64) (int, error)
	Truncate(size int64) error
	Size() (int64, error)
	Sync() error // make the file's written data durable
	Close() error
}

// Info describes a directory entry.
type Info struct {
	Name  string // base name
	Size  int64  // file size; 0 for a directory
	IsDir bool
}
