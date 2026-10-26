// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// osFS is the FS of a directory of the operating system's file system.
type osFS struct{ root string }

var _ FS = (*osFS)(nil)

// path maps a store name to a path below the root. It returns an error for a name that fails
// fs.ValidPath, so no name can leave the root.
func (o *osFS) path(op, name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	return filepath.Join(o.root, filepath.FromSlash(name)), nil
}

func (o *osFS) Create(name string) (File, error) {
	p, err := o.path("Create", name)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644) //nolint:gosec // the store's files are world-readable by design; names are validated
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (o *osFS) Open(name string) (File, error) {
	p, err := o.path("Open", name)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_RDWR, 0) //nolint:gosec // names are validated
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (o *osFS) Remove(name string) error {
	p, err := o.path("Remove", name)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

func (o *osFS) Rename(oldname, newname string) error {
	from, err := o.path("Rename", oldname)
	if err != nil {
		return err
	}
	to, err := o.path("Rename", newname)
	if err != nil {
		return err
	}
	return os.Rename(from, to)
}

func (o *osFS) MkdirAll(name string) error {
	p, err := o.path("MkdirAll", name)
	if err != nil {
		return err
	}
	return os.MkdirAll(p, 0o755) //nolint:gosec // the store's directories are world-readable by design
}

func (o *osFS) ReadDir(name string) ([]Info, error) {
	p, err := o.path("ReadDir", name)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(p) // sorted by file name
	if err != nil {
		return nil, err
	}
	infos := make([]Info, 0, len(entries))
	for _, e := range entries {
		info := Info{Name: e.Name(), IsDir: e.IsDir()}
		if !info.IsDir {
			fi, err := e.Info()
			if err != nil {
				return nil, err
			}
			info.Size = fi.Size()
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// SyncDir flushes the entry changes of a directory. Windows cannot open a directory for Sync,
// so there it does nothing.
func (o *osFS) SyncDir(name string) (err error) {
	p, err := o.path("SyncDir", name)
	if err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(p) //nolint:gosec // names are validated
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	return d.Sync()
}

// osFile adds Size to *os.File.
type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
