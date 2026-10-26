// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// recFS wraps an FS for tests. It records every call of an FS or File method as
// "<Method> <name>[ <details>]", counts the files that Create and Open returned against the
// Close calls on them, and makes calls fail on demand: through the rules that fail adds, then
// through failOn.
type recFS struct {
	FS
	mu     sync.Mutex
	ops    []string
	open   int         // files returned by Create or Open and not closed yet
	rules  []*failRule // checked in the order added
	failOn func(op string) error
}

// failRule fails the calls of one method, optionally on one name, that which picks.
type failRule struct {
	method string
	name   string // "" matches every name
	which  func(n int) bool
	err    error
	n      int // calls matched so far
}

// nth picks the k-th matching call.
func nth(k int) func(int) bool { return func(n int) bool { return n == k } }

// every picks every k-th matching call: k, 2k, 3k and so on.
func every(k int) func(int) bool { return func(n int) bool { return n%k == 0 } }

// always picks every matching call.
func always(int) bool { return true }

// fail makes the calls of method (on name, unless name is "") that which picks return err.
// which receives the number of the call among the matching ones, counting from 1. For Rename,
// name is the old name.
func (r *recFS) fail(method, name string, which func(n int) bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rules = append(r.rules, &failRule{method: method, name: name, which: which, err: err})
}

// rec records a call and returns the error the call must fail with, if any.
func (r *recFS) rec(method, name, details string) error {
	op := method + " " + name
	if details != "" {
		op += " " + details
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
	for _, rule := range r.rules {
		if rule.method != method || (rule.name != "" && rule.name != name) {
			continue
		}
		rule.n++
		if rule.which(rule.n) {
			return rule.err
		}
	}
	if r.failOn != nil {
		return r.failOn(op)
	}
	return nil
}

// calls returns every call recorded so far.
func (r *recFS) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

// mark returns a position in the record for since.
func (r *recFS) mark() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// since returns the calls recorded after mark returned m.
func (r *recFS) since(m int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops[m:]...)
}

// openFiles returns the number of files returned by Create or Open and not closed.
func (r *recFS) openFiles() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.open
}

// checkClosed fails the test if a file is open.
func (r *recFS) checkClosed(t *testing.T) {
	t.Helper()
	if n := r.openFiles(); n != 0 {
		t.Fatalf("%d files left open; calls:\n%q", n, r.calls())
	}
}

func (r *recFS) wrap(name string, f File, err error) (File, error) {
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.open++
	r.mu.Unlock()
	return &recFile{File: f, r: r, name: name}, nil
}

func (r *recFS) Create(name string) (File, error) {
	if err := r.rec("Create", name, ""); err != nil {
		return nil, err
	}
	f, err := r.FS.Create(name)
	return r.wrap(name, f, err)
}

func (r *recFS) Open(name string) (File, error) {
	if err := r.rec("Open", name, ""); err != nil {
		return nil, err
	}
	f, err := r.FS.Open(name)
	return r.wrap(name, f, err)
}

func (r *recFS) Remove(name string) error {
	if err := r.rec("Remove", name, ""); err != nil {
		return err
	}
	return r.FS.Remove(name)
}

func (r *recFS) Rename(oldname, newname string) error {
	if err := r.rec("Rename", oldname, newname); err != nil {
		return err
	}
	return r.FS.Rename(oldname, newname)
}

func (r *recFS) MkdirAll(name string) error {
	if err := r.rec("MkdirAll", name, ""); err != nil {
		return err
	}
	return r.FS.MkdirAll(name)
}

func (r *recFS) ReadDir(name string) ([]Info, error) {
	if err := r.rec("ReadDir", name, ""); err != nil {
		return nil, err
	}
	return r.FS.ReadDir(name)
}

func (r *recFS) SyncDir(name string) error {
	if err := r.rec("SyncDir", name, ""); err != nil {
		return err
	}
	return r.FS.SyncDir(name)
}

// recFile is a File of a recFS.
type recFile struct {
	File
	r    *recFS
	name string
}

func (f *recFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.r.rec("ReadAt", f.name, fmt.Sprintf("%d %d", off, len(p))); err != nil {
		return 0, err
	}
	return f.File.ReadAt(p, off)
}

// partialWrite is an injected WriteAt error that writes the first n bytes before it fails, as
// a write cut short by a full disk does.
type partialWrite struct {
	n   int
	err error
}

func (p *partialWrite) Error() string { return p.err.Error() }
func (p *partialWrite) Unwrap() error { return p.err }

// WriteAt records the written bytes as a string.
func (f *recFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.r.rec("WriteAt", f.name, string(p)); err != nil {
		var pw *partialWrite
		if errors.As(err, &pw) {
			n, werr := f.File.WriteAt(p[:min(pw.n, len(p))], off)
			if werr != nil {
				return n, werr
			}
			return n, pw.err
		}
		return 0, err
	}
	return f.File.WriteAt(p, off)
}

func (f *recFile) Truncate(size int64) error {
	if err := f.r.rec("Truncate", f.name, fmt.Sprint(size)); err != nil {
		return err
	}
	return f.File.Truncate(size)
}

func (f *recFile) Size() (int64, error) {
	if err := f.r.rec("Size", f.name, ""); err != nil {
		return 0, err
	}
	return f.File.Size()
}

func (f *recFile) Sync() error {
	if err := f.r.rec("Sync", f.name, ""); err != nil {
		return err
	}
	return f.File.Sync()
}

// Close closes the file and counts it as closed even when it returns an injected error.
func (f *recFile) Close() error {
	injected := f.r.rec("Close", f.name, "")
	f.r.mu.Lock()
	f.r.open--
	f.r.mu.Unlock()
	if err := f.File.Close(); err != nil {
		return err
	}
	return injected
}

// newRecFS returns a recFS over the OS file system of a new temporary directory, and the
// directory.
func newRecFS(t *testing.T) (*recFS, string) {
	t.Helper()
	dir := t.TempDir()
	return &recFS{FS: &osFS{root: dir}}, dir
}
