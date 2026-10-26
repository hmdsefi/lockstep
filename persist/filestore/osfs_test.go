// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// umaskModes returns the directory and file modes the process creates with 0o755 and 0o644,
// after its umask.
func umaskModes(t *testing.T) (dir, file fs.FileMode) {
	t.Helper()
	probe := t.TempDir()
	d := filepath.Join(probe, "d")
	f := filepath.Join(probe, "f")
	if err := os.Mkdir(d, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f, nil, 0o666); err != nil { //nolint:gosec // probes the umask
		t.Fatal(err)
	}
	di, err := os.Stat(d)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(f)
	if err != nil {
		t.Fatal(err)
	}
	return 0o755 & di.Mode().Perm(), 0o644 & fi.Mode().Perm()
}

func TestATPER42OpenCreatesDirAndFormat(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits")
	}
	wantDir, wantFile := umaskModes(t)
	base := t.TempDir()
	root := filepath.Join(base, "a", "b", "store")
	s, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s == nil {
		t.Fatal("Open returned a nil Store")
	}
	for _, d := range []string{"a", filepath.Join("a", "b"), filepath.Join("a", "b", "store"),
		filepath.Join("a", "b", "store", "journal"), filepath.Join("a", "b", "store", "snapshots")} {
		fi, err := os.Stat(filepath.Join(base, d))
		if err != nil || !fi.IsDir() {
			t.Fatalf("directory %s: %v", d, err)
		}
		if fi.Mode().Perm() != wantDir {
			t.Fatalf("directory %s mode = %v, want %v", d, fi.Mode().Perm(), wantDir)
		}
	}
	fi, err := os.Stat(filepath.Join(root, "FORMAT"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != wantFile {
		t.Fatalf("FORMAT mode = %v, want %v", fi.Mode().Perm(), wantFile)
	}
	got, err := os.ReadFile(filepath.Join(root, "FORMAT"))
	if err != nil || string(got) != "lockstep-filestore 1\n" {
		t.Fatalf("FORMAT = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(root, "FORMAT.tmp")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("FORMAT.tmp left behind: %v", err)
	}
	if _, err := Open(root); err != nil {
		t.Fatalf("second Open of the same directory: %v", err)
	}
}

func TestOpenErrors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{file, filepath.Join(file, "sub")} {
		s, err := Open(dir)
		if err == nil || s != nil {
			t.Fatalf("Open(%q) = %v, %v; want an error and no Store", dir, s, err)
		}
		var pe *fs.PathError
		if !errors.As(err, &pe) {
			t.Fatalf("Open(%q) error %v does not wrap a *fs.PathError", dir, err)
		}
	}
}

func TestATPER42InvalidNames(t *testing.T) {
	fsys := &osFS{root: t.TempDir()}
	bad := []string{"../x", "/x", "a/../b", "a//b", "a/", "", "..", `..\x`, `a\b`, `\x`}
	calls := map[string]func(name string) error{
		"Create":      func(n string) error { f, err := fsys.Create(n); closeIf(f); return err },
		"Open":        func(n string) error { f, err := fsys.Open(n); closeIf(f); return err },
		"Remove":      fsys.Remove,
		"MkdirAll":    fsys.MkdirAll,
		"ReadDir":     func(n string) error { _, err := fsys.ReadDir(n); return err },
		"SyncDir":     fsys.SyncDir,
		"Rename from": func(n string) error { return fsys.Rename(n, "ok") },
		"Rename to":   func(n string) error { return fsys.Rename("ok", n) },
	}
	for _, name := range bad {
		for op, call := range calls {
			err := call(name)
			var pe *fs.PathError
			if !errors.As(err, &pe) || !errors.Is(err, fs.ErrInvalid) || pe.Path != name {
				t.Fatalf("%s(%q) = %#v, want a *fs.PathError for %q wrapping fs.ErrInvalid", op, name, err, name)
			}
		}
	}
	_, err := fsys.Create("../x")
	var pe *fs.PathError
	if !errors.As(err, &pe) || pe.Op != "Create" || pe.Path != "../x" || pe.Err != fs.ErrInvalid {
		t.Fatalf("Create(\"../x\") = %#v, want &fs.PathError{Op: \"Create\", Path: \"../x\", Err: fs.ErrInvalid}", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(fsys.root), "x")); err == nil {
		t.Fatal("Create(\"../x\") created a file outside the root")
	}
}

func closeIf(f File) {
	if f != nil {
		_ = f.Close()
	}
}

func TestOSFSFiles(t *testing.T) {
	fsys := &osFS{root: t.TempDir()}
	if err := fsys.MkdirAll("d/e"); err != nil {
		t.Fatal(err)
	}
	f, err := fsys.Create("d/e/f")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.WriteAt([]byte("hello"), 0); n != 5 || err != nil {
		t.Fatalf("WriteAt = %d, %v", n, err)
	}
	if n, err := f.WriteAt([]byte("!"), 7); n != 1 || err != nil { // leaves a hole
		t.Fatalf("WriteAt past the end = %d, %v", n, err)
	}
	if size, err := f.Size(); err != nil || size != 8 {
		t.Fatalf("Size = %d, %v; want 8", size, err)
	}
	buf := make([]byte, 5)
	if n, err := f.ReadAt(buf, 0); n != 5 || err != nil || string(buf) != "hello" {
		t.Fatalf("ReadAt = %d, %v, %q", n, err, buf)
	}
	if n, err := f.ReadAt(make([]byte, 4), 6); n != 2 || !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt across the end = %d, %v; want 2 and io.EOF", n, err)
	}
	if err := f.Truncate(3); err != nil {
		t.Fatal(err)
	}
	if size, _ := f.Size(); size != 3 {
		t.Fatalf("Size after Truncate = %d, want 3", size)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("x"), 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("WriteAt after Close = %v, want fs.ErrClosed", err)
	}

	// Create truncates; Open keeps the content and does not create.
	g, err := fsys.Open("d/e/f")
	if err != nil {
		t.Fatal(err)
	}
	if size, _ := g.Size(); size != 3 {
		t.Fatalf("Open then Size = %d, want 3", size)
	}
	if n, err := g.WriteAt([]byte("X"), 1); n != 1 || err != nil { // Open is read-write
		t.Fatalf("WriteAt on an opened file = %d, %v", n, err)
	}
	_ = g.Close()
	h, err := fsys.Create("d/e/f")
	if err != nil {
		t.Fatal(err)
	}
	if size, _ := h.Size(); size != 0 {
		t.Fatalf("Create did not truncate: size %d", size)
	}
	_ = h.Close()
	if _, err := fsys.Open("d/e/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open of a missing file = %v, want fs.ErrNotExist", err)
	}
}

func TestOSFSDirectories(t *testing.T) {
	fsys := &osFS{root: t.TempDir()}
	for _, d := range []string{"d", "d/sub", "d/Z"} {
		if err := fsys.MkdirAll(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := fsys.MkdirAll("d/sub"); err != nil {
		t.Fatalf("MkdirAll of an existing directory = %v", err)
	}
	for name, content := range map[string]string{"d/b": "xx", "d/a": "", "d/Y": "x", "d/sub/inner": "y"} {
		f, err := fsys.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte(content), 0); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
	got, err := fsys.ReadDir("d")
	if err != nil {
		t.Fatal(err)
	}
	want := []Info{{"Y", 1, false}, {"Z", 0, true}, {"a", 0, false}, {"b", 2, false}, {"sub", 0, true}}
	if len(got) != len(want) {
		t.Fatalf("ReadDir = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ReadDir[%d] = %+v, want %+v (sorted by byte order)", i, got[i], want[i])
		}
	}
	if root, err := fsys.ReadDir("."); err != nil || len(root) != 1 || root[0].Name != "d" {
		t.Fatalf("ReadDir(\".\") = %+v, %v", root, err)
	}
	if _, err := fsys.ReadDir("nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadDir of a missing directory = %v", err)
	}

	if err := fsys.SyncDir("d"); err != nil {
		t.Fatalf("SyncDir: %v", err)
	}
	if err := fsys.SyncDir("."); err != nil {
		t.Fatalf("SyncDir(\".\"): %v", err)
	}

	if err := fsys.Rename("d/a", "d/sub/a2"); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.Open("d/a"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("renamed file still at the old name: %v", err)
	}
	if err := fsys.Remove("d/sub/a2"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Remove("d/sub"); err == nil {
		t.Fatal("Remove of a non-empty directory succeeded")
	}
	if err := fsys.Remove("d/sub/inner"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Remove("d/sub"); err != nil {
		t.Fatalf("Remove of an empty directory: %v", err)
	}
	if err := fsys.Remove("d/sub"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Remove of a missing name = %v, want fs.ErrNotExist", err)
	}
}
