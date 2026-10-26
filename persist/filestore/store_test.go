// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestPER320Init(t *testing.T) {
	const format = "lockstep-filestore 1\n"

	t.Run("new store", func(t *testing.T) {
		r, dir := newRecFS(t)
		s := New(r)
		if len(r.calls()) != 0 {
			t.Fatalf("New performed I/O: %v", r.calls())
		}
		if err := s.ensureInit(); err != nil {
			t.Fatalf("ensureInit: %v", err)
		}
		want := []string{
			"MkdirAll journal",
			"MkdirAll snapshots",
			"Open FORMAT",
			"Create FORMAT.tmp",
			"WriteAt FORMAT.tmp " + format,
			"Sync FORMAT.tmp",
			"Close FORMAT.tmp",
			"Rename FORMAT.tmp FORMAT",
			"SyncDir .",
		}
		if got := r.calls(); !reflect.DeepEqual(got, want) {
			t.Fatalf("calls =\n%q\nwant\n%q", got, want)
		}
		if got, err := os.ReadFile(filepath.Join(dir, "FORMAT")); err != nil || string(got) != format {
			t.Fatalf("FORMAT = %q, %v", got, err)
		}
		if _, err := os.Stat(filepath.Join(dir, "FORMAT.tmp")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("FORMAT.tmp = %v, want it renamed away", err)
		}
		for _, d := range []string{"journal", "snapshots"} {
			if fi, err := os.Stat(filepath.Join(dir, d)); err != nil || !fi.IsDir() {
				t.Fatalf("%s: %v", d, err)
			}
		}
		if n := r.openFiles(); n != 0 {
			t.Fatalf("%d files left open", n)
		}

		// Initialization runs once: the next call does no I/O.
		before := len(r.calls())
		if err := s.ensureInit(); err != nil || len(r.calls()) != before {
			t.Fatalf("second ensureInit = %v with %d new calls", err, len(r.calls())-before)
		}
	})

	t.Run("existing format", func(t *testing.T) {
		r, dir := newRecFS(t)
		if err := os.WriteFile(filepath.Join(dir, "FORMAT"), []byte(format), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := New(r).ensureInit(); err != nil {
			t.Fatalf("ensureInit: %v", err)
		}
		want := []string{"MkdirAll journal", "MkdirAll snapshots", "Open FORMAT", "Size FORMAT",
			"ReadAt FORMAT 0 21", "Close FORMAT", "SyncDir ."}
		if got := r.calls(); !reflect.DeepEqual(got, want) {
			t.Fatalf("calls =\n%q\nwant\n%q", got, want)
		}
		if n := r.openFiles(); n != 0 {
			t.Fatalf("%d files left open", n)
		}
	})

	t.Run("leftover FORMAT.tmp", func(t *testing.T) {
		r, dir := newRecFS(t)
		if err := os.WriteFile(filepath.Join(dir, "FORMAT.tmp"), []byte("half-writ"+format+format), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := New(r).ensureInit(); err != nil {
			t.Fatalf("ensureInit: %v", err)
		}
		if got, _ := os.ReadFile(filepath.Join(dir, "FORMAT")); string(got) != format {
			t.Fatalf("FORMAT = %q", got)
		}
	})

	t.Run("unsupported format", func(t *testing.T) {
		for _, content := range []string{"lockstep-filestore 2\n", "lockstep-filestore 1", "", "lockstep-filestore 1\nx"} {
			r, dir := newRecFS(t)
			if err := os.WriteFile(filepath.Join(dir, "FORMAT"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			s := New(r)
			for i := range 2 { // an init error is returned again by the next call
				err := s.ensureInit()
				want := fmt.Sprintf("filestore: FORMAT %q: filestore: unsupported store format", content)
				if !errors.Is(err, ErrFormat) || err.Error() != want {
					t.Fatalf("content %q, call %d: err = %v, want %q wrapping ErrFormat", content, i, err, want)
				}
			}
			if n := r.openFiles(); n != 0 {
				t.Fatalf("%d files left open", n)
			}
			if got, _ := os.ReadFile(filepath.Join(dir, "FORMAT")); string(got) != content {
				t.Fatalf("FORMAT overwritten with %q", got)
			}
		}
	})

	t.Run("failed init is retried", func(t *testing.T) {
		boom := errors.New("injected")
		steps := []string{"MkdirAll journal", "MkdirAll snapshots", "Open FORMAT", "Create FORMAT.tmp",
			"WriteAt FORMAT.tmp " + format, "Sync FORMAT.tmp", "Rename FORMAT.tmp FORMAT", "SyncDir ."}
		for _, failing := range steps {
			r, dir := newRecFS(t)
			failures := 1
			r.failOn = func(op string) error {
				if op == failing && failures > 0 {
					failures--
					if op == "Open FORMAT" {
						return &fs.PathError{Op: "open", Path: "FORMAT", Err: boom} // not ErrNotExist
					}
					return boom
				}
				return nil
			}
			s := New(r)
			err := s.ensureInit()
			if !errors.Is(err, boom) {
				t.Fatalf("failing %q: err = %v, want it to wrap the FS error", failing, err)
			}
			if n := r.openFiles(); n != 0 {
				t.Fatalf("failing %q: %d files left open", failing, n)
			}
			if err := s.ensureInit(); err != nil {
				t.Fatalf("failing %q: retry = %v", failing, err)
			}
			if got, err := os.ReadFile(filepath.Join(dir, "FORMAT")); err != nil || string(got) != format {
				t.Fatalf("failing %q: FORMAT after retry = %q, %v", failing, got, err)
			}
		}
	})

	t.Run("concurrent first calls initialize once", func(t *testing.T) {
		r, _ := newRecFS(t)
		s := New(r)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.ensureInit(); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		creates := 0
		for _, c := range r.calls() {
			if c == "Create FORMAT.tmp" {
				creates++
			}
		}
		if creates != 1 {
			t.Fatalf("%d FORMAT.tmp creations, want 1", creates)
		}
	})
}

func TestWithSegmentSize(t *testing.T) {
	if got := New(nil).set.segmentSize; got != DefaultSegmentSize {
		t.Fatalf("default segment size = %d, want %d", got, DefaultSegmentSize)
	}
	for _, n := range []int64{1, 4096, 1 << 30} {
		if got := New(nil, WithSegmentSize(n)).set.segmentSize; got != n {
			t.Fatalf("WithSegmentSize(%d) gave %d", n, got)
		}
	}
	if got := New(nil, WithSegmentSize(10), WithSegmentSize(20)).set.segmentSize; got != 20 {
		t.Fatalf("last option wins: got %d, want 20", got)
	}
}

func TestWithSegmentSizePanics(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{
		{0, "filestore: WithSegmentSize: 0 out of range [1, 1073741824]"},
		{1<<30 + 1, "filestore: WithSegmentSize: 1073741825 out of range [1, 1073741824]"},
		{-5, "filestore: WithSegmentSize: -5 out of range [1, 1073741824]"},
	} {
		func() {
			defer func() {
				r := recover()
				if got, ok := r.(string); !ok || got != tc.want {
					t.Fatalf("WithSegmentSize(%d) panic = %#v, want %q", tc.n, r, tc.want)
				}
			}()
			WithSegmentSize(tc.n)
		}()
	}
}

func TestLockID(t *testing.T) {
	s := New(nil)
	unlockA := s.lockID("a")
	other := make(chan struct{})
	go func() {
		defer close(other)
		s.lockID("b")() // a different ID is not blocked
	}()
	select {
	case <-other:
	case <-time.After(5 * time.Second):
		t.Fatal("locking another ID blocked")
	}

	acquired := make(chan struct{})
	go func() {
		unlock := s.lockID("a")
		close(acquired)
		unlock()
	}()
	select {
	case <-acquired:
		t.Fatal("second lock of the same ID acquired while the first was held")
	case <-time.After(50 * time.Millisecond):
	}
	unlockA()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("lock of the same ID not acquired after unlock")
	}

	// A lock nobody holds or waits for leaves no entry behind.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.locksMu.Lock()
		n := len(s.locks)
		s.locksMu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d lock entries left", n)
		}
		time.Sleep(time.Millisecond)
	}
}
