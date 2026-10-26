// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/persist"
)

// snapName is the FS name of snapshot seq of id.
func snapName(id string, seq uint64) string { //nolint:unparam // the scenarios use one ID
	return snapDir(id) + "/" + seqName(seq, snapExt)
}

// tmpName is the FS name of the temporary file of snapshot seq of id.
func tmpName(id string, seq uint64) string { //nolint:unparam // the scenarios use one ID
	return snapDir(id) + "/" + seqName(seq, tmpExt)
}

// snap returns the snapshot with sequence number seq and the data "s<seq>".
func snap(seq uint64) persist.Snapshot {
	return persist.Snapshot{Seq: seq, Data: []byte(fmt.Sprintf("s%d", seq))}
}

func mustSave(t *testing.T, s *Store, id string, seqs ...uint64) {
	t.Helper()
	for _, seq := range seqs {
		if err := s.Save(context.Background(), id, snap(seq)); err != nil {
			t.Fatalf("Save(%q, %d): %v", id, seq, err)
		}
	}
}

// mustLoad checks the result of Load: no snapshot if want is 0, else the snap(want).
func mustLoad(t *testing.T, s *Store, id string, want uint64) {
	t.Helper()
	got, ok, err := s.Load(context.Background(), id)
	if err != nil {
		t.Fatalf("Load(%q): %v", id, err)
	}
	if want == 0 {
		if ok {
			t.Fatalf("Load(%q) = %+v, want none", id, got)
		}
		return
	}
	if !ok || got.Seq != want || !bytes.Equal(got.Data, snap(want).Data) {
		t.Fatalf("Load(%q) = %+v, %v, want %+v", id, got, ok, snap(want))
	}
}

// names returns the names in the directory name below dir, or nil if it does not exist.
func names(t *testing.T, dir, name string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, filepath.FromSlash(name)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// writeRaw writes a file below dir, creating its directory.
func writeRaw(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil { //nolint:gosec // the store writes its files with this mode
		t.Fatal(err)
	}
}

func snapRecord(t *testing.T, seq uint64, data string) []byte {
	t.Helper()
	rec, err := encodeSnapshotRecord(persist.Snapshot{Seq: seq, Data: []byte(data)})
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// AT-PER-39 (OS part)
func TestATPER39SaveReplaces(t *testing.T) {
	s, _, dir := newJournal(t)
	ctx := context.Background()
	if err := s.Save(ctx, "c-1", persist.Snapshot{Seq: 4, Data: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "c-1", persist.Snapshot{Seq: 4, Data: []byte("second, longer")}); err != nil {
		t.Fatal(err)
	}
	if got, want := names(t, dir, snapDir("c-1")), []string{seqName(4, snapExt)}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("snapshot directory = %q, want %q", got, want)
	}
	if got, want := readFile(t, dir, snapName("c-1", 4)), snapRecord(t, 4, "second, longer"); !bytes.Equal(got, want) {
		t.Fatalf("snapshot file = %x, want %x", got, want)
	}
	got, ok, err := s.Load(ctx, "c-1")
	if err != nil || !ok || string(got.Data) != "second, longer" {
		t.Fatalf("Load = %+v, %v, %v", got, ok, err)
	}

	t.Run("failing save removes its temporary file", func(t *testing.T) {
		s, r, dir := newJournal(t)
		boom := errors.New("injected")
		mustSave(t, s, "c-1", 3)
		r.fail("Sync", tmpName("c-1", 5), nth(1), boom)
		err := s.Save(ctx, "c-1", snap(5))
		if !errors.Is(err, boom) {
			t.Fatalf("Save = %v, want the injected error", err)
		}
		if want := "filestore: save snapshot c-1 seq 5: "; !strings.HasPrefix(err.Error(), want) {
			t.Fatalf("Save error = %q, want prefix %q", err, want)
		}
		if got := names(t, dir, snapDir("c-1")); fmt.Sprint(got) != fmt.Sprint([]string{seqName(3, snapExt)}) {
			t.Fatalf("snapshot directory = %q, want only the 3 snapshot", got)
		}
		mustLoad(t, s, "c-1", 3)
	})

	t.Run("seq 0", func(t *testing.T) {
		s, _, dir := newJournal(t)
		err := s.Save(ctx, "c-1", persist.Snapshot{Data: []byte("x")})
		if !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("Save(seq 0) = %v, want one wrapping fs.ErrInvalid", err)
		}
		if want := "filestore: save snapshot c-1 seq 0: "; !strings.HasPrefix(err.Error(), want) {
			t.Fatalf("Save error = %q, want prefix %q", err, want)
		}
		if got := names(t, dir, snapDir("c-1")); got != nil {
			t.Fatalf("Save(seq 0) created %q", got)
		}
	})

	t.Run("stale temporary files", func(t *testing.T) {
		s, r, dir := newJournal(t)
		mustSave(t, s, "c-1", 1)
		writeRaw(t, dir, tmpName("c-1", 9), []byte("stale"))
		writeRaw(t, dir, snapDir("c-1")+"/other.tmp", []byte("stale"))
		writeRaw(t, dir, snapDir("c-1")+"/notes.txt", []byte("keep"))
		m := r.mark()
		mustSave(t, s, "c-1", 2)
		got := names(t, dir, snapDir("c-1"))
		want := []string{seqName(1, snapExt), seqName(2, snapExt), "notes.txt"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("snapshot directory = %q, want %q (calls %q)", got, want, r.since(m))
		}
	})
}

// initStore makes the Store run its initialization, so the calls a test records afterwards are
// those of the method under test.
func initStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.ensureInit(); err != nil {
		t.Fatal(err)
	}
}

// PER-400: the calls of a save, in order.
func TestPER400SaveSteps(t *testing.T) {
	s, r, _ := newJournal(t)
	initStore(t, s)
	d := snapDir("c-1")
	rec := string(snapRecord(t, 7, "s7"))

	m := r.mark()
	mustSave(t, s, "c-1", 7)
	checkCalls(t, "first save", r.since(m), []string{
		"ReadDir " + d,
		"MkdirAll " + d,
		"Create " + tmpName("c-1", 7),
		"WriteAt " + tmpName("c-1", 7) + " " + rec,
		"Sync " + tmpName("c-1", 7),
		"Close " + tmpName("c-1", 7),
		"Rename " + tmpName("c-1", 7) + " " + snapName("c-1", 7),
		"SyncDir " + d,
		"SyncDir " + snapshotsDir,
	})

	m = r.mark()
	mustSave(t, s, "c-1", 8)
	rec = string(snapRecord(t, 8, "s8"))
	checkCalls(t, "second save", r.since(m), []string{
		"ReadDir " + d,
		"MkdirAll " + d,
		"Create " + tmpName("c-1", 8),
		"WriteAt " + tmpName("c-1", 8) + " " + rec,
		"Sync " + tmpName("c-1", 8),
		"Close " + tmpName("c-1", 8),
		"Rename " + tmpName("c-1", 8) + " " + snapName("c-1", 8),
		"SyncDir " + d,
	})
}

// PER-400: whichever step fails, the temporary file is gone, no file stays open, the error names
// the snapshot, and a later save succeeds.
func TestPER400SaveFailures(t *testing.T) {
	d := snapDir("c-1")
	tmp := tmpName("c-1", 5)
	for _, tc := range []struct{ method, name string }{
		{"ReadDir", d},
		{"MkdirAll", d},
		{"Remove", d + "/" + seqName(2, tmpExt)},
		{"Create", tmp},
		{"WriteAt", tmp},
		{"Sync", tmp},
		{"Close", tmp},
		{"Rename", tmp},
		{"SyncDir", d},
		{"SyncDir", snapshotsDir},
	} {
		t.Run(tc.method+" "+tc.name, func(t *testing.T) {
			s, r, dir := newJournal(t)
			mustSave(t, s, "c-2", 1)     // so that initialization is done and c-1 is the first of its ID
			if tc.name != snapshotsDir { // else the directory of c-1 must be new
				writeRaw(t, dir, d+"/"+seqName(2, tmpExt), []byte("stale"))
			}
			boom := errors.New("injected")
			r.fail(tc.method, tc.name, nth(1), boom)
			err := s.Save(context.Background(), "c-1", snap(5))
			if !errors.Is(err, boom) {
				t.Fatalf("Save = %v, want the injected error", err)
			}
			if want := "filestore: save snapshot c-1 seq 5: "; !strings.HasPrefix(err.Error(), want) {
				t.Fatalf("Save error = %q, want prefix %q", err, want)
			}
			r.checkClosed(t)
			for _, n := range names(t, dir, d) {
				if n == seqName(5, tmpExt) {
					t.Fatalf("temporary file left behind: %q", names(t, dir, d))
				}
			}
			if tc.method != "SyncDir" && fileSize(t, dir, snapName("c-1", 5)) >= 0 {
				t.Fatalf("a failed save left %s", snapName("c-1", 5))
			}
			mustSave(t, s, "c-1", 5)
			mustLoad(t, s, "c-1", 5)
			if got := names(t, dir, d); fmt.Sprint(got) != fmt.Sprint([]string{seqName(5, snapExt)}) {
				t.Fatalf("snapshot directory = %q, want only the 5 snapshot", got)
			}
		})
	}
}

// PER-400: a Save that created the ID's directory and failed leaves the snapshots directory
// unsynced; the next successful Save syncs it, and later ones do not.
func TestPER400ParentSyncAfterFailedSave(t *testing.T) {
	s, r, _ := newJournal(t)
	initStore(t, s)
	boom := errors.New("injected")
	r.fail("Sync", tmpName("c-1", 1), nth(1), boom)
	if err := s.Save(context.Background(), "c-1", snap(1)); !errors.Is(err, boom) {
		t.Fatalf("Save = %v, want the injected error", err)
	}
	m := r.mark()
	mustSave(t, s, "c-1", 1)
	if got := countOf(r.since(m), "SyncDir "+snapshotsDir); got != 1 {
		t.Fatalf("second save synced %q %d times", snapshotsDir, got)
	}
	m = r.mark()
	mustSave(t, s, "c-1", 2)
	if got := countOf(r.since(m), "SyncDir "+snapshotsDir); got != 0 {
		t.Fatalf("third save synced %q %d times", snapshotsDir, got)
	}
}

func countOf(calls []string, call string) int {
	n := 0
	for _, c := range calls {
		if c == call {
			n++
		}
	}
	return n
}

func TestPER400SaveRejects(t *testing.T) {
	s, r, dir := newJournal(t)
	ctx := context.Background()
	err := s.Save(ctx, "", snap(1))
	if !errors.Is(err, persist.ErrInvalidID) {
		t.Fatalf("Save(\"\") = %v, want ErrInvalidID", err)
	}
	big := persist.Snapshot{Seq: 1, Data: make([]byte, MaxSnapshotSize)}
	err = s.Save(ctx, "c-1", big)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Save(too large) = %v, want ErrTooLarge", err)
	}
	if got := names(t, dir, snapDir("c-1")); got != nil {
		t.Fatalf("a rejected snapshot created %q", got)
	}
	if n := countOf(r.calls(), "Create "+tmpName("c-1", 1)); n != 0 {
		t.Fatalf("a rejected snapshot created a file")
	}
	// The data may change after Save: the store keeps a copy.
	data := []byte("abc")
	mustSaveData(t, s, "c-1", 2, data)
	data[0] = 'X'
	got, ok, err := s.Load(ctx, "c-1")
	if err != nil || !ok || string(got.Data) != "abc" {
		t.Fatalf("Load = %+v, %v, %v", got, ok, err)
	}
	got.Data[0] = 'Y' // the caller owns what Load returns
	got2, _, _ := s.Load(ctx, "c-1")
	if string(got2.Data) != "abc" {
		t.Fatalf("Load = %q after the caller changed an earlier result", got2.Data)
	}
}

func mustSaveData(t *testing.T, s *Store, id string, seq uint64, data []byte) {
	t.Helper()
	if err := s.Save(context.Background(), id, persist.Snapshot{Seq: seq, Data: data}); err != nil {
		t.Fatal(err)
	}
}

// AT-PER-40
func TestATPER40LoadSkipsDamaged(t *testing.T) {
	s, _, dir := newJournal(t)
	mustSave(t, s, "c-1", 3, 5)
	mustLoad(t, s, "c-1", 5)

	p5 := snapName("c-1", 5)
	b := readFile(t, dir, p5)
	writeRaw(t, dir, p5, flipped(b, len(b)-1)) // a byte of the payload
	mustLoad(t, s, "c-1", 3)

	// 3.snap renamed to 4.snap: its seq is 3, not 4.
	if err := os.Rename(filepath.Join(dir, filepath.FromSlash(snapName("c-1", 3))),
		filepath.Join(dir, filepath.FromSlash(snapName("c-1", 4)))); err != nil {
		t.Fatal(err)
	}
	mustLoad(t, s, "c-1", 0)
}

// PER-410: which files are intact.
func TestPER410LoadEdges(t *testing.T) {
	s, r, dir := newJournal(t)
	ctx := context.Background()
	d := snapDir("c-1")
	mustLoad(t, s, "c-1", 0) // no directory

	good := snapRecord(t, 2, "s2")
	writeRaw(t, dir, snapName("c-1", 2), good)
	for name, data := range map[string][]byte{
		"empty":         {},
		"short header":  good[:5],
		"header only":   good[:8],
		"cut payload":   good[:len(good)-1],
		"trailing byte": append(bytes.Clone(snapRecord(t, 9, "s9")), 0),
		"huge length":   {0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
		"zero length":   {0, 0, 0, 0, 0, 0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			writeRaw(t, dir, d+"/"+seqName(9, snapExt), data)
			mustLoad(t, s, "c-1", 2)
		})
	}
	writeRaw(t, dir, d+"/"+seqName(9, snapExt), []byte("not a record at all"))
	writeRaw(t, dir, d+"/"+seqName(8, tmpExt), snapRecord(t, 8, "s8")) // a temporary file
	writeRaw(t, dir, d+"/9.snap", snapRecord(t, 9, "s9"))              // a name of another width
	writeRaw(t, dir, d+"/"+seqName(7, snapExt)+".bak", snapRecord(t, 7, "s7"))
	if err := os.Mkdir(filepath.Join(dir, filepath.FromSlash(d), seqName(6, snapExt)), 0o755); err != nil {
		t.Fatal(err)
	}
	mustLoad(t, s, "c-1", 2)

	// An empty snapshot is intact, and its Data is empty.
	mustSaveData(t, s, "c-1", 10, nil)
	got, ok, err := s.Load(ctx, "c-1")
	if err != nil || !ok || got.Seq != 10 || len(got.Data) != 0 {
		t.Fatalf("Load = %+v, %v, %v", got, ok, err)
	}

	// An I/O error is returned, not skipped.
	boom := errors.New("injected")
	for _, method := range []string{"ReadDir", "Open", "Size", "ReadAt", "Close"} {
		r.rules = nil
		r.fail(method, "", always, boom)
		_, ok, err := s.Load(ctx, "c-1")
		if !errors.Is(err, boom) || ok {
			t.Fatalf("Load with failing %s = %v, %v", method, ok, err)
		}
		if want := "filestore: load snapshot c-1: "; !strings.HasPrefix(err.Error(), want) {
			t.Fatalf("Load error = %q, want prefix %q", err, want)
		}
		r.checkClosed(t)
	}
}

// AT-PER-41
func TestATPER41DeleteSnapshots(t *testing.T) {
	s, r, dir := newJournal(t)
	mustSave(t, s, "c-1", 1, 2, 3)
	m := r.mark()
	if err := s.DeleteSnapshots(context.Background(), "c-1", 2); err != nil {
		t.Fatal(err)
	}
	if got, want := names(t, dir, snapDir("c-1")), []string{seqName(3, snapExt)}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("snapshot directory = %q, want %q", got, want)
	}
	d := snapDir("c-1")
	checkCalls(t, "DeleteSnapshots", r.since(m), []string{
		"ReadDir " + d,
		"Remove " + snapName("c-1", 1),
		"Remove " + snapName("c-1", 2),
		"SyncDir " + d,
	})
	mustLoad(t, s, "c-1", 3)
}

// PER-420: edges and failures.
func TestPER420DeleteSnapshotsEdges(t *testing.T) {
	ctx := context.Background()
	d := snapDir("c-1")

	t.Run("nothing to delete", func(t *testing.T) {
		s, r, dir := newJournal(t)
		if err := s.DeleteSnapshots(ctx, "c-1", 9); err != nil { // no directory
			t.Fatal(err)
		}
		mustSave(t, s, "c-1", 5)
		m := r.mark()
		if err := s.DeleteSnapshots(ctx, "c-1", 4); err != nil {
			t.Fatal(err)
		}
		checkCalls(t, "to 4", r.since(m), []string{"ReadDir " + d})
		if err := s.DeleteSnapshots(ctx, "c-1", 0); err != nil {
			t.Fatal(err)
		}
		if got := names(t, dir, d); len(got) != 1 {
			t.Fatalf("snapshot directory = %q", got)
		}
	})

	t.Run("only snapshots go", func(t *testing.T) {
		s, _, dir := newJournal(t)
		mustSave(t, s, "c-1", 1, 2)
		writeRaw(t, dir, tmpName("c-1", 1), []byte("tmp"))
		writeRaw(t, dir, d+"/notes.txt", []byte("keep"))
		writeRaw(t, dir, d+"/"+seqName(1, snapExt)+".bak", []byte("keep"))
		if err := s.DeleteSnapshots(ctx, "c-1", ^uint64(0)); err != nil {
			t.Fatal(err)
		}
		want := []string{seqName(1, snapExt) + ".bak", seqName(1, tmpExt), "notes.txt"}
		if got := names(t, dir, d); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("snapshot directory = %q, want %q", got, want)
		}
	})

	t.Run("a snapshot of another id stays", func(t *testing.T) {
		s, _, _ := newJournal(t)
		mustSave(t, s, "c-1", 1)
		mustSave(t, s, "c-2", 1)
		if err := s.DeleteSnapshots(ctx, "c-1", 1); err != nil {
			t.Fatal(err)
		}
		mustLoad(t, s, "c-1", 0)
		mustLoad(t, s, "c-2", 1)
	})

	t.Run("failed removal", func(t *testing.T) {
		s, r, dir := newJournal(t)
		mustSave(t, s, "c-1", 1, 2, 3)
		boom := errors.New("injected")
		r.fail("Remove", snapName("c-1", 2), nth(1), boom)
		m := r.mark()
		err := s.DeleteSnapshots(ctx, "c-1", 3)
		if !errors.Is(err, boom) {
			t.Fatalf("DeleteSnapshots = %v, want the injected error", err)
		}
		if want := "filestore: delete snapshots of c-1 up to 3: "; !strings.HasPrefix(err.Error(), want) {
			t.Fatalf("error = %q, want prefix %q", err, want)
		}
		// The removal of 1 is made durable; nothing after the failure is tried.
		checkCalls(t, "failed delete", r.since(m), []string{
			"ReadDir " + d, "Remove " + snapName("c-1", 1), "Remove " + snapName("c-1", 2), "SyncDir " + d,
		})
		if got := len(names(t, dir, d)); got != 2 {
			t.Fatalf("%d snapshots left, want 2", got)
		}
		if err := s.DeleteSnapshots(ctx, "c-1", 3); err != nil {
			t.Fatal(err)
		}
		mustLoad(t, s, "c-1", 0)
	})

	t.Run("failed sync", func(t *testing.T) {
		s, r, _ := newJournal(t)
		mustSave(t, s, "c-1", 1)
		boom := errors.New("injected")
		r.fail("SyncDir", d, nth(1), boom)
		if err := s.DeleteSnapshots(ctx, "c-1", 1); !errors.Is(err, boom) {
			t.Fatalf("DeleteSnapshots = %v, want the injected error", err)
		}
		r.fail("ReadDir", d, nth(1), boom)
		if err := s.DeleteSnapshots(ctx, "c-1", 1); !errors.Is(err, boom) {
			t.Fatalf("DeleteSnapshots = %v, want the injected error", err)
		}
	})
}

// PER-330: snapshot methods close every file, also when they fail.
func TestPER330SnapshotFilesClosed(t *testing.T) {
	s, r, _ := newJournal(t)
	boom := errors.New("injected")
	r.fail("WriteAt", "", every(3), &partialWrite{n: 5, err: boom})
	r.fail("Sync", "", every(2), boom)
	r.fail("Close", "", every(5), boom)
	ctx := context.Background()
	saved := 0
	for i := range 30 {
		seq := uint64(i%4 + 1)
		if s.Save(ctx, "c-1", snap(seq)) == nil {
			saved++
		}
		r.checkClosed(t)
		_, _, _ = s.Load(ctx, "c-1")
		r.checkClosed(t)
		_ = s.DeleteSnapshots(ctx, "c-1", 1)
		r.checkClosed(t)
	}
	if saved == 0 {
		t.Fatal("no save succeeded")
	}
}
