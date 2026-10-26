// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/hmdsefi/lockstep/persist"
)

// segOf is the FS name of segment seq of id.
func segOf(id string, seq uint64) string {
	return journalDir + "/" + idDirName(id) + "/" + seqName(seq, segExt)
}

// ev returns one event per string, with Seq values first, first+1 and so on.
func ev(first uint64, data ...string) []persist.Event {
	events := make([]persist.Event, len(data))
	for i, d := range data {
		events[i] = persist.Event{Seq: first + uint64(i), Data: []byte(d)}
	}
	return events
}

// jrec returns the journal record of ev(first, data...).
func jrec(t *testing.T, first uint64, data ...string) []byte {
	t.Helper()
	rec, err := encodeJournalRecord(ev(first, data...))
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// flipped returns b with bit 0 of byte i inverted.
func flipped(b []byte, i int) []byte {
	c := bytes.Clone(b)
	c[i] ^= 0x01
	return c
}

// newJournal returns a Store on a recFS over a new temporary directory, and the directory.
func newJournal(t *testing.T, opts ...Option) (*Store, *recFS, string) {
	t.Helper()
	r, dir := newRecFS(t)
	t.Cleanup(func() { r.checkClosed(t) })
	return New(r, opts...), r, dir
}

func mustRecover(t *testing.T, s *Store, id string, want persist.Bounds) {
	t.Helper()
	got, err := s.Recover(context.Background(), id)
	if err != nil {
		t.Fatalf("Recover(%q): %v", id, err)
	}
	if got != want {
		t.Fatalf("Recover(%q) = %+v, want %+v", id, got, want)
	}
}

func mustWrite(t *testing.T, s *Store, id string, events []persist.Event) {
	t.Helper()
	if err := s.Write(context.Background(), id, events); err != nil {
		t.Fatalf("Write(%q, seq %d..): %v", id, events[0].Seq, err)
	}
}

func mustCommit(t *testing.T, s *Store, id string) {
	t.Helper()
	if err := s.Commit(context.Background(), id); err != nil {
		t.Fatalf("Commit(%q): %v", id, err)
	}
}

// fileSize returns the size of name below dir, or -1 if it does not exist.
func fileSize(t *testing.T, dir, name string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, filepath.FromSlash(name)))
	if errors.Is(err, fs.ErrNotExist) {
		return -1
	}
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func readFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func checkCalls(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: calls =\n%q\nwant\n%q", what, got, want)
	}
}

// committedOnReopen checks what a new Store on dir recovers for id.
func committedOnReopen(t *testing.T, dir, id string, want persist.Bounds) {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustRecover(t, s, id, want)
}

func TestATPER27IDDirectories(t *testing.T) {
	s, r, dir := newJournal(t)

	mustRecover(t, s, "Acct.1/%", persist.Bounds{First: 1})
	mustWrite(t, s, "Acct.1/%", ev(1, "x"))
	mustCommit(t, s, "Acct.1/%")
	if fileSize(t, dir, "journal/id-%41cct%2E1%2F%25/00000000000000000001.seg") <= 0 {
		t.Fatal("segment of \"Acct.1/%\" is not journal/id-%41cct%2E1%2F%25/00000000000000000001.seg")
	}

	for _, id := range []string{"a", "A"} {
		mustRecover(t, s, id, persist.Bounds{First: 1})
		mustWrite(t, s, id, ev(1, id))
		mustCommit(t, s, id)
	}
	if !bytes.Equal(readFile(t, dir, "journal/id-a/00000000000000000001.seg"), jrec(t, 1, "a")) ||
		!bytes.Equal(readFile(t, dir, "journal/id-%41/00000000000000000001.seg"), jrec(t, 1, "A")) {
		t.Fatal(`ids "a" and "A" share a directory`)
	}

	long := strings.Repeat("\xff", persist.MaxIDLen)
	mustWrite(t, s, long, ev(1, "x"))
	mustCommit(t, s, long)
	entries, err := os.ReadDir(filepath.Join(dir, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if e.Name() == "id-"+strings.Repeat("%FF", persist.MaxIDLen) {
			found = len(e.Name()) == 243
		}
	}
	if !found {
		t.Fatalf("no 243-byte directory for the 80-byte id among %v", entries)
	}

	m := r.mark()
	ctx := context.Background()
	for _, id := range []string{"", long + "x"} {
		want := fmt.Sprintf("filestore: id %q: persist: invalid persistence id", id)
		_, err := s.Recover(ctx, id)
		if !errors.Is(err, persist.ErrInvalidID) || err.Error() != want {
			t.Fatalf("Recover(%q) = %v, want %q", id, err, want)
		}
		if err := s.Write(ctx, id, ev(1, "x")); !errors.Is(err, persist.ErrInvalidID) {
			t.Fatalf("Write(%q) = %v, want ErrInvalidID", id, err)
		}
		if err := s.Commit(ctx, id); !errors.Is(err, persist.ErrInvalidID) {
			t.Fatalf("Commit(%q) = %v, want ErrInvalidID", id, err)
		}
	}
	checkCalls(t, "invalid ids", r.since(m), nil)
}

func TestATPER28RecordBytes(t *testing.T) {
	s, _, dir := newJournal(t)
	mustWrite(t, s, "c-1", []persist.Event{{Seq: 1, Data: []byte("ab")}, {Seq: 2, Data: []byte("c")}})
	mustCommit(t, s, "c-1")

	payload := cat(u64(1), u32(2), u32(2), []byte("ab"), u32(1), []byte("c"))
	want := cat([]byte{0x17, 0, 0, 0}, u32(crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))), payload)
	seg := segOf("c-1", 1)
	if got := readFile(t, dir, seg); len(got) != 31 || !bytes.Equal(got, want) {
		t.Fatalf("segment = % x (%d bytes)\nwant      % x", got, len(got), want)
	}

	big := []persist.Event{{Seq: 3, Data: make([]byte, MaxRecordSize-15)}}
	err := s.Write(context.Background(), "c-1", big)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize Write = %v, want ErrTooLarge", err)
	}
	if got := readFile(t, dir, seg); !bytes.Equal(got, want) {
		t.Fatalf("oversize Write changed the segment to %d bytes", len(got))
	}
	// The rejected batch changed nothing: seq 3 is still the next one.
	mustWrite(t, s, "c-1", ev(3, "d"))
	mustCommit(t, s, "c-1")
	committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 3})
}

func TestATPER31SegmentRollover(t *testing.T) {
	s, _, dir := newJournal(t, WithSegmentSize(64))
	for seq := uint64(1); seq <= 4; seq++ {
		events := ev(seq, "1234567")
		if rec, _ := encodeJournalRecord(events); len(rec) != 31 {
			t.Fatalf("record is %d bytes, want 31", len(rec))
		}
		mustWrite(t, s, "c-1", events)
	}
	mustCommit(t, s, "c-1")
	if got := fileSize(t, dir, segOf("c-1", 1)); got != 93 {
		t.Fatalf("seg(1) is %d bytes, want 93", got)
	}
	if got := fileSize(t, dir, segOf("c-1", 4)); got != 31 {
		t.Fatalf("seg(4) is %d bytes, want 31", got)
	}
	for _, seq := range []uint64{2, 3} {
		if fileSize(t, dir, segOf("c-1", seq)) != -1 {
			t.Fatalf("seg(%d) exists", seq)
		}
	}
	committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 4})
}

func TestATPER32CommitSyncOrder(t *testing.T) {
	seg1 := segOf("c-1", 1)
	firstCommit := []string{
		"Open " + seg1, "Sync " + seg1, "Close " + seg1,
		"SyncDir journal/id-c-1",
		"SyncDir journal",
	}

	t.Run("first and second commit", func(t *testing.T) {
		s, r, _ := newJournal(t)
		mustRecover(t, s, "c-1", persist.Bounds{First: 1})

		m := r.mark()
		mustWrite(t, s, "c-1", ev(1, "a"))
		checkCalls(t, "first write", r.since(m), []string{
			"MkdirAll journal/id-c-1",
			"Create " + seg1, "WriteAt " + seg1 + " " + string(jrec(t, 1, "a")), "Close " + seg1,
		})

		m = r.mark()
		mustCommit(t, s, "c-1")
		checkCalls(t, "first commit", r.since(m), firstCommit)

		mustWrite(t, s, "c-1", ev(2, "b"))
		m = r.mark()
		mustCommit(t, s, "c-1")
		checkCalls(t, "second commit", r.since(m), []string{"Open " + seg1, "Sync " + seg1, "Close " + seg1})

		m = r.mark()
		mustCommit(t, s, "c-1")
		checkCalls(t, "commit with nothing written", r.since(m), nil)
	})

	t.Run("commit without a write performs no I/O", func(t *testing.T) {
		s, r, _ := newJournal(t)
		mustRecover(t, s, "c-1", persist.Bounds{First: 1})
		m := r.mark()
		mustCommit(t, s, "c-1")
		checkCalls(t, "commit", r.since(m), nil)
	})

	// A failed commit keeps the created flags: the next successful commit syncs the directories
	// again, or the segment's entry could vanish in a crash.
	for _, failing := range []string{"journal/id-c-1", "journal"} {
		t.Run("after a failed SyncDir "+failing, func(t *testing.T) {
			s, r, dir := newJournal(t)
			boom := errors.New("injected SyncDir failure")
			r.fail("SyncDir", failing, nth(1), boom)
			mustWrite(t, s, "c-1", ev(1, "a"))
			err := s.Commit(context.Background(), "c-1")
			if !errors.Is(err, boom) || err.Error() != "filestore: commit c-1: injected SyncDir failure" {
				t.Fatalf("Commit = %v, want the SyncDir error", err)
			}
			if got := fileSize(t, dir, seg1); got != 0 {
				t.Fatalf("seg(1) is %d bytes after the repair, want 0", got)
			}
			mustWrite(t, s, "c-1", ev(1, "a"))
			m := r.mark()
			mustCommit(t, s, "c-1")
			checkCalls(t, "commit after the failed one", r.since(m), firstCommit)
			committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 1})
		})
	}
}

func TestATPER34UnusableAfterFailedRepair(t *testing.T) {
	ctx := context.Background()
	syncErr := errors.New("injected sync failure")
	truncErr := errors.New("injected truncate failure")
	const unusable = "filestore: c-1: filestore: persistence id unusable after a failed repair: injected truncate failure"

	checkUnusable := func(t *testing.T, s *Store, r *recFS) {
		t.Helper()
		m := r.mark()
		_, rerr := s.Recover(ctx, "c-1")
		werr := s.Write(ctx, "c-1", ev(1, "z"))
		cerr := s.Commit(ctx, "c-1")
		for i, err := range []error{rerr, werr, cerr, s.Write(ctx, "c-1", nil)} {
			if !errors.Is(err, ErrUnusable) || !errors.Is(err, truncErr) || err.Error() != unusable {
				t.Fatalf("call %d after the failed repair = %v, want %q", i, err, unusable)
			}
		}
		checkCalls(t, "calls for an unusable id", r.since(m), nil)
	}
	checkOtherID := func(t *testing.T, s *Store, dir string) {
		t.Helper()
		mustRecover(t, s, "c-2", persist.Bounds{First: 1})
		mustWrite(t, s, "c-2", ev(1, "a"))
		mustCommit(t, s, "c-2")
		if fileSize(t, dir, segOf("c-2", 1)) <= 0 {
			t.Fatal("c-2 has no segment")
		}
		committedOnReopen(t, dir, "c-2", persist.Bounds{First: 1, Last: 1})
	}

	t.Run("failed commit", func(t *testing.T) {
		s, r, dir := newJournal(t)
		r.fail("Sync", segOf("c-1", 1), always, syncErr)
		r.fail("Truncate", segOf("c-1", 1), always, truncErr)
		mustRecover(t, s, "c-1", persist.Bounds{First: 1})
		mustWrite(t, s, "c-1", ev(1, "a"))
		err := s.Commit(ctx, "c-1")
		if !errors.Is(err, syncErr) || err.Error() != "filestore: commit c-1: injected sync failure" {
			t.Fatalf("Commit = %v, want the sync error", err)
		}
		checkUnusable(t, s, r)
		checkOtherID(t, s, dir)
	})

	t.Run("failed write", func(t *testing.T) {
		s, r, dir := newJournal(t)
		writeErr := errors.New("injected write failure")
		r.fail("WriteAt", segOf("c-1", 1), always, &partialWrite{n: 5, err: writeErr})
		r.fail("Truncate", segOf("c-1", 1), always, truncErr)
		err := s.Write(ctx, "c-1", ev(1, "a"))
		if !errors.Is(err, writeErr) || err.Error() != "filestore: write c-1 seq 1..1: injected write failure" {
			t.Fatalf("Write = %v, want the write error", err)
		}
		checkUnusable(t, s, r)
		checkOtherID(t, s, dir)
	})
}

func TestPER340SeqConflict(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	{
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		mustWrite(t, s, "c-1", ev(1, "a")) // a Write before any Recover loads the state itself
		mustCommit(t, s, "c-1")
	}
	r := &recFS{FS: &osFS{root: dir}}
	t.Cleanup(func() { r.checkClosed(t) })
	s := New(r)
	mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 1})
	seg := readFile(t, dir, segOf("c-1", 1))

	m := r.mark()
	for _, tc := range []struct {
		events []persist.Event
		want   string
	}{
		{ev(3, "c"), "filestore: write c-1: seq 3 at index 0, expected 2: persist: sequence conflict"},
		{ev(1, "a"), "filestore: write c-1: seq 1 at index 0, expected 2: persist: sequence conflict"},
		{[]persist.Event{{Seq: 2}, {Seq: 4}}, "filestore: write c-1: seq 4 at index 1, expected 3: persist: sequence conflict"},
	} {
		err := s.Write(ctx, "c-1", tc.events)
		if !errors.Is(err, persist.ErrSeqConflict) || err.Error() != tc.want {
			t.Fatalf("Write = %v, want %q", err, tc.want)
		}
	}
	for _, events := range [][]persist.Event{nil, {}} {
		err := s.Write(ctx, "c-1", events)
		if !errors.Is(err, fs.ErrInvalid) || err.Error() != "filestore: write c-1: empty batch: invalid argument" {
			t.Fatalf("Write of an empty batch = %v, want fs.ErrInvalid", err)
		}
	}

	done, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Recover(done, "c-1"); err != context.Canceled { //nolint:errorlint // ctx.Err() itself
		t.Fatalf("Recover with a done ctx = %v", err)
	}
	if err := s.Write(done, "c-1", ev(2, "b")); err != context.Canceled { //nolint:errorlint // ctx.Err() itself
		t.Fatalf("Write with a done ctx = %v", err)
	}
	if err := s.Commit(done, "c-1"); err != context.Canceled { //nolint:errorlint // ctx.Err() itself
		t.Fatalf("Commit with a done ctx = %v", err)
	}
	// A done ctx is checked before the id and before initialization.
	fresh, fr, _ := newJournal(t)
	if err := fresh.Write(done, "", ev(1, "a")); err != context.Canceled { //nolint:errorlint // ctx.Err() itself
		t.Fatalf("Write with a done ctx and an invalid id = %v", err)
	}
	checkCalls(t, "fresh store with a done ctx", fr.calls(), nil)

	checkCalls(t, "rejected calls", r.since(m), nil)
	if got := readFile(t, dir, segOf("c-1", 1)); !bytes.Equal(got, seg) {
		t.Fatal("a rejected call changed the segment")
	}

	// Nothing rejected changed the next sequence number.
	mustWrite(t, s, "c-1", ev(2, "b"))
	mustCommit(t, s, "c-1")
	committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 2})
}

func TestPER360WriteFailureRepair(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("injected")
	seg1, seg2, seg3 := segOf("c-1", 1), segOf("c-1", 2), segOf("c-1", 3)
	r1 := jrec(t, 1, "a")

	writeFails := func(t *testing.T, s *Store, events []persist.Event, want string) {
		t.Helper()
		err := s.Write(ctx, "c-1", events)
		if !errors.Is(err, boom) || err.Error() != want {
			t.Fatalf("Write = %v, want %q", err, want)
		}
	}

	t.Run("failed WriteAt on an existing segment", func(t *testing.T) {
		s, r, dir := newJournal(t)
		mustWrite(t, s, "c-1", ev(1, "a"))
		mustCommit(t, s, "c-1")
		mustWrite(t, s, "c-1", ev(2, "b")) // written, not committed: the repair keeps it
		r.fail("WriteAt", seg1, nth(1), &partialWrite{n: 10, err: boom})
		m := r.mark()
		writeFails(t, s, ev(3, "c", "d"), "filestore: write c-1 seq 3..4: injected")
		size := int64(2 * len(r1))
		calls := r.since(m)
		if last := calls[len(calls)-3:]; !reflect.DeepEqual(last, []string{
			"Open " + seg1, fmt.Sprintf("Truncate %s %d", seg1, size), "Close " + seg1,
		}) {
			t.Fatalf("repair calls = %q", last)
		}
		if got := fileSize(t, dir, seg1); got != size {
			t.Fatalf("seg(1) is %d bytes, want %d", got, size)
		}
		mustWrite(t, s, "c-1", ev(3, "c"))
		mustCommit(t, s, "c-1")
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 3})
	})

	t.Run("failed WriteAt on a new segment", func(t *testing.T) {
		s, r, dir := newJournal(t, WithSegmentSize(1))
		mustWrite(t, s, "c-1", ev(1, "a"))
		mustCommit(t, s, "c-1")
		r.fail("WriteAt", seg2, nth(1), &partialWrite{n: 10, err: boom})
		writeFails(t, s, ev(2, "b"), "filestore: write c-1 seq 2..2: injected")
		if got := fileSize(t, dir, seg2); got != 0 {
			t.Fatalf("seg(2) is %d bytes, want 0", got)
		}
		// The empty segment is the last one; its name is the next seq, so the next Write uses it.
		m := r.mark()
		mustWrite(t, s, "c-1", ev(2, "b"))
		if got := r.since(m)[0]; got != "Open "+seg2 {
			t.Fatalf("next Write starts with %q, want it to open seg(2)", got)
		}
		mustCommit(t, s, "c-1")
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 2})
	})

	t.Run("failed Close after a write", func(t *testing.T) {
		s, r, dir := newJournal(t)
		r.fail("Close", seg1, nth(2), boom) // the Close of the second Write
		mustWrite(t, s, "c-1", ev(1, "a"))
		writeFails(t, s, ev(2, "b"), "filestore: write c-1 seq 2..2: injected")
		if got := fileSize(t, dir, seg1); got != int64(len(r1)) {
			t.Fatalf("seg(1) is %d bytes, want %d", got, len(r1))
		}
		mustWrite(t, s, "c-1", ev(2, "b"))
		mustCommit(t, s, "c-1")
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 2})
	})

	t.Run("failed Open writes nothing", func(t *testing.T) {
		s, r, dir := newJournal(t)
		mustWrite(t, s, "c-1", ev(1, "a"))
		r.fail("Open", seg1, nth(1), boom)
		writeFails(t, s, ev(2, "b"), "filestore: write c-1 seq 2..2: injected")
		mustWrite(t, s, "c-1", ev(2, "b"))
		mustCommit(t, s, "c-1")
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 2})
	})

	t.Run("failed commit", func(t *testing.T) {
		s, r, dir := newJournal(t, WithSegmentSize(40))
		mustWrite(t, s, "c-1", ev(1, "a"))
		mustCommit(t, s, "c-1")
		mustWrite(t, s, "c-1", ev(2, "b")) // seg(1): 2 records
		mustWrite(t, s, "c-1", ev(3, "c")) // seg(1) is full: creates seg(3)
		if fileSize(t, dir, seg3) <= 0 {
			t.Fatal("seg(3) not written")
		}
		r.fail("Sync", seg3, nth(1), boom)
		err := s.Commit(ctx, "c-1")
		if !errors.Is(err, boom) || err.Error() != "filestore: commit c-1: injected" {
			t.Fatalf("Commit = %v", err)
		}
		if got := fileSize(t, dir, seg1); got != int64(len(r1)) {
			t.Fatalf("seg(1) is %d bytes after the repair, want %d", got, len(r1))
		}
		// seg(3) holds no committed event and its name is not the next seq: left empty as the
		// last segment, a scan would take seq 2 for committed.
		if got := fileSize(t, dir, seg3); got != -1 {
			t.Fatalf("seg(3) is %d bytes after the repair, want it removed", got)
		}
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 1})

		// The last written seq is the last committed one again.
		err = s.Write(ctx, "c-1", ev(3, "c"))
		if !errors.Is(err, persist.ErrSeqConflict) || !strings.Contains(err.Error(), "expected 2") {
			t.Fatalf("Write of seq 3 after the failed commit = %v, want a conflict expecting 2", err)
		}
		mustWrite(t, s, "c-1", ev(2, "b"))
		m := r.mark()
		mustCommit(t, s, "c-1")
		// seg(1) was truncated and rewritten; seg(3)'s removal is made durable.
		checkCalls(t, "commit after the repair", r.since(m), []string{
			"Open " + seg1, "Sync " + seg1, "Close " + seg1, "SyncDir journal/id-c-1",
		})
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 2})
	})

	t.Run("failed commit of a new segment", func(t *testing.T) {
		s, r, dir := newJournal(t, WithSegmentSize(1))
		mustWrite(t, s, "c-1", ev(1, "a"))
		mustCommit(t, s, "c-1")
		mustWrite(t, s, "c-1", ev(2, "b"))
		r.fail("Sync", seg2, nth(1), boom)
		if err := s.Commit(ctx, "c-1"); !errors.Is(err, boom) {
			t.Fatalf("Commit = %v", err)
		}
		// The truncated segment stays, empty, as the last one: a scan gives L = name-1.
		if got := fileSize(t, dir, seg2); got != 0 {
			t.Fatalf("seg(2) is %d bytes, want 0", got)
		}
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 1})

		m := r.mark()
		mustWrite(t, s, "c-1", ev(2, "b"))
		checkCalls(t, "next write", r.since(m), []string{
			"Open " + seg2, "WriteAt " + seg2 + " " + string(jrec(t, 2, "b")), "Close " + seg2,
		})
		m = r.mark()
		mustCommit(t, s, "c-1")
		checkCalls(t, "next commit", r.since(m), []string{
			"Open " + seg2, "Sync " + seg2, "Close " + seg2, "SyncDir journal/id-c-1",
		})
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 2})
	})

	t.Run("failed create", func(t *testing.T) {
		s, r, dir := newJournal(t)
		r.fail("Create", seg1, nth(1), boom)
		writeFails(t, s, ev(1, "a"), "filestore: write c-1 seq 1..1: injected")
		mustWrite(t, s, "c-1", ev(1, "a"))
		m := r.mark()
		mustCommit(t, s, "c-1")
		checkCalls(t, "commit", r.since(m), []string{
			"Open " + seg1, "Sync " + seg1, "Close " + seg1, "SyncDir journal/id-c-1", "SyncDir journal",
		})
		committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: 1})
	})
}

func TestPER370RecoverScan(t *testing.T) {
	r1 := jrec(t, 1, "a", "b") // seqs 1-2
	r3 := jrec(t, 3, "c")      // seq 3
	r4 := jrec(t, 4, "dd")     // seq 4
	header := func(l uint32) []byte { return cat(u32(l), u32(0)) }
	countZero := frame(cat(u64(3), u32(0), u32(0)))
	name := func(seq uint64) string { return seqName(seq, segExt) }
	corrupt := func(seq uint64, off int, reason string) string {
		return fmt.Sprintf("filestore: corrupt %s at offset %d: %s: filestore: corrupt data", segOf("c-1", seq), off, reason)
	}

	tests := []struct {
		name    string
		noDir   bool
		files   map[string][]byte
		want    persist.Bounds
		wantErr string
		cut     int64 // size of the last segment after a torn-tail repair; 0: no repair
	}{
		{name: "no directory", noDir: true, want: persist.Bounds{First: 1}},
		{name: "empty directory", want: persist.Bounds{First: 1}},
		{name: "one segment", files: map[string][]byte{name(1): cat(r1, r3)}, want: persist.Bounds{First: 1, Last: 3}},
		{name: "two segments", files: map[string][]byte{name(1): cat(r1, r3), name(4): r4}, want: persist.Bounds{First: 1, Last: 4}},
		{name: "empty only segment", files: map[string][]byte{name(1): nil}, want: persist.Bounds{First: 1}},
		{name: "empty only segment after deletions", files: map[string][]byte{name(5): nil}, want: persist.Bounds{First: 5, Last: 4}},
		{name: "empty last segment", files: map[string][]byte{name(1): cat(r1, r3), name(4): nil}, want: persist.Bounds{First: 1, Last: 3}},
		{
			name: "other names are ignored",
			files: map[string][]byte{
				name(1): r1, "foo": []byte("x"), "1.seg": r3, name(3) + ".tmp": r3,
				"00000000000000000000.seg": r3, "0000000000000000000a.seg": r3, name(9) + "/": nil,
			},
			want: persist.Bounds{First: 1, Last: 2},
		},

		// Torn tails: truncated and reported.
		{name: "cut last record", files: map[string][]byte{name(1): cat(r1, r3[:len(r3)-4])},
			want: persist.Bounds{First: 1, Last: 2, Repaired: int64(len(r3) - 4)}, cut: int64(len(r1))},
		{name: "cut header of the last record", files: map[string][]byte{name(1): cat(r1, r3[:3])},
			want: persist.Bounds{First: 1, Last: 2, Repaired: 3}, cut: int64(len(r1))},
		{name: "header only", files: map[string][]byte{name(1): cat(r1, r3[:8])},
			want: persist.Bounds{First: 1, Last: 2, Repaired: 8}, cut: int64(len(r1))},
		{name: "bad checksum in the last record", files: map[string][]byte{name(1): cat(r1, flipped(r3, 12))},
			want: persist.Bounds{First: 1, Last: 2, Repaired: int64(len(r3))}, cut: int64(len(r1))},
		{name: "bad length in the last record", files: map[string][]byte{name(1): cat(r1, header(MaxRecordSize+1), r3)},
			want: persist.Bounds{First: 1, Last: 2, Repaired: int64(8 + len(r3))}, cut: int64(len(r1))},
		{name: "invalid record followed by bytes that are no record", files: map[string][]byte{name(1): cat(r1, flipped(r3, 12), []byte("garbage-garbage-garbage"))},
			want: persist.Bounds{First: 1, Last: 2, Repaired: int64(len(r3) + 23)}, cut: int64(len(r1))},
		{name: "invalid record followed by a cut record", files: map[string][]byte{name(1): cat(flipped(r1, 12), r3[:len(r3)-1])},
			want: persist.Bounds{First: 1, Last: 0, Repaired: int64(len(r1) + len(r3) - 1)}, cut: -1},
		{name: "torn first record of the last segment", files: map[string][]byte{name(1): cat(r1, r3), name(4): r4[:10]},
			want: persist.Bounds{First: 1, Last: 3, Repaired: 10}, cut: -1},

		// Corruption: reported, nothing changed.
		{name: "bad checksum followed by a valid record", files: map[string][]byte{name(1): cat(flipped(r1, 10), r3)},
			wantErr: corrupt(1, 0, "checksum mismatch")},
		{name: "malformed payload followed by a valid record", files: map[string][]byte{name(1): cat(r1, countZero, jrec(t, 3, "c"))},
			wantErr: corrupt(1, len(r1), "malformed payload")},
		{name: "seq gap", files: map[string][]byte{name(1): cat(r1, r4)},
			wantErr: corrupt(1, len(r1), "seq 4, expected 3")},
		{name: "seq gap in the last record", files: map[string][]byte{name(1): cat(r1, r3, jrec(t, 9, "x"))},
			wantErr: corrupt(1, len(r1)+len(r3), "seq 9, expected 4")},
		{name: "last segment named after another seq", files: map[string][]byte{name(5): r1},
			wantErr: corrupt(5, 0, "seq 1, expected 5")},
		{name: "first segment: bad length", files: map[string][]byte{name(1): cat(header(5), r1), name(3): r3},
			wantErr: corrupt(1, 0, "bad length 5")},
		{name: "first segment: incomplete record", files: map[string][]byte{name(1): r1[:20], name(3): r3},
			wantErr: corrupt(1, 0, "incomplete record")},
		{name: "first segment: incomplete header", files: map[string][]byte{name(1): r1[:5], name(3): r3},
			wantErr: corrupt(1, 0, "incomplete record")},
		{name: "first segment: empty", files: map[string][]byte{name(1): nil, name(3): r3},
			wantErr: corrupt(1, 0, "incomplete record")},
		{name: "first segment: bad checksum", files: map[string][]byte{name(1): flipped(r1, 20), name(3): r3},
			wantErr: corrupt(1, 0, "checksum mismatch")},
		{name: "first segment: malformed payload", files: map[string][]byte{name(1): frame(cat(u64(1), u32(0), u32(0))), name(3): r3},
			wantErr: corrupt(1, 0, "malformed payload")},
		{name: "first segment: seq differs from its name", files: map[string][]byte{name(2): r1, name(3): r3},
			wantErr: corrupt(2, 0, "seq 1, expected 2")},
		// The first segment's later records are read by Read, not by the scan.
		{name: "first segment: only the first record is checked", files: map[string][]byte{name(1): cat(r1, []byte("junk")), name(3): r3},
			want: persist.Bounds{First: 1, Last: 3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, r, dir := newJournal(t)
			idDir := filepath.Join(dir, "journal", "id-c-1")
			if !tc.noDir {
				if err := os.MkdirAll(idDir, 0o750); err != nil {
					t.Fatal(err)
				}
			}
			last := ""
			for n, b := range tc.files {
				if strings.HasSuffix(n, "/") {
					if err := os.Mkdir(filepath.Join(idDir, n), 0o750); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := os.WriteFile(filepath.Join(idDir, n), b, 0o600); err != nil {
					t.Fatal(err)
				}
				if _, ok := parseSeqName(n, segExt); ok && n > last && n != "00000000000000000000.seg" {
					last = n
				}
			}
			ctx := context.Background()
			got, err := s.Recover(ctx, "c-1")

			if tc.wantErr != "" {
				if !errors.Is(err, ErrCorrupt) || err.Error() != tc.wantErr {
					t.Fatalf("Recover = %+v, %v; want %q", got, err, tc.wantErr)
				}
				for n, b := range tc.files {
					if !strings.HasSuffix(n, "/") && !bytes.Equal(readFile(t, dir, "journal/id-c-1/"+n), b) {
						t.Fatalf("%s changed", n)
					}
				}
				// Corruption is reported again, by every method that needs the state.
				if _, err := s.Recover(ctx, "c-1"); err == nil || err.Error() != tc.wantErr {
					t.Fatalf("second Recover = %v", err)
				}
				if err := s.Write(ctx, "c-1", ev(1, "x")); err == nil || err.Error() != tc.wantErr {
					t.Fatalf("Write = %v", err)
				}
				return
			}

			if err != nil || got != tc.want {
				t.Fatalf("Recover = %+v, %v; want %+v", got, err, tc.want)
			}
			if tc.cut != 0 {
				lastSeg := "journal/id-c-1/" + last
				want := max(tc.cut, 0)
				if size := fileSize(t, dir, lastSeg); size != want {
					t.Fatalf("%s is %d bytes after the repair, want %d", last, size, want)
				}
				calls := strings.Join(r.calls(), "\n")
				repair := fmt.Sprintf("Truncate %s %d\nSync %s\nClose %s", lastSeg, want, lastSeg, lastSeg)
				if !strings.Contains(calls, repair) {
					t.Fatalf("calls do not truncate and sync the segment:\n%s", calls)
				}
			} else if strings.Contains(strings.Join(r.calls(), "\n"), "Truncate") {
				t.Fatalf("Recover truncated a file: %q", r.calls())
			}

			// The next Write continues at Last+1, and a new Store recovers it.
			next := tc.want.Last + 1
			mustWrite(t, s, "c-1", ev(next, "n"))
			mustCommit(t, s, "c-1")
			committedOnReopen(t, dir, "c-1", persist.Bounds{First: tc.want.First, Last: next})
		})
	}

	t.Run("recover of a loaded id commits its written data first", func(t *testing.T) {
		s, r, _ := newJournal(t)
		mustWrite(t, s, "c-1", ev(1, "a"))
		m := r.mark()
		mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 1})
		seg1 := segOf("c-1", 1)
		calls := r.since(m)
		if want := []string{"Open " + seg1, "Sync " + seg1, "Close " + seg1, "SyncDir journal/id-c-1", "SyncDir journal", "ReadDir journal/id-c-1"}; !reflect.DeepEqual(calls[:len(want)], want) {
			t.Fatalf("calls = %q, want them to start with %q", calls, want)
		}
	})

	t.Run("recover of a loaded id returns a failed commit after the repair", func(t *testing.T) {
		s, r, dir := newJournal(t)
		boom := errors.New("injected")
		mustWrite(t, s, "c-1", ev(1, "a"))
		mustCommit(t, s, "c-1")
		mustWrite(t, s, "c-1", ev(2, "b"))
		r.fail("Sync", segOf("c-1", 1), nth(1), boom)
		_, err := s.Recover(context.Background(), "c-1")
		if !errors.Is(err, boom) || err.Error() != "filestore: commit c-1: injected" {
			t.Fatalf("Recover = %v, want the commit error", err)
		}
		if got := fileSize(t, dir, segOf("c-1", 1)); got != int64(len(jrec(t, 1, "a"))) {
			t.Fatalf("seg(1) is %d bytes, want the committed size", got)
		}
		mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 1})
	})

	t.Run("failed repair of a torn tail", func(t *testing.T) {
		s, r, dir := newJournal(t)
		boom := errors.New("injected")
		if err := os.MkdirAll(filepath.Join(dir, "journal", "id-c-1"), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(segOf("c-1", 1))), cat(r1, r3[:5]), 0o600); err != nil {
			t.Fatal(err)
		}
		r.fail("Sync", segOf("c-1", 1), nth(1), boom)
		if _, err := s.Recover(context.Background(), "c-1"); !errors.Is(err, boom) {
			t.Fatalf("Recover = %v, want the sync error", err)
		}
		mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 2}) // truncated already, nothing to repair
	})
}

// TestJournalConcurrentIDs runs calls for many IDs at once, and concurrent calls for one ID.
func TestJournalConcurrentIDs(t *testing.T) {
	s, _, dir := newJournal(t, WithSegmentSize(100))
	const ids, batches = 8, 20
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("c-%d", i)
			ctx := context.Background()
			if _, err := s.Recover(ctx, id); err != nil {
				t.Error(err)
				return
			}
			for seq := uint64(1); seq <= batches; seq++ {
				if err := s.Write(ctx, id, ev(seq, id)); err != nil {
					t.Error(err)
					return
				}
				if err := s.Commit(ctx, id); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		wg.Add(1)
		go func() { // contends with the writer of c-0 for its lock
			defer wg.Done()
			for range batches {
				if err := s.Commit(context.Background(), "c-0"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for i := range ids {
		committedOnReopen(t, dir, fmt.Sprintf("c-%d", i), persist.Bounds{First: 1, Last: batches})
	}
}

// TestPER330FilesClosedOnFailures runs writes, commits and recoveries while the FS fails every
// fourth WriteAt and every third Sync, and checks that no call leaves a file open.
func TestPER330FilesClosedOnFailures(t *testing.T) {
	s, r, dir := newJournal(t, WithSegmentSize(64))
	boom := errors.New("injected")
	r.fail("WriteAt", "", every(4), &partialWrite{n: 9, err: boom})
	r.fail("Sync", "", every(3), boom)
	ctx := context.Background()
	committed := uint64(0)
	for range 30 {
		b, err := s.Recover(ctx, "c-1")
		if err != nil {
			r.checkClosed(t)
			continue
		}
		if b.Last < committed {
			t.Fatalf("Recover = %+v after seq %d was committed", b, committed)
		}
		next := b.Last + 1
		if err := s.Write(ctx, "c-1", ev(next, "abc", "de")); err == nil {
			if err := s.Commit(ctx, "c-1"); err == nil {
				committed = next + 1
			}
		}
		r.checkClosed(t)
	}
	if committed == 0 {
		t.Fatal("no commit succeeded")
	}
	committedOnReopen(t, dir, "c-1", persist.Bounds{First: 1, Last: committed})
}

// syncCalls returns the Sync and SyncDir calls among calls.
func syncCalls(calls []string) []string {
	var syncs []string
	for _, c := range calls {
		if strings.HasPrefix(c, "Sync ") || strings.HasPrefix(c, "SyncDir ") {
			syncs = append(syncs, c)
		}
	}
	return syncs
}

// TestRecoverSyncsWhatItReports checks that Recover of an ID that is not loaded makes the events
// it reports durable: an earlier process may have written them and never synced them.
func TestRecoverSyncsWhatItReports(t *testing.T) {
	ctx := context.Background()
	seg1, seg3 := segOf("c-1", 1), segOf("c-1", 3)
	r1, r3 := jrec(t, 1, "a", "b"), jrec(t, 3, "c")

	// fresh returns a Store on a recFS over a directory holding the given segments of c-1.
	fresh := func(t *testing.T, segs map[string][]byte) (*Store, *recFS, string) {
		t.Helper()
		s, r, dir := newJournal(t)
		for name, b := range segs {
			p := filepath.Join(dir, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, b, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return s, r, dir
	}
	wantSyncs := []string{"Sync " + seg3, "SyncDir journal/id-c-1", "SyncDir journal"}

	t.Run("fresh store", func(t *testing.T) {
		s, r, _ := fresh(t, map[string][]byte{seg1: r1, seg3: r3})
		mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 3})
		calls := r.calls()
		checkCalls(t, "syncs", syncCalls(calls), append([]string{"Sync FORMAT.tmp", "SyncDir ."}, wantSyncs...))
		// The last segment is synced after it was read, and before the directories.
		joined := strings.Join(calls, "\n")
		if !strings.Contains(joined, fmt.Sprintf("ReadAt %s 0 %d\nSync %s\nClose %s\nSyncDir journal/id-c-1\nSyncDir journal", seg3, len(r3), seg3, seg3)) {
			t.Fatalf("calls =\n%s", joined)
		}

		// Recover of the loaded ID with nothing written performs no sync.
		m := r.mark()
		mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 3})
		checkCalls(t, "syncs of a loaded id", syncCalls(r.since(m)), nil)
	})

	t.Run("written by this store, committed", func(t *testing.T) {
		s, r, _ := newJournal(t)
		mustWrite(t, s, "c-1", ev(1, "a"))
		mustCommit(t, s, "c-1")
		m := r.mark()
		mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 1})
		checkCalls(t, "syncs", syncCalls(r.since(m)), nil)
	})

	t.Run("torn tail is synced once", func(t *testing.T) {
		s, r, _ := fresh(t, map[string][]byte{seg1: r1, seg3: cat(r3, r3[:5])})
		mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 3, Repaired: 5})
		checkCalls(t, "syncs", syncCalls(r.calls())[2:], wantSyncs)
	})

	t.Run("a Write that loads the state syncs first", func(t *testing.T) {
		s, r, _ := fresh(t, map[string][]byte{seg1: r1})
		mustWrite(t, s, "c-1", ev(3, "c"))
		checkCalls(t, "syncs", syncCalls(r.calls())[2:], []string{"Sync " + seg1, "SyncDir journal/id-c-1", "SyncDir journal"})
	})

	for _, tc := range []struct {
		name string
		segs map[string][]byte
	}{
		{"missing journal", nil},
		{"empty directory", map[string][]byte{"journal/id-c-1/foo": nil}},
		{"empty last segment", map[string][]byte{seg1: r1, seg3: nil}},
		{"torn only record", map[string][]byte{seg1: r1[:7]}},
	} {
		t.Run("no record to sync: "+tc.name, func(t *testing.T) {
			s, r, _ := fresh(t, tc.segs)
			if _, err := s.Recover(ctx, "c-1"); err != nil {
				t.Fatal(err)
			}
			syncs := syncCalls(r.calls())[2:] // after Sync FORMAT.tmp and SyncDir . of the initialization
			if tc.name == "torn only record" {
				checkCalls(t, "syncs", syncs, []string{"Sync " + seg1})
				return
			}
			checkCalls(t, "syncs", syncs, nil)
		})
	}

	for _, failing := range []struct{ method, name string }{
		{"Sync", seg3}, {"SyncDir", "journal/id-c-1"}, {"SyncDir", "journal"},
	} {
		t.Run("failing "+failing.method+" "+failing.name, func(t *testing.T) {
			s, r, _ := fresh(t, map[string][]byte{seg1: r1, seg3: r3})
			boom := errors.New("injected")
			r.fail(failing.method, failing.name, nth(1), boom)
			_, err := s.Recover(ctx, "c-1")
			if !errors.Is(err, boom) || err.Error() != "filestore: recover c-1: injected" {
				t.Fatalf("Recover = %v, want the %s error", err, failing.method)
			}
			// The state was not loaded: the next Recover scans and syncs again.
			m := r.mark()
			mustRecover(t, s, "c-1", persist.Bounds{First: 1, Last: 3})
			calls := r.since(m)
			if calls[0] != "ReadDir journal/id-c-1" {
				t.Fatalf("next Recover starts with %q, want a scan", calls[0])
			}
			checkCalls(t, "syncs of the next Recover", syncCalls(calls), wantSyncs)
		})
	}
}
