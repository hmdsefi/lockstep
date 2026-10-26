// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/persist"
)

// seqsOf returns the sequence numbers of events.
func seqsOf(events []persist.Event) []uint64 {
	seqs := make([]uint64, len(events))
	for i, e := range events {
		seqs[i] = e.Seq
	}
	return seqs
}

// mustRead reads and checks the sequence numbers and data of the result: the event with
// sequence number n carries the data "e<n>".
func mustRead(t *testing.T, s *Store, id string, from uint64, max int, want ...uint64) {
	t.Helper()
	got, err := s.Read(context.Background(), id, from, max)
	if err != nil {
		t.Fatalf("Read(%q, %d, %d): %v", id, from, max, err)
	}
	if fmt.Sprint(seqsOf(got)) != fmt.Sprint(want) {
		t.Fatalf("Read(%q, %d, %d) seqs = %v, want %v", id, from, max, seqsOf(got), want)
	}
	for _, e := range got {
		if string(e.Data) != fmt.Sprintf("e%d", e.Seq) {
			t.Fatalf("Read(%q, %d, %d): event %d has data %q", id, from, max, e.Seq, e.Data)
		}
	}
}

// writeFive writes events 1..5 of "c-1", one per segment with WithSegmentSize(1), and commits.
func writeFive(t *testing.T, s *Store) {
	t.Helper()
	mustRecover(t, s, "c-1", persist.Bounds{First: 1})
	for i := uint64(1); i <= 5; i++ {
		mustWrite(t, s, "c-1", ev(i, fmt.Sprintf("e%d", i)))
	}
	mustCommit(t, s, "c-1")
}

// corruptReason checks that err wraps ErrCorrupt and names the reason.
func corruptReason(t *testing.T, err error, reason string) {
	t.Helper()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want one wrapping ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), ": "+reason+": ") {
		t.Fatalf("error = %q, want reason %q", err, reason)
	}
}

// AT-PER-37
func TestATPER37Read(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithSegmentSize(1))
	if err != nil {
		t.Fatal(err)
	}
	writeFive(t, s)

	mustRead(t, s, "c-1", 2, 3, 2, 3, 4)

	// Bytes appended behind the Store: a fresh Store sees them.
	f, err := os.OpenFile(filepath.Join(dir, filepath.FromSlash(segOf("c-1", 2))), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, WithSegmentSize(1))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Read(context.Background(), "c-1", 1, 5)
	corruptReason(t, err, "trailing bytes")
	// Reads that stop before the damaged segment ends are not affected.
	mustRead(t, s, "c-1", 1, 1, 1)
	mustRead(t, s, "c-1", 3, 3, 3, 4, 5)

	_, err = s.Read(context.Background(), "c-1", 1, 0)
	if !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Read with max 0: %v, want one wrapping fs.ErrInvalid", err)
	}
	_, err = s.Read(context.Background(), "c-1", 1, -3)
	if !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Read with max -3: %v, want one wrapping fs.ErrInvalid", err)
	}
}

// AT-PER-38
func TestATPER38DeleteEvents(t *testing.T) {
	s, r, dir := newJournal(t, WithSegmentSize(1))
	writeFive(t, s)

	m := r.mark()
	if err := s.DeleteEvents(context.Background(), "c-1", 3); err != nil {
		t.Fatal(err)
	}
	checkCalls(t, "DeleteEvents(3)", r.since(m), []string{
		"Remove " + segOf("c-1", 1),
		"Remove " + segOf("c-1", 2),
		"Remove " + segOf("c-1", 3),
		"SyncDir journal/id-c-1",
	})
	for seq := uint64(1); seq <= 5; seq++ {
		want := seq >= 4
		if got := fileSize(t, dir, segOf("c-1", seq)) >= 0; got != want {
			t.Fatalf("seg(%d) exists = %v, want %v", seq, got, want)
		}
	}
	mustRecover(t, s, "c-1", persist.Bounds{First: 4, Last: 5})
	committedOnReopen(t, dir, "c-1", persist.Bounds{First: 4, Last: 5})
	mustRead(t, s, "c-1", 4, 9, 4, 5)

	m = r.mark()
	if err := s.DeleteEvents(context.Background(), "c-1", 9); err != nil {
		t.Fatal(err)
	}
	checkCalls(t, "DeleteEvents(9)", r.since(m), []string{
		"Remove " + segOf("c-1", 4),
		"SyncDir journal/id-c-1",
	})
	if fileSize(t, dir, segOf("c-1", 5)) <= 0 {
		t.Fatal("seg(5) was removed")
	}
	mustRecover(t, s, "c-1", persist.Bounds{First: 5, Last: 5})
	mustRead(t, s, "c-1", 5, 1, 5)

	// Nothing left to remove: no call, no error.
	m = r.mark()
	if err := s.DeleteEvents(context.Background(), "c-1", 9); err != nil {
		t.Fatal(err)
	}
	checkCalls(t, "DeleteEvents with nothing to remove", r.since(m), nil)
}

func TestPER390DeleteEventsEdges(t *testing.T) {
	ctx := context.Background()
	s, r, _ := newJournal(t, WithSegmentSize(1))

	// An ID without a journal: nothing to do.
	m := r.mark()
	if err := s.DeleteEvents(ctx, "none", 5); err != nil {
		t.Fatal(err)
	}
	for _, c := range r.since(m) {
		if strings.HasPrefix(c, "Remove") || strings.HasPrefix(c, "SyncDir journal") {
			t.Fatalf("DeleteEvents on an empty ID called %q", c)
		}
	}

	writeFive(t, s)
	// to below the first event: nothing is removed.
	m = r.mark()
	if err := s.DeleteEvents(ctx, "c-1", 0); err != nil {
		t.Fatal(err)
	}
	for _, c := range r.since(m) {
		if strings.HasPrefix(c, "Remove") {
			t.Fatalf("DeleteEvents(0) called %q", c)
		}
	}
	// The largest to must not overflow.
	if err := s.DeleteEvents(ctx, "c-1", ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	mustRecover(t, s, "c-1", persist.Bounds{First: 5, Last: 5})

	// A done context changes nothing.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	m = r.mark()
	if err := s.DeleteEvents(cctx, "c-1", 9); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteEvents with a done ctx: %v", err)
	}
	if _, err := s.Read(cctx, "c-1", 5, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read with a done ctx: %v", err)
	}
	checkCalls(t, "done ctx", r.since(m), nil)

	// Invalid ID.
	if err := s.DeleteEvents(ctx, "", 1); !errors.Is(err, persist.ErrInvalidID) {
		t.Fatalf("DeleteEvents with an empty id: %v", err)
	}
	if _, err := s.Read(ctx, "", 1, 1); !errors.Is(err, persist.ErrInvalidID) {
		t.Fatalf("Read with an empty id: %v", err)
	}
}

func TestPER390DeleteEventsFailure(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	t.Run("remove", func(t *testing.T) {
		s, r, dir := newJournal(t, WithSegmentSize(1))
		writeFive(t, s)
		r.fail("Remove", segOf("c-1", 2), nth(1), boom)
		m := r.mark()
		err := s.DeleteEvents(ctx, "c-1", 3)
		if !errors.Is(err, boom) {
			t.Fatalf("DeleteEvents: %v, want boom", err)
		}
		// Removal is ascending: seg(1) is gone, seg(2) and seg(3) remain; the removal of seg(1)
		// is synced.
		checkCalls(t, "failed DeleteEvents", r.since(m), []string{
			"Remove " + segOf("c-1", 1),
			"Remove " + segOf("c-1", 2),
			"SyncDir journal/id-c-1",
		})
		if fileSize(t, dir, segOf("c-1", 1)) >= 0 || fileSize(t, dir, segOf("c-1", 2)) < 0 {
			t.Fatal("the failed removal left the wrong segments")
		}
		mustRead(t, s, "c-1", 2, 9, 2, 3, 4, 5)
		// The next call goes on where the failed one stopped.
		if err := s.DeleteEvents(ctx, "c-1", 3); err != nil {
			t.Fatal(err)
		}
		mustRecover(t, s, "c-1", persist.Bounds{First: 4, Last: 5})
	})

	t.Run("syncdir", func(t *testing.T) {
		s, r, _ := newJournal(t, WithSegmentSize(1))
		writeFive(t, s)
		r.fail("SyncDir", "journal/id-c-1", nth(1), boom)
		if err := s.DeleteEvents(ctx, "c-1", 3); !errors.Is(err, boom) {
			t.Fatalf("DeleteEvents: %v, want boom", err)
		}
		mustRead(t, s, "c-1", 4, 9, 4, 5)
		// The next commit repeats the directory sync.
		m := r.mark()
		mustCommit(t, s, "c-1")
		checkCalls(t, "Commit after a failed SyncDir", r.since(m), []string{"SyncDir journal/id-c-1"})
	})
}

func TestPER380ReadBounds(t *testing.T) {
	ctx := context.Background()

	t.Run("from below the first", func(t *testing.T) {
		s, _, _ := newJournal(t, WithSegmentSize(1))
		writeFive(t, s)
		if err := s.DeleteEvents(ctx, "c-1", 3); err != nil {
			t.Fatal(err)
		}
		for _, from := range []uint64{0, 1, 3} {
			_, err := s.Read(ctx, "c-1", from, 5)
			if !errors.Is(err, persist.ErrMissingEvents) {
				t.Fatalf("Read(from %d): %v, want one wrapping ErrMissingEvents", from, err)
			}
		}
		mustRead(t, s, "c-1", 4, 5, 4, 5)
		// An ID without events has First == 1.
		if _, err := s.Read(ctx, "none", 0, 1); !errors.Is(err, persist.ErrMissingEvents) {
			t.Fatalf("Read(from 0) on an empty ID: %v", err)
		}
	})

	t.Run("from above the last", func(t *testing.T) {
		s, _, _ := newJournal(t, WithSegmentSize(1))
		writeFive(t, s)
		for _, from := range []uint64{6, 7, 1 << 40} {
			got, err := s.Read(ctx, "c-1", from, 5)
			if got != nil || err != nil {
				t.Fatalf("Read(from %d) = %v, %v, want nil, nil", from, got, err)
			}
		}
		got, err := s.Read(ctx, "none", 1, 5)
		if got != nil || err != nil {
			t.Fatalf("Read on an empty ID = %v, %v, want nil, nil", got, err)
		}
		mustRead(t, s, "c-1", 5, 5, 5)
		mustRead(t, s, "c-1", 1, 1<<30, 1, 2, 3, 4, 5)
	})

	t.Run("uncommitted events", func(t *testing.T) {
		// Two segments of one event each, one more event in the last segment is written after the
		// commit, and so is a new segment.
		s, _, _ := newJournal(t, WithSegmentSize(1000))
		mustRecover(t, s, "c-1", persist.Bounds{First: 1})
		mustWrite(t, s, "c-1", ev(1, "e1", "e2"))
		mustWrite(t, s, "c-1", ev(3, "e3"))
		mustCommit(t, s, "c-1")
		mustWrite(t, s, "c-1", ev(4, "e4", "e5"))
		mustRead(t, s, "c-1", 1, 10, 1, 2, 3)
		mustRead(t, s, "c-1", 4, 10)
		mustCommit(t, s, "c-1")
		mustRead(t, s, "c-1", 2, 10, 2, 3, 4, 5)

		// Uncommitted events in a segment that is not the last one.
		s2, _, _ := newJournal(t, WithSegmentSize(1))
		mustRecover(t, s2, "c-1", persist.Bounds{First: 1})
		mustWrite(t, s2, "c-1", ev(1, "e1"))
		mustCommit(t, s2, "c-1")
		for i := uint64(2); i <= 4; i++ {
			mustWrite(t, s2, "c-1", ev(i, fmt.Sprintf("e%d", i)))
		}
		mustRead(t, s2, "c-1", 1, 10, 1)
		mustCommit(t, s2, "c-1")
		mustRead(t, s2, "c-1", 1, 10, 1, 2, 3, 4)
	})

	t.Run("batches", func(t *testing.T) {
		s, _, _ := newJournal(t, WithSegmentSize(24))
		mustRecover(t, s, "c-1", persist.Bounds{First: 1})
		mustWrite(t, s, "c-1", ev(1, "e1", "e2", "e3"))
		mustWrite(t, s, "c-1", ev(4, "e4", "e5"))
		mustWrite(t, s, "c-1", ev(6, "e6"))
		mustCommit(t, s, "c-1")
		// from in the middle of a batch, and max cutting a batch.
		mustRead(t, s, "c-1", 2, 2, 2, 3)
		mustRead(t, s, "c-1", 5, 9, 5, 6)
		mustRead(t, s, "c-1", 3, 1, 3)
		mustRead(t, s, "c-1", 1, 6, 1, 2, 3, 4, 5, 6)
	})

	t.Run("renamed next segment", func(t *testing.T) {
		dir := t.TempDir()
		s, err := Open(dir, WithSegmentSize(1))
		if err != nil {
			t.Fatal(err)
		}
		writeFive(t, s)
		if err := os.Rename(
			filepath.Join(dir, filepath.FromSlash(segOf("c-1", 3))),
			filepath.Join(dir, filepath.FromSlash(segOf("c-1", 4)+"x")),
		); err != nil {
			t.Fatal(err)
		}
		// seg(3) is renamed to a name Read does not know: the segment after seg(2) is seg(4).
		s, err = Open(dir, WithSegmentSize(1))
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Read(ctx, "c-1", 1, 5)
		corruptReason(t, err, "seq 4, expected 3")
		// A read that starts behind the gap works.
		mustRead(t, s, "c-1", 4, 5, 4, 5)
		// A read that stops before the gap works.
		mustRead(t, s, "c-1", 1, 2, 1, 2)
	})

	t.Run("damaged record", func(t *testing.T) {
		dir := t.TempDir()
		s, err := Open(dir, WithSegmentSize(1))
		if err != nil {
			t.Fatal(err)
		}
		writeFive(t, s)
		name := filepath.Join(dir, filepath.FromSlash(segOf("c-1", 2)))
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, flipped(b, headerSize+1), 0o644); err != nil {
			t.Fatal(err)
		}
		s, err = Open(dir, WithSegmentSize(1))
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Read(ctx, "c-1", 1, 5)
		corruptReason(t, err, "checksum mismatch")
		mustRead(t, s, "c-1", 3, 5, 3, 4, 5)

		// A record whose first sequence number is wrong.
		wrong := jrec(t, 7, "e2")
		if err := os.WriteFile(name, wrong, 0o644); err != nil {
			t.Fatal(err)
		}
		s, err = Open(dir, WithSegmentSize(1))
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Read(ctx, "c-1", 1, 5)
		corruptReason(t, err, "seq 7, expected 2")
	})

	t.Run("short segment", func(t *testing.T) {
		// A non-last segment that ends before the next one starts.
		dir := t.TempDir()
		s, err := Open(dir, WithSegmentSize(1))
		if err != nil {
			t.Fatal(err)
		}
		writeFive(t, s)
		name := filepath.Join(dir, filepath.FromSlash(segOf("c-1", 2)))
		if err := os.Truncate(name, 0); err != nil {
			t.Fatal(err)
		}
		s, err = Open(dir, WithSegmentSize(1))
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Read(ctx, "c-1", 1, 5)
		corruptReason(t, err, "seq 3, expected 2")
	})

	t.Run("data is copied", func(t *testing.T) {
		s, _, _ := newJournal(t)
		mustRecover(t, s, "c-1", persist.Bounds{First: 1})
		mustWrite(t, s, "c-1", ev(1, "e1", "e2", "e3"))
		mustCommit(t, s, "c-1")
		first, err := s.Read(ctx, "c-1", 1, 3)
		if err != nil {
			t.Fatal(err)
		}
		for i := range first {
			first[i].Seq = 99
			for j := range first[i].Data[:cap(first[i].Data)] {
				first[i].Data[:cap(first[i].Data)][j] = 'X'
			}
		}
		first = append(first[:1], persist.Event{})
		mustRead(t, s, "c-1", 1, 3, 1, 2, 3)
		mustRead(t, s, "c-1", 2, 1, 2)
	})
}

// A read starts at the segment with the greatest name at most from, with several segments
// holding several events each.
func TestPER380ReadStartsAtSegment(t *testing.T) {
	s, r, _ := newJournal(t, WithSegmentSize(20))
	mustRecover(t, s, "c-1", persist.Bounds{First: 1})
	for i := uint64(1); i <= 12; i += 2 {
		mustWrite(t, s, "c-1", ev(i, fmt.Sprintf("e%d", i), fmt.Sprintf("e%d", i+1)))
	}
	mustCommit(t, s, "c-1")
	for from := uint64(1); from <= 12; from++ {
		want := make([]uint64, 0, 12)
		for q := from; q <= 12 && len(want) < 4; q++ {
			want = append(want, q)
		}
		mustRead(t, s, "c-1", from, 4, want...)
	}
	// Segments before the start are not opened: batches of two events fill a segment, so the
	// segments are named 1, 3, 5, 7, 9 and 11.
	m := r.mark()
	mustRead(t, s, "c-1", 12, 1, 12)
	for _, c := range r.since(m) {
		if strings.HasPrefix(c, "Open ") && !strings.HasSuffix(c, "/"+seqName(11, segExt)) {
			t.Fatalf("Read(12) opened %q", c)
		}
	}
}

func TestPER330ReadFilesClosed(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	for _, tc := range []struct {
		name string
		fail func(r *recFS)
	}{
		{"open", func(r *recFS) { r.fail("Open", segOf("c-1", 2), nth(1), boom) }},
		{"size", func(r *recFS) { r.fail("Size", segOf("c-1", 2), nth(1), boom) }},
		{"readat", func(r *recFS) { r.fail("ReadAt", segOf("c-1", 2), nth(1), boom) }},
		{"close", func(r *recFS) { r.fail("Close", segOf("c-1", 2), nth(1), boom) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, _ := newJournal(t, WithSegmentSize(1))
			writeFive(t, s)
			tc.fail(r)
			_, err := s.Read(ctx, "c-1", 1, 5)
			if !errors.Is(err, boom) {
				t.Fatalf("Read: %v, want boom", err)
			}
			if errors.Is(err, ErrCorrupt) {
				t.Fatalf("Read: an I/O error is reported as corruption: %v", err)
			}
			r.checkClosed(t)
			// The rule fired once: the store still works.
			mustRead(t, s, "c-1", 1, 5, 1, 2, 3, 4, 5)
		})
	}

	t.Run("short file", func(t *testing.T) {
		// The file is shorter than the committed size: reported, not a panic or a hang.
		s, r, dir := newJournal(t, WithSegmentSize(1000))
		mustRecover(t, s, "c-1", persist.Bounds{First: 1})
		mustWrite(t, s, "c-1", ev(1, "e1"))
		mustCommit(t, s, "c-1")
		if err := os.Truncate(filepath.Join(dir, filepath.FromSlash(segOf("c-1", 1))), 5); err != nil {
			t.Fatal(err)
		}
		_, err := s.Read(ctx, "c-1", 1, 1)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("Read: %v, want one wrapping io.ErrUnexpectedEOF", err)
		}
		r.checkClosed(t)
	})

	t.Run("delete", func(t *testing.T) {
		s, r, _ := newJournal(t, WithSegmentSize(1))
		writeFive(t, s)
		if err := s.DeleteEvents(ctx, "c-1", 3); err != nil {
			t.Fatal(err)
		}
		r.checkClosed(t)
	})
}

// A Read on an ID the Store has not seen loads it with the scan, as Write and Commit do.
func TestPER380ReadLoadsJournal(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithSegmentSize(1))
	if err != nil {
		t.Fatal(err)
	}
	writeFive(t, s)
	s2, err := Open(dir, WithSegmentSize(1))
	if err != nil {
		t.Fatal(err)
	}
	mustRead(t, s2, "c-1", 2, 3, 2, 3, 4)
	mustWrite(t, s2, "c-1", ev(6, "e6"))
	mustRead(t, s2, "c-1", 5, 9, 5)
	mustCommit(t, s2, "c-1")
	mustRead(t, s2, "c-1", 5, 9, 5, 6)
}
