// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"encoding/binary"
	"testing"
)

// refChain computes Chain(i) from Chain(i-1) as RFT-290 defines it: FNV-1a 64 continued from the
// previous chain value over be64(Term), be32(Client), be64(Seq), be32(len(Cmd)) and Cmd.
func refChain(prev uint64, e Entry) uint64 {
	var b []byte
	b = binary.BigEndian.AppendUint64(b, e.Term)
	b = binary.BigEndian.AppendUint32(b, e.Client)
	b = binary.BigEndian.AppendUint64(b, e.Seq)
	b = binary.BigEndian.AppendUint32(b, uint32(len(e.Cmd))) //nolint:gosec // G115: test commands are short
	b = append(b, e.Cmd...)
	h := prev
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

func logOf(entries ...Entry) *raftLog {
	l := &raftLog{}
	l.append(entries...)
	return l
}

func mustPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s did not panic", name)
		}
	}()
	f()
}

// TestATRFT23ChainHash checks Chain against the RFT-290 definition computed independently.
func TestATRFT23ChainHash(t *testing.T) {
	entries := []Entry{{1, 0, 0, ""}, {1, 2, 7, "pk1:1v"}}
	l := logOf(entries...)

	if got := l.Chain(0); got != ChainSeed {
		t.Errorf("Chain(0) = %d, want ChainSeed %d", got, ChainSeed)
	}
	want := ChainSeed
	for i, e := range entries {
		want = refChain(want, e)
		if got := l.Chain(uint64(i + 1)); got != want {
			t.Errorf("Chain(%d) = %d, want %d", i+1, got, want)
		}
	}
	if l.Chain(1) == l.Chain(2) || l.Chain(1) == ChainSeed {
		t.Errorf("chain values are not distinct: %d %d", l.Chain(1), l.Chain(2))
	}
}

// TestLogTruncateKeepsChains checks that truncating and appending again leaves every chain equal
// to a log built from scratch with the same entries.
func TestLogTruncateKeepsChains(t *testing.T) {
	first := []Entry{{1, 0, 0, ""}, {1, 1, 1, "pa:1b"}, {2, 1, 2, "gx"}, {2, 2, 1, "pc:1d"}, {3, 2, 2, "ge"}}
	l := logOf(first...)
	l.truncate(2)
	if l.LastIndex() != 2 {
		t.Fatalf("LastIndex after truncate(2) = %d, want 2", l.LastIndex())
	}
	more := []Entry{{4, 3, 1, "pq:1r"}, {4, 3, 2, "gq"}}
	l.append(more...)

	want := logOf(append(append([]Entry(nil), first[:2]...), more...)...)
	if l.LastIndex() != want.LastIndex() {
		t.Fatalf("LastIndex = %d, want %d", l.LastIndex(), want.LastIndex())
	}
	for i := uint64(0); i <= want.LastIndex(); i++ {
		if got, w := l.Chain(i), want.Chain(i); got != w {
			t.Errorf("Chain(%d) = %d, want %d", i, got, w)
		}
		if i >= 1 && l.Entry(i) != want.Entry(i) {
			t.Errorf("Entry(%d) = %+v, want %+v", i, l.Entry(i), want.Entry(i))
		}
	}

	l.truncate(10) // beyond the end: no change
	if l.LastIndex() != 4 {
		t.Errorf("LastIndex after truncate(10) = %d, want 4", l.LastIndex())
	}
	l.truncate(0)
	if l.LastIndex() != 0 || l.Chain(0) != ChainSeed {
		t.Errorf("after truncate(0): LastIndex %d, Chain(0) %d", l.LastIndex(), l.Chain(0))
	}
}

// TestLogViewBounds checks the LogView bounds and that slice results are private copies.
func TestLogViewBounds(t *testing.T) {
	l := logOf(Entry{1, 0, 0, ""}, Entry{2, 1, 1, "ga"}, Entry{2, 1, 2, "gb"})
	last := l.LastIndex()

	if l.Term(0) != 0 || l.Term(last+1) != 0 {
		t.Errorf("Term(0) = %d, Term(last+1) = %d, want 0 and 0", l.Term(0), l.Term(last+1))
	}
	if l.Term(1) != 1 || l.Term(last) != 2 {
		t.Errorf("Term(1) = %d, Term(last) = %d, want 1 and 2", l.Term(1), l.Term(last))
	}
	mustPanic(t, "Entry(0)", func() { l.Entry(0) })
	mustPanic(t, "Entry(last+1)", func() { l.Entry(last + 1) })
	mustPanic(t, "Chain(last+1)", func() { l.Chain(last + 1) })
	mustPanic(t, "slice(0, 1)", func() { l.slice(0, 1) })
	mustPanic(t, "slice(1, last+1)", func() { l.slice(1, last+1) })

	s := l.slice(2, 3)
	if len(s) != 2 || s[0] != l.Entry(2) || s[1] != l.Entry(3) {
		t.Fatalf("slice(2, 3) = %+v", s)
	}
	s[0].Term = 99
	s = append(s, Entry{Term: 100})
	_ = s
	if l.Entry(2).Term != 2 || l.LastIndex() != 3 {
		t.Errorf("a slice result aliases the log: Entry(2) = %+v, LastIndex = %d", l.Entry(2), l.LastIndex())
	}
	if a, b := l.slice(1, 3), l.slice(1, 3); &a[0] == &b[0] {
		t.Error("two slice calls share a backing array")
	}
	if got := l.slice(3, 2); len(got) != 0 {
		t.Errorf("slice(3, 2) = %+v, want empty", got)
	}
	if got := l.slice(last+1, last); len(got) != 0 {
		t.Errorf("slice(last+1, last) = %+v, want empty", got)
	}
}

// TestLogFollowerPathDoesNotAllocate checks that the methods a follower uses to compare entries it
// already holds do not allocate.
func TestLogFollowerPathDoesNotAllocate(t *testing.T) {
	l := logOf(Entry{1, 0, 0, ""}, Entry{1, 1, 1, "pa:1b"}, Entry{2, 1, 2, "ga"})
	var sink uint64
	allocs := testing.AllocsPerRun(100, func() {
		sink += l.LastIndex() + l.Term(2) + l.Entry(3).Term + l.Chain(3)
	})
	if allocs != 0 {
		t.Errorf("LogView reads allocate %v times per run, want 0", allocs)
	}
	_ = sink
}
