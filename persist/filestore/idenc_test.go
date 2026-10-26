// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/persist"
)

func TestEncodeID(t *testing.T) {
	t.Run("PER-300 example", func(t *testing.T) {
		if got, want := encodeID("Acct.1/%"), "%41cct%2E1%2F%25"; got != want {
			t.Fatalf("encodeID = %q, want %q", got, want)
		}
	})
	t.Run("kept bytes", func(t *testing.T) {
		const keep = "abcxyz0189-_"
		if got := encodeID(keep); got != keep {
			t.Fatalf("encodeID(%q) = %q, want it unchanged", keep, got)
		}
	})
	t.Run("case differs", func(t *testing.T) {
		if encodeID("a") == encodeID("A") {
			t.Fatal(`encodeID("a") == encodeID("A"): not injective on case-insensitive file systems`)
		}
	})
	t.Run("longest id", func(t *testing.T) {
		got := encodeID(strings.Repeat("\xff", persist.MaxIDLen))
		if len(got) != 240 || got != strings.Repeat("%FF", persist.MaxIDLen) {
			t.Fatalf("encodeID(80 x 0xFF) = %q (%d bytes), want 80 x %%FF (240 bytes)", got, len(got))
		}
		if n := len(idDirName(strings.Repeat("\xff", persist.MaxIDLen))); n != 243 {
			t.Fatalf("directory name is %d bytes, want 243", n)
		}
	})
	t.Run("every byte encodes distinctly", func(t *testing.T) {
		seen := map[string]int{}
		for b := 0; b < 256; b++ {
			enc := encodeID(string([]byte{byte(b)}))
			if prev, dup := seen[enc]; dup {
				t.Fatalf("bytes %#x and %#x both encode to %q", prev, b, enc)
			}
			seen[enc] = b
			kept := b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-' || b == '_'
			if kept && enc != string([]byte{byte(b)}) {
				t.Fatalf("byte %#x = %q encodes to %q, want it kept", b, byte(b), enc)
			}
			if !kept && enc != fmt.Sprintf("%%%02X", b) {
				t.Fatalf("byte %#x encodes to %q, want %%%02X", b, enc, b)
			}
		}
	})
}

func TestIDDirName(t *testing.T) {
	if got, want := idDirName("Acct.1/%"), "id-%41cct%2E1%2F%25"; got != want {
		t.Fatalf("idDirName = %q, want %q", got, want)
	}
}

func TestCheckID(t *testing.T) {
	for _, id := range []string{"", strings.Repeat("a", persist.MaxIDLen+1)} {
		err := checkID(id)
		if !errors.Is(err, persist.ErrInvalidID) {
			t.Fatalf("checkID(%d bytes) = %v, want ErrInvalidID", len(id), err)
		}
	}
	if err := checkID("x"); err != nil {
		t.Fatalf("checkID(\"x\") = %v", err)
	}
	if err := checkID(strings.Repeat("a", persist.MaxIDLen)); err != nil {
		t.Fatalf("checkID(max length) = %v", err)
	}
	const want = `filestore: id "": persist: invalid persistence id`
	if got := checkID("").Error(); got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}

func TestSeqNames(t *testing.T) {
	if got, want := seqName(1, segExt), "00000000000000000001.seg"; got != want {
		t.Fatalf("seqName(1) = %q, want %q", got, want)
	}
	if got, want := seqName(1<<64-1, snapExt), "18446744073709551615.snap"; got != want {
		t.Fatalf("seqName(max) = %q, want %q", got, want)
	}
	for _, seq := range []uint64{0, 1, 42, 1<<64 - 1} {
		got, ok := parseSeqName(seqName(seq, segExt), segExt)
		if !ok || got != seq {
			t.Fatalf("parseSeqName(seqName(%d)) = %d, %v", seq, got, ok)
		}
	}
	for _, name := range []string{
		"", "1.seg", "0000000000000000001.seg", "000000000000000000001.seg", // wrong digit count
		"00000000000000000001.snap", "00000000000000000001.seg.tmp", "00000000000000000001",
		"0000000000000000000a.seg", "+0000000000000000001.seg", "99999999999999999999.seg", // overflow
	} {
		if seq, ok := parseSeqName(name, segExt); ok {
			t.Fatalf("parseSeqName(%q) = %d, true; want the name ignored", name, seq)
		}
	}
	if segExt != ".seg" || snapExt != ".snap" || tmpExt != ".tmp" {
		t.Fatalf("extensions %q %q %q", segExt, snapExt, tmpExt)
	}
}
