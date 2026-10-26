// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hmdsefi/lockstep/persist"
)

const (
	segExt  = ".seg"
	snapExt = ".snap"
	tmpExt  = ".tmp"

	seqDigits = 20 // decimal digits of a uint64, the width of a sequence number in a file name
	hexDigits = "0123456789ABCDEF"
)

// encodeID encodes a persistence ID for use in a directory name: a-z, 0-9, '-' and '_' stay,
// every other byte becomes '%' and two upper-case hex digits.
func encodeID(id string) string {
	var b strings.Builder
	b.Grow(len(id))
	for i := range len(id) {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0x0F])
		}
	}
	return b.String()
}

// idDirName is the name of the directory of id under journal and snapshots.
func idDirName(id string) string { return "id-" + encodeID(id) }

// checkID returns an error wrapping persist.ErrInvalidID if id is empty or longer than
// persist.MaxIDLen bytes.
func checkID(id string) error {
	if id == "" || len(id) > persist.MaxIDLen {
		return fmt.Errorf("filestore: id %q: %w", id, persist.ErrInvalidID)
	}
	return nil
}

// seqName is the file name for sequence number seq: 20 decimal digits, zero-padded, then ext.
func seqName(seq uint64, ext string) string {
	digits := strconv.FormatUint(seq, 10)
	return strings.Repeat("0", seqDigits-len(digits)) + digits + ext
}

// parseSeqName reverses seqName. It reports false for any other name, so a directory scan can
// ignore entries it does not own.
func parseSeqName(name, ext string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, ext)
	if !ok || len(digits) != seqDigits {
		return 0, false
	}
	for i := range len(digits) {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}
