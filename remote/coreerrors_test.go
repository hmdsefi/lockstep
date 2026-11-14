// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hmdsefi/lockstep"
)

// TestCoreErrors checks the errors package remote gives the core and its error-to-code table.
func TestCoreErrors(t *testing.T) {
	var e coreErrors
	var re *Error
	if err := e.Remote("b", uint8(CodeNotFound), "no actor"); !errors.As(err, &re) || *re != (Error{Node: "b", Code: CodeNotFound, Message: "no actor"}) ||
		!errors.Is(err, lockstep.ErrNotFound) {
		t.Errorf("Remote = %#v", err)
	}
	if err := e.Unreachable("b"); !errors.Is(err, ErrUnreachable) || err.Error() != "remote: node unreachable: b" {
		t.Errorf("Unreachable = %v", err)
	}
	if err := e.PeerRestarted("b"); !errors.Is(err, ErrPeerRestarted) || !errors.Is(err, lockstep.ErrTerminated) ||
		err.Error() != "remote: peer restarted: b" {
		t.Errorf("PeerRestarted = %v", err)
	}
	if err := e.FrameTooLarge(5000, 4096); !errors.Is(err, ErrFrameTooLarge) || err.Error() != "remote: frame too large: 5000 bytes > max 4096" {
		t.Errorf("FrameTooLarge = %v", err)
	}
	for _, tc := range []struct {
		err  error
		want Code
	}{
		{errors.New("x"), CodeOther},
		{fmt.Errorf("w: %w", lockstep.ErrTimeout), CodeTimeout},
		{lockstep.ErrTerminated, CodeTerminated},
		{e.PeerRestarted("b"), CodeTerminated},
		{lockstep.ErrNotFound, CodeNotFound},
		{lockstep.ErrWrongType, CodeWrongType},
		{lockstep.ErrMailboxFull, CodeMailboxFull},
		{lockstep.ErrOverloaded, CodeOverloaded},
		{lockstep.ErrStopped, CodeStopped},
		{&Error{Node: "c", Code: CodeStopped}, CodeStopped},
		{&Error{Node: "c", Code: 42}, 42},
	} {
		if got := Code(e.Code(tc.err)); got != tc.want {
			t.Errorf("Code(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
