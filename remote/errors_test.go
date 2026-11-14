// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/remote"
)

func TestCodeString(t *testing.T) {
	tests := []struct {
		c    remote.Code
		want string
	}{
		{remote.CodeOther, "other"},
		{remote.CodeTimeout, "timeout"},
		{remote.CodeTerminated, "terminated"},
		{remote.CodeNotFound, "not-found"},
		{remote.CodeWrongType, "wrong-type"},
		{remote.CodeMailboxFull, "mailbox-full"},
		{remote.CodeOverloaded, "overloaded"},
		{remote.CodeStopped, "stopped"},
		{remote.Code(8), "Code(8)"},
		{remote.Code(255), "Code(255)"},
	}
	for _, tc := range tests {
		if got := tc.c.String(); got != tc.want {
			t.Errorf("Code(%d).String() = %q, want %q", uint8(tc.c), got, tc.want)
		}
	}
}

func TestErrorText(t *testing.T) {
	e := &remote.Error{Node: "n2", Code: remote.CodeNotFound, Message: "no such actor"}
	if got, want := e.Error(), "remote: n2: no such actor"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestErrorUnwrap(t *testing.T) {
	sentinels := map[remote.Code]error{
		remote.CodeTimeout:     lockstep.ErrTimeout,
		remote.CodeTerminated:  lockstep.ErrTerminated,
		remote.CodeNotFound:    lockstep.ErrNotFound,
		remote.CodeWrongType:   lockstep.ErrWrongType,
		remote.CodeMailboxFull: lockstep.ErrMailboxFull,
		remote.CodeOverloaded:  lockstep.ErrOverloaded,
		remote.CodeStopped:     lockstep.ErrStopped,
	}
	for code, want := range sentinels {
		e := &remote.Error{Node: "n", Code: code, Message: "m"}
		if !errors.Is(e, want) {
			t.Errorf("errors.Is(%s, %v) = false", code, want)
		}
		if got := e.Unwrap(); got != want {
			t.Errorf("%s Unwrap() = %v, want %v", code, got, want)
		}
		wrapped := fmt.Errorf("ctx: %w", e)
		var re *remote.Error
		if !errors.As(wrapped, &re) || re != e {
			t.Errorf("errors.As through a wrap failed for %s", code)
		}
		for other, s := range sentinels {
			if other != code && errors.Is(e, s) {
				t.Errorf("%s also matches %v", code, s)
			}
		}
	}
	for _, code := range []remote.Code{remote.CodeOther, remote.Code(8), remote.Code(200)} {
		e := &remote.Error{Node: "n", Code: code, Message: "m"}
		if e.Unwrap() != nil {
			t.Errorf("%s Unwrap() = %v, want nil", code, e.Unwrap())
		}
	}
}
