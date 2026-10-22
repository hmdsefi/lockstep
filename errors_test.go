// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// AT-ENG-06, second half; ENG-120.
func TestATENG06ErrOverloadedReexport(t *testing.T) {
	if ErrOverloaded != engine.ErrOverloaded {
		t.Error("ErrOverloaded is not engine.ErrOverloaded")
	}
	if !errors.Is(fmt.Errorf("x: %w", engine.ErrOverloaded), ErrOverloaded) {
		t.Error("errors.Is does not match a wrapped engine.ErrOverloaded")
	}
	if ErrOverloaded.Error() != "lockstep: overloaded" {
		t.Errorf("text = %q", ErrOverloaded.Error())
	}
}

// ACT §4 and MSG §4.1 texts.
func TestSentinelTexts(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{ErrStopped, "lockstep: stopped"},
		{ErrNameTaken, "lockstep: name taken"},
		{ErrWrongType, "lockstep: wrong message type"},
		{ErrNotFound, "lockstep: not found"},
		{ErrInvalidOption, "lockstep: invalid option"},
		{ErrInvalidName, "lockstep: invalid name"},
		{ErrNotLocal, "lockstep: address not local"},
		{ErrTimeout, "lockstep: timeout"},
		{ErrTerminated, "lockstep: actor terminated"},
		{ErrAwaitInActor, "lockstep: blocking wait inside an actor"},
		{ErrOverloaded, "lockstep: overloaded"},
	}
	seen := map[error]bool{}
	for _, c := range cases {
		if c.err.Error() != c.want {
			t.Errorf("%v: text %q, want %q", c.err, c.err.Error(), c.want)
		}
		if seen[c.err] {
			t.Errorf("%q declared twice", c.want)
		}
		seen[c.err] = true
	}
}
