// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote

import (
	"strconv"

	"github.com/hmdsefi/lockstep"
)

// Code classifies a failure that happened on another node. It crosses the wire as one byte.
type Code uint8

// The failure codes. Each one but CodeOther stands for a lockstep sentinel.
const (
	CodeOther       Code = iota // "other": unwraps to nil
	CodeTimeout                 // "timeout": lockstep.ErrTimeout
	CodeTerminated              // "terminated": lockstep.ErrTerminated
	CodeNotFound                // "not-found": lockstep.ErrNotFound
	CodeWrongType               // "wrong-type": lockstep.ErrWrongType
	CodeMailboxFull             // "mailbox-full": lockstep.ErrMailboxFull
	CodeOverloaded              // "overloaded": lockstep.ErrOverloaded
	CodeStopped                 // "stopped": lockstep.ErrStopped
)

var codeNames = [...]string{
	CodeOther:       "other",
	CodeTimeout:     "timeout",
	CodeTerminated:  "terminated",
	CodeNotFound:    "not-found",
	CodeWrongType:   "wrong-type",
	CodeMailboxFull: "mailbox-full",
	CodeOverloaded:  "overloaded",
	CodeStopped:     "stopped",
}

// String returns the code's name, or "Code(<n>)" for a code that is not defined.
func (c Code) String() string {
	if int(c) < len(codeNames) {
		return codeNames[c]
	}
	return "Code(" + strconv.Itoa(int(c)) + ")"
}

// Error is a failure reported by another node. Code says what kind of failure it was, and
// errors.Is finds the matching lockstep sentinel through it.
type Error struct {
	Node    string // the node that reported the failure
	Code    Code
	Message string
}

// Error returns "remote: <Node>: <Message>".
func (e *Error) Error() string { return "remote: " + e.Node + ": " + e.Message }

// Unwrap returns the lockstep sentinel of e's code: nil for CodeOther and for a code that is not
// defined.
func (e *Error) Unwrap() error {
	switch e.Code {
	case CodeTimeout:
		return lockstep.ErrTimeout
	case CodeTerminated:
		return lockstep.ErrTerminated
	case CodeNotFound:
		return lockstep.ErrNotFound
	case CodeWrongType:
		return lockstep.ErrWrongType
	case CodeMailboxFull:
		return lockstep.ErrMailboxFull
	case CodeOverloaded:
		return lockstep.ErrOverloaded
	case CodeStopped:
		return lockstep.ErrStopped
	}
	return nil
}
