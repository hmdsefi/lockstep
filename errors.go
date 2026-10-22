// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Errors of package lockstep (ACT §4, §7).
var (
	ErrStopped       = errors.New("lockstep: stopped")
	ErrNameTaken     = errors.New("lockstep: name taken")
	ErrWrongType     = errors.New("lockstep: wrong message type")
	ErrNotFound      = errors.New("lockstep: not found")
	ErrInvalidOption = errors.New("lockstep: invalid option")
	ErrInvalidName   = errors.New("lockstep: invalid name")      // (addition)
	ErrNotLocal      = errors.New("lockstep: address not local") // (addition)
)

// Errors of sending and asking (MSG §4.1, §7).
var (
	ErrTimeout      = errors.New("lockstep: timeout")
	ErrTerminated   = errors.New("lockstep: actor terminated")
	ErrAwaitInActor = errors.New("lockstep: blocking wait inside an actor")
)

// ErrOverloaded is returned when an engine refuses work. It is the same value as
// engine.ErrOverloaded, so errors.Is(err, ErrOverloaded) holds for errors from any engine
// (ENG-120).
var ErrOverloaded = engine.ErrOverloaded
