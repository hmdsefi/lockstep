// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Errors of package lockstep.
var (
	ErrStopped       = errors.New("lockstep: stopped")
	ErrNameTaken     = errors.New("lockstep: name taken")
	ErrWrongType     = errors.New("lockstep: wrong message type")
	ErrNotFound      = errors.New("lockstep: not found")
	ErrInvalidOption = errors.New("lockstep: invalid option")
	ErrInvalidName   = errors.New("lockstep: invalid name")
	ErrNotLocal      = errors.New("lockstep: address not local")
)

// Errors of sending and asking.
var (
	ErrTimeout      = errors.New("lockstep: timeout")
	ErrTerminated   = errors.New("lockstep: actor terminated")
	ErrAwaitInActor = errors.New("lockstep: blocking wait inside an actor")
)

// ErrOverloaded is returned when the runtime refuses work: a spawn or a message while admission
// sheds load, or an effect when the effect queue is full. It is the same value in a System
// built by New and in simulation, so errors.Is(err, ErrOverloaded) holds for both.
var ErrOverloaded = engine.ErrOverloaded
