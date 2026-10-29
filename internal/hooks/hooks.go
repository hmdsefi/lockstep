// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package hooks connects packages that must not import each other. Package lockstep sets the
// function variables in its init function; the packages whose specs declare them call them
// (ENG-210).
package hooks

import "github.com/hmdsefi/lockstep/internal/engine"

// Built is what NewSystem returns.
type Built struct {
	System any         // the *lockstep.System
	Host   engine.Host // the system's inbound side, for the engine's transport
}

// NewSystem builds a *lockstep.System on eng. opts is nil or a []lockstep.Option. It is nil until
// package lockstep is initialized. Called by sim (SIM-070); implemented by ACT (ENG-200).
var NewSystem func(eng engine.Engine, opts any) (Built, error)

// Address returns a as a lockstep.Address (ENG-020), so that sim can tell an effect handler the
// address of the actor that called Do (EFF-400). It is nil until package lockstep is
// initialized. Called by sim; implemented by ACT.
var Address func(a engine.Address) any

// PendingAsk is one pending ask of a system (MSG-370).
type PendingAsk struct {
	Ask     engine.Address // the ask address
	Asker   engine.Address // the asking actor, or the address ACT gives a *System sender
	Target  engine.Address // the actor asked; the zero Address for a NewReply ask
	ByActor bool           // true when the asker is an actor (a *Context sender)
}

// Discard discards sys after its node crashed (SIM-110): every pending ask resolves with
// ErrStopped (MSG-360), no Stopped hook runs and nothing is closed (SUP-360), and later calls
// with sys as Sender return errors wrapping ErrStopped. sys is a *lockstep.System; Discard panics
// if it has another type. It is nil until package lockstep is initialized. Called by sim
// (SIM-110); declared by SIM (§5.9) and implemented by ACT, MSG (MSG-360) and SUP (SUP-360).
var Discard func(sys any)

// AskResolved reports, without waiting, whether reply, a *lockstep.Reply[R] of any R, has
// resolved (SIM-200). It panics if reply has another type. It is nil until package lockstep is
// initialized. Called by sim (SIM-200); declared by SIM (§5.9) and implemented by MSG (MSG-450).
var AskResolved func(reply any) bool

// InjectFail pushes an inject{err} command to the system queue of the actor at addr on sys, a
// *lockstep.System passed as any (SFT-030). It returns an error wrapping lockstep.ErrNotFound if
// addr names no live actor of sys, and nil otherwise. It panics if sys has another type or err is
// nil. Implemented by package lockstep: SUP processes the command as SFT-040 says, recording
// lockstep.fail with source "injected". It is nil until package lockstep is initialized. Called by
// sim (SFT-030); declared by SFT and implemented by SUP.
var InjectFail func(sys any, addr engine.Address, err error) error

// PendingAsks returns the pending asks of sys, a *lockstep.System, in ask-counter order (MSG-370,
// SIM-260). It returns nil for a stopped or discarded system, and panics if sys has another type.
// It is nil until package lockstep is initialized. Called by sim (SIM-260); declared by SIM (§5.9)
// and implemented by MSG (MSG-370).
var PendingAsks func(sys any) []PendingAsk

// The declarations below are OBS §4.1 (decision R53): late metric registration, for a package
// enabled on a system after lockstep.New. Package lockstep sets both in its init function
// (ENG-210); they are nil until package lockstep is initialized. Called by remote at
// remote.Enable (REM) and later by cluster and persist when their series exist; implemented by
// OBS (OBS-150). sys is the *lockstep.System, and both panic if it has another type. labels are
// name, value pairs after the node label. Both panic if name is not a series of OBS §5.7's table
// of their kind or labels has odd length, are safe for concurrent use and may be called inside a
// turn.

// MetricCounter registers one counter series of OBS §5.7 on sys's metrics backend and returns its
// Add function. It returns nil when sys has metrics off or once Shutdown has started on it; callers
// check for nil once and then call the function on their event paths (OBS-150). A second call
// with the same name and labels returns the first call's function without calling the backend.
var MetricCounter func(sys any, name, help string, labels ...string) func(n uint64)

// MetricGauge registers fn as one gauge series of OBS §5.7 on sys's metrics backend. It does
// nothing when sys has metrics off, once Shutdown has started on it, or for a name and labels
// registered already. After Shutdown the registered gauge returns 0 and holds no reference to fn
// (OBS-150).
var MetricGauge func(sys any, name, help string, fn func() int64, labels ...string)
