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

// PendingAsks returns the pending asks of sys, a *lockstep.System, in ask-counter order (MSG-370,
// SIM-260). It returns nil for a stopped or discarded system, and panics if sys has another type.
// It is nil until package lockstep is initialized. Called by sim (SIM-260); declared by SIM (§5.9)
// and implemented by MSG (MSG-370).
var PendingAsks func(sys any) []PendingAsk
