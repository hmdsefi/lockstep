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
