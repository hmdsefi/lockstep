// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// init sets the hook of ENG-200 and SIM's three hooks (SIM §5.9); it is the only init function of
// the package.
//
//nolint:gochecknoinits // RFC-0001: internal/hooks is set at package init
func init() {
	hooks.NewSystem = buildSystem
	hooks.Discard = discardHook
	hooks.AskResolved = askResolvedHook
	hooks.PendingAsks = pendingAsksHook
}

// buildSystem is hooks.NewSystem (ENG-200): it builds a system on eng with the options in opts,
// which is nil or a []Option. It returns an error instead of panicking, whatever its input.
func buildSystem(eng engine.Engine, opts any) (hooks.Built, error) {
	if err := eng.Validate(); err != nil {
		return hooks.Built{}, err
	}
	var options []Option
	switch o := opts.(type) {
	case nil:
	case []Option:
		options = o
	default:
		return hooks.Built{}, fmt.Errorf("lockstep: hooks: opts has type %T, want []lockstep.Option", opts)
	}
	sys, err := newSystem(eng, options)
	if err != nil {
		return hooks.Built{}, err
	}
	return hooks.Built{System: sys, Host: host{sys}}, nil
}
