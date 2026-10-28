// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"log/slog"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// WithLogger sets the logger the system writes its log events to and that Context.Logger and
// Context.Logf build on. The default, and the value used when l is nil, is slog.Default() as it
// is when New runs. To discard logs, pass slog.New(slog.DiscardHandler).
func WithLogger(l *slog.Logger) Option {
	return func(c *systemConfig) { c.log = l }
}

// systemLogger returns the system logger of a system built on eng with cfg (OBS-010): WithLogger's
// value if non-nil; otherwise slog.Default() at the time of the call on a blocking engine (New's
// production engine, the local engine), and a logger that discards on any other (the simulation
// and manual engines), so an actor there writes nothing outside the simulation (ACT-130,
// OBS-155). newSystem calls it once, so a later slog.SetDefault does not change a running
// system's logger.
func systemLogger(cfg *systemConfig, eng engine.Engine) *slog.Logger {
	switch {
	case cfg.log != nil:
		return cfg.log
	case eng.Blocking:
		return slog.Default()
	default:
		return slog.New(slog.DiscardHandler)
	}
}
