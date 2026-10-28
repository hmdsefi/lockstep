// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "log/slog"

// WithLogger sets the logger the system writes its log events to and that Context.Logger and
// Context.Logf build on. The default, and the value used when l is nil, is slog.Default() as it
// is when New runs. To discard logs, pass slog.New(slog.DiscardHandler).
func WithLogger(l *slog.Logger) Option {
	return func(c *systemConfig) { c.log = l }
}

// systemLogger returns the system logger of cfg (OBS-010): WithLogger's value, or slog.Default()
// at the time of the call when that is nil or absent. New calls it once, so a later
// slog.SetDefault does not change a running system's logger.
func systemLogger(cfg *systemConfig) *slog.Logger {
	if cfg.log != nil {
		return cfg.log
	}
	return slog.Default()
}
