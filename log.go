// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Logf formats a log line for this actor. It formats with fmt.Sprintf and records lockstep.log
// when the tracer is enabled. In a System built by New it also logs the line at slog.LevelInfo
// through slog.Default() with the attributes node, path and uid (lowercase hex); in simulation
// the line reaches only the trace.
func (c *Context[M]) Logf(format string, args ...any) {
	c.mustInTurn()
	text := fmt.Sprintf(format, args...)
	sys, addr := c.cell.sys, c.cell.addr
	if tr := sys.eng.Tracer; tr.Enabled() {
		tr.Record("lockstep.log",
			engine.Attr{Key: "addr", Value: addr.String()},
			engine.Attr{Key: "text", Value: text},
		)
	}
	// ACT-130 names the local and production engines; Blocking is true for exactly those, and false
	// for the simulation engine and the manual engine.
	if sys.eng.Blocking {
		slog.Default().Info(text,
			"node", sys.node,
			"path", addr.path,
			"uid", strconv.FormatUint(addr.uid, 16),
		)
	}
}
