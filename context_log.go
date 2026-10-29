// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Logger returns the actor logger: the system logger with the attributes node, path and uid of
// this actor. It returns the same *slog.Logger for the actor's lifetime, across restarts. In
// simulation it writes to the node's kernel log instead: each record at slog.LevelInfo or above
// becomes a kernel.log record of the node, and records below Info are dropped.
func (c *Context[M]) Logger() *slog.Logger {
	c.mustInTurn()
	return c.cell.logger()
}

// logger returns the cell's actor logger, building it on the first call (OBS-030). Spawn does not
// build it, so an actor that never logs costs only the pointer. Turns call it and never overlap,
// but ACT-120 does not detect a call from another goroutine during a turn, so the pointer is
// atomic: when two calls race, the first logger stored wins and both return it.
func (c *cell[M]) logger() *slog.Logger {
	if l := c.log.Load(); l != nil {
		return l
	}
	l := c.sys.log.With(
		slog.String("node", c.sys.node),
		slog.String("path", c.addr.path),
		slog.Uint64("uid", c.addr.uid),
	)
	if c.log.CompareAndSwap(nil, l) {
		return l
	}
	return c.log.Load()
}

// Logf logs fmt.Sprintf(format, args...) at slog.LevelInfo through Logger, with the caller of Logf
// as the record's source. When the tracer is enabled it also records lockstep.log with the line.
// It formats the line once, and not at all when Logger is not enabled for slog.LevelInfo and the
// tracer is off.
func (c *Context[M]) Logf(format string, args ...any) {
	c.mustInTurn()
	sys := c.cell.sys
	tr := sys.eng.Tracer
	traced := tr.Enabled()
	l := c.cell.logger()
	logged := l.Enabled(context.Background(), slog.LevelInfo) // OBS-040: no formatting when disabled
	if !traced && !logged {
		return
	}
	text := fmt.Sprintf(format, args...)
	if traced {
		tr.Record("lockstep.log",
			engine.Attr{Key: "addr", Value: c.cell.addr.String()},
			engine.Attr{Key: "text", Value: text},
		)
	}
	if logged {
		// OBS-040: the record's PC is Logf's caller, so HandlerOptions.AddSource points at actor
		// code; skip runtime.Callers and Logf. OBS-050: the handler gets context.Background().
		var pcs [1]uintptr
		runtime.Callers(2, pcs[:])
		r := slog.NewRecord(sys.eng.Clock.Now(), slog.LevelInfo, text, pcs[0])
		_ = l.Handler().Handle(context.Background(), r)
	}
}
