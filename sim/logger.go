// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"context"
	"log/slog"

	"github.com/hmdsefi/faultline/kernel"
)

// kernelHandler is the slog.Handler of a node system's logger (OBS-160). It writes each record at
// slog.LevelInfo or above to the node's kernel log, a kernel.log record, with the text
// "<LEVEL> <msg>[ key=value]...": LEVEL is slog.Level.String(), the pairs are the attributes of
// With and then the record's, in call order, groups flattened as group.key. Each value is resolved
// and printed with kernel.Describe for slog.KindAny and with slog.Value.String otherwise. The
// record's time and PC are ignored, so the text depends only on the call. Sim.boot gives it to
// the node system with lockstep.WithLogger, last among the system's options (SIM-070, OBS-155).
// The pairs follow slog's handler rules: an empty attribute and a group without attributes are
// left out, and a group with an empty key is inlined.
type kernelHandler struct {
	e      *simEngine
	pairs  string // the With attributes, formatted: " key=value" each
	prefix string // the WithGroup names, each followed by ".": "" or "a.b."
}

// newKernelHandler returns the handler of the system of e's node.
func newKernelHandler(e *simEngine) *kernelHandler { return &kernelHandler{e: e} }

// Enabled reports whether level is slog.LevelInfo or above.
func (h *kernelHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelInfo
}

// Handle emits r's text as a kernel.log record of the node. A dead engine emits nothing, as its
// Tracer records nothing (SIM-120).
func (h *kernelHandler) Handle(_ context.Context, r slog.Record) error {
	if h.e.dead {
		return nil
	}
	b := make([]byte, 0, 128)
	b = append(b, r.Level.String()...)
	b = append(b, ' ')
	b = append(b, r.Message...)
	b = append(b, h.pairs...)
	r.Attrs(func(a slog.Attr) bool {
		b = appendPair(b, h.prefix, a)
		return true
	})
	h.e.n.Logf("%s", b)
	return nil
}

// WithAttrs returns a handler whose records carry as after h's attributes, under h's groups.
func (h *kernelHandler) WithAttrs(as []slog.Attr) slog.Handler {
	if len(as) == 0 {
		return h
	}
	b := []byte(h.pairs)
	for _, a := range as {
		b = appendPair(b, h.prefix, a)
	}
	h2 := *h
	h2.pairs = string(b)
	return &h2
}

// WithGroup returns a handler that puts the attributes added later in group name. An empty name
// returns h.
func (h *kernelHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := *h
	h2.prefix = h.prefix + name + "."
	return &h2
}

// appendPair appends " key=value" for a to b, with key prefixed by prefix, or one pair per
// attribute of a group, with the group's key added to the prefix.
func appendPair(b []byte, prefix string, a slog.Attr) []byte {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return b
	}
	if a.Value.Kind() == slog.KindGroup {
		if a.Key != "" {
			prefix += a.Key + "."
		}
		for _, g := range a.Value.Group() {
			b = appendPair(b, prefix, g)
		}
		return b
	}
	b = append(b, ' ')
	b = append(b, prefix...)
	b = append(b, a.Key...)
	b = append(b, '=')
	if a.Value.Kind() == slog.KindAny {
		return append(b, kernel.Describe(a.Value.Any())...)
	}
	return append(b, a.Value.String()...)
}
