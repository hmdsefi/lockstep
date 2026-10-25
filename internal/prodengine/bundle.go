// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Engine returns the bundle for one system (SCH-260). Its Stop field is e.Stop.
func (e *Engine) Engine() engine.Engine {
	return e.bundle
}

// newBundle builds the bundle Engine returns (SCH-260). New calls it once, so every Engine call
// returns the same values, the epoch included.
func (e *Engine) newBundle() engine.Engine {
	admission := e.cfg.Admission
	if admission == nil {
		admission = admitAll{}
	}
	return engine.Engine{
		Node:      e.cfg.Node,
		Epoch:     newEpoch(),
		Blocking:  true,
		Scheduler: &e.s,
		Clock:     e.clock(),
		Effects:   e.effects(),
		Entropy:   entropy{},
		Tracer:    noTracer{},
		Admission: admission,
		Failer:    noFailer{},
		Waiter:    waiter{},
		Stop:      e.Stop,
		// Transport is nil: the system has no other nodes (SCH-230). Interposer is nil: the
		// runtime offers every message directly (ENG-150).
	}
}

// newEpoch returns a non-zero uint32 read with crypto/rand, reading again while it is 0
// (SCH-260, ACT-040).
func newEpoch() uint32 {
	var b [4]byte
	for {
		_, _ = crand.Read(b[:]) // never fails (crypto/rand.Read, Go 1.24+)
		if v := binary.LittleEndian.Uint32(b[:]); v != 0 {
			return v
		}
	}
}

// entropy is the bundle's Entropy (SCH-240, ENG-440). It is not deterministic by design.
type entropy struct{}

// Rand returns a new PCG stream seeded with 16 bytes from crypto/rand, so two calls never share a
// source. It panics for a label without the "lockstep/" prefix (ENG-100).
func (entropy) Rand(label string) *rand.Rand {
	if !strings.HasPrefix(label, "lockstep/") {
		panic(fmt.Sprintf("prodengine: Entropy label %q does not start with \"lockstep/\"", label))
	}
	var b [16]byte
	_, _ = crand.Read(b[:])                                                                            // never fails (crypto/rand.Read, Go 1.24+)
	return rand.New(rand.NewPCG(binary.LittleEndian.Uint64(b[:8]), binary.LittleEndian.Uint64(b[8:]))) //nolint:gosec // G404: seeded from crypto/rand; callers need a *rand.Rand
}

// noTracer is the bundle's Tracer: disabled (SCH-250). Production observability is logs and
// metrics, not trace records.
type noTracer struct{}

func (noTracer) Enabled() bool                 { return false }
func (noTracer) Record(string, ...engine.Attr) {}
func (noTracer) Describe(v any) string         { return fmt.Sprintf("%T", v) }

// admitAll is the bundle's Admission when Config.Admission is nil: it admits everything and
// ignores Bind (SCH-260, ENG-125).
type admitAll struct{}

func (admitAll) AdmitSpawn(bool) error                                  { return nil }
func (admitAll) AdmitSend() error                                       { return nil }
func (admitAll) Bind(func(engine.AdmissionChange), func() engine.Usage) {}

// noFailer is the bundle's Failer: it ignores misuse, which the runtime also returns or logs
// itself (SCH-260, ENG-130).
type noFailer struct{}

func (noFailer) Fail(error) {}

// waiter is the bundle's Waiter (SCH-260, ENG-135). The runtime calls it only outside engine
// context, so blocking the caller never blocks a worker.
type waiter struct{}

// Wait returns nil once done is closed, or ctx.Err() if ctx ends first.
func (waiter) Wait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
