// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
)

// benchSystem returns a New system with opts that discards its logs, and shuts it down when the
// benchmark ends. A system takes a few seconds to stop with the 100,000 actors of
// BenchmarkGaugeDepth, so the bound is a minute.
func benchSystem(b *testing.B, opts ...lockstep.Option) *lockstep.System {
	b.Helper()
	sys := lockstep.New(append([]lockstep.Option{lockstep.WithLogger(slog.New(slog.DiscardHandler))}, opts...)...)
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := sys.Shutdown(ctx); err != nil {
			b.Errorf("Shutdown: %v", err)
		}
	})
	return sys
}

// benchSend measures b.N Sends between two actors, each from inside the turn of the other, on a
// system built with opts.
func benchSend(b *testing.B, opts ...lockstep.Option) {
	b.ReportAllocs()
	sys := benchSystem(b, opts...)
	pp := newPingPong(b, sys)
	pp.timeout = time.Minute
	b.ResetTimer()
	pp.run(b, b.N)
}

// BenchmarkSendMetricsOff measures a Send between two actors on a system without metrics.
func BenchmarkSendMetricsOff(b *testing.B) { benchSend(b) }

// BenchmarkSendMetricsOn measures the same Send on a system with a no-op Metrics.
func BenchmarkSendMetricsOn(b *testing.B) { benchSend(b, lockstep.WithMetrics(noopMetrics{})) }

// BenchmarkDeadLetterDebugOff measures a Send to an address with no actor, a dead letter, on a
// system whose logger is at Info, so the Debug event of the dead letter is not logged.
func BenchmarkDeadLetterDebugOff(b *testing.B) {
	b.ReportAllocs()
	l := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	sys := benchSystem(b, lockstep.WithLogger(l))
	dead := lockstep.PIDAt[ping](sys.Node(), "/user/none")
	b.ResetTimer()
	for range b.N {
		if err := lockstep.Send(sys, dead, ping{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLogf measures Context.Logf with a logger on a discarding handler, which is not enabled
// at any level, and with one on a text handler writing to io.Discard.
func BenchmarkLogf(b *testing.B) {
	for _, tc := range []struct {
		name    string
		handler slog.Handler
	}{
		{"discard", slog.DiscardHandler},
		{"text", slog.NewTextHandler(io.Discard, nil)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			// One turn makes all the calls, and may outlast the watchdog's timeout.
			sys := benchSystem(b, lockstep.WithLogger(slog.New(tc.handler)), lockstep.WithWatchdog(0))
			done := make(chan struct{}, 1)
			pid, err := lockstep.Spawn(sys, "logger", func() lockstep.Actor[int] {
				return &simActor[int]{receive: func(ctx *lockstep.Context[int], n int) {
					for i := range n {
						ctx.Logf("n=%d", i)
					}
					done <- struct{}{}
				}}
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			if err := lockstep.Send(sys, pid, b.N); err != nil {
				b.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(time.Minute):
				b.Fatalf("%d Logf calls took more than a minute", b.N)
			}
		})
	}
}

// BenchmarkGaugeDepth measures a call of the lockstep_mailbox_depth_total function on a system
// with 100,000 idle actors spread under 1,000 parents (101,000 actors in all).
func BenchmarkGaugeDepth(b *testing.B) {
	b.ReportAllocs()
	rec := &recMetrics{}
	sys := benchSystem(b, lockstep.WithMetrics(rec))
	spawnIdleTree(b, sys, 1000, 100)
	depth := rec.find(b, "lockstep_mailbox_depth_total").fn
	b.ResetTimer()
	var total int64
	for range b.N {
		total += depth()
	}
	b.StopTimer()
	if total != 0 {
		b.Fatalf("idle actors hold %d messages, want 0", total)
	}
}
