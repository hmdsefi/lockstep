// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package local_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine/local"
)

// BenchmarkLocalTurns measures one Ready to turn round trip of one actor whose RunTurn returns
// false (ENG §9).
func BenchmarkLocalTurns(b *testing.B) {
	e, err := local.New(local.Config{Node: "bench"})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Stop()
	sched := e.Engine().Scheduler
	turned := make(chan struct{})
	a := newFake("x", func(int) bool { turned <- struct{}{}; return false })
	b.ReportAllocs()
	for b.Loop() {
		sched.Ready(a)
		<-turned
	}
}
