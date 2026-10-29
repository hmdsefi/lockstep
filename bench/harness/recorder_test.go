// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package harness_test

import (
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/bench/harness"
)

// TestATBEN06Recorder covers AT-BEN-06: nearest-rank percentiles, a recorder with one sample and
// with none, Add without allocations within capacity, and Merge. It also checks that Len counts the
// samples (BEN-130 checks sample counts through it) and that Now measures from base.
func TestATBEN06Recorder(t *testing.T) {
	base := time.Now()

	t.Run("samples 1 to 100 in reverse", func(t *testing.T) {
		r := harness.NewRecorder(base, 100)
		for ns := int64(100); ns >= 1; ns-- {
			r.Add(ns)
		}
		if got := r.Len(); got != 100 {
			t.Errorf("Len() = %d, want 100", got)
		}
		for _, c := range []struct {
			p    float64
			want int64
		}{{50, 50}, {99, 99}, {100, 100}} {
			if got := r.Percentile(c.p); got != c.want {
				t.Errorf("Percentile(%v) = %d, want %d", c.p, got, c.want)
			}
		}
	})

	t.Run("one sample", func(t *testing.T) {
		r := harness.NewRecorder(base, 1)
		r.Add(7)
		for _, p := range []float64{0.1, 1, 50, 99, 99.9, 100} {
			if got := r.Percentile(p); got != 7 {
				t.Errorf("Percentile(%v) = %d, want 7", p, got)
			}
		}
	})

	t.Run("no samples", func(t *testing.T) {
		r := harness.NewRecorder(base, 10)
		if got := r.Len(); got != 0 {
			t.Errorf("Len() = %d, want 0", got)
		}
		for _, p := range []float64{1, 50, 99, 100} {
			if got := r.Percentile(p); got != 0 {
				t.Errorf("Percentile(%v) = %d, want 0", p, got)
			}
		}
	})

	t.Run("Add within capacity does not allocate", func(t *testing.T) {
		if raceEnabled {
			t.Skip("the race detector's instrumentation allocates")
		}
		// AllocsPerRun calls the function once more as a warm-up, so 101 calls in all.
		r := harness.NewRecorder(base, 101)
		if allocs := testing.AllocsPerRun(100, func() { r.Add(1) }); allocs != 0 {
			t.Errorf("Add allocates %v times per call within capacity, want 0", allocs)
		}
	})

	t.Run("Merge", func(t *testing.T) {
		a := harness.NewRecorder(base, 3)
		for _, ns := range []int64{3, 1, 2} {
			a.Add(ns)
		}
		b := harness.NewRecorder(base, 4)
		for _, ns := range []int64{7, 5, 4, 6} {
			b.Add(ns)
		}
		a.Merge(b)
		if got := a.Len(); got != 7 {
			t.Errorf("after Merge, Len() = %d, want 7", got)
		}
		// With the seven samples 1 to 7, nearest rank k covers p in ((k-1)*100/7, k*100/7], so the
		// middle of each range gives sample k only if a holds exactly these seven samples.
		for k := int64(1); k <= 7; k++ {
			p := (float64(k) - 0.5) * 100 / 7
			if got := a.Percentile(p); got != k {
				t.Errorf("after Merge, Percentile(%.2f) = %d, want %d", p, got, k)
			}
		}
		if got, n := b.Percentile(1), b.Len(); got != 4 || n != 4 {
			t.Errorf("Merge changed its argument: Percentile(1) = %d, Len() = %d, want 4 and 4", got, n)
		}
	})

	t.Run("Now", func(t *testing.T) {
		r := harness.NewRecorder(time.Now().Add(-time.Second), 0)
		first := r.Now()
		if first < int64(time.Second) {
			t.Errorf("Now() = %d, want at least %d for a base one second ago", first, int64(time.Second))
		}
		if second := r.Now(); second < first {
			t.Errorf("Now() went back from %d to %d", first, second)
		}
	})
}
