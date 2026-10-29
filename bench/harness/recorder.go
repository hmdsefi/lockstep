// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package harness

import (
	"math"
	"slices"
	"time"
)

// Recorder collects latency samples in nanoseconds.
type Recorder struct {
	base    time.Time
	samples []int64
}

// NewRecorder returns a recorder with room for capacity samples, measuring from base.
func NewRecorder(base time.Time, capacity int) *Recorder {
	return &Recorder{base: base, samples: make([]int64, 0, capacity)}
}

// Now returns the nanoseconds since base on the monotonic clock.
func (r *Recorder) Now() int64 {
	return time.Since(r.base).Nanoseconds()
}

// Add appends one sample. It allocates only when capacity is exceeded.
func (r *Recorder) Add(ns int64) {
	r.samples = append(r.samples, ns)
}

// Merge appends all samples of o.
func (r *Recorder) Merge(o *Recorder) {
	r.samples = append(r.samples, o.samples...)
}

// Percentile returns the nearest-rank percentile p (0 < p <= 100): the sorted sample at
// index ceil(p/100*n)-1. It returns 0 with no samples.
func (r *Recorder) Percentile(p float64) int64 {
	n := len(r.samples)
	if n == 0 {
		return 0
	}
	slices.Sort(r.samples)
	// p*n/100 equals p/100*n and is exact for a whole p. p/100*n is not: 7.0/100*100 is
	// 7.000000000000001, whose ceiling would pick rank 8 of 100 instead of 7.
	rank := int(math.Ceil(p * float64(n) / 100))
	rank = min(max(rank, 1), n)
	return r.samples[rank-1]
}
