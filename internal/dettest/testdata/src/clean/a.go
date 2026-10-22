package clean

import (
	"maps"
	"math/rand/v2"
	"slices"
	"time"
)

// Pick returns a choice drawn from r.
func Pick(r *rand.Rand, xs []int) int { return xs[r.IntN(len(xs))] }

// Keys returns the keys of m in sorted order.
func Keys(m map[string]int) []string { return slices.Sorted(maps.Keys(m)) }

// Twice doubles a duration.
func Twice(d time.Duration) time.Duration { return 2 * d }
