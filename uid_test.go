// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// seqSource is a rand.Source that yields the values of vals one by one, then repeats the last.
type seqSource struct {
	vals []uint64
	i    int
}

func (s *seqSource) Uint64() uint64 {
	v := s.vals[min(s.i, len(s.vals)-1)]
	s.i++
	return v
}

// scriptedEntropy returns one stream whose Uint32 values are vals, and records the labels asked.
type scriptedEntropy struct {
	r      *rand.Rand
	labels []string
}

func (e *scriptedEntropy) Rand(label string) *rand.Rand {
	e.labels = append(e.labels, label)
	return e.r
}

// uint32Stream returns a rand.Rand whose Uint32 calls return vals in order. rand.Rand.Uint32
// returns the high 32 bits of the source's Uint64.
func uint32Stream(vals ...uint32) *rand.Rand {
	src := &seqSource{}
	for _, v := range vals {
		src.vals = append(src.vals, uint64(v)<<32)
	}
	return rand.New(src) //nolint:gosec // G404: a scripted stream is the point
}

// AT-ACT-04: ACT-040, ACT-045; ENG-540.
func TestATACT04UidSequence(t *testing.T) {
	t.Run("rollover on a manual system", func(t *testing.T) {
		sys, _ := newManualSystem(t)
		sys.seq = 1<<32 - 2

		// e: the first value of the stream the manual engine hands out that is neither 0 nor 1.
		h := fnv.New64a()
		_, _ = h.Write([]byte("lockstep/local/1/$uid"))
		r := rand.New(rand.NewPCG(1, h.Sum64())) //nolint:gosec // G404: the engine's own derivation
		var e uint64
		for e == 0 || e == 1 {
			e = uint64(r.Uint32())
		}

		want := []uint64{1<<32 | (1<<32 - 2), 1<<32 | (1<<32 - 1), e<<32 | 1}
		for i, w := range want {
			if got := sys.allocUID(); got != w {
				t.Errorf("allocation %d = %#x, want %#x", i, got, w)
			}
		}
		if got := sys.Incarnation(); got != 1 {
			t.Errorf("Incarnation() = %d after the rollover, want 1", got)
		}
		if got := sys.allocUID(); got != e<<32|2 {
			t.Errorf("allocation after the rollover = %#x, want %#x", got, e<<32|2)
		}
	})

	t.Run("an epoch of 0 or an epoch already used is drawn again", func(t *testing.T) {
		b := enginetest.New("local", 1).Engine()
		ent := &scriptedEntropy{r: uint32Stream(1, 0, 5)}
		b.Entropy = ent
		sys, err := newSystem(b, nil)
		if err != nil {
			t.Fatal(err)
		}
		sys.seq = 1<<32 - 1
		if got, want := sys.allocUID(), uint64(1)<<32|(1<<32-1); got != want {
			t.Fatalf("last uid of the first epoch = %#x, want %#x", got, want)
		}
		if got, want := sys.allocUID(), uint64(5)<<32|1; got != want {
			t.Errorf("first uid of the new epoch = %#x, want %#x", got, want)
		}
		if got := sys.Incarnation(); got != 1 {
			t.Errorf("Incarnation() = %d, want 1", got)
		}
		if want := []string{"lockstep/local/1/$uid"}; !slices.Equal(ent.labels, want) {
			t.Errorf("Entropy labels %q, want %q", ent.labels, want)
		}
	})

	t.Run("the stream is fetched once and an epoch is not used twice", func(t *testing.T) {
		b := enginetest.New("local", 1).Engine()
		ent := &scriptedEntropy{r: uint32Stream(5, 5, 6)}
		b.Entropy = ent
		sys, err := newSystem(b, nil)
		if err != nil {
			t.Fatal(err)
		}
		sys.seq = 1<<32 - 1
		sys.allocUID()
		if got, want := sys.allocUID(), uint64(5)<<32|1; got != want {
			t.Fatalf("first rollover uid = %#x, want %#x", got, want)
		}
		sys.seq = 1<<32 - 1
		sys.allocUID()
		// The stream yields 5 again, which this system has used: the draw repeats and takes 6.
		if got, want := sys.allocUID(), uint64(6)<<32|1; got != want {
			t.Errorf("second rollover uid = %#x, want %#x", got, want)
		}
		if len(ent.labels) != 1 {
			t.Errorf("Entropy.Rand called %d times, want 1", len(ent.labels))
		}
	})

	t.Run("10000 allocations from 8 goroutines are distinct", func(t *testing.T) {
		sys := newLocalSystem(t)
		const goroutines, each = 8, 1250
		got := make([][]uint64, goroutines)
		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out := make([]uint64, each)
				for i := range out {
					out[i] = sys.allocUID()
				}
				got[g] = out
			}()
		}
		wg.Wait()
		seen := make(map[uint64]bool, goroutines*each)
		for _, out := range got {
			for _, u := range out {
				if u == 0 {
					t.Fatal("allocated uid 0")
				}
				if seen[u] {
					t.Fatalf("uid %#x allocated twice", u)
				}
				seen[u] = true
			}
		}
		if len(seen) != goroutines*each {
			t.Errorf("%d distinct uids, want %d", len(seen), goroutines*each)
		}
	})
}

// ACT-040: seq starts at 1 for the first uid of the system, so the guardians take 1 and 2 and the
// first spawn takes 3.
func TestUIDAllocationOrder(t *testing.T) {
	sys, _ := newManualSystem(t)
	epoch := uint64(sys.Incarnation())
	if got, want := sys.allocUID(), epoch<<32|3; got != want {
		t.Errorf("first allocation after the guardians = %#x, want %#x", got, want)
	}
}

var _ engine.Entropy = (*scriptedEntropy)(nil)
