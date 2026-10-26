// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// AT-OVL-11, OVL-060: on a production System built by New, 64 goroutines spawn from *System concurrently
// until refused with WithMaxActors(1000): exactly 990 succeed (limit 1000 minus its reserve of
// 10), every refusal is ErrActorCap, and the live count ends at 990.
func TestATOVL11CapUnderConcurrentSpawns(t *testing.T) {
	const (
		limit      = 1000
		reserve    = 10
		goroutines = 64
	)
	sys := newLocalSystem(t, WithMaxActors(limit)) // New builds on the production engine

	var (
		admitted atomic.Int64
		wg       sync.WaitGroup
		errs     = make(chan error, goroutines)
	)
	for range goroutines {
		wg.Go(func() {
			for {
				_, err := Spawn(sys, "", idle[any]())
				if err != nil {
					errs <- err
					return
				}
				admitted.Add(1)
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if !errors.Is(err, ErrActorCap) {
			t.Errorf("a spawn was refused with %v, want ErrActorCap", err)
		}
	}
	if got := admitted.Load(); got != limit-reserve {
		t.Errorf("%d spawns succeeded, want %d", got, limit-reserve)
	}
	if got := sys.liveActors(); got != limit-reserve {
		t.Errorf("live %d, want %d", got, limit-reserve)
	}
}
