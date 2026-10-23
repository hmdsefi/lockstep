// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hmdsefi/lockstep/internal/overload"
)

func TestCapReserve(t *testing.T) {
	for _, tc := range []struct {
		limit, reserve int
	}{
		{1, 0},
		{2, 1},
		{10, 1},
		{150, 1},
		{200, 2},
	} {
		// Normal acquisitions stop at limit-reserve.
		c := overload.NewCap(tc.limit)
		normal := 0
		for {
			ok, live, limit, rsv := c.Acquire(false)
			if limit != tc.limit || rsv != tc.reserve {
				t.Fatalf("limit %d: Acquire reported limit %d, reserve %d; want %d, %d",
					tc.limit, limit, rsv, tc.limit, tc.reserve)
			}
			if live != normal {
				t.Fatalf("limit %d: Acquire reported live %d; want %d", tc.limit, live, normal)
			}
			if !ok {
				break
			}
			normal++
		}
		if want := tc.limit - tc.reserve; normal != want {
			t.Fatalf("limit %d: %d normal acquisitions succeeded; want %d", tc.limit, normal, want)
		}

		// Reserve acquisitions go on up to limit.
		reserved := 0
		for {
			ok, _, _, _ := c.Acquire(true)
			if !ok {
				break
			}
			reserved++
		}
		if reserved != tc.reserve {
			t.Fatalf("limit %d: %d reserve acquisitions succeeded; want %d", tc.limit, reserved, tc.reserve)
		}

		// Release frees exactly one slot.
		c.Release()
		if ok, _, _, _ := c.Acquire(true); !ok {
			t.Fatalf("limit %d: reserve Acquire refused after Release", tc.limit)
		}
		if ok, _, _, _ := c.Acquire(true); ok {
			t.Fatalf("limit %d: Release freed more than one slot", tc.limit)
		}
	}
}

// A normal Acquire after Release needs live < limit-reserve again, not just a free slot.
func TestCapReleaseKeepsReserve(t *testing.T) {
	c := overload.NewCap(10) // reserve 1
	for range 9 {
		if ok, _, _, _ := c.Acquire(false); !ok {
			t.Fatal("normal Acquire refused below limit-reserve")
		}
	}
	if ok, _, _, _ := c.Acquire(true); !ok {
		t.Fatal("reserve Acquire refused at limit-reserve")
	}
	c.Release()
	if ok, live, _, _ := c.Acquire(false); ok || live != 9 {
		t.Fatalf("normal Acquire at live 9 = (%v, live %d); want refused at live 9", ok, live)
	}
}

func TestCapReleaseBelowZero(t *testing.T) {
	c := overload.NewCap(2)
	c.Release()
	if ok, live, _, _ := c.Acquire(false); !ok || live != 0 {
		t.Fatalf("Acquire after a stray Release = (%v, live %d); want admitted at live 0", ok, live)
	}
}

func TestOVL060CapConcurrent(t *testing.T) {
	const (
		limit   = 1000
		workers = 64
		want    = 990 // limit - reserve
	)
	c := overload.NewCap(limit)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ok, _, _, _ := c.Acquire(false)
				if !ok {
					return
				}
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := admitted.Load(); got != want {
		t.Fatalf("%d normal acquisitions succeeded; want %d", got, want)
	}
	// The reserve is still there for reserve spawns, and nothing goes past the limit.
	for range 10 {
		c.Acquire(true)
	}
	if ok, live, _, _ := c.Acquire(true); ok || live != limit {
		t.Fatalf("reserve Acquire past the limit = (%v, live %d); want refused at live %d", ok, live, limit)
	}
}
