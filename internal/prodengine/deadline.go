// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"math"
	"sync"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// A deadline handle is gen<<32 | shard<<dlSlotBits | slot: the slot's generation, its shard and
// its index in the shard's slab. A slot's generation is never 0 and changes each time the slot is
// freed, so a handle names one arming of one slot and is never 0.
const (
	dlSlotBits   = 24
	maxDLSlots   = 1 << dlSlotBits // slots per shard
	maxDLShards  = 256
	dlShardsPerW = 4 // shards per worker, before rounding up to a power of two
)

// deadlines is the bundle's Deadlines: the engine's one structure for ask timeouts. Each shard is
// a min-heap of slots ordered by expiry time, and Arm's key picks the shard, so concurrent callers
// rarely share a lock. A shard has one runtime timer, set no later than its earliest deadline:
// Disarm leaves it set, and when it fires with nothing due the shard sets it again for its new
// earliest deadline, if any. Freed slots are reused, so once a shard has grown, Arm and Disarm
// allocate nothing.
type deadlines struct {
	e      *Engine
	base   time.Time // the origin of expiry times, read on the monotonic clock
	shards []dlShard // a power of two of them
	mask   uint64    // len(shards) - 1: Arm's key masked with it picks the shard
}

var _ engine.Deadlines = (*deadlines)(nil)

// dlShard is one shard of the deadlines.
type dlShard struct {
	d    *deadlines
	run  func() // sh.expire, built once: the callback rt's firing pushes
	mu   sync.Mutex
	slab []dlSlot    // the slots; a handle's slot field indexes it
	free []int32     // freed slots, reused last in, first out
	heap []int32     // the armed slots, a min-heap on when
	rt   *time.Timer // the runtime timer; nil until the shard's first Arm
	set  int64       // when rt fires, or 0 while it is not set
	_    [64]byte    // keeps the next shard's lock off this shard's cache line
}

// dlSlot is one deadline.
type dlSlot struct {
	when int64          // expiry time in nanoseconds since base, at least 1
	x    engine.Expirer // what expires; nil while the slot is free
	gen  uint32         // the slot's generation, part of its handle
	pos  int32          // the slot's index in heap, -1 while it is not armed
}

// init prepares d for the engine e with the given number of workers.
func (d *deadlines) init(e *Engine, workers int) {
	n := 1
	for n < workers*dlShardsPerW && n < maxDLShards {
		n <<= 1
	}
	d.e, d.base, d.shards, d.mask = e, time.Now(), make([]dlShard, n), uint64(n-1) //nolint:gosec // G115: 1 <= n <= maxDLShards
	for i := range d.shards {
		sh := &d.shards[i]
		sh.d, sh.run = d, sh.expire
	}
}

// now returns the time since base in nanoseconds.
func (d *deadlines) now() int64 { return int64(time.Since(d.base)) }

// Arm arms a deadline in the shard key selects. While the engine is closing it arms nothing and
// returns 0, as AfterFunc arms nothing then (SCH-130).
func (d *deadlines) Arm(key uint64, dur time.Duration, x engine.Expirer) uint64 {
	si := key & d.mask
	sh := &d.shards[si]
	now := d.now()
	when := now + max(int64(dur), 0)
	if when < now { // overflow: a duration of centuries
		when = math.MaxInt64
	}
	when = max(when, 1)
	sh.mu.Lock()
	// The closing mark is read under the shard's lock: stop locks every shard after it is set, so
	// it clears every deadline armed before.
	if d.e.s.closing.Load() {
		sh.mu.Unlock()
		return 0
	}
	slot := sh.alloc()
	s := &sh.slab[slot]
	s.when, s.x = when, x
	sh.push(slot)
	if sh.set == 0 || when < sh.set {
		sh.setTimer(when, now)
	}
	h := uint64(s.gen)<<32 | si<<dlSlotBits | uint64(slot) //nolint:gosec // G115: 0 <= slot < maxDLSlots
	sh.mu.Unlock()
	return h
}

// Disarm stops the deadline with handle h if it is still armed. It leaves the shard's runtime
// timer set: firing early only sets it again.
func (d *deadlines) Disarm(h uint64) bool {
	si, slot := int(h>>dlSlotBits&(maxDLShards-1)), int(h&(maxDLSlots-1))
	if h == 0 || si >= len(d.shards) {
		return false
	}
	sh := &d.shards[si]
	sh.mu.Lock()
	if slot >= len(sh.slab) {
		sh.mu.Unlock()
		return false
	}
	s := &sh.slab[slot]
	if s.gen != uint32(h>>32) || s.pos < 0 {
		sh.mu.Unlock()
		return false
	}
	sh.remove(int(s.pos))
	sh.release(int32(slot)) //nolint:gosec // G115: slot < maxDLSlots
	sh.mu.Unlock()
	return true
}

// fire runs on the goroutine of sh's runtime timer: it pushes the callback that expires sh's due
// deadlines. While the engine is closing it runs nothing (SCH-130).
func (sh *dlShard) fire() {
	if sh.d.e.s.closing.Load() {
		return
	}
	sh.d.e.s.pushCallback(sh.run)
}

// expire is the callback of sh's runtime timer, run by a worker: it takes every due deadline out
// of the heap, sets the runtime timer for the earliest one left, and then, without the lock and
// unless the engine is closing, runs the Expire of each taken deadline in expiry order.
func (sh *dlShard) expire() {
	now := sh.d.now()
	var buf [8]engine.Expirer
	due := buf[:0]
	sh.mu.Lock()
	sh.set = 0
	for len(sh.heap) > 0 && sh.slab[sh.heap[0]].when <= now {
		slot := sh.heap[0]
		due = append(due, sh.slab[slot].x)
		sh.remove(0)
		sh.release(slot)
	}
	if len(sh.heap) > 0 {
		sh.setTimer(sh.slab[sh.heap[0]].when, now)
	}
	sh.mu.Unlock()
	if sh.d.e.s.closing.Load() {
		return
	}
	for _, x := range due {
		x.Expire()
	}
}

// stop clears every armed deadline and stops every runtime timer (SCH-200 step 2), so the pending
// count falls to 0 (SCH-140). The caller marked the engine closing first, so an Arm either armed
// its deadline before stop locks its shard and is cleared here, or arms nothing.
func (d *deadlines) stop() {
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.Lock()
		for len(sh.heap) > 0 {
			slot := sh.heap[len(sh.heap)-1]
			sh.remove(len(sh.heap) - 1)
			sh.release(slot)
		}
		if sh.rt != nil {
			sh.rt.Stop()
		}
		sh.set = 0
		sh.mu.Unlock()
	}
}

// pending returns the number of armed deadlines (SCH-140).
func (d *deadlines) pending() int {
	n := 0
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.Lock()
		n += len(sh.heap)
		sh.mu.Unlock()
	}
	return n
}

// setTimer sets sh's runtime timer to fire at when. The caller holds sh.mu.
func (sh *dlShard) setTimer(when, now int64) {
	sh.set = when
	if sh.rt == nil {
		sh.rt = time.AfterFunc(time.Duration(when-now), sh.fire)
		return
	}
	sh.rt.Reset(time.Duration(when - now))
}

// alloc returns a free slot, growing the slab when none is free. The caller holds sh.mu.
func (sh *dlShard) alloc() int32 {
	if n := len(sh.free); n > 0 {
		slot := sh.free[n-1]
		sh.free = sh.free[:n-1]
		return slot
	}
	if len(sh.slab) == maxDLSlots {
		panic("lockstep: prodengine: too many pending deadlines in one shard")
	}
	sh.slab = append(sh.slab, dlSlot{gen: 1, pos: -1})
	return int32(len(sh.slab) - 1) //nolint:gosec // G115: len(sh.slab) <= maxDLSlots
}

// release frees slot, which is not in the heap: it drops what would have expired, so it can be
// collected, and moves to the next generation, so the slot's old handle disarms nothing. The
// caller holds sh.mu.
func (sh *dlShard) release(slot int32) {
	s := &sh.slab[slot]
	s.x, s.pos = nil, -1
	if s.gen++; s.gen == 0 {
		s.gen = 1
	}
	sh.free = append(sh.free, slot)
}

// push adds slot to the heap. The caller holds sh.mu.
func (sh *dlShard) push(slot int32) {
	sh.heap = append(sh.heap, slot)
	i := len(sh.heap) - 1
	sh.slab[slot].pos = int32(i) //nolint:gosec // G115: the heap holds at most maxDLSlots slots
	sh.up(i)
}

// remove takes the slot at index i out of the heap. The caller holds sh.mu.
func (sh *dlShard) remove(i int) {
	n := len(sh.heap) - 1
	if i != n {
		sh.swap(i, n)
		if !sh.down(i, n) {
			sh.up(i)
		}
	}
	sh.heap = sh.heap[:n]
}

func (sh *dlShard) less(i, j int) bool {
	return sh.slab[sh.heap[i]].when < sh.slab[sh.heap[j]].when
}

func (sh *dlShard) swap(i, j int) {
	h := sh.heap
	h[i], h[j] = h[j], h[i]
	sh.slab[h[i]].pos = int32(i) //nolint:gosec // G115: the heap holds at most maxDLSlots slots
	sh.slab[h[j]].pos = int32(j) //nolint:gosec // G115: the heap holds at most maxDLSlots slots
}

func (sh *dlShard) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !sh.less(i, parent) {
			return
		}
		sh.swap(i, parent)
		i = parent
	}
}

// down moves the slot at index i down among the first n, and reports whether it moved.
func (sh *dlShard) down(i, n int) bool {
	start := i
	for {
		child := 2*i + 1
		if child >= n {
			break
		}
		if right := child + 1; right < n && sh.less(right, child) {
			child = right
		}
		if !sh.less(child, i) {
			break
		}
		sh.swap(i, child)
		i = child
	}
	return i > start
}
