// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// ringSlot holds one engine.ActorRef as the two words of its interface value, each read and
// written atomically. A thief reads a slot while the owner may be reusing it; it discards the
// copy when its CAS on head then fails (SCH §11), so the reads must not be data races. Plain
// interface copies would be, which is why the words are split.
type ringSlot struct {
	tab, data atomic.Pointer[byte]
}

// store writes r into the slot.
func (s *ringSlot) store(r engine.ActorRef) {
	w := (*[2]*byte)(unsafe.Pointer(&r)) //nolint:gosec // G103: reads the two words of an interface value
	s.tab.Store(w[0])
	s.data.Store(w[1])
}

// load returns the slot's value. It is only meaningful when no store to the slot ran
// concurrently; callers validate that with a CAS on head.
func (s *ringSlot) load() (r engine.ActorRef) {
	w := (*[2]*byte)(unsafe.Pointer(&r)) //nolint:gosec // G103: writes the two words of an interface value
	w[0] = s.tab.Load()
	w[1] = s.data.Load()
	return r
}

// localRing is a worker's local run queue (SCH-060, SCH-070): a bounded FIFO ring that its
// owner pushes to and pops from, and that other workers steal half of. head and tail only grow
// (they wrap at 2^32); the entries are slots head..tail-1 modulo LocalQueueSize. The zero value
// is an empty ring.
//
// A slot keeps its last value after the entry is taken; the stale reference is overwritten by a
// later push, so a ring retains at most LocalQueueSize references.
type localRing struct {
	head atomic.Uint32 // next entry to take; advanced by CAS (owner or thief)
	tail atomic.Uint32 // next slot to fill; written only by the owner
	buf  [LocalQueueSize]ringSlot
}

// len returns the number of entries. It is exact when called by the owner with no thief active,
// and a snapshot otherwise.
func (q *localRing) len() int {
	return int(q.tail.Load() - q.head.Load())
}

// push appends a at the tail. When the ring is full it moves the oldest LocalQueueSize/2 entries
// plus a to the tail of g in one batch, in order, and reports true (SCH-060). Only the owner
// calls it.
func (q *localRing) push(a engine.ActorRef, g *globalQueue) (overflowed bool) {
	for {
		h := q.head.Load()
		t := q.tail.Load()
		if t-h < LocalQueueSize {
			q.buf[t%LocalQueueSize].store(a)
			q.tail.Store(t + 1)
			return false
		}
		// Full. Copy the oldest half out, then claim it by advancing head; a thief that moved head
		// first makes the CAS fail and the ring has room again, so retry.
		var batch [LocalQueueSize/2 + 1]engine.ActorRef
		for i := range uint32(LocalQueueSize / 2) {
			batch[i] = q.buf[(h+i)%LocalQueueSize].load()
		}
		if !q.head.CompareAndSwap(h, h+LocalQueueSize/2) {
			continue
		}
		batch[LocalQueueSize/2] = a
		g.pushBatch(batch[:])
		return true
	}
}

// pop removes and returns the oldest entry. It reports false when the ring is empty. Only the
// owner calls it.
func (q *localRing) pop() (engine.ActorRef, bool) {
	for {
		h := q.head.Load()
		if h == q.tail.Load() {
			return nil, false
		}
		a := q.buf[h%LocalQueueSize].load()
		if q.head.CompareAndSwap(h, h+1) {
			return a, true
		}
	}
}

// grabHalf moves the oldest ceil(n/2) of q's n entries to the tail of dst, in order, and returns
// how many it moved (SCH-070 step 4). It is called by the owner of dst, which is not q's
// owner, and moves nothing when q is empty or dst lacks room.
func (q *localRing) grabHalf(dst *localRing) int {
	for {
		h := q.head.Load()
		t := q.tail.Load()
		n := t - h
		if n > LocalQueueSize { // h and t were read across a change; read again
			continue
		}
		n -= n / 2
		if n == 0 {
			return 0
		}
		dt := dst.tail.Load()
		if dt-dst.head.Load()+n > LocalQueueSize {
			return 0
		}
		for i := range n {
			dst.buf[(dt+i)%LocalQueueSize].store(q.buf[(h+i)%LocalQueueSize].load())
		}
		if q.head.CompareAndSwap(h, h+n) {
			dst.tail.Store(dt + n)
			return int(n)
		}
	}
}

// globalEntry is one entry of the global queue: a turn of actor, or a callback fn. Exactly one
// field is set.
type globalEntry struct {
	actor engine.ActorRef
	fn    func()
}

// globalMinCap is the buffer size of a global queue's first growth.
const globalMinCap = 16

// globalQueue is the engine's global run queue (SCH-060, SCH-115): a mutex-protected FIFO ring of
// entries that doubles when full and never shrinks. The zero value is an empty queue.
type globalQueue struct {
	mu   sync.Mutex
	buf  []globalEntry
	head int // index of the oldest entry
	n    int // number of entries
}

// len returns the number of entries.
func (g *globalQueue) len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.n
}

// pushTurn appends a turn of a.
func (g *globalQueue) pushTurn(a engine.ActorRef) {
	g.mu.Lock()
	g.grow(1)
	g.buf[g.index(g.n)] = globalEntry{actor: a}
	g.n++
	g.mu.Unlock()
}

// pushCallback appends the callback fn.
func (g *globalQueue) pushCallback(fn func()) {
	g.mu.Lock()
	g.grow(1)
	g.buf[g.index(g.n)] = globalEntry{fn: fn}
	g.n++
	g.mu.Unlock()
}

// pushBatch appends a turn of each of as, in order, in one critical section (SCH-060).
func (g *globalQueue) pushBatch(as []engine.ActorRef) {
	g.mu.Lock()
	g.grow(len(as))
	for _, a := range as {
		g.buf[g.index(g.n)] = globalEntry{actor: a}
		g.n++
	}
	g.mu.Unlock()
}

// pop removes and returns the oldest entry. It reports false when the queue is empty.
func (g *globalQueue) pop() (globalEntry, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.n == 0 {
		return globalEntry{}, false
	}
	e := g.buf[g.head]
	g.buf[g.head] = globalEntry{} // drop the references
	g.head = g.index(1)
	g.n--
	return e, true
}

// index returns the buffer index of the entry i places after the oldest. The caller holds mu and
// the buffer is not empty.
func (g *globalQueue) index(i int) int {
	return (g.head + i) % len(g.buf)
}

// grow makes room for extra more entries by doubling the buffer until they fit, moving the
// entries to the start of the new buffer. The caller holds mu.
func (g *globalQueue) grow(extra int) {
	need := g.n + extra
	if need <= len(g.buf) {
		return
	}
	size := max(len(g.buf), globalMinCap)
	for size < need {
		size *= 2
	}
	buf := make([]globalEntry, size)
	for i := range g.n {
		buf[i] = g.buf[(g.head+i)%len(g.buf)]
	}
	g.buf, g.head = buf, 0
}
