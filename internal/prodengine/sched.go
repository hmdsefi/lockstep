// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// GlobalCheckInterval: every GlobalCheckInterval-th schedtick, a worker takes from the global
// queue before its local queue, so actors made ready from outside the pool are not starved
// by actors that keep readying each other on one worker.
const GlobalCheckInterval = 61

// scheduler is the engine's engine.Scheduler: the workers, their queues and the park registry.
// Its zero value is unusable; start initializes it.
type scheduler struct {
	throughput int         // budget passed to RunTurn (Config.Throughput)
	closing    atomic.Bool // set by stop; Ready and the workers check it (SCH-050, SCH-200)
	global     globalQueue
	workers    []worker
	goids      []uint64 // goids[i] is worker i's goroutine ID; written before start returns
	park       parkList
	exited     sync.WaitGroup // one count per running worker
}

var _ engine.LocalScheduler = (*scheduler)(nil)

// worker is one worker goroutine's state (SCH §3).
type worker struct {
	s     *scheduler
	index int    // 0 … Workers-1
	hint  uint32 // index+1, the value stored in Slot.Hint (SCH-080)
	local localRing
	wake  chan struct{} // capacity 1 (SCH-090)

	// pushing is set while one goroutine writes local's tail end: a push, or the worker's own
	// pop, steal or drop. The ring allows one producer at a time, and ReadyFrom may run on a
	// goroutine other than the worker (see ReadyFrom).
	pushing atomic.Bool

	busy atomic.Bool   // running a turn or a callback (SCH-115); set and cleared by the worker
	turn publishedTurn // the turn in progress, for RunningTurns (SCH-270)

	// Owned by the worker goroutine: written and read only by it.
	schedtick uint32     // successful choices so far (SCH-070)
	victims   *rand.Rand // this worker's own stream for the first steal victim (SCH-070 step 4)

	parked bool // registered in s.park; guarded by s.park.mu

	_ [64]byte // keeps the next worker's ring indexes off this worker's cache line
}

// start initializes s and starts workers worker goroutines. It returns once every worker has
// recorded its goroutine ID, so InTurn is exact from then on.
func (s *scheduler) start(workers, throughput int) {
	s.throughput = throughput
	s.workers = make([]worker, workers)
	s.goids = make([]uint64, workers)
	s.park.stack = make([]*worker, 0, workers)
	var started sync.WaitGroup
	started.Add(workers)
	s.exited.Add(workers)
	for i := range s.workers {
		w := &s.workers[i]
		w.s = s
		w.index = i
		w.hint = uint32(i + 1) //nolint:gosec // G115: i < Workers, a goroutine count
		w.wake = make(chan struct{}, 1)
		w.victims = newVictimRand()
		go w.loop(&started)
	}
	started.Wait()
	if slices.Contains(s.goids, 0) {
		s.stop()
		panic("lockstep: prodengine: cannot parse a worker goroutine's ID from runtime.Stack")
	}
}

// newVictimRand returns a worker's own random stream for victim choice: a PCG generator seeded
// from 16 bytes of crypto/rand. Only its worker draws from it, so it needs no lock and draws
// allocate nothing. The determinism lint forbids the package-level functions of math/rand/v2 in
// every class (DL002), so the stream is explicit.
func newVictimRand() *rand.Rand {
	var seed [16]byte
	_, _ = crand.Read(seed[:]) // never fails (crypto/rand.Read, Go 1.24+)
	s1, s2 := binary.LittleEndian.Uint64(seed[:8]), binary.LittleEndian.Uint64(seed[8:])
	return rand.New(rand.NewPCG(s1, s2)) //nolint:gosec // G404: victim choice needs no cryptographic randomness
}

// stop is Stop's first call (SCH-200 steps 1, 3 and 4): mark closing, wake every parked worker
// and wait until every worker has exited.
func (s *scheduler) stop() {
	s.closing.Store(true)
	s.park.wakeAll()
	s.exited.Wait()
}

// Ready queues one turn of a at the tail of the global queue if its Slot was idle, and wakes a
// parked worker (SCH-050, SCH-060). While the engine is closing it does nothing.
func (s *scheduler) Ready(a engine.ActorRef) {
	if s.closing.Load() {
		return
	}
	if a.Slot().Ready() {
		s.pushGlobal(a)
	}
}

// ReadyFrom is Ready for an actor made ready during the turn of from (SCH-050, SCH-060). If
// from's turn is running, a's turn is queued at the tail of the local queue of the worker running
// it, the worker whose index plus 1 from's Slot.Hint holds; otherwise it goes to the global queue
// as with Ready.
//
// A call from any goroutine is safe. The runtime normally calls it on the worker that runs
// from's turn, but a handler that uses its Context from a helper goroutine makes the runtime call
// it from there (ACT-120). ReadyFrom never asks which goroutine calls it, since that would cost a
// stack trace per call (SCH-110); instead pushLocal lets one goroutine at a time push to a local
// queue, and a push that finds another one in progress goes to the global queue.
func (s *scheduler) ReadyFrom(a, from engine.ActorRef) {
	if s.closing.Load() {
		return
	}
	if !a.Slot().Ready() {
		return
	}
	if w := s.turnWorker(from); w != nil {
		w.pushLocal(a)
		return
	}
	s.pushGlobal(a)
}

// turnWorker returns the worker running from's turn, or nil if from's turn is not running
// (SCH-060). Hint is the worker's index plus 1, stored before RunTurn (SCH-080).
func (s *scheduler) turnWorker(from engine.ActorRef) *worker {
	if from == nil {
		return nil
	}
	slot := from.Slot()
	if st := slot.State(); st != engine.SlotRunning && st != engine.SlotRunningReady {
		return nil
	}
	h := slot.Hint.Load()
	if h == 0 || int(h) > len(s.workers) {
		return nil
	}
	return &s.workers[h-1]
}

// InTurn reports whether the caller is one of this engine's workers running a turn or a
// callback (SCH-115). A worker that is choosing its next entry or parked is not in a turn.
func (s *scheduler) InTurn() bool {
	id := goroutineID()
	for i, g := range s.goids {
		if g == id {
			return s.workers[i].busy.Load()
		}
	}
	return false
}

// pushGlobal appends a turn of a to the global queue and wakes a parked worker (SCH-090).
func (s *scheduler) pushGlobal(a engine.ActorRef) {
	s.global.pushTurn(a)
	s.park.wakeOne()
}

// pushCallback appends the callback fn to the global queue and wakes a parked worker (SCH-115,
// SCH-090). A worker runs fn once, in engine context. It may be called from any goroutine.
func (s *scheduler) pushCallback(fn func()) {
	s.global.pushCallback(fn)
	s.park.wakeOne()
}

// pushLocal appends a turn of a to w's local queue, which spills half of itself to the global
// queue when full (SCH-060), and wakes a parked worker (SCH-090). Any goroutine may call it: if
// another goroutine is writing w's local queue, a goes to the global queue instead, so the queue
// keeps a single producer. One uncontended CAS guards the push; it allocates nothing (SCH-110).
func (w *worker) pushLocal(a engine.ActorRef) {
	if !w.pushing.CompareAndSwap(false, true) {
		w.s.pushGlobal(a)
		return
	}
	w.local.push(a, &w.s.global)
	w.pushing.Store(false)
	w.s.park.wakeOne()
}

// lockLocal makes w's goroutine the only writer of w's local queue until unlockLocal. Only w's
// goroutine calls it. Another goroutine holds the flag only for one push, so w waits briefly at
// most, and only when a handler pushes from a helper goroutine.
func (w *worker) lockLocal() {
	for !w.pushing.CompareAndSwap(false, true) {
		runtime.Gosched()
	}
}

// unlockLocal ends lockLocal.
func (w *worker) unlockLocal() {
	w.pushing.Store(false)
}

// popLocal pops w's local queue under lockLocal. The ring's owner-side operations assume one
// writer of its slots, so a pop must not overlap a push from another goroutine.
func (w *worker) popLocal() (engine.ActorRef, bool) {
	w.lockLocal()
	a, ok := w.local.pop()
	w.unlockLocal()
	return a, ok
}

// loop is the worker goroutine: it records its goroutine ID, then runs entries until the engine
// closes. On the way out it drops the entries left in its own local queue; no other goroutine
// pops a worker's local queue except to steal (SCH-200 step 3).
func (w *worker) loop(started *sync.WaitGroup) {
	defer w.s.exited.Done()
	w.s.goids[w.index] = goroutineID()
	started.Done()
	for {
		ent, ok := w.next()
		if !ok {
			w.lockLocal()
			w.local.drop()
			w.unlockLocal()
			return
		}
		w.run(ent)
	}
}

// next returns the worker's next entry in SCH-070's order, parking while there is none. It
// reports false once the engine is closing.
func (w *worker) next() (globalEntry, bool) {
	for !w.s.closing.Load() {
		ent, ok := w.find(w.schedtick%GlobalCheckInterval == 0)
		if !ok {
			ent, ok = w.park()
		}
		if ok {
			w.schedtick++
			return ent, true
		}
	}
	return globalEntry{}, false
}

// find runs SCH-070 steps 1–4: the global queue first if globalFirst, then the local queue,
// the global queue and stealing.
func (w *worker) find(globalFirst bool) (globalEntry, bool) {
	if globalFirst {
		if ent, ok := w.s.global.pop(); ok {
			return ent, true
		}
	}
	if a, ok := w.popLocal(); ok {
		return globalEntry{actor: a}, true
	}
	if ent, ok := w.s.global.pop(); ok {
		return ent, true
	}
	if a, ok := w.steal(); ok {
		return globalEntry{actor: a}, true
	}
	return globalEntry{}, false
}

// steal runs SCH-070 step 4: starting at a victim drawn uniformly from 0 … Workers-1, it visits every other worker once in
// increasing index order modulo Workers, moves the oldest half of the first non-empty local
// queue to w's own and takes the first of them. w's local queue is usually empty when steal runs
// (step 2 found nothing); a push from another goroutine may have refilled it since, and then
// grabHalf moves fewer entries or none. The grab writes w's tail, so it runs under lockLocal.
func (w *worker) steal() (engine.ActorRef, bool) {
	ws := w.s.workers
	n := len(ws)
	if n == 1 {
		return nil, false
	}
	v := w.victims.IntN(n)
	for range n {
		if v != w.index {
			w.lockLocal()
			moved := ws[v].local.grabHalf(&w.local)
			w.unlockLocal()
			// Another thief may empty w's queue first; then try the next victim.
			if moved > 0 {
				if a, ok := w.popLocal(); ok {
					return a, true
				}
			}
		}
		if v++; v == n {
			v = 0
		}
	}
	return nil, false
}

// park runs SCH-090 for a worker that found no work: register, look again (steps 2–4), and
// only if that finds nothing block until woken. It returns an entry the second look found, or
// false after a wakeup or when the engine is closing; next then starts over at step 1.
func (w *worker) park() (globalEntry, bool) {
	p := &w.s.park
	p.register(w)
	// Stop marks closing before it wakes the registered workers, so a worker that registered
	// too late to be woken sees the mark here.
	if w.s.closing.Load() {
		p.unregister(w)
		return globalEntry{}, false
	}
	// A push that happened before the registration was missed by its wakeOne (the count was
	// 0); this second look finds it. A later push sees the count and wakes a worker.
	if ent, ok := w.find(false); ok {
		w.leavePark()
		return ent, true
	}
	<-w.wake
	p.unregister(w) // in case a stale token woke w while it was still registered
	return globalEntry{}, false
}

// leavePark unregisters w after its second look in park found work. If a pusher unregistered w
// first, it chose w to wake for its own entry, which w may not be about to run; w passes that
// wakeup on to another parked worker, so the entry is not left with every other worker parked.
func (w *worker) leavePark() {
	if p := &w.s.park; !p.unregister(w) {
		p.wakeOne()
	}
}

// run runs one entry as engine context: a callback (SCH-115) or a turn (SCH-080).
func (w *worker) run(ent globalEntry) {
	w.busy.Store(true)
	if ent.fn != nil {
		ent.fn()
	} else {
		w.runTurn(ent.actor)
	}
	w.busy.Store(false)
}

// runTurn runs one turn of a (SCH-080, ENG-050 step 2) and queues the next one if the Slot asks
// for it: behind the global queue after a yield, on w's local queue when the actor was made
// ready during the turn.
func (w *worker) runTurn(a engine.ActorRef) {
	slot := a.Slot()
	slot.Begin()
	slot.Hint.Store(w.hint)
	w.turn.set(a)
	more := a.RunTurn(w.s.throughput)
	w.turn.clear()
	if slot.End(more) {
		if more {
			w.s.pushGlobal(a)
		} else {
			w.pushLocal(a)
		}
	}
}

// parkList is the park registry of SCH §11: a mutex-protected stack of parked workers, plus a
// count that pushes read without the mutex.
//
// No wakeup is lost. A pusher publishes its entry and then reads n; a parking worker increments
// n and then looks at every queue again (park). Both orders are sequentially consistent, so
// either the pusher sees the registration and wakes a registered worker, or the worker's second
// look sees the entry. A woken worker either was blocked, and restarts its choice, or was still
// looking and found other work; then it passes the wakeup on (leavePark). Either way, after a
// push some worker that is not parked will look at the queues again.
type parkList struct {
	n     atomic.Int32 // len(stack); changed only with mu held
	mu    sync.Mutex
	stack []*worker // capacity Workers, so register never allocates
}

// register adds w to the parked workers.
func (p *parkList) register(w *worker) {
	p.mu.Lock()
	p.stack = append(p.stack, w)
	w.parked = true
	p.n.Add(1)
	p.mu.Unlock()
}

// unregister removes w from the parked workers if it is still there, and reports whether it
// was. It reports false when a waker already unregistered w.
func (p *parkList) unregister(w *worker) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !w.parked {
		return false
	}
	w.parked = false
	if i := slices.Index(p.stack, w); i >= 0 {
		p.stack = slices.Delete(p.stack, i, i+1)
	}
	p.n.Add(-1)
	return true
}

// wakeOne unregisters the most recently parked worker, if any, and sends on its wake channel
// without blocking (SCH-090). Every push calls it after the push.
func (p *parkList) wakeOne() {
	if p.n.Load() == 0 {
		return
	}
	p.mu.Lock()
	if len(p.stack) == 0 {
		p.mu.Unlock()
		return
	}
	w := p.stack[len(p.stack)-1]
	p.stack[len(p.stack)-1] = nil
	p.stack = p.stack[:len(p.stack)-1]
	w.parked = false
	p.n.Add(-1)
	p.mu.Unlock()
	signal(w.wake)
}

// wakeAll unregisters every parked worker and sends on each wake channel (SCH-200 step 3).
func (p *parkList) wakeAll() {
	p.mu.Lock()
	for i, w := range p.stack {
		w.parked = false
		signal(w.wake)
		p.stack[i] = nil
	}
	p.n.Add(-int32(len(p.stack))) //nolint:gosec // G115: len(stack) <= Workers, a goroutine count
	p.stack = p.stack[:0]
	p.mu.Unlock()
}

// signal leaves a token in c unless one is there already. A token already there wakes the
// worker just as well.
func signal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// stackBufs holds the buffers goroutineID passes to runtime.Stack. The buffer escapes to the
// heap, so a buffer on goroutineID's stack would cost an allocation on every InTurn call.
var stackBufs = sync.Pool{New: func() any { return new([64]byte) }}

// goroutineID returns the calling goroutine's ID, parsed from the first line of its stack
// trace, "goroutine <id> [" (SCH-270, as ENG-410), or 0 if that cannot be parsed (no goroutine
// has ID 0). It does not allocate in steady state.
func goroutineID() uint64 {
	buf, _ := stackBufs.Get().(*[64]byte)
	id := parseGoroutineID(buf[:runtime.Stack(buf[:], false)])
	stackBufs.Put(buf)
	return id
}

// parseGoroutineID returns the number in b's prefix "goroutine <id> ", or 0.
func parseGoroutineID(b []byte) uint64 {
	const prefix = "goroutine "
	if len(b) <= len(prefix) || string(b[:len(prefix)]) != prefix {
		return 0
	}
	var id uint64
	for _, c := range b[len(prefix):] {
		switch {
		case c == ' ':
			return id
		case '0' <= c && c <= '9':
			id = id*10 + uint64(c-'0')
		default:
			return 0
		}
	}
	return 0
}
