// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// actorCell is the untyped view of a cell: the registry stores cells as this interface (in an
// any), and the system drives them without knowing M. The dynamic type is always *cell[M], so
// Lookup[M] asserts it back (ACT §11).
type actorCell interface {
	engine.ActorRef
	// msgType returns reflect.TypeFor[M](): the exact type identity of ACT-530, for Lookup's
	// ErrWrongType message and for path addresses (ACT-440).
	msgType() reflect.Type
	// offerUser appends e to the user mailbox and schedules the cell (ENG-350). from is the cell
	// whose turn enqueues, or nil. It reports false, and enqueues nothing, when the mailbox is
	// closed: the cell has completed its final stop (MBX-150 (b)).
	offerUser(e entry, from engine.ActorRef) bool
	// pushSystem appends m to the system queue and schedules the cell (ENG-350); a closed queue
	// drops m. from is the cell whose turn pushes, or nil.
	pushSystem(m sysMsg, from engine.ActorRef)
	// requestStop puts stop{reason} on the system queue unless the cell already has a stop
	// request (ACT-710, SUP-050). from is the cell whose turn asks, or nil.
	requestStop(reason StopReason, from engine.ActorRef)
}

// cellState is a cell's lifecycle state as the turn loop sees it. Plan 1.3's state machine (SUP)
// replaces it.
type cellState uint8

const (
	cellStarting cellState = iota // begin not processed yet: user messages wait (ACT-340)
	cellRunning                   // the incarnation accepts user messages (ENG-310 step 2)
	cellFailed                    // a handler failed: no further user message is popped
	cellStopping                  // the stop sequence runs (SUP-060): no user message is popped
	cellStopped                   // the final stop completed; the queues are closed
)

// cell is one actor: the engine.ActorRef that engines schedule (ENG-300). The same cell, with the
// same Slot, serves every incarnation for the PID's lifetime.
//
// Concurrency: mu guards the two queues, which any goroutine may push to while one engine
// goroutine runs the cell's turn, and the fields under "guarded by mu"; the Slot keeps turns from
// overlapping. The fields under "turn-owned" are read and written only by RunTurn. No code that
// holds mu takes the ask table's lock, resolves an ask or calls the engine; only Spawn holds a new
// cell's mu across Registry.Add (lock order mu, then the registry's). addr is written once, by
// Spawn while it holds mu and before the cell's first Ready; code that reaches the cell through
// the registry reads it only after taking mu (as every enqueue does), and turns read it freely.
type cell[M any] struct {
	sys     *System
	addr    Address
	parent  Address
	factory func() Actor[M]
	slot    engine.Slot // a named field: ActorRef needs a Slot method (ENG-300)

	mu   sync.Mutex
	sysq fifo[sysMsg] // system queue (MBX-010's SystemQueue in plan 1.3)
	user fifo[entry]  // user mailbox (MBX's mailbox.Queue[entry] in plan 1.3)

	// guarded by mu
	closed  bool   // both queues are closed: the final stop drained them (SUP-060 step 5)
	stopReq bool   // a stop request was queued; later ones are discarded (SUP-050, ACT-710)
	liveInc uint32 // the live incarnation's number, 0 while none is live: Pipe checks it (MSG-610)

	// turn-owned
	state   cellState
	inc     uint32 // incarnation number: 0 at spawn, +1 per started incarnation (SUP-020)
	actor   Actor[M]
	ctx     *Context[M]
	starter Starter[M]        // the incarnation's optional interfaces, asserted once (ACT-100)
	stopper Stopper[M]        // called by the stop sequence (SUP-060)
	signals SignalReceiver[M] // called by deliverSignal
	failure error             // the error handlerFailed got; supervision's in plan 1.3

	// turn-owned, from cellStopping on (stop.go)
	stopReason StopReason // the reason of the one stop request
	waitChild  Address    // the child whose stopped notice the stop sequence waits for, or zero
}

// Address returns the actor's address (engine.ActorRef).
func (c *cell[M]) Address() engine.Address { return c.addr.engine() }

// msgType returns the cell's message type M (actorCell).
func (*cell[M]) msgType() reflect.Type { return reflect.TypeFor[M]() }

// Slot returns the cell's scheduling state (engine.ActorRef).
func (c *cell[M]) Slot() *engine.Slot { return &c.slot }

// pushSystem appends m to the system queue and then schedules the cell (ENG-350). from is the
// cell whose turn pushes, or nil. After the final stop the queue is closed and m is dropped
// (SUP-060 step 5).
func (c *cell[M]) pushSystem(m sysMsg, from engine.ActorRef) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.sysq.push(m)
	c.mu.Unlock()
	c.sys.ready(c, from)
}

// offerUser appends e to the user mailbox and then schedules the cell (ENG-350). from is the cell
// whose turn enqueues, or nil (actorCell). It reports false, and enqueues nothing, when the
// mailbox is closed (MBX-150 (b)). Plan 1.3's mailbox policies decide here whether the entry fits.
func (c *cell[M]) offerUser(e entry, from engine.ActorRef) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.user.push(e)
	c.mu.Unlock()
	c.sys.ready(c, from)
	return true
}

// offerSelf appends the Self entry e of incarnation e.inc to the user mailbox and schedules the
// cell. It reports false, and enqueues nothing, when that incarnation has ended or the mailbox is
// closed (MSG-610). The mailbox always admits a Self entry of the live incarnation (MBX-030).
func (c *cell[M]) offerSelf(e entry) bool {
	c.mu.Lock()
	if c.closed || c.liveInc != e.inc {
		c.mu.Unlock()
		return false
	}
	c.user.push(e)
	c.mu.Unlock()
	c.sys.ready(c, nil)
	return true
}

// requestStop puts stop{reason} on the system queue and schedules the cell, unless the cell
// already has a stop request, queued or in progress, or has stopped: the first reason wins
// (SUP-050, ACT-710). from is the cell whose turn asks, or nil (actorCell).
func (c *cell[M]) requestStop(reason StopReason, from engine.ActorRef) {
	c.mu.Lock()
	if c.stopReq || c.closed {
		c.mu.Unlock()
		return
	}
	c.stopReq = true
	c.sysq.push(stop{reason: reason})
	c.mu.Unlock()
	c.sys.ready(c, from)
}

// closeQueues closes both queues for the final stop and empties them (SUP-060 step 5): it
// returns the Sent entries of the user mailbox in queue order, for dead letters, and discards the
// Self entries and every system message (MBX-160). Later enqueues are refused.
func (c *cell[M]) closeQueues() []entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var sent []entry
	for {
		e, ok := c.user.pop()
		if !ok {
			break
		}
		if e.kind == entrySent {
			sent = append(sent, e)
		}
	}
	c.sysq = fifo[sysMsg]{}
	return sent
}

// handlerFailed is the one place a failed handler goes (ENG-330): a recovered panic of the
// factory, Started or Receive, or a nil actor from the factory. Until supervision (plan 1.3) it
// keeps err and stops the cell from popping user messages.
func (c *cell[M]) handlerFailed(err error) {
	c.failure = err
	c.state = cellFailed
}

// ready schedules a after an enqueue (ENG-350): through ReadyFrom when the enqueue happens during
// from's turn and the engine's Scheduler is a LocalScheduler, otherwise through Ready.
func (s *System) ready(a, from engine.ActorRef) {
	if from != nil {
		if ls, ok := s.eng.Scheduler.(engine.LocalScheduler); ok {
			ls.ReadyFrom(a, from)
			return
		}
	}
	s.eng.Scheduler.Ready(a)
}

// errNilActor is the failure of a factory that returns nil (SUP-040).
var errNilActor = errors.New("lockstep: factory returned nil actor")

// handlerPanic is the error of a recovered panic in actor code, until SUP's PanicError (plan 1.3)
// replaces it.
type handlerPanic struct{ value any }

func (p *handlerPanic) Error() string { return fmt.Sprintf("lockstep: panic: %v", p.value) }

// sysMsg is one system-queue message: begin, signals (signalMsg), stop and stopped (stop.go).
type sysMsg interface{ isSysMsg() }

// begin starts the first incarnation (ACT-340, SUP-030).
type begin struct{}

func (begin) isSysMsg() {}

// entryKind tells how an entry reached the user mailbox (MBX).
type entryKind uint8

const (
	entrySent entryKind = iota + 1 // Send or Ask: counts against capacity in plan 1.3
	entrySelf                      // a piped reply, timer message or effect result of incarnation inc
)

// entry is one user-mailbox element, MBX §11's shape without plan 1.3's seq and timer fields.
type entry struct {
	msg  any     // the M
	from Address // the sender
	ask  Address // MBX-092's ask address of a request, zero otherwise; Ask sets it
	inc  uint32  // entrySelf's incarnation (MBX-170); Pipe sets it
	kind entryKind
}

// fifo is an unbounded first-in, first-out queue, the placeholder for plan 1.3's mailbox queues.
// It is not safe for concurrent use; the cell's mu guards it.
type fifo[T any] struct {
	buf  []T
	head int // index of the oldest element
}

// push appends v.
func (q *fifo[T]) push(v T) { q.buf = append(q.buf, v) }

// pop removes and returns the oldest element; ok is false when q is empty.
func (q *fifo[T]) pop() (v T, ok bool) {
	if q.head == len(q.buf) {
		return v, false
	}
	var zero T
	v, q.buf[q.head] = q.buf[q.head], zero
	q.head++
	switch {
	case q.head == len(q.buf):
		q.buf, q.head = q.buf[:0], 0
	case q.head >= 32 && q.head*2 >= len(q.buf):
		n := copy(q.buf, q.buf[q.head:])
		clear(q.buf[n:])
		q.buf, q.head = q.buf[:n], 0
	}
	return v, true
}

// size returns the number of elements.
func (q *fifo[T]) size() int { return len(q.buf) - q.head }
