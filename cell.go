// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/mailbox"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// actorCell is the untyped view of a cell: the registry stores cells as this interface (in an
// any), and the system drives them without knowing M. The dynamic type is always *cell[M], so
// Lookup[M] asserts it back (ACT §11).
type actorCell interface {
	engine.ActorRef
	// msgType returns reflect.TypeFor[M](): the exact type identity of ACT-530, for Lookup's
	// ErrWrongType message and for path addresses (ACT-440).
	msgType() reflect.Type
	// estimate returns the actor's mailbox and state bytes (OVL-410, OVL-420).
	estimate() (mailbox, state int64)
	// offerUser offers the Sent entry e to the user mailbox without waiting and returns the
	// queue's outcome (MBX-040, MBX-050). It schedules the cell when the mailbox took an entry
	// (ENG-350). from is the cell whose turn enqueues, or nil. For DroppedOldest it also returns
	// the entry the mailbox evicted. The outcome is Closed, and nothing is enqueued, when the cell
	// has completed its final stop (MBX-150 (b)). forceFull makes the entry take the full branch of
	// the policy: it is true exactly when admission sheds the message (OVL-110, MBX-090).
	offerUser(e entry, from engine.ActorRef, forceFull bool) (mailbox.Outcome, entry)
	// blocksSenders reports whether the user mailbox's policy is BlockSender.
	blocksSenders() bool
	// userQueue returns the user mailbox, to list it while senders wait on it (MBX-075).
	userQueue() *mailbox.Queue[entry]
	// offerWait offers the Sent entry e of an outside sender to the BlockSender user mailbox and
	// waits while the mailbox is full, admit reports false or other senders wait, until the entry
	// is enqueued, the mailbox closes or ctx ends (MBX-070); then the outcome is Rejected and the
	// error ctx.Err(). waiting is called once, holding no lock, when the sender starts waiting. It
	// schedules the cell when the mailbox took the entry (ENG-350). The caller holds no lock.
	offerWait(ctx context.Context, e entry, admit func() bool, waiting func()) (mailbox.Outcome, error)
	// mailboxState returns the number of entries in the user mailbox, its capacity and its policy,
	// for the records of MBX §8.
	mailboxState() (length, capacity int, policy MailboxPolicy)
	// policy returns the user mailbox's policy; it takes no lock.
	policy() MailboxPolicy
	// pushSystem appends m to the system queue and schedules the cell (ENG-350); a closed queue
	// drops m. from is the cell whose turn pushes, or nil.
	pushSystem(m sysMsg, from engine.ActorRef)
	// offerSystem is pushSystem that reports whether the queue took m: false once the cell has
	// completed its final stop.
	offerSystem(m sysMsg, from engine.ActorRef) bool
	// requestStop puts stop{reason} on the system queue unless the cell already has a stop
	// request (ACT-710, SUP-050). from is the cell whose turn asks, or nil.
	requestStop(reason StopReason, from engine.ActorRef)
	// lifecycle returns the cell's state and incarnation number; any goroutine may call it.
	lifecycle() (supervise.State, uint32)
	// turnSeq returns the cell's turn counter, odd while a turn runs (OVL-520); any goroutine may
	// call it.
	turnSeq() uint64
	// markStuck marks the turn whose counter is seq, which started at start, stuck, unless that
	// turn has ended (OVL-520). The watchdog's adapter calls it.
	markStuck(seq uint64, start time.Time)
	// stuck reports whether the turn in progress is marked stuck (OVL-520); any goroutine may call
	// it, and it takes no lock.
	stuck() bool
	// tracks returns the list of the asks the actor tracks (MSG-340); the ask table uses it.
	tracks() *trackList
}

// cell is one actor: the engine.ActorRef that engines schedule (ENG-300). The same cell, with the
// same Slot, serves every incarnation for the PID's lifetime.
//
// Concurrency: the two queues are safe for concurrent use: any goroutine may offer to them while
// one engine goroutine runs the cell's turn, which alone pops. mu makes an enqueue atomic with
// the fields under "guarded by mu"; the Slot keeps turns from overlapping. The fields under
// "turn-owned" are read and written only by RunTurn. No code that holds mu takes the ask table's
// lock, resolves an ask or calls the engine; only Spawn holds a new cell's mu across the live
// index's add and Registry.Add (lock order mu, then a queue's lock, the live index's or the
// registry's; the registry's and the live index's are never held together). addr is written
// once, by Spawn while it holds mu and before the cell's first Ready; code that reaches the cell
// through the registry reads it only after taking mu (as every enqueue does), and turns read it
// freely.
type cell[M any] struct {
	sys     *System
	addr    Address
	parent  Address
	factory func() Actor[M]
	// throughput is WithActorThroughput's value, 0 if absent (SCH-040). Spawn sets it before the
	// cell is registered, so it never changes while turns run.
	throughput int
	slot       engine.Slot // a named field: ActorRef needs a Slot method (ENG-300)

	// The queues are set when the cell is built and never replaced: the mailbox keeps its capacity
	// and policy for the life of the cell, across restarts (MBX-010).
	sysq *mailbox.SystemQueue[sysMsg]
	user *mailbox.Queue[entry]

	mu sync.Mutex

	// mem is the cell's memory estimate (OVL-400): its mailbox bytes change under any goroutine's
	// offer and the turn's pops, its state bytes in turns.
	mem memCount

	// turns is the cell's turn counter, odd while a turn runs, and stuckAt the watchdog's mark of a
	// stuck turn, nil if none (OVL-520, stuck.go). Only RunTurn adds to turns and clears stuckAt;
	// the watchdog's adapter reads turns and stores stuckAt.
	turns   atomic.Uint64
	stuckAt atomic.Pointer[stuckMark]

	// log is the actor logger, nil until the first Context.Logger or Logf call of any incarnation
	// builds it (OBS-030, context_log.go). It lives in the cell, so every incarnation shares it.
	log atomic.Pointer[slog.Logger]

	// live is the cell's place in the system's live index, which the depth gauges iterate (obs.go);
	// the index's lock guards it. It is unused when the system has metrics off.
	live liveSlot

	// tracked is the list of the asks the actor tracks, across incarnations (MSG-340); its own
	// lock guards it (asktable.go).
	tracked trackList

	// guarded by mu
	closed  bool   // both queues are closed: the final stop drained them (SUP-060 step 5)
	stopReq bool   // a stop request was queued; later ones are discarded (SUP-050, ACT-710)
	liveInc uint32 // the live incarnation's number, 0 while none is live: Pipe checks it (MSG-610)
	// seenState and seenInc mirror the turn-owned state and inc for lifecycle.
	seenState supervise.State
	seenInc   uint32
	// held and heldReady are the test-only hold hook (hold.go): the cell runs no turn while held,
	// and heldReady remembers a Ready or a turn that the hold swallowed.
	held, heldReady bool

	// turn-owned
	turn    uint64          // the value of turns during the turn in progress (stuck.go)
	state   supervise.State // changed only by setState, along the edges of SUP-010
	inc     uint32          // incarnation number: 0 at spawn, +1 per started incarnation (SUP-020)
	actor   Actor[M]
	ctx     *Context[M]
	rng     *rand.Rand        // the incarnation's stream, nil until ctx.Rand asks for it (TMR-120)
	starter Starter[M]        // the incarnation's optional interfaces, asserted once (ACT-100)
	stopper Stopper[M]        // called by the stop sequence (SUP-060)
	signals SignalReceiver[M] // called by deliverSignal
	// memSizer is the incarnation's MemSizer while estimates are on, else nil. handled counts the
	// handler calls of the cell and measured the value it had at the last MemSize call (OVL-420).
	memSizer          MemSizer
	handled, measured uint64

	// paused stops the popping of user entries while the mailbox keeps taking them (MBX-190):
	// ActorControl.PauseUser sets it during a turn of the incarnation (PER-250), and begin clears
	// it for every new incarnation (SUP-030). Turn-owned.
	paused bool

	// rebuildSpawn reports that the cell was spawned from a rebuild context, so its first Started
	// runs in one too (OVL §3). Set before the cell is registered, never changed.
	rebuildSpawn bool

	// watchSeq is the sequence number of the last Watch call of any incarnation (ACT-630). It
	// never decreases, so a Terminated queued for an earlier Watch never matches a later one.
	watchSeq uint64

	// turn-owned, from Stopping or Restarting on (stop.go, restart.go)
	stopReason StopReason // the reason of the one stop request
	endErr     error      // the error of the restart that ends the incarnation (SUP-190)
	waitChild  Address    // the child whose stopped notice the stop or restart waits for, or zero
	// endedWatched is the watched set of the incarnation that ended last, as it was when its watches
	// were removed (endIncarnationWork). A Terminated that reaches the cell outside Running, or is
	// left in its system queue at the final stop, is judged against it (watchedSet.skips).
	endedWatched watchedSet

	// tmu is the cell's timer lock (TMR §11): timers run on the engine's callbacks, which on the
	// local engine are other goroutines than the turn. It is taken before mu and the queue's lock
	// (a firing enqueues under it) and never while holding mu, so a turn that pops a timer message
	// has released mu before it takes tmu. nextTimer and timers are guarded by tmu.
	tmu       sync.Mutex
	nextTimer TimerID               // the last TimerID allocated: survives restarts (TMR-040)
	timers    map[TimerID]*timer[M] // timers.go

	// emu is the cell's effect lock (EFF §11): the engine's result and the timeout callbacks run in
	// engine context, which on the local engine is a goroutine other than the turn. It guards
	// effects and the cancelRun field of its entries, and is taken for table reads and writes only:
	// never while holding or taking another lock of the cell, never across an engine call or user
	// code.
	emu     sync.Mutex
	effects map[EffectID]*pendingEffect[M] // the incarnation's pending effects (effect.go)

	// sup is the cell's supervision of its children with the strategy of its WithStrategy option
	// (SUP-170 to SUP-290). Turn-owned.
	sup supervisor
	// kept holds, in arrival order, the signals and the children's failed and ended notices that
	// reached the cell while Suspended (SUP-160). Turn-owned.
	kept []sysMsg
}

// newCell returns a cell whose user mailbox has the capacity and policy mbx, which the caller has
// validated, and whose system queue is unbounded (MBX-010). strategy, also validated, supervises
// the cell's children.
func newCell[M any](sys *System, parent Address, factory func() Actor[M], mbx mailboxConfig, strategy Strategy) *cell[M] {
	return &cell[M]{
		mem:       memCount{sum: &sys.memBytes},
		sup:       supervisor{strategy: strategy},
		sys:       sys,
		parent:    parent,
		factory:   factory,
		sysq:      mailbox.NewSystemQueue[sysMsg](),
		user:      mailbox.New[entry](mbx.capacity, mbx.policy.queuePolicy()),
		state:     supervise.Starting, // SUP-010
		seenState: supervise.Starting,
	}
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
func (c *cell[M]) pushSystem(m sysMsg, from engine.ActorRef) { c.offerSystem(m, from) }

// offerSystem is pushSystem that reports whether the system queue took m: false, with m dropped,
// after the final stop (actorCell).
func (c *cell[M]) offerSystem(m sysMsg, from engine.ActorRef) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.sysq.Push(m)
	c.mu.Unlock()
	c.schedule(from)
	return true
}

// offerUser offers e to the user mailbox and then, if the mailbox took an entry, schedules the
// cell (ENG-350). from is the cell whose turn enqueues, or nil (actorCell). The queue's policy
// decides what a full mailbox, or a shed message (forceFull), does with the entry (MBX-040 to
// MBX-060). A closed cell refuses with Closed (MBX-150 (b)). It holds no lock when it returns, so
// the caller may call askRefused for the entries the outcome names. The entry's size counts toward
// the mailbox bytes once the queue has taken it, and an entry DropOldest evicted no longer counts
// (OVL-410). Both change under mu, which a pop takes too, so a pop never sees the entry uncounted,
// and a refused or dropped entry is never counted.
func (c *cell[M]) offerUser(e entry, from engine.ActorRef, forceFull bool) (mailbox.Outcome, entry) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return mailbox.Closed, entry{}
	}
	out, evicted := c.user.Offer(e, e.kind.queueKind(), forceFull)
	switch out {
	case mailbox.Enqueued:
		c.mem.addMailbox(e.size)
	case mailbox.DroppedOldest:
		c.mem.addMailbox(e.size - evicted.size)
	}
	c.mu.Unlock()
	if out == mailbox.Enqueued || out == mailbox.DroppedOldest {
		c.schedule(from)
	}
	return out, evicted
}

// blocksSenders reports whether the user mailbox's policy is BlockSender (actorCell).
func (c *cell[M]) blocksSenders() bool { return c.user.Policy() == mailbox.BlockSender }

// userQueue returns the user mailbox (actorCell).
func (c *cell[M]) userQueue() *mailbox.Queue[entry] { return c.user }

// tracks returns the list of the asks the actor tracks (actorCell).
func (c *cell[M]) tracks() *trackList { return &c.tracked }

// offerWait offers e to the BlockSender user mailbox and waits for a slot (actorCell). It takes no
// cell lock: a waiter holding mu would keep the turn from popping and so from freeing a slot. The
// queue's own closed state refuses the entry after the final stop, which closes the queue under
// mu (MBX-150 (b)). A slot handed over by the turn's Pop or by Recheck comes with no Ready of its
// own, so the cell is scheduled here; the Slot ignores a Ready for a cell that is scheduled or
// running already. The entry's size counts toward the mailbox bytes from before the offer, since
// a slot can be handed to it and the entry popped before OfferWaitNotify returns: a waiting
// sender's entry counts toward MailboxBytes while it waits (OVL-410).
func (c *cell[M]) offerWait(ctx context.Context, e entry, admit func() bool, waiting func()) (mailbox.Outcome, error) {
	c.mem.addMailbox(e.size) // counted before the entry can be popped; taken back when it is not enqueued (OVL-410)
	out, _, err := c.user.OfferWaitNotify(ctx, e, admit, waiting)
	if out == mailbox.Enqueued {
		c.schedule(nil)
	} else {
		c.mem.addMailbox(-e.size)
	}
	return out, err
}

// policy returns the user mailbox's policy without taking a lock (actorCell).
func (c *cell[M]) policy() MailboxPolicy { return MailboxPolicy(c.user.Policy()) }

// mailboxState returns the user mailbox's length, capacity and policy (actorCell).
func (c *cell[M]) mailboxState() (length, capacity int, policy MailboxPolicy) {
	return c.user.Len(), c.user.Capacity(), MailboxPolicy(c.user.Policy())
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
	out, _ := c.user.Offer(e, mailbox.Self, false)
	c.mu.Unlock()
	if out == mailbox.Closed {
		return false
	}
	c.schedule(nil)
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
	c.sysq.Push(stop{reason: reason})
	c.mu.Unlock()
	c.schedule(from)
}

// closeQueues closes both queues for the final stop and empties them (SUP-060 step 5, MBX-160): it
// returns the Sent entries and the Self entries of the user mailbox, each in queue order, and the
// system messages left that the actor discards as not running, in queue order: each signal message
// with its Watch sequence number (ACT-630), a cleanup failure's ChildFailed among them (SUP-110),
// each inject command (SFT-040) and each internal effect outcome (PER-240). The other system
// messages are discarded. Later enqueues are refused. The caller records what it returns, holding
// no lock.
func (c *cell[M]) closeQueues() (sent, self []entry, notRunning []sysMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for _, qe := range c.user.Close() {
		if qe.Kind == mailbox.Sent {
			c.mem.addMailbox(-qe.V.size)
			sent = append(sent, qe.V)
		} else {
			self = append(self, qe.V)
		}
	}
	for _, m := range c.sysq.Close() {
		switch m := m.(type) {
		case signalMsg:
			notRunning = append(notRunning, m)
		case cleanupFailed:
			notRunning = append(notRunning, m.asSignal())
		case inject, internalResult:
			notRunning = append(notRunning, m)
		}
	}
	return sent, self, notRunning
}

// schedule asks the engine to run c after an enqueue (ENG-350), unless c is held (hold.go). from is
// the cell whose turn enqueues, or nil. The caller holds no lock.
func (c *cell[M]) schedule(from engine.ActorRef) {
	c.mu.Lock()
	held := c.holding()
	c.mu.Unlock()
	if !held {
		c.sys.ready(c, from)
	}
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

// sysMsg is one system-queue message: a command or a notice of SUP §3 (lifecycle.go) or a signal
// (signalMsg).
type sysMsg interface{ isSysMsg() }

// fifo is an unbounded first-in, first-out queue for the guardians' messages (guardian.go). It
// is not safe for concurrent use; its owner's mu guards it.
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
