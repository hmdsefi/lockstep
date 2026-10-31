// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// askShards is the number of shards of an ask table, a power of two.
const askShards = 32

// askTable is a system's ask state (MSG §11): the pending asks whose asker is this system, and the
// asks this system's actors track (MSG-340). It is split into shards by ask uid. A shard's mutex
// guards everything about the asks in it, so applying a reply attempt is atomic with every other
// resolution of the same ask (MSG-420), and asks in different shards share no lock. Each ask keeps
// its number, so the rare paths that list asks in counter order sort them (MSG-360, MSG-370). The
// asks one actor tracks are threaded on a list in its cell, in tracking order (trackList).
//
// Lock order: a shard's lock, then the engine's timer or deadline lock (stopping a timeout) or the
// tracer's lock; nothing that holds those calls back into the table. A track list's lock is taken
// with no shard lock held, except by untrackAll, which takes shard locks while it holds its list's.
type askTable struct {
	counter uint64 // the number of the last ask allocated: /temp/<counter> (MSG-300 step 4); guarded by the uid lock
	shards  [askShards]askShard
}

// askShard is one shard of an ask table.
type askShard struct {
	mu      sync.Mutex
	local   map[uint64]*pendingAsk  // this system's asks that are pending or tracked, by uid
	remote  map[Address]*pendingAsk // asks of other nodes that an actor of this system tracks
	pending *pendingAsk             // the first of the shard's pending asks, listed in no order
	_       [96]byte                // keeps the next shard's lock off this shard's cache line
}

// pendingAsk is one ask in the table: a pending ask of this system (MSG-370), one that resolved
// and is still tracked, or, without a reply, an ask of another node that an actor of this system
// tracks (MSG-340). A Reply holds the entry of its ask, so an ask allocates it with the reply.
type pendingAsk struct {
	sys     *System       // the asker system; nil for an ask of another node
	addr    Address       // the ask address
	n       uint64        // the ask's number, /temp/<n>; 0 for an ask of another node
	asker   Address       // the asking actor, or the system address for a *System sender
	target  Address       // the request's target; the zero Address for NewReply
	timeout time.Duration // the ask's timeout (MSG-320)
	reply   resolver      // the typed Reply, seen without its R; nil for an ask of another node

	// Guarded by the shard's lock; the tracker's list also reads tracker without it.
	pending    bool                      // from newAsk until it resolves or its builder panics
	untracked  bool                      // a claim untracked the ask, which is never tracked again
	prev, next *pendingAsk               // the shard's list of pending asks
	tracker    atomic.Pointer[trackList] // the list of the actor that tracks the ask; nil if none

	// Guarded by the tracker's list lock: the ask's place on that list.
	tprev, tnext *pendingAsk
	linked       bool

	// The timeout timer, armed after the builder by Ask or NewReply: a deadline of the engine's
	// Deadlines, or the Cancel of a Clock.AfterFunc timer on an engine without them. timer decides
	// which of armTimeout and the resolution stops it.
	timer    atomic.Uint32
	deadline uint64
	cancel   engine.Cancel
}

// States of an ask's timer: armTimeout moves it from timerUnarmed to timerArmed; the resolution
// moves it to timerStopped and stops the timer if it was armed. armTimeout stops the timer itself
// when the resolution came first (a builder that leaked the ReplyTo to another goroutine).
const (
	timerUnarmed uint32 = iota
	timerArmed
	timerStopped
)

// Expire is the ask's deadline (MSG-320): it resolves p with ErrTimeout if p is still pending.
func (p *pendingAsk) Expire() { p.sys.askTimedOut(p) }

// resolver is the untyped view of a *Reply[R] (MSG §11), so reply attempts that arrive through
// the transport resolve an ask without knowing R.
type resolver interface {
	// resolve stores the result, v asserted to R, and closes the reply's done channel. It reports
	// whether this call resolved the reply.
	resolve(v any, err error) bool
	// fire runs the continuations of the reply's Pipe calls (MSG-610). The caller has resolved
	// the reply and released the table's lock.
	fire()
	// wrongType returns MSG-420's ErrWrongType error for an answer v that is not an R, and nil
	// for an R or a nil v (the zero R). It runs no user code.
	wrongType(ask Address, v any) error
}

// trackList is the asks one actor tracks, in tracking order (MSG-340): a list threaded through
// their table entries, so tracking an ask of this system allocates nothing. Each cell has one for
// its lifetime. An entry is on the list while the list is its tracker, and for a moment after a
// shard claimed it, until the claimant, which unlinks it once it has released the shard's lock.
type trackList struct {
	mu         sync.Mutex
	head, tail *pendingAsk
}

// link appends p, unless p is on the list already or a claim untracked it since track made this
// list its tracker.
func (l *trackList) link(p *pendingAsk) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p.linked || p.tracker.Load() != l {
		return
	}
	p.tprev, p.tnext, p.linked = l.tail, nil, true
	if l.tail == nil {
		l.head = p
	} else {
		l.tail.tnext = p
	}
	l.tail = p
}

// unlink removes p from the list if p is on it.
func (l *trackList) unlink(p *pendingAsk) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !p.linked {
		return
	}
	if p.tprev == nil {
		l.head = p.tnext
	} else {
		p.tprev.tnext = p.tnext
	}
	if p.tnext == nil {
		l.tail = p.tprev
	} else {
		p.tnext.tprev = p.tprev
	}
	p.tprev, p.tnext, p.linked = nil, nil, false
}

// untracked is a tracked entry that a shard claimed. The claimant unlinks it from its list once it
// has released the shard's lock.
type untracked struct {
	p *pendingAsk
	l *trackList // nil when nothing was claimed
}

func (u untracked) unlink() {
	if u.l != nil {
		u.l.unlink(u.p)
	}
}

// how is the kind of a resolution attempt; it decides the record (MSG §8).
type how uint8

const (
	byReply   how = iota + 1 // a reply attempt (MSG-420): lockstep.reply resolved, or late when not pending
	byRouting                // Ask's routing outcome (MSG-310): lockstep.reply resolved; nothing when not pending
	byTimeout                // the timer (MSG-320): lockstep.timeout; nothing when not pending
	byStop                   // the system stopped (MSG-360): lockstep.reply resolved; nothing when not pending
)

// resolution is one attempt to resolve an ask.
type resolution struct {
	how  how
	from Address // the replier (byReply) or the target (byRouting)
	v    any     // the answer; nil with an error
	err  error
}

// shard returns the shard of the ask with the given uid.
func (t *askTable) shard(uid uint64) *askShard { return &t.shards[uid&(askShards-1)] }

// newAsk allocates the next ask address of s, /temp/<n> with a fresh uid (allocAsk), and enters p
// as the pending ask of that address with the given resolver, asker, target and timeout (MSG-300
// step 4). It reports false, entering nothing, when s is stopped: the stopped mark is read under
// the shard's lock, which askTable.stop takes after it sets the mark, so no ask enters the table
// after the stopped system collected its asks.
func (s *System) newAsk(p *pendingAsk, reply resolver, asker, target Address, timeout time.Duration) bool {
	n, uid, ok := s.allocAsk()
	if !ok {
		return false
	}
	p.sys, p.n, p.reply, p.asker, p.target, p.timeout = s, n, reply, asker, target, timeout
	p.addr = Address{node: s.node, path: "/temp/" + strconv.FormatUint(n, 10), uid: uid}
	sh := s.asks.shard(uid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if s.stopped.Load() {
		return false
	}
	sh.add(p)
	return true
}

// add enters the new ask p as pending. The caller holds sh.mu.
func (sh *askShard) add(p *pendingAsk) {
	if sh.local == nil {
		sh.local = make(map[uint64]*pendingAsk)
	}
	sh.local[p.addr.uid] = p
	p.pending, p.next = true, sh.pending
	if sh.pending != nil {
		sh.pending.prev = p
	}
	sh.pending = p
}

// remove ends the pending state of p: it resolved, or its builder panicked. The caller holds
// sh.mu.
func (sh *askShard) remove(p *pendingAsk) {
	p.pending = false
	if p.prev == nil {
		sh.pending = p.next
	} else {
		p.prev.next = p.next
	}
	if p.next != nil {
		p.next.prev = p.prev
	}
	p.prev, p.next = nil, nil
	sh.forget(p)
}

// lookup returns this system's ask with exactly the address ask (node, path and uid), pending or
// tracked, or nil. The caller holds sh.mu.
func (sh *askShard) lookup(ask Address) *pendingAsk {
	if p := sh.local[ask.uid]; p != nil && p.addr == ask {
		return p
	}
	return nil
}

// claim untracks p and returns the list that tracked it, or nil if none did. The caller holds
// sh.mu, and unlinks p from the list after it has released sh.mu (lock order).
func (sh *askShard) claim(p *pendingAsk) *trackList {
	l := p.tracker.Load()
	if l == nil {
		return nil
	}
	p.tracker.Store(nil)
	p.untracked = true
	sh.forget(p)
	return l
}

// forget drops p from the shard's maps once it is neither pending nor tracked. The caller holds
// sh.mu.
func (sh *askShard) forget(p *pendingAsk) {
	switch {
	case p.pending || p.tracker.Load() != nil:
	case p.reply == nil:
		delete(sh.remote, p.addr)
	default:
		delete(sh.local, p.addr.uid)
	}
}

// drop removes p without resolving it: its builder panicked (MSG-300 step 5).
func (t *askTable) drop(p *pendingAsk) {
	sh := t.shard(p.addr.uid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if p.pending {
		sh.remove(p)
	}
}

// stopTimeout stops p's timeout timer, which armTimeout armed.
func (s *System) stopTimeout(p *pendingAsk) {
	if p.cancel != nil {
		p.cancel()
		return
	}
	if dl := s.eng.Deadlines; dl != nil {
		dl.Disarm(p.deadline)
	}
}

// pendingAsks returns each pending ask of s, with its ask address, asker, target and timer, in
// ask-counter order (MSG-370), and nil once s is stopped or discarded. hooks.PendingAsks, SIM's
// accessor, returns them; it reads only the fields that never change. askTable.stop sets the
// stopped mark before it collects, so a system whose stop is resolving its asks reports none.
func (s *System) pendingAsks() []*pendingAsk {
	if s.stopped.Load() {
		return nil
	}
	return s.asks.collect()
}

// stop sets stopped, the system's stopped mark, and returns the addresses of the pending asks in
// ask-counter order (MSG-360), for the shutdown to resolve with ErrStopped. newAsk reads the mark
// under its shard's lock, which stop takes after setting it, so the list is complete.
func (t *askTable) stop(stopped *atomic.Bool) []Address {
	stopped.Store(true)
	ps := t.collect()
	out := make([]Address, len(ps))
	for i, p := range ps {
		out[i] = p.addr
	}
	return out
}

// collect returns the pending asks in ask-counter order, or nil if there are none.
func (t *askTable) collect() []*pendingAsk {
	var ps []*pendingAsk
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for p := sh.pending; p != nil; p = p.next {
			ps = append(ps, p)
		}
		sh.mu.Unlock()
	}
	slices.SortFunc(ps, func(a, b *pendingAsk) int { return cmp.Compare(a.n, b.n) })
	return ps
}

// len returns the number of pending asks.
func (t *askTable) len() int {
	n := 0
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for p := sh.pending; p != nil; p = p.next {
			n++
		}
		sh.mu.Unlock()
	}
	return n
}

// settle is the one place an ask resolves (MSG-430). Under the lock of the ask's shard it looks up
// the pending ask with exactly the address ask (node, path and uid); if there is one, it removes
// it, stops its timer, emits the record of res.how and resolves the reply, which closes its done
// channel. An answer that is not the ask's R resolves it with ErrWrongType instead (MSG-420).
// Otherwise the ask resolved already, or is unknown here: a reply attempt is recorded as late, and
// logged as dropped once the lock is released, and every other attempt is dropped silently.
// res.err is described before the lock is taken, since its Error or String method is user code
// that may use the table. The resolution then runs the continuations of the reply's Pipe calls,
// after it has released the lock and before settle returns (MSG-610).
func (s *System) settle(ask Address, res resolution) { s.settleTracked(ask, res, false) }

// settleTracked is settle that, when untrack is set, first removes ask from the tracked asks under
// the same lock, as a reply attempt does (MSG-410).
func (s *System) settleTracked(ask Address, res resolution, untrack bool) {
	desc := ""
	if res.how != byTimeout {
		desc = s.describeErr(res.err)
	}
	r, late, u := s.resolveLocked(ask, res, desc, untrack)
	u.unlink()
	if late {
		// Logged after the shard's lock is released: a handler is user code (OBS-060). The ask
		// has resolved, so the reply's subject is the ask address.
		s.logMessageDropped(ask, dropLateReply)
	}
	if r != nil {
		r.fire()
	}
}

// resolveLocked is settleTracked under the lock of the ask's shard. It returns the reply it
// resolved, or nil, whether res is a late reply attempt, and the tracked entry it claimed.
func (s *System) resolveLocked(ask Address, res resolution, desc string, untrack bool) (r resolver, late bool, u untracked) {
	sh := s.asks.shard(ask.uid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	p := sh.lookup(ask)
	if untrack && p != nil {
		u = untracked{p: p, l: sh.claim(p)}
	}
	if p == nil || !p.pending {
		if res.how == byReply {
			s.recordReply(ask, res.from, "late", res.err, desc)
			return nil, true, u
		}
		return nil, false, u
	}
	if res.err == nil {
		if err := p.reply.wrongType(ask, res.v); err != nil {
			// The runtime's own error: describing it under the lock runs no user code.
			res.v, res.err, desc = nil, err, s.describeErr(err)
		}
	}
	sh.remove(p)
	if p.timer.Swap(timerStopped) == timerArmed {
		s.stopTimeout(p)
	}
	if res.how == byTimeout {
		s.recordTimeout(p)
	} else {
		s.recordReply(ask, res.from, "resolved", res.err, desc)
	}
	if !p.reply.resolve(res.v, res.err) {
		return nil, false, u
	}
	return p.reply, false, u
}

// describeErr returns Tracer.Describe(err) for the err attribute of lockstep.reply, or "" when
// err is nil or the tracer is disabled.
func (s *System) describeErr(err error) string {
	tr := s.eng.Tracer
	if err == nil || !tr.Enabled() {
		return ""
	}
	return tr.Describe(err)
}

// askTimedOut is the timeout timer's function and p's Expire (MSG-320): it resolves p with
// ErrTimeout if p is still pending.
func (s *System) askTimedOut(p *pendingAsk) {
	s.settle(p.addr, resolution{
		how: byTimeout,
		err: fmt.Errorf("%w: ask %s to %s after %s", ErrTimeout, p.addr, p.target, p.timeout),
	})
}

// replyAttempt issues one reply attempt for ask from the sender from on s (MSG-410), after the
// ReplyTo checks: a zero ask is only recorded; otherwise the ask leaves s's tracked asks, and a
// local attempt is applied to s's table under the same lock before replyAttempt returns (MSG-420),
// while any other one goes to the asker's node (replyRemote). v is the answer, or nil with a
// non-nil err. The runtime issues its own attempts here too (MSG-350).
func (s *System) replyAttempt(ask, from Address, v any, err error) {
	if ask.IsZero() {
		s.recordReply(ask, from, "zero", err, s.describeErr(err))
		return
	}
	// A reply attempt from a stopped system is dropped without a record (MSG-410). The mark is set
	// only after every actor has stopped, so the Fail(ErrTerminated) attempts of those stops
	// (MSG-340) still flow during the shutdown.
	if s.stopped.Load() {
		return
	}
	if ask.node != s.node {
		s.asks.untrack(ask, false)
		s.replyRemote(ask, from, v, err)
		return
	}
	s.settleTracked(ask, resolution{how: byReply, from: from, v: v, err: err}, true)
}

// replyRemote passes a reply attempt for the ask of another node to Transport.Deliver, from the
// replier to the ask address; the asker's system applies it on arrival (MSG-410, host.Inbound).
// Without a transport it is a dead letter (ENG-080, MBX-150 reason unreachable) from the replier
// to the ask address, whose msg is the answer, or the error of a Fail. A Deliver error is
// dropped: ReplyTo.Send and Fail have no result, and REM-266 decides what happens to such a reply.
func (s *System) replyRemote(ask, from Address, v any, err error) {
	p := replyPayload{v: v, err: err}
	tr := s.eng.Transport
	if tr == nil {
		if err != nil {
			s.deadLetter(from, ask, reasonUnreachable, err)
		} else {
			s.deadLetter(from, ask, reasonUnreachable, v)
		}
		return
	}
	_ = tr.Deliver(engine.Envelope{From: from.engine(), To: ask.engine(), Payload: p}) // MSG-410: not returned
}

// track makes the actor c track ask (MSG-340) and reports whether this call added the entry. The
// router calls it before it enqueues the request, so a reply attempt that follows the enqueue
// always finds the entry to remove. local tells that ask belongs to this system: such an ask is
// tracked only while it is pending here, so one that resolved before its request was routed (its
// builder leaked the ReplyTo) gets no Fail(ErrTerminated), and no late record, at the stop. An ask
// of another node gets an entry of its own, which leaves the shard when it is untracked.
func (t *askTable) track(ask Address, c actorCell, local bool) bool {
	l := c.tracks()
	sh := t.shard(ask.uid)
	sh.mu.Lock()
	var p *pendingAsk
	switch {
	case local:
		p = sh.lookup(ask)
		if p == nil || !p.pending || p.untracked || p.tracker.Load() != nil {
			sh.mu.Unlock()
			return false // not pending here, or a duplicate delivery of the request (MSG-340)
		}
	case sh.remote[ask] != nil:
		sh.mu.Unlock()
		return false // a duplicate delivery of the request: tracked once, by the first actor (MSG-340)
	default:
		if sh.remote == nil {
			sh.remote = make(map[Address]*pendingAsk)
		}
		p = &pendingAsk{addr: ask}
		sh.remote[ask] = p
	}
	p.tracker.Store(l)
	sh.mu.Unlock()
	l.link(p)
	return true
}

// untrack removes ask from the tracked asks of s, if it is there (MSG-410, MSG-345), and reports
// whether it was.
func (s *System) untrack(ask Address) bool { return s.asks.untrack(ask, ask.node == s.node) }

// untrack removes ask, an ask of this system if local is set, from the tracked asks if it is
// there, and reports whether it was.
func (t *askTable) untrack(ask Address, local bool) bool {
	sh := t.shard(ask.uid)
	sh.mu.Lock()
	var p *pendingAsk
	if local {
		p = sh.lookup(ask)
	} else {
		p = sh.remote[ask]
	}
	var u untracked
	if p != nil {
		u = untracked{p: p, l: sh.claim(p)}
	}
	sh.mu.Unlock()
	u.unlink()
	return u.l != nil
}

// untrackAll removes every ask c tracks and returns them in tracking order (MSG-340): the final
// stop of c fails each one. A request offered to c after this finds c's mailbox closed, and the
// router removes its entry again, so no entry tracked by c survives the stop.
func (t *askTable) untrackAll(c actorCell) []Address {
	l := c.tracks()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Address
	for p := l.head; p != nil; {
		next := p.tnext
		p.tprev, p.tnext, p.linked = nil, nil, false
		sh := t.shard(p.addr.uid)
		sh.mu.Lock()
		if p.tracker.Load() == l {
			sh.claim(p)
			out = append(out, p.addr)
		}
		sh.mu.Unlock()
		p = next
	}
	l.head, l.tail = nil, nil
	return out
}

// trackedBy returns the asks c tracks, in tracking order (MSG-340).
func (t *askTable) trackedBy(c actorCell) []Address {
	l := c.tracks()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Address
	for p := l.head; p != nil; p = p.tnext {
		if p.tracker.Load() == l {
			out = append(out, p.addr)
		}
	}
	return out
}

// recordAsk emits lockstep.ask (MSG §8): for Ask before the request is routed, for NewReply
// before it returns. A NewReply ask, the only kind with a zero target, records to=<zero> and an
// empty msg; msg is then ignored.
func (s *System) recordAsk(from Address, p *pendingAsk, msg any) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	described := ""
	if !p.target.IsZero() {
		described = tr.Describe(msg)
	}
	tr.Record("lockstep.ask",
		engine.Attr{Key: "from", Value: from.String()},
		engine.Attr{Key: "to", Value: p.target.String()},
		engine.Attr{Key: "ask", Value: p.addr.String()},
		engine.Attr{Key: "timeout", Value: p.timeout.String()},
		engine.Attr{Key: "msg", Value: described},
	)
}

// recordReply emits lockstep.reply (MSG §8) with the given outcome; if err is not nil, desc, its
// description (describeErr), is a last attribute.
func (s *System) recordReply(ask, from Address, outcome string, err error, desc string) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	attrs := []engine.Attr{
		{Key: "ask", Value: ask.String()},
		{Key: "from", Value: from.String()},
		{Key: "outcome", Value: outcome},
	}
	if err != nil {
		attrs = append(attrs, engine.Attr{Key: "err", Value: desc})
	}
	tr.Record("lockstep.reply", attrs...)
}

// recordTimeout emits lockstep.timeout (MSG §8) for p.
func (s *System) recordTimeout(p *pendingAsk) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.timeout",
		engine.Attr{Key: "ask", Value: p.addr.String()},
		engine.Attr{Key: "to", Value: p.target.String()},
		engine.Attr{Key: "after", Value: p.timeout.String()},
	)
}
