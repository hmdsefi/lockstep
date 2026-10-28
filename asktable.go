// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// askTable is a system's ask state (MSG §11): the pending asks whose asker is this system, and the
// asks this system's actors track (MSG-340). One mutex guards both, so applying a reply attempt
// is atomic with every other resolution of the same ask (MSG-420). Lock order: a caller may hold
// the table's lock while it takes the uid allocator's lock, the engine's timer lock (Cancel) or
// the tracer's lock; nothing that holds those calls back into the table.
type askTable struct {
	mu      sync.Mutex
	counter uint64                 // the number of the last ask allocated: /temp/<counter> (MSG-300 step 4)
	pending map[string]*pendingAsk // the pending asks by ask path; the uid is compared on use (MSG-420)
	order   []*pendingAsk          // the pending asks in counter order, with removed ones until compaction
	removed int                    // removed asks still in order

	tracked map[Address]actorCell // tracked ask address -> the actor that tracks it (MSG-340)
	byActor map[actorCell]*trackList
}

// pendingAsk is one pending ask of the asker system (MSG-370).
type pendingAsk struct {
	addr    Address       // the ask address
	asker   Address       // the asking actor, or the system address for a *System sender
	target  Address       // the request's target; the zero Address for NewReply
	timeout time.Duration // the ask's timeout (MSG-320)
	cancel  engine.Cancel // the timeout timer's Cancel; nil until Ask or NewReply arms it
	reply   resolver      // the typed Reply, seen without its R
	removed bool          // no longer pending: resolved, or dropped after a builder panic
}

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

// trackList is the asks one actor tracks, in tracking order. Entries that left the table's
// tracked map stay until the list is compacted.
type trackList struct {
	asks []Address
	live int
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

// newAsk allocates the next ask address of s, /temp/<n> with a fresh uid, and enters a pending
// ask for it with the given asker, target, timeout and resolver (MSG-300 step 4). The number and
// the uid are allocated under the table's lock, so ask numbers and uids rise together. It returns
// nil, allocating nothing, when s is stopped: the stopped mark is set under the same lock
// (askTable.stop), so no ask enters the table after the stopped system resolved its asks.
func (s *System) newAsk(asker, target Address, timeout time.Duration, reply resolver) *pendingAsk {
	t := &s.asks
	t.mu.Lock()
	defer t.mu.Unlock()
	if s.stopped.Load() {
		return nil
	}
	t.counter++
	p := &pendingAsk{
		addr:    Address{node: s.node, path: "/temp/" + strconv.FormatUint(t.counter, 10), uid: s.allocUID()},
		asker:   asker,
		target:  target,
		timeout: timeout,
		reply:   reply,
	}
	if t.pending == nil {
		t.pending = make(map[string]*pendingAsk)
	}
	t.pending[p.addr.path] = p
	t.order = append(t.order, p)
	return p
}

// drop removes p without resolving it: its builder panicked (MSG-300 step 5).
func (t *askTable) drop(p *pendingAsk) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !p.removed {
		t.remove(p)
	}
}

// remove takes the pending p out of the table. The caller holds t.mu.
func (t *askTable) remove(p *pendingAsk) {
	delete(t.pending, p.addr.path)
	p.removed = true
	t.removed++
	if t.removed*2 > len(t.order) {
		t.order = slices.DeleteFunc(t.order, func(q *pendingAsk) bool { return q.removed })
		t.removed = 0
	}
}

// arm stores the Cancel of p's timeout timer. If p already resolved (a reply attempt can arrive
// through a ReplyTo that build leaked to another goroutine), it cancels the timer instead.
func (t *askTable) arm(p *pendingAsk, cancel engine.Cancel) {
	t.mu.Lock()
	if !p.removed {
		p.cancel = cancel
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	cancel()
}

// pendingAsks returns a copy of each pending ask of s, with its ask address, asker, target and
// timer, in ask-counter order (MSG-370), and nil once s is stopped or discarded. The order comes
// from the table's slice, never from its map. hooks.PendingAsks, SIM's accessor, returns it. The
// stopped mark is read under the table's lock, as askTable.stop sets it, so a system whose stop
// is resolving its asks already reports none.
func (s *System) pendingAsks() []pendingAsk {
	t := &s.asks
	t.mu.Lock()
	defer t.mu.Unlock()
	if s.stopped.Load() || len(t.pending) == 0 {
		return nil
	}
	out := make([]pendingAsk, 0, len(t.pending))
	for _, p := range t.order {
		if !p.removed {
			out = append(out, *p)
		}
	}
	return out
}

// stop sets stopped, the system's stopped mark, and returns the addresses of the pending asks in
// ask-counter order (MSG-360), for the shutdown to resolve with ErrStopped after the lock is
// released. newAsk checks the mark under the same lock, so the list is complete.
func (t *askTable) stop(stopped *atomic.Bool) []Address {
	t.mu.Lock()
	defer t.mu.Unlock()
	stopped.Store(true)
	out := make([]Address, 0, len(t.pending))
	for _, p := range t.order {
		if !p.removed {
			out = append(out, p.addr)
		}
	}
	return out
}

// len returns the number of pending asks.
func (t *askTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

// settle is the one place an ask resolves (MSG-430). Under the table's lock it looks up the
// pending ask with exactly the address ask (path and uid); if there is one, it removes it,
// cancels its timer, emits the record of res.how and resolves the reply, which closes its done
// channel. An answer that is not the ask's R resolves it with ErrWrongType instead (MSG-420).
// Otherwise the ask resolved already, or is unknown here: a reply attempt is recorded as late, and
// logged as dropped once the lock is released, and every other attempt is dropped silently.
// res.err is described before the lock is taken, since its Error or String method is user code
// that may use the table. The resolution then runs the continuations of the reply's Pipe calls,
// after it has released the lock and before settle returns (MSG-610).
func (s *System) settle(ask Address, res resolution) {
	desc := ""
	if res.how != byTimeout {
		desc = s.describeErr(res.err)
	}
	r, late := s.resolveLocked(ask, res, desc)
	if late {
		// Logged after the table's lock is released: a handler is user code (OBS-060). The ask
		// has resolved, so the reply's subject is the ask address.
		s.logMessageDropped(ask, dropLateReply)
	}
	if r != nil {
		r.fire()
	}
}

// resolveLocked is settle under the table's lock. It returns the reply it resolved, or nil, and
// whether res is a late reply attempt.
func (s *System) resolveLocked(ask Address, res resolution, desc string) (r resolver, late bool) {
	t := &s.asks
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.pending[ask.path]
	if p == nil || p.addr.uid != ask.uid {
		if res.how == byReply {
			s.recordReply(ask, res.from, "late", res.err, desc)
			return nil, true
		}
		return nil, false
	}
	if res.err == nil {
		if err := p.reply.wrongType(ask, res.v); err != nil {
			// The runtime's own error: describing it under the lock runs no user code.
			res.v, res.err, desc = nil, err, s.describeErr(err)
		}
	}
	t.remove(p)
	if p.cancel != nil {
		p.cancel()
	}
	if res.how == byTimeout {
		s.recordTimeout(p)
	} else {
		s.recordReply(ask, res.from, "resolved", res.err, desc)
	}
	if !p.reply.resolve(res.v, res.err) {
		return nil, false
	}
	return p.reply, false
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

// askTimedOut is the timeout timer's function (MSG-320): it resolves p with ErrTimeout if p is
// still pending.
func (s *System) askTimedOut(p *pendingAsk) {
	s.settle(p.addr, resolution{
		how: byTimeout,
		err: fmt.Errorf("%w: ask %s to %s after %s", ErrTimeout, p.addr, p.target, p.timeout),
	})
}

// replyAttempt issues one reply attempt for ask from the sender from on s (MSG-410), after the
// ReplyTo checks: a zero ask is only recorded; otherwise the ask leaves s's tracked asks, and then
// a local attempt is applied to s's table before replyAttempt returns (MSG-420) and any other one
// goes to the asker's node (replyRemote). v is the answer, or nil with a non-nil err. The runtime
// issues its own attempts here too (MSG-350).
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
	s.asks.untrack(ask)
	if ask.node != s.node {
		s.replyRemote(ask, from, v, err)
		return
	}
	s.settle(ask, resolution{how: byReply, from: from, v: v, err: err})
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
// builder leaked the ReplyTo) gets no Fail(ErrTerminated), and no late record, at the stop.
func (t *askTable) track(ask Address, c actorCell, local bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if local {
		if p := t.pending[ask.path]; p == nil || p.addr.uid != ask.uid {
			return false
		}
	}
	if t.tracked == nil {
		t.tracked = make(map[Address]actorCell)
		t.byActor = make(map[actorCell]*trackList)
	}
	if _, dup := t.tracked[ask]; dup {
		return false // a duplicate delivery of the request: tracked once, by the first actor (MSG-340)
	}
	t.tracked[ask] = c
	l := t.byActor[c]
	if l == nil {
		l = &trackList{}
		t.byActor[c] = l
	}
	l.asks = append(l.asks, ask)
	l.live++
	return true
}

// untrack removes ask from the tracked asks, if it is there (MSG-410, MSG-345), and reports
// whether it was.
func (t *askTable) untrack(ask Address) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	c, ok := t.tracked[ask]
	if !ok {
		return false
	}
	delete(t.tracked, ask)
	l := t.byActor[c]
	l.live--
	switch {
	case l.live == 0:
		delete(t.byActor, c)
	case l.live*2 < len(l.asks):
		l.asks = slices.DeleteFunc(l.asks, func(a Address) bool { return t.tracked[a] != c })
	}
	return true
}

// untrackAll removes every ask c tracks and returns them in tracking order (MSG-340): the final
// stop of c fails each one. A request offered to c after this finds c's mailbox closed, and the
// router removes its entry again, so no list for c survives the stop.
func (t *askTable) untrackAll(c actorCell) []Address {
	t.mu.Lock()
	defer t.mu.Unlock()
	l := t.byActor[c]
	if l == nil {
		return nil
	}
	out := make([]Address, 0, l.live)
	for _, a := range l.asks {
		if t.tracked[a] == c {
			out = append(out, a)
			delete(t.tracked, a)
		}
	}
	delete(t.byActor, c)
	return out
}

// trackedBy returns the asks c tracks, in tracking order (MSG-340).
func (t *askTable) trackedBy(c actorCell) []Address {
	t.mu.Lock()
	defer t.mu.Unlock()
	l := t.byActor[c]
	if l == nil {
		return nil
	}
	out := make([]Address, 0, l.live)
	for _, a := range l.asks {
		if t.tracked[a] == c {
			out = append(out, a)
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
